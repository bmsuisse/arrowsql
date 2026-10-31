"""Partitioned read_arrow: slice queue, NULL handling and predicate shape.

read_arrow is Rust-port only (native ArrowTable), so the module skips upstream.
"""

import pytest

pa = pytest.importorskip("pyarrow")

import mssql_python  # noqa: E402
from mssql_python import arrow_parallel, ddbc_bindings  # noqa: E402
from mssql_python.db_connection import connect  # noqa: E402

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "ArrowTable"), reason="native Arrow API is Rust-port only"
)

ROWS = 120_000


def _fill(cursor, name, nullable, rows=ROWS, extra_nulls=0):
    null = "NULL" if nullable else "NOT NULL"
    cursor.execute(f"CREATE TABLE dbo.{name} (id bigint {null}, v int NOT NULL)")
    # non-dense key: every third value, so the key range is 3x the row count
    cursor.execute(
        f"INSERT INTO dbo.{name} SELECT TOP ({rows}) 3 * ROW_NUMBER() OVER (ORDER BY (SELECT 1)), "
        "ROW_NUMBER() OVER (ORDER BY (SELECT 1)) % 97 FROM sys.all_columns a CROSS JOIN sys.all_columns b"
    )
    for _ in range(extra_nulls):
        cursor.execute(f"INSERT INTO dbo.{name} VALUES (NULL, -1)")
    cursor.connection.commit()


def _sorted(table):
    return pa.table(table).sort_by([("id", "ascending"), ("v", "ascending")])


def _check(conn_str, name, partitions):
    query = f"SELECT id, v FROM dbo.{name}"
    single = pa.table(mssql_python.read_arrow(conn_str, query))
    parallel = pa.table(
        mssql_python.read_arrow(conn_str, query, partition_on="id", partitions=partitions)
    )
    assert parallel.num_rows == single.num_rows
    assert _sorted(parallel).equals(_sorted(single))
    return parallel


@pytest.mark.parametrize("partitions", [2, 8, 64])
def test_nullable_column_with_nulls_matches_single(conn_str, cursor, table_name, partitions):
    _fill(cursor, table_name, nullable=True, extra_nulls=7)
    result = _check(conn_str, table_name, partitions)
    assert result.num_rows == ROWS + 7
    assert result.column("id").null_count == 7


def test_nullable_column_without_nulls(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=True)
    assert _check(conn_str, table_name, 4).column("id").null_count == 0


def test_not_null_column_matches_single(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False)
    assert _check(conn_str, table_name, 8).num_rows == ROWS


def test_slice_predicates_are_open_ended_and_skip_null_term(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False)
    query = f"SELECT id, v FROM dbo.{table_name}"
    with connect(conn_str, autocommit=True) as conn:
        work = arrow_parallel._plan(conn, query, "[id]", "id", 4)
    assert len(work) > 4
    assert not any("IS NULL" in sql for sql, _ in work)
    assert work[0][0].endswith("[id] < ?")
    assert work[-1][0].endswith("[id] >= ?")
    assert all(sql.count("?") == len(params) for sql, params in work)

    cursor.execute(f"ALTER TABLE dbo.{table_name} ALTER COLUMN id bigint NULL")
    cursor.connection.commit()
    with connect(conn_str, autocommit=True) as conn:
        work = arrow_parallel._plan(conn, query, "[id]", "id", 4)
    assert work[0][0].endswith("[id] IS NULL")
    assert sum("IS NULL" in sql for sql, _ in work) == 1


def test_empty_table(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=True, rows=0)
    result = mssql_python.read_arrow(
        conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=8
    )
    assert pa.table(result).num_rows == 0
    assert pa.table(result).column_names == ["id", "v"]


def test_only_null_keys(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=True, rows=0, extra_nulls=5)
    result = pa.table(
        mssql_python.read_arrow(
            conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=8
        )
    )
    assert result.num_rows == 5


@pytest.mark.parametrize("rows", [1, 3, 25_000])
def test_more_partitions_than_rows(conn_str, cursor, table_name, rows):
    _fill(cursor, table_name, nullable=True, rows=rows)
    assert _check(conn_str, table_name, 16).num_rows == rows


def test_query_with_filter_and_alias(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False)
    query = f"SELECT id AS k, v FROM dbo.{table_name} WHERE v < 50"
    single = pa.table(mssql_python.read_arrow(conn_str, query))
    parallel = pa.table(mssql_python.read_arrow(conn_str, query, partition_on="k", partitions=6))
    key = [("k", "ascending"), ("v", "ascending")]
    assert parallel.sort_by(key).equals(single.sort_by(key))


def test_non_integer_column_error(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (s nvarchar(10), d float)")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (N'a', 1.5), (N'b', 2.5)")
    cursor.connection.commit()
    for column in ("s", "d"):
        with pytest.raises(TypeError, match="integer"):
            mssql_python.read_arrow(
                conn_str, f"SELECT * FROM dbo.{table_name}", partition_on=column, partitions=4
            )


def test_worker_error_propagates(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False)
    # division by zero fails inside a slice; the error reaches the caller
    with pytest.raises(Exception):
        mssql_python.read_arrow(
            conn_str,
            f"SELECT id, v, 1/(v-v) AS boom FROM dbo.{table_name}",
            partition_on="id",
            partitions=4,
        )


def test_bigint_key_range_wider_than_int32(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id bigint NOT NULL, v int NOT NULL)")
    # keys from -6e9 to +6e9: wider than 2**32, slice edges do not fit an int32
    cursor.execute(
        f"INSERT INTO dbo.{table_name} SELECT TOP ({ROWS}) "
        "(ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 60000) * 100000, 1 "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b"
    )
    cursor.connection.commit()
    query = f"SELECT id, v FROM dbo.{table_name}"
    with connect(conn_str, autocommit=True) as conn:
        work = arrow_parallel._plan(conn, query, "[id]", "id", 4)
    edges = [value for _, params in work for value in params]
    assert len(work) > 2
    assert max(edges) > 2**31 and min(edges) < -(2**31)
    assert _check(conn_str, table_name, 4).num_rows == ROWS


class _DescribeFails:
    """Connection proxy whose cursors fail the sys.dm_exec_describe_first_result_set call."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        real = self._conn.cursor()

        class _Cursor:
            def execute(self, sql, *args):
                if "dm_exec_describe_first_result_set" in sql:
                    raise RuntimeError("describe not permitted")
                return real.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(real, name)

        return _Cursor()


def test_describe_failure_counts_as_nullable(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False, extra_nulls=0)
    query = f"SELECT id, v FROM dbo.{table_name}"
    with connect(conn_str, autocommit=True) as conn:
        work = arrow_parallel._plan(_DescribeFails(conn), query, "[id]", "id", 4)
    assert work[0][0].endswith("[id] IS NULL")


def test_extra_worker_connect_failure_aborts(conn_str, cursor, table_name, monkeypatch):
    _fill(cursor, table_name, nullable=False, rows=400_000)
    real_connect = arrow_parallel.connect
    calls = []

    def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) == 3:  # probe is call 1; this is the second extra worker
            raise ConnectionError("no more connections")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(arrow_parallel, "connect", flaky)
    with pytest.raises(ConnectionError, match="no more connections"):
        mssql_python.read_arrow(
            conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=4
        )


@pytest.mark.parametrize("distinct", [1, 5])
def test_many_rows_few_key_values_still_parallel(conn_str, cursor, table_name, monkeypatch, distinct):
    rows = 600_000
    cursor.execute(f"CREATE TABLE dbo.{table_name} (k int NOT NULL, v int NOT NULL)")
    cursor.execute(
        f"INSERT INTO dbo.{table_name} SELECT TOP ({rows}) "
        f"ROW_NUMBER() OVER (ORDER BY (SELECT 1)) % {distinct}, 1 "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b CROSS JOIN sys.all_columns c"
    )
    cursor.connection.commit()
    real_connect = arrow_parallel.connect
    connects = []

    def counting(*args, **kwargs):
        connects.append(1)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(arrow_parallel, "connect", counting)
    result = pa.table(
        mssql_python.read_arrow(
            conn_str,
            f"SELECT k, v, @@SPID AS spid FROM dbo.{table_name}",
            partition_on="k",
            partitions=4,
        )
    )
    assert result.num_rows == rows
    assert sorted(set(result.column("k").to_pylist())) == list(range(distinct))
    if distinct == 1:
        # one key value cannot be cut: one stream
        assert len(connects) == 1
    else:
        assert len(connects) > 1
        assert len(set(result.column("spid").to_pylist())) > 1


def test_slice_count_capped_at_key_span(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (k int NOT NULL, v int NOT NULL)")
    cursor.execute(
        f"INSERT INTO dbo.{table_name} SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) % 3, 1 "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b"
    )
    cursor.connection.commit()
    with connect(conn_str, autocommit=True) as conn:
        work = arrow_parallel._plan(conn, f"SELECT k, v FROM dbo.{table_name}", "[k]", "k", 8)
    assert len(work) == 3  # keys 0..2: one slice per key value, no empty slices


def test_small_table_single_stream(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False, rows=5_000)
    with connect(conn_str, autocommit=True) as conn:
        assert arrow_parallel._plan(conn, f"SELECT id, v FROM dbo.{table_name}", "[id]", "id", 8) == []
