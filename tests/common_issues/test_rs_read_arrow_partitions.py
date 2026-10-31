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
