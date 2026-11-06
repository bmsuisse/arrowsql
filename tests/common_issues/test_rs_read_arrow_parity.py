"""The partitioned read in Rust (arrowsql-core) against the Python implementation it replaced.

``arrow_parallel_reference.py`` is the previous ``arrow_parallel.py`` and
``_utf8_strings.py``, verbatim. Both must plan the same statements (SQL text,
parameter values and order), probe the same metadata, rewrite the same
queries for UTF-8 transfer, read the same rows (in the same order for a read
that is not split) and raise the same exception classes.
"""

import datetime
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")

import mssql_python  # noqa: E402
from mssql_python import arrow_parallel, ddbc_bindings  # noqa: E402
from mssql_python.db_connection import connect  # noqa: E402

import arrow_parallel_reference as ref  # noqa: E402

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "ParallelArrowStream"), reason="native parallel read is Rust-port only"
)

ROWS = 60_000


@pytest.fixture
def table(cursor, table_name):
    """id: bigint, sparse (every 3rd value), NULL every 50th row; k: 7 distinct values;
    s: nvarchar text; d: date; m: decimal."""
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id bigint NULL, k int NOT NULL, s nvarchar(40) NULL, "
        "d date NOT NULL, m decimal(12, 3) NOT NULL)"
    )
    cursor.execute(
        f"INSERT INTO dbo.{table_name} SELECT CASE WHEN n % 50 = 0 THEN NULL ELSE 3 * n END, n % 7, "
        "CONCAT(N'row ä ', RIGHT(CONCAT('00000', n), 5)), DATEADD(day, n % 900, '2020-01-01'), n / 7.0 "
        f"FROM (SELECT TOP ({ROWS}) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b) AS g"
    )
    cursor.connection.commit()
    return f"dbo.{table_name}"


def _old_plan(conn_str, query, partition_on=None, partitions=1, bounds=None, nullable=None, utf8=True):
    if not ref._partitioned(partition_on, partitions, bounds):
        with connect(conn_str, autocommit=True) as conn:
            return [(ref.utf8_query(conn, query) if utf8 else query, ())]
    conn, work = ref._open_plan(conn_str, query, partition_on, partitions, bounds, nullable, {}, utf8)
    conn.close()
    return work


def _norm(work):
    return [(sql, tuple(params)) for sql, params in work]


PLANS = [
    dict(),
    dict(utf8=False),
    dict(partition_on="id", partitions=4),
    dict(partition_on="id", partitions=4, utf8=False),
    dict(partition_on="id", partitions=64),
    dict(partition_on="id", partitions=1),
    dict(partition_on="k", partitions=8),
    dict(partition_on="id", partitions=3, bounds=[30_000, 90_000]),
    dict(partition_on="id", partitions=2, bounds=[30_000], nullable=False),
    dict(partition_on="s", partitions=3, bounds=["row ä 01000", "row ä 30000"]),
    dict(partition_on="d", partitions=2, bounds=[datetime.date(2021, 1, 1), datetime.date(2021, 6, 1)]),
    dict(partition_on="m", partitions=2, bounds=[Decimal("100.5"), Decimal("4000")]),
    dict(partition_on="id", partitions=4, bounds=[]),
]


@pytest.mark.parametrize("args", PLANS, ids=[str(sorted(a.items())) for a in PLANS])
def test_same_plan(conn_str, table, args):
    utf8 = args.get("utf8", True)
    query = f"SELECT id, k, s, d, m FROM {table}"
    old = _old_plan(conn_str, query, args.get("partition_on"), args.get("partitions", 1),
                    args.get("bounds"), args.get("nullable"), utf8)
    new = arrow_parallel._plan_read(
        conn_str, query, args.get("partition_on"), args.get("partitions", 1), args.get("bounds"),
        args.get("nullable"), utf8,
    )
    assert _norm(new) == _norm(old)


@pytest.mark.parametrize("query_kind", ["plain", "filtered", "small"])
@pytest.mark.parametrize("column,partitions", [("id", 4), ("id", 16), ("k", 4)])
def test_same_range_plan_and_probe(conn_str, table, query_kind, column, partitions):
    query = {
        "plain": f"SELECT id, k, s FROM {table}",
        "filtered": f"SELECT id, k, s FROM {table} WHERE k <> 3",
        "small": f"SELECT TOP (1000) id, k, s FROM {table}",
    }[query_kind]
    quoted = f"[{column}]"
    with connect(conn_str, autocommit=True) as conn:
        old = ref._plan(conn, query, quoted, column, partitions)
        new = arrow_parallel._plan(conn, query, quoted, column, partitions)
        assert _norm(new) == _norm(old)
        with conn.cursor() as cur:
            assert arrow_parallel._probe_meta(conn, query, quoted) == ref._probe_meta(cur, query, quoted)
            assert arrow_parallel._probe_meta(conn, query, quoted, with_rows=False) == ref._probe_meta(
                cur, query, quoted, with_rows=False
            )


def test_same_bounds_plan(conn_str, table):
    query = f"SELECT id, s FROM {table}"
    cases = [("[id]", [10, 20, 30], None), ("[id]", [10], False), ("[s]", ["a", "b"], True), ("[s]", ["a"], None), ("[id]", [], None)]
    with connect(conn_str, autocommit=True) as conn:
        for column, bounds, nullable in cases:
            old = ref._bounds_plan(conn, query, column, bounds, nullable)
            new = arrow_parallel._bounds_plan(conn, query, column, bounds, nullable)
            assert _norm(new) == _norm(old)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, s FROM {t}",
        "SELECT id, s FROM {t} ORDER BY id",
        "SELECT id, k FROM {t}",
        "SELECT TOP (10) s, s AS s2 FROM {t}",
        "SELECT REPLICATE(N'中文', 30) AS cjk",
        "SELECT FROM",
    ],
)
def test_same_utf8_rewrite(conn_str, table, query):
    from mssql_python._utf8_strings import utf8_query

    query = query.format(t=table)
    with connect(conn_str, autocommit=True) as conn:
        assert utf8_query(conn, query) == ref.utf8_query(conn, query)


def _sorted(table, key):
    return pa.table(table).sort_by([(key, "ascending"), ("k", "ascending"), ("s", "ascending")])


READS = [
    dict(),
    dict(partition_on="id", partitions=4),
    dict(partition_on="id", partitions=8, utf8_strings=False),
    dict(partition_on="k", partitions=3),
    dict(partition_on="id", partitions=3, partition_bounds=[30_000, 90_000]),
    dict(partition_on="s", partitions=2, partition_bounds=["row ä 20000"]),
]


@pytest.mark.parametrize("kwargs", READS, ids=[str(sorted(a.items())) for a in READS])
def test_same_rows(conn_str, table, kwargs):
    query = f"SELECT id, k, s, d, m FROM {table}"
    old = pa.table(ref.read_arrow(conn_str, query, batch_size=7_000, **kwargs))
    new = pa.table(mssql_python.read_arrow(conn_str, query, batch_size=7_000, **kwargs))
    assert new.schema == old.schema
    assert new.num_rows == old.num_rows == ROWS
    if "partition_on" in kwargs:
        assert _sorted(new, "id").equals(_sorted(old, "id"))
    else:
        # One statement on one connection: the same rows in the same order and batches.
        assert new.equals(old)
        assert [b.num_rows for b in new.to_batches()] == [b.num_rows for b in old.to_batches()]

    stream_old = pa.table(ref.read_arrow_stream(conn_str, query, batch_size=7_000, **kwargs))
    stream_new = pa.table(mssql_python.read_arrow_stream(conn_str, query, batch_size=7_000, **kwargs))
    assert stream_new.schema == stream_old.schema
    assert _sorted(stream_new, "id").equals(_sorted(stream_old, "id"))
    if "partition_on" not in kwargs:
        assert stream_new.equals(stream_old)


def _raised(fn):
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - the class is what is compared
        return type(exc)
    return None


@pytest.mark.parametrize(
    "query,kwargs",
    [
        ("SELECT * FROM dbo.no_such_table_parity", dict(partition_on="id", partitions=4)),
        ("SELECT * FROM dbo.no_such_table_parity", dict()),
        ("SELECT id, s FROM {t}", dict(partition_on="s", partitions=4)),
        ("SELECT id, 1 / (k - 3) AS x FROM {t}", dict(partition_on="id", partitions=4)),
        ("SELECT id, 1 / (k - 3) AS x FROM {t}", dict()),
        ("SELECT 1 AS id", dict(partition_on="id", partition_bounds=[2, 1])),
        ("SELECT 1 AS id", dict(partition_on="id", partition_bounds=[1, "a"])),
        ("SELECT 1 AS id", dict(partition_bounds=[1])),
    ],
)
def test_same_exception_class(conn_str, table, query, kwargs):
    query = query.format(t=table)
    old = _raised(lambda: ref.read_arrow(conn_str, query, **kwargs))
    new = _raised(lambda: mssql_python.read_arrow(conn_str, query, **kwargs))
    assert old is not None
    assert new is old
