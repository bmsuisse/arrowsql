"""UTF-8 transfer of nvarchar/nchar/ntext columns on the Arrow read paths.

read_arrow / cursor.execute_arrow rewrite the query so the server sends those
columns as UTF-8 varchar instead of UTF-16. The result must be identical to
the unrewritten read. Rust-port only.
"""

import pytest

pa = pytest.importorskip("pyarrow")

import mssql_python  # noqa: E402
from mssql_python import ddbc_bindings  # noqa: E402
from mssql_python._utf8_strings import utf8_query  # noqa: E402

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "ArrowTable"), reason="native Arrow API is Rust-port only"
)

LONG = "xyzä€\U0001f600" * 3000  # 18000 chars, > 8000 bytes in UTF-8
VALUES = [
    ("ascii", "ascii", "ab", "plain"),
    ("accénts äöü", "é", "ä", "é"),
    ("中文漢字", "中", "文", "中文"),
    ("emoji \U0001f600\U0001f680 \U0001d11e", "\U0001f600", "\U0001f680\U0001f600", "\U0001f600"),
    (None, None, None, None),
    ("", "", "", ""),
    ("xyz\u00e4\u20ac" * 800, "z", "z", LONG),
    ("x" * 4000, "x" * 10, "end ", "  lead and trail  "),
]


@pytest.fixture
def strings_table(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int NOT NULL PRIMARY KEY, "
        "v nvarchar(4000) NULL, c nchar(10) NULL, c2 nchar(4) NULL, m nvarchar(max) NULL)"
    )
    for i, (v, c, c2, m) in enumerate(VALUES):
        cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?, ?, ?, ?, ?)", (i, v, c, c2, m))
    cursor.connection.commit()
    return table_name


@pytest.fixture
def needs_utf8(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int), "
        "CAST(SERVERPROPERTY('EngineEdition') AS int)"
    )
    major, edition = cur.fetchone()
    cur.close()
    if not ddbc_bindings.utf8_collations_supported(major, edition):
        pytest.skip("server has no UTF-8 collations")


def _pad(text, units):
    """nchar(n) pads to n UTF-16 code units (an emoji counts twice)."""
    return text + " " * (units - len(text.encode("utf-16-le")) // 2)


def _same(a, b):
    ta, tb = pa.table(a), pa.table(b)
    assert ta.schema == tb.schema
    assert ta.to_pydict() == tb.to_pydict()


def test_roundtrip_matches_plain_read(conn_str, conn, strings_table, needs_utf8):
    query = f"SELECT id, v, c, c2, m FROM dbo.{strings_table}"
    assert "UTF8" in utf8_query(conn, query)
    on = mssql_python.read_arrow(conn_str, query)
    off = mssql_python.read_arrow(conn_str, query, utf8_strings=False)
    _same(on, off)
    rows = {r["id"]: r for r in pa.table(on).to_pylist()}
    for i, (v, c, c2, m) in enumerate(VALUES):
        assert rows[i]["v"] == v and rows[i]["m"] == m
        # nchar keeps its blank padding
        assert rows[i]["c"] == (None if c is None else _pad(c, 10))
        assert rows[i]["c2"] == (None if c2 is None else _pad(c2, 4))
    assert len(rows[6]["m"].encode()) > 8000
    assert pa.table(on).schema.field("v").type == pa.large_string()


def test_ntext_and_expressions(conn_str, conn, needs_utf8):
    query = "SELECT CAST(N'a€\U0001f600' AS ntext) AS t, CONVERT(nvarchar(3), N'abc') AS e, 1 AS n, N'' AS z"
    assert "UTF8" in utf8_query(conn, query)
    _same(
        mssql_python.read_arrow(conn_str, query),
        mssql_python.read_arrow(conn_str, query, utf8_strings=False),
    )


def test_not_null_column_keeps_nullability(conn_str, cursor, table_name, needs_utf8):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (a nvarchar(20) NOT NULL, b nchar(3) NULL)")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (N'\u00e4\U0001f600', NULL), (N'', N'x')")
    cursor.connection.commit()
    query = f"SELECT a, b FROM dbo.{table_name}"
    on = mssql_python.read_arrow(conn_str, query)
    _same(on, mssql_python.read_arrow(conn_str, query, utf8_strings=False))
    assert not pa.table(on).schema.field("a").nullable


def test_bracket_in_column_name(conn_str, conn, needs_utf8):
    query = "SELECT N'valä' AS [a]]b], 7 AS [x y], N'\U0001f600' AS [select]"
    wrapped = utf8_query(conn, query)
    assert "[a]]b]" in wrapped
    on = pa.table(mssql_python.read_arrow(conn_str, query))
    assert on.column_names == ["a]b", "x y", "select"]
    assert on.to_pydict() == {"a]b": ["valä"], "x y": [7], "select": ["\U0001f600"]}


def test_duplicate_and_empty_names_fall_back(conn_str, conn, needs_utf8):
    for query in ("SELECT N'a' AS x, N'b' AS x", "SELECT N'a', N'b'", "SELECT N'a' AS x, N'b' AS X"):
        assert utf8_query(conn, query) == query
        _same(
            mssql_python.read_arrow(conn_str, query),
            mssql_python.read_arrow(conn_str, query, utf8_strings=False),
        )


def test_no_national_columns_unchanged(conn):
    for query in ("SELECT 1 AS a, 'x' AS b", "SELECT 1 AS a WHERE 1 = 0"):
        assert utf8_query(conn, query) == query


@pytest.mark.parametrize(
    "query",
    ["SELECT N'a' AS x ORDER BY 1", "EXEC sp_who2", "DECLARE @a int = 1; SELECT N'a' AS x", "SELECT FROM"],
)
def test_unrewritable_queries_fall_back(conn, query):
    assert utf8_query(conn, query) == query


def test_opt_out_runs_original(conn_str, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("describe ran")

    monkeypatch.setattr("mssql_python.arrow_parallel.utf8_query", boom)
    table = mssql_python.read_arrow(conn_str, "SELECT N'aä' AS x", utf8_strings=False)
    assert pa.table(table).to_pydict() == {"x": ["aä"]}


@pytest.mark.parametrize("partitions", [1, 4])
def test_partitioned_on_off_identical(conn_str, cursor, table_name, partitions, needs_utf8):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id bigint NOT NULL, s nvarchar(50) NULL, t nchar(3) NULL)")
    cursor.execute(
        f"INSERT INTO dbo.{table_name} SELECT n, "
        "CASE WHEN n % 7 = 0 THEN NULL ELSE CONCAT(N'row ä€ ', n, N' \U0001f600') END, N'q' "
        "FROM (SELECT TOP (60000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b) AS g"
    )
    cursor.connection.commit()
    query = f"SELECT id, s, t FROM dbo.{table_name}"

    def read(flag):
        table = mssql_python.read_arrow(
            conn_str, query, partition_on="id", partitions=partitions, utf8_strings=flag
        )
        return pa.table(table).sort_by("id")

    on, off = read(True), read(False)
    assert on.num_rows == 60000
    assert on.equals(off)


def test_cursor_execute_arrow(cursor, strings_table):
    query = f"SELECT id, v, c FROM dbo.{strings_table}"
    on = cursor.execute_arrow(query)
    off = cursor.execute_arrow(query, utf8_strings=False)
    _same(on, off)
    # with parameters the query runs unchanged
    t = cursor.execute_arrow(f"SELECT v FROM dbo.{strings_table} WHERE id = ?", (1,))
    assert pa.table(t).to_pydict() == {"v": [VALUES[1][0]]}


def test_server_gate():
    assert not ddbc_bindings.utf8_collations_supported(14, 3)
    assert ddbc_bindings.utf8_collations_supported(15, 3)
    assert ddbc_bindings.utf8_collations_supported(12, 5)
    assert not ddbc_bindings.utf8_collations_supported(None, None)
