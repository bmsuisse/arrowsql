"""Custom result sets through fetchone / fetchall / fetchmany / fetchall_arrow."""

import pytest

import mssql_python

from mockutil import tuples

INT_COLS = [("t", "tinyint"), ("s", "smallint"), ("i", "int"), ("b", "bigint")]


@pytest.fixture
def conn(mock):
    c = mock.connect()
    yield c
    c.close()


def test_integer_types_and_extremes(mock, conn):
    mock.register("SELECT ints", INT_COLS, [[0, -32768, -(2**31), -(2**63)], [255, 32767, 2**31 - 1, 2**63 - 1]])
    cur = conn.cursor()
    cur.execute("SELECT ints")
    assert [d[0] for d in cur.description] == ["t", "s", "i", "b"]
    assert tuple(cur.fetchone()) == (0, -32768, -(2**31), -(2**63))
    assert tuple(cur.fetchone()) == (255, 32767, 2**31 - 1, 2**63 - 1)
    assert cur.fetchone() is None


def test_integer_nulls(mock, conn):
    mock.register("SELECT nulls", INT_COLS, [[None, None, None, None], [1, 2, 3, 4]])
    cur = conn.cursor()
    cur.execute("SELECT nulls")
    assert tuples(cur.fetchall()) == [(None, None, None, None), (1, 2, 3, 4)]


def test_nvarchar_unicode(mock, conn):
    values = ["", "plain", "héllo wörld", "日本語", "emoji \U0001f600 pair"]
    mock.register("SELECT names", [("n", "nvarchar")], [[v] for v in values])
    cur = conn.cursor()
    cur.execute("SELECT names")
    assert [r[0] for r in cur.fetchall()] == values


def test_nvarchar_max_chunks_and_null(mock, conn):
    mock.register(
        "SELECT plp",
        [("id", "int"), ("v", "nvarchar_max")],
        [[1, ["ab", "cé", "\U0001f600"]], [2, []], [3, None], [4, ["x" * 1000]]],
    )
    cur = conn.cursor()
    cur.execute("SELECT plp")
    rows = tuples(cur.fetchall())
    assert rows == [(1, "abcé\U0001f600"), (2, ""), (3, None), (4, "x" * 1000)]


def test_varchar_max_and_varbinary_max_chunks(mock, conn):
    mock.register(
        "SELECT lob",
        [("c", "varchar_max"), ("b", "varbinary_max")],
        [[[b"hel".hex(), b"lo".hex()], [b"\x00\x01".hex(), b"\xfe\xff".hex()]], [[], []]],
    )
    cur = conn.cursor()
    cur.execute("SELECT lob")
    first, second = tuples(cur.fetchall())
    assert first[0] == "hello"
    assert bytes(first[1]) == b"\x00\x01\xfe\xff"
    assert second[0] == "" and bytes(second[1]) == b""


def test_multi_row_fetch_styles(mock, conn):
    mock.register("SELECT many", [("i", "int")], [[i] for i in range(250)])
    cur = conn.cursor()
    cur.execute("SELECT many")
    assert cur.fetchone()[0] == 0
    assert [r[0] for r in cur.fetchmany(9)] == list(range(1, 10))
    assert [r[0] for r in cur.fetchall()] == list(range(10, 250))
    assert cur.fetchall() == []


def test_iteration_and_rowcount_semantics(mock, conn):
    mock.register("SELECT few", [("i", "int")], [[1], [2], [3]])
    cur = conn.cursor()
    cur.execute("SELECT few")
    assert [r[0] for r in cur] == [1, 2, 3]


def test_empty_result_set_keeps_description(mock, conn):
    mock.register("SELECT none", [("a", "int"), ("b", "nvarchar")], [])
    cur = conn.cursor()
    cur.execute("SELECT none")
    assert [d[0] for d in cur.description] == ["a", "b"]
    assert cur.fetchall() == []


def test_fetchall_arrow_types(mock, conn):
    pa = pytest.importorskip("pyarrow")
    mock.register(
        "SELECT arrow",
        INT_COLS + [("n", "nvarchar"), ("p", "nvarchar_max")],
        [[1, 2, 3, 4, "a", ["x", "y"]], [5, 6, 7, 8, "b", ["z"]]],
    )
    cur = conn.cursor()
    cur.execute("SELECT arrow")
    table = pa.table(cur.fetchall_arrow())
    assert table.column_names == ["t", "s", "i", "b", "n", "p"]
    assert table.to_pydict() == {
        "t": [1, 5], "s": [2, 6], "i": [3, 7], "b": [4, 8], "n": ["a", "b"], "p": ["xy", "z"],
    }
    assert pa.types.is_int64(table.schema.field("b").type)


def test_fetchall_arrow_large_result_in_batches(mock, conn):
    pa = pytest.importorskip("pyarrow")
    mock.register("SELECT big", [("i", "int"), ("n", "nvarchar")], [[i, f"row{i}"] for i in range(40)])
    cur = conn.cursor()
    cur.execute("SELECT big")
    table = pa.table(cur.fetchall_arrow(batch_size=7))
    assert table.num_rows == 40
    assert table.column("n")[39].as_py() == "row39"


def test_arrow_rejects_null_in_server_declared_non_nullable_column(mock, conn):
    """The mock declares every column NOT NULL, as a server could for a base-table
    column; a NULL in it is a protocol-level lie and must surface as an error,
    not a crash."""
    pytest.importorskip("pyarrow")
    mock.register("SELECT lie", [("a", "int"), ("n", "nvarchar_max")], [[1, ["x"]], [2, None]])
    cur = conn.cursor()
    cur.execute("SELECT lie")
    with pytest.raises(Exception):
        cur.fetchall_arrow()
    cur.close()
    assert conn.cursor().execute("SELECT 1").fetchone()[0] == 1


def test_unregistered_query_has_no_result_set(mock, conn):
    """The mock answers an unknown batch with a bare DONE: no result set, as ODBC/pyodbc
    report it (fetching raises) rather than an empty row list."""
    cur = conn.cursor()
    cur.execute("SELECT never_registered")
    assert cur.description is None
    with pytest.raises(mssql_python.ProgrammingError):
        cur.fetchall()
    assert cur.execute("SELECT 1").fetchone()[0] == 1


def test_reregistering_replaces_the_response(mock, conn):
    cur = conn.cursor()
    mock.register("SELECT swap", [("i", "int")], [[1]])
    assert cur.execute("SELECT swap").fetchone()[0] == 1
    mock.register("SELECT swap", [("i", "int")], [[2]])
    assert cur.execute("SELECT swap").fetchone()[0] == 2


def test_query_matching_is_case_insensitive(mock, conn):
    mock.register("select Mixed", [("i", "int")], [[7]])
    assert conn.cursor().execute("SELECT MIXED").fetchone()[0] == 7
