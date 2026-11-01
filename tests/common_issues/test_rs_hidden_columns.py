"""Hidden result columns (COLMETADATA fHidden: FOR BROWSE keys/timestamps, the
ROWSTAT of a T-SQL FETCH) never reach the user, like msodbcsql and upstream."""

import asyncio
import uuid

import pytest

from mssql_python import connect, ddbc_bindings

RUST = getattr(ddbc_bindings, "__rust_port__", False)
rust_only = pytest.mark.skipif(not RUST, reason="Arrow / agent / aio are Rust-port only")


@pytest.fixture
def cur(conn_str):
    c = connect(conn_str, autocommit=True)
    cursor = c.cursor()
    yield cursor
    cursor.close()
    c.close()


@pytest.fixture
def browse(cur):
    """A table with a primary key and a timestamp, so FOR BROWSE adds hidden columns."""
    name = f"rs_hidden_{uuid.uuid4().hex[:10]}"
    cur.execute(f"CREATE TABLE dbo.{name} (id int PRIMARY KEY, name varchar(20) NOT NULL, ts timestamp)")
    cur.execute(f"INSERT INTO dbo.{name} (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    yield name
    cur.execute(f"DROP TABLE dbo.{name}")


def as_dict(result):
    import pyarrow as pa

    return pa.table(result).to_pydict()


def q_end(t):
    return f"SELECT name FROM dbo.{t} ORDER BY name FOR BROWSE"


def q_mid(t):
    return f"SELECT name, 7 AS seven FROM dbo.{t} ORDER BY name FOR BROWSE"


def test_description_and_rows(cur, browse):
    cur.execute(q_end(browse))
    assert [d[0] for d in cur.description] == ["name"]
    assert [tuple(r) for r in cur.fetchall()] == [("a",), ("b",), ("c",)]
    cur.execute(q_end(browse))
    assert tuple(cur.fetchone()) == ("a",)
    assert [tuple(r) for r in cur.fetchmany(2)] == [("b",), ("c",)]


def test_hidden_columns_between_visible_ones(cur, browse):
    # id is selected explicitly; the browse key/timestamp are appended hidden
    # after the select list, a computed column in the middle stays aligned.
    cur.execute(f"SELECT name, id, 7 AS seven FROM dbo.{browse} ORDER BY id FOR BROWSE")
    assert [d[0] for d in cur.description] == ["name", "id", "seven"]
    assert [tuple(r) for r in cur.fetchall()] == [("a", 1, 7), ("b", 2, 7), ("c", 3, 7)]
    cur.execute(q_mid(browse))
    assert [d[0] for d in cur.description] == ["name", "seven"]
    assert [tuple(r) for r in cur.fetchall()] == [("a", 7), ("b", 7), ("c", 7)]


def test_row_object(cur, browse):
    cur.execute(q_mid(browse))
    row = cur.fetchone()
    assert len(row) == 2 and row.name == "a" and row.seven == 7


def test_server_cursor_fetch_has_no_rowstat(cur):
    cur.execute(
        "DECLARE c CURSOR LOCAL FOR SELECT 1 AS a, 'x' AS b; OPEN c; FETCH NEXT FROM c; CLOSE c; DEALLOCATE c"
    )
    while cur.description is None or cur.description[0][0] != "a":
        assert cur.nextset()
    assert [d[0] for d in cur.description] == ["a", "b"]
    assert tuple(cur.fetchone()) == (1, "x")


@rust_only
def test_arrow_paths(conn_str, cur, browse):
    expect = {"name": ["a", "b", "c"], "seven": [7, 7, 7]}
    sql = q_mid(browse)
    cur.execute(sql)
    assert as_dict(cur.arrow()) == expect
    cur.execute(sql)
    assert as_dict(cur.fetchall_arrow()) == expect
    cur.execute(sql)
    assert as_dict(cur.arrow_stream()) == expect
    assert as_dict(cur.execute_arrow(sql)) == expect

    from arrowsql import read_arrow, read_arrow_stream

    assert as_dict(read_arrow(conn_str, sql)) == expect
    assert as_dict(read_arrow_stream(conn_str, sql)) == expect


@rust_only
def test_agent_rendering(conn_str, browse):
    from arrowsql.agent import query

    text = query(conn_str, q_mid(browse))
    assert text.splitlines()[0].count("\t") == 1
    assert "rows_shown=3" in text


@rust_only
def test_aio(conn_str, browse):
    from arrowsql import aio

    async def go():
        conn = await aio.connect(conn_str, autocommit=True)
        try:
            cur = await conn.execute(q_mid(browse))
            assert [d[0] for d in cur.description] == ["name", "seven"]
            assert [tuple(r) for r in await cur.fetchall()] == [("a", 7), ("b", 7), ("c", 7)]
            cur = await conn.execute(q_mid(browse))
            assert tuple(await cur.fetchone()) == ("a", 7)
            cur = await conn.execute(q_mid(browse))
            assert as_dict(await cur.fetchall_arrow()) == {"name": ["a", "b", "c"], "seven": [7, 7, 7]}
            assert [tuple(r) for r in await conn.fetch(q_mid(browse))] == [("a", 7), ("b", 7), ("c", 7)]
        finally:
            await conn.close()

    asyncio.run(go())
