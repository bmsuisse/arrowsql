"""r5 fixes in the async session and the executemany path (F12-F14).

Port-only: upstream mssql-python has no ``aio`` module.
"""

import datetime
from decimal import Decimal

import pytest

import mssql_python

aio = pytest.importorskip("mssql_python.aio")
pytest_asyncio = pytest.importorskip("pytest_asyncio")

from mssql_python import ddbc_bindings as _ddbc  # noqa: E402

needs_aio = pytest.mark.skipif(not hasattr(_ddbc, "AioHub"), reason="aio is Unix-only")


@pytest_asyncio.fixture
async def aconn(conn_str):
    connection = await aio.connect(conn_str, autocommit=True)
    yield connection
    await connection.close()


# ---- F12: executemany with a leading NULL decimal keeps precision and scale

_DECIMAL_ROWS = [
    [(None,), (Decimal("1.123456789012345"),)],
    [(None,), (None,), (Decimal("-7.000000000000001"),), (None,)],
]


@pytest.mark.parametrize("rows", _DECIMAL_ROWS)
def test_executemany_leading_null_decimal_sync(conn, rows):
    cur = conn.cursor()
    cur.execute("CREATE TABLE #r5_d (i INT IDENTITY, d decimal(30,15))")
    cur.executemany("INSERT INTO #r5_d (d) VALUES (?)", rows)
    got = [r[0] for r in cur.execute("SELECT d FROM #r5_d ORDER BY i").fetchall()]
    assert got == [None if r[0] is None else r[0] for r in rows]


@needs_aio
@pytest.mark.asyncio
@pytest.mark.parametrize("rows", _DECIMAL_ROWS)
async def test_executemany_leading_null_decimal_async(aconn, rows):
    await aconn.execute("CREATE TABLE #r5_d (i INT IDENTITY, d decimal(30,15))")
    cur = aconn.cursor()
    await cur.executemany("INSERT INTO #r5_d (d) VALUES (?)", rows)
    await cur.execute("SELECT d FROM #r5_d ORDER BY i")
    got = [r[0] for r in await cur.fetchall()]
    assert got == [r[0] for r in rows]


def test_executemany_null_decimal_across_chunks_sync(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE #r5_c (i INT IDENTITY, d decimal(30,15))")
    rows = [(None,)] * 3000 + [(Decimal("1.123456789012345"),)]
    cur.executemany("INSERT INTO #r5_c (d) VALUES (?)", rows)
    assert cur.execute("SELECT d FROM #r5_c WHERE d IS NOT NULL").fetchall()[0][0] == Decimal("1.123456789012345")


@needs_aio
@pytest.mark.asyncio
async def test_executemany_null_decimal_across_chunks_async(aconn):
    await aconn.execute("CREATE TABLE #r5_c (i INT IDENTITY, d decimal(30,15))")
    cur = aconn.cursor()
    rows = [(None,)] * 3000 + [(Decimal("1.123456789012345"),)]
    await cur.executemany("INSERT INTO #r5_c (d) VALUES (?)", rows)
    await cur.execute("SELECT d FROM #r5_c WHERE d IS NOT NULL")
    assert (await cur.fetchall())[0][0] == Decimal("1.123456789012345")


# ---- F14: after a server-side rollback (XACT_ABORT) writes stay rejected until
# commit()/rollback(), whatever the statement looks like


def _aborted_table(cur, name):
    cur.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY)")
    cur.connection.commit()
    cur.execute("SET XACT_ABORT ON")
    cur.execute(f"INSERT INTO {name} VALUES (1)")
    with pytest.raises(mssql_python.IntegrityError):
        cur.execute(f"INSERT INTO {name} VALUES (1)")


_BYPASS_WRITES = [
    "/* comment */ INSERT INTO {t} VALUES (2)",
    "-- c\nINSERT INTO {t} VALUES (2)",
    "WITH x AS (SELECT 2 AS id) INSERT INTO {t} SELECT id FROM x",
    "BEGIN TRY INSERT INTO {t} VALUES (2) END TRY BEGIN CATCH END CATCH",
]


@pytest.mark.parametrize("write", _BYPASS_WRITES)
def test_aborted_transaction_rejects_every_kind_of_write_until_end_sync(conn, cursor, table_name, write):
    _aborted_table(cursor, f"dbo.{table_name}")
    sql = write.format(t=f"dbo.{table_name}")
    # Not consumed by the first rejection: the second one fails too.
    for _ in range(2):
        with pytest.raises(mssql_python.DatabaseError, match="rolled back"):
            cursor.execute(sql)
    with pytest.raises(mssql_python.DatabaseError):
        cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?)", [(3,)])
    # Reads still run.
    assert cursor.execute("SELECT 41 + 1").fetchone()[0] == 42
    conn.commit()
    assert cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0] == 0
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (5)")
    conn.commit()
    assert cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0] == 1


@needs_aio
@pytest.mark.asyncio
@pytest.mark.parametrize("write", _BYPASS_WRITES)
async def test_aborted_transaction_rejects_every_kind_of_write_until_end_async(conn_str, write):
    name = "r5_abort_" + __import__("uuid").uuid4().hex[:10]
    ac = await aio.connect(conn_str, autocommit=False)
    try:
        cur = ac.cursor()
        await cur.execute(f"CREATE TABLE dbo.{name} (id INT PRIMARY KEY)")
        await ac.commit()
        await cur.execute("SET XACT_ABORT ON")
        await cur.execute(f"INSERT INTO dbo.{name} VALUES (1)")
        with pytest.raises(mssql_python.IntegrityError):
            await cur.execute(f"INSERT INTO dbo.{name} VALUES (1)")
        sql = write.format(t=f"dbo.{name}")
        for _ in range(2):
            with pytest.raises(mssql_python.DatabaseError, match="rolled back"):
                await cur.execute(sql)
        with pytest.raises(mssql_python.DatabaseError):
            await cur.executemany(f"INSERT INTO dbo.{name} VALUES (?)", [(3,)])
        await cur.execute("SELECT 41 + 1")
        assert (await cur.fetchone())[0] == 42
        await ac.commit()
        await cur.execute(f"SELECT COUNT(*) FROM dbo.{name}")
        assert (await cur.fetchone())[0] == 0
        await cur.execute(f"INSERT INTO dbo.{name} VALUES (5)")
        await ac.commit()
        await cur.execute(f"SELECT COUNT(*) FROM dbo.{name}")
        assert (await cur.fetchone())[0] == 1
    finally:
        await ac.rollback()
        await (ac.cursor()).execute(f"DROP TABLE IF EXISTS dbo.{name}")
        await ac.commit()
        await ac.close()


@needs_aio
@pytest.mark.asyncio
async def test_without_xact_abort_a_failed_statement_does_not_block_writes_async(conn_str):
    ac = await aio.connect(conn_str, autocommit=False)
    try:
        cur = ac.cursor()
        await cur.execute("CREATE TABLE #r5_na (id INT PRIMARY KEY)")
        await cur.execute("INSERT INTO #r5_na VALUES (1)")
        with pytest.raises(mssql_python.IntegrityError):
            await cur.execute("INSERT INTO #r5_na VALUES (1)")
        await cur.execute("INSERT INTO #r5_na VALUES (2)")
        await ac.commit()
    finally:
        await ac.close()


# ---- F13: cancellation reaches the implicit BEGIN of manual-commit mode


@needs_aio
@pytest.mark.asyncio
async def test_cancel_before_begin_runs_no_user_sql(conn_str):
    import asyncio

    ac = await aio.connect(conn_str, autocommit=False)
    try:
        cur = ac.cursor()
        task = asyncio.ensure_future(cur.execute("CREATE TABLE r5_cancel_probe (id INT)"))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The session is free again and nothing ran.
        await cur.execute("SELECT OBJECT_ID('r5_cancel_probe')")
        assert (await cur.fetchone())[0] is None
        await ac.rollback()
    finally:
        await ac.close()
