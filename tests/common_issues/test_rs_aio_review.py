"""aio review fixes (r4 sweep): cancellation, error propagation, pool races,
stale cursor state.

Port-only: upstream mssql-python has no ``aio`` module.
"""

import asyncio
import os
import subprocess
import sys
import textwrap
import time
import uuid

import pytest

aio = pytest.importorskip("mssql_python.aio")
pytest_asyncio = pytest.importorskip("pytest_asyncio")

from mssql_python import ddbc_bindings as _ddbc  # noqa: E402

pytestmark = pytest.mark.skipif(not hasattr(_ddbc, "AioHub"), reason="aio is Unix-only")

import mssql_python  # noqa: E402


@pytest_asyncio.fixture
async def conn(conn_str):
    connection = await aio.connect(conn_str, autocommit=True)
    yield connection
    await connection.close()


# ---- B2: fetchall_arrow must not hide an error that followed the buffered rows


_ERROR_AFTER_ROWS = "SELECT CAST(1 / (5 - n) AS int) AS v FROM (VALUES (1), (2), (3), (4), (5)) AS t(n)"


@pytest.mark.asyncio
async def test_fetchall_arrow_raises_error_after_prefetched_rows(conn):
    cur = conn.cursor()
    with pytest.raises(mssql_python.Error):
        await cur.execute(_ERROR_AFTER_ROWS)
        await cur.fetchall_arrow()


# ---- S1: cancelling a read that blocks on the wire interrupts it


_SLOW_SCAN = (
    "SELECT COUNT_BIG(*) FROM sys.all_columns a CROSS JOIN sys.all_columns b "
    "CROSS JOIN sys.all_columns c WHERE a.column_id + b.column_id + c.column_id > 0"
)


@pytest.mark.asyncio
async def test_cancel_nextset_blocked_on_the_wire(conn):
    cur = conn.cursor()
    # Execute prefetches 512 rows of the first result; nextset skips the rest
    # and then waits for the second result, which takes minutes to compute.
    await cur.execute(
        "SELECT TOP 1000 REPLICATE(CAST(N'x' AS nvarchar(max)), 3000) AS pad FROM sys.all_columns; " + _SLOW_SCAN
    )
    start = time.monotonic()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(cur.nextset(), 1.5)
    assert await conn.fetchval("SELECT 7") == 7  # the session is usable again, and promptly
    assert time.monotonic() - start < 15


_SLOW_ROW = (
    "SELECT n, REPLICATE(CAST(N'x' AS nvarchar(max)), 3000) AS pad, "
    "IIF(n = 550, (SELECT COUNT_BIG(*) FROM sys.all_columns a CROSS JOIN sys.all_columns b "
    "CROSS JOIN sys.all_columns c WHERE a.column_id + b.column_id + c.column_id > n), 0) AS slow "
    "FROM (SELECT TOP 700 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
    "FROM sys.all_columns a CROSS JOIN sys.all_columns b) AS t"
)


@pytest.mark.asyncio
async def test_cancel_fetchmany_blocked_on_the_wire(conn):
    cur = conn.cursor()
    await cur.execute(_SLOW_ROW)
    start = time.monotonic()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(cur.fetchmany(600), 2)
    assert await conn.fetchval("SELECT 7") == 7
    assert time.monotonic() - start < 15


# ---- S2: a cancelled commit / rollback / set_autocommit must not run


@pytest.mark.asyncio
@pytest.mark.parametrize("op", ["commit", "rollback", "set_autocommit"])
async def test_cancelled_queued_transaction_op_does_not_run(conn_str, op):
    name = f"ci_cancel_{uuid.uuid4().hex[:8]}"
    admin = await aio.connect(conn_str, autocommit=True)
    await admin.execute(f"CREATE TABLE dbo.{name} (id INT)")
    conn = await aio.connect(conn_str, autocommit=False)
    try:
        cur = conn.cursor()
        busy = asyncio.ensure_future(cur.execute(f"INSERT INTO dbo.{name} VALUES (1); WAITFOR DELAY '00:00:02'"))
        await asyncio.sleep(0.3)
        call = {
            "commit": conn.commit,
            "rollback": conn.rollback,
            "set_autocommit": lambda: conn.set_autocommit(True),
        }[op]()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(call, 0.3)
        await busy
        assert conn.autocommit is False
        # Had the cancelled operation run, it would have ended the transaction.
        if op == "rollback":
            await conn.commit()
            expected = 1
        else:
            await conn.rollback()
            expected = 0
        assert await admin.fetchval(f"SELECT COUNT(*) FROM dbo.{name}") == expected
    finally:
        await conn.close()
        await admin.execute(f"DROP TABLE IF EXISTS dbo.{name}")
        await admin.close()


# ---- S5: draining another cursor's result is bounded


@pytest.mark.asyncio
async def test_drain_for_another_cursor_is_bounded(conn):
    big = conn.cursor()
    other = conn.cursor()
    await big.execute(
        "SELECT TOP 100 REPLICATE(CAST('x' AS varchar(max)), 1000000) AS v FROM sys.all_columns"
    )
    assert await big.fetchone() is not None
    await other.execute("SELECT 5 AS v")  # needs the wire: the rest of `big` is cut off
    assert (await other.fetchone())[0] == 5
    with pytest.raises(mssql_python.Error) as err:
        await big.fetchall()
    assert "discarded" in str(err.value)
    assert await conn.fetchval("SELECT 9") == 9


# ---- S6: Python code run during row conversion cannot deadlock the batch


_GC_SCRIPT = textwrap.dedent(
    """
    import asyncio, gc, os
    from mssql_python import aio

    cursor = None

    class Garbage:
        def __init__(self):
            self.me = self
        def __del__(self):
            if cursor is not None and cursor._batch is not None:
                cursor._batch.remaining()

    async def main():
        global cursor
        conn = await aio.connect(os.environ["CS"], autocommit=True)
        cursor = conn.cursor()
        await cursor.execute("SELECT n, N'a' + CAST(n AS nvarchar(10)) FROM (VALUES (1),(2),(3),(4)) t(n)")
        gc.disable()
        [Garbage() for _ in range(50)]
        gc.set_threshold(1, 1, 1)
        gc.enable()
        rows = [await cursor.fetchone() for _ in range(2)]
        rest = await cursor.fetchmany(10)
        print(len(rows) + len(rest))
        await conn.close()

    asyncio.run(main())
    """
)


def test_gc_finalizer_touching_the_batch_during_conversion(conn_str):
    out = subprocess.run(
        [sys.executable, "-I", "-c", _GC_SCRIPT], capture_output=True, text=True, timeout=60,
        env={**os.environ, "CS": conn_str},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.strip() == "4"


# ---- S7: results are retired at every boundary


@pytest.mark.asyncio
async def test_failed_execute_drops_the_previous_result(conn):
    cur = conn.cursor()
    await cur.execute("SELECT 1 AS a UNION ALL SELECT 2")
    with pytest.raises(mssql_python.Error):
        await cur.execute("SELECT 1/0")
    assert cur.description is None
    assert await cur.fetchone() is None
    assert await cur.fetchmany(5) == []
    assert await cur.fetchall() == []


@pytest.mark.asyncio
async def test_executemany_drops_the_previous_result(conn):
    await conn.execute("CREATE TABLE #t_em (id INT)")
    cur = conn.cursor()
    await cur.execute("SELECT 1 AS a UNION ALL SELECT 2")
    await cur.fetchone()
    await cur.executemany("INSERT INTO #t_em VALUES (?)", [(1,), (2,)])
    assert await cur.fetchone() is None
    assert await cur.fetchall() == []


@pytest.mark.asyncio
async def test_final_nextset_drops_buffered_rows(conn):
    cur = conn.cursor()
    await cur.execute("SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3")
    await cur.fetchone()
    await cur.fetchone()
    assert await cur.nextset() is None
    assert await cur.fetchone() is None
    assert await cur.fetchall() == []


@pytest.mark.asyncio
async def test_fetch_after_cursor_close_raises(conn):
    cur = conn.cursor()
    await cur.execute("SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3")
    await cur.fetchone()
    await cur.close()
    with pytest.raises(mssql_python.InterfaceError):
        await cur.fetchone()
    with pytest.raises(mssql_python.InterfaceError):
        await cur.fetchmany(2)
    with pytest.raises(mssql_python.InterfaceError):
        await cur.fetchall()
    with pytest.raises(mssql_python.InterfaceError):
        async for _ in cur:
            pass


@pytest.mark.asyncio
async def test_fetch_after_connection_close_raises(conn_str):
    conn = await aio.connect(conn_str, autocommit=True)
    cur = conn.cursor()
    await cur.execute("SELECT 1 AS a UNION ALL SELECT 2")
    await conn.close()
    with pytest.raises(mssql_python.InterfaceError):
        await cur.fetchone()


# ---- S8: a session returned while the pool closes must not leak


@pytest.mark.asyncio
async def test_checkin_racing_pool_close_does_not_leak(conn_str):
    for _ in range(5):
        pool = await aio.create_pool(conn_str, min_size=1, max_size=2, autocommit=False)
        conn = await pool.acquire()
        await conn.execute("SELECT 1")  # opens a transaction: checkin has a rollback to await
        await asyncio.gather(conn.close(), pool.close())
        assert pool.idle == 0


# ---- pool reset: the next borrower sees default session state


@pytest.mark.asyncio
async def test_pool_reset_restores_isolation_level(conn_str):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=1, reset=True)
    try:
        query = "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
        async with pool.acquire() as first:
            spid = await first.fetchval("SELECT @@SPID")
            default = await first.fetchval(query)
            await first.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            assert await first.fetchval(query) != default
        async with pool.acquire() as second:
            assert await second.fetchval("SELECT @@SPID") == spid
            assert await second.fetchval(query) == default
    finally:
        await pool.close()


# ---- r5 F6: a failed nextset() must not leave the previous result fetchable


@pytest.mark.asyncio
async def test_failed_nextset_retires_the_previous_result(conn):
    cur = conn.cursor()
    await cur.execute("SELECT 1; RAISERROR('boom', 16, 1); SELECT 2")
    with pytest.raises(mssql_python.Error):
        await cur.nextset()
    assert await cur.fetchone() is None
    assert cur.description is None


# ---- r5 F8: a re-entrant next() on one batch must not return the same row twice

_REENTRANT_SCRIPT = textwrap.dedent(
    """
    import asyncio, datetime, os

    seen = []
    state = {"cursor": None, "armed": False}

    class Date(datetime.date):
        # A substituted date class runs Python code while the batch converts a row.
        def __new__(cls, *args):
            if state["armed"]:
                state["armed"] = False
                row = state["cursor"]._batch.next()
                if row is not None:
                    seen.append(row[0])
            return super().__new__(cls, *args)

    datetime.date = Date
    from mssql_python import aio

    async def main():
        conn = await aio.connect(os.environ["CS"], autocommit=True)
        cur = conn.cursor()
        await cur.execute("SELECT n, CAST('2020-01-01' AS date) FROM (VALUES (1),(2),(3),(4)) t(n)")
        state["cursor"] = cur
        state["armed"] = True
        seen.append((await cur.fetchone())[0])
        seen.extend(r[0] for r in await cur.fetchall())
        print(sorted(seen))
        await conn.close()

    asyncio.run(main())
    """
)


def test_reentrant_batch_consumption_never_repeats_rows(conn_str):
    out = subprocess.run(
        [sys.executable, "-I", "-c", _REENTRANT_SCRIPT], capture_output=True, text=True, timeout=60,
        env={**os.environ, "CS": conn_str},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.strip() == "[1, 2, 3, 4]"


# ---- r5 F11: acquire() pending on login must not hand out a session after close()


@pytest.mark.skipif(sys.platform == "win32", reason="aio is Unix-only")
@pytest.mark.asyncio
async def test_acquire_pending_on_login_raises_after_pool_close(conn_str):
    import re

    m = re.search(r"(?i)server\s*=\s*(?:tcp:)?([^,;]+)(?:,(\d+))?", str(conn_str))
    host, port = m.group(1), int(m.group(2) or 1433)
    gate = asyncio.Event()
    tasks = []

    async def pipe(r, w):
        try:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
        except ConnectionError:
            pass
        finally:
            w.close()

    async def handle(r, w):
        await gate.wait()  # the login stalls until the pool is closed
        ur, uw = await asyncio.open_connection(host, port)
        tasks.append(asyncio.ensure_future(pipe(r, uw)))
        tasks.append(asyncio.ensure_future(pipe(ur, w)))

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    proxy_port = server.sockets[0].getsockname()[1]
    proxied = re.sub(r"(?i)server\s*=\s*[^;]+", f"Server=tcp:127.0.0.1,{proxy_port}", str(conn_str), count=1)
    try:
        pool = await aio.create_pool(proxied, min_size=0, max_size=1)
        waiter = asyncio.ensure_future(pool.acquire().__aenter__())
        await asyncio.sleep(0.5)
        await pool.close()
        gate.set()
        with pytest.raises(mssql_python.Error):
            conn = await asyncio.wait_for(waiter, 30)
            await conn.close()
    finally:
        server.close()
        for t in tasks:
            t.cancel()
