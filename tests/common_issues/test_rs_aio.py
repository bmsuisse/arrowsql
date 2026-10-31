"""Native async API (``mssql_python.aio``) on the Rust core.

Port-only: upstream mssql-python has no ``aio`` module (its async preview is
``mssql_python.async_query``), so the whole file skips there.
"""

import asyncio
import sys
import datetime
import decimal
import threading
import time
import uuid

import pytest

aio = pytest.importorskip("mssql_python.aio")
pytest_asyncio = pytest.importorskip("pytest_asyncio")

import mssql_python  # noqa: E402

@pytest_asyncio.fixture
async def conn(conn_str):
    connection = await aio.connect(conn_str, autocommit=True)
    yield connection
    await connection.close()


@pytest_asyncio.fixture
async def pool(conn_str):
    p = await aio.create_pool(conn_str, min_size=2, max_size=16)
    yield p
    await p.close()


@pytest.mark.asyncio
async def test_select_rows_and_description(conn):
    cur = conn.cursor()
    await cur.execute("SELECT 1 AS a, N'x' AS b UNION ALL SELECT 2, N'y' ORDER BY a")
    assert [d[0] for d in cur.description] == ["a", "b"]
    assert cur.description[0][1] is int and cur.description[1][1] is str
    rows = await cur.fetchall()
    assert [tuple(r) for r in rows] == [(1, "x"), (2, "y")]
    assert rows[1].b == "y" and rows[1]["a"] == 2
    assert await cur.fetchone() is None


@pytest.mark.asyncio
async def test_crud_rowcount_and_types(conn):
    cur = conn.cursor()
    await cur.execute(
        "CREATE TABLE #t (id int PRIMARY KEY, s nvarchar(50), d decimal(10,2), "
        "dt datetime2, g uniqueidentifier, b varbinary(10), f float, bt bit, n int NULL)"
    )
    g = uuid.uuid4()
    values = (1, "héllo 😀", decimal.Decimal("12.34"), datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
              g, b"\x00\x01", 1.5, True, None)
    await cur.execute("INSERT INTO #t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
    assert cur.rowcount == 1
    await cur.execute("UPDATE #t SET s = ? WHERE id = ?", "x", 1)
    assert cur.rowcount == 1
    row = await conn.fetchrow("SELECT id, s, d, dt, g, b, f, bt, n FROM #t")
    assert tuple(row) == (1, "x", decimal.Decimal("12.34"), values[3], g, b"\x00\x01", 1.5, True, None)
    await cur.execute("DELETE FROM #t")
    assert cur.rowcount == 1


@pytest.mark.asyncio
async def test_params_tuple_or_varargs(conn):
    assert await conn.fetchval("SELECT ? + ?", 2, 3) == 5
    assert await conn.fetchval("SELECT ? + ?", (2, 3)) == 5


@pytest.mark.asyncio
async def test_none_into_varbinary(conn):
    # sp_describe_undeclared_parameters (how untyped NULLs get their type)
    # cannot see #temp tables, so this needs a real table.
    name = f"aio_nul_{uuid.uuid4().hex[:8]}"
    await conn.execute(f"CREATE TABLE dbo.{name} (v varbinary(max) NULL)")
    try:
        await conn.execute(f"INSERT INTO dbo.{name} VALUES (?)", None)
        assert await conn.fetchval(f"SELECT COUNT(*) FROM dbo.{name} WHERE v IS NULL") == 1
    finally:
        await conn.execute(f"DROP TABLE dbo.{name}")


@pytest.mark.asyncio
async def test_fetchmany_and_async_iteration(conn):
    cur = conn.cursor()
    await cur.execute("SELECT TOP 2000 ROW_NUMBER() OVER (ORDER BY (SELECT 1)) n FROM sys.all_objects a, sys.all_objects b")
    first = await cur.fetchmany(10)
    assert [r[0] for r in first] == list(range(1, 11))
    one = await cur.fetchone()
    assert one[0] == 11
    rest = [r[0] async for r in cur]
    assert rest == list(range(12, 2001))


@pytest.mark.asyncio
async def test_large_result_fetchall(conn):
    cur = conn.cursor()
    await cur.execute("SELECT TOP 50000 a.object_id, N'name' + CAST(a.object_id AS nvarchar(20)) FROM sys.all_objects a, sys.all_objects b")
    rows = await cur.fetchall()
    assert len(rows) == 50000


@pytest.mark.asyncio
async def test_errors_carry_sqlstate(conn):
    with pytest.raises(mssql_python.ProgrammingError) as exc:
        await conn.execute("SELECT * FROM no_such_table_xyz")
    assert exc.value.sqlstate == "42S02" and exc.value.native_error == 208
    cur = conn.cursor()
    await cur.execute("CREATE TABLE #pk (id int PRIMARY KEY)")
    await cur.execute("INSERT INTO #pk VALUES (1)")
    with pytest.raises(mssql_python.IntegrityError) as exc:
        await cur.execute("INSERT INTO #pk VALUES (?)", 1)
    assert exc.value.sqlstate == "23000" and exc.value.native_error == 2627
    # The connection is still usable after errors.
    assert await conn.fetchval("SELECT 7") == 7


@pytest.mark.asyncio
async def test_param_count_mismatch_raises_immediately(conn):
    with pytest.raises(mssql_python.Error) as exc:
        await conn.fetchval("SELECT ?, ?", 1)
    assert exc.value.sqlstate == "07002"


@pytest.mark.asyncio
async def test_cancel_stops_query_on_server(conn):
    task = asyncio.ensure_future(conn.fetchval("WAITFOR DELAY '00:00:20'; SELECT 1"))
    await asyncio.sleep(0.3)
    start = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Next query on the same connection completes quickly: the server-side
    # WAITFOR was cancelled, not left running.
    assert await asyncio.wait_for(conn.fetchval("SELECT 42"), 5) == 42
    assert time.perf_counter() - start < 5


@pytest.mark.asyncio
async def test_wait_for_timeout_cancels(conn):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(conn.fetchval("WAITFOR DELAY '00:00:20'; SELECT 1"), 0.5)
    assert await asyncio.wait_for(conn.fetchval("SELECT 1"), 5) == 1


@pytest.mark.asyncio
async def test_query_timeout(conn_str):
    conn = await aio.connect(conn_str, autocommit=True, timeout=1)
    try:
        start = time.perf_counter()
        with pytest.raises(mssql_python.Error) as exc:
            await conn.fetchval("WAITFOR DELAY '00:00:10'; SELECT 1")
        assert time.perf_counter() - start < 6
        assert exc.value.sqlstate == "HYT00"
        assert await conn.fetchval("SELECT 3") == 3
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_transactions_commit_and_rollback(conn_str):
    name = f"aio_tx_{uuid.uuid4().hex[:8]}"
    setup = await aio.connect(conn_str, autocommit=True)
    await setup.execute(f"CREATE TABLE dbo.{name} (id int)")
    try:
        conn = await aio.connect(conn_str)  # autocommit=False
        assert conn.autocommit is False
        cur = conn.cursor()
        await cur.execute(f"INSERT INTO dbo.{name} VALUES (1)")
        await conn.rollback()
        assert await setup.fetchval(f"SELECT COUNT(*) FROM dbo.{name}") == 0
        await cur.execute(f"INSERT INTO dbo.{name} VALUES (2)")
        await conn.commit()
        assert await setup.fetchval(f"SELECT COUNT(*) FROM dbo.{name}") == 1
        async with conn:
            await conn.execute(f"INSERT INTO dbo.{name} VALUES (3)")
        assert await setup.fetchval(f"SELECT COUNT(*) FROM dbo.{name}") == 2
    finally:
        await setup.execute(f"DROP TABLE dbo.{name}")
        await setup.close()


@pytest.mark.asyncio
async def test_multiple_cursors_interleave(conn):
    a, b = conn.cursor(), conn.cursor()
    await a.execute("SELECT TOP 3000 1 FROM sys.all_objects x, sys.all_objects y")
    first = await a.fetchmany(5)
    await b.execute("SELECT 2")
    assert (await b.fetchone())[0] == 2
    rest = await a.fetchall()
    assert len(first) + len(rest) == 3000


@pytest.mark.asyncio
async def test_nextset(conn):
    cur = conn.cursor()
    await cur.execute("SELECT 1; SELECT 2, 3")
    assert (await cur.fetchall())[0][0] == 1
    assert await cur.nextset() is True
    assert tuple((await cur.fetchall())[0]) == (2, 3)
    assert await cur.nextset() is None


@pytest.mark.asyncio
async def test_executemany(conn):
    cur = conn.cursor()
    await cur.execute("CREATE TABLE #m (a int, b nvarchar(20), c decimal(10,2) NULL)")
    rows = [(i, f"r{i}", decimal.Decimal(i) / 4 if i % 3 else None) for i in range(5000)]
    await cur.executemany("INSERT INTO #m VALUES (?, ?, ?)", rows)
    assert cur.rowcount == 5000
    assert await conn.fetchval("SELECT SUM(a) FROM #m") == sum(range(5000))


@pytest.mark.asyncio
async def test_fetchall_arrow_with_duckdb(conn):
    duckdb = pytest.importorskip("duckdb")
    cur = conn.cursor()
    await cur.execute("SELECT TOP 10000 a.object_id AS id FROM sys.all_objects a, sys.all_objects b")
    head = await cur.fetchone()  # a partly consumed read-ahead batch still converts
    t = await cur.fetchall_arrow()
    assert t.num_rows == 9999
    assert duckdb.sql("SELECT count(*) FROM t").fetchall() == [(9999,)]
    assert head is not None


@pytest.mark.asyncio
async def test_tuple_rows(conn_str):
    conn = await aio.connect(conn_str, autocommit=True, tuple_rows=True)
    try:
        assert await conn.fetchrow("SELECT 1, 2") == (1, 2)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pool_thousand_tasks(pool):
    async def one(i):
        return await pool.fetchval("SELECT ?", i)

    results = await asyncio.gather(*(one(i) for i in range(1000)))
    assert results == list(range(1000))
    assert pool.in_use == 0
    assert 1 <= pool.idle <= 16


@pytest.mark.asyncio
async def test_pool_backpressure(conn_str):
    p = await aio.create_pool(conn_str, min_size=1, max_size=2)
    try:
        start = time.perf_counter()
        await asyncio.gather(*(p.fetchval("WAITFOR DELAY '00:00:00.5'; SELECT 1") for _ in range(4)))
        # 4 half-second queries through 2 sessions take ~1 s, not 0.5 s.
        assert time.perf_counter() - start >= 0.9
        assert p.in_use == 0
    finally:
        await p.close()


@pytest.mark.asyncio
async def test_pool_acquire_and_session_reset(pool):
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE #leak (x int)")
        await conn.execute("SET LOCK_TIMEOUT 1234")
    for _ in range(pool.max_size):
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT @@LOCK_TIMEOUT") == -1
            assert await conn.fetchval("SELECT OBJECT_ID('tempdb..#leak')") is None


@pytest.mark.asyncio
async def test_pool_execute_rowcount(pool):
    assert await pool.execute("DECLARE @x int = 1") == -1
    rows = await pool.execute("SELECT 1")
    assert [tuple(r) for r in rows] == [(1,)]


@pytest.mark.asyncio
async def test_closed_connection_raises(conn_str):
    conn = await aio.connect(conn_str, autocommit=True)
    await conn.close()
    with pytest.raises(mssql_python.InterfaceError):
        conn.cursor()


def test_loop_closed_with_queries_in_flight(conn_str):
    loop = asyncio.new_event_loop()

    async def start():
        conn = await aio.connect(conn_str, autocommit=True)
        asyncio.ensure_future(conn.fetchval("WAITFOR DELAY '00:00:01'; SELECT 1"))
        await asyncio.sleep(0.05)

    loop.run_until_complete(start())
    loop.close()
    time.sleep(1.5)  # the task completes into a hub nobody drains: no crash


def test_event_loops_in_threads(conn_str):
    results = []

    def worker(i):
        async def main():
            conn = await aio.connect(conn_str, autocommit=True)
            try:
                return await conn.fetchval("SELECT ?", i)
            finally:
                await conn.close()

        results.append(asyncio.run(main()))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_concurrency_is_real(pool):
    start = time.perf_counter()
    await asyncio.gather(*(pool.fetchval("WAITFOR DELAY '00:00:01'; SELECT 1") for _ in range(8)))
    assert time.perf_counter() - start < 2.5


# --- review round 2 (r2_aio) -------------------------------------------------

_NUMBERS = (
    "(SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
    "FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c) t"
)


@pytest.mark.asyncio
async def test_pool_session_gets_pool_autocommit_back(conn_str):
    """A borrower that switched autocommit off must not leak that to the next user."""
    p = await aio.create_pool(conn_str, min_size=1, max_size=1, autocommit=True)
    try:
        await p.execute("IF OBJECT_ID('dbo.r2_aio_a1') IS NOT NULL DROP TABLE dbo.r2_aio_a1")
        await p.execute("CREATE TABLE dbo.r2_aio_a1 (i int)")
        async with p.acquire() as c:
            await c.set_autocommit(False)
            await c.fetchval("SELECT 1")
        async with p.acquire() as c:
            assert c.autocommit is True
        async with p.acquire() as c:
            await c.set_autocommit(False)
            await c.fetchval("SELECT 1")
        await p.execute("INSERT INTO dbo.r2_aio_a1 VALUES (1)")
        assert await p.fetchval("SELECT COUNT(*) FROM dbo.r2_aio_a1") == 1
    finally:
        await p.execute("DROP TABLE IF EXISTS dbo.r2_aio_a1")
        await p.close()


@pytest.mark.asyncio
async def test_partial_rows_then_error_raises(conn, pool):
    # error after the rows were prefetched by execute
    sql = f"SELECT 1/(n-5) AS x FROM {_NUMBERS.format(n=20)}"
    cur = conn.cursor()
    try:
        await cur.execute(sql)
    except mssql_python.Error:
        pass  # the server may fail before the first row; also fine
    else:
        with pytest.raises(mssql_python.Error):
            await cur.fetchall()
    with pytest.raises(mssql_python.Error):
        await conn.fetch(sql)
    with pytest.raises(mssql_python.Error):
        await pool.fetch(sql)


@pytest.mark.asyncio
async def test_error_after_prefetch_raises_from_fetch(conn):
    sql = f"SELECT 1 / (CAST(n AS int) - 5000) AS x FROM {_NUMBERS.format(n=20000)}"
    cur = conn.cursor()
    await cur.execute(sql)
    with pytest.raises(mssql_python.Error):
        await cur.fetchall()
    cur = conn.cursor()
    await cur.execute(sql)
    with pytest.raises(mssql_python.Error):
        while await cur.fetchone() is not None:
            pass
    cur = conn.cursor()
    await cur.execute(sql)
    with pytest.raises(mssql_python.Error):
        while await cur.fetchmany(1000):
            pass


@pytest.mark.asyncio
async def test_close_discards_unread_results_fast(conn_str):
    c = await aio.connect(conn_str, autocommit=True)
    cur = c.cursor()
    await cur.execute(f"SELECT n, REPLICATE('x', 100) AS s FROM {_NUMBERS.format(n=20000000)}")
    start = time.perf_counter()
    await c.close()
    assert time.perf_counter() - start < 5


@pytest.mark.asyncio
async def test_pool_checkin_discards_unread_results_fast(conn_str):
    p = await aio.create_pool(conn_str, min_size=1, max_size=1)
    try:
        start = time.perf_counter()
        assert await p.fetchrow(f"SELECT n FROM {_NUMBERS.format(n=20000000)}") is not None
        assert time.perf_counter() - start < 5
        assert await p.fetchval("SELECT 7") == 7
    finally:
        await p.close()


@pytest.mark.asyncio
async def test_second_cursor_handover_is_cancellable(conn):
    first = conn.cursor()
    await first.execute(f"SELECT n, REPLICATE('x', 100) AS s FROM {_NUMBERS.format(n=20000000)}")
    second = conn.cursor()
    task = asyncio.ensure_future(second.execute("SELECT 1"))
    await asyncio.sleep(0.2)
    start = time.perf_counter()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await asyncio.wait_for(conn.fetchval("SELECT 5"), 10) == 5
    assert time.perf_counter() - start < 10


@pytest.mark.asyncio
async def test_pool_cancel_running_query(conn_str):
    p = await aio.create_pool(conn_str, min_size=1, max_size=1)
    try:
        start = time.perf_counter()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(p.fetch("WAITFOR DELAY '00:00:20'; SELECT 1"), 0.3)
        assert await asyncio.wait_for(p.fetchval("SELECT 3"), 10) == 3
        assert time.perf_counter() - start < 10
        assert p.in_use == 0
    finally:
        await p.close()


@pytest.mark.asyncio
async def test_pool_cancel_queued_acquire(conn_str):
    p = await aio.create_pool(conn_str, min_size=1, max_size=1)
    try:
        held = await p.acquire()
        waiter = asyncio.ensure_future(p.acquire())
        await asyncio.sleep(0.2)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        await held.close()
        c = await asyncio.wait_for(p.acquire(), 5)
        await c.close()
        await asyncio.sleep(0.2)
        assert p.in_use == 0
    finally:
        await p.close()


async def _open_cursors(conn):
    for _ in range(50):
        n = conn._c.open_cursors
        if n == 0:
            break
        await asyncio.sleep(0.1)
    return conn._c.open_cursors


@pytest.mark.asyncio
async def test_connection_execute_cursors_are_retired(conn):
    import gc

    for i in range(30):
        await conn.execute("SELECT ?", i)
    gc.collect()
    assert await _open_cursors(conn) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="aio is Unix-only")
def test_closed_event_loop_is_collected():
    import gc
    import weakref

    loop = asyncio.new_event_loop()

    async def touch():
        aio._hub()

    loop.run_until_complete(touch())
    ref = weakref.ref(loop)
    loop.close()
    del loop
    gc.collect()
    assert ref() is None
