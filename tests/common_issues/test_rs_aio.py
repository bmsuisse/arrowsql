"""Native async API (``mssql_python.aio``) on the Rust core.

Port-only: upstream mssql-python has no ``aio`` module (its async preview is
``mssql_python.async_query``), so the whole file skips there.
"""

import asyncio
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
