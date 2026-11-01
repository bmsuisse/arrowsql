"""G4 async micro-benchmarks: python g4_async_micro.py <a|b|c|d|all> -> JSON of seconds."""
import asyncio, json, os, sys, time, datetime, decimal, types

N_A = 100_000


def cs():
    return os.environ["DB_CONNECTION_STRING"]


async def bench_a():
    from arrowsql import aio
    c = await aio.connect(cs(), autocommit=True)
    out = {}
    cur = c.cursor()
    sql = f"SELECT TOP {N_A} a.object_id AS x, 1 AS y FROM sys.all_columns a CROSS JOIN sys.all_columns b"
    await cur.execute(sql)
    t = time.perf_counter(); k = 0
    async for _ in cur:
        k += 1
    out["a_async_for"] = time.perf_counter() - t
    assert k == N_A, k
    await cur.execute(sql)
    t = time.perf_counter()
    while (await cur.fetchone()) is not None:
        pass
    out["a_fetchone"] = time.perf_counter() - t
    await cur.execute(sql)
    t = time.perf_counter()
    rows = await cur.fetchall()
    out["a_fetchall"] = time.perf_counter() - t
    assert len(rows) == N_A
    await c.close()
    return out


async def bench_b(n=20_000):
    from arrowsql import aio
    p = await aio.create_pool(cs(), min_size=1, max_size=1, autocommit=True)
    await p.fetch("SELECT 1 AS a, 2 AS b")
    t = time.perf_counter()
    for _ in range(n):
        await p.fetch("SELECT 1 AS a, 2 AS b")
    r = {"b_pool_fetch": time.perf_counter() - t}
    await p.close()
    return r


async def bench_c(n=100_000):
    from arrowsql import aio
    c = await aio.connect(cs(), autocommit=True)
    cur = c.cursor()
    await cur.execute("CREATE TABLE #g4c (i int, d datetime2, n decimal(18,4), s int NULL)")
    d = datetime.datetime(2024, 1, 2, 3, 4, 5)
    dec = decimal.Decimal("123.4567")
    data = [(i, d, dec, None if i % 3 == 0 else i) for i in range(n)]
    t = time.perf_counter()
    await cur.executemany("INSERT INTO #g4c VALUES (?,?,?,?)", data)
    r = {"c_executemany": time.perf_counter() - t}
    await c.close()
    return r


async def bench_d(n=50_000):
    """async_query wrapper fetchone loop over a fake py-core cursor (no mssql_py_core needed)."""
    from arrowsql.async_query import async_fetch

    class Evt:
        def is_set(self): return True
        async def wait(self): return None

    class Core:
        async def fetchone(self):
            return (1, "x", 3)

    class Cur:
        rowcount = 0
        arraysize = 1
        _result_generation = 1
        _column_map = {"a": 0, "b": 1, "c": 2}
        _uuid_str_indices = None
        _column_map_lower = {"a": 0, "b": 1, "c": 2}
        _column_names = ("a", "b", "c")
        _result_ready = Evt()
        async def _wait_for_result_publication(self): await self._result_ready.wait()
        def _record_fetch(self, *a): pass

    core = Core()
    async_fetch._get_py_core_async_cursor = lambda cur: core
    cur = Cur()
    t = time.perf_counter()
    for _ in range(n):
        await async_fetch.fetchone(cur)
    return {"d_wrapper_fetchone": time.perf_counter() - t}


async def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    res = {}
    for k, f in dict(a=bench_a, b=bench_b, c=bench_c, d=bench_d).items():
        if which in (k, "all"):
            res.update(await f())
    print(json.dumps(res))

asyncio.run(main())
