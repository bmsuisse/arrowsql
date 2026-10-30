"""Concurrency benchmark: N concurrent point lookups, several async strategies.

    python benchmarks/bench_async.py --mode to_thread|native_preview|fastmssql \
        [--concurrency 1 16 64] [--queries 2000]

Modes:
    to_thread       asyncio.to_thread over the sync mssql_python driver,
                    one pooled connection per worker
    native_preview  upstream's private async API (mssql_python.async_query,
                    backed by Microsoft's mssql_py_core), one connection per worker
    fastmssql       FastMssql (tiberius + tokio), shared bb8 pool
    port_aio        this project's mssql_python.aio, one connection per worker
                    (cursor.execute + fetchone)
    port_aio_pool   this project's aio pool, pool.fetchrow() per query

Which mssql_python is measured depends on the interpreter (.venv = port,
.venv-upstream = upstream). Prints one JSON line per concurrency level.
"""

import argparse
import asyncio
import json
import os
import re
import time

QUERY = "SELECT trip_id, total_amount FROM dbo.yellow_trips WHERE trip_id = ?"


def conn_str() -> str:
    return re.sub(r"Database=[^;]*", "Database=nyctaxi", os.environ["DB_CONNECTION_STRING"])


async def run_to_thread(concurrency: int, queries: int) -> None:
    import threading

    import mssql_python

    local = threading.local()

    def lookup(trip_id: int):
        if not hasattr(local, "cur"):
            local.cur = mssql_python.connect(conn_str(), autocommit=True).cursor()
        local.cur.execute(QUERY, (trip_id,))
        return local.cur.fetchone()

    loop = asyncio.get_running_loop()
    from concurrent.futures import ThreadPoolExecutor

    loop.set_default_executor(ThreadPoolExecutor(max_workers=concurrency))
    sem = asyncio.Semaphore(concurrency)

    async def one(i: int):
        async with sem:
            return await asyncio.to_thread(lookup, i % 2_000_000 + 1)

    await asyncio.gather(*(one(i) for i in range(concurrency)))  # warm-up: connect
    start = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(queries)))
    report("to_thread", concurrency, queries, time.perf_counter() - start)


async def run_native_preview(concurrency: int, queries: int) -> None:
    import warnings

    from mssql_python.async_query import _AsyncConnection

    warnings.simplefilter("ignore", FutureWarning)
    conns = [await _AsyncConnection.connect(conn_str(), autocommit=True) for _ in range(concurrency)]

    async def worker(conn, ids):
        cur = conn.cursor()
        for i in ids:
            await cur.execute(QUERY, (i % 2_000_000 + 1,))
            await cur.fetchone()
        await cur.close()

    await asyncio.gather(*(worker(c, [w]) for w, c in enumerate(conns)))  # warm-up
    start = time.perf_counter()
    await asyncio.gather(
        *(worker(c, range(w, queries, concurrency)) for w, c in enumerate(conns))
    )
    report("native_preview", concurrency, queries, time.perf_counter() - start)
    for c in conns:
        await c.close()


async def run_fastmssql(concurrency: int, queries: int) -> None:
    from fastmssql import Connection, PoolConfig

    cs = conn_str().replace("Uid=", "User Id=").replace("Pwd=", "Password=")
    conn = Connection(cs, pool_config=PoolConfig(max_size=concurrency, min_idle=concurrency))
    await conn.__aenter__()
    sql = QUERY.replace("?", "@P1")
    sem = asyncio.Semaphore(concurrency)

    async def one(i: int):
        async with sem:
            return (await conn.query(sql, [i % 2_000_000 + 1])).rows()

    await asyncio.gather(*(one(i) for i in range(concurrency)))
    start = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(queries)))
    report("fastmssql", concurrency, queries, time.perf_counter() - start)


async def run_port_aio(concurrency: int, queries: int) -> None:
    from mssql_python import aio

    conns = await asyncio.gather(
        *(aio.connect(conn_str(), autocommit=True) for _ in range(concurrency))
    )

    async def worker(conn, ids):
        cur = conn.cursor()
        for i in ids:
            await cur.execute(QUERY, i % 2_000_000 + 1)
            await cur.fetchone()

    await asyncio.gather(*(worker(c, [w]) for w, c in enumerate(conns)))  # warm-up
    start = time.perf_counter()
    await asyncio.gather(
        *(worker(c, range(w, queries, concurrency)) for w, c in enumerate(conns))
    )
    report("port_aio", concurrency, queries, time.perf_counter() - start)
    for c in conns:
        await c.close()


async def run_port_aio_pool(concurrency: int, queries: int) -> None:
    from mssql_python import aio

    reset = os.environ.get("AIO_POOL_RESET", "1") == "1"
    pool = await aio.create_pool(conn_str(), min_size=concurrency, max_size=concurrency, reset=reset)
    sem = asyncio.Semaphore(concurrency)

    async def one(i: int):
        async with sem:
            return await pool.fetchrow(QUERY, i % 2_000_000 + 1)

    await asyncio.gather(*(one(i) for i in range(concurrency)))
    start = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(queries)))
    report("port_aio_pool", concurrency, queries, time.perf_counter() - start)
    await pool.close()


def report(mode: str, concurrency: int, queries: int, elapsed: float) -> None:
    print(json.dumps({
        "mode": mode, "concurrency": concurrency, "queries": queries,
        "seconds": round(elapsed, 3), "qps": round(queries / elapsed),
    }), flush=True)


RUNNERS = {
    "to_thread": run_to_thread,
    "native_preview": run_native_preview,
    "fastmssql": run_fastmssql,
    "port_aio": run_port_aio,
    "port_aio_pool": run_port_aio_pool,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=list(RUNNERS), required=True)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 16, 64])
    parser.add_argument("--queries", type=int, default=2000)
    args = parser.parse_args()
    for c in args.concurrency:
        asyncio.run(RUNNERS[args.mode](c, args.queries))


if __name__ == "__main__":
    main()
