"""NYC-taxi benchmark: one driver per process, identical workloads.

    python benchmarks/bench_taxi.py --driver mssql_python [--quick] [--out result.json]
    python benchmarks/bench_taxi.py --driver fastmssql    [--quick] [--out result.json]

Which `mssql_python` gets measured (upstream ODBC wheel vs this Rust port) is
decided by the interpreter you run it with; benchmarks/compare.py runs all of
them and prints the table. Needs DB_CONNECTION_STRING (Database=master is
rewritten to nyctaxi) and the tables from scripts/load_taxi.py.
"""

import argparse
import asyncio
import json
import os
import statistics
import time
from datetime import datetime
from decimal import Decimal

ALL_COLS = "SELECT * FROM dbo.yellow_trips_100k"
NARROW_3M = (
    "SELECT trip_id, tpep_pickup_datetime, trip_distance, total_amount "
    "FROM dbo.yellow_trips"
)
AGG = (
    "SELECT PULocationID, COUNT_BIG(*) AS trips, AVG(total_amount) AS avg_total "
    "FROM dbo.yellow_trips GROUP BY PULocationID ORDER BY trips DESC"
)
POINT = "SELECT * FROM dbo.yellow_trips WHERE trip_id = ?"

INSERT_ROWS = [
    (i, datetime(2024, 1, 1, 0, i % 60), float(i) / 10, Decimal(f"{i % 1000}.25"), f"row-{i}")
    for i in range(10_000)
]


def conn_str() -> str:
    return os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=nyctaxi")


def timed(fn, repeats: int) -> dict:
    try:
        fn()  # warm-up: connection setup, plan cache, page cache
    except Exception as exc:  # a driver that lacks a feature still reports the rest
        return {"error": f"{type(exc).__name__}: {exc}"[:200]}
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        rows = fn()
        samples.append(time.perf_counter() - start)
    return {"median_s": statistics.median(samples), "min_s": min(samples), "rows": rows}


def run_mssql_python(quick: bool) -> dict:
    import mssql_python

    cs = conn_str()
    conn = mssql_python.connect(cs, autocommit=True)
    repeats = 3 if quick else 7

    def fetchall_100k():
        cur = conn.cursor()
        cur.execute(ALL_COLS)
        return len(cur.fetchall())

    def fetchone_100k():
        cur = conn.cursor()
        cur.execute(ALL_COLS)
        n = 0
        while cur.fetchone() is not None:
            n += 1
        return n

    def fetchmany_3m():
        cur = conn.cursor()
        cur.execute(NARROW_3M)
        n = 0
        while batch := cur.fetchmany(10_000):
            n += len(batch)
        return n

    def aggregate():
        cur = conn.cursor()
        cur.execute(AGG)
        return len(cur.fetchall())

    def point_lookups():
        cur = conn.cursor()
        for trip_id in range(1, 1001):
            cur.execute(POINT, (trip_id,))
            cur.fetchall()
        return 1000

    def executemany_10k():
        cur = conn.cursor()
        cur.execute(
            "DROP TABLE IF EXISTS #ins; CREATE TABLE #ins (a int, b datetime2, c float, "
            "d decimal(10,2), e nvarchar(50))"
        )
        cur.executemany("INSERT INTO #ins VALUES (?, ?, ?, ?, ?)", INSERT_ROWS)
        return len(INSERT_ROWS)

    def connect_close_50():
        for _ in range(50):
            mssql_python.connect(cs, autocommit=True).close()
        return 50

    def arrow_100k():
        cur = conn.cursor()
        cur.execute(ALL_COLS)
        return cur.arrow().num_rows

    def arrow_3m_all_cols():
        cur = conn.cursor()
        cur.execute("SELECT * FROM dbo.yellow_trips")
        return cur.arrow(batch_size=65_536).num_rows

    def duckdb_over_arrow_3m():
        import duckdb

        cur = conn.cursor()
        cur.execute("SELECT * FROM dbo.yellow_trips")
        reader = cur.arrow_reader(batch_size=65_536)
        return duckdb.sql(
            "SELECT PULocationID, count(*), avg(total_amount) FROM reader GROUP BY 1"
        ).fetchall().__len__()

    workloads = {
        "arrow_100k_all_cols": arrow_100k,
        "arrow_3m_all_cols": arrow_3m_all_cols,
        "duckdb_over_arrow_3m": duckdb_over_arrow_3m,
        "fetchall_100k_all_cols": fetchall_100k,
        "fetchone_100k_all_cols": fetchone_100k,
        "fetchmany_3m_narrow": fetchmany_3m,
        "aggregate_3m": aggregate,
        "point_lookup_x1000": point_lookups,
        "executemany_10k": executemany_10k,
        "connect_close_x50": connect_close_50,
    }
    if quick:
        for name in ("fetchmany_3m_narrow", "arrow_3m_all_cols", "duckdb_over_arrow_3m"):
            workloads.pop(name)
    return {name: timed(fn, repeats) for name, fn in workloads.items()}


def run_fastmssql(quick: bool) -> dict:
    from fastmssql import Connection

    # FastMssql uses ADO-style keys and @P1 parameters.
    cs = conn_str().replace("Uid=", "User Id=").replace("Pwd=", "Password=")
    repeats = 3 if quick else 7
    loop = asyncio.new_event_loop()

    async def connect():
        conn = Connection(cs)
        await conn.__aenter__()
        return conn

    conn = loop.run_until_complete(connect())

    def run(coro_fn):
        return lambda: loop.run_until_complete(coro_fn())

    async def fetchall_100k():
        return len((await conn.query(ALL_COLS)).rows())

    async def fetchmany_3m():
        return len((await conn.query(NARROW_3M)).rows())

    async def aggregate():
        return len((await conn.query(AGG)).rows())

    async def point_lookups():
        for trip_id in range(1, 1001):
            (await conn.query(POINT.replace("?", "@P1"), [trip_id])).rows()
        return 1000

    workloads = {
        "fetchall_100k_all_cols": fetchall_100k,
        "fetchmany_3m_narrow": fetchmany_3m,
        "aggregate_3m": aggregate,
        "point_lookup_x1000": point_lookups,
    }
    if quick:
        workloads.pop("fetchmany_3m_narrow")
    return {name: timed(run(fn), repeats) for name, fn in workloads.items()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", choices=["mssql_python", "fastmssql"], required=True)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args()
    runner = run_mssql_python if args.driver == "mssql_python" else run_fastmssql
    results = runner(args.quick)
    text = json.dumps(results, indent=2)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text)
    print(text)


if __name__ == "__main__":
    main()
