"""Arrow fetch benchmark on the NYC taxi tables (scripts/load_taxi.py).

    python benchmarks/bench_arrow.py [--repeats 5] [--out result.json]

Runs in whichever interpreter you use: .venv-upstream measures upstream
mssql-python (C++ + ODBC), the repo .venv this port. Workloads a driver lacks
(the native API is port-only) are reported as skipped. Needs
DB_CONNECTION_STRING (Database=master is rewritten to nyctaxi).
"""

import argparse
import json
import os
import statistics
import time

import duckdb
import mssql_python

ALL_3M = "SELECT * FROM dbo.yellow_trips"
ALL_100K = "SELECT * FROM dbo.yellow_trips_100k"
AGG = "SELECT PULocationID, count(*) AS trips, avg(total_amount) AS avg_total FROM src GROUP BY 1"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--out")
    args = parser.parse_args()

    cs = os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=nyctaxi")
    conn = mssql_python.connect(cs, autocommit=True)
    native = hasattr(mssql_python.Cursor, "fetchall_arrow")

    def run(sql):
        cur = conn.cursor()
        cur.execute(sql)
        return cur

    def arrow_100k():
        return run(ALL_100K).arrow().num_rows

    def arrow_3m():
        return run(ALL_3M).arrow(batch_size=65_536).num_rows

    def duckdb_reader_3m():
        src = run(ALL_3M).arrow_reader(batch_size=65_536)  # noqa: F841 (DuckDB scans it by name)
        return len(duckdb.sql(AGG).fetchall())

    def fetchall_arrow_100k():
        return run(ALL_100K).fetchall_arrow().num_rows

    def fetchall_arrow_3m():
        return run(ALL_3M).fetchall_arrow().num_rows

    def duckdb_stream_3m():
        src = run(ALL_3M).arrow_stream(batch_size=65_536)  # noqa: F841
        return len(duckdb.sql(AGG).fetchall())

    def read_arrow_3m(n):
        return lambda: mssql_python.read_arrow(
            cs, ALL_3M, partition_on="trip_id", partitions=n
        ).num_rows

    def duckdb_read_arrow_3m(n):
        def run_it():
            src = mssql_python.read_arrow(cs, ALL_3M, partition_on="trip_id", partitions=n)  # noqa: F841
            return len(duckdb.sql(AGG).fetchall())

        return run_it

    workloads = {
        "arrow_100k_all_cols": (arrow_100k, True),
        "arrow_3m_all_cols": (arrow_3m, True),
        "duckdb_over_arrow_reader_3m": (duckdb_reader_3m, True),
        "fetchall_arrow_100k (native)": (fetchall_arrow_100k, native),
        "fetchall_arrow_3m (native)": (fetchall_arrow_3m, native),
        "duckdb_over_arrow_stream_3m (native)": (duckdb_stream_3m, native),
    }
    for n in (2, 4, 8):
        workloads[f"read_arrow_3m partitions={n} (native)"] = (read_arrow_3m(n), native)
    workloads["duckdb_over_read_arrow_3m partitions=8 (native)"] = (duckdb_read_arrow_3m(8), native)
    results = {}
    for name, (fn, available) in workloads.items():
        if not available:
            results[name] = {"skipped": True}
            continue
        fn()  # warm-up
        samples = []
        for _ in range(args.repeats):
            start = time.perf_counter()
            rows = fn()
            samples.append(time.perf_counter() - start)
        results[name] = {"median_s": statistics.median(samples), "min_s": min(samples), "rows": rows}
        print(f"{name:40} median {results[name]['median_s']:.3f}s  min {min(samples):.3f}s", flush=True)
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(results, fh, indent=2)


if __name__ == "__main__":
    main()
