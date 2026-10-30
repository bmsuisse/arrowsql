"""Upstream mssql-python (C++ + ODBC) read-path measurements for comparison.

    .venv-upstream/bin/python benchmarks/experiments/upstream_reads.py <experiment> [--port 1433] [--extra "PacketSize=16384"]

Experiments: arrow3m, fetchall3m, latency, roundtrips (run through wan_proxy
on --port to count packets), python_objects.
"""

import argparse
import json
import os
import resource
import time


def conn_str(port: int, extra: str) -> str:
    cs = os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=nyctaxi")
    if port != 1433:
        # wan_proxy listens on IPv4 only; ODBC tries ::1 first for "localhost".
        cs = cs.replace("localhost,1433", f"127.0.0.1,{port}")
    return cs + (";" + extra if extra else "")


def cpu() -> float:
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime


def timed(label, fn, repeat=3, **meta):
    fn()
    for _ in range(repeat):
        c0, t0 = cpu(), time.perf_counter()
        rows = fn()
        wall = time.perf_counter() - t0
        print(json.dumps({"exp": label, **meta, "rows": rows, "wall_s": round(wall, 4),
                          "cpu_s": round(cpu() - c0, 4), "rows_per_s": round(rows / wall)}))


def main() -> None:
    import mssql_python

    ap = argparse.ArgumentParser()
    ap.add_argument("experiment")
    ap.add_argument("--port", type=int, default=1433)
    ap.add_argument("--extra", default="")
    ap.add_argument("--n", type=int, default=2000)
    args = ap.parse_args()
    conn = mssql_python.connect(conn_str(args.port, args.extra), autocommit=True)
    cur = conn.cursor()
    meta = {"extra": args.extra}

    if args.experiment == "arrow3m":
        def run():
            cur.execute("SELECT * FROM dbo.yellow_trips")
            return cur.arrow(batch_size=65536).num_rows
        timed("upstream_arrow_3m", run, **meta)
    elif args.experiment == "fetchall3m":
        def run():
            cur.execute("SELECT * FROM dbo.yellow_trips")
            n = 0
            while batch := cur.fetchmany(65536):
                n += len(batch)
            return n
        timed("upstream_fetchmany_3m", run, repeat=2, **meta)
    elif args.experiment in ("latency", "roundtrips"):
        for label, sql, params in (
            ("select1", "SELECT 1", ()),
            ("point_param", "SELECT * FROM dbo.yellow_trips WHERE trip_id = ?", (12345,)),
            ("point_literal", "SELECT * FROM dbo.yellow_trips WHERE trip_id = 12345", ()),
        ):
            cur.execute(sql, params)
            cur.fetchall()
            t0 = time.perf_counter()
            for _ in range(args.n):
                cur.execute(sql, params)
                cur.fetchall()
            us = (time.perf_counter() - t0) * 1e6 / args.n
            print(json.dumps({"exp": "upstream_latency", "query": label, "us_per_query": round(us, 1), **meta}))
        t0 = time.perf_counter()
        for _ in range(20):
            mssql_python.connect(conn_str(args.port, args.extra), autocommit=True).close()
        print(json.dumps({"exp": "upstream_latency", "query": "connect",
                          "ms_per_connect": round((time.perf_counter() - t0) * 50, 2), **meta}))
    else:
        raise SystemExit(f"unknown experiment {args.experiment}")


if __name__ == "__main__":
    main()
