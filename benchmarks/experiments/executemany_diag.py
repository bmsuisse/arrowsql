"""Why is upstream executemany so slow on taxi rows? Variants on 2,000 rows.

    benchmarks/experiments/run_py.sh executemany_diag.py [--port 14330]

Run through wan_proxy (--port) to also get packets per row from its stats.
"""

import argparse
import json
import os
import time

import pyarrow.parquet as pq
from mssql_python import connect


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=1433)
    ap.add_argument("--rows", type=int, default=2000)
    ap.add_argument("--txn", action="store_true", help="autocommit off, one commit at the end")
    args = ap.parse_args()
    cs = os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=perf_research")
    if args.port != 1433:
        cs = cs.replace("localhost,1433", f"127.0.0.1,{args.port}")
    t = pq.read_table(os.environ["TAXI_PARQUET"]).slice(0, args.rows)
    rows = [tuple(r.values()) for r in t.to_pylist()]
    names = t.column_names
    no_null = [tuple(0 if v is None else v for v in r) for r in rows]
    ints_only = [(r[0], r[3] or 0, r[7], r[8]) for r in rows]

    conn = connect(cs, autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS dbo.em; CREATE TABLE dbo.em ("
                "VendorID int, p datetime2(6), d datetime2(6), passenger_count bigint, "
                "trip_distance float, RatecodeID bigint, flag nvarchar(1), pu int, du int, "
                "payment_type bigint, a float, b float, c float, e float, f float, g float, "
                "h float, i float, j float)")
    ph = ",".join("?" * len(names))
    for label, data, sql in (
        ("taxi_float_cols", rows, f"INSERT INTO dbo.em VALUES ({ph})"),
        ("taxi_no_nulls", no_null, f"INSERT INTO dbo.em VALUES ({ph})"),
        ("ints_only_4cols", ints_only, "INSERT INTO dbo.em (VendorID, passenger_count, pu, du) VALUES (?,?,?,?)"),
    ):
        cur.execute("TRUNCATE TABLE dbo.em")
        t0 = time.perf_counter()
        if args.txn:
            conn.autocommit = False
        cur.executemany(sql, data)
        if args.txn:
            conn.commit()
            conn.autocommit = True
        dt = time.perf_counter() - t0
        print(json.dumps({"exp": label, "txn": args.txn, "rows": len(data), "wall_s": round(dt, 3),
                          "rows_per_s": round(len(data) / dt)}), flush=True)
    cur.execute("DROP TABLE dbo.em")
    conn.close()


if __name__ == "__main__":
    main()
