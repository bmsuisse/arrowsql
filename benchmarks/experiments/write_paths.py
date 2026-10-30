"""Write-path measurements with the upstream wheel (bulk copy is mssql-tds Rust).

    benchmarks/experiments/run_py.sh write_paths.py [--rows 1000000] [--only name ...]

Loads the first --rows taxi rows from parquet into a scratch database
`perf_research` (SIMPLE recovery) through each strategy and prints rows/s.
"""

import argparse
import json
import os
import re
import threading
import time
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from mssql_python import connect

PARQUET = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "yellow_tripdata_2024-01.parquet",
)
MONEY = {"fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
         "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"}
COLUMNS = """
    VendorID int NULL, tpep_pickup_datetime datetime2(6) NULL, tpep_dropoff_datetime datetime2(6) NULL,
    passenger_count bigint NULL, trip_distance float NULL, RatecodeID bigint NULL,
    store_and_fwd_flag nvarchar(1) NULL, PULocationID int NULL, DOLocationID int NULL,
    payment_type bigint NULL, fare_amount decimal(10,2) NULL, extra decimal(10,2) NULL,
    mta_tax decimal(10,2) NULL, tip_amount decimal(10,2) NULL, tolls_amount decimal(10,2) NULL,
    improvement_surcharge decimal(10,2) NULL, total_amount decimal(10,2) NULL,
    congestion_surcharge decimal(10,2) NULL, Airport_fee decimal(10,2) NULL"""


def base_cs() -> str:
    return os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=perf_research")


def load_table(rows: int) -> pa.Table:
    path = PARQUET if os.path.exists(PARQUET) else os.environ["TAXI_PARQUET"]
    t = pq.read_table(path).slice(0, rows)
    for i, f in enumerate(t.schema):
        if f.name in MONEY:
            t = t.set_column(i, f.name, t.column(i).cast(pa.decimal128(10, 2)))
    return t.combine_chunks()


def recreate(cur, kind: str) -> None:
    cur.execute("DROP TABLE IF EXISTS dbo.t")
    cur.execute(f"CREATE TABLE dbo.t ({COLUMNS})")
    if kind == "clustered":
        cur.execute("ALTER TABLE dbo.t ADD id bigint IDENTITY PRIMARY KEY CLUSTERED")
    elif kind == "cci":
        cur.execute("CREATE CLUSTERED COLUMNSTORE INDEX cci ON dbo.t")
    cur.execute("CHECKPOINT")


def report(name, rows, seconds, **extra):
    print(json.dumps({"exp": name, "rows": rows, "wall_s": round(seconds, 3),
                      "rows_per_s": round(rows / seconds), **extra}), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args()

    with connect(os.environ["DB_CONNECTION_STRING"], autocommit=True) as c:
        c.cursor().execute(
            "IF DB_ID('perf_research') IS NULL CREATE DATABASE perf_research; "
            "ALTER DATABASE perf_research SET RECOVERY SIMPLE;"
        )
    table = load_table(args.rows)
    mappings = [(i, n) for i, n in enumerate(table.column_names)]
    conn = connect(base_cs(), autocommit=True)
    cur = conn.cursor()

    def bulk(name, kind, batch_size, table_lock, data=table):
        recreate(cur, kind)
        c0, t0 = time.process_time(), time.perf_counter()
        cur.bulkcopy_arrow("dbo.t", data, batch_size=batch_size, timeout=0,
                           column_mappings=mappings, table_lock=table_lock)
        report(name, data.num_rows, time.perf_counter() - t0, kind=kind,
               batch_size=batch_size, table_lock=table_lock,
               client_cpu_s=round(time.process_time() - c0, 3))

    def parallel_bulk(name, kind, workers):
        recreate(cur, kind)
        parts = [table.slice(i * (table.num_rows // workers), table.num_rows // workers)
                 for i in range(workers)]
        conns = [connect(base_cs(), autocommit=True) for _ in parts]

        def run(c, part):
            c.cursor().bulkcopy_arrow("dbo.t", part, batch_size=0, timeout=0,
                                      column_mappings=mappings, table_lock=True)

        threads = [threading.Thread(target=run, args=a) for a in zip(conns, parts)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        report(name, sum(p.num_rows for p in parts), time.perf_counter() - t0,
               kind=kind, workers=workers)
        for c in conns:
            c.close()

    small = table.slice(0, 50_000)
    small_rows = [tuple(r.values()) for r in small.to_pylist()]
    placeholders = ",".join("?" * len(table.column_names))
    insert_cols = ",".join(table.column_names)

    def in_txn(fn):
        # One commit at the end: autocommit costs a log flush per statement.
        conn.autocommit = False
        fn()
        conn.commit()
        conn.autocommit = True

    def executemany():
        recreate(cur, "heap")
        t0 = time.perf_counter()
        in_txn(lambda: cur.executemany(
            f"INSERT INTO dbo.t ({insert_cols}) VALUES ({placeholders})", small_rows))
        report("executemany_50k_txn", len(small_rows), time.perf_counter() - t0)

    def multirow_values():
        # 19 params/row; 2100-param limit -> 110 rows per statement.
        recreate(cur, "heap")
        per = 110

        def run():
            for i in range(0, len(small_rows), per):
                chunk = small_rows[i:i + per]
                values = ",".join(f"({placeholders})" for _ in chunk)
                cur.execute(f"INSERT INTO dbo.t ({insert_cols}) VALUES {values}",
                            [v for row in chunk for v in row])

        t0 = time.perf_counter()
        in_txn(run)
        report("multirow_values_50k_txn", len(small_rows), time.perf_counter() - t0)

    def row_bulkcopy():
        recreate(cur, "heap")
        t0 = time.perf_counter()
        cur.bulkcopy("dbo.t", small_rows, batch_size=0, timeout=0,
                      column_mappings=list(table.column_names), table_lock=True)
        report("bulkcopy_tuples_50k", len(small_rows), time.perf_counter() - t0)

    def openjson():
        recreate(cur, "heap")
        doc = json.dumps([
            {k: (str(v) if isinstance(v, (Decimal,)) else v.isoformat() if hasattr(v, "isoformat") else v)
             for k, v in r.items()} for r in small.to_pylist()
        ])
        with_clause = ",".join(
            f"{n} {t}" for n, t in re.findall(r"(\w+) (\w+(?:\([\d,]+\))?) NULL", COLUMNS)
        )
        t0 = time.perf_counter()
        cur.execute(f"INSERT INTO dbo.t ({insert_cols}) SELECT * FROM OPENJSON(?) WITH ({with_clause})", (doc,))
        report("openjson_50k", len(small_rows), time.perf_counter() - t0, json_mb=round(len(doc) / 1e6, 1))

    experiments = {
        "bulk_heap_nolock": lambda: bulk("bulk_heap_nolock", "heap", 0, False),
        "bulk_heap_tablock": lambda: bulk("bulk_heap_tablock", "heap", 0, True),
        "bulk_heap_tablock_b10k": lambda: bulk("bulk_heap_tablock_b10k", "heap", 10_000, True),
        "bulk_heap_tablock_b100k": lambda: bulk("bulk_heap_tablock_b100k", "heap", 100_000, True),
        "bulk_clustered_tablock": lambda: bulk("bulk_clustered_tablock", "clustered", 0, True),
        "bulk_cci_b1m": lambda: bulk("bulk_cci_b1m", "cci", 1_048_576, True),
        "bulk_cci_b100k": lambda: bulk("bulk_cci_b100k", "cci", 102_400, True),
        "bulk_cci_b10k": lambda: bulk("bulk_cci_b10k", "cci", 10_000, True),
        "parallel_heap_2": lambda: parallel_bulk("parallel_heap_2", "heap", 2),
        "parallel_heap_4": lambda: parallel_bulk("parallel_heap_4", "heap", 4),
        "parallel_heap_8": lambda: parallel_bulk("parallel_heap_8", "heap", 8),
        "parallel_cci_4": lambda: parallel_bulk("parallel_cci_4", "cci", 4),
        "executemany_50k": executemany,
        "multirow_values_50k": multirow_values,
        "bulkcopy_tuples_50k": row_bulkcopy,
        "openjson_50k": openjson,
    }
    for name, fn in experiments.items():
        if args.only and name not in args.only:
            continue
        try:
            fn()
        except Exception as exc:  # report and continue with the next strategy
            print(json.dumps({"exp": name, "error": str(exc)[:300]}), flush=True)
    cur.execute("DROP TABLE IF EXISTS dbo.t")
    conn.close()


if __name__ == "__main__":
    main()
