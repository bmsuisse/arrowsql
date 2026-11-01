"""One timed bulk load, printed as a JSON line. Used by the decimal-width and CCI experiments.

    bulk_load_bench.py dec9|dec9_wide|taxi ROWS PORT [--cci] [--workers N] [--batch B]

Connects to localhost,PORT (a wan_proxy or the server itself) using the
credentials of DB_CONNECTION_STRING, loads into dbo.bench (recreated), and
prints elapsed seconds. The decimal width policy is switched from outside with
ARROWSQL_DECIMAL_FIXED=1.
"""
import argparse
import json
import os
import sys
import threading
import time

import numpy as np
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mssql_python import connect  # noqa: E402


def cs(port):
    base = os.environ["DB_CONNECTION_STRING"]
    return base.replace("localhost,1433", f"localhost,{port}")


def dec9(rows, wide):
    rng = np.random.default_rng(1)
    out = {}
    for i in range(9):
        hi = 10**17 if wide else 10**7
        vals = rng.integers(-hi, hi, rows)
        out[f"d{i}"] = pa.array(vals, pa.int64()).cast(pa.decimal128(38, 0))
    t = pa.table(out)
    # reinterpret unscaled int storage as scale 2
    arrs = []
    for name in t.column_names:
        a = t.column(name).combine_chunks()
        arrs.append(pa.Array.from_buffers(pa.decimal128(38, 2), len(a), a.buffers(), a.null_count))
    ddl = ", ".join(f"d{i} decimal(38,2) NULL" for i in range(9))
    return pa.table(dict(zip(t.column_names, arrs))), ddl


def taxi(rows):
    import write_paths as wp
    return wp.load_table(rows), wp.COLUMNS


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("kind")
    ap.add_argument("rows", type=int)
    ap.add_argument("port", type=int)
    ap.add_argument("--cci", action="store_true")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--batch", type=int, default=0)
    ap.add_argument("--no-load", action="store_true", help="only (re)create the table and exit")
    a = ap.parse_args()
    table, ddl = taxi(a.rows) if a.kind == "taxi" else dec9(a.rows, a.kind == "dec9_wide")
    mappings = [(i, n) for i, n in enumerate(table.column_names)]
    conn = connect(cs(1433), autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS dbo.bench")
    cur.execute(f"CREATE TABLE dbo.bench ({ddl})")
    if a.cci:
        cur.execute("CREATE CLUSTERED COLUMNSTORE INDEX cci ON dbo.bench")
    cur.execute("CHECKPOINT")
    conn.close()
    if a.no_load:
        return
    n = table.num_rows
    parts = [table.slice(i * (n // a.workers), n // a.workers if i < a.workers - 1 else n - i * (n // a.workers))
             for i in range(a.workers)]
    conns = [connect(cs(a.port), autocommit=True) for _ in parts]
    errs = []

    def run(c, part):
        try:
            c.cursor().bulkcopy_arrow("dbo.bench", part, batch_size=a.batch, timeout=0,
                                      column_mappings=mappings, table_lock=not a.cci)
        except Exception as e:  # noqa: BLE001
            errs.append(repr(e))

    t0 = time.perf_counter()
    ths = [threading.Thread(target=run, args=(c, p)) for c, p in zip(conns, parts)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    dt = time.perf_counter() - t0
    for c in conns:
        c.close()
    print(json.dumps({"kind": a.kind, "rows": n, "workers": a.workers, "batch": a.batch, "cci": a.cci,
                      "seconds": round(dt, 3), "errors": errs}), flush=True)


main()
