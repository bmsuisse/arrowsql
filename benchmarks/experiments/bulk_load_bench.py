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


def one(sql):
    c = connect(cs(1433), autocommit=True)
    cur = c.cursor()
    cur.execute(sql)
    rows = [tuple(r) for r in cur.fetchall()]
    c.close()
    return rows


def log_bytes():
    return one("SELECT num_of_bytes_written FROM sys.dm_io_virtual_file_stats(DB_ID(), 2)")[0][0]


def rowgroups():
    rows = one("SELECT state_desc, COALESCE(trim_reason_desc,''), COUNT(*), SUM(total_rows), MIN(total_rows) "
               "FROM sys.dm_db_column_store_row_group_physical_stats WHERE object_id = OBJECT_ID('dbo.bench') "
               "GROUP BY state_desc, trim_reason_desc ORDER BY 1, 2")
    total = sum(r[3] for r in rows)
    comp = sum(r[3] for r in rows if r[0] == "COMPRESSED")
    groups = sum(r[2] for r in rows if r[0] == "COMPRESSED")
    return {"compressed_rowgroups": groups, "compressed_rows": comp, "delta_rows": total - comp,
            "avg_rg_rows": round(comp / groups) if groups else 0,
            "detail": [f"{r[0]}/{r[1]}:{r[2]}x{r[3]}" for r in rows]}


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


def narrow(rows):
    rng = np.random.default_rng(2)
    t = pa.table({
        "id": pa.array(np.arange(rows), pa.int64()),
        "a": pa.array(rng.integers(0, 300, rows), pa.int32()),
        "b": pa.array(rng.integers(0, 10**6, rows), pa.int32()),
        "c": pa.array(rng.integers(0, 10**6, rows), pa.int64()).cast(pa.decimal128(19, 0)),
    })
    c = t.column("c").combine_chunks()
    t = t.set_column(3, "c", pa.Array.from_buffers(pa.decimal128(10, 2), len(c), c.buffers(), 0))
    return t, "id bigint NULL, a int NULL, b int NULL, c decimal(10,2) NULL"


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
    ap.add_argument("--tablock", type=int, default=-1, help="1/0; default: on for heap, off for --cci")
    ap.add_argument("--dup", type=int, default=1, help="repeat the source table N times (taxi: ~3M rows each)")
    ap.add_argument("--no-load", action="store_true", help="only (re)create the table and exit")
    a = ap.parse_args()
    table, ddl = (taxi(a.rows) if a.kind == "taxi" else narrow(a.rows) if a.kind == "narrow"
                  else dec9(a.rows, a.kind == "dec9_wide"))
    if a.dup > 1:
        table = pa.concat_tables([table] * a.dup).combine_chunks()
    tablock = bool(a.tablock) if a.tablock >= 0 else not a.cci
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
    log0 = log_bytes()
    conns = [connect(cs(a.port), autocommit=True) for _ in parts]
    errs = []

    def run(c, part):
        try:
            c.cursor().bulkcopy_arrow("dbo.bench", part, batch_size=a.batch, timeout=0,
                                      column_mappings=mappings, table_lock=tablock)
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
    res = {"kind": a.kind, "rows": n, "workers": a.workers, "batch": a.batch, "cci": a.cci, "tablock": tablock,
           "seconds": round(dt, 3), "errors": errs, "log_mb": round((log_bytes() - log0) / 1e6, 1)}
    if a.cci:
        res.update(rowgroups())
    print(json.dumps(res), flush=True)


main()
