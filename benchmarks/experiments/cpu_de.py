"""Experiment D/E micro-benchmarks (client CPU). Usage: cpu_de.py d1|d2|d3|d4 [reps]
Prints median wall and process CPU (user+sys) seconds of interleaved variants."""

import os
import resource
import statistics
import sys
import time

import numpy as np
import pyarrow as pa

import arrowsql

CS = os.environ["DB_CONNECTION_STRING"]
T = "dbo.de_bench"
BATCH = 65536


def cpu():
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def timed(fn):
    c0, t0 = cpu(), time.perf_counter()
    fn()
    return time.perf_counter() - t0, cpu() - c0


def report(name, runs):
    w = statistics.median(r[0] for r in runs)
    c = statistics.median(r[1] for r in runs)
    print(f"{name:28s} wall {w:7.3f}s  cpu {c:7.3f}s  runs={len(runs)}", flush=True)


def run_bulk(variants, create, reps, **opts):
    conn = arrowsql.connect(CS, autocommit=True)
    try:
        res = {k: [] for k in variants}
        for _ in range(reps):
            for k, tbl in variants.items():
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS {T}")
                cur.execute(create.format(T=T))
                res[k].append(timed(lambda: conn.cursor().bulkcopy_arrow(T, tbl, table_lock=True, **opts)))
        for k, r in res.items():
            report(k, r)
    finally:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {T}")
        conn.close()


def ints(n, cols, start=0):
    return [pa.array(np.arange(start, start + n, dtype=np.int32) + i) for i in range(cols)]


def d1(reps):
    """Zero-copy slices of one big parent vs independent batches (10 wide columns)."""
    n, size, nc = 3_000_000, int(os.environ.get("SLICE", 65536)), 10
    names = [f"c{i}" for i in range(nc)]

    def cols(lo, hi):
        return [pa.array(np.arange(lo, hi, dtype=np.float64) + i) for i in range(nc)]

    parent = pa.RecordBatch.from_arrays(cols(0, n), names=names)
    sliced = pa.Table.from_batches([parent.slice(i, size) for i in range(0, n, size)])
    indep = pa.Table.from_batches(
        [pa.RecordBatch.from_arrays(cols(i, min(i + size, n)), names=names) for i in range(0, n, size)]
    )
    ddl = "CREATE TABLE {T} (" + ",".join(f"{c} float" for c in names) + ")"
    run_bulk({"sliced": sliced, "independent": indep}, ddl, reps)


def d2(reps):
    n = 2_000_000
    words = ["Zürich Öl", "日本語のテキスト", "Привет мир", "plain ascii text", "Ελληνικά κείμενο", "emoji 😀 mix"]
    mixed = pa.array([words[i % 6] * 3 for i in range(n)], pa.string())
    ascii_ = pa.array([("ascii text number %d" % (i % 100)) * 3 for i in range(n)], pa.string())
    t_m = pa.table({"s": mixed})
    t_a = pa.table({"s": ascii_})
    run_bulk(
        {"multilingual": t_m, "ascii": t_a},
        "CREATE TABLE {T} (s varchar(400) COLLATE Latin1_General_100_CI_AS_SC_UTF8)",
        reps,
    )


def d4(reps):
    n = 10_000_000
    names = list("abcd")
    base = pa.RecordBatch.from_arrays(ints(BATCH, 4), names=names)
    tbl = pa.Table.from_batches([base] * (n // BATCH))
    run_bulk({"4int_10M": tbl}, "CREATE TABLE {T} (a int, b int, c int, d int)", reps)


def d3(reps):
    conn = arrowsql.connect(CS, autocommit=True)
    cur = conn.cursor()
    try:
        for sz in (1024, 65536, 1 << 20):
            rows = max(4, (256 << 20) // sz)
            for typ, expr in (
                ("varbinary(max)", f"CAST(REPLICATE(CAST(0x41 AS varchar(max)), {sz}) AS varbinary(max))"),
                ("nvarchar(max)", f"REPLICATE(CAST(N'x' AS nvarchar(max)), {sz // 2})"),
            ):
                cur.execute(f"DROP TABLE IF EXISTS {T}")
                cur.execute(f"CREATE TABLE {T} (v {typ})")
                cur.execute(
                    f"INSERT {T} SELECT TOP ({rows}) {expr} FROM sys.all_columns a CROSS JOIN sys.all_columns b"
                )
                runs = [timed(lambda: conn.cursor().execute(f"SELECT v FROM {T}").arrow(65536)) for _ in range(reps)]
                report(f"{typ} {sz // 1024}KB x{rows}", runs)
    finally:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {T}")
        conn.close()


if __name__ == "__main__":
    globals()[sys.argv[1]](int(sys.argv[2]) if len(sys.argv) > 2 else 5)
