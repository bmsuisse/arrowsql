"""Experiment A: read_arrow_stream vs read_arrow (table) on nyctaxi.dbo.yellow_trips.

    .venv/bin/python benchmarks/experiments/stream_vs_table.py [--reps 5] [--parts 1 4 8] [--out FILE]

Every trial runs in a fresh subprocess (clean peak RSS) under the machine-wide
bench lock. Modes are interleaved A/B per repetition.
  read_table      read_arrow(...)                         -> time, rss
  read_stream     drain read_arrow_stream into batches    -> time, rss
  duck_table      read_arrow then DuckDB aggregate        -> time, rss
  duck_stream     DuckDB aggregate over the stream        -> time, rss
  delta_table     read_arrow then write_deltalake         -> time, rss
  delta_stream    write_deltalake over the stream         -> time, rss
  first_batch     time until the first batch of a stream
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LOCK = os.environ.get(
    "BENCH_LOCK",
    "/tmp/claude-1006/-home-dominikaiuser-devops/029c60c3-79eb-4b6a-aa39-a1c105102018/scratchpad/codex-tds/bench.lock",
)
MODES = ["read_old", "read_table", "read_stream", "duck_table", "duck_stream", "delta_table", "delta_stream", "first_batch"]
QUERY = "SELECT * FROM dbo.yellow_trips"
AGG = "SELECT VendorID, count(*) AS n, sum(total_amount) AS amt, avg(trip_distance) AS dist FROM {} GROUP BY 1 ORDER BY 1"


def child(mode: str, parts: int) -> None:
    import resource

    sys.path.insert(0, os.path.dirname(HERE))
    from common import ConnInfo  # benchmarks/common.py

    import arrowsql

    conn_str = ConnInfo.from_env("nyctaxi").odbc()
    kw = dict(partition_on="trip_id", partitions=parts)
    t0 = time.perf_counter()
    extra = {}
    if mode == "read_old":  # baseline: pre-stream read_arrow (materialize slices, concat)
        sys.path.insert(0, HERE)
        import _old_arrow_parallel as old

        t = old.read_arrow(conn_str, QUERY, **kw)
        extra["rows"] = t.num_rows
    elif mode == "read_table":
        t = arrowsql.read_arrow(conn_str, QUERY, **kw)
        extra["rows"] = t.num_rows
    elif mode == "read_stream":
        import pyarrow as pa

        with arrowsql.read_arrow_stream(conn_str, QUERY, **kw) as s:
            extra["rows"] = sum(b.num_rows for b in pa.RecordBatchReader.from_stream(s))
    elif mode == "first_batch":
        import pyarrow as pa

        s = arrowsql.read_arrow_stream(conn_str, QUERY, **kw)
        r = pa.RecordBatchReader.from_stream(s)
        r.read_next_batch()
        extra["first_batch"] = time.perf_counter() - t0
        r.close()
        s.close()
    elif mode.startswith("duck_"):
        import duckdb

        src = arrowsql.read_arrow(conn_str, QUERY, **kw) if mode == "duck_table" else arrowsql.read_arrow_stream(conn_str, QUERY, **kw)
        extra["rows"] = sum(r[1] for r in duckdb.sql(AGG.format("src")).fetchall())
        if mode == "duck_stream":
            src.close()
    elif mode.startswith("delta_"):
        from deltalake import write_deltalake

        src = arrowsql.read_arrow(conn_str, QUERY, **kw) if mode == "delta_table" else arrowsql.read_arrow_stream(conn_str, QUERY, **kw)
        with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR", "/tmp")) as d:
            write_deltalake(d, src, mode="overwrite")
        if mode == "delta_stream":
            src.close()
    elapsed = time.perf_counter() - t0
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print("RESULT " + json.dumps({"mode": mode, "parts": parts, "seconds": extra.get("first_batch", elapsed), "total": elapsed, "rss_mb": rss, **extra}))


def run_trial(mode: str, parts: int) -> dict:
    cmd = ["flock", LOCK, sys.executable, os.path.abspath(__file__), "--child", mode, str(parts)]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    for line in out.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[7:])
    raise RuntimeError(f"{mode}/{parts} failed: {out.stderr[-800:]}")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        return child(sys.argv[2], int(sys.argv[3]))
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--parts", type=int, nargs="+", default=[1, 4, 8])
    ap.add_argument("--modes", nargs="+", default=MODES)
    ap.add_argument("--out", default=os.path.join(HERE, "stream_vs_table.jsonl"))
    args = ap.parse_args()
    results = []
    with open(args.out, "a") as f:
        for rep in range(args.reps):
            for parts in args.parts:
                for mode in args.modes:
                    r = run_trial(mode, parts)
                    r["rep"] = rep
                    f.write(json.dumps(r) + "\n")
                    f.flush()
                    results.append(r)
                    print(f"rep{rep} parts={parts} {mode}: {r['seconds']:.2f}s rss={r['rss_mb']:.0f}MB", flush=True)
    print("\nmedians (seconds / peak RSS MB)")
    for parts in args.parts:
        for mode in args.modes:
            rs = [r for r in results if r["parts"] == parts and r["mode"] == mode]
            if rs:
                print(f"parts={parts} {mode:13s} {statistics.median(r['seconds'] for r in rs):7.2f}s {statistics.median(r['rss_mb'] for r in rs):7.0f}MB")


if __name__ == "__main__":
    main()
