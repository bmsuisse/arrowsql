"""Thread scaling on free-threaded CPython: point lookups and parallel fetchall.

Each (interpreter, workload) pair runs in a fresh process, so no GIL, import or
allocator state carries over between measurements. One warm-up repetition runs
first, then N timed repetitions; the parent reports medians.

    python benchmarks/experiments/free_threaded_scaling.py \
        --interp ft=<main>/.venv-ft/bin/python --interp gil=.venv/bin/python \
        --interp upstream=<main>/.venv-upstream/bin/python --repeats 5

Workloads (connection per thread, opened before timing):
  point_1t / point_8t / point_16t  each thread does 1000 PK lookups on dbo.yellow_trips
  fetchall_8t                      each of 8 threads runs SELECT * FROM dbo.yellow_trips_100k and fetchall()
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from common import load_env  # noqa: E402

BENCH_LOCK = Path.home() / "devops" / ".mssql-rs-bench.lock"
TAXI_MAX_ID = 2_964_624
POINT_LOOKUPS_PER_THREAD = 1000
FETCH_ROWS = 100_000

WORKLOADS = {
    "point_1t": (1, "point"),
    "point_8t": (8, "point"),
    "point_16t": (16, "point"),
    "fetchall_8t": (8, "fetchall"),
}


def conn_string() -> str:
    load_env()
    return os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=nyctaxi")


def run_point(conn, thread_index: int, rng_seed: int) -> int:
    rng = random.Random(rng_seed + thread_index)
    cursor = conn.cursor()
    hits = 0
    for _ in range(POINT_LOOKUPS_PER_THREAD):
        key = rng.randrange(1, TAXI_MAX_ID + 1)
        cursor.execute("SELECT * FROM dbo.yellow_trips WHERE trip_id = ?", key)
        if cursor.fetchone() is not None:
            hits += 1
    cursor.close()
    return hits


def run_fetchall(conn, _thread_index: int, _rng_seed: int) -> int:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.yellow_trips_100k")
    rows = cursor.fetchall()
    cursor.close()
    return len(rows)


def child(workload: str, repeats: int) -> dict:
    from mssql_python import connect

    threads, kind = WORKLOADS[workload]
    body = run_point if kind == "point" else run_fetchall
    expected = POINT_LOOKUPS_PER_THREAD if kind == "point" else FETCH_ROWS
    conn_str = conn_string()
    gil_enabled = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    samples = []
    for rep in range(repeats + 1):
        conns = [connect(conn_str) for _ in range(threads)]
        barrier = threading.Barrier(threads)
        seed = 7 + rep

        def worker(index: int) -> int:
            barrier.wait()
            return body(conns[index], index, seed)

        with ThreadPoolExecutor(max_workers=threads) as pool:
            start = time.perf_counter()
            counts = list(pool.map(worker, range(threads)))
            elapsed = time.perf_counter() - start
        for conn in conns:
            conn.close()
        if kind == "point":
            # Random keys may miss a gap in the identity range; most must hit.
            if min(counts) < expected * 0.99:
                raise RuntimeError(f"{workload}: only {counts} hits of {expected}")
        elif counts != [expected] * threads:
            raise RuntimeError(f"{workload}: unexpected row counts {counts}")
        if rep > 0:  # rep 0 is the untimed warm-up
            samples.append(elapsed)
    units = POINT_LOOKUPS_PER_THREAD * threads if kind == "point" else FETCH_ROWS * threads
    return {"workload": workload, "gil_enabled": gil_enabled, "units": units,
            "unit": "queries" if kind == "point" else "rows", "samples": samples}


def measure(name: str, interp: Path, workload: str, repeats: int, timeout: int) -> dict:
    cmd = [str(interp), "-I", str(Path(__file__).resolve()), "--child", workload, "--repeats", str(repeats)]
    with open(BENCH_LOCK, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    for line in reversed(proc.stdout.splitlines()):
        if line.startswith("BENCHRESULT "):
            result = json.loads(line[len("BENCHRESULT "):])
            result["interp"] = name
            return result
    tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
    return {"interp": name, "workload": workload, "error": " | ".join(tail)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interp", action="append", default=[], metavar="NAME=PATH")
    parser.add_argument("--workload", action="append", choices=sorted(WORKLOADS))
    parser.add_argument("--child", choices=sorted(WORKLOADS), help=argparse.SUPPRESS)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=1200)
    args = parser.parse_args()

    if args.child:
        print("BENCHRESULT " + json.dumps(child(args.child, args.repeats)), flush=True)
        return 0

    workloads = args.workload or list(WORKLOADS)
    for spec in args.interp:
        name, _, path = spec.partition("=")
        for workload in workloads:
            result = measure(name, Path(path), workload, args.repeats, args.timeout)
            if "error" in result:
                print(json.dumps(result), flush=True)
                continue
            result["median_s"] = statistics.median(result["samples"])
            result["per_s"] = result["units"] / result["median_s"]
            print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
