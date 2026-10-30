#!/usr/bin/env python3
"""Benchmark runner: every (driver, workload) pair in a fresh `python -I` subprocess.

    python benchmarks/run.py --quick                       # whole matrix, small sizes
    python benchmarks/run.py --driver rust_port --driver mssql_python --workload 'read.taxi_100k.*'
    python benchmarks/run.py --list                        # drivers + workloads + who supports what

Needs only the stdlib (any python3). Results append to benchmarks/results/<timestamp>-<host>.jsonl,
one JSON object per line: a `header` record, then one `result` record per pair.
Render them with `python benchmarks/report.py`. See benchmarks/README.md.
"""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import common  # noqa: E402
from drivers.registry import SPECS, env_for, python_for  # noqa: E402

LOAD_WARN = 4.0


def meminfo_mb() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable"):
            return int(line.split()[1]) // 1024
    return -1


def git_info() -> dict:
    def g(*a):
        return subprocess.run(["git", "-C", str(common.ROOT), *a], capture_output=True, text=True).stdout.strip()

    return {"sha": g("rev-parse", "--short", "HEAD"), "branch": g("rev-parse", "--abbrev-ref", "HEAD"),
            "dirty": bool(g("status", "--porcelain", "--", "src", "python"))}


def child_env(spec, main: Path, pooled: bool = False) -> dict:
    env = dict(os.environ)
    env.update(env_for(spec, main, pooled))
    return env


def oom_first():
    """Make the benchmark child the OOM killer's first victim instead of other agents' processes."""
    try:
        Path("/proc/self/oom_score_adj").write_text("1000")
    except OSError:
        pass


def run_child(spec, main: Path, argv: list[str], timeout: int, pooled: bool = False):
    cmd = [str(python_for(spec, main)), "-I", str(HERE / "child.py"), "--driver", spec.name, *argv]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=child_env(spec, main, pooled),
                           preexec_fn=oom_first)
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "reason": f"exceeded {timeout}s"}
    for line in reversed(p.stdout.splitlines()):
        if line.startswith("BENCHRESULT "):
            return json.loads(line[len("BENCHRESULT "):])
    tail = (p.stderr or p.stdout).strip().splitlines()[-4:]
    sig = f"signal {-p.returncode}" if p.returncode < 0 else f"exit {p.returncode}"
    return {"status": "error", "reason": f"child died ({sig}): " + " | ".join(tail)[:400]}


def pick(names: list[str] | None, patterns: list[str] | None) -> list[str]:
    if not patterns:
        return names
    return [n for n in names if any(fnmatch.fnmatch(n, p) for p in patterns)]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--driver", action="append", help="driver name or glob (repeatable); default all")
    ap.add_argument("--workload", action="append", help="workload name or glob (repeatable); default all")
    ap.add_argument("--quick", action="store_true", help="shrink datasets/loops (3M->300k rows, ...), fewer repeats")
    ap.add_argument("--repeats", type=int, help="override measured repetitions per workload")
    ap.add_argument("--timeout", type=int, help="per-process timeout seconds (default 300 quick / 1800 full)")
    ap.add_argument("--out", help="output jsonl path (default benchmarks/results/<ts>-<host>.jsonl)")
    ap.add_argument("--list", action="store_true", help="list drivers and workloads, then exit")
    ap.add_argument("--no-prepare", action="store_true", help="skip dataset creation check")
    args = ap.parse_args()

    import workloads

    if args.list:
        for s in SPECS.values():
            print(f"{s.name:14} {sorted(s.caps)}  # {s.note}")
        for w in workloads.WORKLOADS:
            who = [n for n, s in SPECS.items() if w.requires <= s.caps]
            print(f"{w.name:46} {w.category:8} {','.join(who)}")
        return 0

    main_repo = common.main_repo()
    drivers = pick(list(SPECS), args.driver)
    wls = workloads.select(args.workload)
    if not drivers or not wls:
        sys.exit("nothing selected")

    common.RESULTS.mkdir(parents=True, exist_ok=True)
    lock = open(common.RESULTS / ".run.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("another benchmark run holds benchmarks/results/.run.lock; never run two heavy runs at once")

    load1 = os.getloadavg()[0]
    if load1 > LOAD_WARN:
        print(f"WARNING: load average {load1:.1f} > {LOAD_WARN}; results are noisy (indicative only)", file=sys.stderr)

    if not args.no_prepare:
        import datasets

        datasets.ensure()

    host = platform.node() or "host"
    out = Path(args.out) if args.out else common.RESULTS / f"{time.strftime('%Y%m%dT%H%M%S')}-{host}.jsonl"
    timeout = args.timeout or (300 if args.quick else 1800)

    # probe every selected driver once: version, connectivity, server version
    header = {"type": "header", "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "host": host,
              "cpus": os.cpu_count(), "mem_available_mb": meminfo_mb(), "loadavg": os.getloadavg(),
              "load_warning": load1 > LOAD_WARN, "git": git_info(), "quick": args.quick,
              "kernel": platform.release(), "drivers": {}, "server": None}
    live = []
    for name in drivers:
        spec = SPECS[name]
        py = python_for(spec, main_repo)
        if not py.exists():
            header["drivers"][name] = {"available": False, "reason": f"{py} missing (see setup_drivers.sh)"}
            print(f"skip {name}: {py} missing", file=sys.stderr)
            continue
        r = run_child(spec, main_repo, ["--probe"], 120)
        if r.get("version"):
            header["drivers"][name] = {"available": True, "version": r["version"], "python": r.get("python")}
            header["server"] = header["server"] or r.get("server")
            live.append(name)
        else:
            header["drivers"][name] = {"available": False, "reason": r.get("reason", "probe failed")}
            print(f"skip {name}: {r.get('reason')}", file=sys.stderr)

    with out.open("a") as fh:
        fh.write(json.dumps(header) + "\n")
        fh.flush()
        total = sum(1 for w in wls for n in live if w.requires <= SPECS[n].caps)
        done = 0
        for w in wls:  # workload-major: all drivers see the same machine state for a workload
            for name in live:
                spec = SPECS[name]
                if not w.requires <= spec.caps:
                    continue
                done += 1
                argv = ["--workload", w.name] + (["--quick"] if args.quick else [])
                if args.repeats:
                    argv += ["--repeats", str(args.repeats)]
                avail = meminfo_mb()
                rec = run_child(spec, main_repo, argv, timeout, w.pooled)
                rec.setdefault("driver", name)
                rec.setdefault("workload", w.name)
                rec.setdefault("category", w.category)
                rec.update(type="result", mem_available_mb=avail, loadavg1=round(os.getloadavg()[0], 2))
                fh.write(json.dumps(rec) + "\n")
                fh.flush()
                if rec["status"] == "ok":
                    msg = f"{rec['median_s']:.4f}s"
                else:
                    msg = f"{rec['status']}: {rec.get('reason', '')[:90]}"
                print(f"[{done}/{total}] {w.name:46} {name:13} {msg}", file=sys.stderr)
    print(f"wrote {out}", file=sys.stderr)
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
