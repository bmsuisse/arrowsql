"""Run ONE (driver, workload) pair in this process and print a `BENCHRESULT {json}` line.

Invoked by run.py as `<venv>/bin/python -I benchmarks/child.py ...`; not meant for direct use
(though `--probe` is handy to check that a driver imports and connects).
"""

from __future__ import annotations

import argparse
import gc
import importlib
import json
import resource
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # -I removes the script dir from sys.path

from common import DATASETS_JSON, ConnInfo, summarize  # noqa: E402
from drivers.base import Unsupported  # noqa: E402
from drivers.registry import SPECS  # noqa: E402


def maxrss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # Linux: KiB


def emit(rec: dict) -> None:
    print("BENCHRESULT " + json.dumps(rec), flush=True)


def load_driver(name: str):
    spec = SPECS[name]
    mod = importlib.import_module(f"drivers.{spec.module}")
    return getattr(mod, spec.cls)


def probe(name: str) -> None:
    cls = load_driver(name)
    d = cls(ConnInfo.from_env())
    d.connect()
    server = None
    if "rows" in d.supports or name == "bcp":
        try:
            server = str(d.scalar("SELECT @@VERSION")).splitlines()[0][:90]
        except Exception:
            server = None
    d.close()
    emit({"driver": name, "version": cls.version(), "server": server, "python": sys.version.split()[0]})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--driver", required=True)
    ap.add_argument("--workload")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--repeats", type=int)
    ap.add_argument("--probe", action="store_true")
    args = ap.parse_args()

    if args.probe:
        probe(args.driver)
        return

    import workloads

    wl = workloads.get(args.workload)
    rec = {"driver": args.driver, "workload": wl.name, "category": wl.category, "quick": args.quick,
           "unit": wl.unit, "note": wl.note}
    datasets_info = json.loads(DATASETS_JSON.read_text()) if DATASETS_JSON.exists() else {}
    ctx = workloads.Ctx(args.quick, datasets_info)
    cls = load_driver(args.driver)
    d = cls(ConnInfo.from_env(), pooled=wl.pooled)
    ran = False
    try:
        d.connect()
        if wl.prepare:
            wl.prepare(d, ctx)
        repeats = args.repeats or (wl.quick_repeats if args.quick else wl.repeats)
        warmup = wl.warmup
        rss_base = maxrss_mb()
        wall, cpu, units = [], [], 0
        for i in range(warmup + repeats):
            if wl.setup:
                wl.setup(d, ctx)
            gc.collect()
            c0, t0 = time.process_time(), time.perf_counter()
            units = wl.run(d, ctx)
            t1, c1 = time.perf_counter(), time.process_time()
            if i >= warmup:
                wall.append(t1 - t0)
                cpu.append(c1 - c0)
            ran = True
        if wl.verify:
            wl.verify(d, ctx, units)
        stats = summarize(wall)
        bpr = datasets_info.get(wl.dataset, {}).get("bytes_per_row") if wl.dataset else None
        rec.update(status="ok", samples=[round(x, 6) for x in wall], units=units, repeats=repeats, warmup=warmup,
                   median_s=stats["median"], min_s=stats["min"], p95_s=stats["p95"],
                   cpu_median_s=summarize(cpu)["median"], rss_base_mb=round(rss_base, 1),
                   rss_peak_mb=round(maxrss_mb(), 1),
                   units_per_s=units / stats["median"] if stats["median"] else None,
                   mb_per_s=(units * bpr / 1e6 / stats["median"]) if bpr and stats["median"] else None)
        if args.driver == "bcp":
            rec["cpu_median_s"] = None  # work happens in the container, not in this process
    except Unsupported as exc:
        rec.update(status="unsupported", reason=str(exc) or "not supported by adapter")
    except Exception as exc:  # noqa: BLE001 - every failure becomes a record
        tb = traceback.format_exc().strip().splitlines()
        rec.update(status="error", reason=f"{type(exc).__name__}: {exc}"[:400], traceback="\n".join(tb[-6:]))
    finally:
        try:
            if wl.teardown and d.conn is not None:
                wl.teardown(d, ctx)
        except Exception:  # noqa: BLE001
            pass
        try:
            d.close()
        except Exception:  # noqa: BLE001
            pass
    emit(rec)


if __name__ == "__main__":
    main()
