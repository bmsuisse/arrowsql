"""Run bench_taxi.py for every driver and print a comparison table.

    python benchmarks/compare.py [--quick] [--only rust_port]

Drivers and their interpreters:
    upstream_odbc  .venv-upstream  (mssql-python wheel from PyPI, C++ + ODBC)
    rust_port      .venv           (this repo; build with scripts/build.sh --release first)
    fastmssql      .venv-fastmssql (FastMssql from PyPI, tiberius, async)
Results land in benchmarks/results/<driver>.json; existing files are reused
for drivers not selected with --only, so the baseline only runs once.
"""

import argparse
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
RESULTS = ROOT / "benchmarks" / "results"
DRIVERS = {
    "upstream_odbc": (".venv-upstream", "mssql_python"),
    "rust_port": (".venv", "mssql_python"),
    "fastmssql": (".venv-fastmssql", "fastmssql"),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--only", action="append", choices=list(DRIVERS))
    args = parser.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)

    for name, (venv, driver) in DRIVERS.items():
        out = RESULTS / f"{name}.json"
        if args.only and name not in args.only:
            continue
        python = ROOT / venv / "bin" / "python"
        if not python.exists():
            print(f"skip {name}: {python} missing", file=sys.stderr)
            continue
        cmd = [str(python), "-I", str(ROOT / "benchmarks" / "bench_taxi.py"), "--driver", driver]
        cmd += ["--out", str(out)] + (["--quick"] if args.quick else [])
        print(f"running {name} ...", file=sys.stderr)
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

    results = {
        p.stem: json.loads(p.read_text()) for p in sorted(RESULTS.glob("*.json"))
    }
    workloads = sorted({w for r in results.values() for w in r})
    names = list(results)
    print(f"{'workload':28}" + "".join(f"{n:>16}" for n in names) + f"{'port vs odbc':>14}")
    for w in workloads:
        cells = [results[n].get(w, {}).get("median_s") for n in names]
        line = f"{w:28}" + "".join(f"{c:>15.3f}s" if c else f"{'-':>16}" for c in cells)
        base = results.get("upstream_odbc", {}).get(w, {}).get("median_s")
        port = results.get("rust_port", {}).get(w, {}).get("median_s")
        line += f"{base / port:>13.2f}x" if base and port else f"{'-':>14}"
        print(line)


if __name__ == "__main__":
    main()
