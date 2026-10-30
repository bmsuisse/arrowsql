"""Superseded by benchmarks/run.py (kept as a thin forwarder).

    python benchmarks/compare.py [--quick] [--only rust_port]   ==   run.py [--quick] [--driver ...] then report.py
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ALIASES = {"upstream_odbc": "mssql_python"}


def main() -> None:
    out = []
    it = iter(sys.argv[1:])
    for a in it:
        if a == "--only":
            name = next(it)
            out += ["--driver", ALIASES.get(name, name)]
        else:
            out.append(a)
    print("compare.py is superseded by run.py; forwarding.", file=sys.stderr)
    subprocess.run([sys.executable, str(HERE / "run.py"), *out], check=True)
    subprocess.run([sys.executable, str(HERE / "report.py")], check=True)


if __name__ == "__main__":
    main()
