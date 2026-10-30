#!/usr/bin/env bash
# Measure the release wheel: file size, installed footprint, and required deps.
#
#   scripts/wheel_size.sh [wheel.whl]
#
# Without an argument a release wheel is built into a scratch dir first (maturin
# build --release). The wheel is then installed alone, with --no-deps plus an
# empty venv, so only what the wheel ships is counted. Finally `import
# mssql_python` runs in that venv and reports which optional stacks it pulled in.
set -euo pipefail
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/wheel_size.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

if [ "${1:-}" ]; then
  wheel="$(realpath "$1")"
else
  cd "$root"
  "$root/.venv/bin/maturin" build -q --release -o "$scratch/dist" >&2
  wheel="$(ls "$scratch"/dist/*.whl)"
fi

echo "wheel:           $(basename "$wheel")"
echo "wheel bytes:     $(stat -c %s "$wheel") ($(du -k "$wheel" | cut -f1) KiB)"

# Required deps come from the wheel's METADATA (Requires-Dist without an extra marker).
required="$(unzip -p "$wheel" '*.dist-info/METADATA' | grep '^Requires-Dist:' | grep -v 'extra ==' || true)"
echo "required deps:   ${required:-none}"

venv="$scratch/venv"
uv venv -q -p 3.13 "$venv"
uv pip install -q -p "$venv" --no-deps "$wheel"
site="$("$venv/bin/python" -c 'import site; print(site.getsitepackages()[0])')"
installed_kib="$(du -sk "$site/mssql_python" | cut -f1)"
echo "installed KiB:   ${installed_kib} (mssql_python/ package incl. native .so)"
echo "installed .so:   $(du -k "$site"/mssql_python/*.so | cut -f1) KiB"

# Only the wheel itself is installed; list any optional stack that import pulls in.
# (cd to a neutral dir so the source tree is not on sys.path.)
cd "$scratch"
"$venv/bin/python" -I -c '
import sys
import mssql_python  # noqa: F401
heavy = ["pyarrow", "azure", "mssql_py_core", "polars", "pandas", "numpy"]
loaded = sorted({m for m in sys.modules for h in heavy if m == h or m.startswith(h + ".")})
print("modules pulled by import mssql_python:", ", ".join(loaded) or "none")
'
