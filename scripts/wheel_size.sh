#!/usr/bin/env bash
# Measure the release wheel: file size, what it bundles, installed footprint,
# required deps, and the modules `import mssql_python` pulls in.
#
#   scripts/wheel_size.sh [wheel.whl]
#
# Without an argument a release wheel is built into a scratch dir first
# (maturin build --release, auditwheel repair). The wheel is then installed
# alone into an empty venv, so only what the wheel ships is counted.
set -euo pipefail
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/wheel_size.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

if [ "${1:-}" ]; then
  wheel="$(realpath "$1")"
else
  cd "$root"
  "$root/.venv/bin/maturin" build -q --release -i "$root/.venv/bin/python" -o "$scratch/dist" >&2
  wheel="$(ls "$scratch"/dist/*.whl)"
fi

echo "wheel:           $(basename "$wheel")"
echo "wheel bytes:     $(stat -c %s "$wheel") ($(du -k "$wheel" | cut -f1) KiB)"

# Entries that make up the compressed wheel, largest first.
python3 -I - "$wheel" <<'EOF'
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1])
rows = sorted(z.infolist(), key=lambda i: i.compress_size, reverse=True)
print("largest entries (compressed / uncompressed bytes):")
for i in rows[:6]:
    print(f"  {i.compress_size:>9} / {i.file_size:>9}  {i.filename}")
bundled = [i for i in rows if ".libs/" in i.filename]
print(f"bundled shared libs (auditwheel): {sum(i.compress_size for i in bundled)} compressed bytes")
EOF

# Required deps come from the wheel's METADATA (Requires-Dist without an extra marker).
required="$(python3 -I -c '
import sys, zipfile
z = zipfile.ZipFile(sys.argv[1])
meta = [n for n in z.namelist() if n.endswith(".dist-info/METADATA")][0]
print("\n".join(l for l in z.read(meta).decode().splitlines()
                if l.startswith("Requires-Dist:") and "extra ==" not in l))
' "$wheel")"
echo "required deps:   ${required:-none}"

venv="$scratch/venv"
uv venv -q -p 3.13 "$venv"
uv pip install -q -p "$venv" --no-deps "$wheel"
site="$("$venv/bin/python" -I -c 'import site; print(site.getsitepackages()[0])')"
pkg_kib="$(du -sk "$site/mssql_python" | cut -f1)"
so_kib="$(du -k "$site"/mssql_python/*.so | cut -f1)"
libs_kib=0
if [ -d "$site/mssql_python_rs_port.libs" ]; then
  libs_kib="$(du -sk "$site/mssql_python_rs_port.libs" | cut -f1)"
fi
echo "installed KiB:   $((pkg_kib + libs_kib)) (mssql_python/ ${pkg_kib} + bundled .libs ${libs_kib})"
echo "installed .so:   ${so_kib} KiB"

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

# The test database lives in the main checkout's .env.local (see scripts/env.sh).
if [ -z "${DB_CONNECTION_STRING:-}" ]; then
  main="$(git -C "$root" worktree list --porcelain | awk 'NR==1{print $2}')"
  if [ -f "$main/.env.local" ]; then
    set -a; . "$main/.env.local"; set +a
  fi
fi

# With DB_CONNECTION_STRING set, run a query from the installed wheel alone and
# re-check which optional stacks it loaded.
if [ -n "${DB_CONNECTION_STRING:-}" ]; then
  "$venv/bin/python" -I -c '
import os, sys
import mssql_python
with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
    print("query result:", conn.cursor().execute("SELECT 1").fetchone()[0])
heavy = ["pyarrow", "azure", "mssql_py_core", "polars", "pandas", "numpy"]
loaded = sorted({m for m in sys.modules for h in heavy if m == h or m.startswith(h + ".")})
print("modules pulled by connect + query:", ", ".join(loaded) or "none")
'
fi
