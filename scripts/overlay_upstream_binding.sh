#!/usr/bin/env bash
# Run this repo's Python layer (python/arrowsql) on top of upstream's
# compiled C++/ODBC binding, to validate Python-layer changes independently
# of the Rust core.
#
#   scripts/overlay_upstream_binding.sh [overlay-dir]   # prints the overlay dir
#
# Then run anything with the upstream interpreter and the overlay first on
# sys.path (no -I: PYTHONPATH must apply), e.g.
#   PYTHONPATH="$(scripts/overlay_upstream_binding.sh)" \
#     .venv-upstream/bin/python -m pytest tests/upstream/test_006_exceptions.py
#
# The overlay copies python/arrowsql and the python/mssql_python alias, minus any
# Rust build output, and adds
# the upstream wheel's loader (ddbc_bindings.py) and ddbc_bindings.cp3*.so files.
# The ODBC driver and mssql_py_core come from .venv-upstream as usual.
set -euo pipefail
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
main="$(git -C "$root" worktree list --porcelain | awk 'NR==1{print $2}')"
site="$(ls -d "$main"/.venv-upstream/lib/python3.*/site-packages | head -1)"
out="${1:-$root/.overlay-upstream}"

rm -rf "$out/mssql_python" "$out/arrowsql"
mkdir -p "$out"
cp -r "$root/python/arrowsql" "$out/arrowsql"
cp -r "$root/python/mssql_python" "$out/mssql_python"  # the import alias
find "$out/arrowsql" "$out/mssql_python" \( -name "__pycache__" -prune -o -name "ddbc_bindings*.so" \) -exec rm -rf {} +
cp "$site/mssql_python/ddbc_bindings.py" "$out/arrowsql/"
cp "$site"/mssql_python/ddbc_bindings.cp3*.so "$out/arrowsql/"
if [ -d "$site/mssql_python/libs" ]; then
  cp -r "$site/mssql_python/libs" "$out/arrowsql/"
fi
echo "$out"
