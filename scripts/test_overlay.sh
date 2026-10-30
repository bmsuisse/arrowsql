#!/usr/bin/env bash
# pytest this repo's Python layer on upstream's C++/ODBC binding (see
# overlay_upstream_binding.sh), against a private database.
#   scripts/test_overlay.sh <db-id> <pytest args...>   # paths relative to the repo root
set -euo pipefail
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
main="$(git -C "$root" worktree list --porcelain | awk 'NR==1{print $2}')"
db_id="$1"; shift
source "$root/scripts/env.sh" "$db_id"
overlay="$("$root/scripts/overlay_upstream_binding.sh")"
cd "$root"
PYTHONPATH="$overlay" exec "$main/.venv-upstream/bin/python" -m pytest -p no:cacheprovider "$@"
