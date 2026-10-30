#!/usr/bin/env bash
# Run a Python experiment with the upstream wheel venv and repo credentials.
#   benchmarks/experiments/run_py.sh upstream_reads.py arrow3m [args...]
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
main="$(git -C "$here" worktree list --porcelain | awk 'NR==1{print $2}')"
set -a; . "$main/.env.local"; set +a
script="$1"; shift
exec "${PYTHON:-$main/.venv-upstream/bin/python}" -I "$here/$script" "$@"
