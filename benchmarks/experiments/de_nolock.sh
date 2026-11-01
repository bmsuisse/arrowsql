#!/usr/bin/env bash
# Like de.sh but without the bench lock (functional repros only, never timings).
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
source scripts/env.sh de
exec "${PY:-.venv/bin/python}" "$@"
