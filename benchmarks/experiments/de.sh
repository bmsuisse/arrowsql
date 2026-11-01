#!/usr/bin/env bash
# flock-wrapped benchmark runner: de.sh <script.py args...>  (env: private DB rs_agent_de)
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
source scripts/env.sh de
exec flock /tmp/claude-1006/-home-dominikaiuser-devops/029c60c3-79eb-4b6a-aa39-a1c105102018/scratchpad/codex-tds/bench.lock \
  .venv/bin/python "$@"
