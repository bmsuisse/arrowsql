# Source me: shared environment for builds/tests in this repo and its worktrees.
#   source scripts/env.sh [agent-id]
# With an agent id, DB_CONNECTION_STRING points at a private database
# (rs_agent_<id>) so concurrent agents don't clobber each other's tables.
_root="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
_main="$(git -C "$_root" worktree list --porcelain | awk 'NR==1{print $2}')"
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/devops/.cargo-target-mssql-rs"
set -a; . "$_main/.env.local"; set +a
if [ -n "$1" ]; then
  _db="rs_agent_$(echo "$1" | tr -c 'A-Za-z0-9_\n' '_')"
  # sqlcmd in the container, or over TDS with this repo's driver when podman
  # cannot exec into it (its state DB can drift from the running container).
  podman exec mssql-rs /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -b -Q \
    "IF DB_ID('$_db') IS NULL CREATE DATABASE [$_db]" >/dev/null 2>&1 \
    || RS_DB="$_db" "$_main/.venv/bin/python" -I -c '
import os, mssql_python
db = os.environ["RS_DB"]
conn = mssql_python.connect(os.environ["DB_CONNECTION_STRING"], autocommit=True)
conn.cursor().execute(f"IF DB_ID(N\x27{db}\x27) IS NULL CREATE DATABASE [{db}]")
' >/dev/null
  export DB_CONNECTION_STRING="${DB_CONNECTION_STRING/Database=master/Database=$_db}"
fi
unset _root _main _db
