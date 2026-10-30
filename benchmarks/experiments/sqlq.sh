#!/usr/bin/env bash
# Run T-SQL inside the mssql-rs container as sa: benchmarks/experiments/sqlq.sh "SELECT 1" [db]
set -euo pipefail
main="$(git -C "$(dirname "$0")" worktree list --porcelain | awk 'NR==1{print $2}')"
set -a; . "$main/.env.local"; set +a
exec podman exec mssql-rs /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -d "${2:-nyctaxi}" -W -s"|" -b -Q "$1"
