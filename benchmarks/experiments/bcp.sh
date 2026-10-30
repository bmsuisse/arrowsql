#!/usr/bin/env bash
# Time bcp inside the container (native format): the msodbcsql reference ceiling.
#   benchmarks/experiments/bcp.sh out "SELECT * FROM nyctaxi.dbo.yellow_trips" [extra bcp args]
#   benchmarks/experiments/bcp.sh in  nyctaxi.dbo.target_table [extra bcp args]   (reads /tmp/taxi.bcp)
set -euo pipefail
main="$(git -C "$(dirname "$0")" worktree list --porcelain | awk 'NR==1{print $2}')"
set -a; . "$main/.env.local"; set +a
mode="$1"; what="$2"; shift 2
if [ "$mode" = out ]; then
  cmd=(/opt/mssql-tools18/bin/bcp "$what" queryout /tmp/taxi.bcp -n)
else
  cmd=(/opt/mssql-tools18/bin/bcp "$what" in /tmp/taxi.bcp -n)
fi
start=$(date +%s.%N)
podman exec mssql-rs "${cmd[@]}" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -u "$@" | tail -3
end=$(date +%s.%N)
echo "wall_s=$(echo "$end - $start" | bc)"
podman exec mssql-rs ls -l /tmp/taxi.bcp
