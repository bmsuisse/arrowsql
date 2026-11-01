#!/usr/bin/env bash
# Reproduce the README benchmark with one command (Linux x86_64; needs docker or podman, uv, a Rust toolchain).
# Idempotent: every step checks whether its result already exists, so re-running only does the missing work.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/bench_reproduce.sh [options] [-- extra args for benchmarks/run.py]

Sets up SQL Server, the NYC taxi data and one uv venv per driver, runs benchmarks/run.py, then
benchmarks/report.py. Safe to re-run; finished steps are skipped.

Options:
  --quick               reduced sizes and repeats (~20 min instead of hours); numbers are indicative only
  --drivers a,b         only these drivers (run.py --driver, comma separated; globs allowed)
  --workloads 'glob'    only matching workloads (run.py --workload; repeatable), e.g. 'read.taxi_100k.*'
  --no-docker           do not start a container; use DB_CONNECTION_STRING / .env.local as is
  --skip-setup          skip venvs, ODBC and data download/load; just run and report
  --dry-run             print what would run (and what would be written) without changing anything
  -h, --help            this text

Environment:
  DB_CONNECTION_STRING  use this server instead of starting one (also read from <main>/.env.local)
  BENCH_CONTAINER       container name (default arrowsql-bench; the harness uses it for sqlcmd/bcp)
  BENCH_CONTAINER_ENGINE  docker or podman (default: docker if installed, else podman)
  BENCH_MAIN            primary checkout holding .env.local, venvs, .bench-odbc (default: first `git worktree list` entry)
  BCP_HOST_BIN          dir with host-side sqlcmd/bcp; needed with --no-docker against a server that is not
                        in a local container (the harness otherwise runs sqlcmd via `<engine> exec`)

Results: benchmarks/results/*.jsonl, benchmarks/RESULTS.md, benchmarks/results/report.html
EOF
}

QUICK=0 NO_DOCKER=0 SKIP_SETUP=0 DRY=0
DRIVERS="" WORKLOADS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --quick) QUICK=1 ;;
    --drivers) DRIVERS="${2:?--drivers needs a value}"; shift ;;
    --workloads) WORKLOADS+=("${2:?--workloads needs a value}"); shift ;;
    --no-docker) NO_DOCKER=1 ;;
    --skip-setup) SKIP_SETUP=1 ;;
    --dry-run) DRY=1 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done
EXTRA_ARGS=("$@")

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAIN="${BENCH_MAIN:-$(git -C "$ROOT" worktree list --porcelain | awk 'NR==1{print $2}')}"
export BENCH_MAIN="$MAIN"
DB_CONNECTION_STRING="${DB_CONNECTION_STRING:-}"
PY_VERSION=3.13
IMAGE=mcr.microsoft.com/mssql/server:2022-latest
PARQUET_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
PARQUET="$ROOT/data/yellow_tripdata_2024-01.parquet"
PARQUET_ROWS=2964624
ODBC_DIR="$MAIN/.bench-odbc"

export BENCH_CONTAINER="${BENCH_CONTAINER:-arrowsql-bench}"
if [ -z "${BENCH_CONTAINER_ENGINE:-}" ]; then
  if command -v docker >/dev/null; then BENCH_CONTAINER_ENGINE=docker; else BENCH_CONTAINER_ENGINE=podman; fi
fi
export BENCH_CONTAINER_ENGINE

log() { printf '==> %s\n' "$*"; }
# Run a command, or only print it with --dry-run.
run() {
  if [ "$DRY" = 1 ]; then printf '+ %s\n' "$*"; else "$@"; fi
}

# ---------------------------------------------------------------------------------------------
# 1. SQL Server
# ---------------------------------------------------------------------------------------------
read_env_local() {  # sets DB_CONNECTION_STRING from .env.local if not already set; never prints it
  [ -n "${DB_CONNECTION_STRING:-}" ] && return 0
  [ -f "$MAIN/.env.local" ] || return 0
  local line
  line="$(grep -E '^(export )?DB_CONNECTION_STRING=' "$MAIN/.env.local" | head -n1 || true)"
  [ -n "$line" ] || return 0
  line="${line#export }"; line="${line#DB_CONNECTION_STRING=}"
  line="${line%\"}"; line="${line#\"}"; line="${line%\'}"; line="${line#\'}"
  DB_CONNECTION_STRING="$line"
}

random_password() {  # meets SQL Server complexity (upper, lower, digit, symbol); hex tail has no ; or =
  echo "Aa1-$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
}

container_state() { "$BENCH_CONTAINER_ENGINE" inspect -f '{{.State.Running}}' "$BENCH_CONTAINER" 2>/dev/null || echo missing; }

wait_for_sql() {  # password travels via the environment, not argv
  local pw=$1 i
  for i in $(seq 1 60); do
    if SQLCMDPASSWORD="$pw" "$BENCH_CONTAINER_ENGINE" exec -e SQLCMDPASSWORD "$BENCH_CONTAINER" \
        /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -Q "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "SQL Server in $BENCH_CONTAINER did not become ready within 120 s" >&2
  return 1
}

setup_server() {
  read_env_local
  if [ -n "${DB_CONNECTION_STRING:-}" ]; then
    log "using DB_CONNECTION_STRING from the environment/.env.local (no container started)"
    return 0
  fi
  if [ "$NO_DOCKER" = 1 ]; then
    echo "--no-docker given but DB_CONNECTION_STRING is not set and $MAIN/.env.local has none" >&2
    exit 1
  fi
  command -v "$BENCH_CONTAINER_ENGINE" >/dev/null || { echo "neither docker nor podman found" >&2; exit 1; }

  if [ ! -f "$MAIN/.env.local" ] || ! grep -q '^DB_CONNECTION_STRING=' "$MAIN/.env.local"; then
    log "generating SA password and writing DB_CONNECTION_STRING to $MAIN/.env.local (mode 600)"
    if [ "$DRY" = 1 ]; then
      echo "+ (would append DB_CONNECTION_STRING=Server=localhost,1433;...;Pwd=<random> to .env.local)"
      return 0
    fi
    local pw; pw="$(random_password)"
    ( umask 077
      printf 'DB_CONNECTION_STRING="Server=localhost,1433;Database=master;Uid=sa;Pwd=%s;Encrypt=yes;TrustServerCertificate=yes"\n' "$pw" \
        >> "$MAIN/.env.local" )
    chmod 600 "$MAIN/.env.local"
  fi
  DB_CONNECTION_STRING=""; read_env_local
  [ -n "$DB_CONNECTION_STRING" ] || { echo ".env.local has no usable DB_CONNECTION_STRING" >&2; exit 1; }
  local pw
  pw="$(printf '%s' "$DB_CONNECTION_STRING" | tr ';' '\n' | sed -n 's/^[Pp]wd=//p;s/^[Pp]assword=//p' | head -n1)"

  local state; state="$(container_state)"
  if [ "$state" = true ]; then
    log "container $BENCH_CONTAINER already running"
  elif [ "$state" = false ]; then
    log "starting existing container $BENCH_CONTAINER"
    run "$BENCH_CONTAINER_ENGINE" start "$BENCH_CONTAINER"
  else
    log "creating container $BENCH_CONTAINER ($IMAGE) on port 1433"
    if [ "$DRY" = 1 ]; then
      echo "+ MSSQL_SA_PASSWORD=<from .env.local> $BENCH_CONTAINER_ENGINE run -d --name $BENCH_CONTAINER -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD -p 1433:1433 $IMAGE"
    else
      MSSQL_SA_PASSWORD="$pw" "$BENCH_CONTAINER_ENGINE" run -d --name "$BENCH_CONTAINER" \
        -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD -p 1433:1433 "$IMAGE" >/dev/null
    fi
  fi
  if [ "$DRY" = 1 ]; then echo "+ (would wait for SQL Server readiness in $BENCH_CONTAINER)"; else
    log "waiting for SQL Server"; wait_for_sql "$pw"
  fi
}

# ---------------------------------------------------------------------------------------------
# 2. Venvs (versions pinned to what produced the README numbers)
# ---------------------------------------------------------------------------------------------
mkvenv() {  # mkvenv <dir> <pkg>...
  local dir=$1; shift
  [ -x "$dir/bin/python" ] || run uv venv -q --python "$PY_VERSION" "$dir"
  run uv pip install -q -p "$dir/bin/python" "$@"
}

setup_venvs() {
  log "venvs under $MAIN"
  mkvenv "$MAIN/.venv-upstream" mssql-python==1.16.0 mssql-python-odbc==18.6.2.2 pyarrow==25.0.1 duckdb==2.0.0
  mkvenv "$MAIN/.venv-bench-pyodbc" pyodbc==5.3.0 pyarrow==26.0.0 duckdb==2.0.0
  mkvenv "$MAIN/.venv-bench-connectorx" connectorx==0.4.6 pyarrow==26.0.0 duckdb==2.0.0 pandas==3.1.0 numpy==2.5.4
  mkvenv "$MAIN/.venv-bench-arrow-odbc" arrow-odbc==10.6.3 pyarrow==26.0.0 duckdb==2.0.0
  mkvenv "$MAIN/.venv-bench-pymssql" pymssql==2.4.6 pyarrow==26.0.0
  mkvenv "$MAIN/.venv-fastmssql" fastmssql==0.7.7 pyarrow==26.0.0

  log "building this repo's release wheel into $ROOT/.venv"
  run "$ROOT/scripts/build.sh" --release
  run uv pip install -q -p "$ROOT/.venv/bin/python" pyarrow==25.0.1 duckdb==2.0.0
}

# pyodbc/arrow-odbc need libodbc.so.2; run.py points them at $MAIN/.bench-odbc (LD_LIBRARY_PATH + ODBCSYSINI).
# We always register the msodbcsql18 driver bundled in the upstream wheel there, so no system msodbcsql18 is needed.
setup_odbc() {
  if [ -f "$ODBC_DIR/etc/odbcinst.ini" ] && [ -f "$ODBC_DIR/etc-pool/odbcinst.ini" ] &&
     { [ -e "$ODBC_DIR/lib/libodbc.so.2" ] || ldconfig -p 2>/dev/null | grep -q 'libodbc\.so\.2'; }; then
    log "ODBC shim already in place ($ODBC_DIR)"
    return 0
  fi
  log "setting up unixODBC shim in $ODBC_DIR"
  if [ "$DRY" = 1 ]; then
    echo "+ (would use system libodbc.so.2 if present, else apt-get download libodbc2 libodbcinst2 + dpkg -x into $ODBC_DIR/lib)"
    echo "+ (would write $ODBC_DIR/etc{,-pool}/odbcinst.ini pointing at libmsodbcsql-18 from .venv-upstream)"
    return 0
  fi
  mkdir -p "$ODBC_DIR/lib" "$ODBC_DIR/etc" "$ODBC_DIR/etc-pool"
  if ! ldconfig -p 2>/dev/null | grep -q 'libodbc\.so\.2' && [ ! -e "$ODBC_DIR/lib/libodbc.so.2" ]; then
    if command -v apt-get >/dev/null && command -v dpkg >/dev/null; then
      local tmp; tmp="$(mktemp -d)"
      ( cd "$tmp" && apt-get download libodbc2 libodbcinst2 >/dev/null 2>&1 \
          && for d in ./*.deb; do dpkg -x "$d" ./x; done \
          && cp -a ./x/usr/lib/*/libodbc*.so* "$ODBC_DIR/lib/" ) \
        || echo "WARNING: could not fetch unixODBC (apt-get download failed); pyodbc/arrow_odbc will be skipped by run.py" >&2
      rm -rf "$tmp"
    else
      echo "WARNING: no libodbc.so.2 and no apt-get/dpkg; install unixODBC (e.g. apt install unixodbc). pyodbc/arrow_odbc will be skipped" >&2
    fi
  fi
  local drv
  drv="$(find "$MAIN/.venv-upstream/lib" -path '*mssql_python_odbc/libs/linux/*' -name 'libmsodbcsql-18*.so*' 2>/dev/null | head -n1)"
  if [ -z "$drv" ]; then
    echo "WARNING: bundled msodbcsql18 not found in .venv-upstream; pyodbc/arrow_odbc will be skipped" >&2
    return 0
  fi
  local suffix
  for suffix in "etc:No" "etc-pool:Yes"; do
    {
      printf '[ODBC Driver 18 for SQL Server]\nDescription=msodbcsql18 bundled with mssql-python\nDriver=%s\nUsageCount=1\n\n[ODBC]\nPooling=%s\n' \
        "$drv" "${suffix#*:}"
      [ "${suffix#*:}" = Yes ] && printf 'CPTimeout=120\n'
    } > "$ODBC_DIR/${suffix%%:*}/odbcinst.ini"
  done
}

# ---------------------------------------------------------------------------------------------
# 3. NYC taxi data
# ---------------------------------------------------------------------------------------------
UPSTREAM_PY="$MAIN/.venv-upstream/bin/python"

parquet_ok() {
  [ -f "$PARQUET" ] || return 1
  [ "$DRY" = 1 ] && return 0  # dry-run: never open files/servers; assume the existing file is fine
  [ -x "$UPSTREAM_PY" ] || return 1
  "$UPSTREAM_PY" -I - "$PARQUET" "$PARQUET_ROWS" <<'EOF' 2>/dev/null
import sys, pyarrow.parquet as pq
sys.exit(0 if pq.ParquetFile(sys.argv[1]).metadata.num_rows == int(sys.argv[2]) else 1)
EOF
}

taxi_loaded() {
  [ "$DRY" = 1 ] && { echo "+ (would count rows in nyctaxi.dbo.yellow_trips; load only if != $PARQUET_ROWS)"; return 1; }
  [ -x "$UPSTREAM_PY" ] || return 1
  DB_CONNECTION_STRING="$DB_CONNECTION_STRING" "$UPSTREAM_PY" -I - "$PARQUET_ROWS" <<'EOF' 2>/dev/null
import os, sys
from mssql_python import connect
cs = os.environ["DB_CONNECTION_STRING"].replace("Database=master", "Database=nyctaxi")
c = connect(cs, autocommit=True).cursor()
c.execute("SELECT COUNT_BIG(*) FROM dbo.yellow_trips")
sys.exit(0 if c.fetchone()[0] == int(sys.argv[1]) else 1)
EOF
}

setup_data() {
  if parquet_ok; then
    log "parquet present and verified ($PARQUET_ROWS rows)"
  else
    log "downloading $PARQUET_URL"
    run mkdir -p "$ROOT/data"
    run rm -f "$PARQUET.part"
    run curl -fL --retry 3 -o "$PARQUET.part" "$PARQUET_URL"
    run mv "$PARQUET.part" "$PARQUET"
    if [ "$DRY" = 0 ] && ! parquet_ok; then
      echo "downloaded parquet does not have $PARQUET_ROWS rows; removing it" >&2
      rm -f "$PARQUET"; exit 1
    fi
  fi
  if taxi_loaded; then
    log "nyctaxi.dbo.yellow_trips already loaded ($PARQUET_ROWS rows)"
  else
    log "loading taxi data into nyctaxi (a few minutes)"
    DB_CONNECTION_STRING="$DB_CONNECTION_STRING" run "$UPSTREAM_PY" "$ROOT/scripts/load_taxi.py" "$PARQUET"
  fi
}

# ---------------------------------------------------------------------------------------------
# 4. Run
# ---------------------------------------------------------------------------------------------
run_benchmark() {
  local args=()
  [ "$QUICK" = 1 ] && args+=(--quick)
  if [ -n "$DRIVERS" ]; then
    local d; IFS=, read -ra ds <<< "$DRIVERS"
    for d in "${ds[@]}"; do args+=(--driver "$d"); done
  fi
  local w
  for w in "${WORKLOADS[@]+"${WORKLOADS[@]}"}"; do args+=(--workload "$w"); done
  args+=("${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}")

  # a worktree benchmarks its own release build; in the main checkout this is the same path as the default
  export RUST_PORT_VENV="$ROOT/.venv"
  export DB_CONNECTION_STRING
  log "running benchmarks/run.py ${args[*]:-}"
  run python3 "$ROOT/benchmarks/run.py" "${args[@]+"${args[@]}"}"
  log "building report"
  run python3 "$ROOT/benchmarks/report.py"
  echo
  echo "Results:"
  echo "  raw samples  $ROOT/benchmarks/results/*.jsonl"
  echo "  summary      $ROOT/benchmarks/RESULTS.md"
  echo "  html report  $ROOT/benchmarks/results/report.html"
}

setup_server
if [ "$SKIP_SETUP" = 1 ]; then
  log "--skip-setup: not touching venvs, ODBC or data"
else
  setup_venvs
  setup_odbc
  setup_data
fi
run_benchmark
