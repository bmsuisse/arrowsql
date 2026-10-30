#!/usr/bin/env bash
# Run the Rust TDS probe with credentials from the main checkout's .env.local.
#   benchmarks/experiments/run_probe.sh read --sink noop --encrypt off
set -euo pipefail
main="$(git -C "$(dirname "$0")" worktree list --porcelain | awk 'NR==1{print $2}')"
set -a; . "$main/.env.local"; set +a
exec "${CARGO_TARGET_DIR:-$HOME/devops/.cargo-target-mssql-rs}/release/tds_probe" "$@"
