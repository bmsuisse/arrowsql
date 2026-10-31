#!/usr/bin/env bash
# Build the Rust extension into this worktree's venv.
# Builds are serialized machine-wide with flock: the box has little free RAM
# and all worktrees share one cargo target dir.
#   scripts/build.sh            dev build (default)
#   scripts/build.sh --release  optimized build (benchmarks); adds -z pack-relative-relocs
#                               (glibc >= 2.36), see scripts/release_rustflags.sh
set -euo pipefail
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
source "$root/scripts/env.sh" ""  # explicit arg: else it inherits ours
venv="$root/.venv"
if [ ! -x "$venv/bin/python" ]; then
  main="$(git -C "$root" worktree list --porcelain | awk 'NR==1{print $2}')"
  uv venv -q -p 3.13 "$venv"
  VIRTUAL_ENV="$venv" uv pip install -q -r "$main/requirements-dev.txt"
fi
cd "$root"
for arg in "$@"; do
  if [ "$arg" = "--release" ]; then source "$root/scripts/release_rustflags.sh"; fi
done
exec flock "$CARGO_TARGET_DIR.lock" env VIRTUAL_ENV="$venv" "$venv/bin/maturin" develop -q "$@"
