#!/usr/bin/env bash
# Run two codex reviews of <base>..HEAD in the background:
#   reviews/<label>-bms.md      bms-code-review skill (full mode)
#   reviews/<label>-general.md  plain general code review
# Usage: scripts/codex_review.sh <base-ref> <label>
# Each output file is written only when its run finishes; logs go next to it.
set -euo pipefail
root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
base="$1"; label="$2"; out="$root/reviews"
mkdir -p "$out"
context="Repo: Rust (PyO3 + Microsoft mssql-tds) drop-in replacement for mssql-python's C++/ODBC ddbc_bindings. Read docs/ARCHITECTURE.md first. Goals: pass the upstream behavioral test suite and be much faster than upstream. Review the diff $base..HEAD only."
nohup codex exec -C "$root" -s read-only --skip-git-repo-check -o "$out/$label-bms.md" \
  "Use the bms-code-review skill (full mode, --full) on the diff $base..HEAD. $context Review only: do not modify files or commit. Output a severity-ranked findings list with file:line." \
  > "$out/$label-bms.log" 2>&1 &
nohup codex exec -C "$root" -s read-only --skip-git-repo-check -o "$out/$label-general.md" \
  "Do a general code review of the diff $base..HEAD: correctness, memory safety around PyO3/GIL and unsafe, concurrency and deadlocks, error handling, performance regressions, test quality. $context Review only: do not modify files or commit. Output a severity-ranked findings list with file:line." \
  > "$out/$label-general.log" 2>&1 &
echo "codex reviews started: $out/$label-{bms,general}.md"
