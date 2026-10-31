#!/usr/bin/env python3
"""Run the vendored upstream suite and compare it with the passing baseline.

    DB_CONNECTION_STRING=... python scripts/check_upstream_baseline.py [pytest args]

Runs pytest in tests/upstream (with the interpreter running this script),
skipping everything listed in tests/upstream_excluded.txt, and exits 1 if any
test in tests/upstream_passing_baseline.txt did not pass. Tests outside the
baseline (they fail on upstream here too) are counted but never fail the run.
See docs/TEST_SCOPE.md.
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UPSTREAM = ROOT / "tests" / "upstream"


def excluded_ids() -> list[str]:
    ids = []
    for line in (ROOT / "tests" / "upstream_excluded.txt").read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            ids.append(line.split("\t", 1)[0].strip())
    return ids


def main() -> int:
    baseline = {
        line.strip()
        for line in (ROOT / "tests" / "upstream_passing_baseline.txt").read_text().splitlines()
        if line.strip()
    }
    excluded = excluded_ids()
    cmd = [sys.executable, "-m", "pytest", "-q", "-rA", "--tb=short", "--timeout", "120", "-p", "no:cacheprovider"]
    for entry in excluded:
        # A whole file is ignored; a node id (or prefix) is deselected.
        if "::" not in entry and entry.endswith(".py"):
            cmd.append(f"--ignore={entry}")
        else:
            cmd += ["--deselect", entry]
    cmd += sys.argv[1:]
    print("+", " ".join(cmd), flush=True)

    passed: set[str] = set()
    proc = subprocess.Popen(cmd, cwd=UPSTREAM, stdout=subprocess.PIPE, text=True, errors="replace")
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        match = re.match(r"^PASSED (\S.*)$", line.rstrip("\n"))
        if match:
            passed.add(match.group(1))
    proc.wait()

    def is_excluded(test_id: str) -> bool:
        return any(
            test_id == e or test_id.startswith(e + "::") or test_id.startswith(e + "[") for e in excluded
        )

    missing = sorted(t for t in baseline - passed if not is_excluded(t))
    print(
        f"\nbaseline: {len(baseline)}  passed: {len(passed)}  excluded entries: {len(excluded)}  "
        f"passing outside the baseline: {len(passed - baseline)}  baseline tests not passing: {len(missing)}"
    )
    for test_id in missing[:200]:
        print("  MISSING", test_id)
    if len(missing) > 200:
        print(f"  ... and {len(missing) - 200} more")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
