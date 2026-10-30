# Test scope

## Upstream suite (tests/upstream, vendored at tests/upstream/UPSTREAM_COMMIT)

Baseline: the PyPI `mssql-python` 1.16.0 wheel (C++ + ODBC Driver 18) against
the local podman SQL Server 2022 container: **4253 passed, 64 failed, 114
skipped**. Targets = the 4253 tests listed in tests/upstream_passing_baseline.txt.
Tests failing on upstream in this environment (pooling socket-proxy tests,
bulkcopy auth cleanup, mssql-odbc daily validation, release packaging) are not
targets.

Within the targets, tests of ODBC/C++/packaging internals that have no meaning
for a native-TDS Rust core are excluded, each with a reason, in
tests/upstream_excluded.txt (filled in as triage proceeds; every entry needs a
one-line justification).

## Common-issues suite (tests/common_issues)

Written for this project; ground truth is the upstream ODBC driver. Tests
marked `upstream_bug` assert correct behavior that upstream gets wrong
(xfail strict on upstream; must pass on the port).

## Running

    source scripts/env.sh <your-id>
    cd tests/upstream && ../../.venv/bin/python -m pytest -q --timeout 120 <file>
