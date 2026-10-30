# Roadmap

Goal: blazing fast on every workload (rows, Arrow, async, sync concurrency,
executemany, bulk load) with a super lean package, at full upstream
compatibility.

## Package footprint (installed, Linux x86_64)

| | upstream mssql-python 1.16 | this project today | target |
|---|---:|---:|---:|
| driver | 9.4 MB (`mssql_python`) | 4.7 MB extension + 0.6 MB Python | ~2-3 MB |
| `mssql_py_core` (Rust TDS, via `mssql-python-rs`) | 30 MB | 30 MB (bulkcopy, async preview) | 0 (optional extra) |
| `mssql_python_odbc` (ODBC driver) | 7 MB | 0 | 0 |
| required runtime deps | 2 | 1 | **0** |

Plan: own bulk copy in the Rust core (drops `mssql_py_core` for bulkcopy), the
upstream async preview moves behind an extra, `pyarrow` and `azure-identity`
stay optional, one abi3 wheel per platform if the measured cost is nil,
stripped release builds.

## Wave S (speed + lean), after the wave-C/aio integration

0. pyo3 0.29 -> 0.30 (mechanical, before the parallel wave).
1. Native bulk copy: Arrow -> TDS encoder and row path behind the existing
   `cursor.bulkcopy` / `bulkcopy_arrow` API, plus `workers=N` parallel loads
   into a heap with TABLOCK. Target: beat `bcp`.
2. Sync path: drive each connection's I/O on the calling thread (no runtime
   hop), `DDBCSQLResetStmt` early return, cached `description`, 16 KB packets.
   Target: beat upstream under thread concurrency.
3. Fetch: per-column Decimal cache, small-int / short-string / datetime fast
   paths, two-phase decode (GIL-free decode, one attached build loop).
4. executemany as one multi-RPC message per batch. Target: beat pyodbc
   `fast_executemany`.
5. Lean packaging: zero required deps, extras `[arrow]`, `[azure]`,
   `[async-preview]`, release profile tuned for size and speed, abi3 decision
   backed by numbers, wheel-size check in CI.
6. Free-threaded CPython 3.15t build (`gil_used = false`) once the PyO3 audit
   is done.

## Wave D (compatibility): goal 100 %

Bar: **every** upstream target test passes (tests/upstream_passing_baseline.txt,
4,253 tests), plus the whole common-issues suite.

- Re-examine every entry of tests/upstream_excluded.txt: test_026_odbc_provider
  is Python-level provider-selection behavior and should pass with
  ProviderManager kept upstream-identical (report the selected provider, never
  load it). Only tests that inspect files the port cannot ship (bundled ODBC
  libraries in test_000_dependencies) may stay excluded, each with a reason.
- Remaining clusters: test_009_pooling, test_fetch_settings_cache,
  test_013 narrow-binding encoding, test_004 executemany OUTPUT, AsyncTest,
  plus common-issues failures (savepoints, dead connections, isolation levels,
  parameter typing / plan cache, TVPs, errors mid-result, decimal inference).
- Stretch: the 64 tests that fail on upstream itself in this environment
  (pooling socket-proxy tests, bulkcopy auth cleanup).

Each wave ends with an integration branch, both suites, a codex review
(bms + general) and the benchmark suite.
