# Rules for agents working in this repository

1. **Version trio.** Three different numbers: `pyproject.toml` `version` and `Cargo.toml`
   `version` are the distribution release and must be bumped together (plus a CHANGELOG
   entry). `arrowsql.__version__` (`python/arrowsql/__init__.py`) deliberately reports the
   upstream `mssql-python` release the Python layer is vendored from (currently 1.16.0);
   do not bump it for a release of this project. `ddbc_bindings.__version__` is the Rust
   binding contract version (`docs/CONTRACT.md`).
2. **Vendor patches.** Every change to vendored Python (upstream-derived files) is marked
   `RUST-PORT` in the code and listed in `docs/PYTHON_LAYER_CHANGES.md`. Every change to
   `vendor/mssql-tds` gets an entry in `vendor/mssql-tds/VENDOR.md`.
3. **Upstream baseline gate.** `tests/upstream_passing_baseline.txt` lists upstream tests
   that must keep passing. Run `scripts/check_upstream_baseline.py` (or the baseline
   snippet from the PR template) before pushing; 0 baseline tests may be missing and
   teardown ERRORs count as failures. Never edit the baseline or `upstream_excluded.txt`
   to hide a regression.
4. **Stay in Rust.** Put new behaviour in the Rust core (`src/`) and keep the Python layer
   thin. Python is for the DB-API surface, argument validation and glue.
5. **Tests.** Each fix gets a regression test in `tests/common_issues` (or `tests/port`,
   `tests/mock`). Run tests with `python scripts/run_tests.py <id> <paths>`, which uses a
   private database per id. Full suites run in CI.
6. **Secrets.** Never print, cat or commit `.env.local` or the SA password; connection
   strings in logs and error messages must not contain values. Check
   `git log -p origin/main..HEAD` for the password before every push.
7. **Git.** Never push to `main`; work on a branch and open a PR. Do not restart
   containers or touch other sessions' processes. Use `nice -n 19` for builds and tests
   while benchmarks run.
