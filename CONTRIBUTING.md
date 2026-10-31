# Contributing

## Setup

You need Rust (stable, 1.90+), Python 3.10+ and a SQL Server 2022 you can write to
(`mcr.microsoft.com/mssql/server:2022-latest` in docker or podman is fine).

```bash
uv venv -p 3.13 .venv && VIRTUAL_ENV=.venv uv pip install -r requirements-dev.txt
scripts/build.sh                 # dev build into .venv; --release for benchmarks
export DB_CONNECTION_STRING="Server=localhost,1433;Database=master;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes"
```

`scripts/env.sh` and `scripts/run_tests.py` are conveniences for the maintainers'
setup (a `.env.local` next to the main checkout, one private database per run);
anyone else can just set `DB_CONNECTION_STRING`.

## Tests

```bash
python -m pytest -q --timeout 120 tests/common_issues tests/port
python scripts/check_upstream_baseline.py     # vendored upstream suite vs tests/upstream_passing_baseline.txt
cargo test --lib
```

Scope and rules for the three suites: [docs/TEST_SCOPE.md](docs/TEST_SCOPE.md). A new
behaviour needs a test in `tests/common_issues`. If you exclude an upstream test,
add it to `tests/upstream_excluded.txt` with a one-line reason.

## Code

- `cargo clippy --all-targets` should not add warnings; run `cargo fmt` on files you touch.
- Edits to the vendored Python layer (`python/arrowsql`) and to `vendor/mssql-tds`
  are marked `RUST-PORT` and listed in `docs/PYTHON_LAYER_CHANGES.md` /
  `vendor/mssql-tds/VENDOR.md`. Keep upstream's lines unchanged where you can.
- New binding symbols go into `docs/CONTRACT.md` and `python/arrowsql/ddbc_bindings.pyi`.
- Performance claims need a run of `benchmarks/run.py` (see `benchmarks/README.md`);
  do not hand-edit `benchmarks/RESULTS.md` or the README benchmark tables.
- Do not commit local paths, host names or credentials.

## Pull requests

Describe what changed and how you tested it, update `CHANGELOG.md`, and keep commits
focused. Releases and PyPI publishing are done by the maintainers
(`docs/PACKAGING.md`).
