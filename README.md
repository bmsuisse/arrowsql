# mssql-rs (mssql-python on a Rust core)

[mssql-python](https://github.com/microsoft/mssql-python) with its C++/ODBC
core replaced by Rust: a PyO3 extension over Microsoft's native TDS crate
[`mssql-tds`](https://github.com/microsoft/mssql-rs). No ODBC driver, no
unixODBC, same Python API (`import mssql_python`).

Status: work in progress. The upstream Python layer is vendored unchanged
where possible, and the upstream test suite is the compatibility bar.

## What it adds

- Faster row fetches than upstream on every row-based read in our benchmarks.
- Native Arrow: upstream's `cursor.arrow()`, `arrow_batch()` and
  `arrow_reader()` decode straight into Arrow with the GIL released.
- A zero-dependency Arrow API: `fetchall_arrow()`, `fetchmany_arrow()` and
  `arrow_stream()` return objects that DuckDB, polars and pyarrow read
  directly via the Arrow PyCapsule interface.
- Partitioned parallel reads: `mssql_python.read_arrow(conn_str, query,
  partition_on=, partitions=)`.
- `sqlstate` and `native_error` on every exception
  ([upstream #581](https://github.com/microsoft/mssql-python/issues/581)).

Numbers: [benchmarks/RESULTS.md](benchmarks/RESULTS.md) (multi-driver) and
[docs/ARROW_PLAN.md](docs/ARROW_PLAN.md) (Arrow).

## Build and test

```bash
uv venv -p 3.13 .venv && VIRTUAL_ENV=.venv uv pip install -r requirements-dev.txt
scripts/build.sh                 # dev build into .venv (--release for benchmarks)

# SQL Server 2022 in podman; put DB_CONNECTION_STRING / MSSQL_SA_PASSWORD in .env.local
source scripts/env.sh my-id      # private database per run
cd tests/upstream && ../../.venv/bin/python -m pytest -q
```

Test scope and the upstream baseline: [docs/TEST_SCOPE.md](docs/TEST_SCOPE.md).
NYC taxi benchmark data: `scripts/load_taxi.py`.

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): design, concurrency model, where things live
- [docs/CONTRACT.md](docs/CONTRACT.md): every binding symbol and its status
- [docs/PYTHON_LAYER_CHANGES.md](docs/PYTHON_LAYER_CHANGES.md): deliberate edits to the vendored Python layer
- [docs/PERFORMANCE_RESEARCH.md](docs/PERFORMANCE_RESEARCH.md): where the time goes, roadmap, compression
- [docs/UPSTREAM_ISSUES.md](docs/UPSTREAM_ISSUES.md), [docs/UPSTREAM_BUGS_FOUND.md](docs/UPSTREAM_BUGS_FOUND.md)

## License

MIT. Contains code from microsoft/mssql-python (MIT,
[LICENSE.upstream-mssql-python](LICENSE.upstream-mssql-python)) and depends on
microsoft/mssql-rs (MIT).
