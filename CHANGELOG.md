# Changelog

All notable changes to this project. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org/) once 1.0 is reached (0.x may break).

Version numbers in this project are deliberately three different things:

| what | value | meaning |
|---|---|---|
| distribution `arrowsql` | 0.1.0 | this project's release number (`pyproject.toml`) |
| `arrowsql.__version__` | 1.16.0 | the upstream `mssql-python` release the Python layer is vendored from; kept so code that checks the upstream version keeps working |
| `arrowsql.ddbc_bindings.__version__` | 1.0.0 | version of the Rust binding contract (`docs/CONTRACT.md`) |

## [Unreleased]

### Added

- UTF-8 transfer for Arrow reads: `read_arrow(..., utf8_strings=True)` and the new
  `cursor.execute_arrow(sql, params=None, *, utf8_strings=True, batch_size=...)` describe the
  result and cast nvarchar/nchar/ntext columns to a UTF-8 collated varchar on the server (query
  wrapping is built in Rust: `ddbc_bindings.utf8_wrap_query`). About half the string bytes on
  the wire; same Arrow result. Needs SQL Server 2019+/Azure SQL; falls back to the original
  query on ORDER BY, parameters, duplicate/empty column names, describe failures, a small
  string share of the row, or when the first 256 rows are not at least 10% smaller in UTF-8 (CJK
  grows). `utf8_strings=False` opts out.
- Bulk copy writes each DECIMAL cell at the shortest legal magnitude width (4/8/12/16 bytes)
  instead of the width implied by the column precision. SQL Server accepts this; 1M rows of nine
  `decimal(38,2)` columns with small values upload 55 MB instead of 163 MB. `ARROWSQL_DECIMAL_FIXED=1`
  restores precision-width cells.
- The project is called **arrowsql** (distribution and import name `arrowsql`,
  repository github.com/bmsuisse/arrowsql). A `mssql_python` alias package keeps
  `import mssql_python` working: its modules are the `arrowsql` modules.
- PyO3 extension `arrowsql.ddbc_bindings` over Microsoft's `mssql-tds`
  (vendored, with local patches, in `vendor/mssql-tds`): no ODBC driver, same
  Python API as upstream mssql-python 1.16.0.
- Native Arrow: `cursor.arrow()`, `arrow_batch()`, `arrow_reader()`,
  `fetchall_arrow()`, `fetchmany_arrow()`, `arrow_stream()`, `read_arrow()`
  (partitioned parallel reads).
- Agent mode (`arrowsql.agent`, `python -m arrowsql.agent`, console script
  `arrowsql-agent`), ported from arrowbricks: `query()` / `query_cursor()` return
  a compact typed TSV (or Markdown) preview with a strict byte ceiling, row and
  cell limits, an optional head/tail sample, a footer that never claims an
  unknown total, `-- rows_affected` / `-- message` lines and every result set;
  failures are JSON with a category, the server's own message and a recovery
  hint (SQL Server error number, then SQLSTATE). Rendering is Rust
  (`ddbc_bindings.AgentRender`) over the Arrow sink; `sql_variant` is now
  readable through it.
- `read_arrow_stream()`: partitioned parallel read as a bounded, unordered Arrow
  stream (`__arrow_c_stream__`; native `ArrowMergeStream` fed by one connection
  per worker) so DuckDB / deltalake overlap with the read and memory stays
  bounded. `close()` or an early-stopping consumer cancels the statements and
  closes the worker connections; the first worker error reaches the consumer.
  `read_arrow()` is now built on it. `partition_bounds=` (and
  `partition_nullable=`) take typed boundaries (int, date/datetime, Decimal,
  str) as disjoint half-open slices plus a NULL slice, bound as parameters,
  with no MIN/MAX or size probe.
- Native bulk copy (rows and Arrow), `write_arrow()` parallel loads.
- Native asyncio (`arrowsql.aio`) with a built-in pool (Unix only).
- `sqlstate` and `native_error` on every exception
  ([upstream #581](https://github.com/microsoft/mssql-python/issues/581));
  bulk copy on the cursor's own connection, so `#temp` tables work
  ([#492](https://github.com/microsoft/mssql-python/issues/492)).
- Type stubs for the port-specific API (`ddbc_bindings.pyi`, RUST-PORT section of
  `mssql_python.pyi`).
- CI (`.github/workflows/ci.yml`) and a wheel/sdist build workflow
  (`wheels.yml`, artifacts only, no publishing).
- Project metadata: SPDX license, classifiers, URLs; CONTRIBUTING.

### Changed

- `-z pack-relative-relocs` moved out of `.cargo/config.toml` into the local
  release scripts, so CI manylinux_2_28 wheels do not require glibc 2.36.
- The `async-preview` extra pins `mssql-python-rs==0.4.0` (was 0.3.0).
- The sdist now carries `vendor/mssql-tds`.

### Known gaps

- Pool checkout does not probe an idle connection with a timeout (upstream
  GH #854), and `set_attr(SQL_ATTR_CONNECTION_TIMEOUT)` is not carried across
  pool reuse. See `tests/upstream_excluded.txt`.
- Always Encrypted, `ActiveDirectoryPassword` and NTLM are not supported.
- Only Linux x86_64 has been exercised against a database.
