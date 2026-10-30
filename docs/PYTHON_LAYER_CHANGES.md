# Python layer changes

`python/mssql_python/` is vendored from microsoft/mssql-python (commit in
`tests/upstream/UPSTREAM_COMMIT`). Keep it upstream-identical where possible;
every deliberate edit is marked `RUST-PORT` in the source and listed here.

## Core bindings

- `odbc_provider.py` `ProviderManager.ensure_available`: skip the
  "ODBC provider package must be importable" check when
  `ddbc_bindings.__rust_port__` is set. The Rust bindings talk TDS directly,
  so neither `mssql_python_odbc` nor msodbcsql is needed.
- `ddbc_bindings.py` (the platform-specific `.so` loader) is removed;
  maturin installs the extension as `mssql_python.ddbc_bindings` directly.

Edits to `python/mssql_python` relative to upstream mssql-python (commit in
`tests/upstream/UPSTREAM_COMMIT`). Keep them minimal; each section says why.

## Issue #581: `sqlstate` and `native_error` on exceptions

Upstream issue: microsoft/mssql-python#581. Every DB-API exception now carries
`sqlstate` (ODBC SQLSTATE, e.g. `"23000"`) and `native_error` (SQL Server
engine error number, e.g. `2627`). Both are `None` for errors the driver raises
itself (closed cursor, closed connection, client-side validation). A native
error of 0, which ODBC reports for driver-generated diagnostics such as `HYT00`,
is normalized to `None`.

Backward compatible: positional `(driver_error, ddbc_error)` construction,
`str(exc)`, `exc.args`, `driver_error`/`ddbc_error` and the two-argument forms of
`sqlstate_to_exception` / `raise_exception` are unchanged. Pickles keep the new
attributes; pre-#581 pickles (4 reduce args) still load.

| File | Change |
|---|---|
| `exceptions.py` | `Exception.__init__` and all subclasses accept `sqlstate=None, native_error=None`; `sqlstate_to_exception(sqlstate, ddbc_error, native_error=None)` and `raise_exception(..., native_error=None)` set them, including the unknown-SQLSTATE `DatabaseError` fallback; pickling carries them. |
| `helpers.py` | `check_error` takes `ErrorInfo.nativeError` when the binding has it, else reads the handle's `DDBCSQLGetAllDiagRecords` records (`"[SQLSTATE] (native)"`), preferring the record whose SQLSTATE matches. |
| `connection.py` | `_raise_connection_error` accepts an optional native error number: `SQLSTATE:XXXXX:NATIVE:<n>:<message>` (the old `SQLSTATE:XXXXX:<message>` still works). |
| `async_query/exception_translator.py` | py-core errors get `native_error` from the first `sql_errors[*]["number"]` and the SQLSTATE msodbcsql would report for it. |
| `_server_sqlstate.py` (new) | Error number to SQLSTATE table plus severity fallback (> 18 `HY000`, > 10 `42000`, else `01000`), ported from microsoft/mssql-rs `mssql-odbc/src/api/sqlstate.rs` (MIT). Only the async path uses it. |

### What the Rust binding must provide

- `ErrorInfo.nativeError: int` (0 when the driver generated the error). With it,
  `check_error` skips the extra diag-records call on the error path.
- `DDBCSQLGetAllDiagRecords` records as `("[SSSSS] (native)", message)`, the
  same shape as the C++ binding. Messages and the `check_error` fallback both
  parse it.
- Connection-level failures raised as `RuntimeError("SQLSTATE:XXXXX:NATIVE:<n>:<message>")`
  so login failures carry their number (18456). The C++ binding omits `NATIVE:`,
  so on upstream `native_error` is `None` for connection errors; the
  `tests/common_issues/test_issue_581_sqlstate.py::test_login_failure_is_28000`
  native assertion skips in that case.
- SQLSTATEs must match msodbcsql, quirks included: 2628 ("would be truncated
  in table ...") is `42000`/`ProgrammingError` and 8152 is `22001`/`DataError`;
  1222 (lock timeout) is `42000`; query timeout is `HYT00` with no native
  number. Use the table in `_server_sqlstate.py` (or the mssql-odbc original)
  plus the severity fallback.

### Validating Python-layer changes without the Rust core

`scripts/overlay_upstream_binding.sh` builds `.overlay-upstream/`: this repo's
`python/mssql_python` plus upstream's compiled C++ binding. `scripts/test_overlay.sh
<db-id> <pytest args>` runs pytest on it against a private database.

## Native Arrow API (docs/ARROW_PLAN.md)

- `cursor.py`: new methods `fetchall_arrow(batch_size)`, `fetchmany_arrow(size,
  batch_size)` (return `ddbc_bindings.ArrowTable`) and `arrow_stream(batch_size,
  prefetch)` (returns `ddbc_bindings.ArrowStream`), marked `RUST-PORT`. They
  need no pyarrow: the objects implement the Arrow PyCapsule protocol. The
  upstream methods `arrow()`, `arrow_batch()`, `arrow_reader()` are unchanged
  and run on the same native decoder through `DDBCSQLFetchArrowBatch`.
- `arrow_parallel.py` (new) + `__init__.py` export: `mssql_python.read_arrow(
  connection_str, query, partition_on=None, partitions=1, batch_size=65536)`
  reads integer-range partitions of a query over separate connections in
  parallel threads and returns one `ArrowTable` (`ArrowTable.concat`).
