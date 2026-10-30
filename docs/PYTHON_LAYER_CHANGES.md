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
- `bulk_parallel.py` (new) + `__init__.py` export: `mssql_python.write_arrow(connection_str, table, source, workers=4, batch_size=0, table_lock=True)` cuts an Arrow source into slices and bulk-loads them over `workers` connections, each with `cursor.bulkcopy_arrow(table_lock=True)`. Heap targets only; other targets fall back to one worker with a `RuntimeWarning`.

## Per-query overhead: cached result layout (`cursor.py`, RUST-PORT)

`docs/PERFORMANCE_RESEARCH.md` §6 measured that about a third of upstream's
point-lookup client time was rebuilding `cursor.description` and the type map on
every `execute()`. The port now caches that work per result layout.

| Location | Change |
|---|---|
| `cursor.py` `_SQL_TYPE_TO_PYTHON_TYPE` (module level) | The ODBC type-code table is built once, not on each `_map_data_type` call. |
| `cursor.py` `_ResultLayout` (new) | Holds the metadata list, description, SQL type codes, column maps and the converter map for one result layout. |
| `cursor.py` `_initialize_description` | Reuses the cached `_ResultLayout` when the column metadata and the `lowercase` setting match the previous result; otherwise builds one (`_build_result_layout`). |
| `cursor.py` `execute` | Takes the column maps and converter map from the layout. The converter map is rebuilt only when `connection._converters_generation` changes. Drops the `SQLRowCount` call that was immediately overwritten for SELECTs. |
| `cursor.py` `_SQL_HANDLE_STMT`, `_SQL_SUCCESS_WITH_INFO`, `_SQL_NO_DATA` (module level) | Plain ints for the status checks in `execute`, `fetchone`, `fetchall` and the reset and diagnostics helpers, instead of `ddbc_sql_const.X.value` (an enum descriptor call per access). |
| `cursor.py` `execute` | `_get_encoding_settings()` runs only for a parameterized `DDBCSQLExecute`, not for `DDBCSQLExecDirect`, which takes no encoding argument. |

The per-execute `DDBCSQLDescribeCol` call stays. A cache keyed on a native
layout signature would skip it, but the upstream test
`test_execute_describe_col_exception_resets_description_and_sql_types` requires
that call on every execute.

Behaviour is unchanged: each execute still returns a new `description` only when
the layout differs, the description list is never mutated in place, and the
converter map is still keyed on the converter generation.

## Native bulk copy (docs/ARCHITECTURE.md, "Bulk copy")

- `cursor.py` `bulkcopy` / `bulkcopy_arrow`: when `self.hstmt` is a native
  `ddbc_bindings.SqlHandle`, the load runs in-process on the cursor's own
  connection through `DDBCBulkCopyRows` / `DDBCBulkCopyArrow` (`RUST-PORT`).
  Consequences: a `#temp` target works (upstream #492); in manual-commit mode the
  load joins the open transaction instead of committing on a second connection;
  no token acquisition or `mssql_py_core` import. The preconditions of the old
  path are kept (`_bulkcopy_connection_params`: connection string present, SERVER
  given). `mssql_py_core` serves only cursors without a native handle (mocks) and
  iterables of `RecordBatch`; `_bulkcopy_core_and_validate` is now
  `_bulkcopy_validate` + `_bulkcopy_import_core`.
- `Cursor._bulk_row_iterator` (the former nested `_prepare_row_iterator`) also
  materializes a `Cursor` of the same connection used as a row source, which
  would otherwise deadlock on the connection's wire.
- Timeout errors read "Bulk copy timeout: the operation timed out after N seconds".
