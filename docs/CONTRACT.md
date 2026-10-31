# Binding contract: `mssql_python.ddbc_bindings`

Every symbol the upstream C++ module exported (PYBIND11_MODULE in upstream
`mssql_python/pybind/ddbc_bindings.cpp`) exists with the same name, argument
names and defaults. Return codes: `SQL_SUCCESS` 0, `SQL_SUCCESS_WITH_INFO` 1,
`SQL_NO_DATA` 100, `SQL_ERROR` -1, `SQL_INVALID_HANDLE` -2.

The `Aio*` classes, `aio_connect` and `aio_create_pool` are compiled only on Unix (`#[cfg(unix)]`); on Windows they are absent.

Status: **impl** = implemented, **partial** = works for the common cases
(see TODO in the file), **stub** = accepted / returns `IM001` + `SQL_ERROR`.

## Module attributes

| Symbol | Value | Status | File |
|---|---|---|---|
| `__version__` | `"1.0.0"` | impl | lib.rs |
| `__architecture__`, `ARCHITECTURE` | `std::env::consts::ARCH` | impl | lib.rs |
| `SQL_NO_TOTAL` | -4 | impl | lib.rs |
| `__rust_port__` | `True` (new; lets Python detect the port) | impl | lib.rs |

## Classes

| Class | Constructor / members | Status | File |
|---|---|---|---|
| `Connection` | `Connection(conn_str, use_pool, attrs_before=None, pool_key="", token_factory=None, fedauth_factory=None)`; `close(transaction_already_rolled_back=False)`, `commit()`, `rollback()`, `set_autocommit(bool)`, `get_autocommit()`, `_get_connection_timeout()`, `set_attr(attribute, value)`, `alloc_statement_handle()`, `get_info(info_type)` | impl (`token_factory` invoked on pool miss / non-pooled connect, expiry-aware on checkout; `fedauth_factory(spn, sts_url, method)` returns the JWT for ServicePrincipal / ActiveDirectoryPassword logins once the server names the tenant; a malformed access token raises; `close()` of a non-pooled connection with an open transaction raises 25000 and leaves it open; `get_info`: partial) | connection.rs |
| `SqlHandle` | `free()`, `_close_cursor()`, `_cancel()` | impl | statement.rs |
| `ParamInfo` | `inputOutputType, paramCType, paramSQLType, columnSize, decimalDigits, strLenOrInd, dataPtr, isDAE` (read/write) | impl | types.rs |
| `NumericData` | `NumericData()`, `NumericData(precision, scale, sign, val)`; same attributes | impl | types.rs |
| `ErrorInfo` | `sqlState: str`, `ddbcErrorMsg: str`, **`nativeError: int`** (new: SQL Server error number, 0 for driver errors) | impl | types.rs |

Connection errors raise `RuntimeError("SQLSTATE:XXXXX:NATIVE:<n>:<message>")`.

## Functions

| Function | Signature | Status | File |
|---|---|---|---|
| `DDBCSQLExecDirect` | `(StatementHandle, Query)` | impl | statement.rs |
| `DDBCSQLExecute` | `(statementHandle, query, params, inputSizes, isStmtPrepared, usePrepare, encodingSettings)` | impl (sp_executesql; `inputSizes` hints applied, `encodingSettings` ignored) | statement.rs |
| `SQLExecuteMany` | `(statementHandle, query, columnwise_params, paramInfos, paramSetSize, encodingSettings)` | impl (prepared RPC batches; `OUTPUT` result sets are returned, one per parameter set via `nextset`; DECIMAL/NUMERIC `paramInfos` are applied) | statement.rs |
| `DDBCSQLRowCount` | `(StatementHandle) -> int` | impl | statement.rs |
| `DDBCSQLNumResultCols` | `(statementHandle, messages=None) -> int` | impl | statement.rs |
| `DDBCSQLDescribeCol` | `(StatementHandle, ColumnMetadata, messages=None)`; appends `{"ColumnName","DataType","ColumnSize","DecimalDigits","Nullable"}` | impl | statement.rs, decode/describe.rs |
| `DDBCSQLFetchOne` | `(StatementHandle, row, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None)` | impl (encodings ignored, see decode/text.rs) | statement.rs |
| `DDBCSQLFetchMany` | `(StatementHandle, rows, fetchSize, charEncoding=..., wcharEncoding=..., charCtype=..., messages=None)` | impl | statement.rs |
| `DDBCSQLFetchAll` | `(StatementHandle, rows, charEncoding=..., wcharEncoding=..., charCtype=..., messages=None)` | impl | statement.rs |
| `DDBCSQLFetchScroll` | `(StatementHandle, FetchOrientation, FetchOffset, row_data)` | partial (`SQL_FETCH_NEXT` only, like a forward-only cursor) | statement.rs |
| `DDBCSQLMoreResults` | `(StatementHandle)` | impl | statement.rs |
| `DDBCSQLResetStmt` | `(StatementHandle)` | impl | statement.rs |
| `DDBCSQLFreeHandle` | `(HandleType, Handle)` | impl (a second free returns `SQL_INVALID_HANDLE`) | statement.rs |
| `DDBCSQLSetStmtAttr` | `(stmt, attr, value)` | partial (`SQL_ATTR_QUERY_TIMEOUT`; others accepted) | statement.rs |
| `DDBCSQLCheckError` | `(handleType, handle, retcode) -> ErrorInfo` | impl (first record, with `nativeError`) | statement.rs |
| `DDBCSQLGetAllDiagRecords` | `(handle) -> list[("[SSSSS] (native)", message)]` | impl (C++ tuple shape; native number in the state label) | statement.rs |
| `DDBCSQLFetchArrowBatch` | `(StatementHandle, capsules, arrowBatchSize, charCtype, messages=None)` | implemented | arrow/mod.rs |
| `DDBCArrowFetchTable` (port-only) | `(StatementHandle, max_rows=-1, batch_size=65536, messages=None) -> (ret, ArrowTable)` | implemented | arrow/mod.rs |
| `DDBCArrowStream` (port-only) | `(StatementHandle, batch_size=65536, prefetch=2, owner=None) -> ArrowStream` | implemented | arrow/mod.rs |
| `ArrowTable`, `ArrowStream` (port-only classes) | Arrow PyCapsule protocol; `ArrowTable.concat(tables)` | implemented | arrow/export.rs |
| `AgentRender` (port-only class) | `AgentRender(format="tsv", max_bytes=10000, max_rows=1000, max_cell_chars=500, tail_rows=0, timeout_s=None)`; `result_set(statement, messages=None) -> (ret, exhausted)` renders the current result set (negative `ret`: fetch error, raise with `check_error`; `TimeoutError` past `timeout_s`), `rows_affected(n)`, `message(text)`, `omit(n)`, `has_room()`, `finish() -> str`; `ValueError` for bad budgets | implemented | agent/mod.rs |
| `DDBCBulkCopyRows` (port-only) | `(handle, table_name, rows, batch_size=0, timeout=30, column_mappings=None, keep_identity=False, check_constraints=False, table_lock=False, keep_nulls=False, fire_triggers=False, use_internal_transaction=False)`; loads an iterator of row tuples, returns the `Cursor.bulkcopy` result dict | implemented | bulk/mod.rs |
| `DDBCBulkCopyArrow` (port-only) | same parameters, `source` is anything with `__arrow_c_stream__` / `__arrow_c_array__`; returns the `Cursor.bulkcopy_arrow` result dict; raises `BulkUnsupported` when the schema cannot be encoded natively | implemented | bulk/mod.rs |
| `SQLExecuteManyRows` (port-only) | `(statementHandle, query, rows)`; runs `query` once per row, returns the return code, or `None` (nothing sent) when the caller must use the general `SQLExecuteMany` path | implemented | statement.rs |
| `BulkUnsupported` (port-only exception) | raised by the two bulk functions when the native encoder cannot load the source into the destination; the Python layer falls back | implemented | bulk/mod.rs |
| `AioHub`, `AioConnection`, `AioPool`, `AioRowBatch` (port-only classes, Unix only) | native asyncio core behind `mssql_python.aio`. `AioHub(event_loop, shape_factory)` per loop (`fileno`, `drain`, `pending`); `AioConnection` (`execute`, `executemany`, `fetch`, `fetch_arrow`, `nextset`, `query`, `cursor`, `close_cursor`, `commit`, `rollback`, `set_autocommit`, `cancel`, `close`, `timeout`, `autocommit`, `open_cursors`); `AioPool` (`acquire`, `query`, `cancel`, `close`, `idle`, `in_use`, `max_size`, `timeout`); `AioRowBatch` (`next`, `take`, `remaining`, `shape`) | implemented | aio/ |
| `aio_connect` (port-only) | `(hub, conn_str, autocommit=False, timeout=0, tuple_rows=False)` | implemented (Unix only) | aio/connection.rs |
| `aio_create_pool` (port-only) | `(hub, conn_str, min_size=1, max_size=10, autocommit=True, timeout=0, tuple_rows=False, reset=True)` | implemented (Unix only) | aio/pool.rs |
| `DDBCSQLFetch` | `(StatementHandle)` | stub (unused by the Python layer) | lib.rs |
| `DDBCSQLGetData` | `(StatementHandle, colCount, row, charEncoding, wcharEncoding, charCtype, messages=None)` | stub (unused) | lib.rs |
| `DDBCSQLTables` | `(StatementHandle, catalog="", schema="", table="", tableType="")` | partial (`sp_tables`) | catalog.rs |
| `DDBCSQLColumns` | `(StatementHandle, catalog, schema, table, column)` | partial (`sp_columns_100`) | catalog.rs |
| `DDBCSQLProcedures` | `(StatementHandle, catalog, schema, procedure)` | partial | catalog.rs |
| `DDBCSQLPrimaryKeys` | `(StatementHandle, catalog, schema, table)` | partial | catalog.rs |
| `DDBCSQLForeignKeys` | `(StatementHandle, pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable)` | partial | catalog.rs |
| `DDBCSQLSpecialColumns` | `(StatementHandle, identifierType, catalog, schema, table, scope, nullable)` | partial | catalog.rs |
| `DDBCSQLStatistics` | `(StatementHandle, catalog, schema, table, unique, reserved)` | partial | catalog.rs |
| `DDBCSQLGetTypeInfo` | `(StatementHandle, DataType)` | partial (`sp_datatype_info_100`) | catalog.rs |
| `DDBCSetDecimalSeparator` | `(separator)` | impl (stored; not needed by native decode) | lib.rs |
| `construct_rows` | `(rows_data, row_class, column_map, cursor, column_map_lower=None, column_names=None)` | impl | rowfactory.rs |
| `enable_pooling` | `(max_size=100, idle_timeout=600)` | impl | pooling.rs |
| `close_pooling` | `()` | impl | pooling.rs |
| `disable_pooling` | `()` | impl | pooling.rs |
| `update_log_level` | `(level)` | stub (stores level) | logging.rs |
| `ThrowStdException` | `(message)` -> raises `RuntimeError` | impl | lib.rs |
| `GetDriverPathCpp`, `_get_odbc_driver_path` | `(*args) -> ""` | impl (no driver) | lib.rs |
| `_set_odbc_provider` | `(*args)` | impl (no-op) | lib.rs |

Not exported: the `profiling` submodule (upstream only builds it with
`ENABLE_PROFILING`).
