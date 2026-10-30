# Architecture: Rust `ddbc_bindings`

`python/mssql_python/` is upstream mssql-python's Python layer, nearly
unchanged (edits: `docs/PYTHON_LAYER_CHANGES.md`). Upstream's C++/pybind11
module `ddbc_bindings` called the Microsoft ODBC driver. This crate replaces
it with a PyO3 module of the same name and surface, speaking TDS directly via
Microsoft's `mssql-tds` crate (microsoft/mssql-rs, MIT, pinned in
`Cargo.toml`). No ODBC driver or driver manager is involved.

```
Python (cursor.py / connection.py, upstream)
   │  DDBCSQLExecute(stmt, sql, params, ...)  -> return code + diag records
   ▼
src/statement.rs  ── params/ (Python value -> SqlType, ? -> @Pn)
   │  py.detach (GIL released) + runtime::block_on
   ▼
mssql-tds TdsClient (one per Connection, behind ConnShared::slot)
   │  RowWriter callbacks while decoding TDS rows
   ▼
decode/buffer.rs RowBuffer (GIL-free) ──► decode/python.rs (GIL held) ──► lists of values
```

## Modules (one owner per file)

| File | Responsibility |
|---|---|
| `lib.rs` | module registration, misc compat functions |
| `runtime.rs` | single process-wide tokio runtime, `block_on` |
| `consts.rs` | ODBC constants shared with `constants.py` |
| `connstr.rs` | ODBC connection string -> `ClientContext` |
| `connection.rs` | `Connection` pyclass, `ConnShared` (TDS client slot, autocommit, session state) |
| `statement.rs` | `SqlHandle` pyclass, execute / fetch / more-results state machine, MARS emulation, executemany |
| `diag.rs` | diagnostic records, SQL Server error -> SQLSTATE table |
| `params/mod.rs` | parameter list -> `ParamSet` (+ `conversion_error`) |
| `params/py_types.rs` | per-value Python -> `SqlType` (vendored from mssql-py-core) |
| `params/placeholders.rs` | `?` -> `@Pn`, `{call ...}` escapes |
| `params/null_types.rs` | typing of `None` params via `sp_describe_undeclared_parameters` |
| `params/batch.rs` | column-uniform types for executemany |
| `decode/buffer.rs` | `RowBuffer`: the GIL-free `RowWriter` sink |
| `decode/python.rs` | `RowBuffer` -> Python lists |
| `decode/{numeric,text,temporal,binary}.rs` | one file per SQL type family |
| `decode/describe.rs` | ODBC column metadata (SQLDescribeCol) |
| `catalog.rs` | SQLTables/Columns/... via `sp_*` catalog procedures |
| `getinfo.rs` | SQLGetInfo values (partial) |
| `pooling.rs` | connection pool (RESETCONNECTION on reuse) |
| `rowfactory.rs` | `construct_rows` (Row objects without `__init__`) |
| `types.rs` | `ParamInfo`, `NumericData`, `ErrorInfo` |
| `arrow.rs` | Arrow fetch (stub; seam described in the file) |
| `logging.rs` | log-level bridge (stub) |

## Return codes and diagnostics

Every `DDBCSQL*` function returns an ODBC return code (`SQL_SUCCESS` 0,
`SQL_SUCCESS_WITH_INFO` 1, `SQL_NO_DATA` 100, `SQL_ERROR` -1,
`SQL_INVALID_HANDLE` -2) and mutates the out-lists it is given (`row`,
`rows`, `ColumnMetadata`, `messages`). Errors are not raised from Rust:
they are stored as `DiagRecord`s on the statement, and the Python layer
calls `DDBCSQLCheckError` / `DDBCSQLGetAllDiagRecords` to raise. Message
text keeps msodbcsql's `[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]`
prefix. Connection-level failures raise
`RuntimeError("SQLSTATE:XXXXX:NATIVE:<n>:<message>")`.

Unimplemented functions record `IM001 "<fn> not implemented in Rust port"`
and return `SQL_ERROR` — never panic.

## Concurrency model

- One `TdsClient` per `Connection`, in `ConnShared::slot` (a
  `parking_lot::Mutex<Slot>`), shared by all its statements.
- Lock order: `ConnShared::slot` -> `StmtShared::state`. Both are taken only
  with the GIL released (`py.detach`) and are never held while acquiring the
  GIL, so Python threads can't deadlock against network I/O.
- All network calls run through `runtime::block_on` on the shared runtime
  from the calling thread, with the GIL released.
- `StmtShared::cancel` is a separate lock so `_cancel` from another thread
  never waits behind a running fetch; it cancels via `CancelHandle`.

### MARS emulation

TDS without MARS allows one active request per connection. `Slot::owner`
records which statement's batch is on the wire. When a different statement
(or a commit/rollback) needs the connection, `release_wire` drains the
owner's remaining rows and subsequent results into the owner's own buffers
(`StmtState::pending` / `queued`), so both cursors keep working exactly as
with msodbcsql + MARS. A dropped owner's batch is simply discarded.

## Data flow: execute

1. Python values -> `ParamSet` (GIL held): `params::build_rpc_params`.
2. GIL released: lock slot, free the wire, reset statement state.
3. `prepare_session`: deferred session SQL (isolation, catalog) and, in
   manual-commit mode, `SET IMPLICIT_TRANSACTIONS ON` once — the server then
   opens a transaction with the next statement, with no BEGIN round trip.
4. Untyped `None` params are typed via `sp_describe_undeclared_parameters`
   (cached per SQL text per connection).
5. `client.execute` (no params) or `execute_sp_executesql` (params).
6. On a row result, the first row is read ahead so errors raised before any
   row (lock timeout, divide by zero) fail the execute like msodbcsql.

## Data flow: fetch

`fetch()` pulls rows into `StmtState::pending` (a `RowBuffer`) with the GIL
released, takes the requested rows out, drops the locks, then converts them
to Python objects with the GIL held. `fetchone` reads ahead 256 rows;
`fetchall` works in batches of 8192 rows / 16 MiB, converting each batch
before reading the next. An error met while reading ahead is held in
`pending_error` and reported once the rows before it were delivered.

### The decode seam (for new sinks, e.g. Arrow)

`statement::read_rows_into<W: RowWriter + Send>(client, sink, rows_fn,
bytes_fn, max_rows, max_bytes)` drives mssql-tds's row decoder into any
`mssql_tds::datatypes::row_writer::RowWriter`. `RowBuffer` is the Python
sink; an Arrow sink implements the same trait (see `arrow.rs`).

## executemany

`SQLExecuteMany` normalises each column to one SQL type
(`params/batch.rs`), then sends all rows as `sp_execute` RPCs against one
prepared handle, 2000 rows per TDS request (`execute_prepared_batch`).

## Pooling

`pooling.rs` keeps idle `TdsClient`s per (connection string, pool key).
Checkout arms RESETCONNECTION (`prepare_reset_connection`), applied by the
server with the next request — no extra round trip. `close()` rolls back an
open transaction and returns the session to the pool.

## How to...

- **Add a SQL type (fetch):** handle its `RowWriter::write_*` call in
  `decode/buffer.rs` (new `Cell` variant if needed), convert it in
  `decode/python.rs` through the family module, map metadata in
  `decode/describe.rs`.
- **Add a parameter type:** extend `params/py_types.rs::py_to_sql_type`
  (mark local edits `LOCAL`), plus `params/batch.rs::null_like` and
  `params/null_types.rs::null_of`.
- **Add a catalog function:** in `catalog.rs`, call `run()` with the
  `sp_*` procedure and named args; add ODBC 3 column renames to
  `ODBC3_RENAMES`.
- **Add a SQLSTATE mapping:** insert `(error_number, "SSSSS")` into
  `SERVER_ERROR_TO_SQLSTATE` in `diag.rs`, keeping it sorted.
- **Add a SQLGetInfo value:** add the info type to the match in `getinfo.rs`.

## Build / test

```
scripts/build.sh                      # flock-serialized maturin develop
source scripts/env.sh <agent-id>      # per-agent database on the podman SQL Server
cd tests/upstream && ../../.venv/bin/python -m pytest -q test_004_cursor.py
cargo test --lib                      # pure-Rust unit tests (no DB)
```
