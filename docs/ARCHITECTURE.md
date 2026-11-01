# Architecture: Rust `ddbc_bindings`

`python/arrowsql/` is upstream mssql-python's Python layer, nearly
unchanged (edits: `docs/PYTHON_LAYER_CHANGES.md`). Upstream's C++/pybind11
module `ddbc_bindings` called the Microsoft ODBC driver. This crate replaces
it with a PyO3 module of the same name and surface, speaking TDS directly via
Microsoft's `mssql-tds` crate (microsoft/mssql-rs, MIT, pinned in
`Cargo.toml`). No ODBC driver or driver manager is involved.

The import name is `arrowsql`. `python/mssql_python/` is a compatibility alias
(a meta-path finder): `mssql_python` and every `mssql_python.X` resolve to the
same module objects as `arrowsql` / `arrowsql.X`, so upstream's tests and
existing code import `mssql_python` unchanged.

```
Python (cursor.py / connection.py, upstream)
   │  DDBCSQLExecute(stmt, sql, params, ...)  -> return code + diag records
   ▼
src/statement/   ── params/ (Python value -> SqlType, ? -> @Pn)
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
| `statement/mod.rs` | `StmtShared`, `SqlHandle` pyclass, the internal API used by `catalog.rs` |
| `statement/wire.rs` | wire ownership: `Slot`, which statement owns the TDS stream, buffering (bounded) or discarding its batch |
| `statement/cursor.rs` | a statement's current and queued results; fetch, next-result, the `read_rows_into` decode seam |
| `statement/exec.rs` | `execute` / `executemany`, deferred session SQL, NULL parameter typing |
| `statement/cancel.rs` | cancellation + timeout of one execution, forwarded to a batch being drained |
| `statement/odbc.rs` | the `DDBCSQL*` Python functions (thin wrappers) |
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
| `pooling.rs` | connection pool: per-key capacity leases, idle sessions, generations, token expiry |
| `rowfactory.rs` | `construct_rows` (Row objects without `__init__`) |
| `types.rs` | `ParamInfo`, `NumericData`, `ErrorInfo` |
| `arrow/plan.rs` | SQL column -> Arrow type (mirrors upstream's C++ mapping) |
| `arrow/sink.rs` | `ArrowSink`: `RowWriter` decoding TDS rows into Arrow buffers |
| `arrow/export.rs` | PyCapsule export, `ArrowTable`, pipelined `ArrowStream` |
| `arrow/mod.rs` | `DDBCSQLFetchArrowBatch`, `DDBCArrowFetchTable`, `DDBCArrowStream` |
| `agent/mod.rs` | `AgentRender` pyclass: one statement's bounded text output (result sets, rows affected, messages) under one byte ceiling; chunked Arrow fetch with the GIL released |
| `agent/render.rs` | `Table`: typed TSV / Markdown header, escaping, cell clipping, head/tail sample, footer |
| `agent/sqltype.rs`, `agent/fmt.rs` | SQL type names for the header; date / time / decimal / hex text (also used by `arrow/sink.rs` for `sql_variant`) |
| `bulk/mod.rs` | `DDBCBulkCopyArrow` / `DDBCBulkCopyRows`: bulk load on the cursor's own connection (`with_exclusive_wire`), `BulkUnsupported`, result dict |
| `bulk/settings.rs` | bulk options, `column_mappings` parsing, source -> destination column mapping |
| `bulk/arrow_source.rs` | Arrow PyCapsule (`__arrow_c_stream__` / `__arrow_c_array__`) -> record batch stream |
| `bulk/arrow_encode.rs` | (Arrow type, SQL type) column plans, `ColumnValues` fallback cells, windows of rows |
| `bulk/arrow_wire.rs`, `bulk/wire.rs` | direct TDS encoding of cells into a per-row byte buffer (byte-for-byte what `TdsValueSerializer` writes) |
| `bulk/rows.rs` | Python tuples -> TDS rows, read a window at a time under the GIL |
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
  GIL, so Python threads can't deadlock against network I/O. This includes
  the getters (`DDBCSQLRowCount`, `DDBCSQLNumResultCols`, diagnostics,
  `get_info`): they wait for a running query without the GIL.
- All network calls run through `runtime::block_on` on the shared runtime
  from the calling thread, with the GIL released.
- Waiting for a short query's first response byte spins briefly on the socket
  (`busy_poll`, see `vendor/mssql-tds/VENDOR.md` item 6) instead of sleeping in
  epoll: it skips the scheduler's wake-up latency, which is a few percent of a
  `SELECT 1` round trip, at the price of one busy core for the wait. Adaptive
  (only while this connection's recent waits are under 300 us), at most two
  spinners process-wide, `MSSQL_RS_BUSY_POLL=0` disables it.
- `StmtShared::cancel` is a separate lock so `_cancel` from another thread
  never waits behind a running fetch; it cancels via `CancelHandle`.

### MARS emulation

TDS without MARS allows one active request per connection. `Slot::owner`
records which statement's batch is on the wire. When a different statement
(or a commit/rollback) needs the connection, `wire::buffer_owner` reads the
owner's remaining rows and subsequent results into the owner's own buffers
(`StmtState::pending` / `queued`), so both cursors keep working exactly as
with msodbcsql + MARS.

* The buffering is bounded by `DRAIN_BUDGET_BYTES` (64 MiB per statement).
  Past it the rest of the batch is abandoned and the owner's cursor raises
  `HY000 "Result set discarded ..."` once it reaches the cut.
* It runs under the *requester's* timeout and cancellation: the requester's
  `StmtCancel` forwards `_cancel()` and its deadline to the owner's cancel
  handle for the duration of the drain.
* An owner that goes away (cursor closed or freed, re-executed, connection
  closed) is *discarded*, not drained: the batch is closed if only trailing
  tokens remain, otherwise the server is sent a TDS attention.
* SQL batches run with `BatchErrorMode::Continue`, so a statement error
  neither ends the batch nor hides later results; the error is delivered by
  the call that met it (`fetch`, `execute`, `nextset`).

## Data flow: execute

1. Python values -> `ParamSet` (GIL held): `params::build_rpc_params`.
2. GIL released: lock slot, free the wire, reset statement state.
3. `prepare_session`: deferred session SQL (isolation, catalog) and, in
   manual-commit mode with no open transaction, a TDS transaction-manager
   BEGIN (one round trip, like msodbcsql). `@@TRANCOUNT` is therefore 1
   inside the first statement, and commit()/rollback() do not begin the next
   transaction: system-versioned tables stamp rows with its start time.
   `SET IMPLICIT_TRANSACTIONS` is not used (a bare SELECT would not open one).
   If a statement fails and the transaction is gone, the next statement
   probes `@@OPTIONS & 16384`; with XACT_ABORT on, every statement that is not
   certainly read-only (`src/txn_abort.rs`, shared by sync and async) is rejected
   (25000) until commit()/rollback(), like go-mssqldb. Other failures (a
   conversion error also rolls back) start a new transaction silently.
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
sink; `arrow::sink::ArrowSink` is the Arrow one. Batch sinks implement
`statement::BatchSink` and are filled by `statement::fetch_into`, which
takes rows already buffered as cells first (read ahead by `fetchone`,
or drained for MARS emulation) and then decodes straight off the wire, so
Arrow and row fetches interleave on one result.

## Arrow

`ArrowSink` keeps one plain buffer set per column (values, offsets,
validity bits) and writes each value as mssql-tds decodes it: no Python
objects, no per-cell allocation, UTF-16 converted to UTF-8 in one pass
(ASCII fast path, else `encoding_rs`) straight into the column's data
buffer. A whole batch is decoded with the GIL released; `finish()` wraps
the buffers as arrow-rs arrays without copying.

Why arrow-rs (`arrow-array`/`-buffer`/`-data`/`-schema`, `ffi` feature)
rather than `pyo3-arrow` or a hand-written C Data Interface: `pyo3-arrow`
pins its own pyo3 version (we are on 0.29) and brings a large API we don't
need; hand-writing the release callbacks for schemas, arrays and streams
is exactly the kind of unsafe code arrow-rs already gets right. We use it
only for the array wrappers and the FFI export, not for building.

Three ways out (see `docs/ARROW_PLAN.md`):

- `DDBCSQLFetchArrowBatch`: upstream's contract (struct-array capsules)
  behind `cursor.arrow()`, `arrow_batch()`, `arrow_reader()`.
- `DDBCArrowFetchTable` -> `ArrowTable` (`cursor.fetchall_arrow()`,
  `fetchmany_arrow()`): all batches decoded in one GIL-free call; the
  table exports `__arrow_c_stream__` any number of times.
- `DDBCArrowStream` -> `ArrowStream` (`cursor.arrow_stream()`): a producer
  thread runs `fetch_into` batch by batch (locks taken per batch, so other
  statements on the connection can interleave) into a bounded channel.
  It stops when the receiver is dropped, or when the statement's
  `StmtState::generation` changes (re-execute, nextset, close). A stream
  dropped before its end discards the rest of the result
  (`statement::abandon_result`). It holds the Python cursor so a
  temporary cursor isn't freed under it.

`mssql_python.read_arrow()` (`python/arrowsql/arrow_parallel.py`)
splits a query on an integer column and reads the ranges over separate
connections from plain threads; decode is GIL-free so they run in
parallel. `ArrowTable.concat` joins the parts. One TDS session is capped by
SQL Server's serialization rate, so this is the only way past it.

## Agent mode

`arrowsql.agent` (`python/arrowsql/agent.py`) returns a bounded, typed text
preview of a statement, or a JSON error, as one string (README, "Agent mode").
Python only drives `execute` / `nextset` and classifies exceptions;
everything that touches rows is Rust (`src/agent/`):

```
cursor.execute(sql)  ->  AgentRender.result_set(hstmt)
   chunks of 32 -> 256 rows: ArrowSink + fetch_into (GIL released)
   -> render::Table formats each chunk into the output String
   -> stop when the head is full; fetch 1 more row to prove "more"
```

- No Python row objects, no `arrow_cast`: columns are the `ArrowSink` arrays
  (`Plan::for_display`, which also admits `sql_variant` as text; the sink
  writes variant dates, decimals and binary as text), formatted by
  `agent/fmt.rs`. This keeps the wheel unchanged in size (no new dependency).
- The byte ceiling covers header, escaping, footer and the "parts omitted"
  note. A row that does not fit is dropped whole; the footer reserve is the
  length of the worst-case footer, computed, not guessed.
- Fetching is lazy: a capped preview decodes at most the rows it shows plus a
  small chunk and one look-ahead row, and later result sets stay unread after
  a truncated one. `exhausted` is the only thing that lets the footer say
  `total_rows=` instead of `total_rows>=`. `tail_rows` opts into reading the
  whole result to keep its last rows (a ring of zero-copy batch slices, formatted
  only at the end).
- Errors are raised by `check_error` as usual, so a failure after earlier rows
  produces JSON and never a success footer. `error_json` maps SQL Server
  error numbers (then SQLSTATE) to a category and a recovery hint; it never runs
  SQL.

## Bulk copy

`Cursor.bulkcopy` / `bulkcopy_arrow` call `DDBCBulkCopyRows` / `DDBCBulkCopyArrow`
with the cursor's `SqlHandle`. The load takes the connection's wire like any
statement (`with_exclusive_wire`, GIL released), so it runs on the cursor's own
session: a `#temp` table is visible (upstream #492) and, in manual-commit mode,
the load is part of the open transaction (upstream opened a second, autonomous
connection). Because it holds the wire, a source that itself reads the same
connection cannot be streamed: `Cursor` sources are read first
(`Cursor._bulk_row_iterator`) and a `cursor.arrow_stream()` is drained before the
load starts. Other generators that query the same connection from inside the
load would deadlock.

mssql-tds's `BulkCopy::write_to_server_zerocopy` drives the load from an
iterator of `BulkLoadRow`s. Serializing a row cell by cell through its async
serializer costs ~45 ns (int) to ~90 ns (decimal) per cell, many times the cost
of the bytes. So both sources encode a *window* of rows (up to 8 MiB / 8192
Arrow rows, 4 MiB / 4096 / 20 ms of Python rows) into one buffer in a tight
synchronous loop and each `BulkLoadRow` writes its slice with one
`write_raw_bytes`. `bulk/wire.rs` reproduces `TdsValueSerializer` byte for byte
for ints, bit, real/float, decimal, money, date/time types, wide and narrow text
(PLP included), binary and GUID, driven by the same `TdsTypeContext` facts
(`Layout`: fixed type, fixed length, PLP, precision). Cells without an encoder
(xml, json, vector, ...) stay on the `ColumnValues` path: window `Step`s alternate
between encoded runs and single serialized cells. Every encoded type is tested
against `mssql_py_core`'s stored result (`tests/common_issues/test_rs_bulk_native*.py`).

A value that cannot be encoded does not end the stream quietly: the window
becomes one row whose `write_to_packet` fails, which aborts the load as any bad
value does (nothing commits for `batch_size=0`). A Python exception from a row
iterator is re-raised unchanged.

Planning (Arrow type x SQL type) happens after the destination metadata is read
and before any row is sent; an unsupported pairing raises
`ddbc_bindings.BulkUnsupported` (a `ValueError`). Python falls back to
`mssql_py_core` only for cursors without a native handle.

## executemany

`SQLExecuteMany` normalises each column to one SQL type
(`params/batch.rs`; DECIMAL/NUMERIC columns take their type from
`paramInfos`), then sends all rows as `sp_execute` RPCs against one
prepared handle, 2000 rows per TDS request
(`begin_execute_prepared_batch`, which keeps `OUTPUT` result sets). The
result sets of every chunk but the last are buffered; the last chunk's
stream stays on the wire like an ordinary `execute`.

Write-path fast cells (`statement/batch_rpc.rs`): naive `datetime.datetime`
cells are read through the C-level accessors, and plain `Decimal` cells are
parsed from `str()` without `BigDecimal`. `executemany` sends `sp_execute`
parameters positionally (no per-row `@Pn` names). Single-statement binding
(`params::build_rpc_params`) uses the same cells via `fast_param_cell`, which
reproduces `py_to_sql_type`'s inferred type exactly
(`tests/common_issues/test_rs_param_fast_cells.py`). The vendored
`execute_sp_executesql` no longer clones every parameter to build the
declaration string.

## Pooling

`pooling.rs` keeps idle `TdsClient`s per (connection string, pool key). A
key's `size` counts idle and checked-out sessions, capped at `max_size`
(an acquire past the cap raises instead of waiting). The capacity is a
`Lease` owned by the `Connection`: it is released when a connect fails,
when `close()` finds the session unusable, or when the connection is
dropped. `disable_pooling` / `close_pooling` start a new generation, so a
stale lease cannot touch the replacement pool.

Checkout arms RESETCONNECTION (`prepare_reset_connection`), applied by the
server with the next request — no extra round trip — and queues
`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`, which the reset does not
restore. `close()` rolls back an open transaction and returns the session
to the pool; a dead session or a failed rollback discards it and raises.
Idle sessions older than the idle timeout are closed by any later acquire.

With a `token_factory`, the factory runs (GIL held) when a physical
connection is opened, never on a plain pool hit. A pooled session whose
token expires within 300 s (or whose expiry is unknown while it holds a
token) is re-validated: if the factory returns the same token with a safe
expiry the session is reused, otherwise it and its siblings with the same
token are closed and a new connection is opened with the fresh token.

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
