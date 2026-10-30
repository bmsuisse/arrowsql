# Native async (`mssql_python.aio`)

`mssql_python.aio` is an asyncio API that runs on this project's Rust core.
It is separate from upstream's private preview (`mssql_python.async_query`,
backed by Microsoft's `mssql_py_core`), which stays untouched.

## API

```python
from mssql_python import aio

async with await aio.connect(conn_str, autocommit=True) as conn:
    cur = conn.cursor()
    await cur.execute("SELECT id, name FROM t WHERE id > ?", 10)
    row = await cur.fetchone()          # Row objects, like the sync driver
    rows = await cur.fetchmany(100)
    rest = await cur.fetchall()
    async for row in cur: ...
    table = await cur.fetchall_arrow()  # native ArrowTable (DuckDB/polars/pyarrow)
    await cur.executemany("INSERT INTO t VALUES (?, ?)", rows)
    await cur.nextset()
    rows = await conn.fetch("SELECT ...", *params)      # one round trip, all rows
    row = await conn.fetchrow("SELECT ...", *params)
    value = await conn.fetchval("SELECT count(*) FROM t")
    await conn.commit(); await conn.rollback()

pool = await aio.create_pool(conn_str, min_size=2, max_size=16)
rows = await pool.fetch("SELECT ... WHERE id = ?", 42)   # acquire+query+release in one hop
async with pool.acquire() as conn: ...
await pool.close()
```

Exceptions are the regular `mssql_python` classes (`IntegrityError`,
`ProgrammingError`, ...) with `sqlstate` and `native_error`.
`asyncio.CancelledError` (task cancel, `asyncio.wait_for` timeout) sends a
TDS attention to the server, so the query stops there too, and the
connection stays usable. `aio.connect(..., timeout=n)` sets a per-query
timeout in seconds.

Defaults: `aio.connect` uses `autocommit=False` like the sync driver (DB-API);
a pool uses `autocommit=True`, because the `pool.fetch()`-style shortcuts have
no place to commit. Pooled sessions are reset (TDS RESETCONNECTION flag, no
extra round trip) when they are reused; the server-side reset still costs
roughly 50-80 µs per checkout, so `create_pool(..., reset=False)` skips it for
pools whose queries leave no session state behind (open transactions are
always rolled back). `tuple_rows=True` returns plain tuples instead of `Row`
objects.

Limits: untyped `None` parameters get their type from
`sp_describe_undeclared_parameters`, which cannot see `#temp` tables (same as
the sync driver); bind such values with a typed expression
(`CAST(? AS varbinary(max))`) there.

Unix only for now (the completion hub uses `loop.add_reader` on a socket
pair); Windows' Proactor loop has no `add_reader`.

## How it works

```
asyncio loop thread                         tokio runtime (shared with sync API)
-------------------                         ----------------------------------
conn.execute() -> Rust: convert params,     task: lock session (tokio Mutex),
  create loop future, spawn task  -------->   execute, decode first rows into a
  await future                                RowBuffer (no GIL, no Python objects)
                                              push (request id, outcome) to the
hub.drain() <-- socket readable  <---------   hub queue; write 1 byte only if
  convert outcomes, fut.set_result()          the hub was not already signalled
```

- Worker threads never take the GIL. Every Python object is created on the
  loop thread, in `Hub.drain`, which completes all finished operations of one
  wake-up in a single call (completions are batched under load).
- One `Hub` per event loop: a socket pair registered with `loop.add_reader`,
  a queue, and the table of pending futures (only touched on the loop thread).
- One TDS session per `AsyncConnection`, behind a `tokio::sync::Mutex`, so
  operations on one connection queue up instead of blocking a thread.
- `execute` reads the first rows of the result in the same task (up to 512
  rows / 4 MiB): a small result is complete when `execute` returns, so
  `fetchone/fetchall` after it need no second round trip into Rust. The
  prefetched rows stay in a Rust `RowBatch` and become Python objects only
  when fetched (and go straight into Arrow for `fetchall_arrow`).
- `cursor.description` and the row column maps are built once per result
  shape (cached per hub by a hash of the column metadata), not per query.
- Several cursors on one connection work like the sync driver: when a cursor
  needs the wire while another still has rows on it, those rows are drained
  into that cursor's buffer first.

Source: `src/aio/` (`hub.rs` completion hub and conversion, `session.rs`
async session state machine, `connection.rs` / `pool.rs` pyclasses),
`python/mssql_python/aio.py` (thin Python API).

### Coupling with the sync core

`src/aio` reuses, read-only: `connstr::build_context`, `params::{build_rpc_params,
ParamSet, placeholders, batch::normalize, py_types, null_types::null_of}`,
`decode::{RowBuffer, python::cell_to_py, describe}`, `diag`, `arrow::{plan::Plan,
sink::ArrowSink, export::ArrowTable}`, `statement::BatchSink` (trait only),
`runtime::runtime()`. The only edits outside `src/aio` are `mod aio` +
`aio::register` in `lib.rs` and making `decode::python::cell_to_py`
`pub(crate)`.

## Benchmark

`benchmarks/bench_async.py`: 2,000 point lookups
(`SELECT trip_id, total_amount FROM dbo.yellow_trips WHERE trip_id = ?`) on
the NYC taxi table, queries per second at 1/16/64/256 concurrent workers.
Release build, SQL Server 2022 in podman on the same host, Python 3.13. The
box was shared with other jobs (load average 8-9), so cells are the best of
two full runs and differences under ~15 % are noise.

| mode | 1 | 16 | 64 | 256 |
|---|---:|---:|---:|---:|
| `aio` connection per worker (`port_aio`) | **6,975** | **44,154** | 39,915 | **41,078** |
| `aio` pool, `pool.fetchrow()` (`port_aio_pool`) | 5,656 | 39,021 | **42,473** | 35,664 |
| FastMssql 0.7.7 (tiberius + tokio, bb8 pool) | 2,971 | 13,888 | 15,357 | failed¹ |
| upstream `asyncio.to_thread` over sync mssql-python | 4,988 | 8,864 | 7,654 | 7,233 |
| port `asyncio.to_thread` over sync driver | 4,628 | 8,898 | 5,346 | 7,544 |
| upstream async preview (`mssql_py_core`) | 3,037 | 6,744 | 6,984 | 6,965 |

¹ FastMssql could not open 256 pooled connections ("tls handshake eof").

So `aio` is 2.6-3.2x FastMssql at 16-64 workers, about 5x upstream's
threads and 6x upstream's async preview under concurrency, and 1.4x upstream
`to_thread` with a single worker. Why: worker threads never touch the GIL;
one wake-up of the loop completes every operation finished since the last
one; the first rows come back with `execute`; and the shape (description,
column maps) is built once per result shape, not per query.

## Sync concurrency gap

Port vs upstream with `asyncio.to_thread` over the sync driver (3 alternating
rounds, qps at 1/16/64 threads): port 3,932-4,226 / 7,684-8,465 /
5,074-5,881, upstream 4,416-4,636 / 8,915-9,366 / 7,278-8,372. A later run
with the same build gave the port 7,000-7,800 at 64, so the gap moves with
load: about 10 % single-threaded and up to ~30 % at 64 threads.

Per point lookup on one thread (`syncgap.py`: every binding call wrapped in
a timer, 3,000 lookups):

| | port | upstream |
|---|---:|---:|
| total per query | 153 µs | 145 µs |
| `DDBCSQLExecute` | 122.5 µs | 117.5 µs |
| `DDBCSQLResetStmt` | 2.6 µs | 0.6 µs |
| `DDBCSQLFetchOne` + `DDBCSQLDescribeCol` | 2.3 µs | 7.9 µs |
| Python layer (cursor.py) | ~23 µs | ~22 µs |

The Python layer costs the same in both, and the port's fetch/describe calls
are faster. The difference sits in `DDBCSQLExecute` (+5 µs) and
`DDBCSQLResetStmt` (+2 µs): every sync call does `runtime::block_on` from the
Python thread on the shared multi-thread runtime, so socket readiness is seen
by a tokio worker, which then unparks the calling thread (two thread hops per
network wait, where ODBC's blocking `recv` has one). Under many threads those
extra wake-ups add up. The tokio worker count makes no measurable
difference (1, 2, 8, 24 workers within noise), which also rules out worker
starvation.

Fixes for the core (wave-C files, not applied here):

1. Drive each sync connection's I/O on the calling thread: give every sync
   `Connection` its own current-thread tokio runtime (no threads, a reactor
   only) and `block_on` on it. The connection mutex already guarantees one
   caller at a time, so any thread may drive it, and readiness wakes the
   caller directly. Expected: the +5 µs per execute disappears and the
   high-thread-count gap with it. Touches `runtime.rs`, `connection.rs`
   (create the client inside that runtime), and every `block_on` in
   `statement.rs` / `arrow` (use the connection's runtime). The Arrow stream
   producer thread works unchanged (it takes the same locks).
2. `DDBCSQLResetStmt`: skip the slot lock / state reset when the statement has
   no results (the common case after a fully fetched point lookup).
3. Keep the per-shape description cache idea (done in `aio`) for the sync
   path: today `cursor.py` rebuilds `description` per execute; ~2-3 µs of the
   22 µs Python layer, only worth it together with (1).

## PyO3 performance guide (pyo3.rs/v0.30.1/performance), applied

- **cast instead of extract** for native types: `src/aio` only uses `cast`
  (`PySequence`, `AioRowBatch`); argument parsing is done by `#[pyo3]`
  signatures.
- **`intern!`** for every attribute / method name on hot paths
  (`create_future`, `done`, `set_result`, `set_exception`, Row slot names,
  shape attributes).
- **Rust tuples for calls** (vectorcall): `set_result((value,))`,
  `shape_factory((specs,))`.
- **No `Python::attach` from worker threads at all**: tasks hand plain Rust
  data to the hub; Python objects are created in `Hub.drain`, already on the
  loop thread with the GIL held. So there is nothing to detach around either:
  the loop thread never waits on I/O.
- **`pyo3_disable_reference_pool`**: measured with a separate release build
  (3 alternating rounds, qps at 1/16/64): aio 6,671/48,789/46,453 off vs
  6,727/45,653/40,542 on (and similar spreads in other rounds); `to_thread`
  unchanged. Within noise, maybe +10 % at 64 aio workers. Not enabled: one
  code path drops a `Py` without the GIL by design
  (`arrow::export::StreamCore::_owner`, released from a DuckDB thread when a
  consumer releases an exported C stream), which would abort with the pool
  disabled. `src/aio` itself never drops a `Py` off the GIL.
- **pyo3 0.30**: nothing in `aio` needs it; all of the above works on 0.29.

## Test results

`tests/common_issues/test_rs_aio.py`: 25 passed on Python 3.13 (dev and
release builds) and on Python 3.15.1; skipped on upstream (no `aio` module).
