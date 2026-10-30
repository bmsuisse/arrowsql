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
extra round trip) when they are reused.

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
