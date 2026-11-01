# Features in detail

Moved from the README.


- Faster row fetches than upstream on every row-based read in our benchmarks.
- Native Arrow: upstream's `cursor.arrow()`, `arrow_batch()` and
  `arrow_reader()` decode straight into Arrow with the GIL released.
- A zero-dependency Arrow API: `fetchall_arrow()`, `fetchmany_arrow()` and
  `arrow_stream()` return objects that DuckDB, polars and pyarrow read
  directly via the Arrow PyCapsule interface.
- Partitioned parallel reads: `arrowsql.read_arrow(conn_str, query,
  partition_on=, partitions=)`. Slices are equal-width in the key (skewed
  keys are not rebalanced) and the partitions are not one snapshot.
  `arrowsql.read_arrow_stream(...)` streams the same read (bounded buffer,
  unordered batches) so DuckDB or `deltalake` work while rows still arrive, and
  `partition_bounds=[...]` takes typed boundaries (int, date/datetime, Decimal,
  str) instead of probing MIN/MAX.
- Native bulk copy (rows and Arrow) and parallel loads with
  `arrowsql.write_arrow(conn_str, table, source, workers=N)` and
  `arrowsql.write_rows(conn_str, table, rows, workers=N)` (heap: TABLOCK;
  clustered columnstore: no TABLOCK, batches of at least 102,400 rows; other
  indexed targets use one worker).
- Native asyncio: `arrowsql.aio` with a built-in pool.
- Free-threaded CPython (3.15t) support; zero required dependencies.
- `sqlstate` and `native_error` on every exception
  ([upstream #581](https://github.com/microsoft/mssql-python/issues/581)).

## Async

`arrowsql.aio` is native asyncio on this project's Rust core: waits never
hold the GIL, `execute` returns the first rows itself, and a pool is built in.
Cancelling a task (including `asyncio.wait_for` timeouts) cancels the query on
the server and leaves the connection usable. Unix only (uses `loop.add_reader`).

```python
import asyncio
from arrowsql import aio

async def main():
    async with await aio.connect(conn_str) as conn:
        rows = await conn.fetch("SELECT TOP 3 trip_id, total_amount FROM dbo.yellow_trips WHERE PULocationID = ?", 132)

    pool = await aio.create_pool(conn_str, min_size=4, max_size=32)
    total = await pool.fetchval("SELECT SUM(total_amount) FROM dbo.yellow_trips")
    async with pool.acquire() as conn:
        cur = conn.cursor()
        await cur.execute("SELECT * FROM dbo.yellow_trips_100k")
        table = await cur.fetchall_arrow()          # native Arrow, DuckDB-ready
    await pool.close()

asyncio.run(main())
```

The sync driver also releases the GIL for all network I/O, so
`asyncio.to_thread` over it works too. Upstream's private async preview
(`arrowsql.async_query._AsyncConnection`, backed by Microsoft's
`mssql_py_core`) stays available with the `[async-preview]` extra.

## Errors

```python
try:
    cur.execute("INSERT INTO t (id) VALUES (?)", (1,))
except arrowsql.IntegrityError as exc:
    print(exc.sqlstate, exc.native_error)  # '23000' 2627
```

