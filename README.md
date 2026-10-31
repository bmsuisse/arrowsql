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
- Native bulk copy (rows and Arrow) and parallel loads with
  `mssql_python.write_arrow(conn_str, table, source, workers=N)`.
- Native asyncio: `mssql_python.aio` with a built-in pool.
- Free-threaded CPython (3.15t) support; zero required dependencies.
- `sqlstate` and `native_error` on every exception
  ([upstream #581](https://github.com/microsoft/mssql-python/issues/581)).

## Usage

Same API as mssql-python (DB-API 2.0, `?` parameters):

```python
import mssql_python

conn_str = "Server=localhost,1433;Database=nyctaxi;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes"

with mssql_python.connect(conn_str) as conn:
    cur = conn.cursor()
    cur.execute("SELECT trip_id, total_amount FROM dbo.yellow_trips WHERE PULocationID = ?", (132,))
    for trip_id, total in cur.fetchmany(10):
        print(trip_id, total)
```

### Arrow, DuckDB, polars

```python
import duckdb, mssql_python

cur = mssql_python.connect(conn_str).cursor()

# Upstream-compatible (needs pyarrow): pyarrow.Table / RecordBatchReader
table = cur.execute("SELECT * FROM dbo.yellow_trips_100k").arrow()

# Zero-dependency: a native Arrow table that DuckDB/polars/pyarrow read directly
t = cur.execute("SELECT * FROM dbo.yellow_trips_100k").fetchall_arrow()
duckdb.sql("SELECT PULocationID, count(*) FROM t GROUP BY 1").show()

# Streaming: the next batch is decoded in the background while DuckDB scans
stream = cur.execute("SELECT * FROM dbo.yellow_trips").arrow_stream(batch_size=65_536)
duckdb.sql("SELECT avg(total_amount) FROM stream").show()

# Partitioned parallel read over N connections (integer partition column)
big = mssql_python.read_arrow(conn_str, "SELECT * FROM dbo.yellow_trips",
                              partition_on="trip_id", partitions=8)
```

For polars use `pl.DataFrame(t)`; `pl.from_arrow(t)` returns a struct Series
for capsule streams.

### Async

`mssql_python.aio` is native asyncio on this project's Rust core: waits never
hold the GIL, `execute` returns the first rows itself, and a pool is built in.
Cancelling a task (including `asyncio.wait_for` timeouts) cancels the query on
the server and leaves the connection usable. Unix only (uses `loop.add_reader`).

```python
import asyncio
from mssql_python import aio

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
(`mssql_python.async_query._AsyncConnection`, backed by Microsoft's
`mssql_py_core`) stays available with the `[async-preview]` extra.

### Errors

```python
try:
    cur.execute("INSERT INTO t (id) VALUES (?)", (1,))
except mssql_python.IntegrityError as exc:
    print(exc.sqlstate, exc.native_error)  # '23000' 2627
```

## Benchmarks

NYC yellow taxi data, January 2024 (2,964,624 rows × 20 columns), SQL Server
2022 in podman on the same host. Upstream is mssql-python 1.16.0 (C++ + ODBC
Driver 18). Release build of this project at commit `1182ec1`, median of 5,
one measurement at a time (machine-wide lock), but on a shared dev box with
other jobs running: treat absolute times as indicative and compare ratios.

**Reads**

| workload | upstream | this project | speedup | best other driver |
|---|---:|---:|---:|---|
| `fetchall()`, 100k rows × 20 cols | 0.392 s | 0.142 s | 2.8x | FastMssql 0.660 s |
| `fetchmany(10_000)`, 100k rows | 0.367 s | 0.113 s | 3.2x | FastMssql 0.538 s |
| `fetchone()` loop, 100k rows | 0.450 s | 0.240 s | 1.9x | FastMssql 0.680 s |
| `fetchmany`, 3M rows × 20 cols | 12.93 s | 3.09 s | 4.2x | pyodbc 32.0 s |
| decimal-heavy `fetchmany` | 1.844 s | 0.430 s | 4.3x | pyodbc 5.0 s |
| string-heavy `fetchmany` | 1.476 s | 0.819 s | 1.8x | pyodbc 1.10 s |
| `cursor.arrow()`, 100k rows | 0.155 s | 0.061 s | 2.6x | connectorx 0.350 s |
| decimal-heavy Arrow | 0.800 s | 0.238 s | 3.4x | connectorx 1.43 s |
| DuckDB over the Arrow stream, 3M rows | 4.595 s | 1.987 s | 2.3x | connectorx 9.85 s |
| `read_arrow(..., partitions=8)`, 3M rows | 4.253 s | 0.591 s | 7.2x | connectorx (8 partitions) 2.63 s |

On a single connection SQL Server's row serialization is the limit (draining
3M rows into a sink that discards every value takes about 2 s); the
partitioned read is how to get past it.

**Writes**

| workload | upstream | this project | speedup | best other |
|---|---:|---:|---:|---|
| `bulkcopy_arrow`, 3M rows, heap + TABLOCK | 12.35 s | 6.07 s | 2.0x | `bcp` 6.67 s |
| `write_arrow(..., workers=8)`, 3M rows | 12.35 s | 1.82 s | 6.8x | `bcp` 6.67 s |
| `bulkcopy` (row tuples), 100k rows | 1.561 s | 0.716 s | 2.2x | `bcp` 0.382 s |
| `executemany`, 100k rows | 1.301 s | 1.533 s | 0.85x | pyodbc `fast_executemany` 0.801 s |

**Latency and concurrency**

| workload | upstream | this project | speedup | best other driver |
|---|---:|---:|---:|---|
| 1,000 point lookups | 0.328 s | 0.158 s | 2.1x | pyodbc 0.132 s |
| 16 threads × point lookups | 2.395 s | 0.429 s | 5.6x | FastMssql 0.612 s |
| `SELECT 1` × 5,000 | 0.829 s | 0.741 s | 1.1x | pyodbc 0.674 s |
| connect + close × 50 | 0.199 s | 0.194 s | 1.0x | pyodbc 0.186 s |
| async point lookups, 64 concurrent (`aio`) | ~7k q/s | ~40k q/s | ~5x | FastMssql ~15k q/s |
| free-threaded 3.15t, 16 threads point lookups | 4.3k q/s | 18.7k q/s | 4.4x | – |

Still behind: `executemany` (pyodbc `fast_executemany` is about 2x faster) and
the tiniest round trips (`SELECT 1`, single-row lookups), where pyodbc is
10-20 % faster. Both are being worked on.

Full multi-driver matrix (pyodbc, connectorx, arrow-odbc, pymssql, FastMssql,
`bcp`): [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

```bash
python benchmarks/run.py              # everything (benchmarks/README.md has filters)
python benchmarks/report.py           # -> benchmarks/RESULTS.md + results/report.html
```

Methodology, profiles and the compression findings are in
[docs/PERFORMANCE_RESEARCH.md](docs/PERFORMANCE_RESEARCH.md).

## Install

```bash
pip install mssql-python-rs-port              # zero required dependencies
pip install 'mssql-python-rs-port[arrow]'     # + pyarrow for arrow() / bulkcopy_arrow
pip install 'mssql-python-rs-port[azure]'     # + azure-identity credentials
```

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
