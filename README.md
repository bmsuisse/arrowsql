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

There are two options.

**1. Threads over the sync driver (recommended).** The Rust core releases the
GIL for all network I/O, so `asyncio.to_thread` gives real concurrency. Eight
one-second queries finish in about 1.1 s:

```python
import asyncio, mssql_python

def total_for(trip_id: int):
    with mssql_python.connect(conn_str) as conn:  # pooled
        cur = conn.cursor()
        cur.execute("SELECT total_amount FROM dbo.yellow_trips WHERE trip_id = ?", (trip_id,))
        return cur.fetchone()[0]

async def main():
    totals = await asyncio.gather(*(asyncio.to_thread(total_for, i) for i in range(1, 9)))
    print(totals)

asyncio.run(main())
```

**2. Native async (preview).** Upstream mssql-python ships an experimental
async API backed by Microsoft's `mssql_py_core`, not by this project's Rust
core. It is private (`_AsyncConnection`), emits a `FutureWarning`, and its
signatures may change without notice:

```python
import asyncio
from mssql_python.async_query import _AsyncConnection

async def main():
    async with await _AsyncConnection.connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT TOP 3 trip_id, total_amount FROM dbo.yellow_trips WHERE PULocationID = ?", (132,))
        print(await cur.fetchall())
        await cur.close()

asyncio.run(main())
```

Native async on this project's own core is on the roadmap
([upstream #9](https://github.com/microsoft/mssql-python/issues/9)).

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
Driver 18). These numbers come from a shared dev box under load from other
jobs, so treat them as indicative; compare ratios, not absolute times.

**Arrow** (release build, median of 5):

| workload | upstream | this project | speedup |
|---|---:|---:|---:|
| `cursor.arrow()`, 100k rows | 0.163 s | 0.067 s | 2.4x |
| `cursor.arrow()`, 3M rows | 4.253 s | 2.283 s | 1.9x |
| DuckDB aggregate over `arrow_reader()`, 3M | 4.362 s | 2.006 s | 2.2x |
| `read_arrow(..., partitions=4)`, 3M | 4.253 s | 0.832 s | 5.1x |
| `read_arrow(..., partitions=8)`, 3M | 4.253 s | 0.591 s | 7.2x |
| DuckDB over `read_arrow(..., partitions=8)`, 3M | 4.362 s | 0.459 s | 9.5x |

On a single connection SQL Server's row serialization is the limit: draining
3M rows into a sink that discards every value already takes about 2 s. The
partitioned read is how to get past it.

**Rows** (release build, median of 7):

| workload | upstream | this project | speedup |
|---|---:|---:|---:|
| `fetchone()` loop, 100k rows × 20 cols | 0.502 s | 0.274 s | 1.8x |
| 1,000 point lookups | 0.373 s | 0.235 s | 1.6x |
| `fetchmany(10_000)`, 3M rows × 4 cols | 1.701 s | 1.240 s | 1.4x |
| `fetchall()`, 100k rows × 20 cols | 0.384 s | 0.289 s | 1.3x |

**All drivers.** pyodbc, pyodbc `fast_executemany`, connectorx, arrow-odbc,
pymssql, FastMssql and `bcp`, across 58 workloads:
[benchmarks/RESULTS.md](benchmarks/RESULTS.md). This is a quick run on a dev
build of the port, before the native Arrow merge. Headline: the port is the
fastest Python driver on every row-based read (1.38x geomean vs upstream).
Writes are about even with upstream (0.91x), and `bcp` is still 4x faster on
bulk loads, so that is the next target.

```bash
python benchmarks/run.py              # everything (benchmarks/README.md has filters)
python benchmarks/report.py           # -> benchmarks/RESULTS.md + results/report.html
```

Methodology, profiles and the compression findings are in
[docs/PERFORMANCE_RESEARCH.md](docs/PERFORMANCE_RESEARCH.md).

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
