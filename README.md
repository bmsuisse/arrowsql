# arrowsql (mssql-python on a Rust core)

[mssql-python](https://github.com/microsoft/mssql-python) with its C++/ODBC
core replaced by Rust: a PyO3 extension over Microsoft's native TDS crate
[`mssql-tds`](https://github.com/microsoft/mssql-rs). No ODBC driver, no
unixODBC, same Python API: `import arrowsql`, or keep `import mssql_python`
(a compatibility alias for existing code).

Status: work in progress (alpha, not yet on PyPI). The upstream Python layer is vendored unchanged
where possible, and the upstream test suite is the compatibility bar.

## What it adds

- Faster row fetches than upstream on every row-based read in our benchmarks.
- Native Arrow: upstream's `cursor.arrow()`, `arrow_batch()` and
  `arrow_reader()` decode straight into Arrow with the GIL released.
- A zero-dependency Arrow API: `fetchall_arrow()`, `fetchmany_arrow()` and
  `arrow_stream()` return objects that DuckDB, polars and pyarrow read
  directly via the Arrow PyCapsule interface.
- Partitioned parallel reads: `arrowsql.read_arrow(conn_str, query,
  partition_on=, partitions=)`. Slices are equal-width in the key (skewed
  keys are not rebalanced) and the partitions are not one snapshot.
- Native bulk copy (rows and Arrow) and parallel loads with
  `arrowsql.write_arrow(conn_str, table, source, workers=N)` and
  `arrowsql.write_rows(conn_str, table, rows, workers=N)` (heap: TABLOCK;
  clustered columnstore: no TABLOCK, batches of at least 102,400 rows; other
  indexed targets use one worker).
- Native asyncio: `arrowsql.aio` with a built-in pool.
- Free-threaded CPython (3.15t) support; zero required dependencies.
- `sqlstate` and `native_error` on every exception
  ([upstream #581](https://github.com/microsoft/mssql-python/issues/581)).

## Usage

Same API as mssql-python (DB-API 2.0, `?` parameters):

```python
import arrowsql

# TrustServerCertificate=yes is for local testing only, see "Security note"
conn_str = "Server=localhost,1433;Database=nyctaxi;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes"

with arrowsql.connect(conn_str) as conn:
    cur = conn.cursor()
    cur.execute("SELECT trip_id, total_amount FROM dbo.yellow_trips WHERE PULocationID = ?", (132,))
    for trip_id, total in cur.fetchmany(10):
        print(trip_id, total)
```

### Arrow, DuckDB, polars

```python
import duckdb, arrowsql

cur = arrowsql.connect(conn_str).cursor()

# Upstream-compatible (needs pyarrow): pyarrow.Table / RecordBatchReader
table = cur.execute("SELECT * FROM dbo.yellow_trips_100k").arrow()

# Zero-dependency: a native Arrow table that DuckDB/polars/pyarrow read directly
t = cur.execute("SELECT * FROM dbo.yellow_trips_100k").fetchall_arrow()
duckdb.sql("SELECT PULocationID, count(*) FROM t GROUP BY 1").show()

# Streaming: the next batch is decoded in the background while DuckDB scans
stream = cur.execute("SELECT * FROM dbo.yellow_trips").arrow_stream(batch_size=65_536)
duckdb.sql("SELECT avg(total_amount) FROM stream").show()

# Partitioned parallel read over N connections (integer partition column)
big = arrowsql.read_arrow(conn_str, "SELECT * FROM dbo.yellow_trips",
                              partition_on="trip_id", partitions=8)
```

For polars use `pl.DataFrame(t)`; `pl.from_arrow(t)` returns a struct Series
for capsule streams.

### Async

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

### Errors

```python
try:
    cur.execute("INSERT INTO t (id) VALUES (?)", (1,))
except arrowsql.IntegrityError as exc:
    print(exc.sqlstate, exc.native_error)  # '23000' 2627
```

### Agent mode

A CLI and Python API for LLM agents and terminals (ported from
[arrowbricks](https://github.com/bmsuisse/arrowbricks)): compact typed TSV (or a
Markdown table), explicit truncation, and actionable JSON errors. Rows are
fetched lazily and formatted in Rust, without building Python row objects, so a
capped preview of a huge table costs little more than the statement's start.

```bash
export MSSQL_CONNECTION_STRING='Server=...;Database=...;Uid=...;Pwd=...'   # or DB_CONNECTION_STRING
arrowsql-agent -c "SELECT 1 AS answer, N'x' AS label"      # python -m arrowsql.agent works too
```

```text
answer:int	label:nvarchar(1)
1	x

rows_shown=1; complete=true; total_rows=1
```

```python
from arrowsql.agent import query

text = query(conn_or_connection_string, "SELECT TOP 100 * FROM dbo.orders WHERE id > ?", [42],
             format="tsv",          # or "markdown"
             max_bytes=10_000,      # strict ceiling for the whole string, footer included
             max_rows=1000, max_cell_chars=500, timeout_s=60,
             tail_rows=0)           # N > 0: also keep the last N rows (reads the whole result)
```

- The header carries SQL types (`name:decimal(18,2)`). Tab, newline, CR and `\`
  are backslash-escaped, control characters become `\uXXXX`, NULL is `\N`
  (`NULL` in Markdown, where text is quoted so `"NULL"` differs), binary is `0x...`,
  `datetimeoffset` is shown in UTC, `sql_variant` by its value.
- The footer is `rows_shown=N; complete=true|false; total_rows=N` and says
  `total_rows>=N` unless every row was read. Clipped cells end in
  `…[truncated]` and set `cells_truncated=true`. With `tail_rows`, the footer
  reads `sample=head+tail; rows_omitted=K` and the total is exact.
- A batch prints every result set (footers from the second on start with
  `result_set=k`), `-- rows_affected=N` for DML and `-- message: ...` for
  PRINT / low-severity RAISERROR. After a truncated result set the rest of the
  batch is left unread. Output beyond the byte budget is dropped whole and noted.
- Failures return (CLI: stderr, exit 1) one JSON object: `category`
  (`missing_table`, `missing_column`, `syntax_error`, `authentication`,
  `permission_denied`, `deadlock`, `timeout`, `lock_timeout`, `constraint_violation`,
  `conversion_error`, `connection`, `transient`, ...), `message` (the server's own text),
  `next_step`, `retryable`, `sqlstate`, `native_error`. Advice only: SQL is never
  rewritten, repaired or retried. A failure after earlier rows gives the error, not
  a partial result with a success footer. Bad arguments exit 2, Ctrl-C exits 130.
- `query_cursor(cursor, sql)` runs on an existing cursor; `render_cursor(cursor)`
  renders one that has just executed. String connections use autocommit.

## Support

### Platforms and Python versions

Honest status: **only Linux x86_64 (glibc) is built and tested by hand so far**
(CPython 3.13, plus a free-threaded build). CI (`.github/workflows/ci.yml`) runs the
test suites on Linux x86_64 against SQL Server 2022. The wheel workflow
(`.github/workflows/wheels.yml`) is new and builds the matrix below as workflow
artifacts; nothing has been published to PyPI and no wheel other than Linux
x86_64 has been run against a database.

| platform | wheel built by CI | tested against SQL Server | notes |
|---|---|---|---|
| Linux x86_64, glibc (manylinux_2_28) | yes | yes (CI + local) | reference platform |
| Linux aarch64, glibc | yes | no | built natively on an arm runner; never imported |
| Linux musl (x86_64, aarch64) | yes | no | |
| macOS x86_64 / arm64 | yes | no | arm64 wheel gets an import smoke test in CI, x86_64 is never imported; `aio` should work (Unix) |
| Windows x64 | yes | no | `aio` is not available (needs `loop.add_reader`); wheel gets an import smoke test in CI |

Python: CPython 3.10 to 3.15 and free-threaded 3.14t and 3.15t are built. CI runs
the suites on 3.13 (plus the upstream baseline), 3.15 and 3.15t (3.15t is
non-gating while optional test dependencies lack free-threaded wheels). PyPy and 32-bit platforms are not supported.

### Connection string

Parsed natively (`src/connstr.rs`) and by the vendored Python layer.

| keyword | status |
|---|---|
| `Server` / `Address` / `Data Source`, `Database`, `Uid`, `Pwd`, `APP`, `WSID`, `Language` | supported |
| `Encrypt` (`yes`/`no`/`strict`), `TrustServerCertificate`, `HostNameInCertificate`, `ServerCertificate` | supported (rustls; TLS 1.2 / 1.3). `Encrypt=strict` is implemented but not verifiable against the local SQL Server 2022 container |
| `ConnectRetryCount`, `ConnectRetryInterval`, `MultiSubnetFailover`, `ApplicationIntent`, `PacketSize`, `Connection Timeout` | supported |
| `Trusted_Connection=yes` / `Integrated Security=sspi` | Kerberos via GSSAPI (library loaded at runtime) on Unix, SSPI on Windows; not tested here |
| `Authentication=ActiveDirectoryDefault`, `ActiveDirectoryInteractive`, `ActiveDirectoryDeviceCode`, `ActiveDirectoryMSI`, `ActiveDirectoryServicePrincipal` | acquired in Python with `azure-identity` (the `[azure]` extra) and passed as an access token; not tested against Entra ID here |
| `token_provider=` / `attrs_before={1256: ...}` (access token) | supported |
| `Authentication=ActiveDirectoryPassword`, `SqlPassword`, `ActiveDirectoryIntegrated`, NTLM (`DOMAIN\user`) | not supported |
| Always Encrypted (`ColumnEncryption`) | not supported |

### Environment variables

| variable | effect |
|---|---|
| `MSSQL_RS_BUSY_POLL` | `0`, `off` or `false` disables the short busy poll for the first response byte (default on, at most `min(cpus / 4, 2)` spinning threads; it trades a little CPU for lower latency on very short round trips) |
| `MSSQL_RS_MULTISUBNET_FAILOVER_DEFAULT` | `true` / `false`: deployment-wide default for `MultiSubnetFailover` when the connection string does not set it |
| `MSSQL_PYTHON_NATIVE_PROVIDER` | upstream's ODBC provider selector; has no effect on the Rust path |
| `DB_CONNECTION_STRING` | used by the test suites and benchmarks only, never by the library |

### Do not install both packages

The import name is `arrowsql`. For drop-in use the wheel also ships a
`mssql_python` alias package (every `mssql_python.X` is the same module as
`arrowsql.X`), which has the same name as Microsoft's
[mssql-python](https://github.com/microsoft/mssql-python). Installing both into one
environment makes them overwrite each other's files. Use one or the other, in a
separate virtual environment.

### Security note

The quick-start connection strings above use `TrustServerCertificate=yes`, which
switches certificate validation off. That is for **local testing against a
container only**. For anything else use a certificate the client trusts (or
`ServerCertificate=` / `HostNameInCertificate=`) and keep `Encrypt=yes`.

## Benchmarks

NYC yellow taxi data, January 2024 (2,964,624 rows × 20 columns), SQL Server
2022 in podman on the same host. Upstream is mssql-python 1.16.0 (C++ + ODBC
Driver 18); pyodbc runs with `fast_executemany=True` against the same ODBC
driver. Release build of this project, median of 5, one measurement at a time
(machine-wide lock), on a shared dev box with other jobs running (load average
3.6-5.5): treat absolute times as indicative and compare ratios. Unmarked rows
are from commit `d5476b1`; rows marked † are from commit `1182ec1`, rows marked
* from earlier dedicated runs (Arrow agent, aio agent, free-threaded agent),
rows marked ‡ from commit `b98b64d` at load average 14-20.

**Reads**

| workload | upstream | this project | speedup | pyodbc / best other |
|---|---:|---:|---:|---|
| `fetchall()`, 100k rows × 20 cols | 0.351 s | 0.117 s | 3.0x | pyodbc 0.865 s |
| `fetchmany(10_000)`, 100k rows | 0.314 s | 0.098 s | 3.2x | pyodbc 0.851 s |
| `fetchone()` loop, 100k rows | 0.436 s | 0.197 s | 2.2x | pyodbc 0.720 s |
| `fetchmany`, 3M rows × 20 cols | 9.74 s | 2.75 s | 3.5x | pyodbc 27.0 s |
| decimal-heavy `fetchmany` † | 1.844 s | 0.430 s | 4.3x | pyodbc 5.0 s |
| string-heavy `fetchmany` † | 1.476 s | 0.819 s | 1.8x | pyodbc 1.10 s |
| `cursor.arrow()`, 100k rows | 0.146 s | 0.060 s | 2.4x | connectorx 0.350 s † |
| decimal-heavy Arrow † | 0.800 s | 0.238 s | 3.4x | connectorx 1.43 s |
| DuckDB over the Arrow stream, 3M rows † | 4.595 s | 1.987 s | 2.3x | connectorx 9.85 s |
| `read_arrow(..., partitions=8)`, 3M rows * | 4.253 s | 0.591 s | 7.2x | connectorx (8 partitions) 2.63 s |

On a single connection SQL Server's row serialization is the limit (draining
3M rows into a sink that discards every value takes about 2 s); the
partitioned read is how to get past it.

**Writes**

| workload | upstream | this project | speedup | pyodbc / best other |
|---|---:|---:|---:|---|
| `bulkcopy_arrow`, 3M rows, heap + TABLOCK † | 12.35 s | 6.07 s | 2.0x | `bcp` 6.67 s |
| `write_arrow(..., workers=8)`, 3M rows † | 12.35 s | 1.82 s | 6.8x | `bcp` 6.67 s |
| `bulkcopy` (row tuples), 100k rows † | 1.561 s | 0.716 s | 2.2x | `bcp` 0.382 s |
| `executemany`, 100k rows | 1.257 s | 0.653 s | 1.9x | pyodbc 0.762 s |
| `INSERT ... VALUES`, 200 rows × 5 params per statement, 10k rows | 0.120 s | 0.094 s | 1.3x | pyodbc 0.096 s |
| single-row `INSERT` × 1,000, one commit ‡ | 0.159 s | 0.129 s | 1.2x | pyodbc 0.143 s |

**Latency and concurrency**

| workload | upstream | this project | speedup | pyodbc / best other |
|---|---:|---:|---:|---|
| 1,000 point lookups ‡ | 0.350 s | 0.122 s | 2.9x | pyodbc 0.157 s |
| 16 threads × point lookups † | 2.395 s | 0.429 s | 5.6x | FastMssql 0.612 s |
| `SELECT 1` × 5,000 ‡ | 0.864 s | 0.730 s | 1.2x | pyodbc 0.727 s |
| connect + close × 50 ‡ | 0.150 s | 0.120 s | 1.2x | pyodbc 0.130 s |
| async point lookups, 64 concurrent (`aio`) * | ~7k q/s | ~40k q/s | ~5x | FastMssql ~15k q/s |
| free-threaded 3.15t, 16 threads point lookups * | 4.3k q/s | 18.7k q/s | 4.4x | – |

Point lookups are 15-20 % ahead of pyodbc (three runs); `SELECT 1` is even
(within 2 % either way). Both drivers make one send and one receive per query.
For short queries the synchronous driver busy-polls the socket for the first
response byte instead of sleeping in epoll (adaptive, only while recent waits
are under 300 µs, at most two threads, off on machines with fewer than 4
CPUs). That trades one busy core during the wait for the scheduler's wake-up
latency; set `MSSQL_RS_BUSY_POLL=0` to turn it off. A repeated result shape
also reuses its parsed column metadata.

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
# not on PyPI yet; wheels are CI artifacts (see Support)
pip install arrowsql              # zero required dependencies
pip install 'arrowsql[arrow]'     # + pyarrow for arrow() / bulkcopy_arrow
pip install 'arrowsql[azure]'     # + azure-identity credentials
```

## Build and test

```bash
uv venv -p 3.13 .venv && VIRTUAL_ENV=.venv uv pip install -r requirements-dev.txt
scripts/build.sh                 # dev build into .venv (--release for benchmarks)

# SQL Server 2022 in podman; put DB_CONNECTION_STRING / MSSQL_SA_PASSWORD in .env.local
source scripts/env.sh my-id      # private database per run
cd tests/upstream && ../../.venv/bin/python -m pytest -q
```

CI runs the same suites on every push; `scripts/check_upstream_baseline.py` checks the upstream results against the baseline. Test scope and the upstream baseline: [docs/TEST_SCOPE.md](docs/TEST_SCOPE.md).
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
