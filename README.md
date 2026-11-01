<p align="center"><img src="docs/assets/logo.svg" alt="arrowsql" width="360"></p>

# arrowsql

A fast SQL Server driver for Python with a Rust core. Drop-in compatible with Microsoft's
[mssql-python](https://github.com/microsoft/mssql-python), with native Arrow, DuckDB and polars output.

[![PyPI](https://img.shields.io/pypi/v/arrowsql)](https://pypi.org/project/arrowsql/)
[![Python](https://img.shields.io/pypi/pyversions/arrowsql)](https://pypi.org/project/arrowsql/)
[![License: MIT](https://img.shields.io/pypi/l/arrowsql)](LICENSE)
[![CI](https://github.com/bmsuisse/arrowsql/actions/workflows/ci.yml/badge.svg)](https://github.com/bmsuisse/arrowsql/actions/workflows/ci.yml)

arrowsql replaces the C++/ODBC core of mssql-python with a PyO3 extension over Microsoft's native TDS
crate [`mssql-tds`](https://github.com/microsoft/mssql-rs). No ODBC driver or unixODBC is needed, and the
Python API (DB-API 2.0, `?` parameters) stays the same. Status: alpha.

## Why arrowsql

- **Faster row fetches.** 3.0x to 3.5x faster than mssql-python and 7x to 10x faster than pyodbc on
  `fetchall()` / `fetchmany()` of taxi data (100k to 3M rows x 20 columns).
- **Native Arrow, no GIL.** `cursor.arrow()` is 2.4x faster than upstream; `fetchall_arrow()` and
  `arrow_stream()` need no pyarrow and feed DuckDB, polars and pyarrow directly.
- **Parallel reads and loads.** `read_arrow(..., partitions=8)` reads 3M rows in 0.59 s (upstream
  4.25 s); `write_arrow(..., workers=8)` loads 3M rows in 1.82 s (upstream 12.35 s).
- **Native asyncio.** `arrowsql.aio` with a built-in pool: about 40k point lookups/s at 64 concurrent
  queries, against about 7k/s for upstream.
- **Agent mode.** `arrowsql-agent` CLI and `arrowsql.agent.query()` give LLMs bounded, typed output and
  structured errors.
- **Drop-in.** `import mssql_python` keeps working. Zero required dependencies.

Measured on NYC yellow taxi January 2024 (2,964,624 rows) against SQL Server 2022 in podman on the same
host; see [Benchmarks](#benchmarks) for the setup.

## Install

```bash
pip install arrowsql              # no required dependencies
pip install 'arrowsql[arrow]'     # + pyarrow, for cursor.arrow() and upstream-style Arrow APIs
pip install 'arrowsql[azure]'     # + azure-identity, for Entra ID authentication
pip install 'arrowsql[async-preview]'  # upstream's private async preview (mssql_py_core)
```

Import it as `import arrowsql`. The wheel also ships a `mssql_python` alias package, so existing
`import mssql_python` code works unchanged. Do not install arrowsql and Microsoft's `mssql-python` into
the same environment: they overwrite each other's files.

## Quick start

```python
import arrowsql

conn_str = "Server=localhost,1433;Database=nyctaxi;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes"

with arrowsql.connect(conn_str) as conn:
    cur = conn.cursor()
    cur.execute("SELECT trip_id, total_amount FROM dbo.yellow_trips WHERE PULocationID = ?", (132,))
    for trip_id, total in cur.fetchmany(10):
        print(trip_id, total)
```

Every exception carries `sqlstate` and `native_error`
([upstream #581](https://github.com/microsoft/mssql-python/issues/581)):

```python
try:
    cur.execute("INSERT INTO t (id) VALUES (?)", (1,))
except arrowsql.IntegrityError as exc:
    print(exc.sqlstate, exc.native_error)  # '23000' 2627
```

`TrustServerCertificate=yes` is for local testing only, see [Security](#security).

## Arrow, DuckDB, polars, Delta

```python
import duckdb, arrowsql
cur = arrowsql.connect(conn_str).cursor()

# Zero-dependency Arrow table: DuckDB, polars and pyarrow read it via the Arrow PyCapsule interface
t = cur.execute("SELECT * FROM dbo.yellow_trips_100k").fetchall_arrow()
duckdb.sql("SELECT PULocationID, count(*) FROM t GROUP BY 1").show()

# Streaming: the next batch is decoded in the background while DuckDB scans
stream = cur.execute("SELECT * FROM dbo.yellow_trips").arrow_stream(batch_size=65_536)
duckdb.sql("SELECT avg(total_amount) FROM stream").show()

# Partitioned parallel read over 8 connections (integer partition column)
big = arrowsql.read_arrow(conn_str, "SELECT * FROM dbo.yellow_trips", partition_on="trip_id", partitions=8)

# The same read as a stream; closing it early cancels the running statements
with arrowsql.read_arrow_stream(conn_str, "SELECT * FROM dbo.yellow_trips",
                                partition_on="trip_id", partitions=8) as s:
    duckdb.sql("SELECT avg(total_amount) FROM s").show()
```

- `fetchall_arrow()`, `fetchmany_arrow()`, `arrow_stream()` and `execute_arrow()` return native Arrow
  objects; for polars use `pl.DataFrame(t)`. Upstream's `arrow()`, `arrow_batch()` and `arrow_reader()`
  also work (they need pyarrow).
- `read_arrow` / `read_arrow_stream` cut the key range into equal-width slices (skewed keys are not
  rebalanced; partitions are not one snapshot). Pass `partition_bounds=[...]` (int, date/datetime,
  Decimal or str) to use your own boundaries, such as quantiles, instead of a MIN/MAX probe.
  `read_arrow_stream` returns a stream with a bounded buffer and unordered batches, which `deltalake`,
  DuckDB, polars and pyarrow can consume while rows still arrive.
- `nvarchar` / `nchar` / `ntext` travel as UTF-8 on SQL Server 2019+ and Azure SQL (about half the bytes
  for ASCII text). Opt out with `utf8_strings=False` (`execute_arrow`, `read_arrow`, `read_arrow_stream`).

Runnable scripts: [examples/](examples/).

## Bulk loads

```python
cur.bulkcopy_arrow("dbo.target", duckdb_result_or_arrow_table)       # Arrow in, one connection
cur.bulkcopy("dbo.target", rows)                                      # row tuples
arrowsql.write_arrow(conn_str, "dbo.target", table, workers=8)       # parallel Arrow load
arrowsql.write_rows(conn_str, "dbo.target", rows, workers=8)         # parallel row load
```

`write_arrow` / `write_rows` pick the lock from the target: TABLOCK for a heap, none for a clustered
columnstore (batches of at least 102,400 rows), and one worker for other indexed tables. All of them
accept `batch_size`, `column_mappings`, `keep_identity`, `check_constraints`, `keep_nulls` and
`fire_triggers`. See [examples/write_arrow.py](examples/write_arrow.py).

## Async

```python
from arrowsql import aio

async with await aio.connect(conn_str) as conn:
    rows = await conn.fetch("SELECT TOP 3 trip_id FROM dbo.yellow_trips WHERE PULocationID = ?", 132)

pool = await aio.create_pool(conn_str, min_size=4, max_size=32)
total = await pool.fetchval("SELECT SUM(total_amount) FROM dbo.yellow_trips")
async with pool.acquire() as conn:
    table = await (await conn.cursor().execute("SELECT * FROM dbo.yellow_trips_100k")).fetchall_arrow()
await pool.close()
```

Waits never hold the GIL, and cancelling a task (including `asyncio.wait_for`) cancels the query on the
server and leaves the connection usable. Unix only. Details: [docs/ASYNC.md](docs/ASYNC.md).

## Agent mode

For LLM agents and terminals: compact typed TSV (or Markdown), explicit truncation and actionable JSON errors.

```bash
export MSSQL_CONNECTION_STRING='Server=...;Database=...;Uid=...;Pwd=...'   # or DB_CONNECTION_STRING
arrowsql-agent -c "SELECT 1 AS answer, N'x' AS label"      # python -m arrowsql.agent works too
```

```python
from arrowsql.agent import query
text = query(conn_or_connection_string, "SELECT TOP 100 * FROM dbo.orders WHERE id > ?", [42],
             format="tsv", max_bytes=10_000, max_rows=1000, max_cell_chars=500, timeout_s=60)
```

Output format, footer fields, error categories: [docs/AGENT_MODE.md](docs/AGENT_MODE.md).

## Compatibility with mssql-python

| | |
|---|---|
| Identical | DB-API 2.0 surface, `?` parameters, connection-string keywords, exception hierarchy, `import mssql_python` alias; the upstream test suite is the compatibility bar |
| Added | `sqlstate` / `native_error` on exceptions; `fetchall_arrow`, `fetchmany_arrow`, `arrow_stream`, `execute_arrow`; `read_arrow`, `read_arrow_stream`, `write_arrow`, `write_rows`, `bulkcopy_arrow`; `arrowsql.aio`; `arrowsql.agent` |
| Different | No ODBC driver, unixODBC or `MSSQL_PYTHON_NATIVE_PROVIDER` effect; TLS via rustls; zero required dependencies; `aio` is Unix only |
| Not supported | `Authentication=ActiveDirectoryPassword`, `SqlPassword`, `ActiveDirectoryIntegrated`, NTLM, Always Encrypted |

Long version: [docs/CONTRACT.md](docs/CONTRACT.md) (every binding symbol and its status) and
[docs/PYTHON_LAYER_CHANGES.md](docs/PYTHON_LAYER_CHANGES.md) (edits to the vendored Python layer).

## Benchmarks

NYC yellow taxi January 2024 (2,964,624 rows x 20 columns), SQL Server 2022 in podman on the same host,
release build, median of 5, on a shared dev box: compare ratios, not absolute times. Upstream is
mssql-python 1.16.0 (ODBC Driver 18); pyodbc uses `fast_executemany=True`.

| workload | upstream | arrowsql | speedup | pyodbc / best other |
|---|---:|---:|---:|---|
| `fetchall()`, 100k rows | 0.351 s | 0.117 s | 3.0x | pyodbc 0.865 s |
| `fetchmany`, 3M rows | 9.74 s | 2.75 s | 3.5x | pyodbc 27.0 s |
| decimal-heavy `fetchmany` | 1.844 s | 0.430 s | 4.3x | pyodbc 5.0 s |
| `cursor.arrow()`, 100k rows | 0.146 s | 0.060 s | 2.4x | connectorx 0.350 s |
| `read_arrow(..., partitions=8)`, 3M rows | 4.253 s | 0.591 s | 7.2x | connectorx (8 partitions) 2.63 s |
| `bulkcopy_arrow`, 3M rows, heap | 12.35 s | 6.07 s | 2.0x | `bcp` 6.67 s |
| `write_arrow(..., workers=8)`, 3M rows | 12.35 s | 1.82 s | 6.8x | `bcp` 6.67 s |
| `executemany`, 100k rows | 1.257 s | 0.653 s | 1.9x | pyodbc 0.762 s |
| 16 threads x point lookups | 2.395 s | 0.429 s | 5.6x | FastMssql 0.612 s |
| async point lookups, 64 concurrent | ~7k q/s | ~40k q/s | ~5x | FastMssql ~15k q/s |

Short queries (`SELECT 1`, connect + close) are on par with pyodbc. Reproduce with
`scripts/bench_reproduce.sh` (see [benchmarks/README.md](benchmarks/README.md)); the full multi-driver
matrix is in [benchmarks/RESULTS.md](benchmarks/RESULTS.md), and the complete table with latency rows
and notes in [docs/BENCHMARK_SUMMARY.md](docs/BENCHMARK_SUMMARY.md).

## Platforms and Python versions

- Wheels: Linux glibc and musl (x86_64, arm64), macOS (arm64, x86_64), Windows (x64, arm64).
- CPython 3.10 to 3.15, plus free-threaded 3.14t and 3.15t. Not built: 32-bit targets, ppc64le, s390x, PyPy.
- Full SQL Server tests run in CI on Linux x86_64 and Windows x64; other platforms run server-free tests.
  Details and the build matrix: [docs/PLATFORMS_AND_CONNECTION.md](docs/PLATFORMS_AND_CONNECTION.md),
  [docs/PACKAGING.md](docs/PACKAGING.md).

**Connection string.** Standard keywords (`Server`, `Database`, `Uid`, `Pwd`, `Encrypt`,
`TrustServerCertificate`, `ApplicationIntent`, `MultiSubnetFailover`, ...), Kerberos/SSPI via
`Trusted_Connection=yes`, and Entra ID via `Authentication=ActiveDirectory*` or `token_provider=` (needs
the `[azure]` extra). Full keyword table: [docs/PLATFORMS_AND_CONNECTION.md](docs/PLATFORMS_AND_CONNECTION.md).

### Security

`TrustServerCertificate=yes` switches certificate validation off. Use it for local test containers only.
Otherwise use a certificate the client trusts (or `ServerCertificate=` / `HostNameInCertificate=`) and keep `Encrypt=yes`.

### Environment variables

| variable | effect |
|---|---|
| `MSSQL_RS_BUSY_POLL` | `0`, `off` or `false` disables the short busy poll for the first response byte |
| `MSSQL_RS_MULTISUBNET_FAILOVER_DEFAULT` | `true` / `false`: default for `MultiSubnetFailover` when the connection string omits it |
| `MSSQL_CONNECTION_STRING`, `DB_CONNECTION_STRING` | read by `arrowsql-agent`; `DB_CONNECTION_STRING` is also used by tests, benchmarks and examples |

## Development

```bash
uv venv -p 3.13 .venv && VIRTUAL_ENV=.venv uv pip install -r requirements-dev.txt
scripts/build.sh                      # dev build into .venv (--release for benchmarks)
export DB_CONNECTION_STRING='Server=localhost,1433;Database=master;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes'
python -m pytest -q --timeout 120 tests/common_issues tests/port
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/TEST_SCOPE.md](docs/TEST_SCOPE.md). Design notes:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/PERFORMANCE_RESEARCH.md](docs/PERFORMANCE_RESEARCH.md),
[docs/UPSTREAM_ISSUES.md](docs/UPSTREAM_ISSUES.md), [docs/UPSTREAM_BUGS_FOUND.md](docs/UPSTREAM_BUGS_FOUND.md),
[docs/FEATURES.md](docs/FEATURES.md) (feature list and async notes moved from the old README).

## License and credits

MIT, see [LICENSE](LICENSE). Contains code from [microsoft/mssql-python](https://github.com/microsoft/mssql-python)
(MIT, [LICENSE.upstream-mssql-python](LICENSE.upstream-mssql-python)) and builds on Microsoft's
[mssql-tds](https://github.com/microsoft/mssql-rs) (MIT).
