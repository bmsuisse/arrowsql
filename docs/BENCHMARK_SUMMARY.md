# Benchmark summary tables

Moved from the README. Mixed commits; compare ratios, not absolutes. For reproducible numbers see [../benchmarks/README.md](../benchmarks/README.md) and [../benchmarks/RESULTS.md](../benchmarks/RESULTS.md).

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
