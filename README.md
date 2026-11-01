<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo.svg" alt="arrowsql" width="420">
  </picture>
</p>

<p align="center">
  Fast SQL Server driver for Python with a Rust core. Drop-in for
  <a href="https://github.com/microsoft/mssql-python">mssql-python</a>, Arrow-native.
</p>

<p align="center">
  <a href="https://pypi.org/project/arrowsql/"><img src="https://img.shields.io/pypi/v/arrowsql" alt="PyPI"></a>
  <a href="https://pypi.org/project/arrowsql/"><img src="https://img.shields.io/pypi/pyversions/arrowsql" alt="Python versions"></a>
  <a href="https://github.com/bmsuisse/arrowsql/actions/workflows/ci.yml"><img src="https://github.com/bmsuisse/arrowsql/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT license"></a>
</p>

- **2-7x faster than mssql-python**, up to 10x faster than pyodbc on large reads.
- **Arrow out of the box:** results go straight to DuckDB, polars, pyarrow and Delta, no pyarrow required.
- **Parallel reads and bulk loads** over several connections.
- **No ODBC driver:** native TDS in Rust, zero required dependencies, wheels for Linux, macOS and Windows.
- **Drop-in:** same DB-API, same connection strings; `import mssql_python` keeps working.

## Install

```bash
pip install arrowsql
```

Extras: `arrowsql[arrow]` (pyarrow for `cursor.arrow()`), `arrowsql[azure]` (Entra ID login).
Do not install it next to Microsoft's `mssql-python`: both provide the `mssql_python` package.

## Quick start

```python
import arrowsql

conn = arrowsql.connect("Server=myserver;Database=sales;Uid=app;Pwd=...;Encrypt=yes")
cur = conn.cursor()
cur.execute("SELECT id, total FROM dbo.orders WHERE customer_id = ?", (42,))
for row in cur.fetchall():
    print(row.id, row.total)
```

## Arrow, DuckDB, polars

```python
import duckdb

t = cur.execute("SELECT * FROM dbo.orders").fetchall_arrow()          # native Arrow table
duckdb.sql("SELECT customer_id, sum(total) FROM t GROUP BY 1").show()

# Large tables: read over 8 connections and stream into DuckDB, deltalake or polars
with arrowsql.read_arrow_stream(conn_str, "SELECT * FROM dbo.orders",
                                partition_on="id", partitions=8) as stream:
    duckdb.sql("SELECT avg(total) FROM stream").show()
```

String columns travel as UTF-8 on SQL Server 2019+ (about half the bytes). More in [examples/](examples/).

## Bulk loads

```python
cur.bulkcopy_arrow("dbo.orders", arrow_table)                          # Arrow -> TDS bulk load
arrowsql.write_arrow(conn_str, "dbo.orders", arrow_table, workers=8)   # in parallel
```

## Spark / Databricks

`arrowsql.spark` is a Spark 4 Python data source: every Spark task reads its slice of the
query with arrowsql (Arrow batches, no rows) and writes with the bulk copy. `pyspark` is an
optional extra (`pip install "arrowsql[spark]"`); arrowsql must be installed on the executors
too (Databricks: a notebook `%pip install arrowsql`).

```python
from arrowsql.spark import register
register(spark)

df = (spark.read.format("arrowsql")
      .option("connection_string", conn_str)
      .option("dbtable", "dbo.orders")
      .option("partitionColumn", "id").option("numPartitions", 8)   # 8 tasks, 8 connections
      .load())
df.write.format("arrowsql").option("connection_string", conn_str).option("dbtable", "dbo.orders_copy").mode("append").save()
```

| Option | Default | Meaning |
|---|---|---|
| `connection_string` (or `url`) | required | ODBC connection string |
| `dbtable` / `query` | exactly one | table or view name / SELECT (no trailing ORDER BY) |
| `partitionColumn`, `numPartitions` | one slice | range partitioning on a numeric, date or datetime column; half-open slices, the first also reads NULLs |
| `lowerBound`, `upperBound` | MIN/MAX on the driver | set both to skip that query; rows outside still land in the end slices |
| `hashColumns` + `numPartitions` | none | slice `i` = `(CHECKSUM(cols) & 2147483647) % numPartitions = i`; key columns only |
| `batchSize` | 65536 | rows per Arrow batch |
| `utf8Strings` | `true` | nvarchar over the wire as UTF-8 |
| `timestampType` | `ntz` | `ntz`: datetime/datetime2 as `TimestampNTZType`, exact. `timestamp`: Spark JDBC's `TimestampType`, wall clock read (and written) in `timeZone` |
| `timeZone` | `UTC` | zone for `timestampType=timestamp`; Spark does not pass its session zone to data sources |

Write: `append` bulk-copies each task's batches; `overwrite` runs `TRUNCATE TABLE` once on the
driver first. Neither is transactional across tasks. Unsigned types are widened (`tinyint` is
`smallint` in Spark). Examples: [examples/spark_datasource.py](examples/spark_datasource.py)
(local Spark), [examples/databricks_spark_datasource.py](examples/databricks_spark_datasource.py)
(notebook).

## Async and agent mode

- `arrowsql.aio`: native asyncio with a connection pool; cancelling a task cancels the query
  ([docs/ASYNC.md](docs/ASYNC.md)).
- `arrowsql-agent -c "SELECT ..."`: bounded, typed output and JSON errors for LLM agents
  ([docs/AGENT_MODE.md](docs/AGENT_MODE.md)).

## Benchmarks

NYC taxi data (2.96M rows x 20 columns), SQL Server 2022 on the same host. Compare ratios: the box was shared.

| | mssql-python | arrowsql | pyodbc |
|---|---:|---:|---:|
| `fetchall()`, 100k rows | 0.351 s | **0.117 s** | 0.865 s |
| `fetchmany`, 3M rows | 9.74 s | **2.75 s** | 27.0 s |
| Arrow, 3M rows over 8 connections | 4.25 s | **0.59 s** | – |
| Bulk load, 3M rows (8 workers) | 12.35 s | **1.82 s** | – |
| `executemany`, 100k rows | 1.26 s | **0.65 s** | 0.76 s |
| 16 threads x point lookups | 2.40 s | **0.43 s** | – |

Reproduce: `scripts/bench_reproduce.sh`. Full matrix: [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Docs

[Compatibility](docs/CONTRACT.md) ·
[Platforms and connection strings](docs/PLATFORMS_AND_CONNECTION.md) ·
[Features](docs/FEATURES.md) ·
[Architecture](docs/ARCHITECTURE.md) ·
[Performance notes](docs/PERFORMANCE_RESEARCH.md) ·
[Contributing](CONTRIBUTING.md)

## License

MIT. Builds on [microsoft/mssql-python](https://github.com/microsoft/mssql-python) and
[mssql-tds](https://github.com/microsoft/mssql-rs) (both MIT).
