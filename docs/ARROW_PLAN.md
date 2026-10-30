# Native Arrow plan

Goal: the fastest way to get SQL Server data into Arrow consumers (DuckDB,
polars, pyarrow, Parquet writers), modelled on bmsuisse/arrowbricks, while
keeping upstream mssql-python's Arrow API working unchanged.

## 1. Upstream-compatible API (must keep passing tests/upstream/test_004_cursor_arrow.py)

Documented at https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/arrow-integration:
`cursor.arrow(batch_size=8192)`, `cursor.arrow_batch(batch_size)`,
`cursor.arrow_reader(batch_size)` and the reader's `__arrow_c_stream__`.
They go through `ddbc_bindings.DDBCSQLFetchArrowBatch(stmt, capsules,
arrowBatchSize, charCtype, messages)`, which appends Arrow C Data Interface
PyCapsules (`arrow_schema`, `arrow_array`) to `capsules`. The Rust
implementation fills the same capsules, so the Python layer stays untouched.
The SQL-type -> Arrow-type mapping must match upstream exactly
(tests/common_issues/test_rs_arrow_type_mapping.py pins it).

## 2. arrowbricks-style additions (zero dependencies)

- `cursor.fetchall_arrow()` / `cursor.fetchmany_arrow(n)` return a native
  `ArrowTable` (Rust pyclass): `num_rows`, `num_columns`, `column_names`,
  `len()`, `__arrow_c_stream__`, `__arrow_c_schema__`. No pyarrow needed;
  `duckdb.sql("SELECT ... FROM t")`, `pyarrow.table(t)` and
  `polars.from_arrow(t)` consume it directly.
- `cursor.arrow_stream(batch_size)`: a one-shot native reader that DuckDB can
  scan while rows are still arriving (streams, no full materialization).
- DuckDB's register path calls `__arrow_c_stream__` twice (schema peek, then
  scan); `ArrowTable` must support repeated export.

## 3. Why it can be much faster than upstream

Upstream: ODBC SQLFetch -> C++ buffers -> Arrow arrays, per batch, holding
the GIL between batches. The port:

- Decodes TDS rows straight into Arrow builders (the decode sink seam in
  src/decode/, see docs/ARCHITECTURE.md) with no ODBC layer and no Python
  objects, and the GIL released for the whole batch.
- Pipelines: while Python/DuckDB consumes batch N, the tokio runtime already
  decodes batch N+1 (bounded channel, depth 2).
- Builders sized from column metadata, strings appended as UTF-8 directly from
  the UTF-16 wire bytes in one pass, fixed-width types memcpy'd where the wire
  layout allows.

## 4. Status and results

Implemented (Arrow fork branch): all of 1-3, plus `mssql_python.read_arrow()`
(partitioned parallel reads) and `ArrowTable.concat`. Tests:
tests/upstream/test_004_cursor_arrow.py + test_018 (69 pass),
tests/common_issues/test_rs_arrow_native.py (26),
test_rs_arrow_type_mapping.py + test_rs_duckdb_integration.py (pass; one
strict xfail asks for timestamp[ns] for datetime2(7), which contradicts
upstream's documented timestamp[us] - kept upstream's contract).

`benchmarks/bench_arrow.py`, NYC taxi `dbo.yellow_trips` (2,964,624 rows x 20
columns), release build, median of 5, dev box under load (loadavg 5-10):

| workload | upstream (C++/ODBC) | port | speedup |
|---|---:|---:|---:|
| `cursor.arrow()` 100k rows | 0.163 s | 0.067 s | 2.4x |
| `cursor.arrow()` 3M rows | 4.253 s | 2.283 s | 1.9x |
| DuckDB aggregate over `arrow_reader()` 3M | 4.362 s | 2.006 s | 2.2x |
| `fetchall_arrow()` 3M (native) | - | 2.246 s | 1.9x |
| DuckDB over `arrow_stream()` 3M (native) | - | 2.063 s | 2.1x |
| `read_arrow(..., partitions=2)` 3M | - | 1.306 s | 3.3x |
| `read_arrow(..., partitions=4)` 3M | - | 0.832 s | 5.1x |
| `read_arrow(..., partitions=8)` 3M | - | 0.591 s | 7.2x |
| DuckDB over `read_arrow(..., partitions=8)` 3M | - | 0.459 s | 9.5x vs DuckDB over upstream reader |

Where the single-stream time goes (3M x 20 cols, Encrypt=no):
- reading the result through a sink that discards every value: 1.95-2.16 s;
  the Arrow sink adds 7-10 % on top (2.18-2.32 s).
- during that drain the client uses 0.96 s CPU of 2.11 s wall, while SQL
  Server burns 2.64 s CPU producing the stream: one TDS session is bound by
  the server's serialization rate, not by the client.
- packet size 4096..32767 changes nothing measurable here.
So single-stream Arrow is at ~2x upstream (which is client-bound at ~4.3 s),
and partitioned reads over N sessions are the way past the server cap.
