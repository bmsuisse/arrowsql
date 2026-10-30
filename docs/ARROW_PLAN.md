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

## 4. Benchmarks (benchmarks/bench_taxi.py)

`arrow_100k_all_cols`, `arrow_3m_all_cols`, `duckdb_over_arrow_3m`.
Upstream ODBC baseline, quick run on the dev box: arrow_100k_all_cols 0.19 s.
