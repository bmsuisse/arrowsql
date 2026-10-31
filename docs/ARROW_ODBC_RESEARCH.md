# arrow-odbc, arrow-odbc-py and odbc2parquet: what they do and what we should take

Status: research note, read-only pass over upstream source. Date: 2026-10-30.

Sources (shallow clones, read locally):

| repo | commit | version |
|---|---|---|
| pacman82/arrow-odbc | `92fcbee` | crate 26.0.3 |
| pacman82/arrow-odbc-py | `f29dc49` | pins `arrow-odbc = "26"` (Cargo.lock: 26.0.3) |
| pacman82/odbc2parquet | `adbf154` | CLI, uses `odbc-api` 29, `parquet` 60 |

Paths below are relative to local clones of the reference projects (not tracked).

Our side references are relative to `~/devops/mssql-python-rs`.

## TL;DR

- arrow-odbc is a mature, well-engineered ODBC-to-Arrow reader. Its design is driven by ODBC limits: it must pick buffer sizes before it sees data, and it must guess text widths from metadata. Most of its knobs (`max_text_size`, `max_bytes_per_batch`, `TextEncoding`) exist to paper over that.
- We are not on ODBC, so we avoid most of those problems. Our `LargeUtf8` values grow on demand (`src/arrow/plan.rs`), and we decode TDS directly into builders on a producer thread (`src/arrow/export.rs`). On read, we have no reason to copy their buffer model.
- On read, we have no measured number for our native Arrow path (`rust_port` is `ERR` for every `.arrow` workload in `benchmarks/RESULTS.md`). Upstream mssql_python is the bar, and it beats arrow-odbc on most arrow workloads. The gap that matters on the read side is fidelity: we truncate `datetime2(7)` and `time(7)` to microseconds, and arrow-odbc keeps nanoseconds.
- On write, arrow-odbc is not a model to copy. It uses a parameterized `INSERT` with ODBC array parameters, not bulk copy, and it was far slower in our quick run (20k rows in 130 s). But the benchmark used arrow-odbc 10.6.3, while HEAD pins 26.0.3. That release includes a fix for per-row rebinding of variadic insert buffers (issue #118). The 130 s figure is stale until re-measured. Our own write target is bcp-class throughput: the `bcp` CLI does 100k heap rows with TABLOCK in 0.382 s, against 2.04 s for `rust_port`, so we are 4-5x behind bcp.
- odbc2parquet is a good reference for streaming Parquet export (row group per fetched batch, file splitting, zstd default). It is not a faster engine than what we have. Its `insert` subcommand binds one row per execute (`src/insert.rs`, `into_column_inserter(1, ...)`). Do not copy that.

## Correction to the brief

The brief says arrow-odbc "matched upstream on `read.narrow_3m.arrow` and slightly beat it on `read.all_3m.duckdb`". Both are true in `benchmarks/RESULTS.md`, but they are the good cases. The same run shows arrow-odbc behind mssql_python on most arrow reads:

| workload (`.arrow`) | mssql_python | arrow_odbc | ratio |
|---|---|---|---|
| `read.narrow_3m` | 92.8 ms | 92.3 ms | 1.0x |
| `read.all_3m` | 0.451 s | 0.782 s | 0.6x |
| `read.strings` | 0.116 s | 0.477 s | 0.2x |
| `read.lob` | 50.0 ms | 0.435 s | 0.1x |
| `read.wide` | 0.253 s | 0.417 s | 0.6x |
| `read.decimals` | 65.4 ms | 76.3 ms | 0.9x |
| `read.datetimes` | 32.3 ms | 44.8 ms | 0.7x |
| `read.all_3m.duckdb` | 0.477 s | 0.460 s | 1.0x (only win) |

Also: in that same quick run our own `rust_port` column is `ERR` for every `.arrow` workload. So `benchmarks/RESULTS.md` has no measured number for our native Arrow path. The only comparisons are against upstream mssql_python. Fix the harness before we claim anything against arrow-odbc.

The run was on a loaded box (load average above 20, `--quick`), and the arrow-odbc build was 10.6.3. Treat the table as directional.

---

## 1. Read path (arrow-odbc)

### 1.1 ODBC metadata to Arrow types

Entry point: `arrow_schema_from` and `arrow_field_from` in `src/schema.rs`. It calls `describe_col` per column and maps `odbc_api::DataType`:

- `Numeric`/`Decimal` with precision 0..=38 maps to `Decimal128(p, s)`. If scale exceeds precision, precision is raised to scale (Postgres/Oracle case).
- `Integer`, `SmallInt`, `BigInt`, `TinyInt` (signed/unsigned via `column_is_unsigned`), `Real`, `Float`, `Double`, `Bit`, `Date` map directly.
- `Timestamp { precision }` maps by precision: 0 gives Second, 1-3 gives Millisecond, 4-6 gives Microsecond, anything else gives Nanosecond (falliable).
- `Time { precision }` goes through `precision_to_time`: 0 gives Time32(s), 1-3 Time32(ms), 4-6 Time64(us), 7-9 Time64(ns), and above that Utf8.
- SQL Server `-154` (`SQL_SS_TIME2`) is treated as `TIME` only when `dbms_name == "Microsoft SQL Server"`. Otherwise it is Utf8.
- `Binary{len}` maps to `FixedSizeBinary`. `Varbinary` and `LongVarbinary` map to `Binary` (i32 offsets).
- `Char`, `WChar`, `Varchar`, `WVarchar`, `LongVarchar`, `WLongVarchar`, `Unknown`, and all `Other` (except the two above) map to `Utf8`.
- `DATETIMEOFFSET` (`-155`) is not in `schema.rs`. It falls into the `Other` arm, so it arrives as a **string**. Our `Kind::Timestamp { utc: true }` (`src/arrow/plan.rs`) is better here.
- Nullability is `could_be_nullable()`, OR'd with `map_value_errors_to_null` for nanosecond timestamps.

Note that arrow-odbc's `Utf8` is i32-offset. A single batch cannot exceed 2 GiB of string data. Our `LargeUtf8` (`src/arrow/plan.rs`, `Kind::Text`) avoids that limit.

`schema.rs` also says SQL Server TIME uses `dbms_name` only for `-154`. Other databases' `-154` is a string. We do not need that branch, since our type mapping is per-driver.

### 1.2 Buffer sizing

Buffer sizes are decided in three places.

1. Per-column `BufferDesc` comes from the strategy: `choose_column_strategy` in `src/reader.rs` (about lines 130-210), and `choose_text_strategy` in `src/reader/text.rs`.
   - Text uses the SQL type's length (`utf8_len` or `utf16_len`). If that is missing, it falls back to `col_display_size()` (the `lazy_display_size` closure).
   - If both are missing it raises `ColumnFailure::ZeroSizedColumn`. This is the `VARCHAR(max)` error. Issue #56 shows ODBC reporting size `0`.
   - `max_text_size` caps the length: `min(len, limit)` in `apply_buffer_limit`.
   - Binary uses `column_size()` capped by `max_binary_size`, with the same error.
2. Row size is the sum of `bytes_per_row` over all column descriptors (`ToRecordBatch::row_size_in_bytes`, `src/reader/to_record_batch.rs`).
3. `OdbcReaderBuilder::buffer_size_in_rows` (`src/reader/odbc_reader.rs`) computes `rows = min(max_num_rows_per_batch, max_bytes_per_batch / bytes_per_row)`. If that is zero it errors with `OdbcBufferTooSmall`.

Defaults (`src/reader/odbc_reader.rs`, `DEFAULT_MAX_ROWS_PER_BATCH = 65535`, `DEFAULT_MAX_BYTES_PER_BATCH = 512 MiB`). These are repeated in `arrow-odbc-py/python/arrow_odbc/reader.py` as `DEFAULT_FETCH_BUFFER_LIMIT_IN_BYTES = 2**29` and `..._IN_ROWS = 65535`.

Consequence: text columns dominate buffer size. Each text column reserves its full declared width times row count. For `nvarchar(4000)` on the UTF-16 path that is 8000 bytes per row per column. Five such columns with a 512 MiB budget give about 13k rows per fetch, even if the real values are 20 bytes. This matches issue #87 ("performance extremely sensitive to `max_text_size`"), where a larger limit reduced batch utilization. Lowering `max_text_size` raises rows per fetch but truncates or errors on long values.

Our `plan.rs` has the same sizing problem in a different form: `Plan.widths` is a "pre-sizing estimate, not a limit". Buffers grow on demand. We pay for reallocation, not a fixed up-front reservation. That is the better trade for us.

### 1.3 Columnar bulk fetch

`OdbcReader` binds one `ColumnarDynBuffer` to the cursor with `cursor.bind_buffer(...)`, wrapped in `BlockCursor` (odbc-api). Each `next()` calls `fetch_with_truncation_check(true)` (`src/reader/odbc_reader.rs`, `Iterator::next`). That is one `SQLFetch` with a row array of `row_array_size` rows. The row array is set by `SQL_ATTR_ROW_ARRAY_SIZE`, which is the computed buffer rows.

`fetch_with_truncation_check(true)` turns truncation into an error instead of a silent cut. Issue #97 shows the resulting `TooLargeValueForBuffer { indicator, buffer_index }`. That error means the driver reported a value longer than the bound buffer. The usual cause is a Simba driver reporting column size 255 by default (issue #97 reply, workaround `DefaultStringColumnLength=0`). Our TDS decoder has no equivalent, since we size per value.

Decoding into Arrow happens on the calling thread, one `ReadStrategy::fill_arrow_array` per column (`src/reader/to_record_batch.rs`, `buffer_to_record_batch`). The strategies:

- Fixed-width numerics: `NullableDirectStrategy` / `NonNullDirectStrategy` (`src/reader/map_odbc_to_arrow.rs`). The ODBC buffer slice is appended with `append_slice`. No conversion.
- Timestamps: ODBC `SQL_TIMESTAMP_STRUCT` through `map_infalliable` / `map_falliable` with `ms_since_epoch` etc. (`src/date_time.rs`).
- Time: fetched as **text** `HH:MM:SS.fff` and parsed (`src/reader/time.rs`, `TimeMsI32`, `ticks_since_midnights_from_text`).
- Decimal: fetched as **text**, `max_str_len = precision+2` (or `+3` when scale >= precision), then `decimal_text_to_i128` into `Decimal128Builder` (`src/reader/decimal.rs`). Issue #73, still open, is about optimizing `Decimal(x, 0)` parsing.
- Text: see 1.4.
- Binary: `src/reader/binary.rs`.

Our side: decimals decode from TDS binary into i128 (`src/arrow/sink.rs`, around line 211, `POW10` scaling). Times and timestamps are integer math (`sink.rs` lines 223-234). No text round-trip.

### 1.4 Text encoding

`TextEncoding` (`src/reader/text.rs`) has three values:

- `Auto`: UTF-16 on Windows, UTF-8 (narrow) elsewhere.
- `Utf8`: narrow `SQLGetData`/`SQLBindCol` C type. Assumed UTF-8, validated with `simdutf8`.
- `Utf16`: wide buffers. Converted to UTF-8 with `encoding_rs` (`Utf16ToUtf8Converter`, `WideText::fill_arrow_array`). The data reservation is capped at `3 * utf16_len`, per the comment there.

Linux (narrow) has real failure modes. Issue #155 (arrow-odbc-py) is mojibake for `N'你好，世界'` through the Microsoft Linux driver, and the maintainer traced it to the driver. The workaround is `payload_text_encoding=TextEncoding.Utf16`. Issue #862 in odbc2parquet is a panic on non-ASCII text on Windows, and #484 is "Data source must return valid UTF16" on MSSQL. Both are text-encoding bugs that never happen on our TDS path, because we decode the server's NVARCHAR bytes directly.

### 1.5 Concurrency (ConcurrentOdbcReader)

`src/reader/concurrent_odbc_reader.rs` wraps `odbc_api::ConcurrentBlockCursor`. A dedicated thread calls `SQLFetch` into the second buffer while the caller decodes the first into Arrow. `into_concurrent()` swaps the reader state. It requires `'static` cursors, so the Python binding always holds a shared connection.

Cost: two transit buffers, so about 2x the memory for buffer space (the doc comment says so). The Python default is `fetch_concurrently=True` (`arrow-odbc-py/python/arrow_odbc/connect.py`, line 71).

Our analogue is `ArrowStream::start` in `src/arrow/export.rs`. A producer thread calls `fetch_into(batch_rows)` and builds the Arrow batch itself (`produce`). Batches go through `sync_channel(depth)`. So we already overlap network fetch with Arrow build, and our depth is a bounded queue rather than a fixed double buffer. The difference is that the producer, not the consumer, does the decode. Both designs overlap. Our peak memory is bounded by `depth` times batch size, with no separate fixed transit buffer.

### 1.6 Error handling

- Per column, `ColumnFailure` (`src/reader.rs`) carries the column name and index through `into_crate_error`. Messages are reasonably specific.
- Per batch, mapping errors become `ArrowError::ExternalError` (`MappingError`). Overflow in nanosecond timestamps is falliable and can be turned into null with `value_errors_as_null`.
- Driver errors on fetch become `ArrowError::from_external_error(odbc_error)` (`odbc_to_arrow_error`).

We get the same shape from `Plan::new` (unsupported SQL type errors with column name and ID) and from `produce` (`SQL_ERROR` diagnostics joined into the message). Ours is comparable, and it does not depend on the ODBC diagnostic record set.

---

## 2. Write path (arrow-odbc OdbcWriter)

### 2.1 How it works

Entry points:

- Rust: `insert_into_table(connection, batches, table, batch_size)` in `src/odbc_writer.rs`.
- Python: `insert_into_table` and `from_table_to_db` (`arrow-odbc-py/python/arrow_odbc/insert_into_table.py`), `chunk_size` is the only size knob.

Mechanics (`src/odbc_writer.rs`):

1. `insert_statement_from_schema(schema, table, quoting)` builds `INSERT INTO <t> (<cols>) VALUES (?, ...)`. Identifiers are quoted per the server's `identifier_quote_char`. `QuoteOffensively` quotes whenever needed, and `QuoteDefensively` is used when the driver reports no quote character. Pre-quoted names (`[..]`, `"..."`, backticks) are passed through as documented in the Python docstring.
2. `connection.prepare(sql)` then `OdbcWriter::new(batch_size, schema, statement)`.
3. `OdbcWriter::new` builds one `WriteStrategy` per column (`field_to_write_strategy`). Each strategy declares a `BindParamDesc`. `ColumnarBulkInserter` binds those as array parameter buffers with capacity `row_capacity`.
4. `write_batch` copies Arrow values into the parameter buffers row by row, `chunk_size` at a time (`odbc_writer/*`). When `num_rows == capacity` it calls `flush()`, which does `inserter.execute()` (one `SQLExecute` with paramset size = rows) and `clear()`.
5. Text columns use `Utf8ToNarrow` or `Utf8ToWide` (`src/odbc_writer/text.rs`, chosen by platform). Their buffer width comes from the longest element in the current array (`max_element_byte_len`). If a later batch has a longer element, the buffer is rebound. That is the reallocation issue #118, fixed in 25.1.1.
6. Timestamps: `TimestampNanosecondType` is truncated to 100 ns (drop the last two digits), since they bind precision 7 (`src/odbc_writer/timestamp.rs`). With tz, values are formatted as text.
7. Date and Binary are straightforward (`odbc_writer/date.rs`, `binary.rs`).

Nothing here is BCP or TDS bulk load. The server sees a parameterized INSERT executed with array parameters. Whether the SQL Server ODBC driver turns that into one round trip or one RPC per row is driver behavior, and we have not checked it.

### 2.2 Why it is slow against SQL Server

Measured: 20k rows in 130 s (about 150 rows/s), per `benchmarks/RESULTS.md` line 71. The benchmark adapter (`benchmarks/drivers/arrow_odbc_driver.py`, `bulk_insert_arrow`) passes `chunk_size = 65536`. So 20k rows is one `SQLExecute` with a 20k-row paramset.

That is not consistent with what the same driver does for pyodbc. Our run (`write.executemany_10k`) shows pyodbc `fast_executemany` at 86 ms for 10k rows, and plain pyodbc at 1.16 s. Array parameter binding is fast on this driver. So the 130 s is probably not inherent to array binding. Candidate causes, in order of how well they fit the evidence:

1. Version: arrow-odbc 10.6.3 (the benchmark build) reallocates and rebinds variadic parameters every time a longer string appears (issue #118, fixed in 25.1.1). A text-heavy table at 20k rows would show this clearly. HEAD pins 26.0.3.
2. Very large paramsets: one `SQLExecute` with 20k rows, text bound at max-element width. Chunk sizes of 1k to 4k should be measured.
3. The benchmark table's indexes or triggers (not checked).

Action: re-benchmark arrow-odbc 26.x with `chunk_size` in {1000, 4000, 65536} on the same table, and against pyodbc `fast_executemany`. Do not conclude anything from the 10.6.3 number.

Even if the array-insert path is fixed, it is not the path we are building. Our write target is bulk copy, not INSERT. `RESULTS.md` line 197 (`write.bulk_rows_100k.heap.tablock`) gives bcp 0.382 s, upstream mssql_python 1.64 s, rust_port 2.04 s. So the bar for our write path is bcp, not arrow-odbc.

### 2.3 Error handling on write

- `WriterError` (`src/odbc_writer.rs`) has specific variants per step: bind, execute, rebind, quoting, unsupported Arrow type, invalid time zone.
- Truncation and overflow on insert are not reported as errors. Issue #196 (arrow-odbc-py, closed): a too-long string is silently truncated by the driver, which reports `SUCCESS_WITH_INFO`. The maintainer now fetches diagnostics, but does not abort, since ODBC considers the operation successful. Issue #52 (Spark, closed) was the reverse, a false-positive truncation error, fixed in 1.2.5.

Our bulk path should error, not truncate, when a value exceeds a column width. Check that explicitly.

---

## 3. odbc2parquet

### 3.1 Query to Parquet

Flow (`src/query.rs`, `query()` to `cursor_to_parquet()`):

1. `open_connection`, then `into_cursor(query, params)`.
2. `ConversionStrategy::new(&mut cursor, mapping_options)` describes columns and builds a Parquet schema plus one `ColumnStrategy` per column. Strategy selection is in `src/query/column_strategy.rs` (`strategy_from_column_description`). Options: `prefer_varbinary` (BYTE_ARRAY instead of FIXED_LEN), `avoid_decimal` (output integer or text instead of DECIMAL), `driver_does_support_i64` (fall back to text for BIGINT on drivers that lack 64-bit binding), `column_length_limit`.
3. `fetch_strategy(concurrent, cursor, ...)` (`src/query/fetch_batch.rs`) selects `SequentialFetch` (one buffer) or `ConcurrentFetch` (two buffers, one fetched by a thread, swapped in `next_batch`).
4. `block_cursor_to_parquet` (`src/query/conversion_strategy.rs`, around line 111) loops `fetch_strategy.next_batch()`. Each batch becomes **one row group** via `writer.write_row_group(num_batch, ColumnExporter)`.
5. `ParquetOutput` (`src/query/parquet_writer.rs`) is `StandardOut` or `FileWriter`. `FileWriter` splits by `FileSizeLimit` (`src/query/batch_size_limit.rs`): row groups per file (`--row-groups-per-file`), file size threshold (`--file-size-threshold`, checked after each row group, so files overshoot), or both. Suffix `_01`, `_02`, and so on, padded by `--suffix-length`.

Defaults (`src/main.rs`):

- Batch size: `min(65535 rows, 2 GiB / bytes_per_row)` (`batch_size_limit.rs`, `DEFAULT_BATCH_SIZE_ROWS`, `DEFAULT_BATCH_SIZE_BYTES`). Memory per row counts ODBC buffers plus a fixed Parquet estimate.
- `--column-length-limit 4096`. This caps variadic columns (`VARCHAR(max)`). Values longer than 4096 bytes on those columns will hit the truncation error path. This is inferred from `fetch_with_truncation_check(true)` plus the default, and not run.
- Compression: `zstd`, with `--column-compression-level-default` (default level 3 per the doc comment). Parquet writer version 2.0, optional per-column encodings (`--parquet-column-encoding COL:ENC`).
- `--encoding Auto` (narrow on Linux, wide on Windows).

### 3.2 What makes it fast

Honest answer: it is not a faster engine than we have. Its speed is one cursor, bulk columnar fetch, one row group per batch, and optional double buffering. It is single-query and single-thread on the write side. There is no partitioned parallel export, and no bulk-load fast path.

Useful ideas to take:

- Row group per fetched batch, so memory is bounded and output is streamed. A 3M-row export does not hold the table.
- File splitting by row groups or size, with numbered suffixes.
- zstd as default compression (issue #489 made it the default).
- Explicit control over Decimal output (`--avoid-decimal`) and over binary physical type (`--prefer-varbinary`). Downstream engines like Polars and Spark care.

### 3.3 Insert subcommand

`src/insert.rs` reads Parquet and calls `odbc_conn.prepare(...)` then `statement.into_column_inserter(1, odbc_buf_desc)`. The row capacity is **1**. Every row is one execute. This is the wrong pattern for bulk load. Do not copy it. Issue #675 (SQL Server 2022, 41M rows, closed) is a symptom of the broader design.

### 3.4 Known issues worth knowing (odbc2parquet)

- #862: panic on non-ASCII text on Windows in 8.1.6 (regression, `query/text.rs`). Closed.
- #484: "Data source must return valid UTF16 in wide character buffer" on MSSQL. Closed (fixed in 3.1.1).
- #640: "Memory required to hold a single row is larger than the limit" (batch sizing). Closed.
- #708: buffer calculation wrong for decimal scale > 2. Closed.
- #476, #477: file-size threshold and `--no-empty-file` behavior. Closed.
- #675: SQL Server 2022 insert fails with "[Shared Memory] General network error". Closed. The maintainer asked for `-vvv` logs and suggested DSN and driver-version checks. I read only part of the thread and did not confirm the resolution.

---

## 4. arrow-odbc-py (Python binding)

### 4.1 Zero-copy handoff

Rust side (`src/reader/arrow_odbc_reader.rs`, `next_batch`): each batch is a `StructArray`, which is converted with `FFI_ArrowArray::new(&array_data)` and `FFI_ArrowSchema::try_from`. These are exported through the Arrow C Data Interface.

Python side (`python/arrow_odbc/reader.py`, `BatchReaderRaii.next_batch`): calls `lib.arrow_odbc_reader_next` with cffi-allocated `ArrowArray`/`ArrowSchema` structs, then `Array._import_from_c(array_ptr, schema_ptr)` and `RecordBatch.from_struct_array`. This is zero-copy: pyarrow takes ownership of the same buffers that Rust allocated.

Granularity: per record batch, not a stream. There is no `__arrow_c_stream__`; the Python wrapper implements `BatchReader` by hand (`reader.py`, `BatchReader`, `BatchReaderProtocol` in `batch_reader_protocol.py`). `pyarrow.RecordBatchReader` is what consumers get when they want a real stream.

Ours: `ArrowStream` / `ExportReader` (`src/arrow/export.rs`) exposes a PyCapsule for the C stream interface. That is a true stream, not a per-batch call, and our producer thread is already decoupled from Python.

### 4.2 GIL

cffi releases the GIL around foreign calls by default (cffi documentation). So the Rust fetch and decode inside `lib.arrow_odbc_reader_next` runs without the GIL, and only pyarrow's import and the Python loop hold it. We did not verify this in a running build, so treat it as a claim from the cffi docs. The only thing that could hold it is pyarrow's import, which is cheap.

Ours: the producer thread decodes without the GIL (`src/arrow/export.rs`, `produce`). The consumer takes the lock only to call `recv` (`ExportReader::next`).

### 4.3 API shape

`read_arrow_batches_from_odbc(query, connection_string, batch_size=65535, user, password, parameters, max_bytes_per_batch=2**29, max_text_size, max_binary_size, falliable_allocations, login_timeout_sec, packet_size, schema, map_schema, fetch_concurrently=True, query_timeout_sec, payload_text_encoding)` (`python/arrow_odbc/read_arrow_batches_from_odbc.py`).

Useful features there that we do not have, or have differently:

- `map_schema(schema)` callable, so the caller can rewrite the inferred schema (for example, float32 to float64, or everything to string) while keeping the generic path.
- `schema=` override, for exact types.
- `parameters` are **always bound as VARCHAR** strings (docstring). We bind typed parameters; that is better.
- `query_timeout_sec`, `login_timeout_sec`, `packet_size` per connection.
- Multi-result-set support through `more_results()` (`ArrowOdbcReader` state machine; the cursor state is hidden from Python).
- `Connection.enable_connection_pooling()` (driver manager pooling).

### 4.4 Known issues (arrow-odbc-py, SQL Server relevant)

- #56 (open): `VARCHAR(max)` fails with "ODBC reported a size of '0'". Maintainer's answer: pass `max_text_size`, or avoid unbounded types.
- #155 (closed): MSSQL + Linux, mojibake for N'...' through the narrow path. Driver-side. Use `payload_text_encoding=Utf16`.
- #148 (closed): tz-aware timestamp insert to `DATETIMEOFFSET` failed. Fixed in 9.1.0, via text.
- #196 (closed): silent truncation on insert (see 2.3).
- #247 (open, 2026-10-23): no way to pass an Entra access token. The Microsoft driver supports `SQL_COPT_SS_ACCESS_TOKEN` before connect. arrow-odbc only has connection-string auth. A comment notes mssql-python already has a `token_provider` argument.
- #239 (closed): database keywords as column names. Fixed with quoting rules.
- #191 (closed): Sybase insert performance, 3.7k to 105k rows/s, solved by connection-string options (`EnableBulkLoad=3;HomogeneousBatch=2`). Relevant as evidence that array-insert speed depends on driver options. No Microsoft equivalent is verified here.
- #87, #97, #118: see 1.2, 1.3 and 2.1.

---

## 5. Issues on the arrow-odbc crate itself (SQL Server relevant)

- #87 (closed): performance sensitive to `max_text_size` (see 1.2).
- #97 (closed): `TooLargeValueForBuffer` despite large limits; Simba driver reports column size 255. Workaround `DefaultStringColumnLength=0`.
- #118 (closed, 25.1.1): insert text buffers rebound every time a longer element appears. This is the likely reason for the slow insert in our 10.6.3 run.
- #133 (closed): `LongVarchar` column gives buffer-limit error. Maintainer: streaming per row is not worth it. Bulk fetch still wins down to a few KB.
- #135 (closed): PostgreSQL `TooLargeValueForBuffer` at 32760 bytes.
- #73 (open): optimize `Decimal(x, 0)` parsing. This is the text round trip that we avoid.
- #177 (closed, 2026-03): UTF-16 to UTF-8 conversion sometimes fails.
- #132 (closed): locale-dependent column name length in narrow `SQLDescribeCol`.

Not in the list: I found no issue on read correctness for timestamps or decimals in the titles I scanned. I did not search for "precision" specifically.

---

## 6. Where we already do better

- **No ODBC layer.** No driver manager, no `libodbc`, no DSN or locale decisions, no narrow vs wide choice. Our UTF-16 path decodes NVARCHAR bytes directly (`src/arrow/sink.rs`, `utf16le_to_utf8`). Issues #155, #862, #484 and #177 do not apply to us.
- **No buffer guessing.** `VARCHAR(max)` and `NVARCHAR(max)` work without `max_text_size`, per the plan design. Their `ZeroSizedColumn` error (#56) does not exist for us. (This is inferred from `plan.rs`; we have not run a `VARCHAR(max)` read to confirm it.)
- **No truncation bugs.** Buffers grow per value (`plan.rs` widths are estimates). Issues #97, #52, #133, #135 cannot happen.
- **Timezone-aware timestamps.** `DATETIMEOFFSET` comes out as UTC microseconds (`Kind::Timestamp { utc: true }`). arrow-odbc returns it as a string.
- **Typed parameters** instead of all-VARCHAR.
- **Stream, not batch calls.** PyCapsule C stream (`ArrowStream`) with the GIL released during decode.
- **Bulk-copy write path.** Our target is bulk copy, which arrow-odbc does not implement. We have no like-for-like arrow-odbc write number yet (see 8.1). The only direct comparison is `bcp`, which is ahead of us (see TL;DR).
- **Parallel read.** `python/mssql_python/arrow_parallel.py` partitions reads. arrow-odbc has no parallel read.

Our weak points relative to them:

- **Precision.** `datetime2(7)` and `time(7)` are truncated to microseconds (`sink.rs`, `timestamp()` and the `time()` path around lines 223-234 and 549). arrow-odbc keeps nanoseconds. This is a real fidelity gap and the one thing to fix on read.
- **No byte-budgeted batches.** `export.rs` takes `batch_rows` only. A table of LOBs or 4000-char columns can produce very large batches. arrow-odbc's `max_bytes_per_batch` is the mechanism to copy.
- **No `map_schema` / `schema` override** on the Arrow read API.
- **No stream-to-file export.** odbc2parquet streams to Parquet with bounded memory. We have no equivalent.
- **Native Arrow row is missing from our benchmark** (`ERR` in `RESULTS.md`).

---

## 7. Ideas to adopt, ranked by expected gain for our driver

Gains are estimates from reading the code and the quick-run numbers. None of them has been prototyped.

1. **Keep 100 ns precision for `datetime2(7)` and `time(7)`.** Map precision 7-9 to Timestamp(ns) and Time64(ns), and scale the TDS fraction accordingly (arrow-odbc `schema.rs` `Timestamp` arms and `precision_to_time`; our `sink.rs` `timestamp()` and `time()`). Expected gain: correctness, with no measurable cost. Test: round-trip `datetime2(7)` values with 100 ns digits.

2. **Byte-budgeted batches.** Compute rows per batch as `min(batch_rows, max_bytes / row_width)` from `Plan.widths` plus a `max_bytes_per_batch` option (default 512 MiB, as in arrow-odbc `odbc_reader.rs` `buffer_size_in_rows`). Expected gain: bounded peak memory on LOB and wide-text tables, and steadier batch sizes. Throughput change is small, probably under 10%.

3. **Surface server-side truncation and warnings on write.** For our bulk path, treat any overflow or `SUCCESS_WITH_INFO`-style server message as an error, not a silent cut (arrow-odbc-py #196 is the cautionary case). Expected gain: correctness on the write path. No perf cost. Verify our current behavior with a too-long string test.

4. **Streaming Parquet export with bounded memory.** Adopt odbc2parquet's model: one row group per batch, file splitting by row groups or size with numbered suffixes, zstd default (`query.rs`, `conversion_strategy.rs` `block_cursor_to_parquet`, `parquet_writer.rs` `FileWriter`). Expected gain: peak memory bounded by batch size instead of table size. Speed roughly that of the Parquet encoder, so not a headline win. Do not copy `insert.rs` row-capacity 1.

5. **`insert_into_table(reader, table, chunk_size)` API and identifier quoting rules.** Take the API shape from `arrow-odbc-py/python/arrow_odbc/insert_into_table.py` and quoting from `src/odbc_writer/insert_statement.rs` (`QuoteOffensively` / `QuoteDefensively`, pre-quoted names passed through). Expected gain: API parity and fewer escaping bugs. No performance gain. Do not copy their INSERT path; the bcp reference is the write-side bar.

6. **Connection and query knobs on the read API.** `login_timeout_sec`, `query_timeout_sec`, `packet_size`, and multi-result-set iteration (arrow-odbc-py `connect.py` and `reader.rs` `more_results`). Also `map_schema` as a callable hook. Expected gain: feature parity with the most-used Python ODBC wrapper. No performance gain.

Items considered and rejected:

- Adopting arrow-odbc's `max_text_size`. Our buffers grow per value, so the knob has no purpose for us.
- Adopting the double-buffer `ConcurrentOdbcReader`. Our `sync_channel` producer already overlaps fetch and decode. Tuning `depth` on high-latency links is worth a measurement, but it is not a port.
- Adopting parameters as VARCHAR. Ours are typed.

## 8. Recommended next steps

1. Re-run the arrow-odbc comparison with arrow-odbc 26.x, not 10.6.3, and with `chunk_size` in {1000, 4000, 65536}. Add pyodbc `fast_executemany` on the same table. This settles whether the 130 s insert is version or chunk size. Use `benchmarks/drivers/arrow_odbc_driver.py`.
2. Fix the `rust_port` `ERR` for `.arrow` workloads in `benchmarks/RESULTS.md`, so our native Arrow path has a number in the same table.
3. Write a precision test for `datetime2(7)` and `time(7)` (idea 1).
4. Decide whether byte-budgeted batches (idea 2) belong in the Python API, `read_arrow`, or both.

## Scope and limits

- Read-only. Nothing under `~/devops/mssql-python-rs` was modified except this file. Nothing committed.
- Not read in depth: `odbc-api` internals (for example, the exact `TooLargeValueForBuffer` trigger and `ConcurrentBlockCursor` locking), `insert_statement.rs`, `odbc2parquet` `src/input.rs` (Parquet to ODBC type mapping) and `column_strategy.rs` beyond the entry point, and the `arrow-odbc-py` docs site.
- No code was run. Everything here is from source reading and from `benchmarks/RESULTS.md`. Claims about the cffi GIL behavior and the SQL Server ODBC driver's array-insert implementation come from documentation or inference and need a test.
- The benchmark numbers come from a loaded machine in `--quick` mode. Compare ratios, not absolutes.
- Issues were read from titles, and from bodies for the ones cited. Closed issues are cited as fixed only where the thread says so.
