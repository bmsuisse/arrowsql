# Performance research: fastest paths in and out of SQL Server

Measured 2026-10-30 on the dev box: SQL Server 2022 in podman on localhost
(2.5 GB memory cap), 24 cores, but load average around 20 from concurrent
agents, so every number is a lower bound and noisy (±15%). Data:
`nyctaxi.dbo.yellow_trips`, 2,964,624 rows × 20 columns (9 decimal, 2
datetime2, ints, float, nvarchar(1)), 798 MB on the wire. Reproduce
everything with `benchmarks/experiments/` (see its README section at the end).

## TL;DR

- **Reads are server-bound, not client-bound, once ODBC and Python are out of
  the loop.** A pure-Rust mssql-tds client drains 3M rows in about 2.1 s
  (1.4–1.6M rows/s, about 380 MB/s) using only 0.6–1.2 s of client CPU.
  Upstream's Arrow path takes 4.1–4.4 s and is fully client-CPU-bound;
  upstream `fetchmany` takes 12.2 s.
- **Parallel partitioned reads are the largest read lever:** 8 connections
  give 4.0–4.3M rows/s and 16 give 4.6–5.4M rows/s. That is 6–8× upstream
  Arrow and 17–22× upstream row fetch.
- **Parallel bulk load is the largest write lever:** 8 connections with
  TABLOCK into a heap reach 1.0M rows/s, against 0.26M single-stream and
  0.43M for `bcp` native.
- **Building Python objects is the ceiling for row fetches.** Decimal costs
  about 170 ns per cell, roughly 85% of a taxi row's cost. Caching Decimal
  values per column fixes most of it.
- **Compression:** TDS has no wire compression. Server-side `COMPRESS()` at
  query time is about 40× slower. UTF-8 collations halve string bytes for
  free. Pre-compressed columns only win on links under about 200 Mbit.
- **`executemany` in autocommit is bounded by log flushes:** 187 rows/s,
  against 26k rows/s inside one transaction. That is a semantics trap, not a
  protocol cost.

## 1. Speed of light for reads

`tds_probe read` (Rust, mssql-tds pinned rev, release build), `SELECT * FROM
dbo.yellow_trips`, one connection:

| Client | Wall | Client CPU | Rows/s |
|---|---|---|---|
| mssql-tds, discard rows (`noop` sink), TLS off | 2.11 s | 0.57 s | 1.41M |
| mssql-tds, discard rows, TLS on | 2.19–2.37 s | 0.74–0.81 s | 1.25–1.35M |
| mssql-tds, Arrow-like columnar sink (UTF-16→UTF-8, typed buffers), TLS on | 2.10–2.33 s | 1.22–1.25 s | 1.27–1.41M |
| mssql-tds, `next_row()` → `Vec<ColumnValues>`, TLS on | 2.07–2.52 s | 1.21–1.24 s | 1.18–1.43M |
| upstream mssql-python `cursor.arrow()` (C++ + ODBC 18) | 4.11–4.40 s | 4.06–4.30 s | 0.67–0.72M |
| upstream `fetchmany(65536)` → Python rows | 12.2–12.6 s | 12.0–12.5 s | 0.24M |
| `bcp queryout -n` inside the container (ODBC, writes a file) | 5.76 s | n/a | 0.51M |

Where the time goes, per 3M rows:

- **Server:** `sys.dm_exec_query_stats` shows about 2.36 s of worker CPU per
  `SELECT *` at DOP 1. A single result stream is serialized by one scheduler
  that produces TDS rows. This is the single-stream ceiling, and the Rust
  client already sits on it.
- **TLS:** +0.2–0.3 s client CPU (AES-GCM at about 3 GB/s) and +5–10% wall.
  Not worth turning off.
- **TDS framing and token parsing:** 0.57 s of CPU.
- **Columnar decode (Arrow-equivalent):** +0.5–0.7 s of CPU. It overlaps the
  server when decoding is pipelined, so wall time stays server-bound.
- **Python objects:** about 1.8 µs per taxi row (see §7), roughly 5.4 s
  holding the GIL. This is why upstream's row path is 12 s.

Projection for the port:

| Path | Projected wall (3M rows) | vs upstream |
|---|---|---|
| Native Arrow, single stream | ~2.1 s | 2× upstream Arrow |
| Native Arrow, 8–16 partitions | 0.55–0.75 s | 6–8× upstream Arrow |
| Python rows, with the §7 fixes | ~3 s | ~4× upstream rows |

## 2. Transport knobs

| Knob | Result |
|---|---|
| Packet size 4096 → 8000 → 16384 → 32767 (mssql-tds, columnar, TLS off) | Wall 2.39–2.45 → 2.08–2.23 → 1.85–1.94 → 1.97–2.63 s; client CPU 1.73–1.80 → 1.01–1.12 → 1.00 → 0.92–1.08 s. **Request 16384**: about 10–15% wall and 40% CPU better than 4096; 32767 adds nothing and is noisier. |
| Packet size on upstream ODBC (`PacketSize=16384`) | No change (4.11–4.53 s), because upstream is client-CPU-bound. |
| Encrypt off / on | Covered under "Where the time goes" in §1: +5–10% wall and +0.2–0.3 s CPU per 800 MB. TLS adds 0.4% to wire bytes (798.5 MB → 801.4 MB). |
| Encrypt strict (TDS 8.0) | The handshake fails against this container (`SSL_ERROR_SYSCALL`; the default container has no strict-capable certificate setup). **Not measured.** Its cost should equal TLS on, since only the prelogin ordering differs. |
| Nagle / socket buffers | Not the bottleneck: the result stream is 16.5k reads of about 48 KB at full throughput. mssql-tds already sets TCP_NODELAY. |
| Shared memory / named pipes | Not available on Linux; localhost TCP is the only transport. |

## 3. Compression

Test table: 100k JSON documents built from taxi rows. Stored size: 91.5 MB as
nvarchar(max), 45.7 MB as varchar(max) with a UTF-8 collation, 37.0 MB with
`COMPRESS()` (GZIP of the UTF-16). Client: upstream Arrow fetch, with gzip
decompressed on 8 threads. Links were simulated with
`benchmarks/experiments/wan_proxy.py` (token-bucket bandwidth plus pipelined
one-way delay).

| Arm | Loopback | 1 Gbit, 2 ms RTT | 100 Mbit, 20 ms RTT |
|---|---|---|---|
| nvarchar (UTF-16) | 0.33 s | 0.90 s | 7.83 s |
| UTF-8 collation varchar | **0.23 s** | **0.48 s** | 4.01 s |
| `COMPRESS(doc)` at query time | 13.5 s | 13.1 s | not run (13 s+) |
| pre-compressed varbinary column, client gunzip | 0.75 s | 0.86 s | **3.78 s** |

Server storage compression, full-row read of 500k rows (mssql-tds, discard
rows):

| Storage | Size | Read rate |
|---|---|---|
| Rowstore | 82 MB | 1.58–1.71M rows/s |
| PAGE compression | 25 MB | 0.57–0.65M rows/s (**2.6× slower**: serial decompression on the server) |
| Clustered columnstore | 12 MB | 1.43M rows/s (−10%) |

**Verdict:**

1. TDS has no wire compression, and per-query `COMPRESS()` is a CPU trap
   (about 40× slower).
2. The real "compression" is sending fewer bytes: UTF-8 collations or
   `varchar` instead of `nvarchar` halve string traffic for free and win on
   every link. Pre-compressed GZIP columns only beat that on links under
   about 200 Mbit, and only for long text.
3. For full exports from the server, rowstore or columnstore beats PAGE
   compression.
4. **Driver API:** no connection-level `compress=` option. Offer an opt-in
   `decompress_columns=[...]` on the Arrow path that gunzips varbinary GZIP
   columns in Rust, in parallel and without the GIL, and document UTF-8
   collations. Arrow IPC compression (zstd/lz4) only matters when re-shipping
   results off-box, as arrowbricks does, never for the SQL Server hop.

### 3.1 Shipped: `utf8_strings` on the Arrow read paths

`read_arrow(..., utf8_strings=True)` (default) and `cursor.execute_arrow(sql, params=None, *,
utf8_strings=True)` apply the UTF-8 result above automatically:

- One round trip describes the query (`sys.dm_exec_describe_first_result_set`) together with
  `SERVERPROPERTY('ProductMajorVersion'/'EngineEdition')` (cached per connection); a second
  samples the first 256 rows (also validating the rewrite). The wrapping SQL is built in Rust (`src/utf8_wrap.rs`):
  `SELECT ..., CAST([c] COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(m)) AS [c], ... FROM (<query>) AS q`,
  m = 4 x declared chars when that fits in 8000, else `max`; ntext goes through `nvarchar(max)`.
  NOT NULL columns get `ISNULL(..., '')` so Arrow nullability is unchanged.
- COLLATE must be on the operand. `CAST(c AS varchar(n)) COLLATE ..._UTF8` only relabels text
  that was already squeezed through the database code page (every non-Latin-1 character
  became `?`). The tests catch this with CJK and emoji.
- Skipped (original query runs): server before 2019, ORDER BY in the query, `?` parameters
  (the describe needs their types), duplicate or empty column names, a failing describe or
  sample, no national column, national columns under 25% of the declared row width (keeps
  the taxi table, one `nvarchar(1)` among 19 numbers, unchanged), or a sample of the first
  256 rows whose UTF-8 size is not at least 10% below its UTF-16 size. The sample matters
  because UTF-8 is 3 bytes per character for CJK against 2 in UTF-16: without it a CJK-heavy
  column grew the transfer by 49% (162.0 MB to 242.2 MB, 100k rows x 5 x 160 chars). Cyrillic,
  Greek, Hebrew and Arabic are 2 bytes in both (no change, skipped); ASCII halves (162.0 MB
  to 81.8 MB). The sample is a `TOP (256)` of the query, so an expensive query (sort,
  aggregate) pays for it once more; use `utf8_strings=False` there. The first rows can
  misjudge data whose script changes later in the result.
- The decoder needed no change: a UTF-8 collation arrives with the collation's UTF-8 flag
  (code page 65001) and 4-byte characters, NULL, `''`, > 8000-byte values and nchar blank
  padding round-trip (tests/common_issues/test_rs_utf8_strings.py).

Measured (release build, `read_arrow`, medians, off/on interleaved; machine under load
average ~8, so loopback is noisy):

| Read | Bytes server to client (off, on) | Loopback | 100 Mbit, 20 ms RTT |
|---|---|---|---|
| `bench_strings` (200k rows, 10 x nvarchar(200)) | 424.0 MB, 214.5 MB (-49.4%) | 0.337 s, 0.320 s (100k rows, n=11) | 7.12 s, 3.69 s (40k rows, 1.9x) |
| `yellow_trips` (20 numeric columns, one nvarchar(1)) | 68.16 MB, 67.66 MB without the share rule | rewrite skipped: same query, 0.89 s vs 0.79 s is noise | 3.50 s, 3.51 s (skipped) |

Before the sample/share rules, taxi was 0.70 s off vs 0.79 s on and 0.65 vs 0.64 on loopback
(a 0.7% byte saving does not pay for the server-side conversion), hence the threshold.
Loopback gains are small because the server's UTF-8 conversion costs about what the
smaller transfer saves; the gain appears with any real network.

## 4. Read parallelism (partitioned reads)

`tds_probe parts`: N connections, each reading a contiguous `trip_id` range
(clustered PK), columnar sink, TLS on, packet 16384.

| Partitions | Wall | Client CPU | Rows/s | Speed-up |
|---|---|---|---|---|
| 1 | 1.92–2.21 s | 1.08–1.14 s | 1.34–1.54M | 1× |
| 2 | 1.26 s (one outlier at 2.56 s) | 1.12–1.32 s | 2.34M | 1.6× |
| 4 | 1.14–1.17 s | 1.17–1.26 s | 2.54–2.61M | 1.8× |
| 8 | 0.69–0.74 s | 1.40–1.45 s | 4.01–4.28M | 2.9× |
| 16 | 0.55–0.65 s | 1.57–1.62 s | 4.56–5.43M | 3.5× |

Client CPU stays flat, so scaling is limited by server schedulers on a loaded
box; expect better on a quiet server. Each partition runs at DOP 1 (no
parallel plan for a range scan), so MAXDOP doesn't help a single stream.
Partitioning is what parallelizes the server.

**Proposed API:**

    cursor.read_arrow(sql, partition_on="trip_id", partitions=8, params=None)

- Bounds come from `SELECT MIN/MAX`. For skewed keys, use
  `partition_bounds=[...]`, or `partition_on="%%physloc%%"` /
  `NTILE`-based splits.
- It opens N pooled connections and merges batches into one
  `__arrow_c_stream__` reader. Order across partitions is not guaranteed and
  the docs must say so.
- It must run on one snapshot: use `SNAPSHOT` isolation when available,
  otherwise document that partitions are not transactionally consistent.

## 5. Write path

All on 1M taxi rows (19 columns) into `perf_research`, SIMPLE recovery,
upstream wheel. `bulkcopy_arrow` is Microsoft's Rust `mssql_py_core`.

| Strategy | Rows/s | Notes |
|---|---|---|
| bulk_copy_arrow, heap, no lock | 247k | |
| bulk_copy_arrow, heap, TABLOCK | 257–270k | Client CPU 3.6 s ≈ wall, so **client-bound in the Arrow→TDS encoder** |
| heap TABLOCK, batch 10k / 100k | 204k / 246k | Small batches cost; batch 0 (one batch) is best |
| clustered PK (identity), TABLOCK | 224k | |
| clustered columnstore (CCI), batch 1,048,576 / 102,400 / 10,000 | 176k / 180k / 105k | Batch ≥ 102,400 goes straight to compressed row groups; 10k lands in the delta store |
| **parallel heap TABLOCK, 2 / 4 / 8 connections** | **497k / 702k / 1.015M** | Encoder parallelizes; BU locks are compatible |
| parallel CCI, 4 connections | 205k | Server-bound (columnstore compression) |
| `bcp in -n` with TABLOCK, 3M rows (ceiling for ODBC) | 435k | |
| tuple `bulkcopy` (50k) | 84k | Python tuples → Rust |
| `OPENJSON(?)` of one 24 MB JSON param (50k) | 49k | Useful TVP substitute |
| `executemany`, **autocommit** (2k) | **187** | One log flush per row: WRITELOG / FLUSHFILEBUFFERS dominate |
| `executemany` inside one transaction (50k / 20k narrow) | 26k / 45k; 98k for 4 int columns | One round trip per batch is not the issue; commits are |
| multi-row `VALUES` (110 rows × 19 params per statement), one transaction | 1.9k | Each 2,090-param statement compiles anew. Never do this. |

Takeaways:

- The single-stream bulk encoder is the bottleneck, at about 3.6 µs of client
  CPU per row. A tighter Arrow→TDS encoder (fixed-width memcpy, decimal
  without bigint math, pre-encoded column metadata) should reach the `bcp`
  ceiling or beyond: about 1.7× or more.
- Parallel loads into a heap with TABLOCK scale almost linearly to 8.
- Into a CCI, use batches of at least 102,400 and don't bother going
  parallel past 2.
- ORDER hints were not measured; they only matter for clustered rowstore
  targets.

## 6. Small-query latency

| | Per query |
|---|---|
| mssql-tds `SELECT 1` (TLS on / off) | 150 / 132 µs |
| mssql-tds point lookup by PK, literal (TLS on / off) | 138 / 105 µs |
| upstream `SELECT 1` | 196–227 µs |
| upstream point lookup, `?` param | 357–403 µs |
| upstream point lookup, literal | 340–351 µs |
| server CPU / elapsed for the point lookup | 9 / 10 µs |
| fresh connect, mssql-tds (TLS on / off) | 5.05 / 3.83 ms |
| upstream connect (pooled by default) | 0.70–0.76 ms |

- **Round trips:** through the counting proxy, upstream used exactly one
  round trip per `execute()` (3,010 reads per direction for about 3,006
  executes), so there are no separate prepare or describe round trips to
  eliminate.
- **Where upstream's ~350 µs goes** (`profile_execute.py`):
  - `DDBCSQLExecute` (ODBC + network): about 195 µs.
  - `_initialize_description` → `_map_data_type`: 20 calls per execute,
    triggering 625 `enum.__get__` calls. That is about 1/3 of client time,
    pure Python, and recomputed on every execute for identical metadata.
  - `FetchAll` is cheap for one row.
- **Targets:** about 120 µs of native round trip plus no more than 30 µs of
  Python per execute, so roughly 150 µs (2.3× faster).
- **Pooling:** mandatory, because a fresh TLS login is about 7× a pooled
  checkout.

## 7. Python boundary: cost per object

`pyobj_bench` (PyO3 0.29, raw CPython C API, CPython 3.13, best of 5):

| Object | ns |
|---|---|
| small int (cached, < 256) | 5.3 |
| large int | 21.0 |
| float | 19.7 |
| 1-char str, UTF-8 | 6.7 |
| 1-char str, cached object (incref only) | 3.7 |
| 34-char str, UTF-8 | 44.2 |
| 34-char str, decode from UTF-16 | 52.8 |
| 34-char str, ASCII fast path (`PyUnicode_New` + memcpy) | 33.8 |
| `Decimal("123.45")` via the constructor | 168.3 |
| `Decimal((sign, digits, exp))` | 184.8 |
| `Decimal(int).scaleb(-2)` | 186.6 |
| `datetime` via the PyDateTime C API | 29.1 |
| 20-tuple via `PyTuple_New` + `SET_ITEM` | 82.8 |
| 20-list | 93.7 |

A taxi row costs about 9 × 170 (decimal) + 2 × 29 + 7 × 15 + 20 + 7 + 83
(tuple), roughly **1.8 µs**, about 85% of it Decimal.

- **Decimal cache:** the taxi decimals are very low-cardinality. Across 100k
  rows they have 3–6,172 distinct values per column (`mta_tax` 5,
  `extra` 23, `fare` 894). A per-column cache keyed by (i128 magnitude,
  scale), bounded at about 4–16k entries, turns about 170 ns into roughly
  10 ns per hit.
- **Other techniques:**
  - Cache small ints and 1-char strings.
  - Use the ASCII fast path for strings, checking with SIMD that the UTF-16
    has no high bytes.
  - Build datetimes with the C API, never via `datetime.fromisoformat`.
  - Fill tuples with `SET_ITEM`.
  - Decode without the GIL into columnar buffers, then build a whole batch
    under the GIL in one tight loop.

## Performance roadmap (ranked by gain ÷ effort)

| # | Change (Rust core unless noted) | Evidence | Expected gain | Effort |
|---|---|---|---|---|
| 1 | Native Arrow fetch: decode TDS straight into Arrow builders without the GIL, pipelined on the runtime thread (batch N+1 decodes while Python consumes N) | §1: client CPU 1.2 s vs server 2.1 s; upstream Arrow 4.1–4.4 s, client-bound | **2×** upstream Arrow single-stream; wall = server time | M |
| 2 | Partitioned parallel reads (`read_arrow(..., partition_on=, partitions=)`) merged into one Arrow stream | §4: 1.4M → 4.0–5.4M rows/s at 8–16 partitions | **3–3.5× over #1**, 6–8× upstream Arrow | M |
| 3 | Parallel bulk load (`bulkcopy_arrow(..., workers=N)`): split the source across N pooled connections with TABLOCK into heaps; auto-disable for CCI/clustered | §5: 257k → 1.015M rows/s at 8 workers | **4×** writes, 2.3× `bcp` | M |
| 4 | Own Arrow→TDS bulk encoder in our core (fixed-width memcpy, precomputed metadata, decimal via i128), off `mssql_py_core` | §5: single stream is client-CPU-bound at 3.6 µs/row; `bcp` does 435k/s | ≥ **1.7×** single-stream bulk | M–L |
| 5 | Per-column Decimal value cache plus small-int, 1-char-str and ASCII string fast paths, and datetime via the C API in the row builder | §7: Decimal is 85% of row cost; 3–6k distinct values per 100k rows | **2–3×** on decimal-heavy `fetchall` (taxi: ~1.8 → ~0.5 µs/row) | S |
| 6 | Python layer: cache `cursor.description` and converter maps per result metadata; replace `_map_data_type` enum lookups with a dict | §6: about 1/3 of client time per point query | About 100 µs per execute; point lookup 350 → ~150 µs (**2.3×**) | S |
| 7 | Request packet size 16384 by default | §2 | 10–15% wall, 40% client CPU on large reads | XS |
| 8 | Two-phase fetch: decode a batch without the GIL into columnar buffers, then build all Python rows for the batch in one GIL-held loop (`PyTuple_New` + `SET_ITEM`); overlaps network with Python | §1 and §7 | 20–30% on row fetch beyond #5 | M |
| 9 | `executemany`: all parameter sets in one multi-RPC TDS message (one round trip), plus an opt-in fast path routing large `executemany` to bulk copy. Keep autocommit semantics; document that autocommit means one log flush per row | §5: 187 rows/s in autocommit vs 26k in a transaction vs 84–257k via bulk | 100×+ for users who opt in; 1 RTT per batch for all | S–M |
| 10 | Connection pooling on by default with `sp_reset_connection` piggy-backed on the first request | §6: fresh TLS login 5 ms vs 0.7 ms pooled | **7×** connect-heavy workloads | S |

Also cheap:
- Keep TLS on (5–10% cost).
- Document UTF-8 collations, columnstore, and avoiding PAGE compression for
  export tables.
- Never use multi-row `VALUES` with many parameters.

## Driver API additions

- `cursor.read_arrow(sql, params=None, *, partition_on=None, partitions=1,
  partition_bounds=None, batch_size=65536) -> ArrowStream`. Zero-dependency
  `__arrow_c_stream__`, ready for DuckDB, polars and pyarrow.
- `cursor.bulkcopy_arrow(..., workers=1)`. Reject or downgrade `workers > 1`
  for CCI and clustered targets; force `table_lock=True` when `workers > 1`.
  Default `batch_size` of 102,400 for CCI targets.
- `cursor.fetch_arrow(..., decompress_columns=["doc_gz"])`: GZIP (as written
  by `COMPRESS()`) decompressed in Rust in parallel. With
  `decode="utf-16le"`, it returns strings.
- `connect(..., packet_size=16384)`, defaulted.
- `executemany(..., fast=True)` (opt-in, like pyodbc's `fast_executemany`):
  routes to bulk copy for plain `INSERT INTO t (cols) VALUES (?, ...)`
  statements.
- No transport compression option; there is nothing in TDS to switch on.

## Caveats

- The box was heavily loaded (load average 17–21, swap full). Partition
  scaling and absolute numbers will improve on a quiet server.
- The SQL Server container was killed twice during this work. conmon was
  killed, probably by the host OOM killer, while the server grew its buffer
  pool on a 3M-row `SELECT INTO`. Storage-format tests were therefore cut to
  500k rows.
- Strict encryption (TDS 8) and ORDER hints were not measured.
- Upstream's `Encrypt=no` couldn't be passed, because the base connection
  string already sets `Encrypt` (duplicate-keyword error).
- Aggregate reads (columnstore vs rowstore for `GROUP BY`) were not measured;
  this report is about data movement.

## Reproducing (`benchmarks/experiments/`)

| File | What |
|---|---|
| `tds_probe/` | Rust probe over mssql-tds: `read`, `parts`, `latency`. Build with `flock $CARGO_TARGET_DIR.lock cargo build --release --manifest-path benchmarks/experiments/tds_probe/Cargo.toml`; run via `run_probe.sh` |
| `upstream_reads.py` | Upstream Arrow, `fetchmany` and latency; run via `run_py.sh` |
| `profile_execute.py` | cProfile split of upstream per-execute cost |
| `write_paths.py`, `executemany_diag.py` | Write strategies (needs `TAXI_PARQUET`) |
| `compression.py` | `setup` / `read` arms; combine with `wan_proxy.py --mbit --rtt-ms` |
| `wan_proxy.py` | Byte-counting, bandwidth- and latency-shaping TCP proxy |
| `bcp.sh`, `sqlq.sh` | `bcp` and `sqlcmd` inside the container |
| `pyobj_bench/` | PyO3 per-object micro-bench (`maturin develop --uv --release`, then `run.py`) |
