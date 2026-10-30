# Export research: fast SQL Server to file (CSV, Parquet, Arrow IPC)

Scope: how to get data out of SQL Server into files quickly, what it means for
mssql-python-rs, and what to build or benchmark. Companion to
`docs/PERFORMANCE_RESEARCH.md`, which holds our measured read/write numbers.
Numbers from that file are cited as "(ours)". Everything else is from the
sources below and is marked unverified unless we reproduce it.

## 0. Sources and their status

| # | Source | Status | Notes |
|---|---|---|---|
| 1 | polymorphe.org, "Export data from SQL Server to CSV as fast as possible" | Read in full | Benchmark on TPC-H orders, 15M rows: bcp 21 s, sqlcmd 38 s, SSIS 39 s, FastBCP 3 s. Single run, no hardware given. Author sells DBA services and promotes FastBCP, so treat as marketing-tinged. |
| 2 | stackoverflow.com/questions/68763007 | Not read. Fetch refused. | Only a search snippet was available. Nothing usable. |
| 3 | dba.stackexchange.com/questions/29155 | Not read. Fetch refused. | Search returned only other pages (C# Corner, MS Learn, SQLYARD). No answers from the thread itself. |
| 4 | learn.microsoft.com, Bulk import and export of data (ver17, updated 2026-10-19) | Read, overview only | Lists bcp, BULK INSERT, OPENROWSET, SSIS wizard. Format files. Says bcp is the only tool that generates format files. |
| 5 | reddit.com/r/SQLServer/comments/okowod (files stored in SQL Server) | Not read. Fetch refused. | Nothing usable. The BLOB section below is therefore not backed by this source. |
| 6 | devart.com, export SQL table to CSV | Read | Lists SSMS wizard, SSMS save results, PowerShell, bcp, dbForge. No numbers. Vendor page. |
| 7 | sqlyard.com, 2026-01-29 | Read | Agent-job export patterns: bcp, PowerShell wrapper, CmdExec. Qualitative speed claims only. Contains a job-name mismatch (em dash vs hyphen) and a possibly outdated bcp cert flag. |
| 8 | skyvia.com, export SQL Server to CSV | Read, first 100k chars | Vendor page. Covers headers, RFC 4180 quoting, NULL handling via ISNULL, bcp -C 65001, sqlcmd -f o:65001. No benchmarks. |

Extra search snippets (not fetched): an arpe.io (FastBCP vendor) benchmark
claiming bcp 62 s vs FastBCP 6.6 s on 15M rows, and bcp 98 s vs 3.3 s in a
CLI comparison. Both are vendor claims. The "bcp uses UTF-16 by default"
explanation in that snippet conflicts with bcp -c being non-Unicode. Treat it
as unverified.

Net: we have one real benchmark (polymorphe) and our own numbers. Everything
on FastBCP speed is vendor-sourced.

## 1. Technique table (deduplicated)

Columns: technique | source | claimed speed | applies to our driver? | already covered?

| Technique | Source | Claimed speed | Applies to our driver? | Already covered? |
|---|---|---|---|---|
| `bcp queryout -c -t, -T` (char mode, single stream) | 1, 4, 6, 7, 8 | 21 s for 15M rows (1). 5.76 s for 3M taxi rows in-container (ours, `bcp queryout -n`). | Yes. This is the baseline to beat. | Partly. We measured `-n` only. Char mode is unmeasured. |
| `bcp queryout -n` (native) | 4 (MS Learn: native is the fastest, no conversion) | Not in sources. Ours: 5.76 s / 3M rows. | Yes, as baseline. | Yes (ours). |
| Parallel `bcp queryout` processes, each with a key-range WHERE | search summary, no source benchmark | Unverified. | Yes. Free benchmark arm, no driver work. | No. |
| sqlcmd `-Q ... -o file -s, -W -w N` | 1, 8 | 38 s for 15M rows (1). | Baseline only. Not fast. Header line is malformed (1). | No. Not worth benchmarking beyond a reference point. |
| SSMS "Save Results As" | 6, 8 | No speed claim. | No. GUI, grid loads rows. | n/a |
| SSMS Import/Export wizard, SSIS flat-file destination | 1, 6, 8 | 39 s for 15M rows (1). | No. | n/a |
| SQL Agent CmdExec step calling a .ps1 that runs bcp | 7 | No speed claim. Ops pattern. | No. Operational. | n/a |
| PowerShell `Invoke-Sqlcmd \| Export-Csv` | 6, 7 | No speed claim. | No. Likely slow (unverified). | n/a |
| Header row via separate UNION ALL row, or PowerShell prepend | 7, 8 | n/a | Yes, as a driver option `header=True`. | No. |
| FastBCP: parallel range split (RangeId, Ntile, Physloc), one file per partition, optional merge | 1 (3 s for 15M), search snippet (6.6 s vs 62 s) | Vendor claims. Unverified. | Yes, this is our proposal 2. | No. Read partitioning exists (ours, §4), export does not. |
| FastBCP: Parquet / JSON / XLSX output, cloud targets | 1 (vendor) | n/a | Parquet: yes (proposal 3). | No. |
| Partitioned read by PK range / physloc / NTILE | ours (PERF §4), 1 | 1.4M to 4.6-5.4M rows/s at 1 to 16 partitions (ours) | Yes. Same machinery for export. | Reads yes (ours). Export no. |
| Native Arrow decode then write columnar file | ours (PERF §1) | Read 2.1 s for 3M rows single stream (ours). | Yes. Keep Arrow batches, never build Python objects. | Read side yes (ours). Write side no. |
| Compression of output (gzip, zstd) via driver | 8 (Skyvia option, unspecified) | Unverified. Ours: TDS has no wire compression (PERF §3). | Output file compression: yes, cheap in Rust. Wire compression: no. | No. |
| UTF-8 output: `bcp -C 65001`, `sqlcmd -f o:65001`, or convert after | 4 (bcp 2012 docs say 65001 unsupported, older), 8 | n/a | Yes. Default UTF-8 output. | No. |
| CSV quoting by hand: `'"' + REPLACE(ISNULL(CAST(col AS nvarchar(max)), ''), '"', '""') + '"'` | 8 | n/a | Driver should do this natively (RFC 4180). | No. |
| Column width / truncation fixes (`sqlcmd -w`, SSMS max chars) | 8 | n/a | Not applicable to a driver writer. | n/a |
| Export to file via SQL Server `OPENROWSET`/`BULK` file write | 4 (import only, export not supported) | n/a | No. SQL Server cannot write CSV via BULK. | n/a |
| Data-driven chunking for very large tables (Skyvia advice) | 8 | n/a | Yes, same as partitioned export. | No. |

## 2. Pitfalls

Encoding
- bcp `-c` writes the default code page, not UTF-8. Use `-C 65001` on
  SQL Server 2016+ (8, 7). The 2012 docs say 65001 is unsupported there (4).
- bcp `-w` is UTF-16 (widechar). If the snippet's "bcp defaults to UTF-16"
  claim is true for any mode, it needs a check. Unverified.
- sqlcmd `-u` writes UTF-16, not UTF-8. Use `-f o:65001` (8).
- No BOM by default in bcp. Excel users may want a BOM (`utf-8-sig`). Make it
  an option. Unverified for our driver.
- Non-ASCII in nvarchar: UTF-8 collations (`*_UTF8`) cut string bytes in half
  for transport (ours, PERF §3). Export of a UTF-8 varchar column writes bytes
  without conversion, which is cheaper than UTF-16 to UTF-8.

Quoting
- bcp and sqlcmd have no quoting switch (8). Fields with comma, quote or
  newline break naive CSV. The fix is RFC 4180: wrap in double quotes, double
  inner quotes (8). The driver should do this, not the query.
- Embedded CR or LF in text: rows split. Quote them (RFC 4180 allows it).
- Separator in data: `-t;` is not safe either if values contain `;`.
- Row terminator: bcp default on Windows is CRLF. Pick `\n` for Linux and
  make it explicit (unverified against bcp defaults on Linux).

NULLs
- Concatenation with NULL yields NULL for the whole row. Use `ISNULL` or
  `CONCAT_WS` (8). For a driver, NULL should be an empty field by default and
  `null_as="..."` an option. Behavior of bcp for NULL in char mode: unverified
  in our tests.
- Empty string vs NULL is indistinguishable in CSV. Document it or add a
  quoting rule (`""` vs unquoted empty).

Dates and times
- datetime2(7) and datetimeoffset have more precision than CSV consumers
  expect. Choose a fixed format (ISO 8601 with fixed fractional digits). Sources
  do not cover this (8 says only "may need cleanup").
- bcp `-c` uses a fixed datetime text form. Its exact format for datetime2 and
  time: unverified here.
- For Arrow/Parquet output, keep timestamps typed. Do not stringify. Our
  datetime path is fast via the C API (ours, PERF §7).

Decimal and floats
- Decimal text: trailing zeros are preserved by scale. Float text can differ
  in precision. Unverified against bcp output.
- For decimals, format from i128 plus scale in Rust. Do not go through a
  Python Decimal (ours: about 170 ns per cell in Python, PERF §7).

BLOB and varbinary
- bcp `-c` on varbinary writes hex or garbage. Use `-n` for binary round trip,
  or an explicit export format (base64 or hex). Sources do not state the
  exact bcp behavior, so this is unverified.
- Huge varbinary(max) cells: reading the whole value into one buffer can blow
  memory. Stream with chunked `GetBytes` or the TDS PLP stream. Unverified in
  our driver.
- Files-per-row export needs a safe filename (no user text in paths).
  Source for this pitfall is the general practice, not the Reddit thread
  (fetch refused).

Locking and consistency
- A single `queryout` is one statement and one snapshot in practice (not
  documented in the sources). Parallel partitioned exports read at different
  times. Without SNAPSHOT isolation, rows can change between partitions.
  Document this, or open all partition connections under snapshot isolation
  (when the database allows it). Unverified in our tests.
- `WITH (NOLOCK)` makes the export read dirty data. Do not default to it.
- Long exports hold shared locks under read committed only for the duration of
  each read, not the whole query. Unverified.

Memory
- SSMS and the wizard load the result into a grid. Do not use them for large
  exports (6, 8).
- sqlcmd `-w` truncates long lines. Set width or use bcp (8).
- Our test box has a 2.5 GB memory cap. The container was killed by the OOM
  killer during a 3M-row SELECT INTO (ours, PERF caveats). Any export
  benchmark needs batch-bounded memory and checks on RSS.
- Arrow batches bound memory by batch size. Do not hold a whole partition in
  memory before writing.

Operational
- Agent or scheduled export runs as a different account. Paths and shares may
  not be reachable (7). Not a driver issue, but a doc item.
- Row-count verification: compare file row count with `SELECT COUNT_BIG(*)`
  and the WHERE clause (8). The driver should return the row count it wrote.

## 3. Proposals for the driver (ranked by expected gain / effort)

Baseline to beat (ours, PERF §1, §5): `bcp queryout -n` 5.76 s for 3M taxi
rows. Our single-stream native read is about 2.1 s with a no-op sink. Our
partitioned read at 8 to 16 partitions is 0.55 to 0.75 s (ours). Export wall
time will be the max of read, encode and write, so these are upper bounds on
gain, not promises. CSV formatting cost is unmeasured.

### P1. Rust-side streaming export, no Python objects (CSV and Arrow IPC first)

API sketch:

    mssql_python.export(conn_str, query, path, format="csv" | "arrow-ipc",
                        header=True, encoding="utf-8", null_as="",
                        quoting="rfc4180", batch_size=65536)
    -> ExportResult(rows=..., bytes=..., seconds=...)

- Fetch Arrow batches with the existing native path, format them in Rust, write
  to the file. The GIL is never taken.
- Expected gain: wall roughly equal to the read time, about 2.1 s on taxi
  (ours projection), so about 2.7x bcp. Formatting cost unverified.
- Effort: M. Reuses the native Arrow fetch from PERF roadmap #1.

### P2. File-per-partition parallel export

    mssql_python.export(conn_str, query, "out/part-{i:03}.csv",
                        format="csv", partition_on="trip_id", partitions=8)

- Same partitioning as `read_arrow` (PERF §4). Each partition writes its own
  file. A merge step is optional and separate (FastBCP offers merge as an
  option, unverified).
- Expected gain: read at 8 partitions is 0.55 to 0.75 s (ours), so the target
  is roughly 1 s end to end for 3M rows, which is about 6x bcp (projection).
  FastBCP's vendor claim is 3 s for 15M rows on an unknown box (unverified).
- Effort: M. Needs SNAPSHOT or a documented caveat (see pitfalls).

### P3. Parquet writer, Rust side, zstd default

    mssql_python.export(..., format="parquet", compression="zstd")

- Typed output (timestamps, decimals) with no stringify step. Smaller files.
  Parquet is what FastBCP offers (1, vendor).
- Expected gain: not a speed win over CSV. Win is downstream size and types.
  Speed unverified.
- Effort: M. Check the `parquet` crate's writer fits our Arrow version.

### P4. CSV correctness defaults (RFC 4180, UTF-8, header on)

- Defaults: header on, UTF-8 without BOM (option for `utf-8-sig`), `\n` row
  terminator, RFC 4180 quoting, NULL as empty field, `null_as=` option.
- Expected gain: correctness and fewer user-written quoting expressions (8).
  No speed claim.
- Effort: S.

### P5. Fast decimal and datetime formatting in the writer

- Format decimals from i128 plus scale, and datetime2 from the raw fields, into
  a byte buffer. No Python, no Decimal, no `chrono` string round trip where it
  is slow.
- Expected gain: decimals are 9 of 20 taxi columns, about 85% of Python row
  cost (ours, PERF §7). Formatting gain unmeasured. Guess 2x on the formatting
  stage, unverified.
- Effort: S to M.

### P6. BLOB-to-files export

    mssql_python.export_blobs(conn_str, query, out_dir,
                              name_col="id", blob_col="payload", chunk=1<<20)

- Stream each varbinary(max) value to its own file using chunked reads, no
  full-cell buffer, no Python bytes object.
- Expected gain: unverified. Avoids the memory spike and the GIL. The Reddit
  thread that might give real numbers could not be read.
- Effort: M.

### P7. Snapshot-consistent partitioned export

- Open all partition connections under SNAPSHOT isolation when the database
  allows it. Otherwise document the inconsistency.
- Expected gain: correctness. No speed cost (unverified).
- Effort: S to M.

### P8. Benchmark the honest baselines before building (not a driver change)

- Add these arms to `benchmarks/experiments/` (see §4).
- Expected gain: decides whether P1 and P2 are worth it. Effort: S.

Ranking note: P1 and P2 together are the main bet. P4 and P8 are cheap and
should go first in practice.

## 4. Benchmark workloads to add

Each arm reports wall time, client CPU, rows/s and output bytes. Run on a quiet
box or report load average, since ours was loaded (PERF caveats).

Baselines (external tools)
- `bcp queryout -n` (native), `bcp queryout -c -C 65001 -t, -r "\n"` (char,
  UTF-8).
- `sqlcmd -Q ... -s, -W -w 65535 -f o:65001` (reference only).
- Parallel `bcp queryout` with N processes, each with a key-range WHERE
  (N = 4, 8).
- FastBCP, if a license is available, as a reference point only. Vendor claim
  to check.

Our arms
- `export` CSV single stream, parallel partitioned 2/4/8/16, Arrow IPC, Parquet
  zstd.
- Same table as the taxi reads (3M rows, 20 columns, the decimal-heavy case).

Table shapes (new)
- Narrow ints only (IDs, counts): tests fixed-width formatting.
- Decimal-heavy (taxi). Datetime2-heavy. Date-only.
- Wide nvarchar with high-cardinality text, with commas, quotes and newlines
  in data. Tests quoting cost and correctness.
- UTF-8 collation varchar vs nvarchar, and mixed Unicode (CJK, emoji).
- NULL-dense (80% NULL) and empty-string-dense. Tests null path and
  NULL vs ''.
- varbinary(max) with 1 KB, 100 KB and 10 MB values (for P6).
- Rowstore vs clustered columnstore vs PAGE-compressed (PERF §3 found PAGE
  reads 2.6x slower server-side, so it may slow export too).
- 15M-row orders-like table (polymorphe's setup) so we can compare to their
  published numbers, with the caveat that the hardware differs.

Correctness checks in every arm
- Row count out equals `COUNT_BIG(*)`.
- Round trip: read the file back with pyarrow (run with `-I`, per the sandbox
  rules for untrusted files) and compare typed values, including NULL, '' and
  embedded quotes.

## 5. Defaults to change

| Setting | Current behavior | Proposed default | Why |
|---|---|---|---|
| `connect(packet_size=)` | Driver default (not 16384) | 16384 | PERF §2 (10 to 15% wall, 40% CPU) |
| CSV header | none in bcp by default | header on for export | Sources 7, 8 |
| CSV encoding | bcp code page | UTF-8, no BOM | Sources 4, 8 |
| CSV quoting | none | RFC 4180 quote-minimal | Source 8 |
| NULL in CSV | (bcp: empty, unverified) | empty field, `null_as=` option | Source 8 |
| Partitioned export consistency | not applicable yet | SNAPSHOT when allowed | P7 |
| Export row terminator | platform-dependent (CRLF on Windows) | `\n` | Linux target |
| Export compression | none | off for CSV, zstd for Parquet | P3 |

## 6. Unverified claims (do not cite without a run)

- FastBCP: 3 s for 15M rows (1), 6.6 s vs 62 s (arpe), 3.3 s vs 98 s (arpe).
  All vendor, one box each, no hardware given.
- Parallel bcp with key-range WHERE is "faster": no benchmark found.
- bcp `-c` NULL and datetime text forms: not tested by us.
- bcp default code page / UTF-16 claim from the arpe snippet: conflicts with
  bcp `-c`. Check before using.
- Decimal formatting speedup (P5). Our measurement is for Python Decimal
  only (PERF §7), not text formatting.
- BLOB export memory behavior (P6). Reddit source unavailable.
- Any CSV export wall-time projection for our driver. Ours is an upper bound
  from read time only.
