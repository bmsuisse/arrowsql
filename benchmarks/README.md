# Benchmarks

One entry point: `python benchmarks/run.py`. It compares the Rust port against upstream `mssql-python`
and the other ways of moving data in and out of SQL Server from Python (pyodbc, connectorx,
arrow-odbc, pymssql, fastmssql, and the `bcp` CLI as a bulk speed-of-light reference).

```
python benchmarks/run.py --list                          # drivers x workloads matrix
python benchmarks/run.py --quick                         # everything, reduced sizes (~20 min on a loaded box)
python benchmarks/run.py --driver rust_port --driver mssql_python --workload 'read.taxi_100k.*'
python benchmarks/run.py --workload 'write.bulk_arrow_*' --repeats 5
python benchmarks/report.py                              # latest results -> benchmarks/RESULTS.md + results/report.html
```

`run.py` and `report.py` need only a stdlib `python3`; each driver runs in its own venv.

## One-time setup

* SQL Server 2022 in podman container `mssql-rs`, credentials in `<main repo>/.env.local`
  (`DB_CONNECTION_STRING`; the main repo is the first entry of `git worktree list`, so this works from any worktree).
* Taxi data: `scripts/load_taxi.py` (creates `nyctaxi.dbo.yellow_trips` 2,964,624 rows and `yellow_trips_100k`).
* Synthetic tables (`bench_strings`, `bench_decimals`, `bench_datetimes`, `bench_lob`, `bench_wide`) are created
  by `run.py` on first use (`benchmarks/datasets.py`, via `sqlcmd` in the container so no driver under test is involved)
  together with `results/datasets.json`, the average `DATALENGTH` bytes per row used for MB/s.
* Venvs, all under `<main>/`, python 3.13, created with `uv`:

| driver | venv | notes |
|---|---|---|
| `mssql_python` | `.venv-upstream` | upstream wheel (ODBC) |
| `rust_port` | `.venv` | this repo; `scripts/build.sh --release` first. Skipped if import/connect fails |
| `pyodbc`, `pyodbc_fast` | `.venv-bench-pyodbc` | `fast_executemany` off/on; needs unixODBC, see below |
| `connectorx` | `.venv-bench-connectorx` | Arrow only, read only |
| `arrow_odbc` | `.venv-bench-arrow-odbc` | Arrow read + Arrow insert, needs unixODBC |
| `pymssql` | `.venv-bench-pymssql` | FreeTDS; includes `bulk_copy` |
| `fastmssql` | `.venv-fastmssql` | async; driven from a private event loop |
| `bcp` | (none) | `podman exec mssql-rs /opt/mssql-tools18/bin/bcp`, native format |

```
uv venv --python 3.13 <main>/.venv-bench-X && uv pip install -p <main>/.venv-bench-X <pkg> pyarrow duckdb
```

**ODBC without sudo.** pyodbc and arrow-odbc link `libodbc.so.2`, which this box lacks. `<main>/.bench-odbc/` holds
the unpacked `libodbc2`/`libodbcinst2` debs (`apt-get download libodbc2 libodbcinst2; dpkg -x ...` into `.bench-odbc/lib`)
and two `odbcinst.ini` directories (`etc`, `etc-pool`) that register "ODBC Driver 18 for SQL Server" pointing at the
`libmsodbcsql-18` bundled in upstream mssql-python's wheel (`mssql_python_odbc/libs/linux/debian_ubuntu/...`).
`run.py` sets `LD_LIBRARY_PATH` and `ODBCSYSINI` for those drivers; `etc-pool` enables driver-manager pooling and is
used only by the pooled-connect workload (`Pooling=No` everywhere else, so "connect" really connects).

## Methodology

* **Fresh process per pair.** Every (driver, workload) runs in its own `<venv>/bin/python -I benchmarks/child.py`, so there is no
  import, allocator, GIL or connection state shared between measurements, and peak RSS is attributable to that one workload.
  The child is started with `oom_score_adj=1000` so under memory pressure it dies before the database or other agents' processes.
* **Warm.** Connect and dataset preparation happen before timing; one untimed warm-up repetition follows (plan cache, SQL Server
  buffer pool, driver caches), then N measured repetitions (default 5, heavy workloads 3; `--quick` 3 / 2; `--repeats N` overrides).
  The server buffer pool is *not* flushed: reads are warm-cache by design, writes always target a freshly (re)created table.
* **Timed region** = the workload function only: `execute` + fetch (+ DuckDB aggregation where named), or the whole write including
  commit. Row boxing for rows-based bulk loads (Arrow -> tuples) *is* timed, because that is the real cost of that API; reading the
  Parquet file and building the Arrow table is not. Result verification (`COUNT_BIG`) runs after the last repetition.
* **Recorded per pair:** all samples, median, min, p95, rows (or queries) per second, estimated MB/s (units x average `DATALENGTH` bytes/row,
  an approximation of payload size, not TDS bytes), client CPU time (`process_time`, user+sys of the benchmark process; not available for
  `bcp`, which works in the container), peak RSS (`ru_maxrss`), plus run-level git sha, driver versions, SQL Server version, kernel,
  CPU count, available memory and load average (`run.py` warns when load > 4).
* **One heavy run at a time.** `run.py` holds `benchmarks/results/.run.lock`; a second run refuses to start.
* **Read workloads.** `fetchall`/`fetchmany(10k)`/`fetchone` loops, and Arrow where supported (`cursor.arrow()` for upstream,
  `fetchall_arrow()` for the port when present, `read_arrow_batches`, `connectorx.read_sql(return_type="arrow")`). `*.duckdb`
  registers the Arrow stream in DuckDB and runs a group-by aggregate. `*.partitioned_N` is connectorx `partition_on=trip_id, partition_num=N`.
  `*.export` is `bcp queryout -n` in the container. fastmssql buffers entire results, so it skips results above 400k rows.
* **Write workloads.** Targets are recreated before each repetition. `bulk_*` variants: TABLOCK on/off, heap / clustered PK /
  clustered columnstore, batch size 1k/10k/100k/0 (single batch). Single-row INSERTs run with autocommit off and one commit.
  fastmssql has no Decimal binding, so decimals are passed as floats; its `bulk_insert` is excluded (20k rows took 16 s and 900 MB), and so is arrow-odbc's `insert_into_table` (20k rows took 130 s, ~150 rows/s). Both are reported as `n/a` rather than burning the time budget.
  For `bcp`, the native file is produced from the same source rows outside the timed region; the timed part is `bcp in`.
* **Latency/concurrency.** `connect_close` opens and closes fresh connections with pooling off; `pooled_connect` enables pooling
  (driver pool for mssql_python, ODBC pool for pyodbc). Thread workloads use one connection per thread, point lookups on random
  primary keys, and report the wall time for a fixed total (queries/s is in the details table). `mem_*` run a single fetch
  (3M rows in full mode) and are read for **peak RSS**.
* **`--quick`** shrinks the 3M-row reads to 300k, bulk 1M to 250k, 3M to 750k and loop counts by 2-5x. Names stay identical; the header
  records `quick`. Quick numbers are for smoke-testing and ratios, not absolutes.

## Adding a driver

1. Create the venv (see above) and add `benchmarks/drivers/<name>_driver.py` with a class deriving from `drivers.base.Driver`
   (or `DbApiDriver` for anything PEP-249: implement `_connect`, `ph`, and any override). Implement only what it can do; unsupported
   methods raise `Unsupported`. Set `supports = SPECS["<name>"].caps`.
2. Register a `Spec` in `drivers/registry.py` (venv dir, module, class, capability set; `odbc=True` if it needs unixODBC).
3. `python benchmarks/run.py --driver <name> --workload 'read.taxi_100k.*' --quick`.

Capabilities: `rows arrow duckdb partitioned insert executemany multirow bulk_rows bulk_arrow tablock pool threads connect select1 export`.
A workload runs for a driver iff the workload's `requires` is a subset of the driver's caps.

## Adding a workload

Append a `Workload(...)` in `benchmarks/workloads.py` (`name`, `category` read/write/latency, `requires`, `run(driver, ctx)` returning the
number of units processed, optional `setup`/`prepare`/`verify`/`teardown`, `dataset` for MB/s). Workloads call only the `Driver` interface,
so every driver gets it for free as long as it advertises the capability. New synthetic tables go in `datasets.py`.

## Layout

```
run.py            runner (stdlib), writes results/<ts>-<host>.jsonl
child.py          one pair per process
workloads.py      workload matrix           datasets.py   tables + wire-size estimates
payload.py        bulk-write payloads       common.py     paths, connection info, sqlcmd helper
drivers/          adapters + registry.py
report.py         -> RESULTS.md, results/report.html
results/          local JSONL runs and report.html (untracked), datasets.json
experiments/      other work (not part of this suite)
upstream/         Microsoft's own benchmark scripts, kept for reference
bench_taxi.py, compare.py   superseded by run.py (see below)
```

## Compatibility with `bench_taxi.py` / `compare.py`

Both are folded into `run.py` and now only print a pointer. Mapping: `arrow_100k_all_cols` = `read.taxi_100k.arrow`,
`arrow_3m_all_cols` = `read.all_3m.arrow`, `duckdb_over_arrow_3m` = `read.all_3m.duckdb`, `fetchall_100k_all_cols` = `read.taxi_100k.fetchall`,
`fetchone_100k_all_cols` = `read.taxi_100k.fetchone`, `fetchmany_3m_narrow` = `read.narrow_3m.fetchmany`, `point_lookup_x1000` =
`read.point_lookup_x1000`, `executemany_10k` = `write.executemany_10k`, `connect_close_x50` = `latency.connect_close_x50`.
(`aggregate_3m`, a server-side GROUP BY, was dropped: it measured SQL Server, not the driver.)
