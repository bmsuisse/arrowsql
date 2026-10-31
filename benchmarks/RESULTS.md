# Benchmark results

Run `2026-10-30T19:11:20+0000` on a 24-core dev workstation (9077 MB available), commit `747eddd`, quick mode.  
SQL Server: Microsoft SQL Server 2022 (RTM-CU27-GDR) (KB5126934) - 16.0.4305.5 (X64) .  
Load average at start: 20.5 / 51.3 / 198.7.

> **The box was loaded (load average > 4, other agents and a SQL Server shared its 24 cores and ~4 GB free RAM) and this is a `--quick` run with reduced row counts. Numbers are indicative only; compare ratios, not absolutes.**

Cells are the median wall time over the measured repeats, with the speedup versus upstream `mssql_python` in parentheses (higher is faster, `1.0x` = same). `n/a` = driver cannot run the workload, `ERR` = it failed (see the Errors section). Sizes in `--quick` mode: 3M-row datasets run 300k rows, 1M bulk runs 250k, 3M bulk runs 750k.

## Drivers

| driver | version | status |
|---|---|---|
| mssql_python | mssql-python 1.16.0 (PyPI wheel) | ok |
| rust_port | mssql-python 1.16.0 (in-repo port) | ok |
| pyodbc | pyodbc 5.3.0 | ok |
| pyodbc_fast | pyodbc 5.3.0 | ok |
| connectorx | connectorx 0.4.6 | ok |
| arrow_odbc | arrow-odbc 10.6.3 | ok |
| pymssql | pymssql 2.4.6 | ok |
| fastmssql | fastmssql 0.7.7 | ok |
| bcp | BCP - Bulk Copy Program for Microsoft SQL Server. Copyright (C) Microsoft Corpor | ok |

## Headline

| category | workloads | upstream wins | fastest driver (wins) | geomean speedup of rust_port vs upstream |
|---|---|---|---|---|
| Read | 32 | 7 | rust_port (11), mssql_python (7), connectorx (3) | 1.38x over 12 |
| Write | 19 | 9 | mssql_python (9), rust_port (6), pyodbc_fast (3) | 0.91x over 19 |
| Latency, concurrency and memory | 7 | 3 | mssql_python (3), fastmssql (2), pyodbc_fast (1) | 1.42x over 6 |

`bcp` is excluded from the fastest-driver count: it runs inside the server container from local files and is the speed-of-light reference, not a Python client.

## Read

| workload | mssql_python | rust_port | pyodbc | pyodbc_fast | connectorx | arrow_odbc | pymssql | fastmssql | bcp |
|---|---|---|---|---|---|---|---|---|---|
| `read.taxi_100k.fetchall` | 0.379s | **0.337s (1.1x)** | 0.949s (0.4x) | 1.23s (0.3x) | n/a | n/a | 2.09s (0.2x) | 0.987s (0.4x) | n/a |
| `read.taxi_100k.fetchmany` | 0.425s | **0.315s (1.4x)** | 1.06s (0.4x) | 0.889s (0.5x) | n/a | n/a | 1.96s (0.2x) | 0.555s (0.8x) | n/a |
| `read.taxi_100k.fetchone` | 0.486s | **0.305s (1.6x)** | 0.756s (0.6x) | 0.864s (0.6x) | n/a | n/a | 2.00s (0.2x) | 0.750s (0.6x) | n/a |
| `read.taxi_100k.arrow` | **0.145s** | ERR | n/a | n/a | 0.360s (0.4x) | 0.169s (0.9x) | n/a | n/a | n/a |
| `read.taxi_100k.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.408s** |
| `read.narrow_3m.fetchmany` | 0.183s | **0.150s (1.2x)** | 0.419s (0.4x) | 0.428s (0.4x) | n/a | n/a | 1.19s (0.2x) | 0.316s (0.6x) | n/a |
| `read.narrow_3m.arrow` | 92.8ms | ERR | n/a | n/a | 0.338s (0.3x) | **92.3ms (1.0x)** | n/a | n/a | n/a |
| `read.narrow_3m.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.233s** |
| `read.all_3m.fetchmany` | 1.13s | **1.01s (1.1x)** | 3.14s (0.4x) | 4.04s (0.3x) | n/a | n/a | 7.10s (0.2x) | 2.57s (0.4x) | n/a |
| `read.all_3m.arrow` | **0.451s** | ERR | n/a | n/a | 1.38s (0.3x) | 0.782s (0.6x) | n/a | n/a | n/a |
| `read.all_3m.duckdb` | 0.477s | ERR | n/a | n/a | 1.25s (0.4x) | **0.460s (1.0x)** | n/a | n/a | n/a |
| `read.all_3m.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.744s** |
| `read.strings.fetchmany` | 0.177s | **93.9ms (1.9x)** | 0.119s (1.5x) | 0.139s (1.3x) | n/a | n/a | 0.508s (0.3x) | 0.693s (0.3x) | n/a |
| `read.strings.arrow` | **0.116s** | ERR | n/a | n/a | 0.830s (0.1x) | 0.477s (0.2x) | n/a | n/a | n/a |
| `read.strings.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.452s** |
| `read.decimals.fetchmany` | 0.253s | **0.159s (1.6x)** | 0.567s (0.4x) | 0.876s (0.3x) | n/a | n/a | 1.11s (0.2x) | 0.270s (0.9x) | n/a |
| `read.decimals.arrow` | **65.4ms** | ERR | n/a | n/a | 0.120s (0.5x) | 76.3ms (0.9x) | n/a | n/a | n/a |
| `read.decimals.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.390s** |
| `read.datetimes.fetchmany` | 53.5ms | **40.0ms (1.3x)** | 81.8ms (0.7x) | 84.5ms (0.6x) | n/a | n/a | 0.338s (0.2x) | 0.145s (0.4x) | n/a |
| `read.datetimes.arrow` | **32.3ms** | ERR | n/a | n/a | 0.124s (0.3x) | 44.8ms (0.7x) | n/a | n/a | n/a |
| `read.datetimes.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.166s** |
| `read.lob.fetchmany` | 50.2ms | **32.8ms (1.5x)** | 53.4ms (0.9x) | 48.6ms (1.0x) | n/a | n/a | 41.1ms (1.2x) | 0.258s (0.2x) | n/a |
| `read.lob.arrow` | **50.0ms** | ERR | n/a | n/a | 0.356s (0.1x) | 0.435s (0.1x) | n/a | n/a | n/a |
| `read.lob.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.182s** |
| `read.wide.fetchmany` | 0.267s | **0.152s (1.8x)** | 0.443s (0.6x) | 0.433s (0.6x) | n/a | n/a | 1.01s (0.3x) | 0.408s (0.7x) | n/a |
| `read.wide.arrow` | **0.253s** | ERR | n/a | n/a | 0.322s (0.8x) | 0.417s (0.6x) | n/a | n/a | n/a |
| `read.wide.export` | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | **0.249s** |
| `read.all_3m.partitioned_2` | n/a | n/a | n/a | n/a | **0.644s** | n/a | n/a | n/a | n/a |
| `read.all_3m.partitioned_4` | n/a | n/a | n/a | n/a | **0.409s** | n/a | n/a | n/a | n/a |
| `read.all_3m.partitioned_8` | n/a | n/a | n/a | n/a | **0.293s** | n/a | n/a | n/a | n/a |
| `read.point_lookup_x1000` | 61.5ms | 58.4ms (1.1x) | **28.4ms (2.2x)** | 29.5ms (2.1x) | n/a | n/a | 31.3ms (2.0x) | 84.8ms (0.7x) | n/a |
| `read.small_100_rows_x200` | 30.8ms | **24.9ms (1.2x)** | 43.9ms (0.7x) | 45.5ms (0.7x) | n/a | n/a | 0.101s (0.3x) | 50.5ms (0.6x) | n/a |

<details><summary>Read: throughput, CPU time and peak RSS</summary>

| workload | driver | units/s | MB/s (est. wire) | client CPU s | peak RSS MB |
|---|---|---|---|---|---|
| `read.taxi_100k.fetchall` | mssql_python | 263922 rows/s | 30 | 0.38 | 194 |
| `read.taxi_100k.fetchall` | rust_port | 296665 rows/s | 34 | 0.33 | 220 |
| `read.taxi_100k.fetchall` | pyodbc | 105370 rows/s | 12 | 0.92 | 179 |
| `read.taxi_100k.fetchall` | pyodbc_fast | 81530 rows/s | 9 | 1.16 | 180 |
| `read.taxi_100k.fetchall` | pymssql | 47908 rows/s | 6 | 2.08 | 172 |
| `read.taxi_100k.fetchall` | fastmssql | 101272 rows/s | 12 | 0.97 | 340 |
| `read.taxi_100k.fetchmany` | mssql_python | 235339 rows/s | 27 | 0.42 | 76 |
| `read.taxi_100k.fetchmany` | rust_port | 317913 rows/s | 37 | 0.31 | 79 |
| `read.taxi_100k.fetchmany` | pyodbc | 94067 rows/s | 11 | 1.02 | 62 |
| `read.taxi_100k.fetchmany` | pyodbc_fast | 112442 rows/s | 13 | 0.89 | 62 |
| `read.taxi_100k.fetchmany` | pymssql | 51042 rows/s | 6 | 1.91 | 56 |
| `read.taxi_100k.fetchmany` | fastmssql | 180259 rows/s | 21 | 0.55 | 334 |
| `read.taxi_100k.fetchone` | mssql_python | 205922 rows/s | 24 | 0.48 | 36 |
| `read.taxi_100k.fetchone` | rust_port | 327363 rows/s | 38 | 0.31 | 37 |
| `read.taxi_100k.fetchone` | pyodbc | 132331 rows/s | 15 | 0.75 | 33 |
| `read.taxi_100k.fetchone` | pyodbc_fast | 115736 rows/s | 13 | 0.85 | 33 |
| `read.taxi_100k.fetchone` | pymssql | 50079 rows/s | 6 | 1.93 | 27 |
| `read.taxi_100k.fetchone` | fastmssql | 133278 rows/s | 15 | 0.75 | 337 |
| `read.taxi_100k.arrow` | mssql_python | 691799 rows/s | 80 | 0.14 | 105 |
| `read.taxi_100k.arrow` | connectorx | 277869 rows/s | 32 | 0.31 | 108 |
| `read.taxi_100k.arrow` | arrow_odbc | 592000 rows/s | 68 | 0.19 | 118 |
| `read.taxi_100k.export` | bcp | 245372 rows/s | 28 |  | 20 |
| `read.narrow_3m.fetchmany` | mssql_python | 1641343 rows/s | 48 | 0.18 | 46 |
| `read.narrow_3m.fetchmany` | rust_port | 2002202 rows/s | 58 | 0.15 | 62 |
| `read.narrow_3m.fetchmany` | pyodbc | 715387 rows/s | 21 | 0.42 | 40 |
| `read.narrow_3m.fetchmany` | pyodbc_fast | 701513 rows/s | 20 | 0.43 | 40 |
| `read.narrow_3m.fetchmany` | pymssql | 251341 rows/s | 7 | 1.15 | 32 |
| `read.narrow_3m.fetchmany` | fastmssql | 949129 rows/s | 28 | 0.32 | 224 |
| `read.narrow_3m.arrow` | mssql_python | 3232601 rows/s | 94 | 0.09 | 85 |
| `read.narrow_3m.arrow` | connectorx | 886450 rows/s | 26 | 0.25 | 98 |
| `read.narrow_3m.arrow` | arrow_odbc | 3249571 rows/s | 94 | 0.10 | 79 |
| `read.narrow_3m.export` | bcp | 1287681 rows/s | 37 |  | 19 |
| `read.all_3m.fetchmany` | mssql_python | 264436 rows/s | 30 | 1.13 | 76 |
| `read.all_3m.fetchmany` | rust_port | 297536 rows/s | 34 | 0.99 | 93 |
| `read.all_3m.fetchmany` | pyodbc | 95482 rows/s | 11 | 3.00 | 62 |
| `read.all_3m.fetchmany` | pyodbc_fast | 74236 rows/s | 8 | 3.54 | 62 |
| `read.all_3m.fetchmany` | pymssql | 42252 rows/s | 5 | 6.76 | 55 |
| `read.all_3m.fetchmany` | fastmssql | 116860 rows/s | 13 | 2.50 | 854 |
| `read.all_3m.arrow` | mssql_python | 665363 rows/s | 76 | 0.44 | 154 |
| `read.all_3m.arrow` | connectorx | 217723 rows/s | 25 | 1.01 | 151 |
| `read.all_3m.arrow` | arrow_odbc | 383547 rows/s | 44 | 0.65 | 161 |
| `read.all_3m.duckdb` | mssql_python | 499 rows/s | 0 | 0.50 | 225 |
| `read.all_3m.duckdb` | connectorx | 191 rows/s | 0 | 1.10 | 207 |
| `read.all_3m.duckdb` | arrow_odbc | 518 rows/s | 0 | 0.53 | 213 |
| `read.all_3m.export` | bcp | 403156 rows/s | 46 |  | 20 |
| `read.strings.fetchmany` | mssql_python | 112920 rows/s | 236 | 0.16 | 109 |
| `read.strings.fetchmany` | rust_port | 212926 rows/s | 446 | 0.09 | 112 |
| `read.strings.fetchmany` | pyodbc | 167689 rows/s | 351 | 0.11 | 64 |
| `read.strings.fetchmany` | pyodbc_fast | 143655 rows/s | 301 | 0.12 | 65 |
| `read.strings.fetchmany` | pymssql | 39399 rows/s | 83 | 0.37 | 58 |
| `read.strings.fetchmany` | fastmssql | 28860 rows/s | 60 | 0.69 | 99 |
| `read.strings.arrow` | mssql_python | 172992 rows/s | 362 | 0.09 | 104 |
| `read.strings.arrow` | connectorx | 24109 rows/s | 50 | 0.81 | 107 |
| `read.strings.arrow` | arrow_odbc | 41926 rows/s | 88 | 0.57 | 1072 |
| `read.strings.export` | bcp | 44243 rows/s | 93 |  | 20 |
| `read.decimals.fetchmany` | mssql_python | 197801 rows/s | 13 | 0.25 | 79 |
| `read.decimals.fetchmany` | rust_port | 314572 rows/s | 20 | 0.16 | 78 |
| `read.decimals.fetchmany` | pyodbc | 88136 rows/s | 6 | 0.54 | 66 |
| `read.decimals.fetchmany` | pyodbc_fast | 57073 rows/s | 4 | 0.71 | 66 |
| `read.decimals.fetchmany` | pymssql | 44942 rows/s | 3 | 1.11 | 59 |
| `read.decimals.fetchmany` | fastmssql | 185328 rows/s | 12 | 0.26 | 161 |
| `read.decimals.arrow` | mssql_python | 765002 rows/s | 49 | 0.06 | 84 |
| `read.decimals.arrow` | connectorx | 416986 rows/s | 27 | 0.10 | 96 |
| `read.decimals.arrow` | arrow_odbc | 655491 rows/s | 42 | 0.08 | 96 |
| `read.decimals.export` | bcp | 128230 rows/s | 8 |  | 20 |
| `read.datetimes.fetchmany` | mssql_python | 934661 rows/s | 55 | 0.05 | 50 |
| `read.datetimes.fetchmany` | rust_port | 1249404 rows/s | 74 | 0.04 | 66 |
| `read.datetimes.fetchmany` | pyodbc | 610953 rows/s | 36 | 0.08 | 43 |
| `read.datetimes.fetchmany` | pyodbc_fast | 591388 rows/s | 35 | 0.08 | 43 |
| `read.datetimes.fetchmany` | pymssql | 147806 rows/s | 9 | 0.34 | 36 |
| `read.datetimes.fetchmany` | fastmssql | 345858 rows/s | 20 | 0.14 | 99 |
| `read.datetimes.arrow` | mssql_python | 1547666 rows/s | 91 | 0.03 | 75 |
| `read.datetimes.arrow` | connectorx | 402082 rows/s | 24 | 0.09 | 89 |
| `read.datetimes.arrow` | arrow_odbc | 1114844 rows/s | 66 | 0.05 | 84 |
| `read.datetimes.export` | bcp | 301329 rows/s | 18 |  | 19 |
| `read.lob.fetchmany` | mssql_python | 3984 rows/s | 402 | 0.03 | 44 |
| `read.lob.fetchmany` | rust_port | 6101 rows/s | 615 | 0.03 | 99 |
| `read.lob.fetchmany` | pyodbc | 3746 rows/s | 378 | 0.03 | 43 |
| `read.lob.fetchmany` | pyodbc_fast | 4114 rows/s | 415 | 0.03 | 42 |
| `read.lob.fetchmany` | pymssql | 4868 rows/s | 491 | 0.04 | 36 |
| `read.lob.fetchmany` | fastmssql | 777 rows/s | 78 | 0.26 | 78 |
| `read.lob.arrow` | mssql_python | 3998 rows/s | 403 | 0.03 | 89 |
| `read.lob.arrow` | connectorx | 563 rows/s | 57 | 0.32 | 98 |
| `read.lob.arrow` | arrow_odbc | 460 rows/s | 46 | 0.46 | 1086 |
| `read.lob.export` | bcp | 1099 rows/s | 111 |  | 19 |
| `read.wide.fetchmany` | mssql_python | 18719 rows/s | 42 | 0.27 | 226 |
| `read.wide.fetchmany` | rust_port | 32875 rows/s | 74 | 0.15 | 193 |
| `read.wide.fetchmany` | pyodbc | 11276 rows/s | 25 | 0.43 | 120 |
| `read.wide.fetchmany` | pyodbc_fast | 11535 rows/s | 26 | 0.43 | 121 |
| `read.wide.fetchmany` | pymssql | 4974 rows/s | 11 | 0.99 | 114 |
| `read.wide.fetchmany` | fastmssql | 12248 rows/s | 27 | 0.41 | 221 |
| `read.wide.arrow` | mssql_python | 19748 rows/s | 44 | 0.25 | 370 |
| `read.wide.arrow` | connectorx | 15525 rows/s | 35 | 0.28 | 108 |
| `read.wide.arrow` | arrow_odbc | 12002 rows/s | 27 | 0.56 | 1001 |
| `read.wide.export` | bcp | 20069 rows/s | 45 |  | 19 |
| `read.all_3m.partitioned_2` | connectorx | 465894 rows/s | 53 | 1.02 | 155 |
| `read.all_3m.partitioned_4` | connectorx | 732839 rows/s | 83 | 1.14 | 157 |
| `read.all_3m.partitioned_8` | connectorx | 1023301 rows/s | 116 | 1.73 | 162 |
| `read.point_lookup_x1000` | mssql_python | 3255 queries/s | 0 | 0.04 | 36 |
| `read.point_lookup_x1000` | rust_port | 3427 queries/s | 0 | 0.03 | 45 |
| `read.point_lookup_x1000` | pyodbc | 7049 queries/s | 1 | 0.01 | 32 |
| `read.point_lookup_x1000` | pyodbc_fast | 6772 queries/s | 1 | 0.01 | 32 |
| `read.point_lookup_x1000` | pymssql | 6390 queries/s | 1 | 0.01 | 27 |
| `read.point_lookup_x1000` | fastmssql | 2357 queries/s | 0 | 0.03 | 41 |
| `read.small_100_rows_x200` | mssql_python | 162298 rows/s | 18 | 0.02 | 37 |
| `read.small_100_rows_x200` | rust_port | 200513 rows/s | 23 | 0.02 | 44 |
| `read.small_100_rows_x200` | pyodbc | 113831 rows/s | 13 | 0.04 | 33 |
| `read.small_100_rows_x200` | pyodbc_fast | 109860 rows/s | 12 | 0.04 | 33 |
| `read.small_100_rows_x200` | pymssql | 49398 rows/s | 6 | 0.09 | 27 |
| `read.small_100_rows_x200` | fastmssql | 98928 rows/s | 11 | 0.04 | 39 |

</details>

## Write

| workload | mssql_python | rust_port | pyodbc | pyodbc_fast | pymssql | fastmssql | bcp |
|---|---|---|---|---|---|---|---|
| `write.single_insert_x1000` | 0.142s | 0.159s (0.9x) | 0.126s (1.1x) | **0.119s (1.2x)** | 0.189s (0.8x) | 0.187s (0.8x) | n/a |
| `write.executemany_10k` | 0.128s | 0.130s (1.0x) | 1.16s (0.1x) | **86.1ms (1.5x)** | 1.75s (<0.1x) | n/a | n/a |
| `write.executemany_100k` | 0.270s | 0.291s (0.9x) | 2.77s (<0.1x) | **0.164s (1.7x)** | 3.78s (<0.1x) | n/a | n/a |
| `write.multirow_values_10k` | 78.0ms | 0.181s (0.4x) | **62.5ms (1.2x)** | 65.2ms (1.2x) | 0.121s (0.6x) | 0.133s (0.6x) | n/a |
| `write.bulk_rows_100k.heap.tablock` | 1.64s | 2.04s (0.8x) | n/a | n/a | 3.12s (0.5x) | n/a | **0.382s (4.3x)** |
| `write.bulk_rows_100k.heap.notablock` | 1.65s | 1.58s (1.0x) | n/a | n/a | 3.06s (0.5x) | n/a | **0.446s (3.7x)** |
| `write.bulk_rows_100k.pk.tablock` | 1.67s | 1.94s (0.9x) | n/a | n/a | 3.14s (0.5x) | n/a | **0.403s (4.2x)** |
| `write.bulk_rows_100k.cci.tablock` | 2.52s | 2.44s (1.0x) | n/a | n/a | 3.99s (0.6x) | n/a | **0.987s (2.5x)** |
| `write.bulk_rows_100k.heap.tablock.b1k` | 4.03s | 3.11s (1.3x) | n/a | n/a | 4.86s (0.8x) | n/a | **1.33s (3.0x)** |
| `write.bulk_rows_100k.heap.tablock.b10k` | 1.63s | 1.72s (0.9x) | n/a | n/a | 3.18s (0.5x) | n/a | **0.474s (3.4x)** |
| `write.bulk_rows_100k.heap.tablock.b0` | 1.55s | 1.60s (1.0x) | n/a | n/a | 3.62s (0.4x) | n/a | **0.393s (3.9x)** |
| `write.bulk_rows_1m.heap.tablock` | 4.03s | 3.78s (1.1x) | n/a | n/a | 7.78s (0.5x) | n/a | **0.651s (6.2x)** |
| `write.bulk_arrow_1m.heap.tablock` | 0.992s | 1.09s (0.9x) | n/a | n/a | n/a | n/a | **0.741s (1.3x)** |
| `write.bulk_arrow_1m.heap.notablock` | 1.09s | 1.30s (0.8x) | n/a | n/a | n/a | n/a | **1.04s (1.0x)** |
| `write.bulk_arrow_1m.pk.tablock` | 1.69s | 2.09s (0.8x) | n/a | n/a | n/a | n/a | **1.20s (1.4x)** |
| `write.bulk_arrow_1m.cci.tablock` | 2.27s | **2.11s (1.1x)** | n/a | n/a | n/a | n/a | 2.16s (1.1x) |
| `write.bulk_arrow_1m.heap.tablock.b10k` | 1.41s | 1.74s (0.8x) | n/a | n/a | n/a | n/a | **1.05s (1.3x)** |
| `write.bulk_arrow_1m.heap.tablock.b0` | 0.937s | 0.921s (1.0x) | n/a | n/a | n/a | n/a | **0.639s (1.5x)** |
| `write.bulk_arrow_3m.heap.tablock` | 3.12s | 3.19s (1.0x) | n/a | n/a | n/a | n/a | **1.74s (1.8x)** |

<details><summary>Write: throughput, CPU time and peak RSS</summary>

| workload | driver | units/s | MB/s (est. wire) | client CPU s | peak RSS MB |
|---|---|---|---|---|---|
| `write.single_insert_x1000` | mssql_python | 7031 rows/s |  | 0.04 | 36 |
| `write.single_insert_x1000` | rust_port | 6273 rows/s |  | 0.06 | 44 |
| `write.single_insert_x1000` | pyodbc | 7942 rows/s |  | 0.02 | 33 |
| `write.single_insert_x1000` | pyodbc_fast | 8398 rows/s |  | 0.02 | 34 |
| `write.single_insert_x1000` | pymssql | 5293 rows/s |  | 0.04 | 27 |
| `write.single_insert_x1000` | fastmssql | 5349 rows/s |  | 0.09 | 43 |
| `write.executemany_10k` | mssql_python | 78030 rows/s |  | 0.07 | 45 |
| `write.executemany_10k` | rust_port | 76700 rows/s |  | 0.08 | 65 |
| `write.executemany_10k` | pyodbc | 8627 rows/s |  | 0.22 | 40 |
| `write.executemany_10k` | pyodbc_fast | 116129 rows/s |  | 0.02 | 42 |
| `write.executemany_10k` | pymssql | 5725 rows/s |  | 0.40 | 34 |
| `write.executemany_100k` | mssql_python | 73960 rows/s |  | 0.14 | 53 |
| `write.executemany_100k` | rust_port | 68815 rows/s |  | 0.18 | 85 |
| `write.executemany_100k` | pyodbc | 7224 rows/s |  | 0.53 | 48 |
| `write.executemany_100k` | pyodbc_fast | 122251 rows/s |  | 0.05 | 48 |
| `write.executemany_100k` | pymssql | 5296 rows/s |  | 0.82 | 42 |
| `write.multirow_values_10k` | mssql_python | 25654 rows/s |  | 0.01 | 39 |
| `write.multirow_values_10k` | rust_port | 11077 rows/s |  | 0.01 | 53 |
| `write.multirow_values_10k` | pyodbc | 31980 rows/s |  | 0.01 | 35 |
| `write.multirow_values_10k` | pyodbc_fast | 30685 rows/s |  | 0.01 | 34 |
| `write.multirow_values_10k` | pymssql | 16466 rows/s |  | 0.02 | 28 |
| `write.multirow_values_10k` | fastmssql | 15064 rows/s |  | 0.01 | 78 |
| `write.bulk_rows_100k.heap.tablock` | mssql_python | 61135 rows/s | 7 | 1.58 | 1090 |
| `write.bulk_rows_100k.heap.tablock` | rust_port | 49015 rows/s | 6 | 1.94 | 921 |
| `write.bulk_rows_100k.heap.tablock` | pymssql | 32042 rows/s | 4 | 3.08 | 955 |
| `write.bulk_rows_100k.heap.tablock` | bcp | 262059 rows/s | 30 |  | 20 |
| `write.bulk_rows_100k.heap.notablock` | mssql_python | 60722 rows/s | 7 | 1.63 | 912 |
| `write.bulk_rows_100k.heap.notablock` | rust_port | 63140 rows/s | 7 | 1.55 | 939 |
| `write.bulk_rows_100k.heap.notablock` | pymssql | 32696 rows/s | 4 | 3.05 | 1159 |
| `write.bulk_rows_100k.heap.notablock` | bcp | 224057 rows/s | 25 |  | 20 |
| `write.bulk_rows_100k.pk.tablock` | mssql_python | 59728 rows/s | 7 | 1.57 | 1092 |
| `write.bulk_rows_100k.pk.tablock` | rust_port | 51579 rows/s | 6 | 1.76 | 1098 |
| `write.bulk_rows_100k.pk.tablock` | pymssql | 31814 rows/s | 4 | 3.05 | 944 |
| `write.bulk_rows_100k.pk.tablock` | bcp | 248444 rows/s | 28 |  | 19 |
| `write.bulk_rows_100k.cci.tablock` | mssql_python | 39722 rows/s | 5 | 1.83 | 1007 |
| `write.bulk_rows_100k.cci.tablock` | rust_port | 41064 rows/s | 5 | 1.90 | 1055 |
| `write.bulk_rows_100k.cci.tablock` | pymssql | 25073 rows/s | 3 | 3.37 | 942 |
| `write.bulk_rows_100k.cci.tablock` | bcp | 101286 rows/s | 12 |  | 19 |
| `write.bulk_rows_100k.heap.tablock.b1k` | mssql_python | 24827 rows/s | 3 | 2.06 | 1074 |
| `write.bulk_rows_100k.heap.tablock.b1k` | rust_port | 32114 rows/s | 4 | 1.72 | 936 |
| `write.bulk_rows_100k.heap.tablock.b1k` | pymssql | 20567 rows/s | 2 | 3.25 | 854 |
| `write.bulk_rows_100k.heap.tablock.b1k` | bcp | 75117 rows/s | 9 |  | 20 |
| `write.bulk_rows_100k.heap.tablock.b10k` | mssql_python | 61311 rows/s | 7 | 1.51 | 926 |
| `write.bulk_rows_100k.heap.tablock.b10k` | rust_port | 58202 rows/s | 7 | 1.54 | 958 |
| `write.bulk_rows_100k.heap.tablock.b10k` | pymssql | 31443 rows/s | 4 | 3.01 | 923 |
| `write.bulk_rows_100k.heap.tablock.b10k` | bcp | 210915 rows/s | 24 |  | 19 |
| `write.bulk_rows_100k.heap.tablock.b0` | mssql_python | 64385 rows/s | 7 | 1.53 | 1122 |
| `write.bulk_rows_100k.heap.tablock.b0` | rust_port | 62677 rows/s | 7 | 1.58 | 1113 |
| `write.bulk_rows_100k.heap.tablock.b0` | pymssql | 27621 rows/s | 3 | 3.56 | 1002 |
| `write.bulk_rows_100k.heap.tablock.b0` | bcp | 254207 rows/s | 29 |  | 20 |
| `write.bulk_rows_1m.heap.tablock` | mssql_python | 62047 rows/s | 7 | 3.94 | 898 |
| `write.bulk_rows_1m.heap.tablock` | rust_port | 66067 rows/s | 8 | 3.74 | 999 |
| `write.bulk_rows_1m.heap.tablock` | pymssql | 32136 rows/s | 4 | 7.68 | 937 |
| `write.bulk_rows_1m.heap.tablock` | bcp | 384185 rows/s | 44 |  | 20 |
| `write.bulk_arrow_1m.heap.tablock` | mssql_python | 252094 rows/s | 29 | 0.95 | 1050 |
| `write.bulk_arrow_1m.heap.tablock` | rust_port | 228878 rows/s | 26 | 1.02 | 1055 |
| `write.bulk_arrow_1m.heap.tablock` | bcp | 337531 rows/s | 38 |  | 20 |
| `write.bulk_arrow_1m.heap.notablock` | mssql_python | 230064 rows/s | 26 | 1.03 | 982 |
| `write.bulk_arrow_1m.heap.notablock` | rust_port | 192245 rows/s | 22 | 1.14 | 1039 |
| `write.bulk_arrow_1m.heap.notablock` | bcp | 239355 rows/s | 27 |  | 19 |
| `write.bulk_arrow_1m.pk.tablock` | mssql_python | 148322 rows/s | 17 | 1.01 | 942 |
| `write.bulk_arrow_1m.pk.tablock` | rust_port | 119657 rows/s | 14 | 1.02 | 992 |
| `write.bulk_arrow_1m.pk.tablock` | bcp | 208213 rows/s | 24 |  | 19 |
| `write.bulk_arrow_1m.cci.tablock` | mssql_python | 109941 rows/s | 12 | 0.97 | 1026 |
| `write.bulk_arrow_1m.cci.tablock` | rust_port | 118282 rows/s | 13 | 0.94 | 1076 |
| `write.bulk_arrow_1m.cci.tablock` | bcp | 115866 rows/s | 13 |  | 19 |
| `write.bulk_arrow_1m.heap.tablock.b10k` | mssql_python | 177236 rows/s | 20 | 1.05 | 982 |
| `write.bulk_arrow_1m.heap.tablock.b10k` | rust_port | 143353 rows/s | 16 | 1.06 | 1026 |
| `write.bulk_arrow_1m.heap.tablock.b10k` | bcp | 237929 rows/s | 27 |  | 20 |
| `write.bulk_arrow_1m.heap.tablock.b0` | mssql_python | 266908 rows/s | 30 | 0.92 | 1077 |
| `write.bulk_arrow_1m.heap.tablock.b0` | rust_port | 271298 rows/s | 31 | 0.91 | 1126 |
| `write.bulk_arrow_1m.heap.tablock.b0` | bcp | 391045 rows/s | 44 |  | 19 |
| `write.bulk_arrow_3m.heap.tablock` | mssql_python | 240277 rows/s | 27 | 3.01 | 1058 |
| `write.bulk_arrow_3m.heap.tablock` | rust_port | 234846 rows/s | 27 | 3.03 | 1134 |
| `write.bulk_arrow_3m.heap.tablock` | bcp | 432122 rows/s | 49 |  | 19 |

</details>

## Latency, concurrency and memory

| workload | mssql_python | rust_port | pyodbc | pyodbc_fast | connectorx | arrow_odbc | pymssql | fastmssql |
|---|---|---|---|---|---|---|---|---|
| `latency.connect_close_x50` | **36.7ms** | 37.6ms (1.0x) | 37.7ms (1.0x) | 38.5ms (1.0x) | n/a | 38.9ms (0.9x) | 42.4ms (0.9x) | 62.8ms (0.6x) |
| `latency.pooled_connect_x50` | 3.9ms | 493us (8.0x) | 48us (81.3x) | **32us (123.8x)** | n/a | n/a | n/a | n/a |
| `latency.select1_x5000` | **0.155s** | 0.209s (0.7x) | 0.188s (0.8x) | 0.194s (0.8x) | n/a | n/a | 0.223s (0.7x) | 0.449s (0.3x) |
| `latency.threads8_point_qps` | 0.215s | 0.132s (1.6x) | 0.154s (1.4x) | 0.158s (1.4x) | n/a | n/a | 0.110s (2.0x) | **0.110s (2.0x)** |
| `latency.threads16_point_qps` | 0.492s | 0.509s (1.0x) | 0.445s (1.1x) | 0.390s (1.3x) | n/a | n/a | 0.220s (2.2x) | **0.133s (3.7x)** |
| `latency.mem_fetchall_narrow_3m` | **0.258s** | 0.289s (0.9x) | 0.480s (0.5x) | 0.520s (0.5x) | n/a | n/a | 1.25s (0.2x) | 0.427s (0.6x) |
| `latency.mem_arrow_all_3m` | 0.476s | ERR | n/a | n/a | 0.982s (0.5x) | **0.385s (1.2x)** | n/a | n/a |

<details><summary>Latency, concurrency and memory: throughput, CPU time and peak RSS</summary>

| workload | driver | units/s | MB/s (est. wire) | client CPU s | peak RSS MB |
|---|---|---|---|---|---|
| `latency.connect_close_x50` | mssql_python | 273 connects/s |  | 0.01 | 36 |
| `latency.connect_close_x50` | rust_port | 266 connects/s |  | 0.01 | 42 |
| `latency.connect_close_x50` | pyodbc | 265 connects/s |  | 0.01 | 33 |
| `latency.connect_close_x50` | pyodbc_fast | 260 connects/s |  | 0.01 | 33 |
| `latency.connect_close_x50` | arrow_odbc | 257 connects/s |  | 0.01 | 60 |
| `latency.connect_close_x50` | pymssql | 236 connects/s |  | 0.01 | 26 |
| `latency.connect_close_x50` | fastmssql | 159 connects/s |  | 0.03 | 45 |
| `latency.pooled_connect_x50` | mssql_python | 2546 connects/s |  | 0.00 | 36 |
| `latency.pooled_connect_x50` | rust_port | 20282 connects/s |  | 0.00 | 42 |
| `latency.pooled_connect_x50` | pyodbc | 206958 connects/s |  | 0.00 | 33 |
| `latency.pooled_connect_x50` | pyodbc_fast | 315159 connects/s |  | 0.00 | 33 |
| `latency.select1_x5000` | mssql_python | 6440 queries/s |  | 0.04 | 36 |
| `latency.select1_x5000` | rust_port | 4776 queries/s |  | 0.07 | 43 |
| `latency.select1_x5000` | pyodbc | 5332 queries/s |  | 0.03 | 33 |
| `latency.select1_x5000` | pyodbc_fast | 5153 queries/s |  | 0.03 | 33 |
| `latency.select1_x5000` | pymssql | 4494 queries/s |  | 0.05 | 27 |
| `latency.select1_x5000` | fastmssql | 2230 queries/s |  | 0.14 | 29 |
| `latency.threads8_point_qps` | mssql_python | 3722 queries/s | 0 | 0.25 | 45 |
| `latency.threads8_point_qps` | rust_port | 6055 queries/s | 1 | 0.19 | 60 |
| `latency.threads8_point_qps` | pyodbc | 5196 queries/s | 1 | 0.50 | 35 |
| `latency.threads8_point_qps` | pyodbc_fast | 5066 queries/s | 1 | 0.50 | 35 |
| `latency.threads8_point_qps` | pymssql | 7294 queries/s | 1 | 0.22 | 27 |
| `latency.threads8_point_qps` | fastmssql | 7305 queries/s | 1 | 0.16 | 49 |
| `latency.threads16_point_qps` | mssql_python | 3254 queries/s | 0 | 0.58 | 53 |
| `latency.threads16_point_qps` | rust_port | 3142 queries/s | 0 | 0.59 | 43 |
| `latency.threads16_point_qps` | pyodbc | 3595 queries/s | 0 | 0.88 | 38 |
| `latency.threads16_point_qps` | pyodbc_fast | 4100 queries/s | 0 | 1.23 | 38 |
| `latency.threads16_point_qps` | pymssql | 7280 queries/s | 1 | 0.47 | 28 |
| `latency.threads16_point_qps` | fastmssql | 12027 queries/s | 1 | 0.28 | 33 |
| `latency.mem_fetchall_narrow_3m` | mssql_python | 1161521 rows/s | 34 | 0.26 | 164 |
| `latency.mem_fetchall_narrow_3m` | rust_port | 1039819 rows/s | 30 | 0.28 | 167 |
| `latency.mem_fetchall_narrow_3m` | pyodbc | 625421 rows/s | 18 | 0.48 | 127 |
| `latency.mem_fetchall_narrow_3m` | pyodbc_fast | 577302 rows/s | 17 | 0.48 | 128 |
| `latency.mem_fetchall_narrow_3m` | pymssql | 240282 rows/s | 7 | 1.24 | 120 |
| `latency.mem_fetchall_narrow_3m` | fastmssql | 702279 rows/s | 20 | 0.42 | 218 |
| `latency.mem_arrow_all_3m` | mssql_python | 630500 rows/s | 72 | 1.40 | 156 |
| `latency.mem_arrow_all_3m` | connectorx | 305370 rows/s | 35 | 0.84 | 152 |
| `latency.mem_arrow_all_3m` | arrow_odbc | 779957 rows/s | 89 | 0.44 | 160 |

</details>

## Errors

| workload | driver | reason |
|---|---|---|
| `latency.mem_arrow_all_3m` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.all_3m.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.all_3m.duckdb` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.datetimes.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.decimals.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.lob.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.narrow_3m.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.strings.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.taxi_100k.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |
| `read.wide.arrow` | rust_port | NotSupportedError: Driver Error: Driver does not support this function; DDBC Error: [Microsoft]DDBCSQLFetchArrowBatch not implemented in Rust port |

## Notes

- Methodology and caveats: see `benchmarks/README.md`. Raw samples: the JSONL next to this file in `benchmarks/results/`.
- MB/s uses the average `DATALENGTH` bytes per row of the dataset (computed once, `benchmarks/results/datasets.json`), i.e. an estimate of payload size, not TDS bytes on the wire.
- `bcp` times are for `bcp ... queryout/in -n` inside the container; for writes the file is pre-staged outside the timed region.
- Compatibility: `bench_taxi.py` / `compare.py` were folded into `run.py`; the old workloads map to `read.taxi_100k.*`, `read.narrow_3m.*`, `read.all_3m.*`, `read.point_lookup_x1000`, `write.executemany_10k`, `latency.connect_close_x50`.

