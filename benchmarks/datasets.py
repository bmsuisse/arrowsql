"""Read datasets: the taxi tables plus synthetic tables created once in `nyctaxi`.

    python benchmarks/datasets.py [--force]     # create missing tables, (re)compute wire-byte estimates

Setup goes through sqlcmd in the container, so it is independent of every driver
under test. `datasets.json` stores average DATALENGTH bytes per row per dataset
(the MB/s denominator); it is computed once and committed with the results.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass

from common import DATASETS_JSON, RESULTS, sqlcmd


@dataclass(frozen=True)
class Dataset:
    name: str
    table: str
    full_rows: int
    quick_rows: int
    select: str = "*"
    id_col: str = "id"

    def rows(self, quick: bool) -> int:
        return min(self.quick_rows, self.full_rows) if quick else self.full_rows

    def sql(self, quick: bool) -> str:
        n = self.rows(quick)
        where = f" WHERE {self.id_col} <= {n}" if n < self.full_rows else ""
        return f"SELECT {self.select} FROM {self.table}{where}"


TAXI_ROWS = 2_964_624
NARROW = "trip_id, tpep_pickup_datetime, trip_distance, total_amount"

DATASETS: dict[str, Dataset] = {d.name: d for d in [
    Dataset("taxi_100k", "dbo.yellow_trips_100k", 100_000, 100_000, id_col="trip_id"),
    Dataset("taxi_narrow", "dbo.yellow_trips", TAXI_ROWS, 300_000, NARROW, "trip_id"),
    Dataset("taxi_all", "dbo.yellow_trips", TAXI_ROWS, 300_000, "*", "trip_id"),
    Dataset("strings", "dbo.bench_strings", 200_000, 20_000),
    Dataset("decimals", "dbo.bench_decimals", 500_000, 50_000),
    Dataset("datetimes", "dbo.bench_datetimes", 500_000, 50_000),
    Dataset("lob", "dbo.bench_lob", 2_000, 200),
    Dataset("wide", "dbo.bench_wide", 20_000, 5_000),
]}

_WIDE_TYPES = [
    ("int", "CAST(trip_id + {i} AS int)"),
    ("bigint", "CAST(trip_id AS bigint) * {i}"),
    ("float", "CAST(trip_id AS float) / ({i} + 1.0)"),
    ("decimal(10,2)", "CAST((trip_id % 100000) / 7.0 AS decimal(10,2))"),
    ("nvarchar(30)", "CAST(trip_id * {i} AS nvarchar(30))"),
    ("datetime2(3)", "DATEADD(second, trip_id % 100000 + {i}, '2024-01-01')"),
]


def _rand_str() -> str:
    return ("LEFT(REPLICATE(CONVERT(nvarchar(36), NEWID()), 6) + NCHAR(228) + NCHAR(246) + NCHAR(8364), "
            "20 + ABS(CHECKSUM(NEWID())) % 170)")


def build_sql() -> dict[str, tuple[str, str]]:
    """table -> (create DDL, populate DML)."""
    s_cols = [f"s{i}" for i in range(10)]
    money = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
             "total_amount", "congestion_surcharge", "Airport_fee"]
    wide = [(f"c{i}", *_WIDE_TYPES[i % len(_WIDE_TYPES)]) for i in range(299)]
    return {
        "dbo.bench_strings": (
            "CREATE TABLE dbo.bench_strings (id int NOT NULL PRIMARY KEY, "
            + ", ".join(f"{c} nvarchar(200) NULL" for c in s_cols) + ")",
            "INSERT INTO dbo.bench_strings SELECT CAST(trip_id AS int), " + ", ".join(_rand_str() for _ in s_cols)
            + " FROM dbo.yellow_trips WHERE trip_id <= 200000"),
        "dbo.bench_decimals": (
            "CREATE TABLE dbo.bench_decimals (id int NOT NULL PRIMARY KEY, "
            + ", ".join(f"d{i} decimal(10,2) NULL" for i in range(9)) + ", "
            + ", ".join(f"e{i} decimal(18,6) NULL" for i in range(3)) + ")",
            "INSERT INTO dbo.bench_decimals SELECT CAST(trip_id AS int), " + ", ".join(money) + ", "
            + ", ".join(f"CAST(ISNULL(total_amount, 1) * {i + 1}.0 / 7 AS decimal(18,6))" for i in range(3))
            + " FROM dbo.yellow_trips WHERE trip_id <= 500000"),
        "dbo.bench_datetimes": (
            "CREATE TABLE dbo.bench_datetimes (id int NOT NULL PRIMARY KEY, t0 datetime2(6) NULL, t1 datetime2(6) NULL, "
            "t2 datetime2(6) NULL, t3 datetime2(6) NULL, t4 datetime NULL, t5 datetime NULL, d0 date NULL, tm time(3) NULL)",
            "INSERT INTO dbo.bench_datetimes SELECT CAST(trip_id AS int), tpep_pickup_datetime, tpep_dropoff_datetime, "
            "DATEADD(minute, 7, tpep_pickup_datetime), DATEADD(second, 90, tpep_dropoff_datetime), "
            "CAST(tpep_pickup_datetime AS datetime), CAST(tpep_dropoff_datetime AS datetime), "
            "CAST(tpep_pickup_datetime AS date), CAST(tpep_pickup_datetime AS time(3)) "
            "FROM dbo.yellow_trips WHERE trip_id <= 500000"),
        "dbo.bench_lob": (
            "CREATE TABLE dbo.bench_lob (id int NOT NULL PRIMARY KEY, body nvarchar(max) NULL)",
            "INSERT INTO dbo.bench_lob SELECT CAST(trip_id AS int), REPLICATE(CONVERT(nvarchar(max), NEWID()), 1400) "
            "FROM dbo.yellow_trips WHERE trip_id <= 2000"),
        "dbo.bench_wide": (
            "CREATE TABLE dbo.bench_wide (id int NOT NULL PRIMARY KEY, "
            + ", ".join(f"{c} {t} NULL" for c, t, _ in wide) + ")",
            "INSERT INTO dbo.bench_wide SELECT CAST(trip_id AS int), "
            + ", ".join(e.format(i=i) for i, (_, _, e) in enumerate(wide))
            + " FROM dbo.yellow_trips WHERE trip_id <= 20000"),
    }


def _count(table: str) -> int:
    out = sqlcmd(f"SET NOCOUNT ON; IF OBJECT_ID('{table}') IS NULL SELECT -1 "
                 f"ELSE EXEC('SELECT COUNT_BIG(*) FROM {table}')").strip()
    return int(out.splitlines()[0])


def _bytes_per_row(ds: Dataset) -> float:
    cols = sqlcmd(f"SET NOCOUNT ON; SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('{ds.table}') "
                  "ORDER BY column_id").split()
    if ds.select != "*":
        wanted = [c.strip() for c in ds.select.split(",")]
        cols = [c for c in cols if c in wanted]
    expr = " + ".join(f"CAST(ISNULL(DATALENGTH([{c}]), 0) AS bigint)" for c in cols)
    out = sqlcmd(f"SET NOCOUNT ON; SELECT COUNT_BIG(*), SUM({expr}) FROM {ds.table}").strip().splitlines()[0]
    rows, total = (int(x) for x in out.split("|"))
    return total / rows


def ensure(force: bool = False) -> dict:
    for table, (ddl, dml) in build_sql().items():
        want = next(d.full_rows for d in DATASETS.values() if d.table == table)
        if not force and _count(table) == want:
            continue
        print(f"creating {table} ...", file=sys.stderr)
        sqlcmd(f"IF OBJECT_ID('{table}') IS NOT NULL DROP TABLE {table}; {ddl}; {dml}")
    RESULTS.mkdir(parents=True, exist_ok=True)
    if DATASETS_JSON.exists() and not force:
        return json.loads(DATASETS_JSON.read_text())
    info = {}
    for ds in DATASETS.values():
        n = _count(ds.table)
        if n != ds.full_rows:
            raise SystemExit(f"{ds.table} has {n} rows, expected {ds.full_rows}")
        info[ds.name] = {"table": ds.table, "rows": n, "bytes_per_row": round(_bytes_per_row(ds), 2)}
        print(f"{ds.name}: {n} rows, {info[ds.name]['bytes_per_row']} B/row", file=sys.stderr)
    DATASETS_JSON.write_text(json.dumps(info, indent=2) + "\n")
    return info


if __name__ == "__main__":
    ensure(force="--force" in sys.argv)
