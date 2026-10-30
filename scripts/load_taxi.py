"""Load NYC yellow taxi trips into the `nyctaxi` database.

Uses the upstream mssql-python wheel (bulkcopy_arrow), so run it with
.venv-upstream. Creates dbo.yellow_trips (full month, ~3M rows) and
dbo.yellow_trips_100k (first 100k rows) for quick benchmarks.

    set -a; . .env.local; set +a
    .venv-upstream/bin/python scripts/load_taxi.py data/yellow_tripdata_2024-01.parquet
"""

import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from mssql_python import connect

MONEY_COLUMNS = {
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
}

DDL = """
CREATE TABLE dbo.{name} (
    trip_id               bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    VendorID              int            NULL,
    tpep_pickup_datetime  datetime2(6)   NULL,
    tpep_dropoff_datetime datetime2(6)   NULL,
    passenger_count       bigint         NULL,
    trip_distance         float          NULL,
    RatecodeID            bigint         NULL,
    store_and_fwd_flag    nvarchar(1)    NULL,
    PULocationID          int            NULL,
    DOLocationID          int            NULL,
    payment_type          bigint         NULL,
    fare_amount           decimal(10, 2) NULL,
    extra                 decimal(10, 2) NULL,
    mta_tax               decimal(10, 2) NULL,
    tip_amount            decimal(10, 2) NULL,
    tolls_amount          decimal(10, 2) NULL,
    improvement_surcharge decimal(10, 2) NULL,
    total_amount          decimal(10, 2) NULL,
    congestion_surcharge  decimal(10, 2) NULL,
    Airport_fee           decimal(10, 2) NULL
)
"""


def main(path: str) -> None:
    base = os.environ["DB_CONNECTION_STRING"]
    with connect(base, autocommit=True) as conn:
        conn.cursor().execute(
            "IF DB_ID('nyctaxi') IS NULL CREATE DATABASE nyctaxi; "
            "ALTER DATABASE nyctaxi SET RECOVERY SIMPLE;"
        )

    table = pq.read_table(path)
    # The Arrow bulk-copy writer does not convert float64 into DECIMAL.
    for i, field in enumerate(table.schema):
        if field.name in MONEY_COLUMNS:
            table = table.set_column(i, field.name, table.column(i).cast(pa.decimal128(10, 2)))
    columns = table.column_names
    mappings = [(i, name) for i, name in enumerate(columns)]

    with connect(base.replace("Database=master", "Database=nyctaxi"), autocommit=True) as conn:
        cur = conn.cursor()
        for name, data in (("yellow_trips_100k", table.slice(0, 100_000)), ("yellow_trips", table)):
            cur.execute(f"DROP TABLE IF EXISTS dbo.{name}")
            cur.execute(DDL.format(name=name))
            start = time.perf_counter()
            cur.bulkcopy_arrow(
                f"dbo.{name}",
                data,
                batch_size=100_000,
                timeout=0,
                column_mappings=mappings,
                table_lock=True,
            )
            cur.execute(f"SELECT COUNT_BIG(*) FROM dbo.{name}")
            count = cur.fetchone()[0]
            print(f"{name}: {count} rows in {time.perf_counter() - start:.1f}s")


if __name__ == "__main__":
    main(sys.argv[1])
