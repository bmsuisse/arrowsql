"""Bulk-write payloads: the NYC taxi month as rows or Arrow, plus small synthetic row sets.

The 20-column layout matches dbo.yellow_trips (trip_id first). `rows()` and
`row_chunks()` iterate a cached list of Python tuples once `materialize_rows()` has run
(workloads.py prepare() does that, untimed, for the plain write.bulk_rows_* workloads, so
the timed region measures the driver and not Arrow -> tuple boxing, which bcp also gets
for free). Without the cache they box Arrow cells lazily in 10k-row chunks inside the
timed call: that is the old behaviour, kept as the write.bulk_rows_*.stream workloads.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from common import main_repo

MONEY = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
         "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"]

TAXI_COLUMNS = [
    ("trip_id", "bigint NOT NULL"), ("VendorID", "int NULL"), ("tpep_pickup_datetime", "datetime2(6) NULL"),
    ("tpep_dropoff_datetime", "datetime2(6) NULL"), ("passenger_count", "bigint NULL"),
    ("trip_distance", "float NULL"), ("RatecodeID", "bigint NULL"), ("store_and_fwd_flag", "nvarchar(1) NULL"),
    ("PULocationID", "int NULL"), ("DOLocationID", "int NULL"), ("payment_type", "bigint NULL"),
    *[(m, "decimal(10, 2) NULL") for m in MONEY],
]
TAXI_SOURCE_SQL = "SELECT {top} " + ", ".join(c for c, _ in TAXI_COLUMNS) + " FROM dbo.yellow_trips ORDER BY trip_id"


def taxi_target_ddl(table: str, layout: str) -> str:
    """layout: heap | pk (clustered primary key) | cci (clustered columnstore)."""
    cols = list(TAXI_COLUMNS)
    if layout == "pk":
        cols[0] = ("trip_id", "bigint NOT NULL PRIMARY KEY CLUSTERED")
    ddl = f"CREATE TABLE {table} (" + ", ".join(f"{c} {t}" for c, t in cols) + ")"
    if layout == "cci":
        ddl += f"; CREATE CLUSTERED COLUMNSTORE INDEX cci_{table.split('.')[-1]} ON {table}"
    return ddl


class TaxiPayload:
    """First `n` rows of the taxi month, loaded lazily from the parquet file."""

    columns = [c for c, _ in TAXI_COLUMNS]

    def __init__(self, n: int):
        self.n = n
        self._table = None
        self._rows: list | None = None

    @property
    def source_sql(self) -> str:
        return TAXI_SOURCE_SQL.format(top=f"TOP {self.n}")

    def arrow(self):
        if self._table is None:
            import pyarrow as pa
            import pyarrow.parquet as pq

            t = pq.read_table(main_repo() / "data" / "yellow_tripdata_2024-01.parquet")
            for i, f in enumerate(t.schema):
                if f.name in MONEY:
                    t = t.set_column(i, f.name, t.column(i).cast(pa.decimal128(10, 2)))
            t = t.slice(0, self.n)
            ids = pa.array(range(1, t.num_rows + 1), type=pa.int64())
            self._table = t.add_column(0, "trip_id", ids)
            assert self._table.column_names == self.columns, self._table.column_names
        return self._table

    def materialize_rows(self) -> list:
        """Build (once) the full list of row tuples; ~1 GB at 1M rows."""
        if self._rows is None:
            self._rows = list(self._boxed_rows())
        return self._rows

    def _boxed_rows(self, size: int = 10_000):
        t = self.arrow()
        for start in range(0, t.num_rows, size):
            part = t.slice(start, size)
            yield from zip(*[col.to_pylist() for col in part.columns])

    def row_chunks(self, size: int = 10_000):
        if self._rows is not None:
            for start in range(0, len(self._rows), size):
                yield self._rows[start:start + size]
            return
        t = self.arrow()
        for start in range(0, t.num_rows, size):
            part = t.slice(start, size)
            yield list(zip(*[col.to_pylist() for col in part.columns]))

    def rows(self):
        """A ready list when materialized, else a lazy generator that boxes inside the timed call."""
        if self._rows is not None:
            return self._rows
        return self._boxed_rows()


def small_rows(n: int) -> list[tuple]:
    """5-column rows for single/executemany/multi-row insert workloads."""
    return [(i, dt.datetime(2024, 1, 1, 0, i % 60, i % 60), float(i) / 10, Decimal(f"{i % 1000}.25"), f"row-{i}")
            for i in range(n)]


SMALL_DDL = "CREATE TABLE {t} (a int, b datetime2(6), c float, d decimal(10,2), e nvarchar(50))"
SMALL_INSERT = "INSERT INTO {t} VALUES (?, ?, ?, ?, ?)"
