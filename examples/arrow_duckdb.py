"""Read SQL Server results as Arrow and query them with DuckDB, polars and pandas.

    pip install arrowsql duckdb polars pyarrow pandas
    export DB_CONNECTION_STRING='Server=...;Database=...;Uid=...;Pwd=...;Encrypt=yes'
    python examples/arrow_duckdb.py
"""

import duckdb

import arrowsql
from _demo import TABLE, connection_string, create_demo_table, drop_demo_table

conn_str = connection_string()
create_demo_table(conn_str)

with arrowsql.connect(conn_str) as conn:
    cur = conn.cursor()

    # 1. The whole result as a native Arrow table. No pyarrow needed: DuckDB,
    #    polars and pyarrow read it zero-copy through the Arrow C interfaces.
    trips = cur.execute(f"SELECT * FROM {TABLE}").fetchall_arrow()
    print(f"fetched {trips.num_rows} rows")
    duckdb.sql(
        """
        SELECT zone, count(*) AS trips, round(avg(fare), 2) AS avg_fare
        FROM trips GROUP BY zone ORDER BY trips DESC
        """
    ).show()

    # 2. Streaming: batches are decoded in the background while DuckDB scans,
    #    so a result larger than memory never has to be held at once.
    stream = cur.execute(f"SELECT pickup_at, distance_km FROM {TABLE}").arrow_stream(batch_size=50_000)
    duckdb.sql(
        "SELECT date_trunc('day', pickup_at) AS day, sum(distance_km) AS km "
        "FROM stream GROUP BY 1 ORDER BY 1 LIMIT 5"
    ).show()

    # 3. pyarrow.Table (upstream mssql-python compatible API; needs pyarrow).
    table = cur.execute(f"SELECT TOP 5 trip_id, zone, fare FROM {TABLE} ORDER BY trip_id").arrow()
    print(table.schema)

    # 4. polars and pandas.
    import polars as pl

    df = pl.DataFrame(cur.execute(f"SELECT zone, fare FROM {TABLE}").fetchall_arrow())
    print(df.group_by("zone").agg(pl.col("fare").max()).sort("zone"))

    pdf = cur.execute(f"SELECT TOP 3 * FROM {TABLE} ORDER BY trip_id").arrow().to_pandas()
    print(pdf)

# 5. Partitioned parallel read: the integer column's range is split and read over
#    several connections at once. Worth it for large tables.
big = arrowsql.read_arrow(conn_str, f"SELECT * FROM {TABLE}", partition_on="trip_id", partitions=4)
print(f"read_arrow over 4 connections: {big.num_rows} rows")

# 6. The same read as a stream: DuckDB aggregates batches while the four
#    connections are still reading (bounded memory, unordered batches).
#    Leaving the block cancels anything still running.
with arrowsql.read_arrow_stream(
    conn_str, f"SELECT * FROM {TABLE}", partition_on="trip_id", partitions=4
) as streamed:
    duckdb.sql("SELECT count(*) AS n, avg(fare) AS avg_fare FROM streamed").show()

drop_demo_table(conn_str)
