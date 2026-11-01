"""Bulk-load Arrow data (from DuckDB, polars, pyarrow or Parquet) into SQL Server.

    pip install arrowsql duckdb pyarrow
    python examples/write_arrow.py
"""

import duckdb

import arrowsql
from _demo import TABLE, connection_string, create_demo_table, drop_demo_table

conn_str = connection_string()
# An empty heap target: parallel loads need a heap (or a clustered columnstore).
create_demo_table(conn_str, rows=0, primary_key=False)

# Any Arrow source works: a pyarrow.Table, a RecordBatchReader, a polars
# DataFrame, or anything exposing __arrow_c_stream__ (DuckDB results do).
source = duckdb.sql(
    """
    SELECT i::INTEGER                                   AS trip_id,
           TIMESTAMP '2024-01-01' + INTERVAL (i % 44640) MINUTE AS pickup_at,
           ['Manhattan', 'Brooklyn', 'Queens', 'Bronx'][i % 4 + 1] AS zone,
           (i % 5)::UTINYINT                            AS passengers,
           ((i % 3000) / 100.0)::DECIMAL(9, 2)         AS distance_km,
           (3 + (i % 3000) / 40.0)::DECIMAL(19, 4)     AS fare
    FROM range(1, 500_001) t(i)
    """
).to_arrow_table()  # a table can be loaded twice; .arrow() is a one-shot stream

# One connection: the TDS bulk-load protocol, straight from the Arrow buffers.
with arrowsql.connect(conn_str) as conn:
    result = conn.cursor().bulkcopy_arrow(TABLE, source, table_lock=True)
    conn.commit()
    print(f"bulkcopy_arrow: {result['rows_copied']} rows")

# Several connections in parallel. The target picks the lock: TABLOCK for a
# heap, none for a clustered columnstore; other indexed targets use one worker.
with arrowsql.connect(conn_str, autocommit=True) as conn:
    conn.cursor().execute(f"TRUNCATE TABLE {TABLE}")
stats = arrowsql.write_arrow(conn_str, TABLE, source, workers=4)
print(f"write_arrow: {stats['rows_copied']} rows over {stats['workers']} connection(s), "
      f"{stats['rows_per_second']:,.0f} rows/s")

drop_demo_table(conn_str)
