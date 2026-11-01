"""Read and write SQL Server from Spark with arrowsql's Python data source.

    pip install "arrowsql[spark]"          # pyspark>=4.0 and a Java runtime (17 or newer)
    export ARROWSQL_CONN='Server=localhost,1433;Database=tempdb;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes'
    python examples/spark_datasource.py

Every Spark task opens its own connection and reads its slice of the query with
arrowsql (Arrow batches, no rows, no pandas). Spark's own JDBC source does the same job
row by row through the JVM; this one skips that.
"""

import os
import sys
import tempfile

from pyspark.sql import SparkSession

import arrowsql
from _demo import TABLE, create_demo_table, drop_demo_table

conn_str = os.environ.get("ARROWSQL_CONN") or os.environ.get("DB_CONNECTION_STRING")
if not conn_str:
    sys.exit("Set ARROWSQL_CONN to an ODBC-style connection string (see the docstring).")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers use this interpreter
spark = (
    SparkSession.builder.master("local[4]")
    .appName("arrowsql-spark-example")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
create_demo_table(conn_str, rows=200_000, primary_key=False)

# --- 1. Register the source: spark.read.format("arrowsql") works afterwards ----------
from arrowsql.spark import register  # noqa: E402

register(spark)


def source():
    return spark.read.format("arrowsql").option("connection_string", conn_str)


# --- 2. Read a whole table (one task, one connection) --------------------------------
trips = source().option("dbtable", TABLE).load()
trips.printSchema()  # tinyint arrives as smallint: Spark has no unsigned types
print("rows:", trips.count())

# --- 3. Read a query -----------------------------------------------------------------
# Any SELECT without a trailing ORDER BY. It becomes a derived table, so every
# column needs a name.
busy = source().option("query", f"SELECT zone, fare FROM {TABLE} WHERE passengers >= 3").load()
print("busy trips:", busy.count())

# --- 4. Parallel read, range partitioning (like Spark JDBC) --------------------------
# numPartitions tasks, each reading a half-open range of trip_id on its own connection.
# Rows whose partition column is NULL are read by the first task, so none is lost.
# Without bounds, MIN/MAX come from one query on the driver.
ranged = source().option("dbtable", TABLE).option("partitionColumn", "trip_id").option("numPartitions", 4).load()
print("range, no bounds:", ranged.rdd.getNumPartitions(), "partitions,", ranged.count(), "rows")

# With bounds, the driver runs no query. Rows outside the bounds still land in the
# first and last slice (the bounds only decide the stride), so nothing is dropped.
bounded = (
    source()
    .option("dbtable", TABLE)
    .option("partitionColumn", "trip_id")
    .option("lowerBound", 1)
    .option("upperBound", 200_000)
    .option("numPartitions", 4)
    .load()
)
print("range, bounds:", bounded.count(), "rows")  # date and datetime columns work too

# --- 5. Parallel read, hash partitioning ---------------------------------------------
# For a key that is not numeric or is skewed: task i reads the rows where
# (CHECKSUM(key columns) & 2147483647) % numPartitions = i. Hash key columns only; a
# column that changes between queries (GETUTCDATE()) would break the slices.
hashed = source().option("dbtable", TABLE).option("hashColumns", "trip_id").option("numPartitions", 4).load()
print("hash:", hashed.rdd.getNumPartitions(), "partitions,", hashed.count(), "rows")

# --- 6. Timestamps: ntz (default) vs timestamp ---------------------------------------
# datetime/datetime2 have no time zone. "ntz" keeps the value exactly (TimestampNTZType).
# "timestamp" gives Spark's TimestampType like the JDBC source: the wall-clock value is
# read as local time in `timeZone` (default UTC; set it to spark.sql.session.timeZone).
ntz = source().option("dbtable", TABLE).load().select("pickup_at")
inst = source().option("dbtable", TABLE).option("timestampType", "timestamp").option("timeZone", "UTC").load().select("pickup_at")
print(ntz.schema["pickup_at"].dataType.simpleString(), "vs", inst.schema["pickup_at"].dataType.simpleString())

# --- 7. Write: append and overwrite ---------------------------------------------------
# Each task bulk-copies its Arrow batches over its own connection. Column names must be
# the table's column names. overwrite = TRUNCATE TABLE once on the driver, then append;
# the load is not transactional across tasks.
target = "dbo.arrowsql_example_trips_copy"
with arrowsql.connect(conn_str, autocommit=True) as conn:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {target}")
    cur.execute(f"SELECT * INTO {target} FROM {TABLE} WHERE 1 = 0")


def sink(mode):
    return trips.write.format("arrowsql").option("connection_string", conn_str).option("dbtable", target).mode(mode)


sink("append").save()
sink("append").save()
sink("overwrite").save()  # back to one copy
with arrowsql.connect(conn_str) as conn:
    print("rows after append, append, overwrite:", conn.cursor().execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0])

# --- 8. End to end: SQL Server -> transform -> Delta ----------------------------------
# Delta needs delta-spark (pip install delta-spark) and a session built with it; without
# it this section writes Parquet so the example still runs.
summary = ranged.groupBy("zone").agg({"fare": "sum", "trip_id": "count"}).withColumnRenamed("sum(fare)", "fare_total")
out = os.path.join(tempfile.mkdtemp(), "trips_by_zone")
try:
    summary.write.format("delta").mode("overwrite").save(out)
    print("wrote Delta table to", out)
except Exception as exc:  # pylint: disable=broad-exception-caught
    summary.write.mode("overwrite").parquet(out)
    print("Delta not available here (", type(exc).__name__, "), wrote Parquet to", out)
summary.show()

with arrowsql.connect(conn_str, autocommit=True) as conn:
    conn.cursor().execute(f"DROP TABLE IF EXISTS {target}")
drop_demo_table(conn_str)
spark.stop()
