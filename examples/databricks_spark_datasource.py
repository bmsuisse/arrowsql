# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server to Delta with arrowsql's Spark data source
# MAGIC
# MAGIC Each Spark task reads its slice of a SQL Server table with arrowsql (Arrow batches
# MAGIC straight from the TDS stream) and Spark writes the result to Delta. Needs a cluster
# MAGIC whose runtime supports
# MAGIC [Python data sources](https://docs.databricks.com/aws/en/pyspark/datasources)
# MAGIC and network access from the executors to SQL Server.

# COMMAND ----------

# MAGIC %md
# MAGIC arrowsql must be importable on the driver **and on every executor**. A notebook `%pip install`
# MAGIC does that (it installs into the notebook-scoped environment of all nodes). A `pip install` run
# MAGIC in a subprocess on the driver does not, and the read fails on the executors with
# MAGIC `ModuleNotFoundError: arrowsql`.

# COMMAND ----------

# MAGIC %pip install arrowsql

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("source_table", "dbo.my_big_table")
dbutils.widgets.text("partition_column", "id")
dbutils.widgets.text("num_partitions", "16")
dbutils.widgets.text("target_table", "main.staging.my_big_table")

# Placeholders: use your own secret scope and key names. Keep the connection string out of
# the notebook text, secrets are redacted in the output.
password = dbutils.secrets.get("<secret-scope>", "<sql-password-key>")
conn_str = (
    "Server=<host>,1433;Database=<database>;Uid=<user>;"
    f"Pwd={password};Encrypt=yes"
)

# COMMAND ----------

from arrowsql.spark import register

register(spark)  # spark.read.format("arrowsql") / df.write.format("arrowsql")

# COMMAND ----------

# Parallel read: one task and one connection per range of the partition column. Pick an
# indexed numeric/date column with evenly spread values; no hint about bounds is needed
# (MIN/MAX come from one query on the driver). Use hashColumns instead for a skewed or
# non-numeric key. Keep numPartitions at what the SQL Server can serve at once.
df = (
    spark.read.format("arrowsql")
    .option("connection_string", conn_str)
    .option("dbtable", dbutils.widgets.get("source_table"))
    .option("partitionColumn", dbutils.widgets.get("partition_column"))
    .option("numPartitions", dbutils.widgets.get("num_partitions"))
    # datetime/datetime2 as TimestampNTZType (the exact value). For Spark JDBC's
    # TimestampType use .option("timestampType", "timestamp") and .option("timeZone", <session zone>).
    .load()
)
display(df.limit(10))

# COMMAND ----------

(df.write.format("delta").mode("overwrite").saveAsTable(dbutils.widgets.get("target_table")))

# COMMAND ----------

# MAGIC %md
# MAGIC Write back with `df.write.format("arrowsql").option("connection_string", conn_str)
# MAGIC .option("dbtable", "dbo.target").mode("append").save()`. `overwrite` truncates the table
# MAGIC once on the driver and is not transactional across tasks.
