"""arrowsql Spark data source against the local SQL Server and a local SparkSession."""

import datetime

import pytest

NROWS = 1000


def _fill(run_sql, table, nrows=NROWS):
    """id = 3 * i (not dense), grp, a nullable int, tinyint, datetime2, date, nullable text."""
    run_sql(
        f"CREATE TABLE dbo.[{table}] (id INT NOT NULL, grp INT NOT NULL, n INT NULL, "
        "small TINYINT NOT NULL, ts DATETIME2(3) NULL, d DATE NULL, s NVARCHAR(20) NULL)"
    )
    run_sql(
        f"""
        INSERT INTO dbo.[{table}] (id, grp, n, small, ts, d, s)
        SELECT v.i * 3, v.i % 7, CASE WHEN v.i % 10 = 0 THEN NULL ELSE v.i END, v.i % 256,
               DATEADD(MINUTE, v.i, '2024-03-31T00:30:00'), DATEADD(DAY, v.i, '2020-01-01'),
               CASE WHEN v.i % 5 = 0 THEN NULL ELSE CONCAT(N'row-', v.i, NCHAR(228)) END
        FROM (SELECT TOP ({nrows}) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS i
              FROM sys.all_objects a CROSS JOIN sys.all_objects b) v
        """
    )


def _ids(df):
    return sorted(r["id"] for r in df.select("id").collect())


def _expected_ids(nrows=NROWS):
    return [i * 3 for i in range(1, nrows + 1)]


def test_plain_read_equals_cursor(reader, run_sql, table):
    _fill(run_sql, table)
    df = reader(dbtable=f"dbo.{table}").load()
    assert df.rdd.getNumPartitions() == 1
    got = sorted(tuple(r) for r in df.collect())
    expected = sorted(tuple(r) for r in run_sql(f"SELECT id, grp, n, small, ts, d, s FROM dbo.[{table}]"))
    assert got == expected
    assert any(row[-1] and "ä" in row[-1] for row in got)


def test_query_option(reader, run_sql, table):
    _fill(run_sql, table)
    df = reader(query=f"SELECT id, grp FROM dbo.[{table}] WHERE grp = 3").load()
    assert df.columns == ["id", "grp"]
    assert df.count() == len(run_sql(f"SELECT 1 FROM dbo.[{table}] WHERE grp = 3"))


@pytest.mark.parametrize("bounds", [False, True])
@pytest.mark.parametrize("column", ["id", "n"])
def test_range_partitioning_returns_every_row_once(reader, run_sql, table, column, bounds):
    _fill(run_sql, table)
    options = {"dbtable": f"dbo.{table}", "partitionColumn": column, "numPartitions": "5"}
    if bounds:
        # Bounds narrower than the data: out-of-range rows must land in the open-ended slices.
        options.update(lowerBound="300", upperBound="900")
    df = reader(**options).load()
    assert df.rdd.getNumPartitions() == 5
    assert _ids(df) == _expected_ids()  # includes the rows whose n is NULL


def test_range_partitioning_more_partitions_than_values(reader, run_sql, table):
    _fill(run_sql, table, nrows=4)
    df = reader(dbtable=f"dbo.{table}", partitionColumn="grp", numPartitions="50").load()
    assert df.rdd.getNumPartitions() <= 4
    assert _ids(df) == _expected_ids(4)


def test_range_partitioning_empty_table_and_all_null_column(reader, run_sql, table):
    run_sql(f"CREATE TABLE dbo.[{table}] (id INT NULL)")
    df = reader(dbtable=f"dbo.{table}", partitionColumn="id", numPartitions="4").load()
    assert df.count() == 0
    run_sql(f"INSERT INTO dbo.[{table}] VALUES (NULL), (NULL)")
    assert reader(dbtable=f"dbo.{table}", partitionColumn="id", numPartitions="4").load().count() == 2


def test_range_partitioning_on_date_and_datetime(reader, run_sql, table):
    _fill(run_sql, table)
    for column in ("d", "ts"):
        df = reader(dbtable=f"dbo.{table}", partitionColumn=column, numPartitions="4").load()
        assert df.rdd.getNumPartitions() == 4
        assert _ids(df) == _expected_ids()
    bounded = reader(
        dbtable=f"dbo.{table}",
        partitionColumn="d",
        numPartitions="3",
        lowerBound="2020-02-01",
        upperBound="2020-03-01",
    ).load()
    assert _ids(bounded) == _expected_ids()


def test_hash_partitioning_returns_every_row_once(reader, run_sql, table):
    _fill(run_sql, table)
    # A time-varying column in the query: slices must still be disjoint and complete.
    query = f"SELECT *, SYSUTCDATETIME() AS stamp FROM dbo.[{table}]"
    df = reader(query=query, hashColumns="id", numPartitions="4").load()
    assert df.rdd.getNumPartitions() == 4
    assert _ids(df) == _expected_ids()
    multi = reader(dbtable=f"dbo.{table}", hashColumns="id, grp", numPartitions="3").load()
    assert _ids(multi) == _expected_ids()


def test_tinyint_is_widened(reader, run_sql, table):
    _fill(run_sql, table)
    df = reader(dbtable=f"dbo.{table}").load()
    assert df.schema["small"].dataType.simpleString() == "smallint"
    assert max(r["small"] for r in df.select("small").collect()) == 255


def test_empty_result(reader, run_sql, table):
    _fill(run_sql, table)
    extras = ({}, {"partitionColumn": "id", "numPartitions": "3"}, {"hashColumns": "id", "numPartitions": "3"})
    for extra in extras:
        df = reader(query=f"SELECT id, small, ts FROM dbo.[{table}] WHERE 1 = 0", **extra).load()
        assert df.columns == ["id", "small", "ts"]
        assert df.count() == 0


def test_timestamp_type_ntz_is_exact(reader, run_sql, table):
    _fill(run_sql, table)
    df = reader(dbtable=f"dbo.{table}").load()
    assert df.schema["ts"].dataType.simpleString() == "timestamp_ntz"
    assert df.where("id = 3").select("ts").first()[0] == datetime.datetime(2024, 3, 31, 0, 31)


def test_timestamp_type_timestamp_uses_time_zone(spark, reader, run_sql, table):
    _fill(run_sql, table)
    # Wall clock 2024-03-31 00:31 in Europe/Zurich (UTC+1 then) is 23:31 UTC the day before.
    df = reader(dbtable=f"dbo.{table}", timestampType="timestamp", timeZone="Europe/Zurich").load()
    assert df.schema["ts"].dataType.simpleString() == "timestamp"
    one = df.where("id = 3")
    assert one.selectExpr("date_format(ts, 'yyyy-MM-dd HH:mm')").first()[0] == "2024-03-30 23:31"
    spark.conf.set("spark.sql.session.timeZone", "Europe/Zurich")
    try:
        assert one.selectExpr("date_format(ts, 'yyyy-MM-dd HH:mm')").first()[0] == "2024-03-31 00:31"
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def _write(df, conn_str, table, mode):
    df.write.format("arrowsql").option("connection_string", conn_str).option("dbtable", f"dbo.{table}").mode(mode).save()


def test_write_append_and_overwrite(spark, reader, run_sql, table, conn_str):
    run_sql(f"CREATE TABLE dbo.[{table}] (id INT NOT NULL, label NVARCHAR(20) NULL, ts DATETIME2(6) NULL)")
    df = spark.createDataFrame(
        [(i, None if i % 4 == 0 else f"v{i}", datetime.datetime(2024, 1, 1, 12, 0, i % 60)) for i in range(200)],
        "id int, label string, ts timestamp_ntz",
    ).repartition(3)
    _write(df, conn_str, table, "append")
    _write(df, conn_str, table, "append")
    assert run_sql(f"SELECT COUNT(*) FROM dbo.[{table}]")[0][0] == 400
    _write(df, conn_str, table, "overwrite")
    assert run_sql(f"SELECT COUNT(*) FROM dbo.[{table}]")[0][0] == 200
    back = reader(dbtable=f"dbo.{table}").load()
    assert sorted(tuple(r) for r in back.collect()) == sorted(tuple(r) for r in df.collect())


def test_write_empty_dataframe(spark, run_sql, table, conn_str):
    run_sql(f"CREATE TABLE dbo.[{table}] (id INT NOT NULL)")
    _write(spark.createDataFrame([], "id int"), conn_str, table, "append")
    assert run_sql(f"SELECT COUNT(*) FROM dbo.[{table}]")[0][0] == 0


def test_write_timestamp_type_writes_wall_clock(spark, run_sql, table, conn_str):
    run_sql(f"CREATE TABLE dbo.[{table}] (ts DATETIME2(6) NOT NULL)")
    df = spark.createDataFrame([(datetime.datetime(2024, 5, 6, 7, 8, 9),)], "ts timestamp")  # session zone UTC
    with pytest.raises(Exception, match="datetimeoffset"):
        _write(df, conn_str, table, "append")
    (
        df.write.format("arrowsql")
        .option("connection_string", conn_str)
        .option("dbtable", f"dbo.{table}")
        .option("timestampType", "timestamp")
        .option("timeZone", "Europe/Zurich")
        .mode("append")
        .save()
    )
    # 07:08:09 UTC is 09:08:09 in Zurich (CEST).
    assert run_sql(f"SELECT ts FROM dbo.[{table}]")[0][0] == datetime.datetime(2024, 5, 6, 9, 8, 9)


@pytest.mark.parametrize(
    "options, message",
    [
        ({}, "exactly one"),
        ({"dbtable": "dbo.t", "query": "SELECT 1"}, "exactly one"),
        ({"dbtable": "dbo.t; DROP TABLE x"}, "table or view name"),
        ({"dbtable": "dbo.t", "numPartitions": "4"}, "partitionColumn or hashColumns"),
        ({"dbtable": "dbo.t", "numPartitions": "0"}, "numPartitions"),
        ({"dbtable": "dbo.t", "numPartitions": "x"}, "numPartitions"),
        ({"dbtable": "dbo.t", "lowerBound": "1", "upperBound": "2"}, "need partitionColumn"),
        ({"dbtable": "dbo.t", "partitionColumn": "id", "numPartitions": "2", "lowerBound": "1"}, "together"),
        ({"dbtable": "dbo.t", "partitionColumn": "id", "hashColumns": "id", "numPartitions": "2"}, "not both"),
        ({"dbtable": "dbo.t", "timestampType": "bogus"}, "timestampType"),
        ({"dbtable": "dbo.t", "utf8Strings": "maybe"}, "utf8Strings"),
    ],
)
def test_invalid_options(reader, options, message):
    with pytest.raises(Exception, match=message):
        reader(**options).load()


def test_missing_connection_string(spark):
    with pytest.raises(Exception, match="connection_string"):
        spark.read.format("arrowsql").option("dbtable", "dbo.t").load()


def test_invalid_write_options(spark, conn_str):
    df = spark.createDataFrame([(1,)], "id int")
    with pytest.raises(Exception, match="dbtable"):
        df.write.format("arrowsql").option("connection_string", conn_str).mode("append").save()
    with pytest.raises(Exception, match="table or view name"):
        _write(df, conn_str, "t; DROP TABLE x", "append")


def test_bad_bound_and_unknown_column(reader, run_sql, table):
    run_sql(f"CREATE TABLE dbo.[{table}] (id INT NOT NULL)")
    base = {"dbtable": f"dbo.{table}", "numPartitions": "2"}
    with pytest.raises(Exception, match="cannot parse bound"):
        reader(partitionColumn="id", lowerBound="a", upperBound="b", **base).load().collect()
    with pytest.raises(Exception, match="not a column"):
        reader(partitionColumn="nope", **base).load().collect()

