"""Time-varying queries, a large read with a small driver heap, and SQL Server -> Delta."""

import pytest


@pytest.mark.parametrize(
    "extra",
    [
        {"hashColumns": "id", "numPartitions": "4"},
        {"partitionColumn": "id", "numPartitions": "4"},
        {"partitionColumn": "id", "numPartitions": "4", "lowerBound": "1", "upperBound": "100"},
    ],
)
def test_partitioned_query_with_time_varying_column(reader, run_sql, table, extra):
    # Each slice is its own query: hashing `*` would put a row in no slice or in two.
    run_sql(f"CREATE TABLE dbo.[{table}] (id INT NOT NULL, v INT NOT NULL)")
    run_sql(
        f"INSERT INTO dbo.[{table}] SELECT TOP (3000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 1 "
        "FROM sys.all_objects a, sys.all_objects b"
    )
    query = f"SELECT id, v, CAST(SYSUTCDATETIME() AS DATETIME2(7)) AS ts FROM dbo.[{table}]"
    df = reader(query=query, **extra).load()
    assert df.count() == 3000
    assert df.select("id").distinct().count() == 3000


def test_large_read_does_not_go_through_the_driver(reader, run_sql, table):
    # spark.driver.memory is 1g (conftest): 2M rows with padding do not fit as Python objects.
    run_sql(f"CREATE TABLE dbo.[{table}] (id INT NOT NULL, pad VARCHAR(60) NOT NULL)")
    run_sql(
        f"""
        INSERT INTO dbo.[{table}]
        SELECT TOP (2000000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), REPLICATE('x', 60)
        FROM sys.all_objects a, sys.all_objects b, sys.all_objects c
        """
    )
    df = reader(dbtable=f"dbo.{table}", partitionColumn="id", numPartitions="6").load()
    stats = df.selectExpr("count(*) AS n", "count(DISTINCT id) AS d", "sum(length(pad)) AS bytes").first()
    assert (stats["n"], stats["d"], stats["bytes"]) == (2_000_000, 2_000_000, 120_000_000)


@pytest.mark.parametrize("mode", ["ntz", "timestamp"])
def test_end_to_end_to_delta(spark, reader, run_sql, table, tmp_path, mode):
    pytest.importorskip("delta", reason="delta-spark is not installed")
    run_sql(
        f"""CREATE TABLE dbo.[{table}] (id INT NOT NULL, dt DATETIME NULL, dt2 DATETIME2(6) NULL,
            dec DECIMAL(18, 4) NULL, g UNIQUEIDENTIFIER NULL, bin VARBINARY(20) NULL,
            txt NVARCHAR(MAX) NULL, tiny TINYINT NULL, flag BIT NULL)"""
    )
    run_sql(
        f"""
        INSERT INTO dbo.[{table}]
        SELECT v.i, DATEADD(SECOND, v.i * 7, '2023-06-15T10:00:00'), DATEADD(MICROSECOND, v.i, CAST('2023-06-15T10:00:00' AS DATETIME2(6))),
               v.i / 7.0, NEWID(), CAST(v.i AS VARBINARY(4)), REPLICATE(N'ax', v.i % 50 + 1), v.i % 256,
               CASE WHEN v.i % 3 = 0 THEN NULL ELSE v.i % 2 END
        FROM (SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS i FROM sys.all_objects) v
        UNION ALL SELECT 1000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        """
    )
    src = reader(
        dbtable=f"dbo.{table}", partitionColumn="id", numPartitions="3", timestampType=mode, timeZone="Europe/Zurich"
    ).load()
    path = str(tmp_path / "delta")
    src.write.format("delta").save(path)
    back = spark.read.format("delta").load(path)
    assert [(f.name, f.dataType) for f in back.schema] == [(f.name, f.dataType) for f in src.schema]
    assert back.count() == 501
    assert src.exceptAll(back).count() == 0 and back.exceptAll(src).count() == 0
    # Values, not just a round trip: compare with what the cursor returns.
    expected = {r[0]: r for r in run_sql(f"SELECT id, dec, txt, tiny, flag FROM dbo.[{table}]")}
    for row in back.select("id", "dec", "txt", "tiny", "flag").collect():
        _, dec, txt, tiny, flag = expected[row["id"]]
        assert (row["dec"], row["txt"], row["tiny"], row["flag"]) == (dec, txt, tiny, None if flag is None else bool(flag))
