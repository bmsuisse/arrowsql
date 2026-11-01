"""Fixtures for the Spark data source tests: a local SparkSession and a private table per test.

Skipped as a whole when pyspark or a Java runtime is missing.
"""

import os
import shutil
import sys
import uuid

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark is not installed")
if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    pytest.skip("no Java runtime for Spark", allow_module_level=True)

from mssql_python import connect  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    from arrowsql.spark import register

    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[3]")
        .appName("arrowsql-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # Small driver heap: the large-read test only passes if rows do not pile up there.
        .config("spark.driver.memory", "1g")
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    except ImportError:
        pass
    session = builder.getOrCreate()
    register(session)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def conn_str():
    value = os.environ.get("DB_CONNECTION_STRING")
    if not value:
        pytest.skip("DB_CONNECTION_STRING is not set")
    return value


@pytest.fixture
def run_sql(conn_str):
    def run(sql, params=()):
        with connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params) if params else cursor.execute(sql)
            return cursor.fetchall() if cursor.description else None

    return run


@pytest.fixture
def table(run_sql):
    """A unique table name; dropped afterwards."""
    name = f"spark_t_{uuid.uuid4().hex[:10]}"
    yield name
    run_sql(f"DROP TABLE IF EXISTS dbo.[{name}]")


@pytest.fixture
def reader(spark, conn_str):
    def make(**options):
        r = spark.read.format("arrowsql").option("connection_string", conn_str)
        for key, value in options.items():
            r = r.option(key, value)
        return r

    return make
