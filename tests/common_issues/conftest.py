"""Fixtures for the common-issues suite.

These tests encode real-world driver pain points (pyodbc / mssql-python
issue trackers). Every test must pass on the upstream ODBC driver
(.venv-upstream) unless marked ``upstream_bug``, in which case it asserts the
*correct* behavior and documents how upstream gets it wrong.
"""

import os
import uuid

import pytest
from mssql_python import connect


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "upstream_bug(reason): correct behavior that the upstream ODBC driver gets wrong",
    )


@pytest.fixture(scope="session")
def conn_str():
    value = os.environ.get("DB_CONNECTION_STRING")
    if not value:
        pytest.skip("DB_CONNECTION_STRING not set")
    return value


@pytest.fixture
def conn(conn_str):
    connection = connect(conn_str)
    yield connection
    connection.close()


@pytest.fixture
def cursor(conn):
    cur = conn.cursor()
    yield cur
    cur.close()


@pytest.fixture
def table_name(cursor):
    """A unique permanent table name, dropped after the test."""
    name = f"ci_{uuid.uuid4().hex[:12]}"
    yield name
    cursor.execute(f"DROP TABLE IF EXISTS dbo.{name}")
    cursor.connection.commit()
