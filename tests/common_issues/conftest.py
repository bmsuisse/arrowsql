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


def pytest_collection_modifyitems(config, items):
    """On the Rust port, upstream_bug tests are plain tests: the port must get them right."""
    from mssql_python import ddbc_bindings

    if not getattr(ddbc_bindings, "__rust_port__", False):
        return
    for item in items:
        if item.get_closest_marker("upstream_bug"):
            item.own_markers = [m for m in item.own_markers if m.name != "xfail"]


class _RedactedStr(str):
    """A connection string whose repr never shows the password in tracebacks."""

    def __repr__(self):
        return "<connection string (redacted)>"


@pytest.fixture(scope="session")
def conn_str():
    value = os.environ.get("DB_CONNECTION_STRING")
    if not value:
        pytest.skip("DB_CONNECTION_STRING not set")
    return _RedactedStr(value)


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
