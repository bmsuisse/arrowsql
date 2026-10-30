"""Free-threaded CPython (PEP 703) behaviour of the Rust extension.

The extension declares ``gil_used = false``, so importing it must not turn
the GIL back on in a free-threaded interpreter. The tests run point lookups
and full fetches from many threads at once, each thread on its own
connection, and check every result. On a GIL build they still run and act as
a plain thread-safety check.
"""

import os
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor

import pytest

from mssql_python import connect

pytestmark = pytest.mark.skipif(
    not os.getenv("DB_CONNECTION_STRING"), reason="Database connection string not provided"
)

FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
ROWS = 2000
LOOKUPS_PER_THREAD = 50
TABLE = "rs_ft_scratch"


@pytest.fixture
def conn_str():
    return os.environ["DB_CONNECTION_STRING"]


@pytest.fixture
def scratch_table(conn_str):
    """A permanent scratch table: temp tables are invisible to other connections."""
    conn = connect(conn_str, autocommit=True)
    try:
        cursor = conn.cursor()
        cursor.execute(f"IF OBJECT_ID('{TABLE}') IS NOT NULL DROP TABLE {TABLE}")
        cursor.execute(f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, payload NVARCHAR(50))")
        cursor.execute(
            f"INSERT INTO {TABLE} (id, payload) "
            "SELECT TOP (?) n, CONCAT('row-', n) FROM "
            "(SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b) AS src",
            ROWS,
        )
        cursor.close()
    finally:
        conn.close()
    yield TABLE
    conn = connect(conn_str, autocommit=True)
    try:
        conn.cursor().execute(f"IF OBJECT_ID('{TABLE}') IS NOT NULL DROP TABLE {TABLE}")
    finally:
        conn.close()


def _fetch_all(conn_str):
    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT id, payload FROM {TABLE} ORDER BY id")
        rows = [tuple(r) for r in cursor.fetchall()]
        cursor.close()
        return rows
    finally:
        conn.close()


def _point_lookups(conn_str, worker_index):
    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        for i in range(LOOKUPS_PER_THREAD):
            key = (worker_index * 997 + i * 13) % ROWS + 1
            cursor.execute(f"SELECT id, payload FROM {TABLE} WHERE id = ?", key)
            row = cursor.fetchone()
            assert row is not None and row[0] == key and row[1] == f"row-{key}"
        cursor.close()
        return LOOKUPS_PER_THREAD
    finally:
        conn.close()


def test_import_keeps_gil_disabled_on_free_threaded_build():
    if not FREE_THREADED:
        pytest.skip("GIL build")
    import mssql_python.ddbc_bindings  # noqa: F401

    assert sys._is_gil_enabled() is False


@pytest.mark.parametrize("threads", [8, 16])
def test_concurrent_point_lookups(conn_str, scratch_table, threads):
    with ThreadPoolExecutor(max_workers=threads) as pool:
        done = list(pool.map(lambda w: _point_lookups(conn_str, w), range(threads)))
    assert sum(done) == LOOKUPS_PER_THREAD * threads


def test_concurrent_fetchall_matches_serial(conn_str, scratch_table):
    serial = _fetch_all(conn_str)
    assert len(serial) == ROWS
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: _fetch_all(conn_str), range(8)))
    assert all(rows == serial for rows in results)
