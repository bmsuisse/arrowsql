"""Connection lifecycle, pooling and result-value ownership, ported from FastMssql.

Adapted from Rivendael/FastMssql tests/test_explicit_connection_advanced.py,
tests/test_connection_pool.py and tests/test_lazy_conversion.py (MIT), Copyright (c) 2025
Rivendael. The MIT permission notice is reproduced in
tests/common_issues/ported/test_port_fastmssql_async_threads.py, which this file shares a
license with. FastMssql's explicit connect()/disconnect() and pool-statistics API does not
exist here, so these tests use mssql_python.connect(), close(), the module-level pooling()
switch and the DB-API cursor. Lazy conversion is not a DB-API concept; its test intent is
kept as "values already fetched stay correct after the driver reuses its buffers".
"""

from concurrent.futures import ThreadPoolExecutor

import pytest
from mssql_python import connect, pooling
from mssql_python.exceptions import InterfaceError


@pytest.fixture
def pooled():
    """Configure pooling for one test. Pooling is auto-enabled with defaults, so restore those."""
    pooling(max_size=10, idle_timeout=600, enabled=True)
    yield
    pooling(max_size=100, idle_timeout=600, enabled=True)


def test_connect_query_close_cycles_all_succeed(conn_str):
    for i in range(5):
        connection = connect(conn_str, autocommit=True)
        cursor = connection.cursor()
        cursor.execute("SELECT ? AS cycle", i)
        assert cursor.fetchone()[0] == i
        connection.close()
        assert connection.closed


def test_double_close_is_harmless(conn_str):
    connection = connect(conn_str)
    connection.close()
    connection.close()

    assert connection.closed


def test_context_manager_rolls_back_and_closes_when_block_raises(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY)")
    cursor.connection.commit()

    kept = None
    with pytest.raises(ValueError):
        with connect(conn_str) as connection:
            kept = connection
            connection.cursor().execute(f"INSERT INTO dbo.{table_name} VALUES (1)")
            raise ValueError("abort the unit of work")

    assert kept.closed
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


def test_uncommitted_insert_is_discarded_by_close(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY)")
    cursor.connection.commit()

    writer = connect(conn_str)
    writer.cursor().execute(f"INSERT INTO dbo.{table_name} VALUES (7)")
    writer.close()

    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


def test_rows_fetched_earlier_keep_their_values_after_buffer_reuse(cursor):
    big = "x" * 9000
    cursor.execute("SELECT ? AS s, 1 AS n UNION ALL SELECT ?, 2", big, "short")
    first, second = cursor.fetchall()

    cursor.execute("SELECT REPLICATE(N'y', 5000) AS s, 3 AS n")
    cursor.fetchone()

    assert (first[0], first[1]) == (big, 1)
    assert (second[0], second[1]) == ("short", 2)


def test_rows_survive_cursor_and_connection_close(conn_str):
    connection = connect(conn_str, autocommit=True)
    cursor = connection.cursor()
    cursor.execute("SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    assert [tuple(r) for r in rows] == [(1,), (2,), (3,)]
    assert rows[-1][0] == 3


def test_autocommit_connection_has_no_open_transaction(conn_str):
    connection = connect(conn_str, autocommit=True)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT @@TRANCOUNT")
        assert cursor.fetchone()[0] == 0
    finally:
        connection.close()


def test_pooled_connection_does_not_inherit_session_set_options(conn_str, pooled):
    baseline_conn = connect(conn_str, autocommit=True)
    baseline_cur = baseline_conn.cursor()
    baseline_cur.execute("SELECT @@DATEFIRST")
    baseline = baseline_cur.fetchone()[0]
    baseline_conn.close()

    first = connect(conn_str, autocommit=True)
    first.cursor().execute("SET DATEFIRST 3")
    first.close()

    second = connect(conn_str, autocommit=True)
    try:
        cursor = second.cursor()
        cursor.execute("SELECT @@DATEFIRST")
        assert cursor.fetchone()[0] == baseline
    finally:
        second.close()


def test_pooled_connection_does_not_carry_open_transaction(conn_str, cursor, table_name, pooled):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY)")
    cursor.connection.commit()

    dirty = connect(conn_str)
    dirty.cursor().execute(f"INSERT INTO dbo.{table_name} VALUES (9)")
    dirty.close()

    clean = connect(conn_str, autocommit=True)
    try:
        probe = clean.cursor()
        probe.execute("SELECT @@TRANCOUNT")
        assert probe.fetchone()[0] == 0
        probe.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        assert probe.fetchone()[0] == 0
    finally:
        clean.close()


def test_sequential_pooled_connections_all_work(conn_str, pooled):
    results = []
    for i in range(20):
        connection = connect(conn_str, autocommit=True)
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT ?", i)
            results.append(cursor.fetchone()[0])
        finally:
            connection.close()

    assert results == list(range(20))


def test_parallel_checkouts_from_pool_do_not_interfere(conn_str, pooled):
    def worker(w):
        values = []
        for i in range(10):
            connection = connect(conn_str, autocommit=True)
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT ?, ?", w, i)
                values.append(tuple(cursor.fetchone()))
            finally:
                connection.close()
        return values

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(worker, range(8)))

    assert results == [[(w, i) for i in range(10)] for w in range(8)]


def test_use_of_closed_connection_is_rejected_after_pool_return(conn_str, pooled):
    connection = connect(conn_str, autocommit=True)
    connection.close()

    with pytest.raises(InterfaceError):
        connection.cursor()
