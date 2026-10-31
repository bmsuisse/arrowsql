"""Resource lifecycle: execute/fetch loops, unclosed cursors, many open cursors,
cursor reuse after errors, connection close with open cursors, and server
session release on connection teardown.

Native-TDS rewrite checks. Everything here runs on the upstream ODBC driver as
ground truth; a native implementation must reproduce the same values, types and
exception classes, and must not grow memory or leak server sessions.

Research (issue trackers and docs, checked for this suite):
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/cursor-management?view=sql-server-ver17
  (cursors that are not closed hold server-side resources until the connection
  closes; use fetchmany for large results; one active result per connection)
- https://github.com/microsoft/mssql-python/issues/131 (segfault at interpreter
  exit when cursors are alive during Connection/Cursor teardown)
- https://github.com/microsoft/mssql-python/pull/412 (GC-time cleanup changed to
  leak handles rather than crash; _connection_closed guard)
- https://github.com/microsoft/mssql-python/pull/189 (Connection.execute creates
  cursors that must not accumulate; cursor tracking via WeakSet)
- https://github.com/mkleehammer/pyodbc/issues/402 (fetch loops whose memory
  grows until swap; per-fetch growth is a driver bug, not expected behaviour)
- https://github.com/mkleehammer/pyodbc/issues/854 (memory not released after
  connection close with fast_executemany; close must free per-connection state)
- https://github.com/mkleehammer/pyodbc/issues/360 (fetchall silently capped
  at 100k rows in some builds; row counts must be exact)
- https://github.com/tiberius-rs/tiberius/pull/451 (dropped/cancelled requests
  must not leave a connection that is reused in a poisoned state)
- https://github.com/lokander/hubro/pull/157 (a connection whose request was
  cancelled must be closed, not returned to a pool)
"""

import os
import gc
import time

import pytest

import mssql_python as mssql
from mssql_python import connect
from mssql_python.exceptions import InterfaceError, IntegrityError, ProgrammingError

psutil = pytest.importorskip("psutil")


def _rss_mb():
    gc.collect()
    return psutil.Process().memory_info().rss / (1024 * 1024)


def _session_row_count(monitor_cursor, session_id):
    monitor_cursor.execute(
        "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = ?",
        session_id,
    )
    return monitor_cursor.fetchone()[0]


def _wait_session_gone(monitor_cursor, session_id, timeout=10.0):
    deadline = time.monotonic() + timeout
    while True:
        if _session_row_count(monitor_cursor, session_id) == 0:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.25)


@pytest.fixture
def pooling_off():
    """Disable driver connection pooling so closed/collected connections really
    disconnect. Restores pooling on teardown."""
    mssql.pooling(enabled=False)
    try:
        yield
    finally:
        mssql.pooling(enabled=True)


@pytest.fixture
def monitor(conn_str):
    mon = connect(conn_str)
    cur = mon.cursor()
    yield cur
    cur.close()
    mon.close()


def test_execute_fetchone_5000_loop_rss_stable(conn):
    cur = conn.cursor()
    payload = "x" * 2000
    for i in range(200):  # warm-up so allocator pools settle
        cur.execute("SELECT ? AS n, ? AS s", i, payload)
        cur.fetchone()
    base = _rss_mb()

    for i in range(5000):
        cur.execute("SELECT ? AS n, ? AS s", i, payload)
        row = cur.fetchone()
        assert row[0] == i
        assert type(row[0]) is int
        assert row[1] == payload
    assert cur.fetchone() is None
    growth = _rss_mb() - base
    cur.close()
    assert growth < 25, f"RSS grew {growth:.1f} MB over 5000 execute/fetch iterations"


def test_execute_fetchall_5000_loop_exact_results(conn):
    cur = conn.cursor()
    base = _rss_mb()
    for i in range(5000):
        cur.execute("SELECT ?", i)
        rows = cur.fetchall()
        assert [tuple(r) for r in rows] == [(i,)]
    growth = _rss_mb() - base
    cur.close()
    assert growth < 25, f"RSS grew {growth:.1f} MB over 5000 execute/fetchall iterations"


def test_unclosed_cursors_collected_leave_connection_usable(conn):
    base = _rss_mb()
    for i in range(2000):
        c = conn.cursor()
        c.execute("SELECT ?", i)
        assert tuple(c.fetchone()) == (i,)
        del c  # never closed explicitly
    growth = _rss_mb() - base
    assert growth < 20, f"RSS grew {growth:.1f} MB after 2000 unclosed cursors"

    cur = conn.cursor()
    cur.execute("SELECT 42")
    assert tuple(cur.fetchone()) == (42,)
    cur.close()


def test_200_open_cursors_on_one_connection(conn):
    cursors = [conn.cursor() for _ in range(200)]
    try:
        for i, c in enumerate(cursors):
            c.execute("SELECT ? AS idx", i)
            assert [tuple(r) for r in c.fetchall()] == [(i,)]
        # Each cursor still independently usable after all others ran.
        for i in reversed(range(200)):
            cursors[i].execute("SELECT ? * 2", i)
            assert tuple(cursors[i].fetchone()) == (i * 2,)
    finally:
        for c in cursors:
            c.close()


def test_partially_fetched_cursor_reused_for_new_query(conn):
    cur = conn.cursor()
    cur.execute("SELECT v FROM (VALUES (1), (2), (3)) t(v) ORDER BY v")
    assert tuple(cur.fetchone()) == (1,)
    # Abandon the rest of the first result set and reuse the same cursor.
    cur.execute("SELECT ? AS fresh", "after")
    assert [tuple(r) for r in cur.fetchall()] == [("after",)]
    cur.close()


def test_cursor_reuse_after_syntax_error(conn):
    cur = conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.execute("SELEC 1 FRO nowhere")
    cur.execute("SELECT ?", 7)
    assert tuple(cur.fetchone()) == (7,)
    cur.close()


def test_cursor_reuse_after_integrity_error(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE #dup_guard (id INT PRIMARY KEY)")
    cur.execute("INSERT INTO #dup_guard VALUES (1)")
    with pytest.raises(IntegrityError):
        cur.execute("INSERT INTO #dup_guard VALUES (1)")
    cur.execute("SELECT COUNT(*) FROM #dup_guard")
    assert tuple(cur.fetchone()) == (1,)
    cur.close()


def test_pending_results_discarded_by_error_then_reuse(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1 AS a UNION ALL SELECT 2")
    assert tuple(cur.fetchone()) == (1,)
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT * FROM definitely_missing_table_zz")
    cur.execute("SELECT 'ok' AS s")
    assert [tuple(r) for r in cur.fetchall()] == [("ok",)]
    cur.close()


def test_closed_cursor_execute_raises_programming_error(conn):
    cur = conn.cursor()
    cur.close()
    with pytest.raises(ProgrammingError, match="closed"):
        cur.execute("SELECT 1")


def test_cursor_double_close_is_noop(conn):
    cur = conn.cursor()
    cur.close()
    cur.close()  # must not raise
    assert tuple(conn.cursor().execute("SELECT 5").fetchone()) == (5,)


def test_connection_double_close_is_noop(conn_str):
    c = connect(conn_str)
    c.close()
    c.close()  # must not raise


def test_cursor_context_manager_closes_cursor(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 9")
        assert tuple(cur.fetchone()) == (9,)
    with pytest.raises(ProgrammingError, match="closed"):
        cur.execute("SELECT 1")


def test_fetch_after_exhaustion_returns_empty(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1 WHERE 1 = 0")
    assert cur.fetchone() is None
    assert cur.fetchone() is None
    assert [tuple(r) for r in cur.fetchall()] == []
    assert cur.fetchmany(5) == []
    cur.close()


def test_connection_close_with_open_unfetched_cursor(conn_str):
    c = connect(conn_str)
    cur = c.cursor()
    cur.execute("SELECT v FROM (VALUES (1), (2), (3)) t(v)")
    c.close()
    with pytest.raises(ProgrammingError, match="closed"):
        cur.fetchone()
    with pytest.raises(InterfaceError):
        c.cursor()


def test_explicit_close_releases_server_session(pooling_off, conn_str, monitor):
    c = connect(conn_str)
    cur = c.cursor()
    cur.execute("SELECT @@SPID")
    spid = cur.fetchone()[0]
    cur.close()
    c.close()
    assert _wait_session_gone(monitor, spid), f"session {spid} still alive after close()"


def test_del_connection_without_close_releases_server_session(pooling_off, conn_str, monitor):
    def _open_and_forget():
        c = connect(conn_str)
        cur = c.cursor()
        cur.execute("SELECT @@SPID")
        return cur.fetchone()[0]

    spid = _open_and_forget()
    gc.collect()
    assert _wait_session_gone(monitor, spid), (
        f"session {spid} still alive after its connection was dropped without close()"
    )


def test_del_connection_with_open_cursor_releases_server_session(pooling_off, conn_str, monitor):
    c = connect(conn_str)
    cur = c.cursor()
    cur.execute("SELECT @@SPID")
    spid = cur.fetchone()[0]
    cur.execute("SELECT v FROM (VALUES (1), (2)) t(v)")  # left unfetched
    del cur
    del c
    gc.collect()
    assert _wait_session_gone(monitor, spid), (
        f"session {spid} survived del of connection with an unfetched open cursor"
    )


def test_500_connect_close_cycles_no_session_or_rss_growth(pooling_off, conn_str, monitor):
    # Only this process's sessions: other test runs share the server.
    def _count_user_sessions():
        monitor.execute(
            "SELECT COUNT(*) FROM sys.dm_exec_sessions "
            "WHERE is_user_process = 1 AND host_process_id = ?",
            (os.getpid(),),
        )
        return monitor.fetchone()[0]

    base_sessions = _count_user_sessions()
    base_rss = _rss_mb()
    for _ in range(500):
        c = connect(conn_str)
        cur = c.cursor()
        cur.execute("SELECT 1")
        assert tuple(cur.fetchone()) == (1,)
        cur.close()
        c.close()
    growth = _rss_mb() - base_rss
    assert growth < 25, f"RSS grew {growth:.1f} MB over 500 connect/close cycles"
    assert _count_user_sessions() == base_sessions
