"""Dead connection handling: the server session is KILLed from a second
connection, then the first connection is used again.

Expected behavior (upstream ODBC driver): the next execute/commit/rollback
raises a clean OperationalError (no hang, no crash), the cursor and
connection objects stay usable as Python objects, a new connection works,
and the killed session's uncommitted work and locks are gone.

Ground truth is the upstream ODBC driver (.venv-upstream).
"""

import threading
import time

import pytest
from mssql_python import connect
from mssql_python.exceptions import (
    DatabaseError,
    Error,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

LINK_FAILURE = "Communication link failure"


# --- helpers -----------------------------------------------------------------


def _spid(conn):
    cur = conn.cursor()
    cur.execute("SELECT @@SPID")
    value = cur.fetchone()[0]
    cur.close()
    return int(value)


def _kill(conn_str, spid):
    """KILL from a separate autocommit connection (KILL is not allowed in a
    user transaction)."""
    killer = connect(conn_str, autocommit=True)
    try:
        killer.cursor().execute(f"KILL {int(spid)}")
    finally:
        killer.close()


def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def _scalar(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql) if params is None else cur.execute(sql, params)
        return cur.fetchone()[0]
    finally:
        cur.close()


@pytest.fixture
def victim(conn_str):
    """A connection that the test is allowed to kill. Closed at teardown,
    tolerating the error a dead connection raises on close()."""
    c = connect(conn_str)
    yield c
    _safe_close(c)


@pytest.fixture
def killed(conn_str, victim):
    """A victim connection whose server session has been KILLed."""
    spid = _spid(victim)
    _kill(conn_str, spid)
    return victim, spid


# --- next statement after the kill ------------------------------------------


def test_execute_after_kill_raises_operational_error(killed):
    conn, _ = killed
    cur = conn.cursor()
    with pytest.raises(OperationalError) as excinfo:
        cur.execute("SELECT 1")
    assert excinfo.value.driver_error == LINK_FAILURE
    assert excinfo.value.ddbc_error.startswith("[Microsoft]")


def test_execute_after_kill_error_is_database_error_hierarchy(killed):
    conn, _ = killed
    with pytest.raises(DatabaseError) as excinfo:
        conn.cursor().execute("SELECT 1")
    assert type(excinfo.value) is OperationalError
    assert isinstance(excinfo.value, Error)


def test_repeated_execute_after_kill_keeps_raising_operational_error(killed):
    conn, _ = killed
    cur = conn.cursor()
    for _ in range(3):
        with pytest.raises(OperationalError) as excinfo:
            cur.execute("SELECT 1")
        assert excinfo.value.driver_error == LINK_FAILURE


def test_commit_after_kill_raises_operational_error(killed):
    conn, _ = killed
    with pytest.raises(OperationalError) as excinfo:
        conn.commit()
    assert excinfo.value.driver_error == LINK_FAILURE


def test_rollback_after_kill_raises_operational_error(killed):
    conn, _ = killed
    with pytest.raises(OperationalError) as excinfo:
        conn.rollback()
    assert excinfo.value.driver_error == LINK_FAILURE


def test_new_cursor_on_dead_connection_is_created_but_cannot_execute(killed):
    conn, _ = killed
    cur = conn.cursor()
    assert cur is not None
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1")


def test_fetch_after_failed_execute_raises_programming_error(killed):
    conn, _ = killed
    cur = conn.cursor()
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1")
    # No stale rows: the failed statement produced no result set.
    with pytest.raises(ProgrammingError) as excinfo:
        cur.fetchone()
    assert excinfo.value.driver_error == "Function sequence error"


# --- connection.closed state -------------------------------------------------


def test_closed_flag_is_false_after_kill_before_any_close(killed):
    # The driver does not learn of the death until it talks to the server.
    conn, _ = killed
    assert conn.closed is False
    with pytest.raises(OperationalError):
        conn.cursor().execute("SELECT 1")
    assert conn.closed is False


def test_close_on_dead_connection_raises_and_still_sets_closed(killed):
    # Upstream deliberately raises here and its own suite pins it
    # (tests/upstream/test_006_exceptions.py::test_close_cleans_up_after_rollback_failure),
    # so this is the compatible behaviour. An earlier upstream_bug test that
    # required close() to stay silent contradicted those target tests and was
    # removed.
    conn, _ = killed
    with pytest.raises(OperationalError) as excinfo:
        conn.close()
    assert excinfo.value.driver_error == LINK_FAILURE
    assert conn.closed is True


def test_cursor_close_on_dead_connection_is_silent(killed):
    conn, _ = killed
    cur = conn.cursor()
    assert cur.close() is None


# --- cursor / connection use after an explicit (healthy) close ---------------


def test_execute_on_cursor_after_connection_close_raises_programming_error(conn_str):
    conn = connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    conn.close()
    assert conn.closed is True
    with pytest.raises(ProgrammingError) as excinfo:
        cur.execute("SELECT 1")
    assert excinfo.value.driver_error == "Operation cannot be performed: The cursor is closed."


def test_new_cursor_after_connection_close_raises_interface_error(conn_str):
    conn = connect(conn_str)
    conn.close()
    with pytest.raises(InterfaceError) as excinfo:
        conn.cursor()
    assert excinfo.value.driver_error == "Cannot create cursor on closed connection"


def test_commit_after_connection_close_raises_interface_error(conn_str):
    conn = connect(conn_str)
    conn.close()
    with pytest.raises(InterfaceError) as excinfo:
        conn.commit()
    assert excinfo.value.driver_error == "Cannot commit on a closed connection"


def test_second_close_on_healthy_connection_is_silent(conn_str):
    conn = connect(conn_str)
    conn.close()
    assert conn.close() is None
    assert conn.closed is True


def test_cursor_close_after_connection_close_is_silent(conn_str):
    conn = connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    conn.close()
    assert cur.close() is None


# --- state the killed session leaves behind ----------------------------------


def test_kill_rolls_back_uncommitted_insert(conn, cursor, table_name, conn_str, victim):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, v NVARCHAR(20))")
    conn.commit()

    spid = _spid(victim)
    vc = victim.cursor()
    vc.execute(f"INSERT INTO dbo.{table_name} VALUES (1, N'uncommitted')")
    _kill(conn_str, spid)

    assert _scalar(conn, f"SELECT COUNT(*) FROM dbo.{table_name}") == 0


def test_kill_releases_row_locks_and_restores_committed_value(
    conn, cursor, table_name, conn_str, victim
):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1, N'seed')")
    conn.commit()

    spid = _spid(victim)
    vc = victim.cursor()
    vc.execute(f"UPDATE dbo.{table_name} SET v = N'pending' WHERE id = 1")
    _kill(conn_str, spid)

    # A new session must not block on the dead session's lock, and must see
    # the last committed value, not the killed session's pending change.
    fresh = connect(conn_str)
    try:
        fresh.cursor().execute("SET LOCK_TIMEOUT 5000")
        assert _scalar(fresh, f"SELECT v FROM dbo.{table_name} WHERE id = 1") == "seed"
        upd = fresh.cursor()
        upd.execute(f"UPDATE dbo.{table_name} SET v = N'after' WHERE id = 1")
        assert upd.rowcount == 1
        fresh.commit()
        assert _scalar(fresh, f"SELECT v FROM dbo.{table_name} WHERE id = 1") == "after"
    finally:
        _safe_close(fresh)


def test_temp_table_of_killed_session_is_not_visible_to_new_connection(conn_str, victim):
    vc = victim.cursor()
    vc.execute("CREATE TABLE #dead_session_tmp (x INT)")
    vc.execute("INSERT INTO #dead_session_tmp VALUES (7)")
    victim.commit()
    spid = _spid(victim)
    _kill(conn_str, spid)

    fresh = connect(conn_str)
    try:
        with pytest.raises(ProgrammingError):
            fresh.cursor().execute("SELECT x FROM #dead_session_tmp")
    finally:
        _safe_close(fresh)


# --- a new connection after the kill ----------------------------------------


def test_new_connection_works_after_kill_and_gets_new_session(conn_str, killed):
    _, dead_spid = killed
    fresh = connect(conn_str)
    try:
        assert _spid(fresh) != dead_spid
        assert _scalar(fresh, "SELECT 1") == 1
    finally:
        _safe_close(fresh)


def test_new_connection_can_run_dml_and_commit_after_kill(conn_str, killed, table_name):
    fresh = connect(conn_str)
    try:
        cur = fresh.cursor()
        cur.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL, v NVARCHAR(10))")
        cur.execute(f"INSERT INTO dbo.{table_name} VALUES (?, ?)", (42, "ok"))
        fresh.commit()
        assert _scalar(fresh, f"SELECT COUNT(*) FROM dbo.{table_name} WHERE id = ?", (42,)) == 1
    finally:
        _safe_close(fresh)


# --- in-flight kill: the statement is running when its session dies ---------


def test_kill_during_running_statement_raises_operational_error_without_hang(conn_str):
    victim = connect(conn_str)
    try:
        spid = _spid(victim)
        cur = victim.cursor()
        outcome = {}

        def run():
            started = time.monotonic()
            try:
                cur.execute("WAITFOR DELAY '00:00:30'; SELECT 1")
                outcome["error"] = None
            except Exception as exc:  # captured for the assertions below
                outcome["error"] = exc
            outcome["elapsed"] = time.monotonic() - started

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        time.sleep(2)
        _kill(conn_str, spid)
        worker.join(timeout=30)

        assert not worker.is_alive(), "execute() hung after the session was killed"
        assert outcome["elapsed"] < 20
        exc = outcome["error"]
        assert type(exc) is OperationalError
        # The server's "terminated" error token usually arrives first (HY000); on
        # Windows the socket reset can win the race and surface as 08S01.
        if exc.driver_error == "Communication link failure":
            assert exc.sqlstate == "08S01"
        else:
            assert exc.driver_error == "General error"
            assert "Connection may have been terminated by the server" in exc.ddbc_error
    finally:
        _safe_close(victim)
