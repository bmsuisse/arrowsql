"""Query timeouts, WAITFOR interruption, and cross-thread cancel.

Covers the common driver pain points:

* ``Connection.timeout`` validation and default
* cursors snapshot the query timeout when they are created
* a timed-out statement raises ``OperationalError`` and leaves the
  connection and cursor usable
* a timeout does not roll back an open transaction by itself
* timeouts stay isolated per connection / thread
* cancelling a running statement from another thread

Upstream has no public ``Cursor.cancel()``. Cross-thread cancel is exercised
through ``cursor.close()`` from a second thread, which the upstream driver
gets wrong (it segfaults or blocks until the statement finishes). That test
runs in a subprocess so a native crash cannot take down the pytest process.
"""

import json
import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest
from mssql_python import DatabaseError, OperationalError, ProgrammingError, connect

TIMEOUT_MESSAGE_FRAGMENT = "timeout expired"


def _rows(cursor):
    """Fetch all rows as plain tuples (driver Row objects are not tuples)."""
    return [tuple(row) for row in cursor.fetchall()]


def _elapsed_wait(cursor, sql):
    """Run ``sql`` and return (exception_or_None, elapsed_seconds)."""
    started = time.monotonic()
    try:
        cursor.execute(sql)
    except Exception as exc:  # noqa: BLE001 - the caller asserts the exact class
        return exc, time.monotonic() - started
    return None, time.monotonic() - started


# ---------------------------------------------------------------------------
# Connection.timeout attribute
# ---------------------------------------------------------------------------


def test_connection_timeout_defaults_to_zero(conn):
    assert conn.timeout == 0
    assert type(conn.timeout) is int


def test_connection_timeout_roundtrip(conn):
    conn.timeout = 5
    assert conn.timeout == 5
    assert type(conn.timeout) is int
    conn.timeout = 0
    assert conn.timeout == 0


def test_connection_timeout_rejects_negative(conn):
    with pytest.raises(ValueError, match="cannot be negative"):
        conn.timeout = -1
    assert conn.timeout == 0


@pytest.mark.parametrize("bad_value", ["5", 2.0, True])
def test_connection_timeout_rejects_non_integer(conn, bad_value):
    with pytest.raises(TypeError, match="must be an integer"):
        conn.timeout = bad_value
    assert conn.timeout == 0


def test_login_timeout_kwarg_does_not_set_query_timeout(conn_str):
    conn = connect(conn_str, timeout=7)
    try:
        # connect(timeout=...) is the login timeout; the query timeout stays off.
        assert conn.timeout == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Cursor timeout inheritance
# ---------------------------------------------------------------------------


def test_cursor_created_after_timeout_inherits_it(conn):
    conn.timeout = 2
    cursor = conn.cursor()
    conn.timeout = 0  # changing the connection afterwards must not affect the cursor
    exc, elapsed = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:10'")
    assert isinstance(exc, OperationalError)
    assert TIMEOUT_MESSAGE_FRAGMENT in str(exc).lower()
    assert 1.5 <= elapsed < 8.0


def test_cursor_created_before_timeout_keeps_no_timeout(conn):
    conn.timeout = 0
    cursor = conn.cursor()
    conn.timeout = 2  # set after the cursor exists; cursor snapshot stays at 0
    exc, elapsed = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:3'")
    assert exc is None
    assert 2.5 <= elapsed < 10.0


def test_sibling_cursors_have_independent_timeouts(conn):
    conn.timeout = 2
    timed = conn.cursor()
    conn.timeout = 0
    untimed = conn.cursor()

    exc, _ = _elapsed_wait(timed, "WAITFOR DELAY '00:00:10'")
    assert isinstance(exc, OperationalError)

    exc, elapsed = _elapsed_wait(untimed, "WAITFOR DELAY '00:00:3'")
    assert exc is None
    assert elapsed >= 2.5


# ---------------------------------------------------------------------------
# WAITFOR DELAY interrupted by the query timeout
# ---------------------------------------------------------------------------


def test_waitfor_interrupted_raises_operational_error(conn):
    conn.timeout = 2
    cursor = conn.cursor()
    exc, elapsed = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:10'")
    assert type(exc) is OperationalError
    assert isinstance(exc, DatabaseError)
    assert TIMEOUT_MESSAGE_FRAGMENT in str(exc).lower()
    assert 1.5 <= elapsed < 8.0


def test_waitfor_shorter_than_timeout_completes_with_no_rows(conn):
    conn.timeout = 5
    cursor = conn.cursor()
    exc, elapsed = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:01'")
    assert exc is None
    assert elapsed >= 0.9
    # WAITFOR produces no result set, so there is no description to fetch from.
    assert cursor.description is None


def test_timeout_zero_means_no_timeout(conn):
    conn.timeout = 0
    cursor = conn.cursor()
    exc, elapsed = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:2'")
    assert exc is None
    assert elapsed >= 1.9


def test_timeout_reset_to_zero_allows_long_wait(conn):
    conn.timeout = 1
    timed = conn.cursor()
    exc, _ = _elapsed_wait(timed, "WAITFOR DELAY '00:00:5'")
    assert isinstance(exc, OperationalError)

    conn.timeout = 0
    untimed = conn.cursor()
    exc, elapsed = _elapsed_wait(untimed, "WAITFOR DELAY '00:00:2'")
    assert exc is None
    assert elapsed >= 1.9


def test_repeated_timeouts_on_same_cursor_each_raise(conn):
    conn.timeout = 1
    cursor = conn.cursor()
    for _ in range(2):
        exc, elapsed = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:5'")
        assert type(exc) is OperationalError
        assert 0.8 <= elapsed < 5.0


def test_non_timeout_error_is_programming_error_and_connection_survives(conn):
    conn.timeout = 5
    cursor = conn.cursor()
    with pytest.raises(ProgrammingError, match="Invalid object name"):
        cursor.execute("SELECT * FROM dbo.ci_no_such_table_timeouts")
    cursor.execute("SELECT ? AS v", (42,))
    assert _rows(cursor) == [(42,)]


# ---------------------------------------------------------------------------
# Connection and cursor usability after a timeout
# ---------------------------------------------------------------------------


def test_same_cursor_usable_after_timeout(conn):
    conn.timeout = 1
    cursor = conn.cursor()
    exc, _ = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:5'")
    assert isinstance(exc, OperationalError)

    cursor.execute("SELECT ? AS v", (7,))
    assert _rows(cursor) == [(7,)]


def test_new_cursor_usable_after_timeout(conn):
    conn.timeout = 1
    cursor = conn.cursor()
    exc, _ = _elapsed_wait(cursor, "WAITFOR DELAY '00:00:5'")
    assert isinstance(exc, OperationalError)

    conn.timeout = 0
    fresh = conn.cursor()
    fresh.execute("SELECT 1 AS one")
    assert tuple(fresh.fetchone()) == (1,)
    assert conn.closed is False


def test_uncommitted_work_survives_timeout_until_rollback(conn, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v INT NOT NULL)")
    conn.commit()

    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (1)")
    conn.timeout = 1
    timed = conn.cursor()
    exc, _ = _elapsed_wait(timed, "WAITFOR DELAY '00:00:5'")
    assert isinstance(exc, OperationalError)

    # A query timeout does not abort the transaction on its own: the insert is
    # still visible to this connection.
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (1,)

    conn.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (0,)


def test_commit_after_timeout_keeps_work_done_before_timeout(conn, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v INT NOT NULL)")
    conn.commit()

    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (10)")
    conn.timeout = 1
    timed = conn.cursor()
    exc, _ = _elapsed_wait(timed, "WAITFOR DELAY '00:00:5'")
    assert isinstance(exc, OperationalError)

    conn.commit()
    cursor.execute(f"SELECT v FROM dbo.{table_name}")
    assert _rows(cursor) == [(10,)]


# ---------------------------------------------------------------------------
# Thread isolation
# ---------------------------------------------------------------------------


def test_timeout_in_worker_thread_does_not_affect_main_thread(conn_str):
    worker_conn = connect(conn_str)
    main_conn = connect(conn_str)
    try:
        worker_conn.timeout = 1
        main_conn.timeout = 0
        worker_cursor = worker_conn.cursor()
        main_cursor = main_conn.cursor()
        worker_result = {}

        def worker():
            exc, elapsed = _elapsed_wait(worker_cursor, "WAITFOR DELAY '00:00:5'")
            worker_result["exc"] = exc
            worker_result["elapsed"] = elapsed

        thread = threading.Thread(target=worker)
        thread.start()
        exc, elapsed = _elapsed_wait(main_cursor, "WAITFOR DELAY '00:00:2'")
        thread.join(30)

        assert not thread.is_alive()
        assert type(worker_result["exc"]) is OperationalError
        assert worker_result["elapsed"] < 5.0
        assert exc is None
        assert elapsed >= 1.9
    finally:
        worker_conn.close()
        main_conn.close()


# ---------------------------------------------------------------------------
# Cancel from another thread
# ---------------------------------------------------------------------------

_CROSS_THREAD_CANCEL_SCRIPT = textwrap.dedent(
    """
    import os
    import json
    import threading
    import time

    from mssql_python import connect

    conn = connect(os.environ["DB_CONNECTION_STRING"])
    cursor = conn.cursor()
    result = {}

    def worker():
        started = time.monotonic()
        try:
            cursor.execute("WAITFOR DELAY '00:00:08'")
            result["outcome"] = "completed"
        except Exception as exc:
            result["outcome"] = type(exc).__name__
        result["elapsed"] = round(time.monotonic() - started, 2)

    thread = threading.Thread(target=worker)
    thread.start()
    time.sleep(1.0)
    cursor.close()
    thread.join(20)
    print("RESULT " + json.dumps(result), flush=True)
    """
)


@pytest.mark.upstream_bug(
    reason=(
        "upstream has no public Cursor.cancel(); cursor.close() from another thread "
        "while a statement runs segfaults or blocks until the statement finishes "
        "instead of interrupting it"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_cross_thread_cancel_interrupts_running_statement(conn_str):
    # Run in a subprocess: the upstream crash is a native SIGSEGV that would
    # otherwise kill the whole pytest process.
    env = dict(os.environ, DB_CONNECTION_STRING=conn_str)
    proc = subprocess.run(
        [sys.executable, "-c", _CROSS_THREAD_CANCEL_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, f"child exited {proc.returncode}: {proc.stderr[-300:]}"
    line = next(
        (ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")),
        None,
    )
    assert line is not None, proc.stdout
    result = json.loads(line[len("RESULT ") :])
    assert result["outcome"] == "OperationalError"
    assert result["elapsed"] < 6.0


def test_closing_cursor_with_unfinished_batch_does_not_wait_for_it(conn_str):
    # The first result is fetched completely; the batch still has a long
    # WAITFOR to go. Closing the cursor abandons it and must not wait it out.
    import time

    from mssql_python import connect

    c = connect(conn_str)
    try:
        c.timeout = 0
        cur = c.cursor()
        # NOWAIT flushes the first result to the client before the server waits.
        cur.execute("SELECT 1; RAISERROR('flush', 0, 1) WITH NOWAIT; WAITFOR DELAY '00:00:30'; SELECT 2")
        assert cur.fetchall()[0][0] == 1
        started = time.monotonic()
        cur.close()
        assert time.monotonic() - started < 5
        cur2 = c.cursor()
        cur2.execute("SELECT 3")
        assert cur2.fetchone()[0] == 3
    finally:
        c.close()
