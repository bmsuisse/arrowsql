"""Cursor.cancel() and Ctrl-C during execute / fetch (Rust port).

pyodbc's ``Cursor.cancel()`` is safe from another thread and a no-op when idle.
A KeyboardInterrupt raised while a statement waits for the server cancels it
there and is raised promptly, leaving the connection usable.
"""

import os
import signal
import threading
import time

import pytest

import mssql_python
from mssql_python import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False), reason="Rust port only"
)

LONG_WAIT = "WAITFOR DELAY '00:00:30'"
# Billions of rows: a fetchall that runs until it is cancelled.
HUGE_RESULT = (
    "SELECT a.object_id, b.object_id AS b FROM sys.all_objects a "
    "CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c"
)


def _later(delay, fn):
    timer = threading.Timer(delay, fn)
    timer.start()
    return timer


def _assert_usable(conn):
    cur = conn.cursor()
    cur.execute("SELECT 42")
    assert cur.fetchone()[0] == 42
    cur.close()


def test_cancel_when_idle_is_a_noop(cursor):
    cursor.cancel()
    cursor.execute("SELECT 1")
    cursor.cancel()
    assert cursor.fetchone()[0] == 1
    cursor.cancel()
    assert cursor.fetchall() == []


def test_cancel_on_closed_cursor_is_a_noop(conn):
    cur = conn.cursor()
    cur.close()
    cur.cancel()


def test_cancel_from_another_thread_during_execute(conn):
    cur = conn.cursor()
    timer = _later(0.5, cur.cancel)
    start = time.monotonic()
    with pytest.raises(mssql_python.Error):
        cur.execute(LONG_WAIT)
    timer.join()
    assert time.monotonic() - start < 10
    _assert_usable(conn)
    # the same cursor runs again
    cur.execute("SELECT 7")
    assert cur.fetchone()[0] == 7


def test_cancel_from_another_thread_during_fetch(conn):
    cur = conn.cursor()
    cur.execute(HUGE_RESULT)
    timer = _later(0.5, cur.cancel)
    start = time.monotonic()
    with pytest.raises(mssql_python.Error):
        cur.fetchall()
    timer.join()
    assert time.monotonic() - start < 10
    _assert_usable(conn)


def _sigint_after(delay):
    return _later(delay, lambda: os.kill(os.getpid(), signal.SIGINT))


@pytest.fixture
def default_sigint():
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    yield
    signal.signal(signal.SIGINT, previous)


@pytest.mark.skipif(os.name == "nt", reason="SIGINT delivery differs on Windows")
def test_sigint_interrupts_execute(conn, default_sigint):
    cur = conn.cursor()
    timer = _sigint_after(0.5)
    start = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        cur.execute(LONG_WAIT)
    timer.join()
    assert time.monotonic() - start < 5
    _assert_usable(conn)
    cur.execute("SELECT 5")
    assert cur.fetchone()[0] == 5


@pytest.mark.skipif(os.name == "nt", reason="SIGINT delivery differs on Windows")
def test_sigint_interrupts_fetchall(conn, default_sigint):
    cur = conn.cursor()
    cur.execute(HUGE_RESULT)
    timer = _sigint_after(0.5)
    start = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        cur.fetchall()
    timer.join()
    assert time.monotonic() - start < 5
    _assert_usable(conn)


@pytest.mark.skipif(os.name == "nt", reason="SIGINT delivery differs on Windows")
def test_sigint_twice_then_reuse(conn, default_sigint):
    cur = conn.cursor()
    for _ in range(2):
        timer = _sigint_after(0.3)
        with pytest.raises(KeyboardInterrupt):
            cur.execute(LONG_WAIT)
        timer.join()
    _assert_usable(conn)


@pytest.mark.skipif(os.name == "nt", reason="SIGINT delivery differs on Windows")
def test_signal_handler_that_returns_does_not_cancel(conn):
    seen = []
    previous = signal.signal(signal.SIGINT, lambda *_: seen.append(1))
    try:
        cur = conn.cursor()
        timer = _sigint_after(0.3)
        cur.execute("WAITFOR DELAY '00:00:01'; SELECT 3")
        timer.join()
        assert cur.fetchone()[0] == 3
        assert seen
    finally:
        signal.signal(signal.SIGINT, previous)
