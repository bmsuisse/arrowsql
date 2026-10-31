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


@pytest.mark.skipif(os.name == "nt", reason="SIGINT delivery differs on Windows")
def test_signal_handler_may_use_the_cursor_it_interrupted(conn):
    """A handler runs while the interrupted statement holds the connection, so
    a driver call that needs it fails fast instead of deadlocking."""
    cur = conn.cursor()
    refused = []

    def handler(*_):
        try:
            cur.close()
        except BaseException as exc:  # noqa: BLE001 - a PanicException derives from BaseException
            refused.append(str(exc))
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, handler)
    try:
        timer = _sigint_after(0.3)
        # A deadlock would hang the run: a watchdog ends the process instead.
        watchdog = _later(15, lambda: os._exit(70))
        start = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            cur.execute(LONG_WAIT)
        watchdog.cancel()
        timer.join()
        assert time.monotonic() - start < 8
    finally:
        signal.signal(signal.SIGINT, previous)
    assert refused and "signal handler" in refused[0]
    _assert_usable(conn)


@pytest.mark.skipif(os.name == "nt", reason="SIGINT delivery differs on Windows")
def test_other_thread_can_read_cursor_metadata_while_main_waits(conn):
    """arrow_stream() / description from a second thread while the main thread
    waits in execute must not deadlock on the statement locks and the GIL."""
    pytest.importorskip("pyarrow")
    cur = conn.cursor()
    cur.execute("SELECT 1 AS a")
    cur.fetchall()
    results = []

    def probe():
        time.sleep(0.5)
        for _ in range(50):
            try:
                cur.description  # noqa: B018
                cur.arrow_stream().close()
            except Exception as exc:  # noqa: BLE001 - any answer is fine, a hang is not
                results.append(type(exc).__name__)
            time.sleep(0.02)
        results.append("done")

    thread = threading.Thread(target=probe, daemon=True)
    thread.start()
    watchdog = _later(20, lambda: os._exit(71))
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        # Only the execute is raced: the probe's streams discard the result they
        # export, so the main thread must not fetch from it afterwards.
        cur.execute("WAITFOR DELAY '00:00:03'; SELECT 7")
    finally:
        signal.signal(signal.SIGINT, previous)
        watchdog.cancel()
    thread.join(10)
    assert "done" in results


def test_cancel_during_fetchall_lands_at_any_point(conn):
    """One cancel at a random moment of a fetchall. It may land between two
    batches, while rows are converted and no lock is held: it must still stop
    the next batch instead of being dropped as a cancel of an idle cursor."""
    import random

    rng = random.Random(7)
    for _ in range(25):
        cur = conn.cursor()
        cur.execute(HUGE_RESULT)
        # A hung fetchall would never return; end the run instead of the suite.
        watchdog = _later(60, lambda: os._exit(72))
        timer = _later(rng.uniform(0.1, 0.5), cur.cancel)
        with pytest.raises(mssql_python.Error):
            cur.fetchall()
        timer.join()
        watchdog.cancel()
        cur.close()
    _assert_usable(conn)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_ctrl_c_works_in_a_child_forked_from_a_worker_thread(conn_str):
    """A worker thread that already ran a query cached 'not the main thread';
    after fork() it is the child's main thread and must be watched again."""
    outcome = {}

    def worker():
        conn = mssql_python.connect(conn_str)
        conn.cursor().execute("WAITFOR DELAY '00:00:00.3'")  # long enough for a signal check: caches the role
        conn.close()
        pid = os.fork()
        if pid == 0:
            code = 1
            try:
                signal.signal(signal.SIGINT, signal.default_int_handler)
                child = mssql_python.connect(conn_str)
                _sigint_after(0.5)
                start = time.monotonic()
                try:
                    child.cursor().execute(LONG_WAIT)
                except KeyboardInterrupt:
                    code = 0 if time.monotonic() - start < 10 else 2
            finally:
                os._exit(code)
        outcome["status"] = os.waitpid(pid, 0)[1]

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(60)
    assert not thread.is_alive()
    assert os.waitstatus_to_exitcode(outcome["status"]) == 0


def test_rollback_after_closing_a_stream_early(conn):
    """Closing an Arrow stream cancels the execution it reads; a commit or
    rollback afterwards must not inherit that cancellation (it did, about one
    time in a hundred, as "Operation canceled")."""
    pytest.importorskip("pyarrow")
    for _ in range(300):
        cur = conn.cursor()
        cur.execute("SELECT 7 AS a UNION ALL SELECT 8")
        cur.arrow_stream().close()
        conn.rollback()
        conn.commit()
