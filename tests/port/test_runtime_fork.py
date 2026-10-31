"""Fork safety of the process-wide Rust runtime (src/runtime.rs).

A child forked after the parent has already run queries must be able to open
its own connections and query, and the parent must keep working afterwards.
Before the pid check, the child inherited a runtime with no worker threads and
the first query in it hung.
"""

import os
import traceback

import pytest

from mssql_python import connect

pytestmark = [
    pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX-only"),
    pytest.mark.skipif(
        not os.getenv("DB_CONNECTION_STRING"), reason="Database connection string not provided"
    ),
]


def _select_one(conn_str):
    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        value = cursor.fetchone()[0]
        cursor.close()
        return value
    finally:
        conn.close()


def _run_in_child(conn_str):
    """Query from a forked child and report success through the exit code."""
    code = 1
    try:
        code = 0 if _select_one(conn_str) == 1 else 1
    except BaseException:
        traceback.print_exc()
    finally:
        os._exit(code)


def test_query_in_child_and_parent_after_fork():
    conn_str = os.environ["DB_CONNECTION_STRING"]

    # Build the runtime and keep one connection open across the fork.
    assert _select_one(conn_str) == 1
    parent_conn = connect(conn_str)

    pid = os.fork()
    if pid == 0:
        _run_in_child(conn_str)

    _, wait_status = os.waitpid(pid, 0)
    assert os.WIFEXITED(wait_status), "forked child did not exit normally"
    assert os.WEXITSTATUS(wait_status) == 0, "query in forked child failed"

    cursor = parent_conn.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
    cursor.close()
    parent_conn.close()

    assert _select_one(conn_str) == 1


def test_several_children_forked_from_one_parent():
    conn_str = os.environ["DB_CONNECTION_STRING"]
    assert _select_one(conn_str) == 1

    pids = []
    for _ in range(2):
        pid = os.fork()
        if pid == 0:
            _run_in_child(conn_str)
        pids.append(pid)

    for pid in pids:
        _, wait_status = os.waitpid(pid, 0)
        assert os.WIFEXITED(wait_status) and os.WEXITSTATUS(wait_status) == 0

    assert _select_one(conn_str) == 1


def test_fork_while_other_threads_use_the_runtime():
    # Threads running statements with a timeout call runtime() (cancellation
    # timer) in a loop. A child forked while one holds the runtime lock must not
    # inherit it locked.
    import signal
    import threading
    import time

    conn_str = os.environ["DB_CONNECTION_STRING"]
    assert _select_one(conn_str) == 1
    stop = threading.Event()

    def hammer():
        conn = connect(conn_str)
        conn.timeout = 30
        cursor = conn.cursor()
        while not stop.is_set():
            cursor.execute("SELECT 1")
            cursor.fetchall()
        conn.close()

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for thread in threads:
        thread.start()
    hung = 0
    try:
        for _ in range(40):
            pid = os.fork()
            if pid == 0:
                _run_in_child(conn_str)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                done, status = os.waitpid(pid, os.WNOHANG)
                if done:
                    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
                    break
                time.sleep(0.01)
            else:
                hung += 1
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
    finally:
        stop.set()
        for thread in threads:
            thread.join()
    assert hung == 0, f"{hung} forked children hung on an inherited runtime lock"
