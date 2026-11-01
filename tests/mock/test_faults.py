"""Fault injection: delays (timeout, cancel), info messages, errors, redirection,
concurrency and the server dying mid-query."""

import threading
import time

import pytest

import mssql_python
from mockutil import tuples


def make(mock, timeout=None):
    conn = mock.connect()
    if timeout is not None:
        conn.timeout = timeout  # applies to cursors created afterwards
    return conn


# --- delays: query timeout and cancel ------------------------------------------

def test_query_timeout_raises_operational_error_and_connection_survives(mock):
    mock.register("SELECT slow", [("i", "int")], [[1]], delay_ms=4000)
    mock.register("SELECT fast", [("i", "int")], [[2]])
    conn = make(mock, timeout=1)
    try:
        cur = conn.cursor()
        start = time.monotonic()
        with pytest.raises(mssql_python.OperationalError):
            cur.execute("SELECT slow").fetchall()
        assert time.monotonic() - start < 3.5
        # the attention cancels the delayed reply; the connection is reusable
        assert cur.execute("SELECT fast").fetchone()[0] == 2
    finally:
        conn.close()


def test_no_timeout_waits_for_the_delayed_reply(mock):
    mock.register("SELECT slowish", [("i", "int")], [[1]], delay_ms=600)
    conn = make(mock)
    try:
        start = time.monotonic()
        assert conn.cursor().execute("SELECT slowish").fetchone()[0] == 1
        assert time.monotonic() - start >= 0.5
    finally:
        conn.close()


def test_cancel_from_another_thread_interrupts_a_delayed_query(mock):
    mock.register("SELECT slow", [("i", "int")], [[1]], delay_ms=10_000)
    mock.register("SELECT fast", [("i", "int")], [[2]])
    conn = make(mock)
    try:
        cur = conn.cursor()
        timer = threading.Timer(0.4, cur.cancel)
        timer.start()
        start = time.monotonic()
        with pytest.raises(mssql_python.OperationalError):
            cur.execute("SELECT slow").fetchall()
        timer.join()
        assert time.monotonic() - start < 5
        assert cur.execute("SELECT fast").fetchone()[0] == 2
    finally:
        conn.close()


def test_login_delay_exceeding_login_timeout_fails_cleanly(mock):
    mock.register("@login_delay", delay_ms=5000)
    start = time.monotonic()
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(mock.conn_str(), attrs_before={103: 1})  # SQL_ATTR_LOGIN_TIMEOUT
    assert time.monotonic() - start < 4.5


# --- info messages ---------------------------------------------------------------

def test_info_messages_land_in_cursor_messages(mock):
    mock.register(
        "SELECT info", [("a", "int")], [[1]],
        info=[{"number": 50002, "severity": 10, "message": "before rows"}],
        trailing_info=[{"number": 50003, "severity": 10, "message": "after rows"}],
    )
    conn = make(mock)
    try:
        cur = conn.cursor()
        cur.execute("SELECT info")
        assert tuples(cur.fetchall()) == [(1,)]
        texts = [m[1] for m in cur.messages]
        assert any("before rows" in t for t in texts)
        assert any("after rows" in t for t in texts)
        codes = [m[0] for m in cur.messages]
        assert any("50002" in c for c in codes) and any("50003" in c for c in codes)
    finally:
        conn.close()


def test_messages_reset_on_next_execute(mock):
    mock.register("SELECT info", [("a", "int")], [[1]], info=[{"number": 50002, "severity": 10, "message": "hi"}])
    mock.register("SELECT quiet", [("a", "int")], [[2]])
    conn = make(mock)
    try:
        cur = conn.cursor()
        cur.execute("SELECT info")
        cur.fetchall()
        assert cur.messages
        cur.execute("SELECT quiet")
        cur.fetchall()
        assert cur.messages == []
    finally:
        conn.close()


# --- errors ------------------------------------------------------------------------

@pytest.mark.parametrize(
    "number, severity, exc",
    [
        (50001, 16, mssql_python.ProgrammingError),
        (2627, 14, mssql_python.IntegrityError),
        (1205, 13, mssql_python.OperationalError),
    ],
)
def test_server_error_maps_to_exception_class(mock, number, severity, exc):
    mock.register_error("SELECT err", number, "injected failure", severity=severity)
    conn = make(mock)
    try:
        with pytest.raises(exc) as info:
            conn.cursor().execute("SELECT err")
        assert "injected failure" in str(info.value)
        assert conn.cursor().execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()


def test_error_carries_sqlstate_and_native_error(mock):
    mock.register_error("SELECT err", 50001, "custom failure")
    conn = make(mock)
    try:
        with pytest.raises(mssql_python.ProgrammingError) as info:
            conn.cursor().execute("SELECT err")
        err = info.value
        assert err.sqlstate == "42000"
        assert err.native_error == 50001
    finally:
        conn.close()


def test_leading_error_before_result_set_raises(mock):
    mock.register(
        "SELECT lead", [("a", "int")], [[1]],
        leading_error={"number": 50004, "severity": 16, "message": "statement failed first"},
    )
    conn = make(mock)
    try:
        cur = conn.cursor()
        with pytest.raises(mssql_python.ProgrammingError, match="statement failed first"):
            cur.execute("SELECT lead")
            cur.fetchall()
        assert cur.execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()


def test_delayed_error(mock):
    mock.register_error("SELECT blocked", 1222, "Lock request time out period exceeded.", delay_ms=300)
    conn = make(mock)
    try:
        start = time.monotonic()
        with pytest.raises(mssql_python.Error, match="Lock request"):
            conn.cursor().execute("SELECT blocked")
        assert time.monotonic() - start >= 0.25
    finally:
        conn.close()


# --- redirection ---------------------------------------------------------------------

def test_plain_login_redirection(start_mock):
    target = start_mock()
    target.register("SELECT where_am_i", [("v", "nvarchar")], [["target"]])
    router = start_mock("--redirect", f"127.0.0.1:{target.port}")
    conn = mssql_python.connect(router.conn_str())
    try:
        assert conn.cursor().execute("SELECT where_am_i").fetchone()[0] == "target"
    finally:
        conn.close()
    assert len(target.connections()) == 1
    assert target.connections()[0]["authenticated"]
    assert len(router.connections()) == 1  # the client did visit the router first


def test_enhanced_routing_redirects_and_sets_database(start_mock):
    target = start_mock()
    target.register("SELECT where_am_i", [("v", "nvarchar")], [["enhanced"]])
    router = start_mock("--redirect", f"127.0.0.1:{target.port}:routed_db")
    conn = mssql_python.connect(router.conn_str())
    try:
        assert conn.cursor().execute("SELECT where_am_i").fetchone()[0] == "enhanced"
    finally:
        conn.close()
    (info,) = target.connections()
    assert info["database"] == "routed_db"


def test_redirection_to_dead_port_fails_cleanly(start_mock):
    dead = start_mock()
    dead_port = dead.port
    dead.close()
    router = start_mock("--redirect", f"127.0.0.1:{dead_port}")
    start = time.monotonic()
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(router.conn_str(), attrs_before={103: 3})
    assert time.monotonic() - start < 10


# --- concurrency -----------------------------------------------------------------------

def test_many_concurrent_connections(mock):
    mock.register("SELECT worker", [("i", "int")], [[i] for i in range(20)], delay_ms=50)
    errors, results = [], []

    def work(n):
        try:
            conn = mock.connect()
            try:
                cur = conn.cursor()
                for _ in range(5):
                    cur.execute("SELECT worker")
                    results.append(len(cur.fetchall()))
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - collected and asserted below
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not errors, errors
    assert results == [20] * (16 * 5)
    assert len(mock.connections()) == 16


def test_one_connection_blocked_does_not_stall_others(mock):
    mock.register("SELECT slow", [("i", "int")], [[1]], delay_ms=2000)
    slow_conn, fast_conn = mock.connect(), mock.connect()
    try:
        t = threading.Thread(target=lambda: slow_conn.cursor().execute("SELECT slow").fetchall())
        t.start()
        time.sleep(0.2)
        start = time.monotonic()
        assert fast_conn.cursor().execute("SELECT 1").fetchone()[0] == 1
        assert time.monotonic() - start < 1.0
        t.join(10)
    finally:
        slow_conn.close()
        fast_conn.close()


# --- server dying -------------------------------------------------------------------------

def test_server_killed_mid_query_raises_clean_error(mock):
    mock.register("SELECT slow", [("i", "int")], [[1]], delay_ms=10_000)
    conn = make(mock)
    cur = conn.cursor()
    threading.Timer(0.4, mock.kill).start()
    start = time.monotonic()
    with pytest.raises(mssql_python.Error):
        cur.execute("SELECT slow").fetchall()
    assert time.monotonic() - start < 8
    with pytest.raises(mssql_python.Error):
        cur.execute("SELECT 1")
    try:
        conn.close()  # may raise on a dead link (rollback of the open transaction), as upstream
    except mssql_python.Error:
        pass


def test_server_gone_before_connect(start_mock):
    server = start_mock()
    cs = server.conn_str()
    server.kill()
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(cs)
