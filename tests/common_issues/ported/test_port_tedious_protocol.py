"""Protocol-level behaviour: large parameters, packet boundaries, slow consumers,
dropped sockets and malformed or silent servers during login.

Adapted from tediousjs/tedious (MIT), Copyright (c) 2010-2026 Mike D Pilsbury
and tedious contributors. Sources, all under test/integration/:
- streaming-parameters-test.ts (large varbinary(max) / nvarchar(max) / varchar(max)
  values, multi-packet payloads)
- multi-packet-request-test.ts (nvarchar values that span several TDS packets)
- pause-resume-test.ts (a consumer that stalls part-way through a long result,
  and a request that is not read at all)
- socket-error-test.ts (a socket failing while a request is in flight)
- invalid-packet-stream-test.ts (a server that sends a malformed packet)

The translations are behavioural. Python has no async-iterable parameter
streaming or pause/resume API, so chunking and pause hooks are replaced by
ordinary fetches and sleeps. The socket-failure cases use a small TCP proxy
that can be killed from another thread. Upstream ODBC driver is ground truth.
"""

import re
import socket
import threading
import time
from contextlib import contextmanager

import pytest
from mssql_python import Error, OperationalError, connect

PACKET_SIZE_CHARS = [1, 2046, 2047, 2048, 2049, 4095, 4096, 8191, 8192, 16384]


def _bmp_text(n):
    """Exactly n UTF-16 code units, all in the BMP, so DATALENGTH is 2 * n."""
    pattern = "abcdé€xyz"
    return (pattern * (n // len(pattern) + 1))[:n]


def _server_addr(conn_str):
    match = re.search(r"Server=([^;]*)", conn_str, re.IGNORECASE)
    host, _, port = match.group(1).partition(",")
    return host, int(port or 1433)


def _with_server(conn_str, host, port):
    return re.sub(r"Server=[^;]*", f"Server={host},{port}", conn_str, count=1, flags=re.IGNORECASE)


class _KillableProxy:
    """Forwards TCP bytes to a target; kill() drops every connection at once."""

    def __init__(self, host, port):
        self._target = (host, port)
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._pairs = []
        self._lock = threading.Lock()
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            server = socket.create_connection(self._target)
            with self._lock:
                self._pairs.append((client, server))
            threading.Thread(target=self._pump, args=(client, server), daemon=True).start()
            threading.Thread(target=self._pump, args=(server, client), daemon=True).start()

    @staticmethod
    def _pump(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for sock in (src, dst):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def kill(self):
        with self._lock:
            pairs, self._pairs = self._pairs, []
        for pair in pairs:
            for sock in pair:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
        self._listener.close()


@pytest.fixture
def proxy(conn_str):
    host, port = _server_addr(conn_str)
    instance = _KillableProxy(host, port)
    yield instance
    instance.kill()


def _proxied_conn_str(conn_str, proxy):
    return _with_server(conn_str, "127.0.0.1", proxy.port)


# --- Large parameters (streaming-parameters, multi-packet) ----------------


def test_varbinary_max_100kb_parameter_roundtrips_exactly(cursor):
    blob = bytes(i % 251 for i in range(100_000))
    cursor.execute("SELECT CAST(? AS varbinary(max))", [blob])
    (got,) = cursor.fetchone()
    assert isinstance(got, bytes)
    assert len(got) == 100_000
    assert got == blob


def test_nvarchar_max_emoji_parameter_roundtrips_with_utf16_length(cursor):
    value = "streamed \U0001F600 " * 5000
    cursor.execute("SELECT CAST(? AS nvarchar(max)), DATALENGTH(CAST(? AS nvarchar(max)))", [value, value])
    got, nbytes = cursor.fetchone()
    assert got == value
    assert nbytes == len(value.encode("utf-16-le"))


def test_varchar_max_parameter_roundtrips_exactly(cursor):
    value = "streamed value " * 5000
    cursor.execute("SELECT CAST(? AS varchar(max))", [value])
    (got,) = cursor.fetchone()
    assert got == value


def test_large_parameters_inserted_into_table_and_read_back(cursor):
    cursor.execute("CREATE TABLE #big (id int NOT NULL, b varbinary(max) NULL, s nvarchar(max) NULL)")
    blob = bytes(range(256)) * 400
    text = _bmp_text(60_000)
    cursor.execute("INSERT INTO #big (id, b, s) VALUES (?, ?, ?)", [1, blob, text])
    cursor.execute("SELECT b, s FROM #big WHERE id = ?", [1])
    got_blob, got_text = cursor.fetchone()
    assert got_blob == blob
    assert got_text == text


@pytest.mark.parametrize("n", PACKET_SIZE_CHARS)
def test_nvarchar_parameter_around_packet_boundaries_roundtrips(cursor, n):
    value = _bmp_text(n)
    # CAST pins the parameter to nvarchar; a bare one-character str is bound as varchar.
    cursor.execute("SELECT ?, DATALENGTH(CAST(? AS nvarchar(max)))", [value, value])
    got, nbytes = cursor.fetchone()
    assert got == value
    assert nbytes == 2 * n


def test_several_large_parameters_in_one_request_all_arrive_intact(cursor):
    a = _bmp_text(3000)
    b = _bmp_text(5000)
    blob = bytes(range(256)) * 200
    cursor.execute("SELECT ?, ?, ?", [a, b, blob])
    got_a, got_b, got_blob = cursor.fetchone()
    assert got_a == a
    assert got_b == b
    assert got_blob == blob


# --- Consumers that stall (pause-resume) -----------------------------------

RECURSIVE_SQL = (
    "with cte1 as (select 1 as i union all select i + 1 from cte1 where i < 20000) "
    "select i from cte1 option (maxrecursion 0)"
)


def test_consumer_that_stalls_mid_result_still_receives_every_row_in_order(cursor):
    cursor.execute(RECURSIVE_SQL)
    seen = []
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        seen.append(row[0])
        if row[0] == 1000:
            time.sleep(1.0)
    assert len(seen) == 20000
    assert seen == list(range(1, 20001))


def test_result_left_unread_for_a_while_is_complete_when_read_later(cursor):
    cursor.execute(RECURSIVE_SQL)
    time.sleep(0.5)
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == list(range(1, 20001))


def test_reexecute_after_partial_fetch_discards_unread_rows(cursor):
    cursor.execute(RECURSIVE_SQL)
    assert len(cursor.fetchmany(1000)) == 1000
    cursor.execute("SELECT 2")
    assert [tuple(r) for r in cursor.fetchall()] == [(2,)]


def test_close_connection_with_unread_rows_returns_promptly(conn_str):
    conn = connect(conn_str)
    cur = conn.cursor()
    cur.execute(RECURSIVE_SQL)
    assert len(cur.fetchmany(1000)) == 1000
    start = time.monotonic()
    conn.close()
    assert time.monotonic() - start < 10

    with connect(conn_str) as other:
        assert tuple(other.cursor().execute("SELECT 1").fetchone()) == (1,)


# --- Socket failures while a request is in flight (socket-error) -----------


def test_socket_dropped_during_waitfor_raises_error_without_waiting_out_the_delay(conn_str, proxy):
    conn = connect(_proxied_conn_str(conn_str, proxy))
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()

    timer = threading.Timer(1.0, proxy.kill)
    timer.start()
    start = time.monotonic()
    try:
        with pytest.raises(OperationalError):
            cur.execute("WAITFOR DELAY '00:00:30'")
        assert time.monotonic() - start < 20
    finally:
        timer.cancel()
    try:
        conn.close()
    except Error:
        pass


def test_execute_after_socket_drop_raises_and_server_is_still_reachable(conn_str, proxy):
    conn = connect(_proxied_conn_str(conn_str, proxy))
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    proxy.kill()
    with pytest.raises(Error):
        cur.execute("SELECT 2")
    with connect(conn_str) as direct:
        assert tuple(direct.cursor().execute("SELECT 3").fetchone()) == (3,)


def test_uncommitted_insert_is_rolled_back_when_socket_drops(conn_str, proxy, table_name):
    with connect(conn_str, autocommit=True) as admin:
        admin.cursor().execute(f"CREATE TABLE dbo.{table_name} (id int NOT NULL)")

    conn = connect(_proxied_conn_str(conn_str, proxy), autocommit=False)
    cur = conn.cursor()
    cur.execute(f"INSERT INTO dbo.{table_name} (id) VALUES (?)", [1])
    proxy.kill()
    with pytest.raises(Error):
        conn.commit()
    try:
        conn.close()
    except Error:
        pass

    deadline = time.monotonic() + 15
    count = None
    with connect(conn_str, autocommit=True) as direct:
        while time.monotonic() < deadline:
            count = direct.cursor().execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0]
            if count == 0:
                break
            time.sleep(0.25)
    assert count == 0


# --- Malformed or silent servers during login (invalid-packet-stream) ------


@contextmanager
def _fake_tds_server(behaviour):
    """Accepts one connection and reacts to it with ``behaviour(sock)``."""
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    held = []

    def serve():
        try:
            sock, _ = listener.accept()
        except OSError:
            return
        held.append(sock)
        try:
            behaviour(sock)
        except OSError:
            pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        for sock in held:
            sock.close()
        listener.close()


def _read_prelogin(sock):
    sock.recv(4096)


def _invalid_packet(sock):
    _read_prelogin(sock)
    # Header declares a length of 5 bytes, which is smaller than the header itself.
    bad = b"\x04\x01\x00\x05\x00\x00\x01\x00" + b"test1234"
    sock.sendall(bad)
    time.sleep(1)


def _close_after_prelogin(sock):
    _read_prelogin(sock)


def _blackhole(sock):
    _read_prelogin(sock)
    time.sleep(30)


def test_invalid_packet_from_server_raises_operational_error_promptly():
    with _fake_tds_server(_invalid_packet) as port:
        start = time.monotonic()
        with pytest.raises(OperationalError):
            connect(f"Server=127.0.0.1,{port};Database=master;UID=sa;PWD=x;Encrypt=no", timeout=5)
        assert time.monotonic() - start < 10


def test_server_closing_during_prelogin_raises_error_promptly():
    with _fake_tds_server(_close_after_prelogin) as port:
        start = time.monotonic()
        with pytest.raises(Error):
            connect(f"Server=127.0.0.1,{port};Database=master;UID=sa;PWD=x;Encrypt=no", timeout=5)
        assert time.monotonic() - start < 10


def test_silent_server_is_bounded_by_login_timeout():
    with _fake_tds_server(_blackhole) as port:
        start = time.monotonic()
        with pytest.raises(OperationalError):
            connect(f"Server=127.0.0.1,{port};Database=master;UID=sa;PWD=x;Encrypt=no", timeout=2)
        elapsed = time.monotonic() - start
        assert 1.5 <= elapsed < 10
