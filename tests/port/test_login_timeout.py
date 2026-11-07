"""Login timeout: how an expired login reports (set with connect(timeout=)).

msodbcsql reports an expired login timeout as HYT00 "Login timeout expired"
(the same SQLSTATE as a query timeout, a different message). The server here
is a local listener that accepts TCP and never answers the prelogin, so no
SQL Server is needed.
"""

import socket
import threading
import time

import pytest

import mssql_python


@pytest.fixture
def silent_server():
    """A TCP listener that accepts connections and never sends a byte."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    accepted = []
    stop = threading.Event()

    def accept_loop():
        listener.settimeout(0.2)
        while not stop.is_set():
            try:
                accepted.append(listener.accept()[0])
            except OSError:
                continue

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    yield f"127.0.0.1,{listener.getsockname()[1]}"
    stop.set()
    thread.join()
    for sock in accepted:
        sock.close()
    listener.close()


def test_expired_login_timeout_is_reported_as_login_timeout(silent_server):
    conn_str = (
        f"Server={silent_server};UID=sa;PWD=x;Encrypt=yes;TrustServerCertificate=yes;"
        "ConnectRetryCount=0"
    )
    started = time.monotonic()
    with pytest.raises(mssql_python.OperationalError) as info:
        mssql_python.connect(conn_str, timeout=2)
    elapsed = time.monotonic() - started

    assert 1.5 < elapsed < 10, elapsed
    message = str(info.value)
    assert "Login timeout expired" in message
    assert "Query timeout" not in message
    assert info.value.sqlstate == "HYT00"
