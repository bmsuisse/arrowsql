"""Login timeout: connection string keywords and how an expired login reports.

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
from mssql_python.connection_string_parser import _ConnectionStringParser


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


@pytest.mark.parametrize(
    "keyword", ["Connect Timeout", "Connection Timeout", "Login Timeout", "LoginTimeout", "connect timeout"]
)
def test_login_timeout_keywords_are_accepted(keyword):
    params = _ConnectionStringParser()._parse(f"Server=localhost;{keyword}=7")
    assert _ConnectionStringParser._normalize_params(params) == {"Server": "localhost", "Connect Timeout": "7"}


def test_other_connection_behaviour_keywords_keep_upstream_names():
    # The part of upstream's test_normalize_key_connection_params that does not
    # concern the timeout keywords (that test is excluded as a GAP).
    expected = {
        "connectretrycount": "ConnectRetryCount",
        "connectretryinterval": "ConnectRetryInterval",
        "multisubnetfailover": "MultiSubnetFailover",
        "applicationintent": "ApplicationIntent",
        "keepalive": "KeepAlive",
        "keepaliveinterval": "KeepAliveInterval",
        "ipaddresspreference": "IpAddressPreference",
    }
    for key, canonical in expected.items():
        assert _ConnectionStringParser.normalize_key(key) == canonical
    # A bare "Timeout" stays rejected, as upstream has it.
    assert _ConnectionStringParser.normalize_key("timeout") is None


@pytest.mark.parametrize("keyword", ["Connect Timeout", "Login Timeout"])
def test_expired_login_timeout_is_reported_as_login_timeout(silent_server, keyword):
    conn_str = (
        f"Server={silent_server};UID=sa;PWD=x;Encrypt=yes;TrustServerCertificate=yes;"
        f"ConnectRetryCount=0;{keyword}=2"
    )
    started = time.monotonic()
    with pytest.raises(mssql_python.OperationalError) as info:
        mssql_python.connect(conn_str)
    elapsed = time.monotonic() - started

    assert 1.5 < elapsed < 10, elapsed
    message = str(info.value)
    assert "Login timeout expired" in message
    assert "Query timeout" not in message
    assert info.value.sqlstate == "HYT00"
