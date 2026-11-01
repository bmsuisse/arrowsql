"""Authentication= and network keywords reach the Rust core, or fail clearly.

Rust-port specific (no ODBC driver interprets the connection string here):
- every ``Authentication`` value either works or raises, it never degrades to
  a SQL login with the client id / UPN;
- ServicePrincipal and ActiveDirectoryPassword hand a token factory to the
  native login (mocked here, no Entra needed);
- ServerSPN, KeepAlive, KeepAliveInterval and IpAddressPreference are applied
  and validated;
- the pool carries the connection timeout across reuse, and the checkin probe
  cannot stall ``close()`` on a half-open socket.
"""

import re
import socket
import sys
import threading
import time
import types

import pytest
import mssql_python
from mssql_python import connect, ddbc_bindings
from mssql_python.exceptions import Error, InterfaceError

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False), reason="Rust port only"
)

BOGUS = "Server=localhost,1999;Encrypt=no;"


class _Stop(Exception):
    """Raised by the stub native Connection to end connect() after capture."""


@pytest.fixture
def native_calls(monkeypatch):
    """Replace the native Connection with a recorder that aborts connect()."""
    calls = []

    class _Recorder:
        def __init__(self, *args):
            calls.append(args)
            raise _Stop

    monkeypatch.setattr(ddbc_bindings, "Connection", _Recorder)
    return calls


def _fake_azure(monkeypatch, tokens):
    """Stand-ins for the azure-identity credentials; ``tokens`` records their use."""

    def make(name):
        class _Cred:
            def __init__(self, **kwargs):
                tokens.append((name, kwargs))

            def get_token(self, scope):
                tokens.append((name, "scope", scope))
                return types.SimpleNamespace(token="jwt-" + name, expires_on=0)

        _Cred.__name__ = name
        return _Cred

    import azure.identity

    for name in ("ClientSecretCredential", "UsernamePasswordCredential"):
        monkeypatch.setattr(azure.identity, name, make(name))


# --- Authentication keyword -------------------------------------------------


@pytest.mark.parametrize("value", ["Bogus", "ActiveDirectoryInteractiv", "Kerberos", "sql"])
def test_unknown_authentication_value_is_rejected(value):
    with pytest.raises(InterfaceError, match="Authentication"):
        connect(BOGUS + f"Authentication={value};UID=u;PWD=p")


def test_ad_integrated_fails_clearly_and_points_to_trusted_connection():
    with pytest.raises(InterfaceError, match="Trusted_Connection"):
        connect(BOGUS + "Authentication=ActiveDirectoryIntegrated")


def test_windows_interactive_without_native_flow_does_not_fall_back_to_sql_login(monkeypatch):
    import mssql_python.auth as auth

    monkeypatch.setattr(auth.platform, "system", lambda: "Windows")
    with pytest.raises(InterfaceError, match="not available"):
        connect(BOGUS + "Authentication=ActiveDirectoryInteractive;UID=u")


def test_managed_identity_alias_takes_the_token_path(monkeypatch):
    import mssql_python.connection as connection

    seen = []

    def fake(auth_type, kwargs=None):
        seen.append((auth_type, kwargs))
        raise RuntimeError("stop")

    monkeypatch.setattr(connection, "get_auth_token_info", fake)
    with pytest.raises(InterfaceError, match="msi"):
        connect(BOGUS + "Authentication=ActiveDirectoryManagedIdentity;UID=client-id")
    assert seen and seen[0] == ("msi", {"client_id": "client-id"})


def test_sql_password_is_a_plain_sql_login(native_calls):
    with pytest.raises(_Stop):
        connect(BOGUS + "Authentication=SqlPassword;UID=u;PWD=p")
    assert native_calls[0][5] is None  # no federated factory


@pytest.mark.parametrize(
    "auth", ["ActiveDirectoryServicePrincipal", "ActiveDirectoryPassword"]
)
def test_federated_modes_need_uid_and_pwd(auth, monkeypatch):
    _fake_azure(monkeypatch, [])
    with pytest.raises(InterfaceError, match="UID"):
        connect(BOGUS + f"Authentication={auth};UID=only-uid")


@pytest.mark.parametrize(
    "auth", ["ActiveDirectoryServicePrincipal", "ActiveDirectoryPassword"]
)
def test_federated_modes_need_azure_identity(auth, monkeypatch):
    monkeypatch.setitem(sys.modules, "azure.identity", None)
    with pytest.raises(InterfaceError, match=re.escape("[azure]")):
        connect(BOGUS + f"Authentication={auth};UID=u;PWD=p")


def test_service_principal_passes_a_token_factory_to_the_core(native_calls, monkeypatch):
    used = []
    _fake_azure(monkeypatch, used)
    with pytest.raises(_Stop):
        connect(BOGUS + "Authentication=ActiveDirectoryServicePrincipal;UID=cid;PWD=sec")
    factory = native_calls[0][5]
    assert callable(factory)
    token = factory(
        "https://database.windows.net/",
        "https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/",
        "ActiveDirectoryServicePrincipal",
    )
    assert token == "jwt-ClientSecretCredential".encode("utf-16-le")
    cred = next(u for u in used if u[0] == "ClientSecretCredential" and isinstance(u[1], dict) and "client_id" in u[1])
    assert cred[1]["tenant_id"] == "11111111-2222-3333-4444-555555555555"
    assert cred[1]["client_id"] == "cid"
    assert ("ClientSecretCredential", "scope", "https://database.windows.net/.default") in used


def test_ad_password_passes_a_token_factory_to_the_core(native_calls, monkeypatch):
    used = []
    _fake_azure(monkeypatch, used)
    with pytest.raises(_Stop):
        connect(BOGUS + "Authentication=ActiveDirectoryPassword;UID=a@contoso.com;PWD=pw")
    factory = native_calls[0][5]
    token = factory(
        "https://database.windows.net/",
        "https://login.microsoftonline.com/contoso.onmicrosoft.com/",
        "ActiveDirectoryPassword",
    )
    assert token == "jwt-UsernamePasswordCredential".encode("utf-16-le")
    kwargs = next(u[1] for u in used if u[0] == "UsernamePasswordCredential" and isinstance(u[1], dict))
    assert (kwargs["username"], kwargs["password"]) == ("a@contoso.com", "pw")
    assert kwargs["tenant_id"] == "contoso.onmicrosoft.com"


def test_password_factory_rejects_a_server_without_tenant(monkeypatch):
    _fake_azure(monkeypatch, [])
    from mssql_python._rust_auth import make_password_token_factory

    factory = make_password_token_factory("u", "p")
    with pytest.raises(RuntimeError, match="tenant"):
        factory("https://database.windows.net/", "not-a-url", "ActiveDirectoryPassword")


def _native(conn_str, factory=None):
    return ddbc_bindings.Connection(conn_str, False, {}, "", None, factory)


@pytest.mark.parametrize(
    "auth",
    [
        "ActiveDirectoryServicePrincipal",
        "ActiveDirectoryPassword",
        "ActiveDirectoryInteractive",
        "ActiveDirectoryDefault",
        "ActiveDirectoryIntegrated",
    ],
)
def test_native_core_never_logs_in_with_sql_auth_for_an_entra_mode(auth):
    """Direct use of the binding (aio, tests) gets the same guarantee."""
    with pytest.raises(Error, match=auth):
        _native(BOGUS + f"Authentication={auth};UID=client-id;PWD=secret")


def test_native_core_rejects_an_unknown_authentication_value():
    with pytest.raises(Error, match="Authentication"):
        _native(BOGUS + "Authentication=Bogus;UID=u;PWD=p")


# --- network keywords -------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["KeepAlive=0", "KeepAlive=abc", "KeepAliveInterval=-1", "IpAddressPreference=nope"],
)
def test_invalid_network_keyword_values_are_rejected(bad):
    with pytest.raises(Error, match=bad.split("=")[0]):
        connect(BOGUS + bad)


def test_network_keywords_are_accepted_by_a_real_login(conn_str):
    extra = "ServerSPN=MSSQLSvc/example:1433;KeepAlive=30;KeepAliveInterval=5;IpAddressPreference=IPv4First"
    with connect(conn_str + ";" + extra) as c:
        assert c.cursor().execute("SELECT 1").fetchone()[0] == 1


# --- pooling ----------------------------------------------------------------


@pytest.fixture
def pooled():
    mssql_python.pooling(max_size=1)
    yield
    mssql_python.pooling(enabled=False)


def test_pool_carries_the_connection_timeout_across_reuse(conn_str, pooled):
    first = connect(conn_str, autocommit=True)
    first.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 7)
    spid = first.cursor().execute("SELECT @@SPID").fetchone()[0]
    first.close()
    with connect(conn_str, autocommit=True) as reused:
        assert reused._conn._get_connection_timeout() == 7
        assert reused.cursor().execute("SELECT @@SPID").fetchone()[0] == spid


class _FreezableProxy:
    """TCP forwarder that can go silent without closing: a half-open socket."""

    def __init__(self, host, port):
        self.target = (host, port)
        self.frozen = threading.Event()
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.socks = [self.listener]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            upstream = socket.create_connection(self.target)
            self.socks += [client, upstream]
            threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    def _pump(self, src, dst):
        try:
            while data := src.recv(65536):
                while self.frozen.is_set():
                    time.sleep(0.05)
                dst.sendall(data)
        except OSError:
            pass

    def close(self):
        self.frozen.clear()
        for s in self.socks:
            try:
                s.close()
            except OSError:
                pass


def test_checkin_probe_is_bounded_on_a_half_open_socket(conn_str, pooled):
    match = re.search(r"(?i)(?:^|;)\s*Server\s*=\s*(?:tcp:)?([^,;]+)(?:,(\d+))?", conn_str)
    host, port = match.group(1), int(match.group(2) or 1433)
    proxy = _FreezableProxy(host, port)
    try:
        via = re.sub(r"(?i)(^|;)\s*Server\s*=[^;]*", rf"\1Server=127.0.0.1,{proxy.port}", conn_str, count=1)
        c = connect(via, autocommit=True)
        c.cursor().execute("SELECT 1").fetchall()
        proxy.frozen.set()
        started = time.monotonic()
        c.close()
        assert time.monotonic() - started < 20
    finally:
        proxy.close()
