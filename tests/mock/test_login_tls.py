"""Login, FedAuth token, TLS 7.4 / strict TDS 8 and ENVCHANGE against the mock TDS server."""

import os
import struct
import subprocess
import sys

import pytest

import mssql_python
from mockutil import tuples


def token_blob(token: str) -> bytes:
    """SQL_COPT_SS_ACCESS_TOKEN layout: u32 byte length + UTF-16LE token."""
    raw = token.encode("utf-16-le")
    return struct.pack("<I", len(raw)) + raw


def test_user_password_login(mock):
    conn = mock.connect()
    try:
        assert tuples(conn.cursor().execute("SELECT 1").fetchall()) == [(1,)]
    finally:
        conn.close()
    (info,) = mock.connections()
    assert info["authenticated"] is True
    assert info["token"] is None
    assert info["user_agent"]


def test_login_advertises_expected_features(mock):
    mock.connect().close()
    (info,) = mock.connections()
    # 0x0F enhanced routing, 0x0A UTF-8 / column encryption group present in the login ext
    assert 0x0F in info["feature_ids"]


def test_database_and_server_name_in_login(mock):
    mock.connect(Database="somedb").close()
    (info,) = mock.connections()
    assert info["database"] == "somedb"
    assert info["server_name"] == f"127.0.0.1,{mock.port}"


def test_fedauth_access_token_is_sent_verbatim(mock):
    token = "eyJ0eXAi.fake-jwt.ésignature"
    conn = mssql_python.connect(
        f"Server=127.0.0.1,{mock.port};Encrypt=no;TrustServerCertificate=yes",
        attrs_before={1256: token_blob(token)},
    )
    try:
        assert conn.cursor().execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()
    (info,) = mock.connections()
    assert info["token"] == token
    assert info["authenticated"] is True


@pytest.mark.parametrize("blob", [b"", b"abc", struct.pack("<I", 4) + b"ab", struct.pack("<I", 3) + b"abc"])
def test_malformed_access_token_is_rejected_before_connecting(mock, blob):
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(
            f"Server=127.0.0.1,{mock.port};Encrypt=no", attrs_before={1256: blob}
        )
    assert mock.connections() == []


def test_login_envchanges_leave_connection_usable(mock):
    """Login is answered with database, collation and 4096-byte packet-size
    ENVCHANGEs; the connection must apply them and keep working.
    (The mock does not reassemble multi-packet requests, so a request larger
    than the negotiated packet size cannot be tested here.)"""
    mock.register("SELECT 'env'", [("v", "nvarchar")], [["ok"]])
    conn = mock.connect(Database="somedb")
    try:
        cur = conn.cursor()
        for _ in range(3):
            cur.execute("SELECT 'env'")
            assert tuples(cur.fetchall()) == [("ok",)]
    finally:
        conn.close()


def test_transaction_headers_use_outstanding_request_count_one(mock):
    conn = mock.connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
    finally:
        conn.close()
    (info,) = mock.connections()
    assert info["tx_headers"], "client sent no ALL_HEADERS"
    assert all(count == 1 for _desc, count in info["tx_headers"])


# --- TLS ---------------------------------------------------------------------

PROBE = """
import sys, json, mssql_python
try:
    c = mssql_python.connect(sys.argv[1])
    ok = c.cursor().execute("SELECT 1").fetchone()[0] == 1
    c.close()
    print(json.dumps({"ok": ok}))
except mssql_python.Error as e:
    print(json.dumps({"ok": False, "error": type(e).__name__, "message": str(e)}))
"""


def probe(conn_str, ca=None):
    """Connect in a fresh process: the trust store is loaded once per process."""
    env = {k: v for k, v in os.environ.items() if k != "SSL_CERT_FILE"}
    if ca:
        env["SSL_CERT_FILE"] = ca
    out = subprocess.run(
        [sys.executable, "-I", "-c", PROBE, conn_str], capture_output=True, text=True, env=env, timeout=60
    )
    assert out.returncode == 0, out.stderr
    import json

    return json.loads(out.stdout.strip().splitlines()[-1])


# The driver validates against the OS store on Windows (Schannel), not SSL_CERT_FILE.
needs_ca_env = pytest.mark.skipif(sys.platform == "win32", reason="Schannel ignores SSL_CERT_FILE")


@pytest.fixture
def tls_server(start_mock, tls_material):
    def make(mode):
        return start_mock("--tls", mode, *tls_material.server_args)

    return make


def test_tls_with_trust_server_certificate(tls_server):
    server = tls_server("optional")
    conn = mssql_python.connect(server.conn_str(Encrypt="yes", TrustServerCertificate="yes"))
    try:
        assert conn.cursor().execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()
    assert server.connections()[0]["authenticated"] is True


def test_strict_ignores_trust_server_certificate(tls_server):
    """Like msodbcsql, Encrypt=strict always validates the chain."""
    server = tls_server("strict")
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(server.conn_str(Encrypt="strict", TrustServerCertificate="yes"))


@needs_ca_env
@pytest.mark.parametrize("mode", ["optional", "strict"])
def test_tls_validates_chain_and_hostname_with_trusted_ca(tls_server, tls_material, mode):
    server = tls_server(mode)
    encrypt = "strict" if mode == "strict" else "yes"
    cs = server.conn_str(Encrypt=encrypt, TrustServerCertificate="no", Server=f"localhost,{server.port}")
    assert probe(cs, ca=tls_material.ca_pem) == {"ok": True}


@pytest.mark.parametrize("mode", ["optional", "strict"])
def test_tls_rejects_untrusted_certificate(tls_server, mode):
    server = tls_server(mode)
    encrypt = "strict" if mode == "strict" else "yes"
    cs = server.conn_str(Encrypt=encrypt, TrustServerCertificate="no", Server=f"localhost,{server.port}")
    result = probe(cs)
    assert result["ok"] is False
    assert result["error"] in {"OperationalError", "InterfaceError", "DatabaseError"}


@needs_ca_env
def test_tls_hostname_mismatch_is_rejected(tls_server, tls_material):
    server = tls_server("optional")
    cs = server.conn_str(
        Encrypt="yes", TrustServerCertificate="no", Server=f"127.0.0.1,{server.port}",
        HostNameInCertificate="wrong.example.com",
    )
    assert probe(cs, ca=tls_material.ca_pem)["ok"] is False


@needs_ca_env
def test_tls_hostname_in_certificate_overrides_server_name(tls_server, tls_material):
    server = tls_server("optional")
    cs = server.conn_str(
        Encrypt="yes", TrustServerCertificate="no", Server=f"127.0.0.1,{server.port}",
        HostNameInCertificate="localhost",
    )
    assert probe(cs, ca=tls_material.ca_pem) == {"ok": True}


def test_encrypt_required_against_plaintext_server_fails(mock):
    """Encrypt=yes must not silently downgrade when the server offers no TLS."""
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(mock.conn_str(Encrypt="yes", TrustServerCertificate="yes"))


def test_strict_against_non_tls_server_fails(mock):
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(mock.conn_str(Encrypt="strict", TrustServerCertificate="yes"))
