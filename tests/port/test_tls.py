"""TLS validation of the rustls backend (vendor/mssql-tds rustls_engine.rs).

Each case connects in a fresh subprocess, because the trust store is loaded
once per process (and SSL_CERT_FILE is read at that point).

The chain and hostname cases need a server certificate signed by a CA we
control: run scripts/setup_tls_cert.sh once (it writes <main>/.tls-test/ca.pem
and restarts the container), or point MSSQL_TEST_TLS_CA at the CA that signed
the server's certificate for "localhost". Without it those cases are skipped;
SQL Server's auto-generated certificate is X.509 v1 and cannot be validated
by any webpki-based client.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from mssql_python.connection_string_builder import _ConnectionStringBuilder
from mssql_python.connection_string_parser import _ConnectionStringParser

pytestmark = pytest.mark.skipif(
    not os.getenv("DB_CONNECTION_STRING"), reason="Database connection string not provided"
)

PROBE = """
import json, sys
import mssql_python
try:
    conn = mssql_python.connect(sys.argv[1])
    cur = conn.cursor()
    cur.execute("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID")
    print(json.dumps({"ok": True, "encrypted": cur.fetchone()[0] == "TRUE"}))
except mssql_python.Error as exc:
    print(json.dumps({"ok": False, "error": type(exc).__name__, "message": str(exc)}))
"""


@pytest.fixture
def conn_str():
    return os.environ["DB_CONNECTION_STRING"]


def _main_checkout() -> Path:
    out = subprocess.run(
        ["git", "-C", str(Path(__file__).parent), "worktree", "list", "--porcelain"],
        capture_output=True, text=True, check=False,
    ).stdout
    return Path(out.splitlines()[0].split(" ", 1)[1]) if out else Path(__file__).parents[2]


def _test_ca():
    path = os.getenv("MSSQL_TEST_TLS_CA") or str(_main_checkout() / ".tls-test" / "ca.pem")
    return path if os.path.exists(path) else None


needs_ca = pytest.mark.skipif(_test_ca() is None, reason="run scripts/setup_tls_cert.sh first")


def _connect(conn_str, ca=None, **overrides):
    params = _ConnectionStringParser._normalize_params(_ConnectionStringParser()._parse(conn_str))
    for key, value in overrides.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    env = {k: v for k, v in os.environ.items() if k not in ("SSL_CERT_FILE", "SSL_CERT_DIR")}
    if ca:
        env["SSL_CERT_FILE"] = ca
    out = subprocess.run(
        [sys.executable, "-c", PROBE, _ConnectionStringBuilder(params).build()],
        env=env, capture_output=True, text=True, timeout=120, check=False,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_trust_server_certificate_connects_encrypted(conn_str):
    result = _connect(conn_str, Encrypt="yes", TrustServerCertificate="yes")
    assert result == {"ok": True, "encrypted": True}


def test_untrusted_certificate_is_rejected(conn_str):
    # The server's certificate is self-signed or signed by our test CA; neither
    # is in the system trust store, so validation must fail.
    result = _connect(conn_str, Encrypt="yes", TrustServerCertificate="no")
    assert result["ok"] is False
    assert result["error"] == "OperationalError"


@needs_ca
def test_trusted_chain_and_matching_name_connects(conn_str):
    result = _connect(conn_str, ca=_test_ca(), Server="localhost,1433",
                      Encrypt="yes", TrustServerCertificate="no")
    assert result == {"ok": True, "encrypted": True}


@needs_ca
def test_hostname_mismatch_is_rejected(conn_str):
    # The certificate names "localhost" only; 127.0.0.1 is the same server but
    # not the name in the certificate.
    result = _connect(conn_str, ca=_test_ca(), Server="127.0.0.1,1433",
                      Encrypt="yes", TrustServerCertificate="no")
    assert result["ok"] is False
    assert "TLS handshake failed" in result["message"]


@needs_ca
def test_host_name_in_certificate_is_validated(conn_str):
    # Stricter than msodbcsql, which accepts this connection: the name the
    # caller says the certificate carries must actually be in it.
    result = _connect(conn_str, ca=_test_ca(), Server="localhost,1433",
                      Encrypt="yes", TrustServerCertificate="no",
                      HostNameInCertificate="evil.example")
    assert result["ok"] is False


@pytest.mark.parametrize("encrypt", ["no", "optional"])
def test_encrypt_off_connects_without_encryption(conn_str, encrypt):
    result = _connect(conn_str, Encrypt=encrypt, TrustServerCertificate=None)
    assert result == {"ok": True, "encrypted": False}
