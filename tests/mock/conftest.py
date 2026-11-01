"""Mock TDS server fixtures: driver tests that need no SQL Server.

The server is Microsoft's `mssql-mock-tds` (MIT, github.com/microsoft/mssql-rs),
wrapped by tests/mock/server (a small Rust binary that exposes the library's
query registry and connection store as JSON lines on stdin/stdout, because the
upstream CLI and Python bindings cannot register query responses).

The binary is found via $ARROWSQL_MOCK_SERVER, then tests/mock/server/target/.
If it is missing it is built with cargo (set ARROWSQL_MOCK_NO_BUILD=1 to skip
that). A still-missing binary skips the tests, unless ARROWSQL_MOCK_REQUIRED=1
(set in CI), which fails them instead.
"""

import datetime
import ipaddress
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

SERVER_DIR = Path(__file__).parent / "server"
EXE = "arrowsql-mock-server" + (".exe" if sys.platform == "win32" else "")


def _find_binary():
    env = os.environ.get("ARROWSQL_MOCK_SERVER")
    if env:
        return Path(env)
    for profile in ("release", "debug"):
        candidate = SERVER_DIR / "target" / profile / EXE
        if candidate.exists():
            return candidate
    return None


@pytest.fixture(scope="session")
def mock_binary(tmp_path_factory):
    path = _find_binary()
    if path is None and not os.environ.get("ARROWSQL_MOCK_NO_BUILD"):
        cargo = shutil.which("cargo") or str(Path.home() / ".cargo" / "bin" / "cargo")
        features = ["--features", "vendored-openssl"] if os.environ.get("ARROWSQL_MOCK_VENDORED_OPENSSL") else []
        build = subprocess.run(
            [cargo, "build", "--manifest-path", str(SERVER_DIR / "Cargo.toml"), *features],
            capture_output=True, text=True,
        )
        if build.returncode == 0:
            path = _find_binary()
        else:
            print(build.stderr[-2000:], file=sys.stderr)
    if path is None or not path.exists():
        message = "mock TDS server binary not built (cargo build --manifest-path tests/mock/server/Cargo.toml)"
        if os.environ.get("ARROWSQL_MOCK_REQUIRED"):
            pytest.fail(message)
        pytest.skip(message)
    return path


class Mock:
    """One running mock server process. Commands are JSON lines over stdin."""

    def __init__(self, binary, *args):
        self.proc = subprocess.Popen(
            [str(binary), *args], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self._lock = threading.Lock()
        hello = self.proc.stdout.readline()
        if not hello:
            raise RuntimeError("mock server failed to start: " + self.proc.stderr.read())
        self.port = json.loads(hello)["port"]

    def command(self, **cmd):
        with self._lock:
            self.proc.stdin.write(json.dumps(cmd) + "\n")
            self.proc.stdin.flush()
            reply = json.loads(self.proc.stdout.readline())
        assert reply["ok"], reply
        return reply

    def register(self, query, columns=(), rows=(), **kw):
        """columns: [(name, type)], type in tinyint smallint int bigint nvarchar
        nvarchar_max varchar_max varbinary_max. Cells: ints, str, None; PLP cells
        are lists of chunks (str for nvarchar_max, hex str for the other two).
        kw: delay_ms, info, trailing_info, leading_error, error ({number, severity, message})."""
        cols = [{"name": n, "type": t} for n, t in columns]
        self.command(cmd="register", query=query, columns=cols, rows=[list(r) for r in rows], **kw)

    def register_error(self, query, number, message, severity=16, **kw):
        self.register(query, error={"number": number, "severity": severity, "message": message}, **kw)

    def connections(self):
        return self.command(cmd="connections")["connections"]

    def conn_str(self, **extra):
        parts = {
            "Server": f"127.0.0.1,{self.port}", "Uid": "mockuser", "Pwd": "mockpass",
            "Encrypt": "no", "TrustServerCertificate": "yes",
            **extra,
        }
        return ";".join(f"{k}={v}" for k, v in parts.items())

    def connect(self, **extra):
        import mssql_python

        return mssql_python.connect(self.conn_str(**extra))

    def kill(self):
        """Hard stop (simulates the server dying mid-query)."""
        self.proc.kill()
        self.proc.wait()

    def close(self):
        if self.proc.poll() is None:
            try:
                self.proc.stdin.write('{"cmd":"quit"}\n')
                self.proc.stdin.flush()
            except OSError:
                pass
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        for stream in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
            try:
                stream.close()
            except OSError:
                pass


@pytest.fixture
def start_mock(mock_binary):
    """Factory: start_mock("--tls", "strict", ...) -> Mock. All are stopped at teardown."""
    started = []

    def start(*args):
        mock = Mock(mock_binary, *args)
        started.append(mock)
        return mock

    yield start
    for mock in started:
        mock.close()


@pytest.fixture
def mock(start_mock):
    """A plain (no TLS) mock server."""
    return start_mock()


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --- TLS material -------------------------------------------------------------

@pytest.fixture(scope="session")
def tls_material(tmp_path_factory):
    """A CA plus a server leaf certificate valid for localhost / 127.0.0.1.

    Returns a namespace with ca_pem (trust anchor), and server args for the mock
    (PEM cert/key on POSIX, PKCS#12 on Windows, where the mock cannot read PEM).
    """
    pytest.importorskip("cryptography")
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    root = tmp_path_factory.mktemp("mock-tls")
    now = datetime.datetime.now(datetime.timezone.utc)

    def name(cn):
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder().subject_name(name("arrowsql mock CA")).issuer_name(name("arrowsql mock CA"))
        .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = (
        x509.CertificateBuilder().subject_name(name("localhost")).issuer_name(ca.subject)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=30))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    ca_pem = root / "ca.pem"
    ca_pem.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    if sys.platform == "win32":
        from cryptography.hazmat.primitives.serialization import pkcs12

        pfx = root / "server.pfx"
        pfx.write_bytes(pkcs12.serialize_key_and_certificates(
            b"mock", key, leaf, [ca], serialization.BestAvailableEncryption(b"mock")))
        server_args = ["--pfx", str(pfx), "--pfx-password", "mock"]
    else:
        cert_file, key_file = root / "server.pem", root / "server.key"
        cert_file.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
        key_file.write_bytes(key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        server_args = ["--cert", str(cert_file), "--key", str(key_file)]

    class Material:
        pass

    m = Material()
    m.ca_pem = str(ca_pem)
    m.server_args = server_args
    return m
