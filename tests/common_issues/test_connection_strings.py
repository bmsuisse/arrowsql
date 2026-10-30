"""Connection strings, database selection, login/timeout errors and closed-connection usage.

Common-issue coverage:
- braced ``PWD={...}`` escaping for passwords containing ``;``, ``{``, ``}``,
  ``=`` and spaces (``}`` is escaped as ``}}``), verified with a real login;
- unbraced special characters are rejected loudly instead of silently
  truncating the value;
- ``Database=`` keyword, ``USE`` switching and nonexistent databases;
- bad server, bad host, unknown login and missing SERVER keyword surface as
  clean ``OperationalError`` instead of hanging beyond the login timeout;
- ``connect()`` keyword arguments overriding the connection string;
- usage of closed connections, cursors and the context-manager exit.

Ground truth is the upstream ODBC driver (.venv-upstream).
"""

import re
import time
import uuid

import pytest
from mssql_python import connect
from mssql_python.exceptions import (
    ConnectionStringParseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

# A port nothing listens on. Connection attempts fail until the login timeout.
UNUSED_PORT_SERVER = "localhost,1999"
# Generous upper bound: login timeout is 5s in these tests; anything near a
# minute means the driver hung instead of failing cleanly.
NO_HANG_SECONDS = 30


def _set_keyword(conn_str, key, value):
    """Return ``conn_str`` with ``key`` replaced (or appended) as ``value``."""
    kept = [
        part
        for part in conn_str.split(";")
        if part and part.split("=", 1)[0].strip().lower() != key.lower()
    ]
    kept.append(f"{key}={value}")
    return ";".join(kept)


def _server_part(conn_str):
    match = re.search(r"(?i)(?:^|;)\s*Server\s*=\s*([^;]*)", conn_str)
    assert match, "conn_str must contain a Server keyword"
    return match.group(1).strip()


def _brace(value):
    """Wrap a password in braces, doubling any closing brace inside it."""
    return "{" + value.replace("}", "}}") + "}"


def _drop_login(admin, name):
    """Drop a temporary login, killing its pooled sessions first.

    Closed connections stay in the driver's connection pool (pooling is on by
    default), so DROP LOGIN would otherwise fail with "currently logged in".
    """
    cur = admin.cursor()
    spids = [
        row[0]
        for row in cur.execute(
            "SELECT session_id FROM sys.dm_exec_sessions WHERE login_name = ?", name
        ).fetchall()
    ]
    for spid in spids:
        cur.execute(f"KILL {int(spid)}")
    cur.execute(f"DROP LOGIN [{name}]")


@pytest.fixture
def admin(conn_str):
    """Autocommit connection used for server-level setup (CREATE LOGIN)."""
    connection = connect(conn_str, autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def special_login(admin):
    """A temporary SQL login whose password contains ; { } = and spaces."""
    name = f"ci_cs_{uuid.uuid4().hex[:10]}"
    password = "Pa;ss{w}=d e 9x"
    admin.cursor().execute(
        f"CREATE LOGIN [{name}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF"
    )
    yield name, password
    _drop_login(admin, name)


def _login_name_via(conn_str_for_login):
    connection = connect(conn_str_for_login)
    try:
        return connection.cursor().execute("SELECT SUSER_SNAME()").fetchone()[0]
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Password escaping in the connection string
# ---------------------------------------------------------------------------


def test_braced_password_with_semicolon_equals_and_spaces_logs_in(
    conn_str, special_login
):
    login, password = special_login
    server = _server_part(conn_str)
    s = (
        f"Server={server};Database=master;Uid={login};"
        f"PWD={_brace(password)};Encrypt=yes;TrustServerCertificate=yes"
    )

    assert _login_name_via(s) == login


def test_braced_password_with_braces_doubled_logs_in(conn_str, admin):
    # Password containing a closing brace: it must be written as '}}' inside the braces.
    login = f"ci_cs_{uuid.uuid4().hex[:10]}"
    password = "Br{ace}s}x;9Zq"
    admin.cursor().execute(
        f"CREATE LOGIN [{login}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF"
    )
    try:
        server = _server_part(conn_str)
        s = (
            f"Server={server};Database=master;Uid={login};"
            f"PWD={_brace(password)};Encrypt=yes;TrustServerCertificate=yes"
        )
        assert _login_name_via(s) == login
    finally:
        _drop_login(admin, login)


def test_unbraced_password_with_semicolon_is_rejected_not_truncated(conn_str):
    # Without braces, ';' ends the value. The driver must raise a parse error
    # rather than silently connecting with a truncated password.
    server = _server_part(conn_str)
    s = f"Server={server};Uid=sa;PWD=Pa;ss9x;Encrypt=yes;TrustServerCertificate=yes"

    with pytest.raises(ConnectionStringParseError) as info:
        connect(s)
    assert "ss9x" in str(info.value)


def test_wrong_password_for_existing_login_is_operational_error(
    conn_str, special_login
):
    login, _password = special_login
    server = _server_part(conn_str)
    s = (
        f"Server={server};Database=master;Uid={login};"
        f"PWD={_brace('definitely;not=the{pw}')};Encrypt=yes;TrustServerCertificate=yes"
    )

    with pytest.raises(OperationalError) as info:
        connect(s)
    assert "Login failed" in str(info.value)


# ---------------------------------------------------------------------------
# Database keyword and switching
# ---------------------------------------------------------------------------


def test_database_keyword_sets_initial_database(conn_str):
    s = _set_keyword(conn_str, "Database", "tempdb")
    connection = connect(s)
    try:
        name = connection.cursor().execute("SELECT DB_NAME()").fetchone()[0]
    finally:
        connection.close()

    assert name == "tempdb"


def test_use_statement_switches_database_on_same_connection(conn):
    cur = conn.cursor()
    original = cur.execute("SELECT DB_NAME()").fetchone()[0]

    cur.execute("USE tempdb")
    assert cur.execute("SELECT DB_NAME()").fetchone()[0] == "tempdb"

    cur.execute(f"USE [{original}]")
    assert cur.execute("SELECT DB_NAME()").fetchone()[0] == original


def test_nonexistent_database_raises_operational_error(conn_str):
    s = _set_keyword(conn_str, "Database", f"ci_no_such_db_{uuid.uuid4().hex[:8]}")

    with pytest.raises(OperationalError):
        connect(s, timeout=5)


# ---------------------------------------------------------------------------
# Invalid server / login: clean exceptions, bounded by the login timeout
# ---------------------------------------------------------------------------


def test_unreachable_port_raises_operational_error_within_timeout(conn_str):
    s = _set_keyword(conn_str, "Server", UNUSED_PORT_SERVER)
    started = time.monotonic()

    with pytest.raises(OperationalError) as info:
        connect(s, timeout=5)

    elapsed = time.monotonic() - started
    assert "unable to establish connection" in str(info.value)
    assert elapsed < NO_HANG_SECONDS


def test_unresolvable_host_raises_operational_error_within_timeout(conn_str):
    s = _set_keyword(conn_str, "Server", "ci-no-such-host.invalid")
    started = time.monotonic()

    with pytest.raises(OperationalError) as info:
        connect(s, timeout=5)

    elapsed = time.monotonic() - started
    assert "unable to establish connection" in str(info.value)
    assert elapsed < NO_HANG_SECONDS


def test_unknown_login_raises_operational_error_quickly(conn_str):
    s = _set_keyword(conn_str, "Uid", f"ci_nobody_{uuid.uuid4().hex[:8]}")
    started = time.monotonic()

    with pytest.raises(OperationalError) as info:
        connect(s, timeout=10)

    assert "Login failed" in str(info.value)
    assert time.monotonic() - started < NO_HANG_SECONDS


def test_missing_server_keyword_raises_operational_error():
    with pytest.raises(OperationalError) as info:
        connect("Uid=sa;PWD=x;Encrypt=yes;TrustServerCertificate=yes", timeout=5)

    assert "SERVER keyword" in str(info.value)


def test_unknown_connection_string_keyword_raises_parse_error(conn_str):
    s = conn_str + ";CiBogusKeyword=1"

    with pytest.raises(ConnectionStringParseError) as info:
        connect(s)
    assert "cibogus" in str(info.value).lower()


# ---------------------------------------------------------------------------
# Keyword syntax and connect() keyword overrides
# ---------------------------------------------------------------------------


def test_connection_string_keywords_are_case_insensitive(conn_str):
    lowered = (
        conn_str.replace("Server=", "server=")
        .replace("Database=", "database=")
        .replace("Uid=", "uid=")
        .replace("Pwd=", "pwd=")
    )
    connection = connect(lowered)
    try:
        assert connection.cursor().execute("SELECT 1").fetchone()[0] == 1
    finally:
        connection.close()


def test_whitespace_around_equals_is_accepted(conn_str):
    spaced = conn_str.replace("=", " = ")
    connection = connect(spaced)
    try:
        assert connection.cursor().execute("SELECT 2").fetchone()[0] == 2
    finally:
        connection.close()


def test_connect_database_kwarg_overrides_connection_string(conn_str):
    s = _set_keyword(conn_str, "Database", "master")
    connection = connect(s, database="tempdb")
    try:
        assert connection.cursor().execute("SELECT DB_NAME()").fetchone()[0] == "tempdb"
    finally:
        connection.close()


def test_connect_autocommit_kwarg_overrides_default(conn_str):
    default = connect(conn_str)
    overridden = connect(conn_str, autocommit=True)
    try:
        assert default.autocommit is False
        assert overridden.autocommit is True
    finally:
        default.close()
        overridden.close()


# ---------------------------------------------------------------------------
# Closed connections and cursors
# ---------------------------------------------------------------------------


def test_cursor_on_closed_connection_raises_interface_error(conn_str):
    connection = connect(conn_str)
    connection.close()

    assert connection.closed is True
    with pytest.raises(InterfaceError) as info:
        connection.cursor()
    assert "closed connection" in str(info.value)


def test_commit_and_rollback_on_closed_connection_raise_interface_error(conn_str):
    connection = connect(conn_str)
    connection.close()

    with pytest.raises(InterfaceError) as commit_info:
        connection.commit()
    with pytest.raises(InterfaceError) as rollback_info:
        connection.rollback()

    assert "closed connection" in str(commit_info.value)
    assert "closed connection" in str(rollback_info.value)


def test_close_is_idempotent_on_connection(conn_str):
    connection = connect(conn_str)
    connection.close()
    connection.close()

    assert connection.closed is True


def test_cursor_created_before_close_raises_programming_error(conn_str):
    connection = connect(conn_str)
    cur = connection.cursor()
    cur.execute("SELECT 1")
    connection.close()

    with pytest.raises(ProgrammingError) as info:
        cur.execute("SELECT 1")
    assert "cursor is closed" in str(info.value)


def test_cursor_close_is_idempotent(conn):
    cur = conn.cursor()
    cur.close()
    cur.close()

    with pytest.raises(ProgrammingError):
        cur.execute("SELECT 1")


def test_context_manager_closes_connection_on_exit(conn_str):
    with connect(conn_str) as connection:
        assert connection.closed is False
        assert connection.cursor().execute("SELECT 3").fetchone()[0] == 3

    assert connection.closed is True
    with pytest.raises(InterfaceError):
        connection.cursor()
