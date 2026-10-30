"""Database context: USE inside execute, DB_NAME(), getinfo, three-part names.

Failure modes a native-TDS reimplementation can easily get wrong:

- ``USE <db>`` sent through ``execute()`` must actually switch the session's
  database, and everything that reads the current database (``DB_NAME()``,
  ``getinfo(SQL_DATABASE_NAME)``, ``SQL_ATTR_CURRENT_CATALOG``) must follow it.
  A driver that caches the database name at connect time gets this wrong.
- ``USE`` produces an informational message and no result set. A ``USE`` in
  front of a ``SELECT`` in one batch needs ``nextset()`` before the rows are
  reachable; a driver that treats the first token stream as the result set
  breaks.
- A failed ``USE`` must leave the connection and cursor usable and must not
  change the current database.
- ``ROLLBACK`` does not undo a database switch; a new connection starts in the
  database from the connection string, not in the one a previous session
  switched to (pool hazard).
- Three-part names (``db.schema.object``) resolve against any database on the
  server, independent of the current database, including DML that commits
  through the connection's transaction.
- ``USE`` takes an identifier, not a parameter.

Sources (research, issue trackers and docs):
- https://github.com/mkleehammer/pyodbc/issues/647 (USE before SELECT in one batch: "No results. Previous SQL was not a query."; nextset() is the workaround)
- https://github.com/mkleehammer/pyodbc/issues/413 (users run USE after connecting, and need qualified names even with Database= set)
- https://github.com/mkleehammer/pyodbc/issues/858 (USE and "Changed database context" info message on Linux/unixODBC)
- https://github.com/mkleehammer/pyodbc/issues/90 (USE of a database that does not exist, on the client's connection)
- https://learn.microsoft.com/en-us/SQL/connect/python/mssql-python/connection-management (SQL_ATTR_CURRENT_CATALOG, getinfo SQL_DATABASE_NAME)
- https://github.com/microsoft/mssql-python/releases (fix for decoding the value returned by getinfo(SQL_DATABASE_NAME))
- https://github.com/bartbeecoders/sqail/issues/57 (USE and other session state leaking to the next pooled connection; no sp_reset_connection in tiberius)
- https://github.com/prisma/tiberius/issues/281 (server "Changed database context" info messages surfaced by a Rust driver)
"""

import re
import uuid

import mssql_python
import pytest

from mssql_python import connect


def _default_database(conn_str: str) -> str:
    match = re.search(r"Database=([^;]+)", conn_str, re.IGNORECASE)
    assert match, "connection string must name a Database"
    return match.group(1)


def _master_conn_str(conn_str: str) -> str:
    return re.sub(r"Database=[^;]+", "Database=master", conn_str, flags=re.IGNORECASE)


def _rows(cursor):
    return [tuple(row) for row in cursor.fetchall()]


def _scalar(cursor, sql, params=None):
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)
    return cursor.fetchone()[0]


def test_db_name_matches_connection_string_database(conn_str, cursor):
    expected = _default_database(conn_str)
    value = _scalar(cursor, "SELECT DB_NAME()")
    assert type(value) is str
    assert value == expected


def test_getinfo_database_name_matches_db_name_on_connect(conn, cursor):
    info = conn.getinfo(mssql_python.SQL_DATABASE_NAME)
    assert type(info) is str
    assert info == _scalar(cursor, "SELECT DB_NAME()")


def test_use_changes_db_name_and_getinfo(conn, cursor):
    cursor.execute("USE master")
    assert _scalar(cursor, "SELECT DB_NAME()") == "master"
    assert conn.getinfo(mssql_python.SQL_DATABASE_NAME) == "master"


def test_use_produces_no_result_set_and_unknown_rowcount(cursor):
    cursor.execute("USE master")
    assert cursor.description is None
    assert cursor.rowcount == -1


def test_use_accepts_bracketed_identifier(conn, cursor):
    cursor.execute("USE [master]")
    assert _scalar(cursor, "SELECT DB_NAME()") == "master"
    assert conn.getinfo(mssql_python.SQL_DATABASE_NAME) == "master"


def test_use_nonexistent_database_raises_and_keeps_connection_usable(conn_str, conn, cursor):
    original = _default_database(conn_str)
    db_error = None
    try:
        cursor.execute("USE rs_no_such_db_" + uuid.uuid4().hex[:10])
    except mssql_python.DatabaseError as exc:
        db_error = exc
    assert db_error is not None, "USE of a missing database must raise a DB-API error"
    assert _scalar(cursor, "SELECT DB_NAME()") == original
    assert conn.getinfo(mssql_python.SQL_DATABASE_NAME) == original


@pytest.mark.upstream_bug(
    reason=(
        "A USE of a missing database is a name/syntax-level server error (DB-API "
        "ProgrammingError), but upstream raises OperationalError with the text "
        "'Server rejected the connection', although the connection is still open "
        "and usable afterwards."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_upstream_bug_use_missing_database_is_programming_error(cursor):
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("USE rs_no_such_db_" + uuid.uuid4().hex[:10])


def test_use_then_select_in_one_batch_requires_nextset(conn_str):
    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("USE master; SELECT DB_NAME() AS n")
        with pytest.raises(mssql_python.ProgrammingError):
            cursor.fetchall()
    finally:
        conn.close()

    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("USE master; SELECT DB_NAME() AS n")
        assert cursor.nextset() is True
        assert [c[0] for c in cursor.description] == ["n"]
        assert _rows(cursor) == [("master",)]
    finally:
        conn.close()


def test_failed_use_inside_batch_leaves_database_unchanged(conn_str, cursor):
    original = _default_database(conn_str)
    with pytest.raises(mssql_python.DatabaseError):
        cursor.execute("USE rs_no_such_db_" + uuid.uuid4().hex[:10])
    assert _scalar(cursor, "SELECT DB_NAME()") == original


def test_temp_table_survives_use(conn, cursor):
    cursor.execute("CREATE TABLE #ctx_probe (x INT NOT NULL)")
    cursor.execute("INSERT INTO #ctx_probe (x) VALUES (?)", [41])
    cursor.execute("USE master")
    assert _scalar(cursor, "SELECT DB_NAME()") == "master"
    cursor.execute("SELECT x + 1 FROM #ctx_probe")
    assert _rows(cursor) == [(42,)]


def test_rollback_does_not_restore_database(conn_str, conn, cursor):
    original = _default_database(conn_str)
    assert conn.autocommit is False
    cursor.execute("USE master")
    conn.rollback()
    assert _scalar(cursor, "SELECT DB_NAME()") == "master"
    assert conn.getinfo(mssql_python.SQL_DATABASE_NAME) == "master"
    assert original != "master"


def test_new_connection_starts_in_connection_string_database(conn_str):
    original = _default_database(conn_str)
    first = connect(conn_str)
    try:
        first.cursor().execute("USE master")
    finally:
        first.close()
    second = connect(conn_str)
    try:
        assert _scalar(second.cursor(), "SELECT DB_NAME()") == original
        assert second.getinfo(mssql_python.SQL_DATABASE_NAME) == original
    finally:
        second.close()


def test_use_works_in_autocommit_connection(conn_str):
    original = _default_database(conn_str)
    conn = connect(conn_str, autocommit=True)
    try:
        cursor = conn.cursor()
        cursor.execute("USE master")
        assert _scalar(cursor, "SELECT DB_NAME()") == "master"
        assert conn.getinfo(mssql_python.SQL_DATABASE_NAME) == "master"
        assert conn.autocommit is True
    finally:
        conn.close()
    assert original != "master"


def test_set_attr_current_catalog_switches_database(conn):
    conn.set_attr(mssql_python.SQL_ATTR_CURRENT_CATALOG, "tempdb")
    cursor = conn.cursor()
    assert _scalar(cursor, "SELECT DB_NAME()") == "tempdb"
    assert conn.getinfo(mssql_python.SQL_DATABASE_NAME) == "tempdb"


def test_three_part_name_resolves_from_any_current_database(conn_str, conn, cursor):
    original = _default_database(conn_str)
    cursor.execute("USE master")
    name = _scalar(
        cursor, f"SELECT name FROM [{original}].sys.databases WHERE database_id = DB_ID(?)", [original]
    )
    assert name == original
    assert _scalar(cursor, "SELECT name FROM master.sys.databases WHERE database_id = 1") == "master"


def test_db_name_with_database_id_and_null_for_unknown_id(cursor):
    assert _scalar(cursor, "SELECT DB_NAME(999999)") is None
    assert _scalar(cursor, "SELECT DB_NAME(DB_ID('master'))") == "master"
    assert type(_scalar(cursor, "SELECT DB_ID('master')")) is int


def test_cross_database_insert_and_read_with_three_part_names(conn_str, conn, cursor):
    """Permanent table in a second database, written and read through three-part names
    from a connection whose current database is the default one, then committed."""
    other_db = "rs_ctx_xdb_" + uuid.uuid4().hex[:10]
    table = f"ctx_t_{uuid.uuid4().hex[:10]}"
    admin = connect(_master_conn_str(conn_str), autocommit=True)
    try:
        admin.cursor().execute(f"CREATE DATABASE [{other_db}]")
        cursor.execute(f"CREATE TABLE [{other_db}].dbo.[{table}] (id INT NOT NULL, label NVARCHAR(40) NOT NULL)")
        cursor.execute(f"INSERT INTO [{other_db}].dbo.[{table}] (id, label) VALUES (?, ?)", [7, "héllo"])
        conn.commit()
        cursor.execute(f"SELECT id, label FROM [{other_db}].dbo.[{table}]")
        assert _rows(cursor) == [(7, "héllo")]
        assert _scalar(cursor, "SELECT DB_NAME()") == _default_database(conn_str)
    finally:
        # Close the test connection first; SINGLE_USER ROLLBACK IMMEDIATE below would kill it.
        conn.close()
        # SINGLE_USER ROLLBACK IMMEDIATE drops any pooled session still attached to the database.
        admin.cursor().execute(
            f"IF DB_ID(N'{other_db}') IS NOT NULL BEGIN "
            f"ALTER DATABASE [{other_db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE [{other_db}]; END"
        )
        admin.close()
