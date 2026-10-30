"""Session SET options the driver establishes, read back via @@OPTIONS and sys.dm_exec_sessions.

Sources (research):

* Microsoft, SET Options That Affect Results (ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS,
  ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER, NUMERIC_ROUNDABORT and the
  @@OPTIONS bitmask): https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175088(v=sql.105)
* Microsoft, Create Indexed Views (required SET values per client library, ARITHABORT
  failure "INSERT failed because the following SET options have incorrect settings"):
  https://learn.microsoft.com/en-us/sql/relational-databases/views/create-indexed-views?view=sql-server-ver17
* Microsoft, Indexes on computed columns (same SET requirements):
  https://learn.microsoft.com/en-us/sql/relational-databases/indexes/indexes-on-computed-columns?view=sql-server-ver17
* Brent Ozar, Checking for Strange Client Settings with sys.dm_exec_sessions (ODBC/OLE DB
  clients often connect with ARITHABORT OFF, unlike SSMS):
  https://www.brentozar.com/archive/2017/03/checking-strange-client-settings-sys-dm_exec_sessions/
* VladDBA, Dangerous SET options stored procedures inherit from your session (procedures
  keep the options they were created with):
  https://vladdba.com/2025/03/03/dangerous-set-options-stored-procedures/
* jwcooney, "SELECT failed because the following SET options have incorrect settings:
  'ARITHABORT'": https://jwcooney.com/2013/10/29/correcting-select-failed-because-the-following-set-options-have-incorrect-settings-arithabort/
* SQLServerCentral, Set options with indexed views:
  https://www.sqlservercentral.com/forums/topic/set-options-with-indexed-views
* pyodbc issue tracker (mkleehammer/pyodbc) searched for SET-option reports: no direct
  ARITHABORT/QUOTED_IDENTIFIER issue found; pyodbc exposes only autocommit and timeout as
  connection attributes, so session SET state must be reached through T-SQL.
* No microsoft/mssql-python, prisma/tiberius or pymssql issue specific to these options was
  found in the search index.

Behaviors a native-TDS reimplementation can get wrong: the connection-level defaults set
during login (the ODBC defaults, not the server defaults) and the reset of session state on
a new connection.
"""

import time

import pytest
from mssql_python import DatabaseError, DataError, ProgrammingError, connect

# @@OPTIONS bitmask values (SET Options That Affect Results).
OPT_ANSI_WARNINGS = 8
OPT_ANSI_PADDING = 16
OPT_ANSI_NULLS = 32
OPT_ARITHABORT = 64
OPT_QUOTED_IDENTIFIER = 256
OPT_NOCOUNT = 512
OPT_ANSI_NULL_DFLT_ON = 1024
OPT_CONCAT_NULL_YIELDS_NULL = 4096
OPT_NUMERIC_ROUNDABORT = 8192
OPT_XACT_ABORT = 16384

# sys.dm_exec_sessions column -> (@@OPTIONS bit, expected default on an ODBC-style connection)
DM_OPTION_BITS = [
    ("ansi_nulls", OPT_ANSI_NULLS, True),
    ("ansi_padding", OPT_ANSI_PADDING, True),
    ("ansi_warnings", OPT_ANSI_WARNINGS, True),
    ("arithabort", OPT_ARITHABORT, False),
    ("concat_null_yields_null", OPT_CONCAT_NULL_YIELDS_NULL, True),
    ("quoted_identifier", OPT_QUOTED_IDENTIFIER, True),
    ("ansi_null_dflt_on", OPT_ANSI_NULL_DFLT_ON, True),
]


def _session_row(cursor):
    """Return the current session's SET state as a dict (sys.dm_exec_sessions)."""
    cursor.execute(
        """
        SELECT ansi_nulls, ansi_padding, ansi_warnings, arithabort,
               concat_null_yields_null, quoted_identifier, ansi_null_dflt_on,
               date_first, language, lock_timeout, text_size,
               transaction_isolation_level
        FROM sys.dm_exec_sessions
        WHERE session_id = @@SPID
        """
    )
    row = cursor.fetchone()
    names = [d[0] for d in cursor.description]
    return dict(zip(names, row))


def _options_bitmask(cursor):
    cursor.execute("SELECT @@OPTIONS")
    return int(cursor.fetchone()[0])


def _connect(conn_str):
    return connect(conn_str)


# ---------------------------------------------------------------------------
# Connection defaults established at login
# ---------------------------------------------------------------------------


def test_default_options_bitmask_matches_odbc_defaults(cursor):
    bits = _options_bitmask(cursor)
    assert bits & OPT_ANSI_NULLS
    assert bits & OPT_ANSI_PADDING
    assert bits & OPT_ANSI_WARNINGS
    assert bits & OPT_CONCAT_NULL_YIELDS_NULL
    assert bits & OPT_QUOTED_IDENTIFIER
    assert bits & OPT_ANSI_NULL_DFLT_ON
    assert not bits & OPT_ARITHABORT
    assert not bits & OPT_NUMERIC_ROUNDABORT
    assert not bits & OPT_XACT_ABORT
    assert not bits & OPT_NOCOUNT


def test_dm_exec_sessions_agrees_with_options_bitmask(cursor):
    row = _session_row(cursor)
    bits = _options_bitmask(cursor)
    for dm_col, bit, expected in DM_OPTION_BITS:
        assert row[dm_col] is expected, dm_col
        assert bool(bits & bit) is expected, dm_col


def test_default_datefirst_language_and_dateformat(cursor):
    row = _session_row(cursor)
    assert row["date_first"] == 7  # Sunday, US default
    assert row["language"] == "us_english"
    cursor.execute("SELECT @@DATEFIRST, DATEPART(weekday, '2026-01-05'), DATENAME(month, '2026-03-01')")
    datefirst, weekday, month = cursor.fetchone()
    assert datefirst == 7
    assert weekday == 2  # Monday is 2 when Sunday is day 1
    assert month == "March"


def test_default_lock_timeout_and_textsize(cursor):
    row = _session_row(cursor)
    assert row["lock_timeout"] == -1  # wait indefinitely
    assert row["text_size"] == -1
    cursor.execute("SELECT @@LOCK_TIMEOUT, @@TEXTSIZE")
    assert tuple(cursor.fetchone()) == (-1, -1)


def test_default_isolation_is_read_committed(cursor):
    row = _session_row(cursor)
    assert row["transaction_isolation_level"] == 2  # READ COMMITTED


def test_session_state_does_not_leak_between_connections(conn_str):
    a = _connect(conn_str)
    b = _connect(conn_str)
    try:
        ca = a.cursor()
        cb = b.cursor()
        ca.execute("SET ARITHABORT ON; SET DATEFIRST 1; SET LANGUAGE German; SET LOCK_TIMEOUT 1234")
        ca.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        assert _session_row(ca)["arithabort"] is True
        assert _session_row(ca)["date_first"] == 1
        assert _session_row(ca)["language"] == "Deutsch"

        row_b = _session_row(cb)
        assert row_b["arithabort"] is False
        assert row_b["date_first"] == 7
        assert row_b["language"] == "us_english"
        assert row_b["lock_timeout"] == -1
        assert row_b["transaction_isolation_level"] == 2
    finally:
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# SET options changed on the session and read back
# ---------------------------------------------------------------------------


def test_arithabort_set_on_is_reflected_in_dm_and_bitmask(cursor):
    cursor.execute("SET ARITHABORT ON")
    assert _session_row(cursor)["arithabort"] is True
    assert _options_bitmask(cursor) & OPT_ARITHABORT
    cursor.execute("SET ARITHABORT OFF")
    assert _session_row(cursor)["arithabort"] is False
    assert not _options_bitmask(cursor) & OPT_ARITHABORT


def test_set_isolation_level_serializable_then_reset_on_new_connection(conn_str):
    c1 = _connect(conn_str)
    try:
        cur = c1.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        assert _session_row(cur)["transaction_isolation_level"] == 4
    finally:
        c1.close()
    c2 = _connect(conn_str)
    try:
        assert _session_row(c2.cursor())["transaction_isolation_level"] == 2
    finally:
        c2.close()


def test_datefirst_set_is_session_scoped(cursor):
    cursor.execute("SET DATEFIRST 1")
    cursor.execute("SELECT @@DATEFIRST, DATEPART(weekday, '2026-01-05')")
    datefirst, weekday = cursor.fetchone()
    assert datefirst == 1
    assert weekday == 1  # Monday is day 1 when Monday starts the week
    assert _session_row(cursor)["date_first"] == 1


def test_language_set_changes_datename_and_is_session_scoped(conn_str):
    c1 = _connect(conn_str)
    try:
        cur = c1.cursor()
        cur.execute("SET LANGUAGE German")
        cur.execute("SELECT DATENAME(month, '2026-03-01')")
        assert cur.fetchone()[0] == "März"
        assert _session_row(cur)["language"] == "Deutsch"
    finally:
        c1.close()
    c2 = _connect(conn_str)
    try:
        c2_cur = c2.cursor()
        c2_cur.execute("SELECT DATENAME(month, '2026-03-01')")
        assert c2_cur.fetchone()[0] == "March"
    finally:
        c2.close()


def test_set_lock_timeout_makes_blocked_select_fail(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int PRIMARY KEY, v int)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, v) VALUES (1, 10)")
    cursor.connection.commit()

    blocker = _connect(conn_str)
    blocked = _connect(conn_str)
    try:
        bcur = blocker.cursor()
        bcur.execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")  # holds X lock, open tran

        ccur = blocked.cursor()
        ccur.execute("SET LOCK_TIMEOUT 500")
        assert _session_row(ccur)["lock_timeout"] == 500

        started = time.monotonic()
        # Upstream surfaces lock timeout (error 1222) as ProgrammingError, not OperationalError.
        with pytest.raises(ProgrammingError, match="Lock request time out"):
            ccur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
        elapsed = time.monotonic() - started
        assert elapsed < 10
    finally:
        blocker.rollback()
        blocker.close()
        blocked.close()


# ---------------------------------------------------------------------------
# Behavior the options drive
# ---------------------------------------------------------------------------


def test_ansi_nulls_off_makes_equals_null_match_null_rows(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int, v int NULL)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, v) VALUES (1, NULL), (2, 5)")
    cursor.connection.commit()

    cursor.execute(f"SELECT id FROM dbo.{table_name} WHERE v = NULL")
    assert cursor.fetchall() == []  # ANSI_NULLS ON: = NULL never matches

    cursor.execute("SET ANSI_NULLS OFF")
    assert _session_row(cursor)["ansi_nulls"] is False
    cursor.execute(f"SELECT id FROM dbo.{table_name} WHERE v = NULL")
    assert [r[0] for r in cursor.fetchall()] == [1]


def test_concat_null_yields_null_off_treats_null_as_empty(cursor):
    cursor.execute("SELECT 'a' + NULL")
    assert cursor.fetchone()[0] is None  # default ON

    cursor.execute("SET CONCAT_NULL_YIELDS_NULL OFF")
    cursor.execute("SELECT 'a' + NULL")
    assert cursor.fetchone()[0] == "a"
    assert _session_row(cursor)["concat_null_yields_null"] is False


@pytest.mark.upstream_bug(
    reason=(
        "SQL Server error 8152 (SQLSTATE 22001, string or binary data would be truncated) "
        "is surfaced as a generic ProgrammingError (SQLSTATE 42000 'Syntax error or access "
        "violation'); the driver's own SQLSTATE table maps 22001 to DataError, which is what "
        "DB-API 2.0 requires for data errors"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_ansi_warnings_default_raises_dataerror_on_truncation(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v varchar(3))")
    cursor.connection.commit()
    with pytest.raises(DataError, match="truncated"):
        cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", ["abcdef"])


def test_ansi_warnings_default_rejects_truncation(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v varchar(3))")
    cursor.connection.commit()
    with pytest.raises(DatabaseError, match="would be truncated"):
        cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", ["abcdef"])
    cursor.connection.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


def test_ansi_warnings_off_silently_truncates(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v varchar(3))")
    cursor.connection.commit()
    cursor.execute("SET ANSI_WARNINGS OFF")
    assert _session_row(cursor)["ansi_warnings"] is False
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", ["abcdef"])
    cursor.execute(f"SELECT v FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == "abc"


def test_ansi_padding_keeps_trailing_spaces(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (c char(5), v varchar(10))")
    cursor.execute(f"INSERT INTO dbo.{table_name} (c, v) VALUES (?, ?)", ["ab", "ab  "])
    cursor.connection.commit()
    cursor.execute(f"SELECT c, v, DATALENGTH(c), DATALENGTH(v) FROM dbo.{table_name}")
    c, v, dlc, dlv = cursor.fetchone()
    assert c == "ab   "
    assert v == "ab  "
    assert (dlc, dlv) == (5, 4)


def test_quoted_identifier_default_treats_double_quotes_as_identifiers(cursor):
    with pytest.raises(ProgrammingError, match="Invalid column name"):
        cursor.execute('SELECT "not_a_column_here"')


def test_quoted_identifier_off_treats_double_quotes_as_literals(cursor):
    cursor.execute("SET QUOTED_IDENTIFIER OFF")
    assert _session_row(cursor)["quoted_identifier"] is False
    cursor.execute('SELECT "literal text"')
    assert cursor.fetchone()[0] == "literal text"


def _temp_col_is_nullable(cursor, tmp_name, col):
    # COLUMNPROPERTY resolves against the current database, so read tempdb.sys.columns.
    cursor.execute(
        "SELECT is_nullable FROM tempdb.sys.columns "
        "WHERE object_id = OBJECT_ID(?) AND name = ?",
        [f"tempdb..{tmp_name}", col],
    )
    return cursor.fetchone()[0]


def test_ansi_null_dflt_on_makes_new_columns_nullable(cursor):
    cursor.execute("CREATE TABLE #dflt (a int)")
    assert _temp_col_is_nullable(cursor, "#dflt", "a") is True

    cursor.execute("SET ANSI_NULL_DFLT_OFF ON")
    cursor.execute("CREATE TABLE #dflt2 (a int)")
    assert _temp_col_is_nullable(cursor, "#dflt2", "a") is False


# ---------------------------------------------------------------------------
# Options that matter for indexed views and indexes on computed columns
# ---------------------------------------------------------------------------


def test_computed_column_index_default_session_succeeds(cursor, table_name):
    """ODBC-style default (ARITHABORT OFF) works because ANSI_WARNINGS ON implies ARITHABORT at compat >= 90."""
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int PRIMARY KEY, a int, b AS a * 2 PERSISTED)"
    )
    cursor.execute(f"CREATE INDEX ix_{table_name} ON dbo.{table_name} (b)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, a) VALUES (1, 21)")
    cursor.execute(f"SELECT b FROM dbo.{table_name} WHERE b = 42")
    assert cursor.fetchone()[0] == 42
    cursor.connection.commit()


def test_computed_column_index_rejected_when_ansi_warnings_off(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int PRIMARY KEY, a int, b AS a * 2 PERSISTED)"
    )
    cursor.connection.commit()
    cursor.execute("SET ANSI_WARNINGS OFF")
    with pytest.raises(ProgrammingError, match="SET options.*'ANSI_WARNINGS'"):
        cursor.execute(f"CREATE INDEX ix_{table_name} ON dbo.{table_name} (b)")
    cursor.connection.rollback()


def test_computed_column_index_succeeds_with_arithabort_on(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int PRIMARY KEY, a int, b AS a * 2 PERSISTED)"
    )
    cursor.connection.commit()
    cursor.execute("SET ARITHABORT ON")
    cursor.execute(f"CREATE INDEX ix_{table_name} ON dbo.{table_name} (b)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, a) VALUES (1, 21)")
    cursor.execute(f"SELECT b FROM dbo.{table_name} WHERE b = 42")
    assert cursor.fetchone()[0] == 42
    cursor.connection.commit()


def _make_indexed_view(cursor, table_name):
    view = f"{table_name}_v"
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int PRIMARY KEY, grp int NOT NULL, amt int NOT NULL)"
    )
    cursor.execute("SET ARITHABORT ON")
    cursor.execute(
        f"""
        CREATE VIEW dbo.{view} WITH SCHEMABINDING AS
        SELECT grp, COUNT_BIG(*) AS n, SUM(amt) AS total
        FROM dbo.{table_name}
        GROUP BY grp
        """
    )
    cursor.execute(f"CREATE UNIQUE CLUSTERED INDEX ix_{view} ON dbo.{view} (grp)")
    cursor.connection.commit()
    return view


def test_indexed_view_maintained_by_default_session(conn_str, cursor, table_name):
    view = _make_indexed_view(cursor, table_name)
    try:
        writer = _connect(conn_str)
        try:
            wcur = writer.cursor()
            wcur.execute(f"INSERT INTO dbo.{table_name} (id, grp, amt) VALUES (1, 1, 5), (2, 1, 7)")
            writer.commit()
            wcur.execute(f"SELECT n, total FROM dbo.{view} WITH (NOEXPAND) WHERE grp = 1")
            assert tuple(wcur.fetchone()) == (2, 12)
        finally:
            writer.close()
    finally:
        cursor.execute(f"DROP VIEW IF EXISTS dbo.{view}")
        cursor.connection.commit()


def test_indexed_view_dml_rejected_when_ansi_warnings_off(conn_str, cursor, table_name):
    view = _make_indexed_view(cursor, table_name)
    try:
        writer = _connect(conn_str)
        try:
            wcur = writer.cursor()
            wcur.execute("SET ANSI_WARNINGS OFF")
            with pytest.raises(ProgrammingError, match="SET options.*'ANSI_WARNINGS'"):
                wcur.execute(f"INSERT INTO dbo.{table_name} (id, grp, amt) VALUES (1, 1, 5)")
            writer.rollback()
        finally:
            writer.close()
    finally:
        cursor.execute(f"DROP VIEW IF EXISTS dbo.{view}")
        cursor.connection.commit()
