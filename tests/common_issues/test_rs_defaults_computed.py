"""DEFAULT constraints and computed columns.

Research sources (issue trackers and docs consulted; findings noted per test):

* https://learn.microsoft.com/en-us/sql/relational-databases/indexes/indexes-on-computed-columns
  Indexes on computed columns require the connection that creates them, and
  every connection that modifies the indexed values, to use the ANSI-compatible
  SET options (ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT,
  CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON; NUMERIC_ROUNDABORT OFF). A
  SELECT from a connection with wrong options silently ignores the index.
* https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008/ms191250(v=sql.100)
  Computed columns: a PERSISTED column is needed for indexes on non-precise
  expressions and for use in CHECK / FOREIGN KEY / NOT NULL constraints.
* https://social.technet.microsoft.com/Forums/windowsserver/en-US/d24f7e32-4632-484c-8c58-c194837fbd71/insert-failed-because-the-following-set-options-have-incorrect-settings-arithabort
  "INSERT failed because the following SET options have incorrect settings:
  'ARITHABORT'": writes to a table with an index on a computed column fail when
  the session's ARITHABORT is OFF. Driver-side fix is to send the right
  options at logon.
* https://siderite.dev/blog/insert-or-update-or-delete-failed.html
  Same error family; the usual driver-side fix is to set the SET options on
  connect. Also documents the 64 bit in @@OPTIONS as the ARITHABORT check.
* https://github.com/yiisoft/yii2/issues/18287
  Inserts into a table with a computed column that calls a non-schema-bound
  function failed when the insert was rewritten to ``OUTPUT inserted.*``
  ("Column 'x' cannot be referenced in the OUTPUT clause..."). Relevant to
  the OUTPUT tests below, which must return server-generated defaults.
* https://github.com/cockroachdb/cockroach/issues/22434
  Discussion of ``DEFAULT`` as the per-column value in a VALUES list, and of
  whether a computed column may be given ``DEFAULT``. SQL Server rejects it
  (see test_default_keyword_into_computed_column_is_rejected).
* https://github.com/mkleehammer/pyodbc/issues/873
  Parameters are always sent as literal values, never as SQL fragments. A
  ``?`` cannot stand in for the ``DEFAULT`` keyword.

Several issue-tracker searches for microsoft/mssql-python returned no
computed-column or DEFAULT-specific reports; the tests below therefore derive
from the server's documented behavior and the SET-option/OUTPUT failure modes
above.
"""

import datetime
import uuid
from decimal import Decimal

import pytest
from mssql_python.exceptions import IntegrityError, ProgrammingError


@pytest.fixture
def defaults_table(cursor, conn):
    cursor.execute(
        "CREATE TABLE #d ("
        " id int IDENTITY(1,1) PRIMARY KEY,"
        " a int NOT NULL DEFAULT 5,"
        " b nvarchar(10) NULL DEFAULT N'x',"
        " f bit NOT NULL DEFAULT 1,"
        " g decimal(10,2) NOT NULL DEFAULT 1.5,"
        " u uniqueidentifier NOT NULL DEFAULT NEWID(),"
        " dt datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),"
        " c AS a * 2 PERSISTED,"
        " d AS a + 1,"
        " n AS b + N'!'"
        ")"
    )
    conn.commit()
    yield cursor


def test_insert_default_values_applies_every_default(defaults_table, conn):
    defaults_table.execute("INSERT INTO #d DEFAULT VALUES")
    assert defaults_table.rowcount == 1
    conn.commit()
    defaults_table.execute("SELECT id, a, b, f, g FROM #d")
    row = tuple(defaults_table.fetchone())
    assert row == (1, 5, "x", True, Decimal("1.50"))
    assert type(row[0]) is int
    assert type(row[1]) is int
    assert type(row[2]) is str
    assert type(row[3]) is bool
    assert type(row[4]) is Decimal


def test_identity_increments_across_default_values_inserts(defaults_table, conn):
    for _ in range(3):
        defaults_table.execute("INSERT INTO #d DEFAULT VALUES")
    conn.commit()
    defaults_table.execute("SELECT id FROM #d ORDER BY id")
    assert [r[0] for r in defaults_table.fetchall()] == [1, 2, 3]


def test_default_values_getdate_style_uses_server_clock_as_naive_datetime(defaults_table, conn):
    before = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=5)
    defaults_table.execute("INSERT INTO #d DEFAULT VALUES")
    conn.commit()
    defaults_table.execute("SELECT dt FROM #d")
    dt = defaults_table.fetchone()[0]
    assert type(dt) is datetime.datetime
    assert dt.tzinfo is None
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    assert before <= dt <= now + datetime.timedelta(minutes=5)


def test_newid_default_yields_distinct_uuid_objects(defaults_table, conn):
    defaults_table.execute("INSERT INTO #d DEFAULT VALUES")
    defaults_table.execute("INSERT INTO #d DEFAULT VALUES")
    conn.commit()
    defaults_table.execute("SELECT u FROM #d ORDER BY id")
    u1, u2 = (r[0] for r in defaults_table.fetchall())
    assert type(u1) is uuid.UUID and type(u2) is uuid.UUID
    assert u1 != u2


def test_default_keyword_in_values_mixed_with_parameter(defaults_table, conn):
    defaults_table.execute("INSERT INTO #d (a, b) VALUES (DEFAULT, ?)", ("hello",))
    conn.commit()
    defaults_table.execute("SELECT a, b FROM #d")
    assert tuple(defaults_table.fetchone()) == (5, "hello")


def test_omitted_column_gets_default_while_explicit_null_stores_null(defaults_table, conn):
    # Column b is nullable with default N'x': omitting it takes the default,
    # but an explicit NULL (bound as None) must store NULL, not the default.
    defaults_table.execute("INSERT INTO #d (a) VALUES (?)", (9,))
    defaults_table.execute("INSERT INTO #d (a, b) VALUES (?, ?)", (9, None))
    conn.commit()
    defaults_table.execute("SELECT b FROM #d ORDER BY id")
    assert [r[0] for r in defaults_table.fetchall()] == ["x", None]


def test_explicit_null_into_not_null_column_with_default_is_rejected(defaults_table, conn):
    # A default only applies when the column is omitted or DEFAULT is used.
    # Binding None into a NOT NULL column must fail, not silently use 5.
    with pytest.raises(IntegrityError):
        defaults_table.execute("INSERT INTO #d (a) VALUES (?)", (None,))
    conn.rollback()
    defaults_table.execute("SELECT COUNT(*) FROM #d")
    assert defaults_table.fetchone()[0] == 0


def test_default_values_on_table_without_defaults_is_rejected(cursor, conn):
    cursor.execute("CREATE TABLE #nodef (a int NOT NULL)")
    conn.commit()
    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #nodef DEFAULT VALUES")
    conn.rollback()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_default_keyword_in_update_resets_column(defaults_table, conn):
    defaults_table.execute("INSERT INTO #d (a, b) VALUES (7, N'zz')")
    conn.commit()
    defaults_table.execute("UPDATE #d SET a = DEFAULT, b = DEFAULT WHERE id = 1")
    assert defaults_table.rowcount == 1
    conn.commit()
    defaults_table.execute("SELECT a, b FROM #d")
    assert tuple(defaults_table.fetchone()) == (5, "x")


def test_persisted_computed_column_returns_expression_value_and_type(defaults_table, conn):
    defaults_table.execute("INSERT INTO #d (a) VALUES (?)", (21,))
    conn.commit()
    defaults_table.execute("SELECT c, d FROM #d")
    c, d = tuple(defaults_table.fetchone())
    assert (c, d) == (42, 22)
    assert type(c) is int and type(d) is int


def test_computed_column_tracks_base_column_update(defaults_table, conn):
    defaults_table.execute("INSERT INTO #d DEFAULT VALUES")
    conn.commit()
    defaults_table.execute("UPDATE #d SET a = ? WHERE id = 1", (100,))
    conn.commit()
    defaults_table.execute("SELECT c, d FROM #d")
    assert tuple(defaults_table.fetchone()) == (200, 101)


def test_computed_concat_with_null_is_null_under_concat_null_yields_null(defaults_table, conn):
    # n = b + N'!'. With CONCAT_NULL_YIELDS_NULL ON (ODBC default) a NULL b
    # produces NULL, not "!". A rewrite that sends the wrong SET option changes this.
    defaults_table.execute("INSERT INTO #d (a, b) VALUES (1, NULL)")
    defaults_table.execute("INSERT INTO #d (a, b) VALUES (1, N'hi')")
    conn.commit()
    defaults_table.execute("SELECT n FROM #d ORDER BY id")
    assert [r[0] for r in defaults_table.fetchall()] == [None, "hi!"]


def test_default_keyword_into_computed_column_is_rejected(defaults_table, conn):
    # Computed columns cannot be written, even with DEFAULT.
    with pytest.raises(ProgrammingError):
        defaults_table.execute("INSERT INTO #d (c) VALUES (DEFAULT)")
    conn.rollback()


def test_writing_computed_column_by_parameter_is_rejected(defaults_table, conn):
    with pytest.raises(ProgrammingError):
        defaults_table.execute("INSERT INTO #d (d) VALUES (?)", (3,))
    conn.rollback()


def test_output_inserted_returns_server_generated_defaults(defaults_table, conn):
    # OUTPUT returns the defaults and computed values in the same round trip.
    # yii2 #18287 shows OUTPUT-based inserts break on computed columns that
    # reference functions, so the result set itself must be read correctly.
    defaults_table.execute(
        "INSERT INTO #d OUTPUT inserted.id, inserted.a, inserted.c DEFAULT VALUES"
    )
    row = tuple(defaults_table.fetchone())
    conn.commit()
    assert row == (1, 5, 10)


def test_indexed_persisted_computed_column_supports_dml_and_seek(cursor, conn):
    # An index on a persisted computed column needs the session SET options the
    # server expects. Writes and lookups through the driver must both succeed.
    cursor.execute(
        "CREATE TABLE #idx (id int PRIMARY KEY, a int NOT NULL, c AS a * 2 PERSISTED)"
    )
    cursor.execute("CREATE INDEX ix_c ON #idx (c)")
    conn.commit()
    cursor.executemany(
        "INSERT INTO #idx (id, a) VALUES (?, ?)",
        [(1, 3), (2, 4), (3, 5)],
    )
    cursor.execute("UPDATE #idx SET a = 10 WHERE id = 2")
    conn.commit()
    cursor.execute("SELECT id FROM #idx WHERE c = ?", (20,))
    assert [tuple(r) for r in cursor.fetchall()] == [(2,)]
    cursor.execute("SELECT id FROM #idx WHERE c = ? ORDER BY id", (6,))
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]


def test_connection_session_options_match_odbc_defaults(cursor):
    # These are the options the rewrite must send at logon for indexed
    # computed columns and for CONCAT_NULL_YIELDS_NULL semantics.
    cursor.execute(
        "SELECT SESSIONPROPERTY('ANSI_NULLS'), SESSIONPROPERTY('QUOTED_IDENTIFIER'),"
        " SESSIONPROPERTY('ANSI_WARNINGS'), SESSIONPROPERTY('ANSI_PADDING'),"
        " SESSIONPROPERTY('CONCAT_NULL_YIELDS_NULL')"
    )
    assert tuple(cursor.fetchone()) == (1, 1, 1, 1, 1)
