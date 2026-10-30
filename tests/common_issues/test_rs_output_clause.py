"""OUTPUT clause on INSERT/UPDATE/DELETE: row-returning DML, OUTPUT INTO table variables,
executemany with OUTPUT, and how rowcount interacts with the result-set walk.

Research sources (issue trackers and docs consulted for real-world failure modes):
- https://github.com/mkleehammer/pyodbc/discussions/1162  "insert statements get id": the
  maintainer-recommended pattern is INSERT ... OUTPUT inserted.id + fetchval() on the cursor
- https://github.com/microsoft/mssql-python/issues/333  a UNIQUE violation in a multi-row
  INSERT ... OUTPUT inserted is only surfaced when the OUTPUT rows are fetched, so callers
  must fetch to see errors
- https://bitbucket.org/zzzeek/sqlalchemy_old/issues/4062  pyodbc cannot report rowcount
  for UPDATE/DELETE with OUTPUT in effect (SQLAlchemy added sane_rowcount_w_returning)
- https://github.com/microsoft/mssql-python/issues/863  Cursor.nextset() does not refresh
  rowcount for the next result (fixed in PR #864); rowcount must follow the result slot
- https://github.com/mkleehammer/pyodbc/discussions/1434  triggers add extra result slots;
  drain with nextset() and check description/rowcount on each slot
- https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql  OUTPUT
  rules: OUTPUT without INTO is rejected on tables with enabled triggers (error 334); rows
  are returned even if the statement later fails and rolls back; order of returned rows
  is not guaranteed
- https://learn.microsoft.com/en-us/sql/t-sql/statements/set-nocount-transact-sql  SET
  NOCOUNT suppresses the per-statement DONE_IN_PROC count, so the DML count slot disappears

Behaviors a native-TDS reimplementation can get wrong:
- An OUTPUT statement yields a row-set slot first, then a DML count slot (or the reverse
  when the batch starts with DECLARE/INSERT INTO @table_variable). The driver must map
  description/rowcount to the slot actually current, not to the last count seen.
- executemany() with OUTPUT yields one result set per parameter set, while a plain
  executemany() reports the summed count.
- Errors from a multi-row OUTPUT statement can surface at fetch time, not at execute time.
- rowcount is -1 until the result set is consumed (PEP 249 "undetermined").
"""

import uuid
from decimal import Decimal

import pytest
from mssql_python import IntegrityError, ProgrammingError


def _names(cursor):
    return [d[0] for d in cursor.description]


def _rows(cursor):
    """fetchall() as plain tuples (driver rows compare unequal to tuples)."""
    return [tuple(r) for r in cursor.fetchall()]


def _make_temp_t(cursor):
    cursor.execute(
        "CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(10) NOT NULL)"
    )


def _make_temp_t_unique(cursor):
    cursor.execute(
        "CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(10) UNIQUE NOT NULL)"
    )


def test_insert_output_returns_row_after_execute(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) OUTPUT inserted.id, inserted.v VALUES (?)", "a")

    assert _names(cursor) == ["id", "v"]
    assert cursor.description[0][1] is int
    assert cursor.description[1][1] is str
    assert cursor.rowcount == -1
    assert _rows(cursor) == [(1, "a")]
    assert cursor.rowcount == 1


def test_insert_output_multirow_returns_every_row(cursor):
    _make_temp_t(cursor)
    cursor.execute(
        "INSERT INTO #t (v) OUTPUT inserted.id, inserted.v VALUES (?), (?), (?)",
        "x",
        "y",
        "z",
    )
    # SQL Server does not guarantee OUTPUT row order, so compare as a sorted set.
    assert sorted(_rows(cursor)) == [(1, "x"), (2, "y"), (3, "z")]
    assert cursor.rowcount == 3


def test_fetchone_then_fetchall_splits_output_rows(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a'), ('b'), ('c')")
    cursor.execute("UPDATE #t SET v = 'w' OUTPUT inserted.id")

    first = cursor.fetchone()
    assert first is not None and tuple(first) in {(1,), (2,), (3,)}
    rest = sorted(_rows(cursor))
    assert len(rest) == 2
    assert sorted([tuple(first)] + rest) == [(1,), (2,), (3,)]
    assert cursor.fetchone() is None
    assert cursor.rowcount == 3


def test_update_output_deleted_and_inserted_pairs(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a'), ('b'), ('c')")
    cursor.execute(
        "UPDATE #t SET v = 'z' OUTPUT deleted.v, inserted.v WHERE id >= 2"
    )
    assert len(cursor.description) == 2
    assert sorted(_rows(cursor)) == [("b", "z"), ("c", "z")]
    assert cursor.rowcount == 2


def test_delete_output_zero_matches_still_has_description(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a')")
    cursor.execute("DELETE FROM #t OUTPUT deleted.id WHERE id = -1")
    assert cursor.description is not None
    assert _names(cursor) == ["id"]
    assert _rows(cursor) == []
    assert cursor.rowcount == 0


def test_delete_output_returns_removed_rows(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a'), ('b'), ('c')")
    cursor.execute("DELETE FROM #t OUTPUT deleted.v WHERE id <= 2")
    assert sorted(_rows(cursor)) == [("a",), ("b",)]
    cursor.execute("SELECT COUNT(*) FROM #t")
    assert cursor.fetchone()[0] == 1


def test_output_into_table_variable_count_slot_then_rows(cursor):
    _make_temp_t(cursor)
    cursor.execute(
        "DECLARE @o TABLE (id INT); "
        "INSERT INTO #t (v) OUTPUT inserted.id INTO @o VALUES (?); "
        "SELECT id FROM @o",
        "a",
    )
    # First slot: the INSERT's DML count; no result set yet.
    assert cursor.description is None
    assert cursor.rowcount == 1
    assert cursor.nextset() is True
    assert _names(cursor) == ["id"]
    assert _rows(cursor) == [(1,)]
    assert cursor.nextset() is False


def test_output_into_table_variable_nocount_on_rows_come_first(cursor):
    _make_temp_t(cursor)
    cursor.execute(
        "SET NOCOUNT ON; "
        "DECLARE @o TABLE (id INT); "
        "INSERT INTO #t (v) OUTPUT inserted.id INTO @o VALUES (?); "
        "SELECT id FROM @o",
        "a",
    )
    assert _names(cursor) == ["id"]
    assert cursor.rowcount == -1
    assert _rows(cursor) == [(1,)]


def test_output_into_table_variable_update_count_then_rows(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a'), ('b')")
    cursor.execute(
        "DECLARE @o TABLE (id INT); "
        "UPDATE #t SET v = 'k' OUTPUT inserted.id INTO @o; "
        "SELECT id FROM @o ORDER BY id"
    )
    assert cursor.description is None
    assert cursor.rowcount == 2
    assert cursor.nextset() is True
    assert _rows(cursor) == [(1,), (2,)]


def test_fetch_on_count_slot_raises_programming_error(cursor):
    _make_temp_t(cursor)
    cursor.execute(
        "DECLARE @o TABLE (id INT); "
        "INSERT INTO #t (v) OUTPUT inserted.id INTO @o VALUES ('a'); "
        "SELECT id FROM @o"
    )
    assert cursor.description is None
    with pytest.raises(ProgrammingError):
        cursor.fetchall()
    # The cursor is still usable: the rows are reachable via nextset().
    assert cursor.nextset() is True
    assert _rows(cursor) == [(1,)]


def test_pending_output_rows_do_not_block_next_execute(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a'), ('b')")
    cursor.execute("UPDATE #t SET v = 'z' OUTPUT inserted.id")
    # Deliberately not fetching the OUTPUT rows before re-executing.
    cursor.execute("SELECT 42")
    assert _rows(cursor) == [(42,)]


def test_executemany_output_one_result_set_per_parameter_set(cursor):
    _make_temp_t(cursor)
    cursor.executemany(
        "INSERT INTO #t (v) OUTPUT inserted.id VALUES (?)",
        [("p",), ("q",), ("r",)],
    )
    assert _names(cursor) == ["id"]
    slots = [_rows(cursor)]
    while cursor.nextset():
        slots.append(_rows(cursor))
    assert [[tuple(r) for r in s] for s in slots] == [[(1,)], [(2,)], [(3,)]]


def test_executemany_plain_rowcount_is_total_and_no_description(cursor):
    _make_temp_t(cursor)
    cursor.executemany("INSERT INTO #t (v) VALUES (?)", [("p",), ("q",), ("r",)])
    assert cursor.description is None
    assert cursor.rowcount == 3


def test_nocount_on_plain_update_rowcount_is_minus_one(cursor):
    _make_temp_t(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('a'), ('b')")
    cursor.execute("SET NOCOUNT ON; UPDATE #t SET v = 'q'")
    assert cursor.rowcount == -1


def test_output_without_into_on_table_with_trigger_raises(conn, cursor, table_name):
    conn.autocommit = True
    log = f"{table_name}_log"
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT IDENTITY PRIMARY KEY, v INT)")
    cursor.execute(f"CREATE TABLE dbo.{log} (n INT)")
    try:
        cursor.execute(
            f"CREATE TRIGGER dbo.trg_{table_name} ON dbo.{table_name} AFTER INSERT AS "
            f"BEGIN SET NOCOUNT ON; INSERT INTO dbo.{log} SELECT 1 FROM inserted; END"
        )
        with pytest.raises(ProgrammingError, match="OUTPUT"):
            cursor.execute(f"INSERT INTO dbo.{table_name} (v) OUTPUT inserted.id VALUES (1)")
        # The failed statement must not leave the connection unusable.
        cursor.execute("SELECT 1")
        assert _rows(cursor) == [(1,)]
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        assert cursor.fetchone()[0] == 0
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{log}")


def test_output_into_table_variable_works_with_trigger(conn, cursor, table_name):
    conn.autocommit = True
    log = f"{table_name}_log"
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT IDENTITY PRIMARY KEY, v INT)")
    cursor.execute(f"CREATE TABLE dbo.{log} (n INT)")
    try:
        cursor.execute(
            f"CREATE TRIGGER dbo.trg_{table_name} ON dbo.{table_name} AFTER INSERT AS "
            f"BEGIN SET NOCOUNT ON; INSERT INTO dbo.{log} SELECT 1 FROM inserted; END"
        )
        cursor.execute(
            "DECLARE @o TABLE (id INT); "
            f"INSERT INTO dbo.{table_name} (v) OUTPUT inserted.id INTO @o VALUES (7); "
            "SELECT id FROM @o"
        )
        assert cursor.description is None
        assert cursor.rowcount == 1
        assert cursor.nextset() is True
        assert _rows(cursor) == [(1,)]
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{log}")
        assert cursor.fetchone()[0] == 1
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{log}")


def test_duplicate_key_in_multirow_output_raises_integrity_error(cursor):
    _make_temp_t_unique(cursor)
    cursor.execute("INSERT INTO #t (v) VALUES ('dup')")
    # The error may surface at execute() or only when the OUTPUT rows are fetched.
    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #t (v) OUTPUT inserted.id VALUES ('x'), ('dup'), ('y')")
        cursor.fetchall()
    cursor.execute("SELECT v FROM #t ORDER BY id")
    assert _rows(cursor) == [("dup",)]


def test_check_violation_in_output_statement_is_atomic(cursor):
    cursor.execute(
        "CREATE TABLE #r (id INT IDENTITY PRIMARY KEY, v INT CHECK (v > 0))"
    )
    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #r (v) OUTPUT inserted.id VALUES (5), (-1)")
        cursor.fetchall()
    cursor.execute("SELECT COUNT(*) FROM #r")
    assert cursor.fetchone()[0] == 0


def test_output_rows_visible_then_discarded_on_rollback(conn, cursor):
    _make_temp_t(cursor)
    conn.commit()  # start from a clean transaction boundary
    cursor.execute("INSERT INTO #t (v) OUTPUT inserted.id VALUES ('a')")
    assert _rows(cursor) == [(1,)]
    conn.rollback()
    cursor.execute("SELECT COUNT(*) FROM #t")
    assert cursor.fetchone()[0] == 0


def test_output_column_types_uuid_null_datetime_and_money(cursor):
    cursor.execute(
        "CREATE TABLE #u (d DATETIME2(3) NULL, b BIT NULL, m MONEY NULL, "
        "g UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID())"
    )
    cursor.execute(
        "INSERT INTO #u (d, b, m) OUTPUT inserted.g, inserted.d, inserted.b, inserted.m "
        "VALUES (NULL, 1, 12.5)"
    )
    row = _rows(cursor)[0]
    assert isinstance(row[0], uuid.UUID)
    assert row[1] is None
    assert row[2] is True
    assert row[3] == Decimal("12.50")
    assert isinstance(row[3], Decimal)
