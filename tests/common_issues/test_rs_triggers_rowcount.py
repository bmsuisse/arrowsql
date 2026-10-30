"""Triggers and rowcount: DONE_IN_PROC count slots, cursor.rowcount, nextset(), THROW in triggers.

Research sources (issue trackers and docs consulted for real-world failure modes):
- https://github.com/microsoft/mssql-python/issues/863  Cursor.nextset() does not update
  rowcount for the next result (stays at the execute() value of the first statement)
- https://github.com/microsoft/mssql-python/pull/864  "FIX: Update rowcount in nextset()",
  expected sequence 1, 2, -1, 3, 0 across nextset() calls
- https://github.com/mkleehammer/pyodbc/discussions/1434  triggers / PRINT / ANSI_WARNINGS
  adding extra result sets; recommended pattern is `while cursor.nextset()` checking
  description and rowcount on each set
- https://bitbucket.org/zzzeek/sqlalchemy_old/issues/641/pyodbc-global-set-nocount-on
  an AFTER INSERT trigger producing a second "rows inserted" result set makes pyodbc hit
  "invalid cursor state" unless the statements are drained or SET NOCOUNT ON is used
- https://www.red-gate.com/hub/product-learning/sql-prompt/finding-code-smells-using-sql-prompt-set-nocount-problem-pe008-pe009/
  SET NOCOUNT ON suppresses the DONE_IN_PROC count messages sent per statement inside
  procedures and triggers; @@ROWCOUNT itself is unaffected
- https://learn.microsoft.com/en-us/sql/t-sql/statements/set-nocount-transact-sql?view=sql-server-ver17
  SET NOCOUNT semantics (extra count result set per statement)
- https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms187316(v=sql.105)
  @@ROWCOUNT is set by DML and reset by statements such as PRINT, SET, RAISERROR
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/cursor-management?view=sql-server-ver17
  mssql-python cursor and result-set management (nextset, description, rowcount)

Behaviors a native-TDS reimplementation can get wrong:
- A trigger without SET NOCOUNT ON sends its own DONE_IN_PROC count, so one INSERT can
  yield two result slots; a driver that reads only the first slot, or uses the last count
  it saw, reports the trigger's row count instead of the statement's.
- executemany() with an insert trigger must sum only the statement's own counts.
- Under session SET NOCOUNT ON the DML count is not sent, so rowcount is -1, not a guess.
- THROW inside a trigger surfaces as ProgrammingError (SQLSTATE 42000) and rolls back.

Upstream (ODBC) status: the rowcount bugs below are asserted with the correct behavior and
marked xfail(strict=True) with upstream_bug.
"""

import uuid

import pytest
from mssql_python import ProgrammingError


def _names(cursor):
    return None if cursor.description is None else [d[0] for d in cursor.description]


def _walk_slots(cursor):
    """Drain every result slot. Returns ("count", rowcount) or ("rows", names, rows) per slot."""
    slots = []
    while True:
        if cursor.description is None:
            slots.append(("count", cursor.rowcount))
        else:
            rows = [tuple(r) for r in cursor.fetchall()]
            slots.append(("rows", _names(cursor), rows))
        if not cursor.nextset():
            break
    return slots


@pytest.fixture
def trig(cursor):
    """Permanent source/log tables plus a view, with unique names, dropped after the test."""
    suffix = uuid.uuid4().hex[:12]
    src = f"ci_trg_src_{suffix}"
    log = f"ci_trg_log_{suffix}"
    view = f"ci_trg_v_{suffix}"
    cursor.execute(f"CREATE TABLE dbo.{src} (id INT NOT NULL)")
    cursor.execute(f"CREATE TABLE dbo.{log} (id INT NOT NULL)")
    cursor.connection.commit()
    names = {"src": src, "log": log, "view": view}
    yield names
    cursor.execute(f"DROP VIEW IF EXISTS dbo.{view}")
    cursor.execute(f"DROP TABLE IF EXISTS dbo.{src}")
    cursor.execute(f"DROP TABLE IF EXISTS dbo.{log}")
    cursor.connection.commit()


def _after_insert_log_ids(cursor, names, where="1 = 1"):
    """Create AFTER INSERT trigger copying matching inserted ids into the log table."""
    cursor.execute(
        f"CREATE TRIGGER dbo.tr_{names['src']}_ai ON dbo.{names['src']} AFTER INSERT AS "
        f"INSERT INTO dbo.{names['log']} SELECT id FROM inserted WHERE {where}"
    )
    cursor.connection.commit()


def test_plain_insert_rowcount_is_statement_count(cursor, trig):
    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2),(3)")

    assert cursor.rowcount == 3
    assert cursor.description is None
    assert cursor.nextset() is False


def test_nocount_on_inside_trigger_keeps_single_result_slot(cursor, trig):
    cursor.execute(
        f"CREATE TRIGGER dbo.tr_{trig['src']}_ai ON dbo.{trig['src']} AFTER INSERT AS "
        f"BEGIN SET NOCOUNT ON; INSERT INTO dbo.{trig['log']} SELECT id FROM inserted; END"
    )
    cursor.connection.commit()

    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2)")

    assert cursor.rowcount == 2
    assert cursor.description is None
    assert cursor.nextset() is False


def test_trigger_without_nocount_adds_second_count_slot(cursor, trig):
    _after_insert_log_ids(cursor, trig)

    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2)")
    slots = _walk_slots(cursor)

    # Statement count first, then the trigger's own INSERT count: both have no columns.
    assert len(slots) == 2
    assert all(slot[0] == "count" for slot in slots)


def test_session_nocount_on_plain_insert_rowcount_is_minus_one(cursor, trig):
    cursor.execute("SET NOCOUNT ON")
    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2)")

    # The DML count is suppressed on the wire, so the driver has no value to report.
    assert cursor.rowcount == -1
    assert cursor.description is None
    assert cursor.nextset() is False
    cursor.execute("SET NOCOUNT OFF")


def test_session_nocount_on_with_trigger_yields_no_extra_slots(cursor, trig):
    _after_insert_log_ids(cursor, trig)
    cursor.execute("SET NOCOUNT ON")

    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2)")

    assert cursor.description is None
    assert cursor.nextset() is False
    cursor.execute("SET NOCOUNT OFF")


def test_insert_then_select_batch_skips_count_slot_to_rows(cursor, trig):
    cursor.execute(
        f"INSERT INTO dbo.{trig['src']} VALUES (1),(2); "
        f"SELECT id FROM dbo.{trig['src']} ORDER BY id"
    )

    # The INSERT count occupies the first slot with no columns.
    assert cursor.description is None
    assert cursor.nextset() is True
    assert _names(cursor) == ["id"]
    assert [tuple(r) for r in cursor.fetchall()] == [(1,), (2,)]
    assert cursor.nextset() is False
    assert cursor.description is None


def test_insert_with_trigger_then_select_batch_reaches_rows_after_extra_slot(cursor, trig):
    _after_insert_log_ids(cursor, trig)
    cursor.execute(
        f"INSERT INTO dbo.{trig['src']} VALUES (7); "
        f"SELECT id FROM dbo.{trig['src']}; SELECT COUNT(*) AS n FROM dbo.{trig['log']}"
    )

    slots = _walk_slots(cursor)

    assert [s[0] for s in slots] == ["count", "count", "rows", "rows"]
    assert slots[2] == ("rows", ["id"], [(7,)])
    assert slots[3] == ("rows", ["n"], [(1,)])


def test_select_rowcount_minus_one_before_fetch_then_cumulative(cursor, trig):
    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2),(3)")
    cursor.execute(f"SELECT id FROM dbo.{trig['src']}")

    assert cursor.rowcount == -1
    assert len(cursor.fetchall()) == 3
    assert cursor.rowcount == 3


def test_ddl_rowcount_minus_one_and_no_description(cursor):
    cursor.execute("CREATE TABLE #ddl_probe (id INT)")

    assert cursor.rowcount == -1
    assert cursor.description is None
    assert cursor.nextset() is False


def test_update_matching_no_rows_with_trigger_reports_zero(cursor, trig):
    cursor.execute(
        f"CREATE TRIGGER dbo.tr_{trig['src']}_au ON dbo.{trig['src']} AFTER UPDATE AS "
        f"INSERT INTO dbo.{trig['log']} SELECT id FROM inserted"
    )
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1)")
    cursor.execute(f"UPDATE dbo.{trig['src']} SET id = id + 1 WHERE 1 = 0")

    assert cursor.rowcount == 0
    assert cursor.description is None


def test_delete_rowcount_unaffected_by_insert_trigger(cursor, trig):
    _after_insert_log_ids(cursor, trig)
    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2)")
    cursor.execute(f"DELETE FROM dbo.{trig['src']}")

    assert cursor.rowcount == 2
    assert cursor.description is None


def test_instead_of_insert_reports_view_rows_and_writes_base(cursor, trig):
    cursor.execute(f"CREATE VIEW dbo.{trig['view']} AS SELECT id FROM dbo.{trig['src']}")
    cursor.execute(
        f"CREATE TRIGGER dbo.tr_{trig['view']}_io ON dbo.{trig['view']} INSTEAD OF INSERT AS "
        f"INSERT INTO dbo.{trig['src']} SELECT id FROM inserted"
    )
    cursor.connection.commit()

    cursor.execute(f"INSERT INTO dbo.{trig['view']} VALUES (10),(11)")

    assert cursor.rowcount == 2
    _walk_slots(cursor)
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{trig['src']}")
    assert tuple(cursor.fetchone()) == (2,)


def test_throw_in_after_trigger_raises_programming_error_and_rolls_back(cursor, trig):
    cursor.execute(
        f"CREATE TRIGGER dbo.tr_{trig['src']}_th ON dbo.{trig['src']} AFTER INSERT AS "
        f"IF EXISTS (SELECT 1 FROM inserted WHERE id = 99) THROW 50001, 'no 99 allowed', 1"
    )
    cursor.connection.commit()

    with pytest.raises(ProgrammingError, match="no 99 allowed"):
        cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (98),(99)")

    cursor.execute(f"SELECT COUNT(*) FROM dbo.{trig['src']}")
    assert tuple(cursor.fetchone()) == (0,)


def test_executemany_rowcount_plain_table_is_total(cursor, trig):
    cursor.executemany(f"INSERT INTO dbo.{trig['src']} VALUES (?)", [(1,), (2,), (3,)])

    assert cursor.rowcount == 3


@pytest.mark.upstream_bug(
    reason="executemany with an AFTER INSERT trigger that inserts rows reports 6 "
    "(statement 3 + trigger 3) on upstream; correct DB-API total is 3"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_rowcount_with_insert_trigger_counts_statement_rows_only(cursor, trig):
    _after_insert_log_ids(cursor, trig)

    cursor.executemany(f"INSERT INTO dbo.{trig['src']} VALUES (?)", [(8,), (9,), (10,)])

    assert cursor.rowcount == 3


@pytest.mark.upstream_bug(
    reason="execute() rowcount for an INSERT takes the trigger's DONE_IN_PROC count "
    "(1 here) instead of the statement's own 2 affected rows"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_execute_rowcount_is_statement_count_not_trigger_count(cursor, trig):
    _after_insert_log_ids(cursor, trig, where="id = 1")

    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (1),(2)")

    assert cursor.rowcount == 2


@pytest.mark.upstream_bug(
    reason="execute() rowcount for an INSERT reports the trigger's 0 rows logged "
    "instead of the statement's 3 affected rows"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_execute_rowcount_when_trigger_logs_nothing_is_statement_count(cursor, trig):
    _after_insert_log_ids(cursor, trig, where="id = 1")

    cursor.execute(f"INSERT INTO dbo.{trig['src']} VALUES (3),(4),(4)")

    assert cursor.rowcount == 3


@pytest.mark.upstream_bug(
    reason="nextset() does not refresh rowcount: the second INSERT (1 row) still reports "
    "the first statement's 3 (mssql-python issue 863)"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_nextset_refreshes_rowcount_for_second_dml_statement(cursor, trig):
    cursor.execute(
        f"INSERT INTO dbo.{trig['src']} VALUES (1),(2),(3); "
        f"INSERT INTO dbo.{trig['src']} VALUES (4)"
    )
    assert cursor.rowcount == 3

    assert cursor.nextset() is True
    assert cursor.description is None
    assert cursor.rowcount == 1
    assert cursor.nextset() is False
