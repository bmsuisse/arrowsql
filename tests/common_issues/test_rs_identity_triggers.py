"""IDENTITY columns interacting with triggers, IDENTITY_INSERT and rollback.

Native-TDS rewrite checks. Everything here runs on the upstream ODBC driver as
ground truth; a native implementation must reproduce the same values, types and
exception classes.

Research (issue trackers and docs, checked for this suite):
- https://github.com/microsoft/mssql-django/issues/333 (INSERT into a table with
  an SQL trigger fails with engine error 334 when the statement uses OUTPUT
  without INTO)
- https://github.com/prisma/prisma/issues/4535 (create() on a table with a
  trigger fails with 334 because the ORM emits OUTPUT INSERTED.*)
- https://github.com/yiisoft/yii2/issues/18221 (OUTPUT INSERTED.* generated
  for inserts only, which fails on triggered tables)
- https://github.com/sqlalchemy/sqlalchemy/issues/537 (fetching the inserted
  id on a table with a trigger: @@IDENTITY returns the trigger table's id;
  SCOPE_IDENTITY is the right call)
- https://github.com/hugr-lab/mssql-extension/issues/471 (INSERT ... RETURNING
  on a triggered table, error 334, fixed by OUTPUT ... INTO a table variable)
- https://github.com/sqlalchemy/sqlalchemy/issues/5464 (Set IDENTITY_INSERT
  OFF errors when the explicit value is supplied for a non-identity key)
- https://github.com/blue-yonder/turbodbc/issues/93 (IDENTITY_INSERT must
  survive across executes on the same connection; statement handles closed
  after execute lose the setting)
- https://github.com/tediousjs/node-mssql/issues/936 (IDENTITY_INSERT ON set
  on one pooled connection does not apply to another)
- https://github.com/mkleehammer/pyodbc/issues/334 (pyodbc: OUTPUT clause with
  triggers, multiple result sets and rowcount)
- https://learn.microsoft.com/en-us/sql/t-sql/statements/set-identity-insert-transact-sql
- https://learn.microsoft.com/en-us/sql/t-sql/functions/scope-identity-transact-sql
- https://learn.microsoft.com/en-us/sql/t-sql/functions/ident-current-transact-sql
- https://learn.microsoft.com/en-us/sql/t-sql/statements/dbcc-checkident-transact-sql
- https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql
  (error 334: OUTPUT without INTO is refused on a table with an enabled trigger)

Failure modes covered:
- explicit identity value rejected with IDENTITY_INSERT OFF (IntegrityError)
- IDENTITY_INSERT ON persists across executes on one connection and is per table
- explicit values advance the next auto-generated value
- @@IDENTITY picks up the trigger's identity; SCOPE_IDENTITY does not
- IDENT_CURRENT is table-scoped and returns Decimal, not int
- DBCC CHECKIDENT RESEED on an empty table: the RESEED value is the first id handed out
- OUTPUT INSERTED without INTO on a triggered table fails with 334
  (ProgrammingError); OUTPUT ... INTO a table variable works
- a disabled trigger lifts the OUTPUT restriction
- rollback burns identity values (gaps), also for the trigger's table
- SCOPE_IDENTITY after a multi-row INSERT ... SELECT returns the last id
"""

import uuid
from decimal import Decimal

import pytest
from mssql_python import IntegrityError, ProgrammingError


@pytest.fixture
def tables(conn, cursor):
    """Permanent parent table with an AFTER INSERT trigger writing to a log table.

    Both identities are seeded so their values are easy to recognise:
    parent starts at 1, log starts at 100.
    """
    suffix = uuid.uuid4().hex[:10]
    parent = f"ci_idt_{suffix}"
    log = f"ci_idl_{suffix}"
    trig = f"trg_ci_idt_{suffix}"
    cursor.execute(
        f"CREATE TABLE dbo.{parent} (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20) NOT NULL)"
    )
    cursor.execute(
        f"CREATE TABLE dbo.{log} (id INT IDENTITY(100,1) PRIMARY KEY, src INT NULL)"
    )
    cursor.execute(
        f"CREATE TRIGGER dbo.{trig} ON dbo.{parent} AFTER INSERT AS "
        f"INSERT INTO dbo.{log} (src) SELECT id FROM inserted"
    )
    conn.commit()
    yield parent, log, trig
    conn.rollback()
    cursor.execute(f"IF OBJECT_ID('dbo.{trig}', 'TR') IS NOT NULL DROP TRIGGER dbo.{trig}")
    cursor.execute(f"DROP TABLE IF EXISTS dbo.{parent}")
    cursor.execute(f"DROP TABLE IF EXISTS dbo.{log}")
    conn.commit()


def _one(cursor, sql, params=()):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


def test_insert_explicit_identity_value_rejected_when_identity_insert_off(conn, cursor, tables):
    parent, _, _ = tables
    with pytest.raises(IntegrityError, match="IDENTITY_INSERT"):
        cursor.execute(f"INSERT INTO dbo.{parent} (id, v) VALUES (?, ?)", (500, "x"))
    conn.rollback()


def test_identity_insert_on_accepts_explicit_value(conn, cursor, tables):
    parent, _, _ = tables
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} ON")
    cursor.execute(f"INSERT INTO dbo.{parent} (id, v) VALUES (?, ?)", (500, "explicit"))
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} OFF")
    conn.commit()
    cursor.execute(f"SELECT id, v FROM dbo.{parent}")
    row = cursor.fetchone()
    assert (row[0], row[1]) == (500, "explicit")
    assert type(row[0]) is int


def test_identity_insert_setting_persists_across_execute_calls(conn, cursor, tables):
    parent, _, _ = tables
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} ON")
    # Separate execute on the same connection must still see IDENTITY_INSERT ON.
    cursor.execute(f"INSERT INTO dbo.{parent} (id, v) VALUES (?, ?)", (42, "a"))
    cursor.execute(f"INSERT INTO dbo.{parent} (id, v) VALUES (?, ?)", (43, "b"))
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} OFF")
    conn.commit()
    cursor.execute(f"SELECT id FROM dbo.{parent} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [42, 43]


def test_explicit_identity_value_advances_next_auto_value(conn, cursor, tables):
    parent, _, _ = tables
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} ON")
    cursor.execute(f"INSERT INTO dbo.{parent} (id, v) VALUES (?, ?)", (500, "explicit"))
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} OFF")
    new_id = _one(
        cursor,
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) VALUES (?); "
        f"SELECT CAST(SCOPE_IDENTITY() AS INT)",
        ("auto",),
    )
    conn.commit()
    assert new_id == 501


def test_identity_insert_is_scoped_per_table(conn, cursor, tables):
    parent, log, _ = tables
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} ON")
    with pytest.raises(IntegrityError, match="IDENTITY_INSERT"):
        cursor.execute(f"INSERT INTO dbo.{log} (id, src) VALUES (?, ?)", (7, 1))
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} OFF")
    conn.rollback()


def test_at_at_identity_returns_trigger_table_id(conn, cursor, tables):
    parent, _, _ = tables
    cursor.execute(
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) VALUES (?); "
        f"SELECT CAST(@@IDENTITY AS INT), CAST(SCOPE_IDENTITY() AS INT)",
        ("x",),
    )
    at_at, scope = cursor.fetchone()
    conn.commit()
    # @@IDENTITY was overwritten by the AFTER INSERT trigger (log id 100).
    assert at_at == 100
    # SCOPE_IDENTITY stays in the INSERT's own scope (parent id 1).
    assert scope == 1


def test_scope_identity_ignores_trigger_insert(conn, cursor, tables):
    parent, log, _ = tables
    own_id = _one(
        cursor,
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) VALUES (?); "
        f"SELECT CAST(SCOPE_IDENTITY() AS INT)",
        ("y",),
    )
    conn.commit()
    log_id = _one(cursor, f"SELECT CAST(MAX(id) AS INT) FROM dbo.{log}")
    assert own_id == 1
    assert log_id == 100


def test_ident_current_is_table_scoped_and_decimal_typed(conn, cursor, tables):
    parent, log, _ = tables
    cursor.execute(f"INSERT INTO dbo.{parent} (v) VALUES (?)", ("z",))
    conn.commit()
    cursor.execute(f"SELECT IDENT_CURRENT('dbo.{parent}'), IDENT_CURRENT('dbo.{log}')")
    parent_cur, log_cur = cursor.fetchone()
    assert isinstance(parent_cur, Decimal)
    assert isinstance(log_cur, Decimal)
    assert int(parent_cur) == 1
    # The trigger consumed log identity 100, so IDENT_CURRENT(log) is 100.
    assert int(log_cur) == 100


def test_dbcc_checkident_reseed_on_empty_table_first_insert_gets_seed_value(conn, cursor, tables):
    parent, _, _ = tables
    # On a table with no rows the RESEED value itself is handed out first ...
    cursor.execute(f"DBCC CHECKIDENT ('dbo.{parent}', RESEED, 1000) WITH NO_INFOMSGS")
    conn.commit()
    first = _one(
        cursor,
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) VALUES (?); "
        f"SELECT CAST(SCOPE_IDENTITY() AS INT)",
        ("r1",),
    )
    conn.commit()
    # ... and the following insert is the increment past it.
    second = _one(
        cursor,
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) VALUES (?); "
        f"SELECT CAST(SCOPE_IDENTITY() AS INT)",
        ("r2",),
    )
    conn.commit()
    assert (first, second) == (1000, 1001)


def test_output_inserted_without_into_on_triggered_table_raises_334(conn, cursor, tables):
    parent, _, _ = tables
    with pytest.raises(ProgrammingError, match="cannot have any enabled triggers"):
        cursor.execute(f"INSERT INTO dbo.{parent} (v) OUTPUT INSERTED.id VALUES (?)", ("o",))
    conn.rollback()


def test_output_into_table_variable_works_with_trigger(conn, cursor, tables):
    parent, log, _ = tables
    cursor.execute(
        f"SET NOCOUNT ON; DECLARE @o TABLE (id INT NOT NULL); "
        f"INSERT INTO dbo.{parent} (v) OUTPUT INSERTED.id INTO @o VALUES (?); "
        f"SELECT id FROM @o",
        ("t",),
    )
    row = cursor.fetchone()
    conn.commit()
    assert row is not None
    assert row[0] == 1
    assert type(row[0]) is int
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{log}")
    assert cursor.fetchone()[0] == 1


def test_disabled_trigger_allows_output_without_into(conn, cursor, tables):
    parent, _, trig = tables
    cursor.execute(f"ALTER TABLE dbo.{parent} DISABLE TRIGGER {trig}")
    conn.commit()
    try:
        cursor.execute(f"INSERT INTO dbo.{parent} (v) OUTPUT INSERTED.id VALUES (?)", ("d",))
        row = cursor.fetchone()
        conn.commit()
        assert row[0] == 1
    finally:
        cursor.execute(f"ALTER TABLE dbo.{parent} ENABLE TRIGGER {trig}")
        conn.commit()


def test_rollback_burns_identity_value_leaving_gap(conn, cursor, tables):
    parent, _, _ = tables
    cursor.execute(f"INSERT INTO dbo.{parent} (v) VALUES (?)", ("gone",))
    conn.rollback()
    new_id = _one(
        cursor,
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) VALUES (?); "
        f"SELECT CAST(SCOPE_IDENTITY() AS INT)",
        ("kept",),
    )
    conn.commit()
    # Identity 1 was consumed by the rolled-back row and never reused.
    assert new_id == 2
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{parent}")
    assert cursor.fetchone()[0] == 1


def test_rollback_also_burns_trigger_table_identity(conn, cursor, tables):
    parent, log, _ = tables
    cursor.execute(f"INSERT INTO dbo.{parent} (v) VALUES (?)", ("gone",))
    conn.rollback()
    cursor.execute(f"SELECT CAST(IDENT_CURRENT('dbo.{log}') AS INT)")
    # Trigger wrote log id 100, the rollback did not restore the counter.
    assert cursor.fetchone()[0] == 100
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{log}")
    assert cursor.fetchone()[0] == 0


def test_ident_current_reports_burned_value_after_rollback(conn, cursor, tables):
    parent, _, _ = tables
    cursor.execute(f"INSERT INTO dbo.{parent} (v) VALUES (?)", ("a",))
    cursor.execute(f"INSERT INTO dbo.{parent} (v) VALUES (?)", ("b",))
    conn.rollback()
    cursor.execute(f"SELECT CAST(IDENT_CURRENT('dbo.{parent}') AS INT)")
    assert cursor.fetchone()[0] == 2
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{parent}")
    assert cursor.fetchone()[0] == 0


def test_scope_identity_after_multi_row_insert_select_is_last_row(conn, cursor, tables):
    parent, _, _ = tables
    last_id = _one(
        cursor,
        f"SET NOCOUNT ON; INSERT INTO dbo.{parent} (v) "
        f"SELECT v FROM (VALUES (N'a'), (N'b'), (N'c')) AS s(v); "
        f"SELECT CAST(SCOPE_IDENTITY() AS INT)",
    )
    conn.commit()
    cursor.execute(f"SELECT CAST(MAX(id) AS INT), COUNT(*) FROM dbo.{parent}")
    max_id, count = cursor.fetchone()
    assert count == 3
    assert last_id == max_id == 3
    assert type(last_id) is int


def test_identity_insert_on_trigger_sees_explicit_value(conn, cursor, tables):
    parent, log, _ = tables
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} ON")
    cursor.execute(f"INSERT INTO dbo.{parent} (id, v) VALUES (?, ?)", (777, "e"))
    cursor.execute(f"SET IDENTITY_INSERT dbo.{parent} OFF")
    conn.commit()
    cursor.execute(f"SELECT src FROM dbo.{log}")
    assert cursor.fetchone()[0] == 777
