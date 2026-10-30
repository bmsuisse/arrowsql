"""Savepoints, nested transactions and doomed-transaction handling on the driver.

Research sources (real reported problems / documented behavior this suite guards):
- https://github.com/sqlalchemy/sqlalchemy/issues/2678 (SAVE TRANSACTION / ROLLBACK to a
  savepoint over pyodbc on MSSQL: rows appear to vanish, savepoint rollback not observed)
- https://github.com/sqlalchemy/sqlalchemy/issues/13640 (SAVE TRANSACTION / ROLLBACK
  TRANSACTION <savepoint> cursors leaked by the MSSQL dialect for begin_nested())
- https://github.com/laravel/framework/issues/61829 (SQL Server: a savepoint wiped out by a
  nested error, then the savepoint rollback fails with "No transaction or savepoint of that
  name was found" and the server transaction count stays at 1)
- https://github.com/mkleehammer/pyodbc/issues/1335 (explicit BEGIN TRANSACTION on top of an
  autocommit=False pyodbc transaction pushes @@TRANCOUNT higher; rollback surprises)
- https://github.com/mkleehammer/pyodbc/issues/524 (procedures that check @@TRANCOUNT and
  only commit when they started the transaction, under a client-owned outer transaction)
- https://learn.microsoft.com/en-us/answers/questions/900084/msg-266-level-16-state-2 (error 266
  "Transaction count after EXECUTE indicates a mismatching number of BEGIN and COMMIT")
- https://learn.microsoft.com/en-us/answers/questions/784287/transaction-count-after-execute-indicates-a-mismat
  (266 from a CATCH block that does not roll back; 266 from a ROLLBACK inside a nested proc)
- https://learn.microsoft.com/en-us/sql/t-sql/functions/xact-state-transact-sql?view=sql-server-ver17
  (XACT_STATE: 1 committable, 0 no transaction, -1 uncommittable; @@TRANCOUNT cannot tell)
- https://learn.microsoft.com/en-us/sql/t-sql/language-elements/try-catch-transact-sql?view=sql-server-ver17
  (inside TRY an error makes the transaction uncommittable; only read or ROLLBACK allowed)
- https://learn.microsoft.com/en-us/sql/t-sql/statements/set-xact-abort-transact-sql?view=sql-server-ver17
  (SET XACT_ABORT ON rolls back the whole transaction on run-time error)
- https://github.com/MicrosoftDocs/sql-docs/blob/live/docs/t-sql/language-elements/rollback-transaction-transact-sql.md
  (ROLLBACK <savepoint> does not decrement @@TRANCOUNT; a plain ROLLBACK sets it to 0)

Ground truth is the upstream ODBC driver. Savepoints and nested transactions are per session,
so tests use a #temp table (created and committed first, since with autocommit off a CREATE
inside the driver's implicit transaction would be rolled back). Procedures use unique names
and are dropped in teardown. The driver keeps one implicit transaction open at all times when
autocommit is off, so the baseline @@TRANCOUNT is 1, not 0.
"""

import uuid

import pytest
from mssql_python import IntegrityError, OperationalError, ProgrammingError, connect


def _scalar(cur, sql):
    cur.execute(sql)
    row = cur.fetchone()
    return row[0]


def _trancount(cur) -> int:
    return _scalar(cur, "SELECT @@TRANCOUNT")


def _xact_state(cur) -> int:
    return _scalar(cur, "SELECT XACT_STATE()")


def _advance_to_rowset(cur) -> None:
    """Skip statements that return no rowset (DML/control flow) inside a batch."""
    while cur.description is None:
        if not cur.nextset():
            raise AssertionError("batch returned no result set")


def _rows(cur, table: str) -> list[tuple]:
    cur.execute(f"SELECT id, v FROM {table} ORDER BY id")
    return [tuple(r) for r in cur.fetchall()]


@pytest.fixture
def temp_table(conn, cursor):
    """#temp table (id PK, v) committed, so later rollbacks cannot drop it."""
    cursor.execute("CREATE TABLE #sp (id int NOT NULL PRIMARY KEY, v int NOT NULL)")
    conn.commit()
    return "#sp"


def _proc_name() -> str:
    return f"ci_sp_{uuid.uuid4().hex[:12]}"


def test_baseline_trancount_is_one_with_autocommit_off(cursor):
    # With autocommit off the driver opens an implicit transaction on first statement.
    assert _trancount(cursor) == 1
    assert _xact_state(cursor) == 1


def test_trancount_and_xact_state_are_plain_ints(cursor):
    cursor.execute("SELECT @@TRANCOUNT, XACT_STATE()")
    tc, xs = cursor.fetchone()
    assert type(tc) is int and tc == 1
    assert type(xs) is int and xs == 1


def test_xact_state_is_zero_and_trancount_zero_under_autocommit(conn, cursor):
    conn.autocommit = True
    assert _trancount(cursor) == 0
    assert _xact_state(cursor) == 0


def test_save_transaction_then_rollback_to_keeps_outer_transaction(conn, cursor, temp_table):
    cursor.execute("BEGIN TRANSACTION")
    assert _trancount(cursor) == 2
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 10)")
    cursor.execute("SAVE TRANSACTION sp_keep")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (2, 20)")
    cursor.execute("ROLLBACK TRANSACTION sp_keep")

    # Rollback to a savepoint does not decrement @@TRANCOUNT and does not end the transaction.
    assert _trancount(cursor) == 2
    assert _xact_state(cursor) == 1
    assert _rows(cursor, temp_table) == [(1, 10)]

    conn.commit()
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == [(1, 10)]


def test_rollback_to_savepoint_does_not_decrement_trancount_in_nested_begin(conn, cursor, temp_table):
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("SAVE TRANSACTION inner_sp")
    assert _trancount(cursor) == 3
    cursor.execute("ROLLBACK TRANSACTION inner_sp")
    assert _trancount(cursor) == 3
    cursor.execute("ROLLBACK TRANSACTION")  # plain ROLLBACK unwinds every level
    # Right after the plain ROLLBACK the server count is 0; the driver re-opens its txn on next use.
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == []


def test_reused_savepoint_name_rolls_back_to_most_recent_save(conn, cursor, temp_table):
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 1)")
    cursor.execute("SAVE TRANSACTION dup_sp")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (2, 2)")
    cursor.execute("SAVE TRANSACTION dup_sp")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (3, 3)")
    cursor.execute("ROLLBACK TRANSACTION dup_sp")
    assert _rows(cursor, temp_table) == [(1, 1), (2, 2)]
    conn.commit()
    assert _rows(cursor, temp_table) == [(1, 1), (2, 2)]


def test_nested_begin_commit_decrements_depth_and_outer_rollback_discards_all(conn, cursor, temp_table):
    base = _trancount(cursor)
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("BEGIN TRANSACTION")
    assert _trancount(cursor) == base + 2
    cursor.execute(f"INSERT INTO {temp_table} VALUES (7, 70)")
    cursor.execute("COMMIT TRANSACTION")  # inner commit: depth only, no durability
    assert _trancount(cursor) == base + 1
    assert _rows(cursor, temp_table) == [(7, 70)]
    conn.rollback()  # driver rollback unwinds the whole stack
    assert _rows(cursor, temp_table) == []


def test_plain_rollback_in_inner_level_unwinds_all_levels(conn, cursor, temp_table):
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 1)")
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (2, 2)")
    assert _trancount(cursor) == 3
    cursor.execute("ROLLBACK TRANSACTION")
    # A plain ROLLBACK sets the server count to 0; the next statement re-opens the driver txn.
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == []


def test_python_rollback_resets_stacked_manual_begins(conn, cursor, temp_table):
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (5, 5)")
    assert _trancount(cursor) == 3
    conn.rollback()
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == []


def test_rollback_to_unknown_savepoint_raises_operational_error(conn, cursor, temp_table):
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 1)")
    with pytest.raises(OperationalError, match="No transaction or savepoint of that name was found"):
        cursor.execute("ROLLBACK TRANSACTION no_such_savepoint")
    # The connection and the uncommitted insert survive the failed savepoint rollback.
    assert _rows(cursor, temp_table) == [(1, 1)]
    conn.commit()
    assert _rows(cursor, temp_table) == [(1, 1)]


def test_error_without_xact_abort_keeps_transaction_committable(conn, cursor, temp_table):
    cursor.execute("SET XACT_ABORT OFF")
    conn.commit()
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 1)")
    with pytest.raises(IntegrityError):
        cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 2)")
    # Statement-level rollback only: the transaction is still committable.
    assert _xact_state(cursor) == 1
    conn.commit()
    assert _rows(cursor, temp_table) == [(1, 1)]


def test_xact_abort_on_rolls_back_explicit_transaction_on_constraint_error(conn, cursor, temp_table):
    cursor.execute("SET XACT_ABORT ON")
    conn.commit()
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 1)")
    conn.commit()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute(f"INSERT INTO {temp_table} VALUES (2, 2)")
    with pytest.raises(IntegrityError):
        cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 9)")
    # The whole explicit transaction, including the (2, 2) insert, was rolled back by the server.
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == [(1, 1)]


def test_xact_state_minus_one_inside_catch_for_doomed_transaction(conn, cursor, temp_table):
    cursor.execute("SET XACT_ABORT ON")
    conn.commit()
    cursor.execute(f"INSERT INTO {temp_table} VALUES (1, 1)")
    conn.commit()
    cursor.execute(
        "BEGIN TRY "
        f"INSERT INTO {temp_table} VALUES (1, 2) "
        "END TRY "
        "BEGIN CATCH "
        "SELECT XACT_STATE() AS xs, ERROR_NUMBER() AS en; "
        "END CATCH"
    )
    # The TRY path produces no result set; the CATCH SELECT is the next result set.
    assert cursor.nextset() is True
    row = cursor.fetchone()
    assert tuple(row) == (-1, 2627)
    assert type(row[0]) is int
    # The batch ends with an uncommittable transaction; the driver reports it and rolls back.
    with pytest.raises(ProgrammingError, match="Uncommittable transaction"):
        cursor.nextset()
    conn.rollback()
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == [(1, 1)]


def test_doomed_transaction_rejects_writes_with_error_3930(conn, cursor, temp_table):
    cursor.execute("SET XACT_ABORT ON")
    conn.commit()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute(
        "BEGIN TRY "
        f"INSERT INTO {temp_table} VALUES (1, 1) "
        f"INSERT INTO {temp_table} VALUES (1, 2) "
        "END TRY "
        "BEGIN CATCH "
        "SELECT XACT_STATE() AS xs, ERROR_NUMBER() AS en; "
        "BEGIN TRY "
        f"INSERT INTO {temp_table} VALUES (9, 9) "
        "END TRY "
        "BEGIN CATCH "
        "SELECT ERROR_NUMBER() AS en2, XACT_STATE() AS xs2; "
        "END CATCH "
        "END CATCH"
    )
    rowsets = []
    # The batch ends with an uncommittable transaction; the driver reports it on the final nextset.
    with pytest.raises(ProgrammingError, match="Uncommittable transaction is detected"):
        while True:
            if cursor.description is not None:
                rowsets.append([tuple(r) for r in cursor.fetchall()])
            if not cursor.nextset():
                break
    # Outer CATCH: the duplicate key made the transaction uncommittable (XACT_STATE -1).
    assert rowsets[0] == [(-1, 2627)]
    # Inside the doomed transaction the write is refused with error 3930, state still -1.
    assert rowsets[1] == [(3930, -1)]
    conn.rollback()
    assert _trancount(cursor) == 1
    assert _rows(cursor, temp_table) == []


def test_error_266_when_procedure_leaves_transaction_open(conn, cursor):
    name = _proc_name()
    cursor.execute("SET XACT_ABORT OFF")
    conn.commit()
    cursor.execute(f"CREATE PROCEDURE dbo.{name} AS BEGIN TRANSACTION; SELECT 1 AS x;")
    conn.commit()
    try:
        cursor.execute(f"EXEC dbo.{name}")
        with pytest.raises(OperationalError, match="Previous count = 1, current count = 2"):
            cursor.fetchall()
        # The procedure's open BEGIN TRANSACTION is still counted by the server.
        assert _trancount(cursor) == 2
    finally:
        conn.rollback()
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{name}")
        conn.commit()


def test_error_266_when_procedure_rolls_back_caller_transaction(conn, cursor):
    name = _proc_name()
    cursor.execute("SET XACT_ABORT OFF")
    conn.commit()
    cursor.execute(f"CREATE PROCEDURE dbo.{name} AS ROLLBACK TRANSACTION; SELECT 2 AS x;")
    conn.commit()
    try:
        cursor.execute("BEGIN TRANSACTION")
        assert _trancount(cursor) == 2
        cursor.execute(f"EXEC dbo.{name}")
        with pytest.raises(OperationalError, match="mismatching number of BEGIN and COMMIT"):
            cursor.fetchall()
    finally:
        conn.rollback()
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{name}")
        conn.commit()


def test_savepoint_inside_procedure_is_balanced_and_raises_no_266(conn, cursor, temp_table):
    name = _proc_name()
    cursor.execute(
        f"CREATE PROCEDURE dbo.{name} AS "
        "SAVE TRANSACTION proc_sp; "
        f"INSERT INTO {temp_table} VALUES (1, 1); "
        f"INSERT INTO {temp_table} VALUES (2, 2); "
        "ROLLBACK TRANSACTION proc_sp; "
        "SELECT 3 AS x;"
    )
    conn.commit()
    try:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(f"EXEC dbo.{name}")
        _advance_to_rowset(cursor)
        assert [tuple(r) for r in cursor.fetchall()] == [(3,)]
        # ROLLBACK to a procedure-local savepoint leaves the caller's depth untouched.
        assert _trancount(cursor) == 2
        assert _rows(cursor, temp_table) == []
        conn.rollback()
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{name}")
        conn.commit()


def test_balanced_begin_commit_inside_procedure_raises_no_266(conn, cursor):
    name = _proc_name()
    cursor.execute(f"CREATE PROCEDURE dbo.{name} AS BEGIN TRANSACTION; SELECT 4 AS x; COMMIT;")
    conn.commit()
    try:
        cursor.execute(f"EXEC dbo.{name}")
        _advance_to_rowset(cursor)
        assert [tuple(r) for r in cursor.fetchall()] == [(4,)]
        assert _trancount(cursor) == 1
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{name}")
        conn.commit()
