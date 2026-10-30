"""Scope traps: SCOPE_IDENTITY / @@IDENTITY, OUTPUT INSERTED, #temp tables and
table variables across executes, with and without parameters.

Upstream notes encoded here:
- A batch that starts with a row-count-only statement (INSERT without
  SET NOCOUNT ON) leaves ``cursor.description`` as None after execute();
  the SELECT result is only reachable via ``nextset()``.
- ``SCOPE_IDENTITY()`` is per scope (batch / module); ``@@IDENTITY`` is per
  session and picks up identity values written by triggers.
- ``#temp`` tables live for the connection; ``#temp`` tables created inside
  ``sp_executesql`` are dropped when that call returns.
- Table variables are batch-scoped.
"""

import uuid

import pytest
from mssql_python import connect
from mssql_python.exceptions import ProgrammingError


def _one(cur):
    row = cur.fetchone()
    return None if row is None else tuple(row)


def _all(cur):
    return [tuple(r) for r in cur.fetchall()]


def _temp_name(prefix):
    return f"#{prefix}_{uuid.uuid4().hex[:8]}"


def test_scope_identity_same_batch_with_param_returns_new_id(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute(
        "SET NOCOUNT ON; "
        "INSERT INTO #t (v) VALUES (?); "
        "SELECT CAST(SCOPE_IDENTITY() AS INT)",
        "first",
    )
    row = _one(cursor)
    assert row == (1,)
    assert type(row[0]) is int


def test_scope_identity_without_nocount_needs_nextset(cursor):
    # Without SET NOCOUNT ON, the INSERT's row count is the first result, so
    # the SELECT is not reachable via fetch right after execute().
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute(
        "INSERT INTO #t (v) VALUES (?); SELECT CAST(SCOPE_IDENTITY() AS INT)",
        "first",
    )
    assert cursor.description is None
    assert cursor.nextset() is True
    assert _one(cursor) == (1,)


def test_scope_identity_increments_across_separate_inserts(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(10,5) PRIMARY KEY, v INT)")
    ids = []
    for value in (1, 2, 3):
        cursor.execute(
            "SET NOCOUNT ON; INSERT INTO #t (v) VALUES (?); "
            "SELECT CAST(SCOPE_IDENTITY() AS INT)",
            value,
        )
        ids.append(_one(cursor)[0])
    assert ids == [10, 15, 20]


def test_scope_identity_is_null_in_a_later_execute(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v INT)")
    cursor.execute("INSERT INTO #t (v) VALUES (?)", 42)
    cursor.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
    assert _one(cursor) == (None,)


def test_at_at_identity_is_visible_across_executes(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v INT)")
    cursor.execute("INSERT INTO #t (v) VALUES (?)", 42)
    cursor.execute("SELECT CAST(@@IDENTITY AS INT), CAST(SCOPE_IDENTITY() AS INT)")
    assert _one(cursor) == (1, None)


def test_scope_identity_bigint_identity_keeps_full_value(cursor):
    seed = 9_000_000_000
    cursor.execute(f"CREATE TABLE #t (id BIGINT IDENTITY({seed},1) PRIMARY KEY, v INT)")
    cursor.execute(
        "SET NOCOUNT ON; INSERT INTO #t (v) VALUES (?); "
        "SELECT CAST(SCOPE_IDENTITY() AS BIGINT)",
        7,
    )
    assert _one(cursor) == (seed,)


def test_at_at_identity_follows_trigger_insert_scope_identity_does_not(cursor, conn, table_name):
    audit = f"{table_name}_audit"
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT IDENTITY(1,1) PRIMARY KEY, v INT)"
    )
    cursor.execute(
        f"CREATE TABLE dbo.{audit} (aid INT IDENTITY(100,1) PRIMARY KEY, src INT)"
    )
    cursor.execute(
        f"CREATE TRIGGER dbo.{table_name}_trg ON dbo.{table_name} AFTER INSERT AS "
        f"BEGIN INSERT INTO dbo.{audit} (src) SELECT id FROM inserted; END"
    )
    conn.commit()
    try:
        cursor.execute(
            f"SET NOCOUNT ON; INSERT INTO dbo.{table_name} (v) VALUES (?); "
            "SELECT CAST(SCOPE_IDENTITY() AS INT), CAST(@@IDENTITY AS INT)",
            5,
        )
        # SCOPE_IDENTITY ignores the trigger's insert; @@IDENTITY sees it.
        assert _one(cursor) == (1, 100)
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{audit}")
        conn.commit()


def test_output_inserted_returns_generated_id_and_columns(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute(
        "INSERT INTO #t (v) OUTPUT INSERTED.id, INSERTED.v VALUES (?)",
        "beta",
    )
    assert [d[0] for d in cursor.description] == ["id", "v"]
    assert _all(cursor) == [(1, "beta")]


def test_output_inserted_multi_row_values_returns_all_ids(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute(
        "INSERT INTO #t (v) OUTPUT INSERTED.id, INSERTED.v VALUES (?), (?), (?)",
        "a",
        "b",
        "c",
    )
    rows = sorted(_all(cursor))
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_output_into_table_variable_in_same_batch(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v INT)")
    cursor.execute(
        "SET NOCOUNT ON; "
        "DECLARE @out TABLE (id INT, v INT); "
        "INSERT INTO #t (v) OUTPUT INSERTED.id, INSERTED.v INTO @out VALUES (?); "
        "SELECT id, v FROM @out",
        99,
    )
    assert _all(cursor) == [(1, 99)]


def test_output_without_into_on_table_with_trigger_raises(cursor, conn, table_name):
    audit = f"{table_name}_audit"
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT IDENTITY(1,1) PRIMARY KEY, v INT)"
    )
    cursor.execute(
        f"CREATE TABLE dbo.{audit} (aid INT IDENTITY(1,1) PRIMARY KEY, src INT)"
    )
    cursor.execute(
        f"CREATE TRIGGER dbo.{table_name}_trg ON dbo.{table_name} AFTER INSERT AS "
        f"BEGIN INSERT INTO dbo.{audit} (src) SELECT id FROM inserted; END"
    )
    conn.commit()
    try:
        with pytest.raises(ProgrammingError, match="OUTPUT clause without INTO"):
            cursor.execute(
                f"INSERT INTO dbo.{table_name} (v) OUTPUT INSERTED.id VALUES (?)",
                1,
            )
        # The failed statement must not poison the connection.
        cursor.execute("SELECT 1")
        assert _one(cursor) == (1,)
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{audit}")
        conn.commit()


def test_temp_table_created_then_used_in_next_execute_with_params(cursor):
    cursor.execute("CREATE TABLE #stage (k INT, name NVARCHAR(10))")
    cursor.execute("INSERT INTO #stage VALUES (?, ?)", 1, "a")
    cursor.execute("INSERT INTO #stage VALUES (?, ?)", 2, "b")
    cursor.execute("INSERT INTO #stage VALUES (?, ?)", 3, "c")
    cursor.execute("SELECT k, name FROM #stage WHERE k >= ? ORDER BY k", 2)
    assert _all(cursor) == [(2, "b"), (3, "c")]


def test_temp_table_update_and_delete_with_params_report_rowcount(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute("INSERT INTO #t (v) VALUES (?), (?)", "x", "y")
    cursor.execute("UPDATE #t SET v = ? WHERE id = ?", "z", 2)
    assert cursor.rowcount == 1
    cursor.execute("DELETE FROM #t WHERE v = ?", "x")
    assert cursor.rowcount == 1
    cursor.execute("SELECT id, v FROM #t ORDER BY id")
    assert _all(cursor) == [(2, "z")]


def test_temp_table_survives_many_executes_on_same_cursor(cursor):
    cursor.execute("CREATE TABLE #t (n INT)")
    for i in range(5):
        cursor.execute("INSERT INTO #t (n) VALUES (?)", i)
    cursor.execute("SELECT COUNT(*), SUM(n) FROM #t")
    assert _one(cursor) == (5, 10)


def test_temp_table_not_visible_to_another_connection(cursor, conn_str):
    name = _temp_name("private")
    cursor.execute(f"CREATE TABLE {name} (x INT)")
    other = connect(conn_str)
    try:
        other_cur = other.cursor()
        with pytest.raises(ProgrammingError, match="Invalid object name"):
            other_cur.execute(f"SELECT COUNT(*) FROM {name}")
    finally:
        other.close()


def test_temp_table_created_in_sp_executesql_is_dropped_on_return(cursor):
    cursor.execute(
        "EXEC sp_executesql N'CREATE TABLE #dyn (k INT); INSERT INTO #dyn VALUES (@p)', "
        "N'@p INT', @p = ?",
        7,
    )
    with pytest.raises(ProgrammingError, match="Invalid object name"):
        cursor.execute("SELECT k FROM #dyn")


def test_scope_identity_null_after_nested_sp_executesql_insert(cursor):
    cursor.execute("CREATE TABLE #t (id INT IDENTITY(1,1) PRIMARY KEY, v NVARCHAR(20))")
    cursor.execute(
        "SET NOCOUNT ON; "
        "EXEC sp_executesql N'INSERT INTO #t (v) VALUES (@p)', N'@p NVARCHAR(20)', @p = ?; "
        "SELECT CAST(SCOPE_IDENTITY() AS INT), CAST(@@IDENTITY AS INT)",
        "nested",
    )
    # The insert ran in a child scope, so SCOPE_IDENTITY() is NULL here;
    # @@IDENTITY still reports the value.
    assert _one(cursor) == (None, 1)


def test_table_variable_not_visible_in_next_execute(cursor):
    cursor.execute("DECLARE @x TABLE (i INT)")
    with pytest.raises(ProgrammingError, match="Must declare the table variable"):
        cursor.execute("SELECT i FROM @x")


def test_table_variable_scope_identity_in_single_batch_with_param(cursor):
    cursor.execute(
        "SET NOCOUNT ON; "
        "DECLARE @t TABLE (id INT IDENTITY(1,1), v INT); "
        "INSERT INTO @t (v) VALUES (?); "
        "SELECT CAST(SCOPE_IDENTITY() AS INT), v FROM @t",
        9,
    )
    assert _one(cursor) == (1, 9)


def test_scope_identity_after_insert_with_no_identity_column_is_null(cursor):
    cursor.execute("CREATE TABLE #plain (v INT)")
    cursor.execute(
        "SET NOCOUNT ON; INSERT INTO #plain (v) VALUES (?); "
        "SELECT CAST(SCOPE_IDENTITY() AS INT)",
        3,
    )
    assert _one(cursor) == (None,)
