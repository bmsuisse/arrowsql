"""Multiple result sets: nextset() across mixed batches, PRINT ordering, rowcount, empty sets.

Covers the classic pyodbc / mssql-python pain points:
- nextset() walking SELECT/SELECT and INSERT/SELECT batches
- "No results. Previous SQL was not a query" when a batch starts with PRINT
- rowcount for DML statements inside a multi-statement batch
- SET NOCOUNT ON/OFF changing which statements produce count results
- empty result sets keeping their column metadata
"""

import datetime
import decimal

import pytest
from mssql_python import DataError, ProgrammingError


def _names(cursor):
    return None if cursor.description is None else [d[0] for d in cursor.description]


def _fetchall(cursor):
    # Rows are driver Row objects, which do not compare equal to plain tuples.
    return [tuple(row) for row in cursor.fetchall()]


def _fetchone(cursor):
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def test_nextset_walks_two_select_result_sets(cursor):
    cursor.execute("SELECT 1 AS a UNION ALL SELECT 2; SELECT 'x' AS b")

    assert _names(cursor) == ["a"]
    assert _fetchall(cursor) == [(1,), (2,)]

    assert cursor.nextset() is True
    assert _names(cursor) == ["b"]
    assert _fetchall(cursor) == [("x",)]

    assert cursor.nextset() is False
    assert cursor.description is None


def test_nextset_returns_false_for_single_result_set(cursor):
    cursor.execute("SELECT 7 AS only")

    assert _fetchall(cursor) == [(7,)]
    assert cursor.nextset() is False
    assert cursor.description is None


def test_insert_then_select_batch_exposes_dml_count_first(cursor):
    cursor.execute("CREATE TABLE #ins (id INT)")
    cursor.execute("INSERT INTO #ins VALUES (1),(2),(3); SELECT id FROM #ins ORDER BY id;")

    # The INSERT count is a result slot with no column metadata, so the
    # cursor starts with no description and the rowcount of the INSERT.
    assert cursor.description is None
    assert cursor.rowcount == 3

    assert cursor.nextset() is True
    assert _names(cursor) == ["id"]
    assert _fetchall(cursor) == [(1,), (2,), (3,)]
    assert cursor.nextset() is False


def test_fetch_on_dml_count_before_nextset_raises_programming_error(cursor):
    cursor.execute("CREATE TABLE #ins2 (id INT)")
    cursor.execute("INSERT INTO #ins2 VALUES (1); SELECT id FROM #ins2;")

    with pytest.raises(ProgrammingError):
        cursor.fetchall()

    # Calling nextset() moves to the SELECT, and the rows are then readable.
    assert cursor.nextset() is True
    assert _fetchall(cursor) == [(1,)]


def test_single_dml_without_select_has_rowcount_and_no_result_set(cursor):
    cursor.execute("CREATE TABLE #single (id INT)")
    cursor.execute("INSERT INTO #single VALUES (10),(20)")

    assert cursor.description is None
    assert cursor.rowcount == 2
    assert cursor.nextset() is False

    with pytest.raises(ProgrammingError):
        cursor.fetchall()


def test_print_before_select_yields_result_set_after_nextset(cursor):
    cursor.execute("PRINT 'hello'; SELECT 42 AS v;")

    # PRINT produces a message, not a result set: no columns on the first slot.
    assert cursor.description is None
    assert cursor.nextset() is True
    assert _names(cursor) == ["v"]
    assert _fetchall(cursor) == [(42,)]
    assert cursor.nextset() is False


def test_fetch_on_print_only_slot_raises_programming_error(cursor):
    cursor.execute("PRINT 'before the select'; SELECT 1 AS n")

    with pytest.raises(ProgrammingError):
        cursor.fetchall()
    with pytest.raises(ProgrammingError):
        cursor.fetchone()

    assert cursor.nextset() is True
    assert _fetchone(cursor) == (1,)


def test_print_text_lands_in_messages_not_in_results(cursor):
    cursor.execute("PRINT 'hello there'; SELECT 1 AS z")

    texts = [message[1] for message in cursor.messages]
    assert any("hello there" in text for text in texts)

    assert cursor.nextset() is True
    assert _fetchall(cursor) == [(1,)]


def test_stored_proc_print_then_two_selects(cursor):
    cursor.execute(
        "CREATE PROCEDURE #wavea_print_first AS BEGIN "
        "PRINT 'starting'; "
        "SELECT 10 AS x; "
        "SELECT 'y' AS y; "
        "END"
    )
    cursor.execute("EXEC #wavea_print_first")

    assert cursor.description is None
    assert cursor.nextset() is True
    assert _names(cursor) == ["x"]
    assert _fetchall(cursor) == [(10,)]

    assert cursor.nextset() is True
    assert _names(cursor) == ["y"]
    assert _fetchall(cursor) == [("y",)]

    assert cursor.nextset() is False


def test_nocount_on_hides_dml_count_slots(cursor):
    cursor.execute(
        "SET NOCOUNT ON; CREATE TABLE #nc (id INT); "
        "INSERT INTO #nc VALUES (1),(2); SELECT id FROM #nc ORDER BY id;"
    )

    # With NOCOUNT ON the INSERT produces no count slot, so the SELECT is first.
    assert _names(cursor) == ["id"]
    assert _fetchall(cursor) == [(1,), (2,)]
    assert cursor.nextset() is False


def test_nocount_on_single_insert_rowcount_is_minus_one(cursor):
    cursor.execute("SET NOCOUNT ON")
    cursor.execute("CREATE TABLE #nc_single (id INT)")
    cursor.execute("INSERT INTO #nc_single VALUES (1),(2),(3)")

    assert cursor.rowcount == -1
    assert cursor.description is None
    assert cursor.nextset() is False


def test_nocount_off_single_insert_rowcount_is_row_count(cursor):
    cursor.execute("SET NOCOUNT OFF")
    cursor.execute("CREATE TABLE #nc_off (id INT)")
    cursor.execute("INSERT INTO #nc_off VALUES (1),(2),(3),(4)")

    assert cursor.rowcount == 4


def test_empty_result_set_keeps_column_metadata(cursor):
    cursor.execute("SELECT 1 AS id, 'a' AS label WHERE 1 = 0")

    assert _names(cursor) == ["id", "label"]
    assert _fetchall(cursor) == []
    assert _fetchone(cursor) is None
    assert cursor.nextset() is False


def test_empty_result_set_then_non_empty_result_set(cursor):
    cursor.execute("CREATE TABLE #empty (id INT)")
    cursor.execute("SELECT id FROM #empty; SELECT 5 AS z")

    assert _names(cursor) == ["id"]
    assert _fetchone(cursor) is None

    assert cursor.nextset() is True
    assert _names(cursor) == ["z"]
    assert _fetchone(cursor) == (5,)
    assert _fetchone(cursor) is None
    assert cursor.nextset() is False


def test_nextset_without_fetching_discards_unread_rows(cursor):
    cursor.execute("SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3; SELECT 'x' AS b")

    assert cursor.nextset() is True
    assert _names(cursor) == ["b"]
    assert _fetchall(cursor) == [("x",)]


def test_mixed_types_are_preserved_across_result_sets(cursor):
    cursor.execute(
        "SELECT CAST(1.50 AS DECIMAL(5,2)) AS d, CAST(7 AS INT) AS i; "
        "SELECT N'text' AS t; "
        "SELECT CAST('2024-01-02T03:04:05' AS DATETIME2) AS ts"
    )

    assert _fetchall(cursor) == [(decimal.Decimal("1.50"), 7)]
    assert cursor.nextset() is True
    assert _fetchall(cursor) == [("text",)]
    assert cursor.nextset() is True
    assert _fetchall(cursor) == [(datetime.datetime(2024, 1, 2, 3, 4, 5),)]


def test_divide_by_zero_in_later_statement_raises_from_nextset(cursor):
    cursor.execute("SELECT 1 AS a; SELECT 1/0 AS b; SELECT 3 AS c")

    assert _fetchall(cursor) == [(1,)]
    with pytest.raises(DataError, match="Division by zero"):
        cursor.nextset()

    # The cursor is still usable for a fresh statement after the batch error.
    cursor.execute("SELECT 'after' AS s")
    assert _fetchall(cursor) == [("after",)]


def test_rowcount_per_separate_execute_is_independent(cursor):
    cursor.execute("CREATE TABLE #per_exec (id INT)")

    cursor.execute("INSERT INTO #per_exec VALUES (5),(6)")
    assert cursor.rowcount == 2

    cursor.execute("INSERT INTO #per_exec VALUES (7),(8),(9),(10),(11)")
    assert cursor.rowcount == 5

    cursor.execute("DELETE FROM #per_exec WHERE id > 8")
    assert cursor.rowcount == 3


def test_permanent_table_insert_select_batch(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL, label NVARCHAR(20) NOT NULL)")
    cursor.connection.commit()

    cursor.execute(
        f"INSERT INTO dbo.{table_name} (id, label) VALUES (1, N'one'), (2, N'two'); "
        f"SELECT id, label FROM dbo.{table_name} ORDER BY id;"
    )
    assert cursor.description is None
    assert cursor.rowcount == 2

    assert cursor.nextset() is True
    assert _names(cursor) == ["id", "label"]
    assert _fetchall(cursor) == [(1, "one"), (2, "two")]
    assert cursor.nextset() is False
    cursor.connection.commit()


@pytest.mark.upstream_bug(
    reason=(
        "After nextset() moves onto a later INSERT/UPDATE/DELETE count slot, upstream "
        "keeps reporting the previous statement's rowcount instead of the new one"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_rowcount_follows_nextset_onto_later_dml(cursor):
    cursor.execute("CREATE TABLE #rc (id INT)")
    cursor.execute("INSERT INTO #rc VALUES (1); INSERT INTO #rc VALUES (2),(3),(4); SELECT COUNT(*) AS n FROM #rc")

    assert cursor.rowcount == 1
    assert cursor.nextset() is True
    assert cursor.description is None
    assert cursor.rowcount == 3

    assert cursor.nextset() is True
    assert _fetchall(cursor) == [(4,)]
    assert cursor.nextset() is False
