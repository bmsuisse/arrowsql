"""Errors that occur after rows (or after earlier result sets) in a batch.

Research sources:
- https://github.com/microsoft/mssql-python/issues/333 (error only surfaces when rows are fetched; fixed in PR #338)
- https://github.com/microsoft/mssql-python/pull/338/files (tests: execute succeeds, fetchall raises IntegrityError)
- https://github.com/mkleehammer/pyodbc/issues/673 (RAISERROR before rows, "No results", needs nextset())
- https://github.com/mkleehammer/pyodbc/issues/69 (divide-by-zero in a procedure only surfaces on nextset(); SET NOCOUNT ON advice)
- https://github.com/mkleehammer/pyodbc/issues/700 (fetch works only after nextset() when earlier statements emit extra result sets)
- https://github.com/mkleehammer/pyodbc/issues/740 (extra messages from a backup add result sets before the real one)
- https://github.com/microsoft/mssql-python/pull/618 (PRINT / messages between result sets were lost; SQL_ERROR handled via check_error)
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/troubleshoot-query-data-and-operations
- https://learn.microsoft.com/en-us/sql/t-sql/language-elements/throw-transact-sql
- https://learn.microsoft.com/en-us/sql/t-sql/language-elements/try-catch-transact-sql

Ground truth: the upstream ODBC driver (.venv-upstream). Behaviors asserted here were
confirmed by probing that driver: a divide-by-zero that occurs after N good rows
returns those rows through fetchmany() and raises DataError on the fetch that reaches
the bad row; an error in result set K surfaces from nextset() that moves into set K;
a THROW inside TRY is caught and returned as a result set (preceded by an empty set
for a failing SELECT); an uncaught severity-16 RAISERROR or THROW surfaces as
ProgrammingError (SQLSTATE 42000). Two upstream bugs are marked xfail(strict): a
severity-10 RAISERROR or a DONE count between result sets breaks the next result set.
"""

import pytest
from mssql_python import DataError, ProgrammingError


def rows(result):
    """Normalize driver Row objects to plain tuples for comparison."""
    return [tuple(r) for r in result]


ROWS_THEN_DIV0 = (
    "SELECT 1/(n-5) AS v FROM (VALUES (1),(2),(3),(4),(5),(6)) t(n) ORDER BY n"
)


def test_fetchmany_returns_rows_before_error_then_raises(cursor):
    cursor.execute(ROWS_THEN_DIV0)
    assert rows(cursor.fetchmany(3)) == [(0,), (0,), (0,)]
    # Rows 4 is still good; the batch is short because row 5 is the bad one.
    assert rows(cursor.fetchmany(3)) == [(-1,)]
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.fetchmany(3)


def test_fetchall_raises_and_does_not_return_partial_rows(cursor):
    cursor.execute(ROWS_THEN_DIV0)
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.fetchall()


def test_fetchone_loop_yields_good_rows_then_raises(cursor):
    cursor.execute(ROWS_THEN_DIV0)
    seen = []
    with pytest.raises(DataError, match="Divide by zero"):
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            seen.append(tuple(row))
    # Rows n=1..4 give 1/(n-5): 0, 0, 0, -1. Row n=5 is the failing one.
    assert seen == [(0,), (0,), (0,), (-1,)]


def test_error_on_first_row_raises_at_execute(cursor):
    # No row precedes the error, so the failure is reported by execute() itself.
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.execute("SELECT 1/0 AS v")


def test_connection_usable_after_mid_fetch_error(cursor):
    cursor.execute(ROWS_THEN_DIV0)
    with pytest.raises(DataError):
        cursor.fetchall()
    cursor.execute("SELECT 7 AS k")
    assert rows(cursor.fetchall()) == [(7,)]


def test_error_in_second_result_set_surfaces_at_nextset(cursor):
    cursor.execute("SELECT 1 AS a; SELECT 1/0 AS b")
    assert rows(cursor.fetchall()) == [(1,)]
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.nextset()


def test_error_in_third_result_set_via_nextset(cursor):
    cursor.execute("SELECT 1 AS a; SELECT 2 AS b; SELECT 3/0 AS c")
    assert rows(cursor.fetchall()) == [(1,)]
    assert cursor.nextset() is True
    assert rows(cursor.fetchall()) == [(2,)]
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.nextset()


def test_nextset_after_error_moves_past_failed_set(cursor):
    # The failed result set is skipped; the following one is still readable.
    cursor.execute("SELECT 1 AS a; SELECT 1/0 AS b; SELECT 3 AS c")
    assert rows(cursor.fetchall()) == [(1,)]
    with pytest.raises(DataError):
        cursor.nextset()
    assert cursor.nextset() is True
    assert rows(cursor.fetchall()) == [(3,)]
    assert cursor.nextset() is False


def test_cursor_iteration_stops_at_end_of_current_set_without_error(cursor):
    # Iteration covers only the current result set; the error in set 3 is not reached.
    cursor.execute("SELECT 1 AS a; SELECT 2 AS b; SELECT 3/0 AS c")
    assert rows(cursor) == [(1,)]


def test_try_catch_swallows_throw_and_returns_error_row(cursor):
    cursor.execute(
        "BEGIN TRY "
        "  SELECT 1 AS x; "
        "  THROW 50001, 'boom', 1; "
        "END TRY "
        "BEGIN CATCH "
        "  SELECT ERROR_NUMBER() AS n, ERROR_MESSAGE() AS m; "
        "END CATCH"
    )
    assert rows(cursor.fetchall()) == [(1,)]
    assert cursor.nextset() is True
    assert rows(cursor.fetchall()) == [(50001, "boom")]
    assert cursor.nextset() is False


def test_try_catch_divide_by_zero_caught_in_batch(cursor):
    # SQL Server sends the TRY SELECT's column header before the error, so the
    # caught error leaves an empty result set ahead of the CATCH result set.
    cursor.execute(
        "BEGIN TRY SELECT 1/0 AS v; END TRY "
        "BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH"
    )
    assert rows(cursor.fetchall()) == []
    assert cursor.nextset() is True
    assert rows(cursor.fetchall()) == [(8134,)]
    assert cursor.nextset() is False


def test_uncaught_throw_after_rows_raises_programming_error_on_nextset(cursor):
    cursor.execute("SELECT 1 AS a; THROW 50002, 'x', 1")
    assert rows(cursor.fetchall()) == [(1,)]
    with pytest.raises(ProgrammingError, match="x"):
        cursor.nextset()


def test_raiserror_severity_16_after_rows_raises_on_nextset(cursor):
    cursor.execute("SELECT 1 AS a; RAISERROR('warn', 16, 1); SELECT 2 AS b")
    assert rows(cursor.fetchall()) == [(1,)]
    with pytest.raises(ProgrammingError, match="warn"):
        cursor.nextset()


@pytest.mark.upstream_bug(
    reason="informational RAISERROR (severity 10) between result sets is treated as a "
    "result set; the next set's fetch fails with 'Invalid cursor state' (SQLSTATE 24000)"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_raiserror_informational_does_not_break_result_sets(cursor):
    # Severity 10 is a message, not an error: result sets keep flowing.
    cursor.execute("SELECT 1 AS a; RAISERROR('info', 10, 1); SELECT 2 AS b")
    assert rows(cursor.fetchall()) == [(1,)]
    assert cursor.nextset() is True
    assert rows(cursor.fetchall()) == [(2,)]


@pytest.mark.upstream_bug(
    reason="UPDATE (DONE count, no result set) between result sets: nextset() returns True "
    "for the count-only step and the following fetch fails with 'Invalid cursor state'"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_rowcount_only_statement_between_sets_is_skipped_by_nextset(cursor):
    # Without NOCOUNT, the UPDATE produces a row-count token but no result set.
    cursor.execute("CREATE TABLE #mid0 (v INT); INSERT INTO #mid0 VALUES (1), (2)")
    cursor.execute("SELECT 1 AS a; UPDATE #mid0 SET v = v; SELECT 2 AS b")
    assert rows(cursor.fetchall()) == [(1,)]
    assert cursor.nextset() is True
    assert rows(cursor.fetchall()) == [(2,)]


def test_nocount_on_rowcount_between_sets_then_error_in_third_set(cursor):
    # With NOCOUNT ON the UPDATE emits no count token; the error in set 3 surfaces at nextset().
    cursor.execute("CREATE TABLE #mid (v INT); INSERT INTO #mid VALUES (1), (2)")
    cursor.execute(
        "SET NOCOUNT ON; "
        "SELECT v FROM #mid ORDER BY v; "
        "UPDATE #mid SET v = v + 10; "
        "SELECT v / 0 AS bad FROM #mid"
    )
    assert rows(cursor.fetchall()) == [(1,), (2,)]
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.nextset()


def test_error_inside_procedure_after_rows(cursor):
    cursor.execute("CREATE PROCEDURE #p_mid AS BEGIN SELECT 1 AS a; SELECT 1/0 AS b; END")
    cursor.execute("EXEC #p_mid")
    assert rows(cursor.fetchall()) == [(1,)]
    with pytest.raises(DataError, match="Divide by zero"):
        cursor.nextset()
