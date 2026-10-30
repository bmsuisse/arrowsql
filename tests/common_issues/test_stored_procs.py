"""Stored procedures: input/output params, return values, result sets, NOCOUNT.

Procedures are created as session-scoped temp procedures (``#name``) so each
test cleans up with its connection. Where a test needs a permanent table it
uses the ``table_name`` fixture.
"""

import datetime
from decimal import Decimal

import pytest
from mssql_python import ProgrammingError


def _create_proc(cursor, ddl):
    cursor.execute(ddl)


def _rows(cursor):
    """Materialize driver Row objects as plain tuples for exact comparison."""
    return [tuple(r) for r in cursor.fetchall()]


def _row(cursor):
    row = cursor.fetchone()
    return None if row is None else tuple(row)


# ---------------------------------------------------------------------------
# Input parameters
# ---------------------------------------------------------------------------


def test_exec_with_qmark_params_returns_computed_row(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #add @a INT, @b INT AS BEGIN SET NOCOUNT ON; SELECT @a + @b AS s; END",
    )
    cursor.execute("EXEC #add ?, ?", [2, 3])
    rows = _rows(cursor)
    assert rows == [(5,)]
    assert type(rows[0][0]) is int
    assert cursor.description[0][0] == "s"


def test_exec_with_named_params_and_defaults(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #diff @x INT = 10, @y INT = 20 AS "
        "BEGIN SET NOCOUNT ON; SELECT @x - @y AS d; END",
    )
    cursor.execute("EXEC #diff @x=?, @y=?", [5, 2])
    assert _rows(cursor) == [(3,)]
    # Omitted parameters fall back to the procedure's declared defaults.
    cursor.execute("EXEC #diff")
    assert _rows(cursor) == [(-10,)]


def test_none_param_is_passed_as_sql_null(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #add2 @a INT, @b INT AS BEGIN SET NOCOUNT ON; SELECT @a + @b AS s; END",
    )
    cursor.execute("EXEC #add2 ?, ?", [None, 1])
    assert _rows(cursor) == [(None,)]


def test_odbc_call_escape_without_return_value(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #add3 @a INT, @b INT AS BEGIN SET NOCOUNT ON; SELECT @a + @b AS s; END",
    )
    cursor.execute("{CALL #add3(?, ?)}", [7, 8])
    assert _rows(cursor) == [(15,)]


def test_wrong_param_count_raises_programming_error(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #add4 @a INT, @b INT AS BEGIN SET NOCOUNT ON; SELECT @a + @b AS s; END",
    )
    with pytest.raises(ProgrammingError, match="expects parameter '@b'"):
        cursor.execute("EXEC #add4 ?", [1])


def test_missing_procedure_raises_programming_error(conn, cursor):
    with pytest.raises(ProgrammingError, match="Could not find stored procedure"):
        cursor.execute("EXEC dbo.ci_no_such_proc_stored_procs")


# ---------------------------------------------------------------------------
# Return values
# ---------------------------------------------------------------------------


def test_return_value_via_t_sql_declare_and_exec_assignment(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #ret_double @a INT AS BEGIN RETURN @a * 2 END",
    )
    cursor.execute("DECLARE @r INT; EXEC @r = #ret_double ?; SELECT @r AS r", [21])
    assert _rows(cursor) == [(42,)]


def test_proc_with_only_return_value_has_no_result_set(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #ret_only @a INT AS BEGIN RETURN @a END",
    )
    cursor.execute("EXEC #ret_only ?", [5])
    assert cursor.description is None
    with pytest.raises(ProgrammingError):
        _rows(cursor)


@pytest.mark.upstream_bug(
    reason="ODBC call escape with a return-value marker '{? = CALL proc(?)}' "
    "is rejected by upstream with ProgrammingError, although it is standard ODBC "
    "syntax and the procedure is valid"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_call_escape_with_return_value_marker(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #ret_double_esc @a INT AS BEGIN RETURN @a * 2 END",
    )
    # Correct behavior: the escape is accepted and executes the procedure.
    cursor.execute("{? = CALL #ret_double_esc(?)}", [None, 21])


# ---------------------------------------------------------------------------
# Output parameters
# ---------------------------------------------------------------------------


def test_output_param_int_read_via_select(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #out_plus @a INT, @o INT OUTPUT AS BEGIN SET @o = @a + 100 END",
    )
    cursor.execute("DECLARE @o INT; EXEC #out_plus ?, @o OUTPUT; SELECT @o AS v", [1])
    assert _row(cursor) == (101,)


def test_output_param_nvarchar_preserves_unicode(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #out_greet @a NVARCHAR(50), @o NVARCHAR(50) OUTPUT "
        "AS BEGIN SET @o = N'hi ' + @a END",
    )
    cursor.execute(
        "DECLARE @o NVARCHAR(50); EXEC #out_greet ?, @o OUTPUT; SELECT @o AS v",
        ["Zürich ✓"],
    )
    assert _row(cursor) == ("hi Zürich ✓",)


def test_multiple_output_params_decimal_and_date(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #out_types @o DECIMAL(10,2) OUTPUT, @d DATE OUTPUT "
        "AS BEGIN SET @o = 12.5; SET @d = '2024-02-29' END",
    )
    cursor.execute(
        "DECLARE @o DECIMAL(10,2), @d DATE; EXEC #out_types @o OUTPUT, @d OUTPUT; "
        "SELECT @o AS o, @d AS d"
    )
    row = _row(cursor)
    assert row == (Decimal("12.50"), datetime.date(2024, 2, 29))
    assert type(row[0]) is Decimal
    assert type(row[1]) is datetime.date


def test_string_args_implicitly_converted_to_int_params(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #add_str @a INT, @b INT AS BEGIN SET NOCOUNT ON; SELECT @a + @b AS s; END",
    )
    # SQL Server converts the string "5" to INT for the procedure parameter.
    cursor.execute("EXEC #add_str ?, ?", ["5", "6"])
    assert _rows(cursor) == [(11,)]


def test_invalid_string_for_int_param_raises_programming_error(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #echo_int @a INT AS BEGIN SET NOCOUNT ON; SELECT @a AS v END",
    )
    # Conversion failures while binding procedure parameters come back as SQLSTATE
    # 42000, which upstream maps to ProgrammingError (not DataError).
    with pytest.raises(
        ProgrammingError, match="Error converting data type varchar to int"
    ):
        cursor.execute("EXEC #echo_int ?", ["not-a-number"])


# ---------------------------------------------------------------------------
# Result sets
# ---------------------------------------------------------------------------


def test_result_column_types_round_trip(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #types AS BEGIN SET NOCOUNT ON; "
        "SELECT CAST(7 AS INT) AS i, N'ü' AS s, CAST(1.25 AS DECIMAL(5,2)) AS d, "
        "CAST('2024-01-02T03:04:05' AS DATETIME2) AS t, CAST(NULL AS INT) AS n; END",
    )
    cursor.execute("EXEC #types")
    assert _rows(cursor) == [
        (7, "ü", Decimal("1.25"), datetime.datetime(2024, 1, 2, 3, 4, 5), None)
    ]


def test_proc_with_empty_result_set_returns_no_rows_but_columns(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #empty_rs AS BEGIN SET NOCOUNT ON; SELECT 1 AS a WHERE 1 = 0 END",
    )
    cursor.execute("EXEC #empty_rs")
    assert _rows(cursor) == []
    assert [d[0] for d in cursor.description] == ["a"]


def test_multiple_result_sets_with_nextset(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #two_rs AS BEGIN SET NOCOUNT ON; "
        "SELECT 'a' AS first_col; SELECT 'b' AS second_col, 3 AS n; END",
    )
    cursor.execute("EXEC #two_rs")
    assert [d[0] for d in cursor.description] == ["first_col"]
    assert _rows(cursor) == [("a",)]

    assert cursor.nextset() is True
    assert [d[0] for d in cursor.description] == ["second_col", "n"]
    assert _rows(cursor) == [("b", 3)]

    assert cursor.nextset() is False


def test_dml_before_select_hides_first_result_until_nextset(conn, cursor):
    cursor.execute("CREATE TABLE #pr_log (id INT)")
    _create_proc(
        cursor,
        "CREATE PROCEDURE #dml_then_select AS BEGIN "
        "INSERT INTO #pr_log VALUES (1); SELECT 'x' AS k; END",
    )
    cursor.execute("EXEC #dml_then_select")
    # Without SET NOCOUNT ON the INSERT's row count is a message, not a result set.
    assert cursor.description is None
    with pytest.raises(ProgrammingError):
        _rows(cursor)

    assert cursor.nextset() is True
    assert [d[0] for d in cursor.description] == ["k"]
    assert _rows(cursor) == [("x",)]


def test_second_call_on_same_cursor_returns_fresh_results(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #echo @a INT AS BEGIN SET NOCOUNT ON; SELECT @a AS v END",
    )
    cursor.execute("EXEC #echo ?", [1])
    assert _rows(cursor) == [(1,)]
    cursor.execute("EXEC #echo ?", [2])
    assert _rows(cursor) == [(2,)]


# ---------------------------------------------------------------------------
# SET NOCOUNT and rowcount
# ---------------------------------------------------------------------------


def test_rowcount_is_minus_one_under_set_nocount_on(conn, cursor):
    cursor.execute("CREATE TABLE #w (id INT, v INT)")
    cursor.execute("INSERT INTO #w VALUES (1, 0), (2, 0), (3, 0)")
    _create_proc(
        cursor,
        "CREATE PROCEDURE #upd_quiet AS BEGIN SET NOCOUNT ON; UPDATE #w SET v = 1 END",
    )
    cursor.execute("EXEC #upd_quiet")
    assert cursor.rowcount == -1


def test_rowcount_reports_affected_rows_without_set_nocount(conn, cursor):
    cursor.execute("CREATE TABLE #w2 (id INT, v INT)")
    cursor.execute("INSERT INTO #w2 VALUES (1, 0), (2, 0), (3, 0)")
    _create_proc(
        cursor,
        "CREATE PROCEDURE #upd_loud AS BEGIN UPDATE #w2 SET v = 2 END",
    )
    cursor.execute("EXEC #upd_loud")
    assert cursor.rowcount == 3


# ---------------------------------------------------------------------------
# Errors and transactions
# ---------------------------------------------------------------------------


def test_raiserror_inside_proc_raises_programming_error(conn, cursor):
    _create_proc(
        cursor,
        "CREATE PROCEDURE #boom AS BEGIN RAISERROR('boom from proc', 16, 1) END",
    )
    with pytest.raises(ProgrammingError, match="boom from proc"):
        cursor.execute("EXEC #boom")


def test_proc_writes_to_permanent_table_and_rollback_undoes_them(
    conn, cursor, table_name
):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL)")
    conn.commit()
    _create_proc(
        cursor,
        f"CREATE PROCEDURE #ins_row @id INT AS BEGIN SET NOCOUNT ON; "
        f"INSERT INTO dbo.{table_name} (id) VALUES (@id) END",
    )
    conn.commit()  # temp procedure creation must not be rolled back with the test's work
    cursor.execute("EXEC #ins_row ?", [10])
    conn.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert _row(cursor) == (0,)

    cursor.execute("EXEC #ins_row ?", [11])
    conn.commit()
    cursor.execute(f"SELECT id FROM dbo.{table_name}")
    assert _rows(cursor) == [(11,)]


def test_proc_reads_caller_session_temp_table(conn, cursor):
    cursor.execute("CREATE TABLE #caller_rows (k INT)")
    cursor.execute("INSERT INTO #caller_rows VALUES (4), (9)")
    _create_proc(
        cursor,
        "CREATE PROCEDURE #sum_caller AS BEGIN SET NOCOUNT ON; "
        "SELECT SUM(k) AS total FROM #caller_rows END",
    )
    cursor.execute("EXEC #sum_caller")
    assert _rows(cursor) == [(13,)]
