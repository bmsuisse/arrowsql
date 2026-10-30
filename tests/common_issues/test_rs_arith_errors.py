"""Arithmetic and conversion errors: 8134 divide by zero, 8115/220 overflow,
245/8114 conversion failures, ARITHABORT / ANSI_WARNINGS OFF NULL behavior,
TRY_CAST / TRY_CONVERT, and DB-API exception classes (DataError expected).

Research sources (issue trackers and docs, consulted for real-world failure modes):
- https://github.com/sqlalchemy/sqlalchemy/issues/13585 (mssql-python 1.15 Decimal
  binding: values that were sent as strings now go as SQL_NUMERIC and overflow with
  "Arithmetic overflow error converting varchar to data type numeric")
- https://github.com/microsoft/mssql-django/issues/310 (unexpected arithmetic overflow
  on bulk_create when some decimal rows are NULL and others are not)
- https://github.com/mkleehammer/pyodbc/issues/935 (Msg 8134 "Divide by zero" lost or
  repeated across SQLMoreResults in multi-statement batches)
- https://github.com/celery/celery/issues/8634 (integer id arithmetic overflow on the
  MSSQL backend when the column is narrower than the Python value)
- https://learn.microsoft.com/en-us/sql/t-sql/statements/set-arithabort-transact-sql
  (ARITHABORT / ANSI_WARNINGS interaction; both OFF yields NULL for divide by zero)
- https://learn.microsoft.com/en-us/sql/t-sql/functions/try-cast-transact-sql and
  https://learn.microsoft.com/en-us/sql/t-sql/functions/try-convert-transact-sql
  (TRY_* returns NULL for conversion and overflow, not for divide by zero)
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql
  (integer ranges; overflow in SUM needs a bigint cast)
- https://sql-server-helper.com/error-messages/msg-8115-numeric-to-varchar.aspx
  (8115 state codes and numeric precision overflow)
- https://www.sqlservercentral.com/forums/topic/msg-8134-divide-by-zero-error-encountered
  (NULLIF divisor guard as the usual fix)
- https://www.python.org/dev/peps/pep-0249/ (DataError: problems with the processed
  data such as division by zero or numeric value out of range)

Upstream behavior that is an open question: Msg 8114 (varchar to float) is raised as
ProgrammingError (SQLSTATE 42000), while Msg 245 (varchar to int) is raised as DataError.
Both are data conversion failures, so the test below asserts DataError and is marked
upstream_bug.
"""

from decimal import Decimal

import pytest
from mssql_python.exceptions import DataError


# --- divide by zero (8134) -------------------------------------------------

def test_divide_by_zero_raises_data_error(cursor):
    with pytest.raises(DataError, match="[Dd]ivide by zero"):
        cursor.execute("SELECT 1/0")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1.0/0",
        "SELECT 1/0.0",
        "SELECT CAST(0 AS FLOAT)/0",
        "SELECT ISNULL(NULL, 3)/0",
    ],
)
def test_divide_by_zero_for_decimal_float_and_expression_operands(cursor, sql):
    with pytest.raises(DataError):
        cursor.execute(sql)


def test_divide_by_null_divisor_returns_null_not_error(cursor):
    cursor.execute("SELECT 1/NULL AS x")
    assert cursor.fetchone()[0] is None


def test_nullif_guard_returns_null_without_error(cursor):
    cursor.execute("SELECT CAST(10 AS INT) / NULLIF(CAST(0 AS INT), 0) AS x")
    assert cursor.fetchone()[0] is None


def test_divide_by_zero_in_one_row_of_many_raises(cursor):
    # Earlier rows compute fine; the batch still fails on the zero divisor.
    with pytest.raises(DataError):
        cursor.execute("SELECT 10 / v AS q FROM (VALUES (1), (2), (0)) AS t(v)")
        cursor.fetchall()


def test_arithabort_and_ansi_warnings_off_returns_null(conn):
    cur = conn.cursor()
    cur.execute("SET ARITHABORT OFF; SET ANSI_WARNINGS OFF")
    cur.execute("SELECT 1/0 AS x")
    assert cur.fetchone()[0] is None
    cur.close()


def test_arithabort_off_alone_still_raises_divide_by_zero(conn):
    # ANSI_WARNINGS stays ON, so the divide by zero is still reported.
    cur = conn.cursor()
    cur.execute("SET ARITHABORT OFF")
    with pytest.raises(DataError):
        cur.execute("SELECT 1/0")
    cur.close()


def test_try_cast_does_not_swallow_divide_by_zero(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT TRY_CAST(1/0 AS INT) AS c")


# --- overflow (8115 / 220) ---------------------------------------------------

def test_int_overflow_on_add_raises_data_error(cursor):
    with pytest.raises(DataError, match="[Aa]rithmetic overflow"):
        cursor.execute("SELECT CAST(2147483647 AS INT) + 1 AS x")


def test_overflow_returns_null_only_when_both_arithabort_and_ansi_off(conn):
    cur = conn.cursor()
    cur.execute("SET ARITHABORT OFF; SET ANSI_WARNINGS OFF")
    cur.execute("SELECT CAST(2147483647 AS INT) + 1 AS x")
    assert cur.fetchone()[0] is None
    cur.close()


def test_tinyint_and_decimal_out_of_range_raise_data_error(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST(300 AS TINYINT) AS x")
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST(9999.99 AS DECIMAL(5,2)) AS x")


def test_sum_int_overflow_raises_and_bigint_cast_fixes_it(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT SUM(v) FROM (VALUES (2147483647), (1)) AS t(v)")
    cursor.execute("SELECT SUM(CAST(v AS BIGINT)) FROM (VALUES (2147483647), (1)) AS t(v)")
    total = cursor.fetchone()[0]
    assert type(total) is int
    assert total == 2147483648


def test_parameter_overflow_int_cast_raises(cursor):
    # Python int 2**31-1 is sent as a value, then +1 is computed as INT.
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST(? AS INT) + 1", (2147483647,))


def test_decimal_parameter_out_of_range_raises(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST(? AS DECIMAL(5,2))", (Decimal("9999.99"),))


def test_parameter_divide_by_zero_raises(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT ? / ?", (1, 0))


# --- conversion (245 / 8114) -------------------------------------------------

@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CAST('abc' AS INT)",
        "SELECT CAST('1.5' AS INT)",
        "SELECT CAST('-45.1598' AS INT)",
        "SELECT CAST('2147483648' AS INT)",
    ],
)
def test_string_to_int_conversion_raises_data_error(cursor, sql):
    with pytest.raises(DataError):
        cursor.execute(sql)


@pytest.mark.upstream_bug(
    reason=(
        "Msg 8114 (varchar to float) is raised as ProgrammingError (SQLSTATE 42000); "
        "it is a data conversion failure like Msg 245, which is raised as DataError"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_string_to_float_conversion_raises_data_error(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST('abc' AS FLOAT) AS x")


def test_conversion_failure_does_not_mask_column_position(cursor):
    # Earlier valid rows do not prevent the failing row from raising.
    with pytest.raises(DataError):
        cursor.execute(
            "SELECT CAST(v AS INT) FROM (VALUES ('1'), ('x'), ('3')) AS t(v)"
        )
        cursor.fetchall()


# --- TRY_CAST / TRY_CONVERT --------------------------------------------------

def test_try_cast_and_try_convert_return_null_on_failure(cursor):
    cursor.execute(
        "SELECT TRY_CAST('abc' AS INT) AS a, "
        "TRY_CONVERT(INT, '12x') AS b, "
        "TRY_CAST('2147483648' AS INT) AS c, "
        "TRY_CONVERT(DATE, 'nope') AS d"
    )
    assert tuple(cursor.fetchone()) == (None, None, None, None)


def test_try_convert_success_returns_typed_value(cursor):
    cursor.execute("SELECT TRY_CONVERT(INT, '42') AS a, TRY_CAST('2.50' AS DECIMAL(5,2)) AS b")
    a, b = cursor.fetchone()
    assert type(a) is int and a == 42
    assert type(b) is Decimal and b == Decimal("2.50")


# --- session state after errors ----------------------------------------------

def test_connection_usable_after_arithmetic_error(conn):
    cur = conn.cursor()
    with pytest.raises(DataError):
        cur.execute("SELECT 1/0")
    cur.execute("SELECT 7 AS after")
    row = cur.fetchone()
    assert type(row[0]) is int and row[0] == 7
    cur.close()


def test_arithmetic_error_rolls_back_only_failing_statement(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE #arith (id INT PRIMARY KEY, v INT)")
    cur.execute("INSERT INTO #arith VALUES (1, 10)")
    with pytest.raises(DataError):
        cur.execute("UPDATE #arith SET v = 1/0 WHERE id = 1")
    conn.commit()
    cur.execute("SELECT id, v FROM #arith")
    assert [tuple(r) for r in cur.fetchall()] == [(1, 10)]
    cur.close()
