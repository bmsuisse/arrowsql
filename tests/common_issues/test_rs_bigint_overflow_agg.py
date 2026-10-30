"""Integer aggregate overflow and int/bigint parameter range edge cases.

Sources (research, consulted while writing these tests):
- https://learn.microsoft.com/en-us/sql/t-sql/functions/sum-transact-sql
  SUM over an int expression returns int; overflow raises an arithmetic error.
- https://learn.microsoft.com/en-us/sql/t-sql/functions/count-big-transact-sql
  COUNT returns int (overflows past 2^31-1); COUNT_BIG returns bigint.
- https://learn.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql
  AVG over int returns int (fraction truncated).
- https://blog.sqlauthority.com/2017/04/24/sql-server-fix-msg-8115-level-16-arithmetic-overflow-error-converting-identity-data-type-int/
  Msg 8115 "Arithmetic overflow error converting expression to data type int".
- https://database.guide/fix-arithmetic-overflow-error-converting-expression-to-data-type-int-in-sql-server/
  Fix pattern SUM(CAST(col AS bigint)).
- https://github.com/microsoft/mssql-django/issues/310
  Server 8115 surfaces through the driver as SQLSTATE 22003 (DataError class).
- https://github.com/mkleehammer/pyodbc/issues/841
  "OverflowError: int too big to convert" when a Python int exceeds the bound type.
- https://github.com/mkleehammer/pyodbc/issues/312
  Integer parameter type detection for BIGINT columns (numpy.int64 rejected).
- https://github.com/microsoft/mssql-python/pull/611 and
  https://github.com/sqlalchemy/sqlalchemy/issues/13585
  Parameter C-type detection vs. converted value mismatch in mssql-python.

Behaviors a native-TDS reimplementation can easily get wrong:
- Python ints must bind as BIGINT when they fit in 64 bits, not INT4; a
  parameter of 2**31 must not overflow in the WHERE clause or SELECT ?.
- Ints outside [-2^63, 2^63-1] must fail client-side (ValueError), not be
  sent and wrapped.
- Server-side overflow must surface as DataError, and the connection must
  stay usable afterwards.
- AVG over int is integer division, not a float.
"""

import pytest
from mssql_python import DataError

INT_MAX = 2**31 - 1
INT_MIN = -(2**31)
BIGINT_MAX = 2**63 - 1
BIGINT_MIN = -(2**63)


def _rows(cursor):
    return [tuple(row) for row in cursor.fetchall()]


def _scalar(cursor):
    return tuple(cursor.fetchone())[0]


# --- SUM over int / bigint -------------------------------------------------


def test_sum_int_overflow_raises_dataerror_and_connection_survives(cursor):
    cursor.execute("CREATE TABLE #sum_int (v int NOT NULL)")
    cursor.execute("INSERT INTO #sum_int (v) VALUES (?), (?)", [INT_MAX, 1])
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("SELECT SUM(v) FROM #sum_int")
    # A statement-level arithmetic error must not poison the connection.
    cursor.execute("SELECT 42")
    assert _scalar(cursor) == 42


def test_sum_int_negative_intermediate_overflow_is_dataerror(cursor):
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("SELECT SUM(x) FROM (VALUES (-2147483648), (-1)) AS t(x)")


def test_sum_cast_to_bigint_returns_exact_value_as_python_int(cursor):
    cursor.execute("SELECT SUM(CAST(x AS bigint)) FROM (VALUES (2147483647), (1)) AS t(x)")
    value = _scalar(cursor)
    assert value == 2147483648
    assert type(value) is int


def test_sum_bigint_overflow_raises_dataerror(cursor):
    cursor.execute("CREATE TABLE #sum_big (v bigint NOT NULL)")
    cursor.execute("INSERT INTO #sum_big (v) VALUES (?), (?)", [BIGINT_MAX, BIGINT_MAX])
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("SELECT SUM(v) FROM #sum_big")


def test_sum_bigint_extremes_without_intermediate_overflow_is_exact(cursor):
    cursor.execute("CREATE TABLE #sum_ext (v bigint NOT NULL)")
    cursor.execute("INSERT INTO #sum_ext (v) VALUES (?), (?)", [BIGINT_MIN, BIGINT_MAX])
    cursor.execute("SELECT SUM(v) FROM #sum_ext")
    value = _scalar(cursor)
    assert value == -1
    assert type(value) is int


def test_sum_int_result_type_is_python_int(cursor):
    cursor.execute("CREATE TABLE #sum_small (v int NOT NULL)")
    cursor.execute("INSERT INTO #sum_small (v) VALUES (?), (?), (?)", [1, 2, 3])
    cursor.execute("SELECT SUM(v) FROM #sum_small")
    value = _scalar(cursor)
    assert value == 6
    assert type(value) is int


def test_sum_all_null_and_empty_set_return_none(cursor):
    cursor.execute("CREATE TABLE #sum_null (v int NULL)")
    cursor.execute("INSERT INTO #sum_null (v) VALUES (NULL), (NULL)")
    cursor.execute("SELECT SUM(v) FROM #sum_null")
    assert _scalar(cursor) is None
    cursor.execute("SELECT SUM(v) FROM #sum_null WHERE 1 = 0")
    assert _scalar(cursor) is None


# --- COUNT vs COUNT_BIG ----------------------------------------------------


def test_count_and_count_big_return_same_small_value_as_python_int(cursor):
    cursor.execute("CREATE TABLE #cnt (v int NULL)")
    cursor.execute("INSERT INTO #cnt (v) VALUES (1), (NULL), (3)")
    cursor.execute("SELECT COUNT(*), COUNT_BIG(*), COUNT(v), COUNT_BIG(v) FROM #cnt")
    row = tuple(cursor.fetchone())
    assert row == (3, 3, 2, 2)
    assert all(type(v) is int for v in row)


def test_count_big_on_empty_table_is_zero(cursor):
    cursor.execute("CREATE TABLE #cnt_empty (v int NULL)")
    cursor.execute("SELECT COUNT_BIG(*) FROM #cnt_empty")
    value = _scalar(cursor)
    assert value == 0
    assert type(value) is int


# --- AVG over int ---------------------------------------------------------


def test_avg_over_int_truncates_to_integer(cursor):
    # AVG(int) is integer arithmetic on the server: (1 + 2) / 2 == 1, not 1.5.
    cursor.execute("CREATE TABLE #avg_int (v int NOT NULL)")
    cursor.execute("INSERT INTO #avg_int (v) VALUES (1), (2)")
    cursor.execute("SELECT AVG(v) FROM #avg_int")
    value = _scalar(cursor)
    assert value == 1
    assert type(value) is int


# --- Python int parameters: bigint range ----------------------------------


@pytest.mark.parametrize("value", [INT_MAX, INT_MAX + 1, BIGINT_MAX, BIGINT_MIN])
def test_python_int_param_roundtrips_exactly_through_select(cursor, value):
    # 2**31 and beyond must bind as BIGINT, not overflow an INT4 parameter.
    cursor.execute("SELECT ?", [value])
    got = _scalar(cursor)
    assert got == value
    assert type(got) is int


def test_int_param_above_int_range_in_where_clause_is_not_an_error(cursor):
    cursor.execute("CREATE TABLE #where_int (v int NOT NULL)")
    cursor.execute("INSERT INTO #where_int (v) VALUES (?)", [INT_MAX])
    cursor.execute("SELECT COUNT_BIG(*) FROM #where_int WHERE v = ?", [INT_MAX + 1])
    assert _scalar(cursor) == 0
    cursor.execute("SELECT COUNT_BIG(*) FROM #where_int WHERE v = ?", [INT_MAX])
    assert _scalar(cursor) == 1


@pytest.mark.parametrize("value", [BIGINT_MAX + 1, BIGINT_MIN - 1])
def test_python_int_outside_bigint_range_raises_valueerror_client_side(cursor, value):
    # Out of SQL BIGINT range is rejected before anything is sent to the server.
    with pytest.raises(ValueError, match="BIGINT"):
        cursor.execute("SELECT ?", [value])
    cursor.execute("SELECT 1")
    assert _scalar(cursor) == 1


def test_int_param_2_pow_31_into_int_column_raises_dataerror_and_leaves_no_row(cursor):
    cursor.execute("CREATE TABLE #int_col (v int NOT NULL)")
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("INSERT INTO #int_col (v) VALUES (?)", [INT_MAX + 1])
    cursor.execute("SELECT COUNT_BIG(*) FROM #int_col")
    assert _scalar(cursor) == 0


# --- Python int parameters into narrow columns ----------------------------


@pytest.mark.parametrize("value", [256, -1])
def test_tinyint_param_out_of_range_raises_dataerror(cursor, value):
    cursor.execute("CREATE TABLE #tiny (v tinyint NOT NULL)")
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("INSERT INTO #tiny (v) VALUES (?)", [value])


def test_tinyint_param_boundaries_are_accepted(cursor):
    cursor.execute("CREATE TABLE #tiny_ok (v tinyint NOT NULL)")
    cursor.execute("INSERT INTO #tiny_ok (v) VALUES (?), (?)", [0, 255])
    cursor.execute("SELECT v FROM #tiny_ok ORDER BY v")
    assert _rows(cursor) == [(0,), (255,)]


@pytest.mark.parametrize("value", [32768, -32769])
def test_smallint_param_out_of_range_raises_dataerror(cursor, value):
    cursor.execute("CREATE TABLE #small (v smallint NOT NULL)")
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("INSERT INTO #small (v) VALUES (?)", [value])


def test_smallint_param_boundaries_are_accepted(cursor):
    cursor.execute("CREATE TABLE #small_ok (v smallint NOT NULL)")
    cursor.execute("INSERT INTO #small_ok (v) VALUES (?), (?)", [32767, -32768])
    cursor.execute("SELECT v FROM #small_ok ORDER BY v")
    assert _rows(cursor) == [(-32768,), (32767,)]


# --- Transactions around aggregate errors ---------------------------------


def test_failed_sum_inside_transaction_keeps_transaction_usable(conn, cursor):
    cursor.execute("CREATE TABLE #tx_sum (v int NOT NULL)")
    cursor.execute("INSERT INTO #tx_sum (v) VALUES (?), (?)", [INT_MAX, 1])
    with pytest.raises(DataError, match="(?i)overflow"):
        cursor.execute("SELECT SUM(v) FROM #tx_sum")
    # The statement error is not a transaction abort: the table stays
    # readable and a later write in the same transaction commits.
    cursor.execute("INSERT INTO #tx_sum (v) VALUES (?)", [5])
    conn.commit()
    cursor.execute("SELECT SUM(CAST(v AS bigint)) FROM #tx_sum")
    assert _scalar(cursor) == INT_MAX + 1 + 5
