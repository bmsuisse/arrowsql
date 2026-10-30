"""Decimal parameter type inference: scale, exponent, precision and batch rows.

Research sources (issue trackers and docs consulted for real failure modes):
- https://github.com/mkleehammer/pyodbc/issues/1507
  fast_executemany rounds Decimal parameters to the thread's decimal context
  precision (28 digits by default) before binding.
- https://github.com/mkleehammer/pyodbc/issues/996
  TVP/bulk binding takes a Decimal column's precision from the first row; a
  later, wider value then fails with "Numeric value out of range".
- https://github.com/mkleehammer/pyodbc/issues/786
  executemany mixing floats and Decimals into a numeric column raises a
  conversion error.
- https://github.com/mkleehammer/pyodbc/issues/1232 and
  https://github.com/mkleehammer/pyodbc/issues/845
  "String data, right truncation" when a Decimal(m,n) is sent with sign and
  point counted into the buffer length.
- https://github.com/microsoft/mssql-python/pull/752
  "bind executemany money-range Decimals as SQL_NUMERIC": batch-wide
  precision/scale is derived from Decimal rows only, which can under-size
  NUMERIC metadata for mixed-type batches.
- https://github.com/microsoft/mssql-python/pull/287
  Raised the Decimal precision limit from 15 to 38 and moved binding to
  SQL_NUMERIC_STRUCT (16 little-endian bytes) instead of a uint64.
- https://github.com/sqlalchemy/sqlalchemy/issues/13585
  mssql-python 1.15 binds every Decimal as SQL_NUMERIC; values with large
  exponents then overflow in a dialect that stringifies them.
- https://github.com/Synergex/tiberius/pull/9
  Precision of values below one counted a phantom integer digit, so a
  Decimal(scale 9) reported precision 10 and failed round-trip checks.
- https://github.com/tiberius-rs/tiberius/issues/31
  Numeric parameters were read-only for a long time; writing them was broken.
- https://github.com/tomanizer/connector-x/issues/125
  DECIMAL values with more than 28 significant digits fail when routed
  through rust_decimal, the Rust-side representation a port might use.
- Microsoft Learn, decimal and numeric (T-SQL): precision 1..38, scale
  0..precision, implicit rounding on conversion (not fetched; cited from docs).

Expected values are what the upstream ODBC driver returns today, except where
a test is marked upstream_bug: then it asserts the correct value and is an
xfail(strict=True) so the suite flags the gap until upstream is fixed.
"""

from decimal import Decimal

import pytest
from mssql_python.exceptions import DataError

# 38 significant digits with scale 6: 32 integer digits, 6 fractional digits.
BIG_38_6 = Decimal("1" * 32 + ".123456")


# --- single-row binding: exponent and scale --------------------------------


def test_positive_exponent_decimal_into_scale2_column(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (Decimal("1E+2"),))
    (value,) = cursor.fetchone()
    assert value == Decimal("100.00")
    assert str(value) == "100.00"
    assert cursor.description[0][5] == 2


def test_exponent_1e5_into_wide_column_is_exact_integer(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(38,0))", (Decimal("1E+5"),))
    (value,) = cursor.fetchone()
    assert isinstance(value, Decimal)
    assert str(value) == "100000"
    assert cursor.description[0][5] == 0


def test_exponent_1e5_overflowing_narrow_column_is_dataerror(cursor):
    # DECIMAL(5,2) tops out at 999.99; 100000 must not wrap or truncate.
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST(? AS DECIMAL(5,2))", (Decimal("1E+5"),))


@pytest.mark.parametrize("value", [Decimal("1E-3"), Decimal("-1E-3")])
def test_negative_exponent_rounds_to_zero_at_scale2(cursor, value):
    # Below half of 0.01: rounds to zero. SQL NUMERIC has no negative zero.
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (value,))
    (result,) = cursor.fetchone()
    assert str(result) == "0.00"


def test_single_execute_38_digit_value_scale6_exact(cursor):
    # Reference point: the same value that breaks in executemany below
    # must round-trip exactly through plain execute.
    cursor.execute("CREATE TABLE #single (d DECIMAL(38,6))")
    cursor.execute("INSERT INTO #single VALUES (?)", (BIG_38_6,))
    cursor.execute("SELECT d FROM #single")
    (value,) = cursor.fetchone()
    assert str(value) == "11111111111111111111111111111111.123456"
    assert cursor.description[0][4] == 38
    assert cursor.description[0][5] == 6


def test_single_float_half_up_into_scale2_decimal(cursor):
    # 0.125 is exact in binary; half rounds away from zero to 0.13.
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (0.125,))
    (value,) = cursor.fetchone()
    assert isinstance(value, Decimal)
    assert str(value) == "0.13"


# --- precision above 38 ----------------------------------------------------


def test_precision_over_38_in_batch_rejected_and_cursor_usable(cursor):
    # A 39-digit row must raise before any value is sent; the cursor stays usable.
    cursor.execute("CREATE TABLE #over (d DECIMAL(38,0))")
    with pytest.raises(ValueError, match="38"):
        cursor.executemany(
            "INSERT INTO #over VALUES (?)",
            [(Decimal("1"),), (Decimal("1" * 39),)],
        )
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (Decimal("3.14"),))
    (value,) = cursor.fetchone()
    assert value == Decimal("3.14")


# --- executemany: rounding and scale per row -------------------------------


def test_executemany_rounds_each_row_to_column_scale_half_away(cursor):
    cursor.execute("CREATE TABLE #round (d DECIMAL(10,2))")
    cursor.executemany(
        "INSERT INTO #round VALUES (?)",
        [(Decimal("1.5"),), (Decimal("2.555"),), (Decimal("3"),)],
    )
    cursor.execute("SELECT d FROM #round ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["1.50", "2.56", "3.00"]


def test_executemany_negative_half_away_rounding(cursor):
    cursor.execute("CREATE TABLE #neg (d DECIMAL(10,2))")
    cursor.executemany(
        "INSERT INTO #neg VALUES (?)",
        [(Decimal("-1"),), (Decimal("-0.125"),)],
    )
    cursor.execute("SELECT d FROM #neg ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["-1.00", "-0.13"]


def test_executemany_scale_not_locked_by_first_row(cursor):
    # Classic pyodbc pitfall: a first row with scale 0 must not truncate a
    # later row with fractional digits.
    cursor.execute("CREATE TABLE #scale (d DECIMAL(10,2))")
    cursor.executemany(
        "INSERT INTO #scale VALUES (?)",
        [(Decimal("10"),), (Decimal("0.5"),)],
    )
    cursor.execute("SELECT d FROM #scale ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["10.00", "0.50"]


def test_executemany_exponent_row_mixed_with_plain_rows(cursor):
    cursor.execute("CREATE TABLE #exp (d DECIMAL(38,0))")
    cursor.executemany(
        "INSERT INTO #exp VALUES (?)",
        [(Decimal("1"),), (Decimal("1E+5"),)],
    )
    cursor.execute("SELECT d FROM #exp ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["1", "100000"]


def test_executemany_row_overflowing_column_is_dataerror(cursor):
    cursor.execute("CREATE TABLE #ovf (d DECIMAL(10,2))")
    with pytest.raises(DataError):
        cursor.executemany(
            "INSERT INTO #ovf VALUES (?)",
            [(Decimal("1.00"),), (Decimal("100000000.00"),)],
        )
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (Decimal("2.00"),))
    (value,) = cursor.fetchone()
    assert value == Decimal("2.00")


def test_executemany_float_only_batch_into_numeric_column(cursor):
    cursor.execute("CREATE TABLE #flt (d DECIMAL(10,2))")
    cursor.executemany("INSERT INTO #flt VALUES (?)", [(2.5,), (3.25,)])
    cursor.execute("SELECT d FROM #flt ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["2.50", "3.25"]


@pytest.mark.upstream_bug(
    reason="a plain int row in the batch locks scale to 0; Decimal 1.5 becomes 1.00"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_decimal_first_then_int_row(cursor):
    # A later plain int must not truncate the fractional part of an earlier Decimal.
    cursor.execute("CREATE TABLE #di (d DECIMAL(10,2))")
    cursor.executemany("INSERT INTO #di VALUES (?)", [(Decimal("1.5"),), (2,)])
    cursor.execute("SELECT d FROM #di ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["1.50", "2.00"]


# --- executemany: upstream gaps (assert correct behavior, xfail strict) ----


@pytest.mark.upstream_bug(
    reason="executemany rejects a fitting 38-digit Decimal that execute accepts"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_38_digit_decimal_exact_roundtrip(cursor):
    cursor.execute("CREATE TABLE #big38 (d DECIMAL(38,6))")
    cursor.executemany("INSERT INTO #big38 VALUES (?)", [(BIG_38_6,)])
    cursor.execute("SELECT d FROM #big38")
    (value,) = cursor.fetchone()
    assert str(value) == "11111111111111111111111111111111.123456"


@pytest.mark.upstream_bug(
    reason="batch with a small first row then a 38-digit row fails on precision"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_small_then_big_precision_row(cursor):
    cursor.execute("CREATE TABLE #sb (d DECIMAL(38,6))")
    cursor.executemany(
        "INSERT INTO #sb VALUES (?)",
        [(Decimal("1.0"),), (BIG_38_6,)],
    )
    cursor.execute("SELECT d FROM #sb ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == [
        "1.000000",
        "11111111111111111111111111111111.123456",
    ]


@pytest.mark.upstream_bug(
    reason="int first row fixes param type; later Decimal 1.5 silently becomes 1.00"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_int_first_row_does_not_truncate_later_decimal(cursor):
    # Silent data loss: the fractional part of a later row must survive.
    cursor.execute("CREATE TABLE #intfirst (d DECIMAL(10,2))")
    cursor.executemany(
        "INSERT INTO #intfirst VALUES (?)",
        [(1,), (Decimal("1.5"),)],
    )
    cursor.execute("SELECT d FROM #intfirst ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["1.00", "1.50"]


@pytest.mark.upstream_bug(
    reason="executemany raises RuntimeError when a Decimal row follows a float row"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_float_first_then_decimal_mixed_batch(cursor):
    cursor.execute("CREATE TABLE #fd (d DECIMAL(10,2))")
    cursor.executemany(
        "INSERT INTO #fd VALUES (?)",
        [(2.5,), (Decimal("1.25"),)],
    )
    cursor.execute("SELECT d FROM #fd ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["2.50", "1.25"]


@pytest.mark.upstream_bug(
    reason="executemany raises RuntimeError when a float row follows a Decimal row"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_decimal_first_then_float_mixed_batch(cursor):
    cursor.execute("CREATE TABLE #df (d DECIMAL(10,2))")
    cursor.executemany(
        "INSERT INTO #df VALUES (?)",
        [(Decimal("1.25"),), (2.5,)],
    )
    cursor.execute("SELECT d FROM #df ORDER BY (SELECT 1)")
    assert [str(r[0]) for r in cursor.fetchall()] == ["1.25", "2.50"]
