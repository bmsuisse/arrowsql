"""MONEY / SMALLMONEY deep dive: arithmetic, aggregates, and inputs.

Research sources (issue trackers and docs, consulted for real-world failure modes):
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/decimal-money
  (money -> decimal.Decimal, 19 digits / 4 places; smallmoney 10 digits / 4 places;
  build Decimals from strings, not from float literals)
- https://github.com/microsoft/mssql-python/issues/745
  (money-range Decimal still string-binds on the executemany path)
- https://github.com/microsoft/mssql-python/pull/752 (executemany money-range
  Decimals bound as SQL_NUMERIC; precision/scale must fit every row in a batch)
- https://github.com/sqlalchemy/sqlalchemy/issues/13585 (mssql-python 1.15 changed
  how Decimal is bound; downstream dialects broke)
- https://github.com/mkleehammer/pyodbc/issues/463 (CAST(0.1234 AS money) came back
  as Decimal('0.12') through FreeTDS: the driver, not the Python layer, truncated)
- https://github.com/mkleehammer/pyodbc/issues/786 (Decimal into float column under
  fast_executemany raised "Converting decimal loses precision")
- https://github.com/e-omunoz/celer/issues/68 (money/smallmoney decoded as f64 loses
  cents above ~9e11 and drops the 4-place scale)
- https://github.com/tiberius-rs/tiberius/issues/358 (money bulk insert rejected F64
  values; money is a distinct wire type from decimal)
- https://github.com/sfu-db/connector-x/issues/678 (money read as F64 instead of
  Decimal in tiberius 0.5.x)
- https://sqlenlight.com/support/help/sa0247b/ (MONEY/SMALLMONEY are integers scaled
  by 10^4 on the server; arithmetic inherits that truncation)

Overlap with test_decimal_numeric.py (rounding, bounds, overflow on insert) is
deliberately avoided here. This file targets aggregates, integer-scaled division,
currency-string casts, float/Decimal inputs into money, and metadata.
"""

from decimal import Decimal

import pytest
from mssql_python.exceptions import DatabaseError, DataError


def test_money_fetch_has_four_places_and_decimal_type(cursor):
    cursor.execute("SELECT CAST(1 AS MONEY), CAST(0.5 AS SMALLMONEY)")
    whole, half = cursor.fetchone()
    assert type(whole) is Decimal
    assert type(half) is Decimal
    assert str(whole) == "1.0000"
    assert str(half) == "0.5000"


def test_money_and_smallmoney_column_metadata(cursor):
    cursor.execute("SELECT CAST(1 AS MONEY) AS m, CAST(1 AS SMALLMONEY) AS s")
    money_col, small_col = cursor.description[0], cursor.description[1]
    # description tuple: (name, type_code, display_size, internal_size, precision, scale, null_ok)
    assert money_col[0] == "m"
    assert money_col[4] == 19 and money_col[5] == 4
    assert small_col[0] == "s"
    assert small_col[4] == 10 and small_col[5] == 4


def test_money_from_currency_string_cast(cursor):
    # SQL Server's varchar->money conversion accepts currency symbols and thousands separators.
    cursor.execute("SELECT CAST(? AS MONEY), CAST(? AS MONEY)", ("$1,234.56", "$0.1"))
    big, small = cursor.fetchone()
    assert big == Decimal("1234.56")
    assert str(big) == "1234.5600"
    assert str(small) == "0.1000"


def test_money_to_varchar_currency_styles(cursor):
    # Documented money->varchar styles: 0 = 2 places, no commas; 1 = 2 places with commas;
    # 2 = 4 places, no commas.
    cursor.execute(
        "SELECT CONVERT(VARCHAR(30), CAST(1234.5 AS MONEY), 0), "
        "       CONVERT(VARCHAR(30), CAST(1234.5 AS MONEY), 1), "
        "       CONVERT(VARCHAR(30), CAST(1234.5 AS MONEY), 2)"
    )
    plain, grouped, four_places = cursor.fetchone()
    assert plain == "1234.50"
    assert grouped == "1,234.50"
    assert four_places == "1234.5000"


def test_sum_money_is_exact_and_skips_nulls(cursor):
    cursor.execute("CREATE TABLE #m_sum (amt MONEY)")
    cursor.execute("INSERT INTO #m_sum VALUES (?), (NULL), (?), (?)",
                   (Decimal("1.0001"), Decimal("2.0002"), Decimal("3.0000")))
    cursor.execute("SELECT SUM(amt), COUNT(amt), COUNT(*) FROM #m_sum")
    total, counted, rows = cursor.fetchone()
    assert type(total) is Decimal
    assert str(total) == "6.0003"
    assert counted == 3
    assert rows == 4


def test_sum_of_all_null_money_is_none(cursor):
    cursor.execute("CREATE TABLE #m_null (amt MONEY)")
    cursor.execute("INSERT INTO #m_null VALUES (NULL), (NULL)")
    cursor.execute("SELECT SUM(amt) FROM #m_null")
    assert cursor.fetchone()[0] is None


def test_sum_money_overflow_raises_while_decimal_cast_does_not(cursor):
    # SUM(money) is a money aggregate: 5e14 + 5e14 exceeds the 922337203685477.5807 ceiling.
    cursor.execute("CREATE TABLE #m_ovf (amt MONEY)")
    cursor.execute("INSERT INTO #m_ovf VALUES (?), (?)",
                   (Decimal("500000000000000"), Decimal("500000000000000")))
    with pytest.raises(DataError):
        cursor.execute("SELECT SUM(amt) FROM #m_ovf")
        cursor.fetchone()
    # Widening to DECIMAL(19,4) before summing keeps the exact total.
    cursor.execute("SELECT SUM(CAST(amt AS DECIMAL(19,4))) FROM #m_ovf")
    (total,) = cursor.fetchone()
    assert str(total) == "1000000000000000.0000"


def test_avg_money_truncates_to_four_places(cursor):
    # Money AVG is integer division on the x10000 scaled value: (1+2)/2 units = 0.00015 -> 0.0001.
    cursor.execute("CREATE TABLE #m_avg (amt MONEY)")
    cursor.execute("INSERT INTO #m_avg VALUES (?), (?)", (Decimal("0.0001"), Decimal("0.0002")))
    cursor.execute("SELECT AVG(amt) FROM #m_avg")
    (mean,) = cursor.fetchone()
    assert str(mean) == "0.0001"


def test_money_division_truncates_where_decimal_keeps_digits(cursor):
    cursor.execute(
        "SELECT CAST(2 AS MONEY) / 3, "
        "       CAST(2 AS DECIMAL(19,4)) / 3"
    )
    money_quot, decimal_quot = cursor.fetchone()
    # Money result is truncated to 4 places (not rounded to 0.6667).
    assert str(money_quot) == "0.6666"
    # The DECIMAL result keeps far more digits than the money one.
    assert len(str(decimal_quot).split(".")[1]) > 4
    assert str(decimal_quot).startswith("0.666666")


def test_money_times_integer_and_money_times_money(cursor):
    cursor.execute("SELECT CAST(1.2345 AS MONEY) * 2, CAST(1.5 AS MONEY) * CAST(1.5 AS MONEY)")
    doubled, squared = cursor.fetchone()
    assert str(doubled) == "2.4690"
    assert str(squared) == "2.2500"


def test_money_division_by_zero_raises(cursor):
    with pytest.raises(DatabaseError, match="(?i)divide by zero"):
        cursor.execute("SELECT CAST(1 AS MONEY) / CAST(0 AS MONEY)")
        cursor.fetchone()


def test_float_param_into_money_rounds_to_four_places(cursor):
    # Floats are not exact decimals; the server converts them to money at 4 places.
    cursor.execute("SELECT CAST(? AS MONEY), CAST(? AS MONEY), CAST(? AS MONEY)",
                   (0.1, 0.1 + 0.2, 12345.6789))
    tenth, sum_of_floats, exact = cursor.fetchone()
    assert str(tenth) == "0.1000"
    assert str(sum_of_floats) == "0.3000"
    assert str(exact) == "12345.6789"


def test_decimal_param_with_extra_digits_rounds_half_away_from_zero(cursor):
    cursor.execute("SELECT CAST(? AS MONEY), CAST(? AS MONEY)",
                   (Decimal("-1.23455"), Decimal("1.23455")))
    negative, positive = cursor.fetchone()
    assert str(negative) == "-1.2346"
    assert str(positive) == "1.2346"


def test_money_to_decimal_cast_rounds_half_up(cursor):
    cursor.execute("SELECT CAST(CAST(1.005 AS MONEY) AS DECIMAL(10,2))")
    (value,) = cursor.fetchone()
    assert value == Decimal("1.01")
    assert str(value) == "1.01"


def test_money_negative_zero_normalizes(cursor):
    cursor.execute("SELECT CAST(? AS MONEY)", (Decimal("-0.0000"),))
    (value,) = cursor.fetchone()
    assert value == Decimal("0")
    assert str(value) == "0.0000"


def test_smallmoney_plus_money_yields_exact_money(cursor):
    cursor.execute("SELECT CAST(1.5 AS SMALLMONEY) + CAST(2.25 AS MONEY)")
    (value,) = cursor.fetchone()
    assert type(value) is Decimal
    assert str(value) == "3.7500"


def test_money_null_arithmetic_propagates(cursor):
    cursor.execute("SELECT CAST(NULL AS MONEY) + 1, CAST(NULL AS MONEY) * CAST(2 AS MONEY)")
    plus, times = cursor.fetchone()
    assert plus is None
    assert times is None


def test_money_min_max_and_ordering_return_decimals(cursor):
    cursor.execute("CREATE TABLE #m_order (amt MONEY)")
    cursor.execute("INSERT INTO #m_order VALUES (?), (?), (?)",
                   (Decimal("-922337203685477.5808"), Decimal("0.0001"), Decimal("922337203685477.5807")))
    cursor.execute("SELECT MIN(amt), MAX(amt) FROM #m_order")
    low, high = cursor.fetchone()
    assert str(low) == "-922337203685477.5808"
    assert str(high) == "922337203685477.5807"
    cursor.execute("SELECT amt FROM #m_order ORDER BY amt")
    ordered = [row[0] for row in cursor.fetchall()]
    assert ordered == [Decimal("-922337203685477.5808"), Decimal("0.0001"), Decimal("922337203685477.5807")]


def test_money_equality_param_matches_stored_value(cursor):
    cursor.execute("CREATE TABLE #m_eq (amt MONEY)")
    cursor.execute("INSERT INTO #m_eq VALUES (?)", (Decimal("10.1"),))
    cursor.execute("SELECT COUNT(*) FROM #m_eq WHERE amt = ?", (Decimal("10.10"),))
    assert cursor.fetchone()[0] == 1


def test_money_executemany_roundtrip_preserves_scale(cursor):
    # Batch of money-range Decimals with mixed scales, through executemany (see mssql-python #745).
    rows = [
        (Decimal("0.0001"),),
        (Decimal("12.3"),),
        (Decimal("-7"),),
        (Decimal("922337203685477.5807"),),
        (Decimal("-922337203685477.5808"),),
    ]
    cursor.execute("CREATE TABLE #m_many (amt MONEY)")
    cursor.executemany("INSERT INTO #m_many (amt) VALUES (?)", rows)
    cursor.execute("SELECT amt FROM #m_many ORDER BY amt")
    fetched = [row[0] for row in cursor.fetchall()]
    assert [str(v) for v in fetched] == [
        "-922337203685477.5808",
        "-7.0000",
        "0.0001",
        "12.3000",
        "922337203685477.5807",
    ]


def test_smallmoney_column_insert_and_sum_range(cursor):
    cursor.execute("CREATE TABLE #sm_col (amt SMALLMONEY)")
    cursor.execute("INSERT INTO #sm_col VALUES (?), (?)",
                   (Decimal("214748.3647"), Decimal("-214748.3648")))
    cursor.execute("SELECT amt FROM #sm_col ORDER BY amt")
    low, high = [row[0] for row in cursor.fetchall()]
    assert str(low) == "-214748.3648"
    assert str(high) == "214748.3647"
