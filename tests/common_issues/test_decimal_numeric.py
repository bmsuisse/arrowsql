"""DECIMAL / NUMERIC / MONEY / SMALLMONEY behavior of the driver.

Covers precision 38 extremes, scale 0..38, negative values, trailing zeros,
Decimal parameters in exponent notation, money rounding and overflow, tiny
values, and rejection of non-finite or over-precise Decimals. Expected values
are what the upstream ODBC driver returns today; the Rust port must replicate
them.

Note: fetched rows are compared via tuple(row) because the driver's Row type
does not compare equal to a plain tuple.
"""

from decimal import Decimal

import pytest
from mssql_python.exceptions import DatabaseError, DataError

MAX_38 = "9" * 38


def _max_coefficient(scale: int, negative: bool = False) -> Decimal:
    """Largest 38-digit coefficient at the given scale.

    Built from a tuple rather than scaleb(), which rounds to the default
    28-digit context and would silently lose digits.
    """
    digits = tuple(int(d) for d in MAX_38)
    return Decimal((1 if negative else 0, digits, -scale))


# --- precision 38 ----------------------------------------------------------


def test_precision_38_extremes_roundtrip(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(38,0)), CAST(? AS DECIMAL(38,0))",
                   (Decimal(MAX_38), Decimal("-" + MAX_38)))
    positive, negative = cursor.fetchone()
    assert isinstance(positive, Decimal)
    assert str(positive) == MAX_38
    assert str(negative) == "-" + MAX_38


def test_precision_38_column_metadata_reported(cursor):
    cursor.execute("SELECT CAST(1 AS DECIMAL(38,10)) AS v")
    col = cursor.description[0]
    # (name, type_code, display_size, internal_size, precision, scale, null_ok)
    assert col[0] == "v"
    assert col[4] == 38
    assert col[5] == 10


def test_precision_38_mixed_fraction_roundtrip_exact(cursor):
    # 38 significant digits split 20/18 around the decimal point.
    literal = "12345678901234567890.123456789012345678"
    cursor.execute("SELECT CAST(? AS DECIMAL(38,18))", (Decimal(literal),))
    (value,) = cursor.fetchone()
    assert str(value) == literal


def test_precision_38_no_float_loss(cursor):
    # A float would lose everything past ~17 significant digits.
    literal = "99999999999999999999.99999999999999999"  # 37 digits, scale 17
    cursor.execute("SELECT CAST(? AS DECIMAL(38,17))", (Decimal(literal),))
    (value,) = cursor.fetchone()
    assert str(value) == literal


# --- scale 0..38 -----------------------------------------------------------


def test_every_scale_roundtrips_max_coefficient(cursor):
    """Max-coefficient value at every legal scale, positive and negative."""
    for scale in range(0, 39):
        for negative in (False, True):
            value = _max_coefficient(scale, negative)
            cursor.execute(f"SELECT CAST(? AS DECIMAL(38,{scale}))", (value,))
            (result,) = cursor.fetchone()
            assert result == value, f"scale {scale}: {result!r} != {value!r}"
            assert str(result) == str(value), f"scale {scale} text mismatch"
            assert cursor.description[0][5] == scale, f"scale {scale} metadata"


def test_scale_38_max_fraction(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(38,38))", (Decimal("0." + "9" * 38),))
    (value,) = cursor.fetchone()
    assert str(value) == "0." + "9" * 38


# --- tiny values -----------------------------------------------------------


def test_tiny_values_at_scale_38_are_exact_and_nonzero(cursor):
    literals = ["1E-38", "-1E-38", "0.00000000000000000000000000000000000001"]
    for literal in literals:
        cursor.execute("SELECT CAST(? AS DECIMAL(38,38)), "
                       "CASE WHEN CAST(? AS DECIMAL(38,38)) <> 0 THEN 1 ELSE 0 END",
                       (Decimal(literal), Decimal(literal)))
        value, nonzero = cursor.fetchone()
        assert value == Decimal(literal), literal
        assert nonzero == 1, f"{literal} collapsed to zero"


# --- negatives, trailing zeros, NULL, arithmetic ---------------------------


def test_negative_values_in_permanent_table(cursor, conn, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, v DECIMAL(10,3))")
    rows = [(1, Decimal("-0.001")), (2, Decimal("-123.450")), (3, Decimal("-9999999.999"))]
    cursor.executemany(f"INSERT INTO dbo.{table_name} (id, v) VALUES (?, ?)", rows)
    conn.commit()
    cursor.execute(f"SELECT id, v FROM dbo.{table_name} ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == rows


def test_trailing_zeros_follow_column_scale(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2)), CAST(? AS DECIMAL(10,4))",
                   (Decimal("1.50"), Decimal("1.50")))
    short, long_ = cursor.fetchone()
    assert str(short) == "1.50"
    assert str(long_) == "1.5000"
    assert short == long_


def test_null_decimal_roundtrip(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(38,10))", (None,))
    assert cursor.fetchone()[0] is None


def test_negative_zero_is_plain_zero(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (Decimal("-0"),))
    (value,) = cursor.fetchone()
    assert value == 0
    assert str(value) == "0.00"


def test_decimal_arithmetic_is_exact_no_float_drift(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(38,2)) + CAST(? AS DECIMAL(38,2))",
                   (Decimal("0.10"), Decimal("0.20")))
    (value,) = cursor.fetchone()
    assert str(value) == "0.30"


# --- exponent-notation Decimal parameters ----------------------------------


def test_exponent_notation_params(cursor):
    cursor.execute("SELECT ?, CAST(? AS DECIMAL(10,2)), CAST(? AS DECIMAL(10,2)), ?",
                   (Decimal("1E+3"), Decimal("1E+3"), Decimal("12345E-2"),
                    Decimal("1.0000000000000000000000000000000000000E+10")))
    bare_positive, scaled_positive, scaled_negative_exp, many_zeros = cursor.fetchone()
    # Bare exponent binds as an integer-valued decimal.
    assert str(bare_positive) == "1000"
    # Into scale 2 the value is padded to the column scale.
    assert str(scaled_positive) == "1000.00"
    assert str(scaled_negative_exp) == "123.45"
    # Returned scale is SQL Server's, not the input's, so compare by value.
    assert many_zeros == Decimal("10000000000")


# --- rounding into a column's scale ----------------------------------------


def test_decimal_rounds_half_away_from_zero_into_scale(cursor):
    cases = {
        "1.005": "1.01",
        "-1.005": "-1.01",
        "1.234": "1.23",
        "-1.234": "-1.23",
        "1.999": "2.00",
    }
    for literal, expected in cases.items():
        cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (Decimal(literal),))
        (value,) = cursor.fetchone()
        assert str(value) == expected, literal


def test_decimal_column_limit_accepted_and_overflow_raises_dataerror(cursor):
    cursor.execute("SELECT CAST(? AS DECIMAL(5,2))", (Decimal("999.99"),))
    assert cursor.fetchone()[0] == Decimal("999.99")
    with pytest.raises(DataError) as info:
        cursor.execute("SELECT CAST(? AS DECIMAL(5,2))", (Decimal("1000.00"),))
    assert isinstance(info.value, DatabaseError)


def test_decimal_equality_in_where_clause_ignores_trailing_zero(cursor, conn, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v DECIMAL(10,2))")
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", (Decimal("1.1"),))
    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE v = ?", (Decimal("1.10"),))
    assert cursor.fetchone()[0] == 1


# --- MONEY / SMALLMONEY ----------------------------------------------------


def test_money_rounds_to_four_places(cursor):
    cases = {
        "0.12345": "0.1235",    # 5th digit 5 rounds up, away from zero
        "-0.12345": "-0.1235",
        "1.00005": "1.0001",
        "1.00015": "1.0002",
        "12.3456789": "12.3457",
        "0.00001": "0.0000",    # below the smallest unit rounds to zero
        "-0.00004": "0.0000",
    }
    for literal, expected in cases.items():
        cursor.execute("SELECT CAST(? AS MONEY)", (Decimal(literal),))
        (value,) = cursor.fetchone()
        assert str(value) == expected, literal


def test_money_max_and_min_roundtrip(cursor):
    cursor.execute("SELECT CAST(? AS MONEY), CAST(? AS MONEY)",
                   (Decimal("922337203685477.5807"), Decimal("-922337203685477.5808")))
    maximum, minimum = cursor.fetchone()
    assert str(maximum) == "922337203685477.5807"
    assert str(minimum) == "-922337203685477.5808"


def test_money_overflow_after_rounding_raises_dataerror(cursor):
    # The 5th decimal rounds the value past the MONEY maximum.
    with pytest.raises(DataError):
        cursor.execute("SELECT CAST(? AS MONEY)", (Decimal("922337203685477.5808"),))


def test_money_column_roundtrip_with_null_in_permanent_table(cursor, conn, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, amount MONEY)")
    rows = [(1, Decimal("19.99")), (2, Decimal("-0.005")), (3, None)]
    cursor.executemany(f"INSERT INTO dbo.{table_name} (id, amount) VALUES (?, ?)", rows)
    conn.commit()
    cursor.execute(f"SELECT id, amount FROM dbo.{table_name} ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [
        (1, Decimal("19.9900")),
        (2, Decimal("-0.0050")),
        (3, None),
    ]


def test_smallmoney_bounds_roundtrip(cursor):
    cursor.execute("SELECT CAST(? AS SMALLMONEY), CAST(? AS SMALLMONEY)",
                   (Decimal("214748.3647"), Decimal("-214748.3648")))
    upper, lower = cursor.fetchone()
    assert str(upper) == "214748.3647"
    assert str(lower) == "-214748.3648"


def test_smallmoney_overflow_raises_dataerror(cursor):
    # Includes an over-range integer and a value whose rounding crosses the max.
    for literal in ("214749", "214748.36475", "-214748.36485"):
        with pytest.raises(DataError):
            cursor.execute("SELECT CAST(? AS SMALLMONEY)", (Decimal(literal),))


# --- rejected inputs -------------------------------------------------------


def test_precision_over_38_rejected_before_send(cursor):
    for literal in ("1" + "0" * 38, "1E+40", "1" * 39 + ".5"):
        with pytest.raises(ValueError, match="maximum precision supported by SQL Server is 38"):
            cursor.execute("SELECT ?", (Decimal(literal),))


def test_non_finite_decimal_rejected(cursor):
    for literal in ("NaN", "sNaN", "Infinity", "-Infinity"):
        with pytest.raises(ValueError, match="non-finite"):
            cursor.execute("SELECT ?", (Decimal(literal),))


def test_cursor_usable_after_rejected_decimal(cursor):
    with pytest.raises(ValueError):
        cursor.execute("SELECT ?", (Decimal("NaN"),))
    cursor.execute("SELECT CAST(? AS DECIMAL(10,2))", (Decimal("3.14"),))
    assert cursor.fetchone()[0] == Decimal("3.14")
