"""Single-statement binding of Decimal and datetime values keeps its inferred SQL type.

The Rust port converts plain `Decimal` and naive `datetime` parameters on a
fast path; the declared precision, scale and value must be those the general
path infers (and upstream sends).
"""

import datetime as dt
import decimal
from decimal import Decimal

import pytest

DECIMALS = [
    "0", "0.00", "-0.0", "1", "-1", "3.25", "0.25", "0.025", "100", "100.50",
    "123456789012345678901234567890.123456", "-99999999999999999999999999999999999999",
    "0.000000000000000000000000000000000001",
]


def _props(cursor, value):
    cursor.execute(
        "SELECT CAST(SQL_VARIANT_PROPERTY(?, 'BaseType') AS varchar(30)),"
        " CAST(SQL_VARIANT_PROPERTY(?, 'Precision') AS int),"
        " CAST(SQL_VARIANT_PROPERTY(?, 'Scale') AS int)",
        (value, value, value),
    )
    return tuple(cursor.fetchone())


@pytest.mark.parametrize("text", DECIMALS)
def test_decimal_declares_digits_and_scale_of_its_coefficient(cursor, text):
    value = Decimal(text)
    digits = max(len(value.as_tuple().digits), -value.as_tuple().exponent)
    scale = max(0, -value.as_tuple().exponent)
    base, precision, got_scale = _props(cursor, value)
    assert (base, precision, got_scale) == ("numeric", digits, scale)


@pytest.mark.parametrize("text", DECIMALS)
def test_decimal_round_trips(cursor, text):
    value = Decimal(text)
    cursor.execute("SELECT ?", (value,))
    assert cursor.fetchone()[0] == value


def test_exponent_notation_decimal_still_binds(cursor):
    cursor.execute("SELECT ?", (Decimal("1E+3"),))
    assert cursor.fetchone()[0] == Decimal("1000")


def test_decimal_subclass_binds(cursor):
    class Money(Decimal):
        pass

    cursor.execute("SELECT ?", (Money("12.50"),))
    assert cursor.fetchone()[0] == Decimal("12.50")


@pytest.mark.parametrize(
    "value",
    [
        dt.datetime(1, 1, 1, 0, 0, 0),
        dt.datetime(1969, 12, 31, 23, 59, 59, 999999),
        dt.datetime(1970, 1, 1),
        dt.datetime(2000, 2, 29, 12, 30, 45, 123456),
        dt.datetime(2024, 3, 1, 1, 2, 3, 4),
        dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
    ],
)
def test_naive_datetime_is_datetime2_and_round_trips(cursor, value):
    base, _, _ = _props(cursor, value)
    assert base == "datetime2"
    cursor.execute("SELECT CAST(? AS datetime2(6))", (value,))
    assert cursor.fetchone()[0] == value


def test_datetime_subclass_and_aware_datetime_bind(cursor):
    class Stamp(dt.datetime):
        pass

    cursor.execute("SELECT CAST(? AS datetime2(6))", (Stamp(2024, 5, 6, 7, 8, 9, 10),))
    assert cursor.fetchone()[0] == dt.datetime(2024, 5, 6, 7, 8, 9, 10)

    aware = dt.datetime(2024, 5, 6, 7, 8, 9, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    cursor.execute("SELECT CAST(? AS datetimeoffset(0))", (aware,))
    assert cursor.fetchone()[0] == aware
