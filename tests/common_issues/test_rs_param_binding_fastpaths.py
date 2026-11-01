"""Parameter binding fast paths: exact TIME/datetime/UUID values, hinted DECIMAL
rounding, int+Decimal executemany columns and the untyped-NULL describe cache."""

import datetime as dt
import decimal
import uuid

import mssql_python as m
import pytest
from mssql_python import connect

D = decimal.Decimal
CTX = decimal.Context(prec=100)


def _one(cursor, sql, *params):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


# --- TIME / DATETIMEOFFSET / UUID ---------------------------------------------


def test_exact_time_round_trips_with_microseconds(cursor):
    value = dt.time(23, 59, 58, 123456)
    assert _one(cursor, "SELECT CAST(? AS TIME(6))", value) == value


def test_aware_time_is_rejected(cursor):
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", (dt.time(1, 2, 3, tzinfo=dt.timezone.utc),))


def test_time_subclass_with_broken_isoformat_is_rejected(cursor):
    class BadTime(dt.time):
        def isoformat(self, *a, **k):
            return 42

    with pytest.raises(Exception):
        cursor.execute("SELECT ?", (BadTime(1, 2, 3),))


@pytest.mark.parametrize("hours", [-14, -5, 0, 1, 14])
def test_aware_datetime_offsets_round_trip(cursor, hours):
    tz = dt.timezone(dt.timedelta(hours=hours))
    value = dt.datetime(2020, 2, 29, 23, 30, 1, 5, tzinfo=tz)
    got = _one(cursor, "SELECT CAST(? AS DATETIMEOFFSET(7))", value)
    assert got == value
    assert got.utcoffset() == value.utcoffset()


def test_sub_minute_offset_and_out_of_range_offset_are_rejected(cursor):
    for tz in (dt.timezone(dt.timedelta(seconds=30)), dt.timezone(dt.timedelta(microseconds=1)),
               dt.timezone(dt.timedelta(hours=15))):
        with pytest.raises(Exception):
            cursor.execute("SELECT ?", (dt.datetime(2020, 1, 1, tzinfo=tz),))


def test_out_of_range_utc_instant_is_a_data_error(cursor):
    tz = dt.timezone(dt.timedelta(hours=-1))
    with pytest.raises(m.DataError):
        cursor.execute("SELECT ?", (dt.datetime(9999, 12, 31, 23, 30, tzinfo=tz),))


@pytest.mark.parametrize("value", [dt.datetime(1, 1, 1), dt.datetime(1970, 1, 1), dt.datetime(2000, 2, 29, 12, 0, 0, 7),
                                   dt.datetime(2024, 12, 31, 23, 59, 59, 999999), dt.datetime(9999, 12, 31, 23, 59, 59)])
def test_naive_datetime_days_match_python(cursor, value):
    assert _one(cursor, "SELECT CAST(? AS DATETIME2(7))", value) == value


def test_uuid_round_trip(cursor):
    for value in (uuid.UUID(int=0), uuid.UUID(int=2**128 - 1), uuid.uuid4(), uuid.uuid1()):
        assert _one(cursor, "SELECT CAST(? AS UNIQUEIDENTIFIER)", value) == value


def test_date_param(cursor):
    assert _one(cursor, "SELECT ?", dt.date(2024, 2, 29)) == dt.date(2024, 2, 29)


# --- hinted DECIMAL rounding ---------------------------------------------------

ROUND_CASES = [
    ("9.995", 2), ("-9.995", 2), ("0.005", 2), ("-0.005", 2), ("0.004", 2), ("-0.004", 2), ("-0.0", 2),
    ("1.5", 0), ("-1.5", 0), ("0.49", 0), ("99999999999999999999999999999999.995", 2),
    ("-99999999999999999999999999999999.995", 2), ("1.23456", 2), ("123.456", 5), ("0.999999", 3),
    ("12345678901234567890.123456789", 8), ("0.0000000000000000000000000000000000005", 36),
    ("7.65", 1), ("7.55", 1), ("-7.65", 1),
]


@pytest.mark.parametrize("text,scale", ROUND_CASES)
def test_hinted_decimal_rounds_half_up_like_quantize(cursor, text, scale):
    expected = D(text).quantize(D(1).scaleb(-scale), rounding=decimal.ROUND_HALF_UP, context=CTX)
    cursor.setinputsizes([(m.SQL_DECIMAL, 38, scale)])
    got = _one(cursor, "SELECT ?", D(text))
    assert got == expected


def test_money_value_still_binds(cursor):
    assert _one(cursor, "SELECT CAST(? AS MONEY)", D("12.3456")) == D("12.3456")


# --- executemany: TIME, DATETIMEOFFSET and int+Decimal columns -----------------


def test_executemany_time_offset_and_mixed_numeric_columns(cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (t TIME(6), a DATETIMEOFFSET(0), n DECIMAL(38,4))")
    tz = dt.timezone(dt.timedelta(hours=2))
    rows = [
        (dt.time(1, 2, 3, 4), dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=tz), 5),
        (dt.time(0, 0), dt.datetime(2021, 1, 2, tzinfo=tz), D("-0.0000")),
        (None, None, D("12345678901234567890.1234")),
        (dt.time(23, 59, 59, 999999), dt.datetime(2022, 3, 4, 5, 6, 7, tzinfo=tz), -(2**63)),
        (dt.time(5), dt.datetime(2023, 3, 4, tzinfo=tz), 0),
    ]
    cursor.executemany(f"INSERT INTO {table_name} VALUES (?,?,?)", rows)
    cursor.execute(f"SELECT t, a, n FROM {table_name}")
    got = [tuple(r) for r in cursor.fetchall()]
    assert [(r[0], r[1]) for r in got] == [(r[0], r[1]) for r in rows]
    assert [r[2] for r in got] == [D(r[2]) if r[2] is not None else None for r in rows]


# --- untyped NULL describe cache -----------------------------------------------


def test_untyped_null_types_survive_cache_pressure(cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (i INT, s NVARCHAR(10), d DATETIME2)")
    sqls = [f"INSERT INTO {table_name} (i, s, d) SELECT ?, ?, ? WHERE {k} = {k}" for k in range(600)]
    for sql in sqls:
        cursor.execute(sql, (None, None, None))
    cursor.execute(sqls[0], (None, "x", None))
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert cursor.fetchone()[0] == 601
