"""Temporal type precision, rounding and range behavior.

Covers date/time/datetime/datetime2(0..7)/smalldatetime/datetimeoffset:
SQL Server's 100ns precision versus Python's microseconds, rounding and
truncation, the 0001-01-01 and 9999-12-31 extremes, smalldatetime's
minute-level rounding, timezone-aware parameters, and time(7).

Every assertion here reflects what the upstream ODBC driver returns today.
Where upstream diverges from SQL Server itself, the expectation follows the
server.
"""

import datetime as dt

import pytest
from mssql_python import DataError

UTC_PLUS_0530 = dt.timezone(dt.timedelta(hours=5, minutes=30))
UTC_MINUS_0530 = dt.timezone(dt.timedelta(hours=-5, minutes=-30))
UTC_PLUS_14 = dt.timezone(dt.timedelta(hours=14))
UTC_MINUS_14 = dt.timezone(dt.timedelta(hours=-14))
UTC_PLUS_2 = dt.timezone(dt.timedelta(hours=2))


def _select_cast(cursor, value, sql_type):
    """Round-trip a single Python value through CAST(? AS <sql_type>)."""
    cursor.execute(f"SELECT CAST(? AS {sql_type})", (value,))
    return cursor.fetchone()[0]


def _select_literal(cursor, literal, sql_type):
    """Let the server parse a string literal, then return the value."""
    cursor.execute(f"SELECT CAST('{literal}' AS {sql_type})")
    return cursor.fetchone()[0]


def test_datetime2_7_roundtrips_microseconds_exactly(cursor):
    value = dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
    result = _select_cast(cursor, value, "datetime2(7)")
    assert type(result) is dt.datetime
    assert result == value
    assert result.microsecond == 123456


def test_datetime2_7_server_nanoseconds_truncate_to_microseconds(cursor):
    # 100ns digits beyond microseconds cannot fit in a Python datetime, so the
    # driver drops them (truncation, not rounding).
    assert _select_literal(cursor, "2024-01-02 03:04:05.1234567", "datetime2(7)") == (
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
    )
    assert _select_literal(cursor, "2024-01-02 03:04:05.9999999", "datetime2(7)") == (
        dt.datetime(2024, 1, 2, 3, 4, 5, 999999)
    )


@pytest.mark.parametrize(
    "fraction, expected_micro",
    [
        ("1234565", 123457),  # 7 digits -> 6: half rounds up on the server
        ("1234564", 123456),
    ],
)
def test_datetime2_6_server_rounds_half_up(cursor, fraction, expected_micro):
    value = _select_literal(cursor, f"2024-01-02 03:04:05.{fraction}", "datetime2(6)")
    assert value == dt.datetime(2024, 1, 2, 3, 4, 5, expected_micro)


@pytest.mark.parametrize(
    "micro, expected_second",
    [
        (500000, 6),  # exactly half a second rounds up
        (499999, 5),
        (999999, 6),
    ],
)
def test_datetime2_0_param_rounds_fraction_to_nearest_second(cursor, micro, expected_second):
    value = _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 5, micro), "datetime2(0)")
    assert value == dt.datetime(2024, 1, 2, 3, 4, expected_second)
    assert value.microsecond == 0


def test_datetime2_0_rounding_carries_into_next_year(cursor):
    value = _select_cast(cursor, dt.datetime(2024, 12, 31, 23, 59, 59, 500000), "datetime2(0)")
    assert value == dt.datetime(2025, 1, 1, 0, 0, 0)


def test_datetime2_3_param_rounds_to_milliseconds(cursor):
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 5, 123456), "datetime2(3)") == (
        dt.datetime(2024, 1, 2, 3, 4, 5, 123000)
    )
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 5, 1234), "datetime2(3)") == (
        dt.datetime(2024, 1, 2, 3, 4, 5, 1000)
    )


def test_datetime_param_rounds_to_three_hundredths_of_a_second(cursor):
    # datetime stores 1/300 s ticks, so the server snaps to 0/3.33/6.67 ms.
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 5, 998000), "datetime") == (
        dt.datetime(2024, 1, 2, 3, 4, 5, 997000)
    )
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 5, 1234), "datetime") == (
        dt.datetime(2024, 1, 2, 3, 4, 5, 0)
    )
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 59, 999999), "datetime") == (
        dt.datetime(2024, 1, 2, 3, 5, 0)
    )




def test_datetime_max_with_microseconds_does_not_overflow(cursor):
    # 23:59:59.999999 is above the datetime maximum of .997, so it rounds down
    # within the same value rather than raising.
    value = _select_cast(cursor, dt.datetime(9999, 12, 31, 23, 59, 59, 999999), "datetime")
    assert value == dt.datetime(9999, 12, 31, 23, 59, 59, 997000)


def test_datetime_range_starts_at_1753(cursor):
    assert _select_cast(cursor, dt.datetime(1753, 1, 1), "datetime") == dt.datetime(1753, 1, 1)
    with pytest.raises(DataError):
        _select_cast(cursor, dt.datetime(1752, 12, 31, 23, 59, 59), "datetime")




def test_datetime2_min_and_max_roundtrip(cursor):
    # 0001-01-01 and 9999-12-31 with full microsecond precision.
    assert _select_cast(cursor, dt.datetime(1, 1, 1), "datetime2(7)") == dt.datetime(1, 1, 1)
    assert _select_cast(
        cursor, dt.datetime(9999, 12, 31, 23, 59, 59, 999999), "datetime2(7)"
    ) == dt.datetime(9999, 12, 31, 23, 59, 59, 999999)
    assert _select_literal(cursor, "0001-01-01 00:00:00", "datetime2") == dt.datetime(1, 1, 1)


@pytest.mark.parametrize(
    "value, expected",
    [
        (dt.datetime(1900, 1, 1, 0, 0), dt.datetime(1900, 1, 1, 0, 0)),
        (dt.datetime(2079, 6, 6, 23, 59), dt.datetime(2079, 6, 6, 23, 59)),
    ],
)
def test_smalldatetime_range_boundaries_roundtrip(cursor, value, expected):
    assert _select_cast(cursor, value, "smalldatetime") == expected


def test_smalldatetime_rounds_to_nearest_minute_with_ties_up(cursor):
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 29, 999999), "smalldatetime") == (
        dt.datetime(2024, 1, 2, 3, 4)
    )
    assert _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 30), "smalldatetime") == (
        dt.datetime(2024, 1, 2, 3, 5)
    )


def test_smalldatetime_out_of_range_raises_dataerror(cursor):
    with pytest.raises(DataError):
        _select_cast(cursor, dt.datetime(2080, 1, 1), "smalldatetime")
    # 23:59:59 rounds up to 2079-06-07, which is past the smalldatetime maximum.
    with pytest.raises(DataError):
        _select_cast(cursor, dt.datetime(2079, 6, 6, 23, 59, 59), "smalldatetime")


def test_time7_keeps_microseconds_and_truncates_100ns(cursor):
    assert _select_cast(cursor, dt.time(1, 2, 3, 456789), "time(7)") == dt.time(1, 2, 3, 456789)
    assert _select_literal(cursor, "12:34:56.1234567", "time(7)") == dt.time(12, 34, 56, 123456)


def test_time0_rounds_half_up_and_clamps_at_midnight(cursor):
    assert _select_cast(cursor, dt.time(3, 4, 5, 500000), "time(0)") == dt.time(3, 4, 6)
    # time values do not wrap past 23:59:59.9999999; the rounded value clamps.
    assert _select_literal(cursor, "23:59:59.6", "time(0)") == dt.time(23, 59, 59)




def test_timezone_aware_param_into_datetime2_keeps_wall_clock_and_drops_offset(cursor):
    # tzinfo is not applied to datetime2: the wall-clock fields are stored as-is.
    cursor.execute(
        "SELECT CAST(? AS datetime2(7))",
        (dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC_PLUS_0530),),
    )
    value = cursor.fetchone()[0]
    assert value == dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
    assert value.tzinfo is None


def test_timezone_aware_param_into_datetimeoffset_preserves_offset(cursor):
    for tz in (UTC_PLUS_0530, UTC_MINUS_0530, UTC_PLUS_2):
        value = dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=tz)
        result = _select_cast(cursor, value, "datetimeoffset(7)")
        assert result.replace(tzinfo=None) == dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
        assert result.utcoffset() == tz.utcoffset(None)


def test_naive_datetime_into_datetimeoffset_reads_back_as_utc(cursor):
    result = _select_cast(cursor, dt.datetime(2024, 1, 2, 3, 4, 5, 123456), "datetimeoffset(7)")
    assert result.replace(tzinfo=None) == dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
    assert result.utcoffset() == dt.timedelta(0)


def test_datetimeoffset_server_literal_keeps_fraction_and_offset(cursor):
    result = _select_literal(cursor, "2024-01-02 03:04:05.1234567 +05:30", "datetimeoffset(7)")
    assert result == dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC_PLUS_0530)
    assert result.utcoffset() == dt.timedelta(hours=5, minutes=30)


def test_datetimeoffset_min_with_negative_offset_roundtrips(cursor):
    value = dt.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=UTC_MINUS_0530)
    result = _select_cast(cursor, value, "datetimeoffset(7)")
    assert result.replace(tzinfo=None) == dt.datetime(1, 1, 1)
    assert result.utcoffset() == dt.timedelta(hours=-5, minutes=-30)


def test_datetimeoffset_range_checks_are_on_utc_value(cursor):
    # 0001-01-01 00:00 at +05:30 is 0000-12-31 18:30 UTC, outside the range.
    with pytest.raises(DataError):
        _select_cast(
            cursor,
            dt.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=UTC_PLUS_0530),
            "datetimeoffset(7)",
        )
    # 9999-12-31 23:59:59.999999 at -14:00 is 10000-01-01 UTC, outside the range.
    with pytest.raises(DataError):
        _select_cast(
            cursor,
            dt.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC_MINUS_14),
            "datetimeoffset(7)",
        )
    # The same instant at +14:00 is in range.
    result = _select_cast(
        cursor,
        dt.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC_PLUS_14),
        "datetimeoffset(7)",
    )
    assert result.replace(tzinfo=None) == dt.datetime(9999, 12, 31, 23, 59, 59, 999999)
    assert result.utcoffset() == dt.timedelta(hours=14)


def test_datetimeoffset_offset_beyond_14_hours_raises_runtimeerror(cursor):
    # Raised by the driver before any SQL is sent. Upstream uses RuntimeError,
    # not a DB-API class, so the Rust port must match that.
    value = dt.datetime(2024, 1, 2, tzinfo=dt.timezone(dt.timedelta(hours=15)))
    with pytest.raises(RuntimeError, match="out of SQL Server range"):
        _select_cast(cursor, value, "datetimeoffset(7)")






def test_null_temporal_params_roundtrip_as_none(cursor):
    cursor.execute(
        "SELECT CAST(? AS datetime2(7)), CAST(? AS datetimeoffset(7)), CAST(? AS time(7))",
        (None, None, None),
    )
    assert tuple(cursor.fetchone()) == (None, None, None)


def test_description_reports_temporal_python_types_and_scale(cursor):
    cursor.execute(
        "SELECT CAST(NULL AS datetime2(7)) AS a, CAST(NULL AS datetimeoffset(3)) AS b, "
        "CAST(NULL AS time(4)) AS c, CAST(NULL AS smalldatetime) AS d"
    )
    desc = {col[0]: col for col in cursor.description}
    assert desc["a"][1] is dt.datetime and desc["a"][5] == 7
    assert desc["b"][1] is dt.datetime and desc["b"][5] == 3
    assert desc["c"][1] is dt.time and desc["c"][5] == 4
    assert desc["d"][1] is dt.datetime and desc["d"][5] == 0


def test_permanent_table_stores_each_column_at_its_own_precision(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        "id int NOT NULL, d0 datetime2(0), d3 datetime2(3), d7 datetime2(7), "
        "dt datetime, sd smalldatetime, t7 time(7))"
    )
    value = dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (id, d0, d3, d7, dt, sd, t7) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, value, value, value, value, value, dt.time(1, 2, 3, 456789)),
            (2, dt.datetime(1, 1, 1), dt.datetime(1, 1, 1), dt.datetime(1, 1, 1),
             dt.datetime(1753, 1, 1), dt.datetime(1900, 1, 1), dt.time(0, 0)),
        ],
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT id, d0, d3, d7, dt, sd, t7 FROM dbo.{table_name} ORDER BY id")
    rows = [tuple(row) for row in cursor.fetchall()]
    assert rows[0] == (
        1,
        dt.datetime(2024, 1, 2, 3, 4, 5, 0),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123000),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123000),
        dt.datetime(2024, 1, 2, 3, 4, 0),
        dt.time(1, 2, 3, 456789),
    )
    assert rows[1] == (
        2,
        dt.datetime(1, 1, 1),
        dt.datetime(1, 1, 1),
        dt.datetime(1, 1, 1),
        dt.datetime(1753, 1, 1),
        dt.datetime(1900, 1, 1),
        dt.time(0, 0),
    )
