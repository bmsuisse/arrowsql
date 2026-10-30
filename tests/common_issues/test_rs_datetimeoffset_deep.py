"""datetimeoffset deep dive: offsets, DST, tz-aware params, scale, comparisons.

Research sources (real reported problems this suite guards against):
- https://github.com/microsoft/mssql-python/issues/213  (datetimeoffset column raised "Unsupported data type ... Type -155")
- https://github.com/microsoft/mssql-python/pull/281/files  (driver used to convert fetched values to UTC; offsets must survive)
- https://github.com/microsoft/mssql-python/pull/260.diff  (SQL Server keeps 100ns, Python datetime only 1us; the offset must be kept with the value)
- https://github.com/microsoft/mssql-python/issues/869  (fractional seconds truncated to 6 digits when parsed through Python datetime)
- https://github.com/mkleehammer/pyodbc/issues/1141  (tz-aware datetimeoffset param comes back as UTC; offset lost)
- https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function  (type -155 is not native in ODBC clients)
- https://github.com/sqlalchemy/sqlalchemy/issues/5075  (tz offset dropped when a tz-aware datetime is a raw-SQL parameter)
- https://github.com/sqlalchemy/sqlalchemy/issues/6366  (tz-aware datetime fails on INSERT with mssql+pyodbc)
- https://github.com/sqlalchemy/sqlalchemy/issues/13610  (years below 1000 mis-formatted without zero padding)
- https://github.com/microsoft/mssql-jdbc/issues/3072  (pre-1582 datetimeoffset values shifted by Julian/Gregorian calendar)
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql  (offset range -14:00..+14:00, scale 0..7, stored/compared as UTC)

Ground truth is the upstream ODBC driver (mssql_python 1.16.0, .venv-upstream).
Native-TDS pitfalls this file targets: offset minutes encoding for odd offsets,
UTC-instant vs wall-clock storage, naive-datetime defaulting, scale rounding
on the server vs truncation on the client, and out-of-range offsets.
"""

import datetime as dt
import zoneinfo

import pytest
from mssql_python import DataError

UTC = dt.timezone.utc


def _tz(hours=0, minutes=0):
    return dt.timezone(dt.timedelta(hours=hours, minutes=minutes))


# (literal offset, expected utcoffset)
OFFSETS = [
    ("-14:00", dt.timedelta(hours=-14)),
    ("-09:30", dt.timedelta(hours=-9, minutes=-30)),
    ("+05:45", dt.timedelta(hours=5, minutes=45)),
    ("+09:30", dt.timedelta(hours=9, minutes=30)),
    ("+13:45", dt.timedelta(hours=13, minutes=45)),
    ("+14:00", dt.timedelta(hours=14)),
]


@pytest.mark.parametrize("literal, expected_offset", OFFSETS)
def test_string_cast_keeps_wall_clock_and_offset(cursor, literal, expected_offset):
    cursor.execute(
        "SELECT CAST(? AS DATETIMEOFFSET(7))",
        (f"2024-03-10 01:30:00.1234567 {literal}",),
    )
    value = cursor.fetchone()[0]
    assert type(value) is dt.datetime
    assert value.utcoffset() == expected_offset
    assert value.replace(tzinfo=None) == dt.datetime(2024, 3, 10, 1, 30, 0, 123456)


@pytest.mark.parametrize("literal, expected_offset", OFFSETS)
def test_column_roundtrip_preserves_each_offset(cursor, literal, expected_offset):
    cursor.execute("CREATE TABLE #dto (id INT, v DATETIMEOFFSET(7))")
    wall = dt.datetime(2024, 3, 10, 1, 30, 0, 123456)
    cursor.execute(
        "INSERT INTO #dto VALUES (?, ?)",
        (1, wall.replace(tzinfo=dt.timezone(expected_offset))),
    )
    cursor.execute("SELECT v FROM #dto WHERE id = 1")
    value = cursor.fetchone()[0]
    assert value.utcoffset() == expected_offset
    assert value.replace(tzinfo=None) == wall


def test_aware_param_offset_is_not_converted_to_utc(cursor):
    # pyodbc #1141 / mssql-python #281 class: an aware param must come back
    # with its own offset, not re-expressed in UTC.
    value_in = dt.datetime(2024, 6, 1, 12, 0, 0, tzinfo=_tz(13, 45))
    cursor.execute("SELECT ?", (value_in,))
    value = cursor.fetchone()[0]
    assert value.utcoffset() == dt.timedelta(hours=13, minutes=45)
    assert value.replace(tzinfo=None) == dt.datetime(2024, 6, 1, 12, 0)
    assert value == value_in


def test_executemany_mixed_tzinfos_each_row_keeps_its_offset(cursor):
    cursor.execute("CREATE TABLE #mix (id INT, v DATETIMEOFFSET(6))")
    rows = [
        (1, dt.datetime(2024, 1, 1, 0, 0, tzinfo=_tz(14))),
        (2, dt.datetime(2024, 1, 1, 0, 0, tzinfo=_tz(-14))),
        (3, dt.datetime(2024, 1, 1, 6, 0, tzinfo=UTC)),
        (4, dt.datetime(2024, 1, 1, 5, 45, tzinfo=_tz(5, 45))),
    ]
    cursor.executemany("INSERT INTO #mix VALUES (?, ?)", rows)
    cursor.execute("SELECT id, v FROM #mix ORDER BY id")
    fetched = cursor.fetchall()
    assert [r[0] for r in fetched] == [1, 2, 3, 4]
    for (_, got), (_, want) in zip(fetched, rows):
        assert got.utcoffset() == want.utcoffset()
        assert got == want
    assert fetched[0][1].utcoffset() == dt.timedelta(hours=14)
    assert fetched[1][1].utcoffset() == dt.timedelta(hours=-14)


def test_zoneinfo_param_uses_the_dst_offset_in_force_at_that_instant(cursor):
    berlin = zoneinfo.ZoneInfo("Europe/Berlin")
    summer = dt.datetime(2024, 7, 1, 12, 0, tzinfo=berlin)
    winter = dt.datetime(2024, 1, 1, 12, 0, tzinfo=berlin)
    cursor.execute("SELECT ?, ?", (summer, winter))
    got_summer, got_winter = cursor.fetchone()
    assert got_summer.utcoffset() == dt.timedelta(hours=2)
    assert got_winter.utcoffset() == dt.timedelta(hours=1)
    assert got_summer == summer
    assert got_winter == winter


def test_naive_datetime_param_into_datetimeoffset_is_taken_as_utc(cursor):
    naive = dt.datetime(2024, 1, 1, 12, 0, 0)
    cursor.execute("SELECT CAST(? AS DATETIMEOFFSET(7))", (naive,))
    value = cursor.fetchone()[0]
    assert value.utcoffset() == dt.timedelta(0)
    assert value.replace(tzinfo=None) == naive


@pytest.mark.parametrize(
    "scale, expected_microsecond",
    [
        (0, 0),  # .1234567 rounds to 0 seconds fraction
        (1, 100000),  # .1
        (2, 120000),  # .12
        (3, 123000),  # .123
        (4, 123500),  # .1235 (rounded, not truncated)
        (5, 123460),  # .12346 (rounded)
        (6, 123457),  # .123457 (rounded)
        (7, 123456),  # 100ns digit cannot exist in a Python datetime: truncated
    ],
)
def test_scale_rounds_server_side_and_truncates_at_python_microseconds(cursor, scale, expected_microsecond):
    cursor.execute(
        f"SELECT CAST(? AS DATETIMEOFFSET({scale}))",
        ("2024-01-01 12:00:00.1234567 +02:00",),
    )
    value = cursor.fetchone()[0]
    assert value.utcoffset() == dt.timedelta(hours=2)
    assert value.microsecond == expected_microsecond
    assert value.second == 0


def test_scale_rounding_carries_into_the_next_day(cursor):
    cursor.execute(
        "SELECT CAST('2024-01-01 23:59:59.9999995 +00:00' AS DATETIMEOFFSET(6))"
    )
    value = cursor.fetchone()[0]
    assert value == dt.datetime(2024, 1, 2, 0, 0, tzinfo=UTC)
    assert value.utcoffset() == dt.timedelta(0)


def test_scale3_column_rounds_fraction_on_insert(cursor):
    cursor.execute("CREATE TABLE #s3 (v DATETIMEOFFSET(3))")
    cursor.execute("INSERT INTO #s3 VALUES (?)", ("2024-01-01 12:00:00.123999 +02:00",))
    cursor.execute("SELECT v FROM #s3")
    value = cursor.fetchone()[0]
    assert value.microsecond == 124000
    assert value.utcoffset() == dt.timedelta(hours=2)


def test_microsecond_param_into_scale7_column_is_exact(cursor):
    cursor.execute("CREATE TABLE #s7 (v DATETIMEOFFSET(7))")
    value_in = dt.datetime(2024, 5, 5, 5, 5, 5, 999999, tzinfo=_tz(5, 45))
    cursor.execute("INSERT INTO #s7 VALUES (?)", (value_in,))
    cursor.execute("SELECT v FROM #s7")
    value = cursor.fetchone()[0]
    assert value.microsecond == 999999
    assert value.utcoffset() == dt.timedelta(hours=5, minutes=45)
    assert value == value_in


def test_min_and_max_values_roundtrip_exactly(cursor):
    cursor.execute(
        "SELECT CAST('0001-01-01 00:00:00.0000000 +00:00' AS DATETIMEOFFSET(7)),"
        " CAST('9999-12-31 23:59:59.9999999 +00:00' AS DATETIMEOFFSET(7))"
    )
    low, high = cursor.fetchone()
    assert low == dt.datetime(1, 1, 1, 0, 0, tzinfo=UTC)
    assert high == dt.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)


def test_extreme_offsets_at_calendar_edges_are_valid(cursor):
    # Valid only because the UTC instant stays inside 0001..9999.
    cursor.execute(
        "SELECT CAST('0001-01-02 00:00:00 -14:00' AS DATETIMEOFFSET(7)),"
        " CAST('9999-12-30 23:59:59.9999999 -14:00' AS DATETIMEOFFSET(7))"
    )
    low, high = cursor.fetchone()
    assert low.utcoffset() == dt.timedelta(hours=-14)
    assert low.replace(tzinfo=None) == dt.datetime(1, 1, 2, 0, 0)
    assert high.utcoffset() == dt.timedelta(hours=-14)
    assert high.replace(tzinfo=None) == dt.datetime(9999, 12, 30, 23, 59, 59, 999999)


def test_utc_overflow_from_aware_param_raises_dataerror(cursor):
    # Wall clock 0001-01-01 00:00 at +14:00 is year 0 in UTC: not representable.
    with pytest.raises(DataError):
        cursor.execute(
            "SELECT CAST(? AS DATETIMEOFFSET(7))",
            (dt.datetime(1, 1, 1, 0, 0, tzinfo=_tz(14)),),
        )


@pytest.mark.upstream_bug(
    reason=(
        "Offset beyond SQL Server's -14:00..+14:00 range raises a bare RuntimeError "
        "from the driver instead of a DB-API DataError"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_offset_beyond_fourteen_hours_raises_dataerror(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT ?", (dt.datetime(2024, 1, 1, 12, 0, tzinfo=_tz(15)),))


@pytest.mark.parametrize(
    "zone, utc_input, expected_wall, expected_offset",
    [
        ("Chatham Islands Standard Time", "2024-01-15 12:00:00 +00:00", dt.datetime(2024, 1, 16, 1, 45), dt.timedelta(hours=13, minutes=45)),
        ("Chatham Islands Standard Time", "2024-07-15 12:00:00 +00:00", dt.datetime(2024, 7, 16, 0, 45), dt.timedelta(hours=12, minutes=45)),
        ("Lord Howe Standard Time", "2024-01-15 12:00:00 +00:00", dt.datetime(2024, 1, 15, 23, 0), dt.timedelta(hours=11)),
        ("Lord Howe Standard Time", "2024-07-15 12:00:00 +00:00", dt.datetime(2024, 7, 15, 22, 30), dt.timedelta(hours=10, minutes=30)),
    ],
)
def test_at_time_zone_odd_offsets_with_half_hour_and_quarter_hour_dst(cursor, zone, utc_input, expected_wall, expected_offset):
    cursor.execute(
        f"SELECT CAST(? AS DATETIMEOFFSET(0)) AT TIME ZONE '{zone}'",
        (utc_input,),
    )
    value = cursor.fetchone()[0]
    assert value.utcoffset() == expected_offset
    assert value.replace(tzinfo=None) == expected_wall


@pytest.mark.parametrize(
    "utc_input, expected_wall, expected_offset",
    [
        ("2024-03-10 06:59:59 +00:00", dt.datetime(2024, 3, 10, 1, 59, 59), dt.timedelta(hours=-5)),
        ("2024-03-10 07:00:00 +00:00", dt.datetime(2024, 3, 10, 3, 0), dt.timedelta(hours=-4)),
        ("2024-11-03 06:30:00 +00:00", dt.datetime(2024, 11, 3, 1, 30), dt.timedelta(hours=-5)),
    ],
)
def test_at_time_zone_eastern_dst_transitions(cursor, utc_input, expected_wall, expected_offset):
    cursor.execute(
        "SELECT CAST(? AS DATETIMEOFFSET(0)) AT TIME ZONE 'Eastern Standard Time'",
        (utc_input,),
    )
    value = cursor.fetchone()[0]
    assert value.utcoffset() == expected_offset
    assert value.replace(tzinfo=None) == expected_wall


def test_switchoffset_from_odd_offset_to_negative_offset(cursor):
    cursor.execute(
        "SELECT SWITCHOFFSET(CAST('2024-01-01 12:00:00 +05:45' AS DATETIMEOFFSET(7)), '-09:00')"
    )
    value = cursor.fetchone()[0]
    assert value.utcoffset() == dt.timedelta(hours=-9)
    assert value.replace(tzinfo=None) == dt.datetime(2023, 12, 31, 21, 15)
    assert value == dt.datetime(2024, 1, 1, 6, 15, tzinfo=UTC)


def test_todatetimeoffset_applies_odd_offset_to_datetime2(cursor):
    cursor.execute(
        "SELECT TODATETIMEOFFSET(CAST('2024-01-01 12:00:00' AS DATETIME2(7)), '+13:45')"
    )
    value = cursor.fetchone()[0]
    assert value.utcoffset() == dt.timedelta(hours=13, minutes=45)
    assert value.replace(tzinfo=None) == dt.datetime(2024, 1, 1, 12, 0)


def test_same_instant_with_different_offsets_is_equal_and_deduplicates(cursor):
    cursor.execute(
        "SELECT COUNT(DISTINCT v), COUNT(*) FROM (VALUES"
        " (CAST('2024-01-01 12:00 +02:00' AS DATETIMEOFFSET(0))),"
        " (CAST('2024-01-01 10:00 +00:00' AS DATETIMEOFFSET(0))),"
        " (CAST('2024-01-01 05:00 -05:00' AS DATETIMEOFFSET(0)))"
        ") t(v)"
    )
    distinct_count, total = cursor.fetchone()
    assert (distinct_count, total) == (1, 3)


def test_equality_with_aware_param_compares_instants_not_wall_clock(cursor):
    cursor.execute(
        "SELECT CASE WHEN CAST(? AS DATETIMEOFFSET(7)) = ? THEN 1 ELSE 0 END, "
        "       CASE WHEN CAST(? AS DATETIMEOFFSET(7)) = ? THEN 1 ELSE 0 END",
        (
            "2024-01-01 00:00:00 +01:00",
            dt.datetime(2023, 12, 31, 23, 0, tzinfo=UTC),
            "2024-01-01 00:00:00 +01:00",
            dt.datetime(2024, 1, 1, 0, 0, tzinfo=_tz(2)),
        ),
    )
    same_instant, same_wall_other_offset = cursor.fetchone()
    assert same_instant == 1
    assert same_wall_other_offset == 0


def test_order_by_and_where_use_utc_instant(cursor):
    cursor.execute("CREATE TABLE #ord (id INT, v DATETIMEOFFSET(0))")
    cursor.executemany(
        "INSERT INTO #ord VALUES (?, ?)",
        [
            (1, "2024-01-01 00:00:00 +14:00"),  # 2023-12-31 10:00Z
            (2, "2024-01-01 00:00:00 -14:00"),  # 2024-01-01 14:00Z
            (3, "2024-01-01 06:00:00 +00:00"),  # 2024-01-01 06:00Z
        ],
    )
    cursor.execute("SELECT id FROM #ord ORDER BY v")
    assert [r[0] for r in cursor.fetchall()] == [1, 3, 2]

    cursor.execute(
        "SELECT id FROM #ord WHERE v < ? ORDER BY v",
        (dt.datetime(2024, 1, 1, 6, 30, tzinfo=UTC),),
    )
    assert [r[0] for r in cursor.fetchall()] == [1, 3]


def test_python_equality_and_hash_of_fetched_same_instant_values(cursor):
    cursor.execute(
        "SELECT CAST('2024-01-01 12:00:00 +02:00' AS DATETIMEOFFSET(0)),"
        " CAST('2024-01-01 10:00:00 +00:00' AS DATETIMEOFFSET(0))"
    )
    a, b = cursor.fetchone()
    assert a == b
    assert hash(a) == hash(b)
    assert a.utcoffset() != b.utcoffset()


def test_null_datetimeoffset_roundtrips_as_none(cursor):
    cursor.execute("SELECT CAST(? AS DATETIMEOFFSET(7))", (None,))
    assert cursor.fetchone()[0] is None


@pytest.mark.parametrize("year", [1, 99, 1500, 1582])
def test_pre_gregorian_and_short_year_values_keep_wall_clock(cursor, year):
    # mssql-jdbc #3072 / sqlalchemy #13610 class: Julian/Gregorian or zero-padding bugs.
    wall = dt.datetime(year, 3, 1, 12, 0, 0, 500000)
    cursor.execute("SELECT CAST(? AS DATETIMEOFFSET(6))", (wall.replace(tzinfo=_tz(2)),))
    value = cursor.fetchone()[0]
    assert value.replace(tzinfo=None) == wall
    assert value.utcoffset() == dt.timedelta(hours=2)
