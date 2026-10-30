"""TIME(0)..TIME(7) fetch and parameter behavior, and time/timedelta binding.

Research sources (issue trackers and docs, consulted for failure modes):
- https://github.com/microsoft/mssql-python/pull/479 : time(6) microseconds were
  lost on bind/fetch; fixed by moving datetime.time to a text bind and parsing
  TIME2 text on fetch (regression test for TIME(6) microseconds).
- https://github.com/microsoft/mssql-python/issues/869 : fractional seconds
  beyond microseconds are silently truncated to 6 digits by the Python side.
- https://github.com/microsoft/mssql-python/pull/872 : Timestamp() signature
  and microsecond handling in the DB-API helpers.
- https://github.com/mkleehammer/Pyodbc/wiki/Tips-and-Tricks-by-Database-Platform :
  pyodbc sends datetime.time via ODBC TIME_STRUCT, which has no fractional
  seconds, so sub-second parameter values are dropped.
- https://github.com/sqlalchemy/sqlalchemy/issues/2340 : TIME(7) values with
  100ns digits raised ValueError on read; fixed by truncating to microseconds.
- https://github.com/hugr-lab/mssql-extension/pull/462 : TIME(7) read as
  microseconds, the 100ns digit is dropped (truncation, not rounding).
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/datetime-handling?view=sql-server-ver17
  : documented mapping of time to datetime.time (100ns range, Python holds
  microseconds only).

Expectations were cross-checked against SQL Server 2022 with sqlcmd: typed
time casts round half up at the target scale and saturate at 23:59:59.9999999
(they do not wrap past midnight), while string literals parsed directly into
time(n) follow the same rule. Python microsecond values are the only thing the
driver can return, so 100ns digits are truncated on fetch.
"""

import datetime as dt

import pytest


@pytest.mark.parametrize(
    "scale, expected",
    [
        (0, dt.time(23, 59, 59, 0)),
        (3, dt.time(23, 59, 59, 999000)),
        (6, dt.time(23, 59, 59, 999999)),
        (7, dt.time(23, 59, 59, 999999)),
    ],
)
def test_time_scale_max_literal_saturates_at_max_time(cursor, scale, expected):
    # 23:59:59.9999999 rounded to any scale must stay within the day: the
    # server clamps, it does not wrap to 00:00:00.
    cursor.execute(f"SELECT CAST('23:59:59.9999999' AS time({scale}))")
    value = cursor.fetchone()[0]
    assert type(value) is dt.time
    assert value == expected


def test_time7_fetch_truncates_100ns_digits_instead_of_rounding(cursor):
    # Server holds 12:34:56.1234569; Python can only carry microseconds, so the
    # 7th digit is dropped (123456, not 123457 as server rounding would give).
    cursor.execute("SELECT CAST('12:34:56.1234569' AS time(7))")
    value = cursor.fetchone()[0]
    assert type(value) is dt.time
    assert value == dt.time(12, 34, 56, 123456)


def test_time6_fetch_keeps_six_digits_and_rounds_half_up_on_server(cursor):
    # The server rounds the 7th digit at scale 6 (half up), so the python value
    # is 123457 microseconds.
    cursor.execute("SELECT CAST('12:34:56.1234565' AS time(6))")
    assert cursor.fetchone()[0] == dt.time(12, 34, 56, 123457)


def test_time_param_roundtrips_exact_microseconds_through_time7(cursor):
    value = dt.time(1, 2, 3, 123456)
    cursor.execute("SELECT CAST(? AS time(7))", (value,))
    result = cursor.fetchone()[0]
    assert type(result) is dt.time
    assert result == value
    assert result.microsecond == 123456


def test_time_param_midnight_and_max_roundtrip(cursor):
    cursor.execute("SELECT CAST(? AS time(7)), CAST(? AS time(7))", (dt.time(0, 0, 0), dt.time(23, 59, 59, 999999)))
    midnight, last = cursor.fetchone()
    assert midnight == dt.time(0, 0, 0)
    assert last == dt.time(23, 59, 59, 999999)


@pytest.mark.parametrize(
    "param, scale, expected",
    [
        (dt.time(12, 0, 0, 999999), 3, dt.time(12, 0, 1, 0)),
        (dt.time(12, 0, 0, 999999), 2, dt.time(12, 0, 1, 0)),
        (dt.time(12, 0, 0, 999999), 6, dt.time(12, 0, 0, 999999)),
        (dt.time(12, 0, 0, 500000), 0, dt.time(12, 0, 1, 0)),
        (dt.time(12, 0, 0, 499999), 0, dt.time(12, 0, 0, 0)),
        (dt.time(23, 59, 59, 500000), 0, dt.time(23, 59, 59, 0)),
    ],
)
def test_time_param_is_rounded_by_server_to_target_scale(cursor, param, scale, expected):
    cursor.execute(f"SELECT CAST(? AS time({scale}))", (param,))
    value = cursor.fetchone()[0]
    assert type(value) is dt.time
    assert value == expected


def test_time_column_insert_and_select_exact_microseconds(cursor):
    cursor.execute("CREATE TABLE #t7 (id int NOT NULL, t time(7) NULL)")
    cursor.execute("INSERT INTO #t7 (id, t) VALUES (?, ?)", (1, dt.time(8, 15, 30, 654321)))
    cursor.execute("SELECT t FROM #t7 WHERE id = 1")
    value = cursor.fetchone()[0]
    assert type(value) is dt.time
    assert value == dt.time(8, 15, 30, 654321)


def test_time0_column_stores_rounded_value_from_param(cursor):
    cursor.execute("CREATE TABLE #t0 (id int NOT NULL, t time(0) NOT NULL)")
    cursor.executemany(
        "INSERT INTO #t0 (id, t) VALUES (?, ?)",
        [
            (1, dt.time(12, 0, 0, 500000)),
            (2, dt.time(12, 0, 0, 499999)),
            (3, dt.time(23, 59, 59, 999999)),
        ],
    )
    cursor.execute("SELECT id, t FROM #t0 ORDER BY id")
    rows = cursor.fetchall()
    assert [(r[0], r[1]) for r in rows] == [
        (1, dt.time(12, 0, 1)),
        (2, dt.time(12, 0, 0)),
        (3, dt.time(23, 59, 59)),
    ]


def test_time_null_roundtrips_as_none(cursor):
    cursor.execute("CREATE TABLE #tnull (t time(7) NULL)")
    cursor.execute("INSERT INTO #tnull (t) VALUES (?)", (None,))
    cursor.execute("SELECT t FROM #tnull")
    assert cursor.fetchone()[0] is None


def test_time_executemany_mixed_precision_preserves_order(cursor):
    cursor.execute("CREATE TABLE #tmany (id int NOT NULL, t time(7) NOT NULL)")
    values = [
        (1, dt.time(0, 0, 0, 1)),
        (2, dt.time(0, 0, 0, 0)),
        (3, dt.time(12, 30, 0, 250000)),
        (4, dt.time(23, 59, 59, 999999)),
    ]
    cursor.executemany("INSERT INTO #tmany (id, t) VALUES (?, ?)", values)
    cursor.execute("SELECT t FROM #tmany ORDER BY t")
    assert [r[0] for r in cursor.fetchall()] == [
        dt.time(0, 0, 0, 0),
        dt.time(0, 0, 0, 1),
        dt.time(12, 30, 0, 250000),
        dt.time(23, 59, 59, 999999),
    ]


def test_time_param_into_datetime2_column_gets_1900_01_01(cursor):
    # A bare time has no date; SQL Server supplies 1900-01-01 for datetime2.
    cursor.execute("CREATE TABLE #tdt (d datetime2(7) NOT NULL)")
    cursor.execute("INSERT INTO #tdt (d) VALUES (?)", (dt.time(10, 30, 0, 123456),))
    cursor.execute("SELECT d FROM #tdt")
    value = cursor.fetchone()[0]
    assert type(value) is dt.datetime
    assert value == dt.datetime(1900, 1, 1, 10, 30, 0, 123456)


def test_time_param_into_datetime2_scale0_rounds_fraction(cursor):
    cursor.execute("SELECT CAST(? AS datetime2(0))", (dt.time(10, 30, 0, 600000),))
    value = cursor.fetchone()[0]
    assert type(value) is dt.datetime
    assert value == dt.datetime(1900, 1, 1, 10, 30, 1)


def test_time_comparison_in_where_clause_matches_exact_value(cursor):
    cursor.execute("CREATE TABLE #twhere (id int NOT NULL, t time(7) NOT NULL)")
    cursor.executemany(
        "INSERT INTO #twhere (id, t) VALUES (?, ?)",
        [(1, dt.time(9, 0, 0, 0)), (2, dt.time(9, 0, 0, 1)), (3, dt.time(9, 0, 0, 2))],
    )
    cursor.execute("SELECT id FROM #twhere WHERE t = ?", (dt.time(9, 0, 0, 1),))
    assert [r[0] for r in cursor.fetchall()] == [2]


def test_time7_values_differing_only_at_100ns_are_distinct_on_server_but_equal_in_python(cursor):
    # Documents the precision gap: the server sees two distinct time(7)
    # values, but both fetch back as the same datetime.time.
    cursor.execute(
        "SELECT CAST('12:00:00.1234561' AS time(7)) AS a, CAST('12:00:00.1234562' AS time(7)) AS b"
    )
    a, b = cursor.fetchone()
    assert a == b == dt.time(12, 0, 0, 123456)
    cursor.execute(
        "SELECT COUNT(DISTINCT t) FROM (SELECT CAST('12:00:00.1234561' AS time(7)) AS t "
        "UNION ALL SELECT CAST('12:00:00.1234562' AS time(7))) x"
    )
    assert cursor.fetchone()[0] == 2


def test_timedelta_param_is_rejected_with_typeerror(cursor):
    # timedelta has no SQL Server time mapping in this driver; it must fail
    # loudly rather than be silently converted or sent as a string.
    with pytest.raises(TypeError):
        cursor.execute("SELECT ?", (dt.timedelta(hours=1),))


def test_timedelta_param_does_not_insert_row_into_time_column(cursor):
    cursor.execute("CREATE TABLE #ttd (t time(7) NULL)")
    with pytest.raises(TypeError):
        cursor.execute("INSERT INTO #ttd (t) VALUES (?)", (dt.timedelta(seconds=5),))
    cursor.execute("SELECT COUNT(*) FROM #ttd")
    assert cursor.fetchone()[0] == 0


def test_connection_usable_after_timedelta_rejection(cursor):
    with pytest.raises(TypeError):
        cursor.execute("SELECT ?", (dt.timedelta(minutes=1),))
    cursor.execute("SELECT CAST(? AS time(7))", (dt.time(7, 0, 0, 1),))
    assert cursor.fetchone()[0] == dt.time(7, 0, 0, 1)
