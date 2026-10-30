"""Legacy datetime / smalldatetime rounding, range, and comparison pitfalls.

Research sources (issue trackers and SQL Server docs):
- https://github.com/microsoft/mssql-python/issues/869 (datetime2 fraction handling on the bulkcopy string path)
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/datetime-handling?view=sql-server-ver17
- https://github.com/hugr-lab/mssql-extension/issues/426 (WHERE dt = <value read from a datetime column> misses rows when compared as datetime2)
- https://gist.github.com/chadbaldwin/666a05304245ce201f7005105af15924 (datetime vs datetime2 comparison oddities)
- https://learn.microsoft.com/en-us/answers/questions/574416/select-gives-wrong-result-in-case-of-comparing-dat (datetime vs datetime2 comparison gives wrong result)
- https://www.sql.kiwi/2024/08/dont-mix-with-datetime/ (Paul White on mixing datetime and datetime2)
- https://github.com/pymssql/pymssql/pull/665 (rounding of datetime's 1/300 s ticks to milliseconds)
- https://github.com/mkleehammer/pyodbc/issues/235 (milliseconds dropped from datetime values)

Facts asserted here:
- datetime stores 1/300 s ticks. Values round to .000 / .003 / .007 / ... / .997,
  so a Python microsecond value does not survive a round trip.
- smalldatetime keeps whole minutes; seconds >= 30 round up, below 30 truncate.
- datetime range is 1753-01-01 .. 9999-12-31 and smalldatetime is 1900-01-01 .. 2079-06-06.
  Values outside those ranges (or rounded outside them) raise DataError.
- Comparing a datetime column with a datetime2 parameter (the default binding for
  Python datetime) compares at full precision, so a value read back from the column
  does not match itself. Casting the parameter to datetime makes equality work.
"""

import datetime as dt

import pytest
from mssql_python import DataError

# datetime tick milliseconds within one second: round(k * 10 / 3) for k in 0..299.
DATETIME_TICK_MS = {(k * 10 + 1) // 3 for k in range(300)}


def _cast_scalar(cursor, value, sql_type):
    cursor.execute(f"SELECT CAST(? AS {sql_type})", (value,))
    return cursor.fetchone()[0]


def _count(cursor, sql, *params):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


@pytest.mark.parametrize(
    "micro, expected",
    [
        (0, dt.datetime(2024, 1, 1, 10, 0, 0, 0)),
        (1000, dt.datetime(2024, 1, 1, 10, 0, 0, 0)),  # .001 -> .000
        (2000, dt.datetime(2024, 1, 1, 10, 0, 0, 3000)),  # .002 -> .003
        (4000, dt.datetime(2024, 1, 1, 10, 0, 0, 3000)),  # .004 -> .003
        (5000, dt.datetime(2024, 1, 1, 10, 0, 0, 7000)),  # .005 -> .007
        (6000, dt.datetime(2024, 1, 1, 10, 0, 0, 7000)),  # .006 -> .007
        (996000, dt.datetime(2024, 1, 1, 10, 0, 0, 997000)),  # .996 -> .997
        (998000, dt.datetime(2024, 1, 1, 10, 0, 0, 997000)),  # .998 -> .997
        (999000, dt.datetime(2024, 1, 1, 10, 0, 1, 0)),  # .999 -> next second
    ],
)
def test_datetime_insert_rounds_microseconds_to_tick(cursor, micro, expected):
    cursor.execute("CREATE TABLE #dt_round (dt datetime NOT NULL)")
    cursor.execute(
        "INSERT INTO #dt_round (dt) VALUES (?)", (dt.datetime(2024, 1, 1, 10, 0, 0, micro),)
    )
    cursor.execute("SELECT dt FROM #dt_round")
    value = cursor.fetchone()[0]
    assert type(value) is dt.datetime
    assert value == expected


def test_datetime_readback_only_produces_tick_values(cursor):
    values = [dt.datetime(2024, 1, 1, 10, 0, 0, us) for us in range(0, 1_000_000, 1000)]
    cursor.execute("CREATE TABLE #dt_ticks (id int IDENTITY PRIMARY KEY, dt datetime NOT NULL)")
    cursor.executemany("INSERT INTO #dt_ticks (dt) VALUES (?)", [(v,) for v in values])
    cursor.execute("SELECT dt FROM #dt_ticks ORDER BY id")
    rows = [r[0] for r in cursor.fetchall()]
    assert len(rows) == len(values)
    for row in rows:
        assert type(row) is dt.datetime
        assert row.microsecond % 1000 == 0
        assert row.microsecond // 1000 in DATETIME_TICK_MS


@pytest.mark.parametrize(
    "literal, expected",
    [
        ("2024-01-02 03:04:05.001", dt.datetime(2024, 1, 2, 3, 4, 5, 0)),
        ("2024-01-02 03:04:05.002", dt.datetime(2024, 1, 2, 3, 4, 5, 3000)),
        ("2024-01-02 03:04:05.005", dt.datetime(2024, 1, 2, 3, 4, 5, 7000)),
        ("2024-01-02 03:04:05.998", dt.datetime(2024, 1, 2, 3, 4, 5, 997000)),
    ],
)
def test_datetime_string_literal_rounds_to_tick(cursor, literal, expected):
    cursor.execute(f"SELECT CAST('{literal}' AS datetime)")
    assert cursor.fetchone()[0] == expected


def test_datetime_and_datetime2_same_input_store_different_precision(cursor):
    value = dt.datetime(2024, 1, 1, 10, 0, 0, 2000)
    cursor.execute(
        "CREATE TABLE #dt_vs_d2 (dt datetime NOT NULL, d2 datetime2(7) NOT NULL)"
    )
    cursor.execute("INSERT INTO #dt_vs_d2 (dt, d2) VALUES (?, ?)", (value, value))
    cursor.execute("SELECT dt, d2 FROM #dt_vs_d2")
    dt_value, d2_value = cursor.fetchone()
    assert dt_value == dt.datetime(2024, 1, 1, 10, 0, 0, 3000)
    assert d2_value == value


def test_datetime_whole_second_matches_datetime2_parameter(cursor):
    cursor.execute("CREATE TABLE #dt_eq_whole (dt datetime NOT NULL)")
    cursor.execute(
        "INSERT INTO #dt_eq_whole (dt) VALUES (?)", (dt.datetime(2024, 1, 1, 10, 0, 0),)
    )
    assert _count(cursor, "SELECT COUNT(*) FROM #dt_eq_whole WHERE dt = ?", dt.datetime(2024, 1, 1, 10, 0, 0)) == 1


def test_datetime_readback_value_does_not_match_datetime2_parameter(cursor):
    # Classic pitfall: the row contains 10:00:00.003 (stored tick), but a
    # datetime2 parameter is compared at full precision, so the row is not found.
    cursor.execute("CREATE TABLE #dt_eq_gotcha (dt datetime NOT NULL)")
    cursor.execute(
        "INSERT INTO #dt_eq_gotcha (dt) VALUES (?)", (dt.datetime(2024, 1, 1, 10, 0, 0, 2000),)
    )
    cursor.execute("SELECT dt FROM #dt_eq_gotcha")
    readback = cursor.fetchone()[0]
    assert readback == dt.datetime(2024, 1, 1, 10, 0, 0, 3000)
    assert _count(cursor, "SELECT COUNT(*) FROM #dt_eq_gotcha WHERE dt = ?", readback) == 0


def test_cast_parameter_to_datetime_matches_datetime_readback(cursor):
    cursor.execute("CREATE TABLE #dt_eq_cast (dt datetime NOT NULL)")
    cursor.execute(
        "INSERT INTO #dt_eq_cast (dt) VALUES (?)", (dt.datetime(2024, 1, 1, 10, 0, 0, 2000),)
    )
    cursor.execute("SELECT dt FROM #dt_eq_cast")
    readback = cursor.fetchone()[0]
    assert _count(cursor, "SELECT COUNT(*) FROM #dt_eq_cast WHERE dt = CAST(? AS datetime)", readback) == 1


@pytest.mark.parametrize(
    "value",
    [
        dt.datetime(1752, 1, 1),
        dt.datetime(1752, 12, 31, 23, 59, 59),
        dt.datetime(1, 1, 1),
    ],
)
def test_datetime_insert_before_1753_raises_dataerror(cursor, value):
    cursor.execute("CREATE TABLE #dt_range_low (dt datetime NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #dt_range_low (dt) VALUES (?)", (value,))
    assert _count(cursor, "SELECT COUNT(*) FROM #dt_range_low") == 0


def test_datetime_rounding_up_into_1753_is_accepted(cursor):
    # 1752-12-31 23:59:59.999 rounds up to 1753-01-01 00:00:00, which is in range.
    result = _cast_scalar(cursor, dt.datetime(1752, 12, 31, 23, 59, 59, 999000), "datetime")
    assert result == dt.datetime(1753, 1, 1, 0, 0, 0)


def test_datetime2_roundtrips_full_python_range(cursor):
    low = dt.datetime(1, 1, 1, 0, 0, 0)
    high = dt.datetime(9999, 12, 31, 23, 59, 59, 999999)
    assert _cast_scalar(cursor, low, "datetime2(7)") == low
    assert _cast_scalar(cursor, high, "datetime2(7)") == high


def test_datetime_max_is_9999_12_31_23_59_59_997(cursor):
    assert _cast_scalar(cursor, dt.datetime(9999, 12, 31, 23, 59, 59, 997000), "datetime") == (
        dt.datetime(9999, 12, 31, 23, 59, 59, 997000)
    )


def test_datetime_9999_fraction_clamps_to_last_tick(cursor):
    # .999 is clamped to the last datetime tick (.997), not overflowed to 10000.
    assert _cast_scalar(cursor, dt.datetime(9999, 12, 31, 23, 59, 59, 999000), "datetime") == (
        dt.datetime(9999, 12, 31, 23, 59, 59, 997000)
    )


def test_smalldatetime_9999_raises_dataerror(cursor):
    # smalldatetime tops out at 2079-06-06, so 9999 is out of range.
    with pytest.raises(DataError):
        _cast_scalar(cursor, dt.datetime(9999, 12, 31, 23, 59, 59), "smalldatetime")


@pytest.mark.parametrize(
    "second, micro, expected",
    [
        (29, 998000, dt.datetime(2024, 1, 1, 10, 0, 0)),  # 29.998 s truncates
        (29, 999999, dt.datetime(2024, 1, 1, 10, 0, 0)),
        (30, 0, dt.datetime(2024, 1, 1, 10, 1, 0)),  # exactly 30 s rounds up
        (59, 999999, dt.datetime(2024, 1, 1, 10, 1, 0)),
    ],
)
def test_smalldatetime_rounds_to_minute(cursor, second, micro, expected):
    result = _cast_scalar(cursor, dt.datetime(2024, 1, 1, 10, 0, second, micro), "smalldatetime")
    assert type(result) is dt.datetime
    assert result == expected


def test_smalldatetime_range_boundaries(cursor):
    assert _cast_scalar(cursor, dt.datetime(1899, 12, 31, 23, 59, 30), "smalldatetime") == (
        dt.datetime(1900, 1, 1, 0, 0, 0)
    )
    assert _cast_scalar(cursor, dt.datetime(2079, 6, 6, 23, 59, 29), "smalldatetime") == (
        dt.datetime(2079, 6, 6, 23, 59, 0)
    )
    with pytest.raises(DataError):
        _cast_scalar(cursor, dt.datetime(1899, 12, 31, 23, 59, 29), "smalldatetime")
    with pytest.raises(DataError):
        _cast_scalar(cursor, dt.datetime(2079, 6, 6, 23, 59, 30), "smalldatetime")


def test_datetime_and_smalldatetime_executemany_rounding_and_null(cursor):
    cursor.execute(
        "CREATE TABLE #dt_batch (id int IDENTITY PRIMARY KEY, dt datetime NULL, sdt smalldatetime NULL)"
    )
    cursor.executemany(
        "INSERT INTO #dt_batch (dt, sdt) VALUES (?, ?)",
        [
            (dt.datetime(2024, 1, 1, 10, 0, 0, 0), dt.datetime(2024, 1, 1, 10, 0, 0)),
            (dt.datetime(2024, 1, 1, 10, 0, 0, 2000), None),
            (dt.datetime(2024, 1, 1, 10, 0, 0, 5000), dt.datetime(2024, 1, 1, 10, 0, 30)),
        ],
    )
    cursor.execute("SELECT dt, sdt FROM #dt_batch ORDER BY id")
    rows = [tuple(r) for r in cursor.fetchall()]
    assert len(rows) == 3
    assert rows[0] == (dt.datetime(2024, 1, 1, 10, 0, 0), dt.datetime(2024, 1, 1, 10, 0))
    assert rows[1][0] == dt.datetime(2024, 1, 1, 10, 0, 0, 3000)
    assert rows[1][1] is None
    assert rows[2][0] == dt.datetime(2024, 1, 1, 10, 0, 0, 7000)
    assert rows[2][1] == dt.datetime(2024, 1, 1, 10, 1, 0)


def test_fetched_datetime_values_are_naive(cursor):
    cursor.execute(
        "SELECT CAST('2024-01-01 10:00:00.5' AS datetime), CAST('2024-01-01 10:00' AS smalldatetime)"
    )
    dt_value, sdt_value = cursor.fetchone()
    assert type(dt_value) is dt.datetime and dt_value.tzinfo is None
    assert type(sdt_value) is dt.datetime and sdt_value.tzinfo is None
