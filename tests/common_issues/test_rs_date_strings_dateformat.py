"""String-to-date conversion under DATEFORMAT / LANGUAGE, ISO 8601, CONVERT, TRY_CONVERT.

Covers how SQL Server interprets character strings as dates/times and how the
driver must bind Python ``str`` parameters into date-typed columns:

* ISO 8601 ``yyyy-mm-ddThh:mm:ss[.fff][Z]`` is language-independent (T-form),
  while dashed ``yyyy-mm-dd`` is interpreted by DATEFORMAT / SET LANGUAGE for
  the legacy ``datetime`` type (German reads ``2024-03-05`` as 2024-05-03).
* ``SET DATEFORMAT`` and ``SET LANGUAGE`` are connection-scoped session state
  that persists across statements and commits, and resets on a new connection.
* ``CONVERT`` with explicit styles (23, 104, 105, 126, 127) is the
  language-independent escape hatch.
* ``TRY_CONVERT`` returns NULL for unparsable or impossible dates instead of
  raising error 241/242.
* A Python ``str`` bound as a parameter goes through the server's string-to-date
  rules, so it follows DATEFORMAT; a Python ``date`` does not.

Sources:
* https://learn.microsoft.com/en-us/sql/t-sql/statements/set-dateformat-transact-sql
* https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2005/ms190977(v=sql.90)  (ISO 8601 and CONVERT styles)
* https://learn.microsoft.com/en-us/sql/t-sql/functions/try-convert-transact-sql
* https://github.com/sqlalchemy/sqlalchemy/issues/6366  (tz-aware datetime fails to insert, error 22007/241)
* https://github.com/sqlalchemy/sqlalchemy/issues/7660  (tz-aware datetime fails on UPDATE, 241)
* https://github.com/sqlalchemy/sqlalchemy/discussions/12082  (CONVERT(datetime, ?) with tz-aware value, 22007)
* https://github.com/pandas-dev/pandas/issues/47874  (to_sql string-to-date conversion error 22007/241)

Note: searches of the microsoft/mssql-python issue tracker for DATEFORMAT or
string-to-date problems returned no matching issues; the failure modes above
come from pyodbc/SQLAlchemy/pandas reports and the SQL Server docs.

Every assertion reflects what the upstream ODBC driver returns today, which
matches SQL Server's documented behavior.
"""

import datetime as dt

import pytest
from mssql_python import DataError

# Session settings applied before each probe. The German setting reads
# dotted/dashed strings day-first; us_english and mdy/ymd read slashed strings
# month-first, so the same literal lands on different dates.
LANGUAGE_SETTINGS = [
    "SET LANGUAGE us_english",
    "SET LANGUAGE German",
    "SET DATEFORMAT dmy",
    "SET DATEFORMAT mdy",
    "SET DATEFORMAT ymd",
]


def _exec(cursor, sql):
    """Run a SET statement; it returns no result set, so do not fetch."""
    cursor.execute(sql)


def _scalar(cursor, sql, params=()):
    cursor.execute(sql, params) if params else cursor.execute(sql)
    return cursor.fetchone()[0]


def test_iso8601_t_form_is_language_independent(cursor):
    for setting in LANGUAGE_SETTINGS:
        _exec(cursor, setting)
        value = _scalar(cursor, "SELECT CAST('2024-03-05T10:20:30' AS datetime2)")
        assert type(value) is dt.datetime, setting
        assert value == dt.datetime(2024, 3, 5, 10, 20, 30), setting


def test_iso8601_t_form_with_z_suffix_drops_zone_without_shifting(cursor):
    # The Z designator is ignored for datetime2: no UTC conversion, no shift.
    value = _scalar(cursor, "SELECT CAST('2024-03-05T10:20:30Z' AS datetime2)")
    assert type(value) is dt.datetime
    assert value == dt.datetime(2024, 3, 5, 10, 20, 30)
    assert value.tzinfo is None


def test_convert_style_127_parses_fraction_and_z(cursor):
    value = _scalar(
        cursor, "SELECT CONVERT(datetime2(3), '2024-03-05T10:20:30.123Z', 127)"
    )
    assert type(value) is dt.datetime
    assert value == dt.datetime(2024, 3, 5, 10, 20, 30, 123000)


def test_convert_style_126_parses_iso_t_without_zone(cursor):
    value = _scalar(
        cursor, "SELECT CONVERT(datetime2(3), '2024-03-05T10:20:30.123', 126)"
    )
    assert value == dt.datetime(2024, 3, 5, 10, 20, 30, 123000)


def test_iso8601_t_form_without_seconds_is_rejected(cursor):
    # 'hh:mm' with no seconds is not an accepted ISO 8601 layout for datetime.
    with pytest.raises(DataError):
        _scalar(cursor, "SELECT CAST('2024-03-05T10:20' AS datetime)")


def test_dateformat_dmy_reads_slashed_string_day_first(cursor):
    _exec(cursor, "SET DATEFORMAT dmy")
    value = _scalar(cursor, "SELECT CAST('05/03/2024' AS date)")
    assert type(value) is dt.date
    assert value == dt.date(2024, 3, 5)


def test_dateformat_mdy_reads_slashed_string_month_first(cursor):
    _exec(cursor, "SET DATEFORMAT mdy")
    value = _scalar(cursor, "SELECT CAST('05/03/2024' AS date)")
    assert value == dt.date(2024, 5, 3)


def test_dateformat_ymd_reads_slashed_string_month_first(cursor):
    # ymd-ordering still reads slashed 05/03/2024 as month-first in this server.
    _exec(cursor, "SET DATEFORMAT ymd")
    value = _scalar(cursor, "SELECT CAST('05/03/2024' AS date)")
    assert value == dt.date(2024, 5, 3)


def test_set_language_german_reads_dotted_string_day_first(cursor):
    _exec(cursor, "SET LANGUAGE German")
    assert _scalar(cursor, "SELECT @@LANGUAGE") == "Deutsch"
    assert _scalar(cursor, "SELECT CAST('05.03.2024' AS date)") == dt.date(2024, 3, 5)
    # Same slashed literal that us_english reads as May 3 is day-first here.
    assert _scalar(cursor, "SELECT CAST('03/05/2024' AS date)") == dt.date(2024, 5, 3)


def test_german_dashed_ymd_is_misread_for_legacy_datetime(cursor):
    # Dashed yyyy-mm-dd on the legacy datetime type is read as yyyy-dd-mm under
    # German, while datetime2 reads the same string correctly.
    _exec(cursor, "SET LANGUAGE German")
    legacy = _scalar(cursor, "SELECT CAST('2024-03-05' AS datetime)")
    modern = _scalar(cursor, "SELECT CAST('2024-03-05' AS datetime2)")
    assert legacy == dt.datetime(2024, 5, 3, 0, 0, 0)
    assert modern == dt.datetime(2024, 3, 5, 0, 0, 0)


def test_language_setting_is_session_scoped_not_global(conn_str):
    from mssql_python import connect

    first = connect(conn_str)
    try:
        cur = first.cursor()
        _exec(cur, "SET LANGUAGE German")
        assert _scalar(cur, "SELECT @@LANGUAGE") == "Deutsch"
    finally:
        first.close()

    second = connect(conn_str)
    try:
        cur = second.cursor()
        # A fresh connection starts with the server default, so 05/03 is month-first.
        assert _scalar(cur, "SELECT @@LANGUAGE") == "us_english"
        assert _scalar(cur, "SELECT CAST('05/03/2024' AS date)") == dt.date(2024, 5, 3)
    finally:
        second.close()


def test_dateformat_survives_commit_on_same_connection(conn, cursor):
    _exec(cursor, "SET DATEFORMAT dmy")
    conn.commit()
    value = _scalar(cursor, "SELECT CAST('05/03/2024' AS date)")
    assert value == dt.date(2024, 3, 5)


def test_convert_styles_23_and_104_are_language_independent(cursor):
    for setting in LANGUAGE_SETTINGS:
        _exec(cursor, setting)
        assert _scalar(cursor, "SELECT CONVERT(date, '2024-03-05', 23)") == dt.date(2024, 3, 5), setting
        assert _scalar(cursor, "SELECT CONVERT(date, '05.03.2024', 104)") == dt.date(2024, 3, 5), setting


def test_convert_style_105_dashed_dmy_reads_day_first(cursor):
    _exec(cursor, "SET LANGUAGE us_english")
    assert _scalar(cursor, "SELECT CONVERT(date, '05-03-2024', 105)") == dt.date(2024, 3, 5)


def test_try_convert_unparsable_string_returns_none(cursor):
    _exec(cursor, "SET LANGUAGE German")
    value = _scalar(cursor, "SELECT TRY_CONVERT(date, 'not a date')")
    assert value is None


def test_try_convert_impossible_calendar_date_returns_none(cursor):
    # 2024 is a leap year, but February never has 30 days.
    value = _scalar(cursor, "SELECT TRY_CONVERT(date, '2024-02-30')")
    assert value is None


def test_try_convert_null_input_returns_none(cursor):
    assert _scalar(cursor, "SELECT TRY_CONVERT(date, NULL)") is None


def test_try_convert_valid_iso_returns_typed_value(cursor):
    day = _scalar(cursor, "SELECT TRY_CONVERT(date, '2024-03-05')")
    assert type(day) is dt.date
    assert day == dt.date(2024, 3, 5)
    stamp = _scalar(cursor, "SELECT TRY_CONVERT(datetime2(3), '2024-03-05T10:20:30.123', 126)")
    assert type(stamp) is dt.datetime
    assert stamp == dt.datetime(2024, 3, 5, 10, 20, 30, 123000)


def test_try_convert_ambiguous_slash_follows_language(cursor):
    _exec(cursor, "SET LANGUAGE German")
    assert _scalar(cursor, "SELECT TRY_CONVERT(date, '02/03/2024')") == dt.date(2024, 3, 2)
    _exec(cursor, "SET LANGUAGE us_english")
    assert _scalar(cursor, "SELECT TRY_CONVERT(date, '02/03/2024')") == dt.date(2024, 2, 3)


def test_param_python_str_iso_t_into_date_column_drops_time(cursor):
    cursor.execute("CREATE TABLE #d (d date NULL)")
    cursor.execute("INSERT INTO #d (d) VALUES (?)", ("2024-03-05T10:20:30",))
    assert _scalar(cursor, "SELECT d FROM #d") == dt.date(2024, 3, 5)


def test_param_python_str_into_date_column_follows_dateformat(cursor):
    cases = [
        ("SET DATEFORMAT dmy", "05/03/2024", dt.date(2024, 3, 5)),
        ("SET DATEFORMAT mdy", "05/03/2024", dt.date(2024, 5, 3)),
        ("SET LANGUAGE German", "05.03.2024", dt.date(2024, 3, 5)),
    ]
    for setting, text, expected in cases:
        _exec(cursor, setting)
        cursor.execute("IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d")
        cursor.execute("CREATE TABLE #d (d date NULL)")
        cursor.execute("INSERT INTO #d (d) VALUES (?)", (text,))
        assert _scalar(cursor, "SELECT d FROM #d") == expected, setting


def test_param_python_date_is_not_affected_by_german_language(cursor):
    # A typed date parameter bypasses string parsing, so German cannot flip it.
    _exec(cursor, "SET LANGUAGE German")
    cursor.execute("CREATE TABLE #x (v datetime NULL)")
    cursor.execute("INSERT INTO #x (v) VALUES (?)", (dt.date(2024, 3, 5),))
    cursor.execute("SELECT v FROM #x")
    assert cursor.fetchone()[0] == dt.datetime(2024, 3, 5, 0, 0, 0)


def test_param_python_str_dashed_ymd_into_legacy_datetime_is_misread_under_german(cursor):
    # The same ISO-looking string, sent as str, takes the German misread path.
    _exec(cursor, "SET LANGUAGE German")
    cursor.execute("CREATE TABLE #x (v datetime NULL)")
    cursor.execute("INSERT INTO #x (v) VALUES (?)", ("2024-03-05",))
    cursor.execute("SELECT v FROM #x")
    assert cursor.fetchone()[0] == dt.datetime(2024, 5, 3, 0, 0, 0)


def test_param_python_str_invalid_date_raises_dataerror(cursor):
    cursor.execute("CREATE TABLE #d (d date NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #d (d) VALUES (?)", ("2024-02-30",))
