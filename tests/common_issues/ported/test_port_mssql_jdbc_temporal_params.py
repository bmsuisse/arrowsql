# Adapted from microsoft/mssql-jdbc (MIT), Copyright (c) Microsoft Corporation.
#   src/test/java/com/microsoft/sqlserver/jdbc/datatypes/DateAndTimeTypeTest.java
#   src/test/java/com/microsoft/sqlserver/jdbc/datatypes/DataTypesTest.java
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the "Software"), to deal
#   in the Software without restriction, including without limitation the rights
#   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#   copies of the Software, and to permit persons to whom the Software is
#   furnished to do so, subject to the following conditions:
#   The above copyright notice and this permission notice shall be included in all
#   copies or substantial portions of the Software.
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#
# Behaviour ported from the JDBC date/time parameter tests: date, time and
# timestamp parameters must match the same-typed column in WHERE clauses, keep
# their precision when the server formats them, and survive range edges and NULLs.
# The JDBC sendTimeAsDatetime / datetimeParameterType connection options have no
# mssql_python equivalent and are dropped. Ground truth is the upstream ODBC
# driver (.venv-upstream).
import datetime as dt

import pytest


def _create(conn, table, ddl):
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table} ({ddl})")
    conn.commit()
    cur.close()


def test_date_param_matches_date_column_in_where_clause(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, d DATE NOT NULL")
    value = dt.date(2010, 2, 1)
    cursor.execute(f"INSERT INTO dbo.{table_name} (d) VALUES (?)", [value])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE d = ?", [value])
    assert cursor.fetchone()[0] == 1


def test_time_param_with_fraction_matches_time7_column(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, t TIME(7) NOT NULL")
    value = dt.time(12, 30, 1, 500000)
    cursor.execute(f"INSERT INTO dbo.{table_name} (t) VALUES (?)", [value])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE t = ?", [value])
    assert cursor.fetchone()[0] == 1


def test_datetime2_param_matches_column_value_with_microseconds(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, ts DATETIME2(6) NOT NULL")
    value = dt.datetime(2010, 2, 2, 23, 59, 59, 123456)
    cursor.execute(f"INSERT INTO dbo.{table_name} (ts) VALUES (?)", [value])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE ts = ?", [value])
    assert cursor.fetchone()[0] == 1


def test_datetime_param_matches_datetime_column_with_half_second(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, dt DATETIME NOT NULL")
    value = dt.datetime(2010, 2, 1, 12, 0, 0, 500000)
    cursor.execute(f"INSERT INTO dbo.{table_name} (dt) VALUES (?)", [value])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE dt = ?", [value])
    assert cursor.fetchone()[0] == 1


def test_datetime2_param_is_formatted_by_server_with_seven_fraction_digits(cursor):
    # The server, not the client, decides the 100 ns digits of a datetime2 value.
    cursor.execute(
        "SELECT CONVERT(VARCHAR(40), ?, 126)",
        [dt.datetime(2010, 2, 2, 23, 59, 59, 123456)],
    )
    assert cursor.fetchone()[0] == "2010-02-02T23:59:59.1234560"


def test_time_param_is_formatted_by_server_with_seven_fraction_digits(cursor):
    cursor.execute("SELECT CONVERT(VARCHAR(16), CAST(? AS TIME(7)), 114)", [dt.time(12, 30, 1, 500000)])
    assert cursor.fetchone()[0] == "12:30:01.5000000"


def test_date_param_into_datetime2_column_is_midnight(cursor):
    cursor.execute("SELECT CAST(? AS DATETIME2(7))", [dt.date(2010, 2, 1)])
    assert cursor.fetchone()[0] == dt.datetime(2010, 2, 1, 0, 0)


def test_time_last_representable_microsecond_roundtrips_through_time7(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, t TIME(7) NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (t) VALUES (?)", [dt.time(23, 59, 59, 999999)])
    cursor.execute(f"SELECT t FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == dt.time(23, 59, 59, 999999)


def test_midnight_time_and_date_are_stored_not_treated_as_null(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, t TIME(0) NULL, d DATE NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (t, d) VALUES (?, ?)", [dt.time(0, 0), dt.date(1900, 1, 1)])
    cursor.execute(f"SELECT t, d FROM dbo.{table_name}")
    row = tuple(cursor.fetchone())
    assert row == (dt.time(0, 0), dt.date(1900, 1, 1))


def test_date_min_and_max_roundtrip(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, d DATE NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (d) VALUES (?)", [dt.date(1753, 1, 1)])
    cursor.execute(f"INSERT INTO dbo.{table_name} (d) VALUES (?)", [dt.date(9999, 12, 31)])
    cursor.execute(f"SELECT d FROM dbo.{table_name} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [dt.date(1753, 1, 1), dt.date(9999, 12, 31)]


@pytest.mark.upstream_bug(reason="upstream rejects DATE 0001-01-01 client-side with a 1753-9999 range error")
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_date_0001_01_01_param_is_accepted(cursor):
    # DATE supports 0001-01-01..9999-12-31; the 1753 limit is DATETIME's.
    cursor.execute("SELECT CAST(? AS DATE)", [dt.date(1, 1, 1)])
    assert cursor.fetchone()[0] == dt.date(1, 1, 1)


def test_datetime2_min_and_max_roundtrip(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, ts DATETIME2(7) NOT NULL")
    lo = dt.datetime(1, 1, 1, 0, 0, 0)
    hi = dt.datetime(9999, 12, 31, 23, 59, 59, 999999)
    cursor.execute(f"INSERT INTO dbo.{table_name} (ts) VALUES (?)", [lo])
    cursor.execute(f"INSERT INTO dbo.{table_name} (ts) VALUES (?)", [hi])
    cursor.execute(f"SELECT ts FROM dbo.{table_name} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [lo, hi]


def test_smalldatetime_param_rounds_to_next_minute_boundary(cursor):
    cursor.execute("SELECT CAST(? AS SMALLDATETIME)", [dt.datetime(2010, 2, 1, 23, 59, 59)])
    assert cursor.fetchone()[0] == dt.datetime(2010, 2, 2, 0, 0)


def test_datetime_param_fraction_snaps_to_grid_without_rolling_the_day(cursor):
    # DATETIME keeps 1/300 s steps: .991 snaps down to .990 and stays on the same day.
    cursor.execute("SELECT CAST(? AS DATETIME)", [dt.datetime(2010, 2, 1, 23, 59, 59, 991000)])
    assert cursor.fetchone()[0] == dt.datetime(2010, 2, 1, 23, 59, 59, 990000)


def test_date_param_added_with_dateadd_uses_the_date_type(cursor):
    cursor.execute("SELECT DATEADD(day, ?, ?)", [3, dt.date(2010, 2, 1)])
    assert cursor.fetchone()[0] == dt.date(2010, 2, 4)


def test_null_temporal_params_into_each_temporal_column(conn, cursor, table_name):
    _create(
        conn,
        table_name,
        "id INT IDENTITY PRIMARY KEY, d DATE NULL, t TIME(7) NULL, dt DATETIME NULL, "
        "dt2 DATETIME2(7) NULL, sdt SMALLDATETIME NULL, dto DATETIMEOFFSET(7) NULL",
    )
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (d, t, dt, dt2, sdt, dto) VALUES (?, ?, ?, ?, ?, ?)",
        [None] * 6,
    )
    cursor.execute(f"SELECT d, t, dt, dt2, sdt, dto FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (None,) * 6


def test_datetime_param_into_time_column_keeps_only_the_time_part(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, t TIME(7) NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (t) VALUES (?)", [dt.datetime(2010, 2, 1, 13, 14, 15)])
    cursor.execute(f"SELECT t FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == dt.time(13, 14, 15)
