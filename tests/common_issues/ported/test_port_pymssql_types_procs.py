"""Type fidelity, stored procedure outputs and repeated failed logins.

Test ideas inspired by pymssql's type, stored procedure and connection tests
(pymssql/pymssql tests/test_types.py, tests/test_sprocs.py and
tests/test_connections.py, LGPL-2.1). No code or test text was copied: these
cases were re-derived from the behaviours they check and are written fresh
against mssql_python.
"""

import datetime
import decimal
import re
import uuid
from decimal import Decimal

import pytest

from mssql_python import connect
from mssql_python.exceptions import OperationalError


def _drop_proc(cursor, name):
    cursor.execute(f"IF OBJECT_ID('dbo.{name}', 'P') IS NOT NULL DROP PROCEDURE dbo.{name}")
    cursor.connection.commit()


def test_decimal_context_precision_is_unchanged_by_roundtrip(cursor, table_name):
    before = decimal.getcontext().prec
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v NUMERIC(38, 2) NULL)")
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", (Decimal("1.2345"),))
    cursor.execute(f"SELECT v FROM dbo.{table_name}")
    cursor.fetchone()
    assert decimal.getcontext().prec == before


def test_multirow_values_keeps_each_decimal_exact(cursor):
    # A tiny negative Decimal stringifies as a float literal (-1E-18) in some
    # clients; in a multi-row VALUES list that would widen the whole column.
    big = Decimal("12345678901234567890.123456789012345678")
    small = Decimal("-0.000000000000000001")
    cursor.execute("SELECT v FROM (VALUES (?), (?)) AS t(v) ORDER BY v DESC", (big, small))
    assert [tuple(r)[0] for r in cursor.fetchall()] == [big, small]


def test_varchar_hex_looking_text_stays_text(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v VARCHAR(20) NULL)")
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", ("0xf00",))
    cursor.execute(f"SELECT v FROM dbo.{table_name}")
    value = cursor.fetchone()[0]
    assert value == "0xf00"
    assert isinstance(value, str)


@pytest.mark.upstream_bug(
    reason="upstream rejects DATE values before year 1753 client-side; SQL Server DATE covers 0001-9999"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_date_column_keeps_ancient_year_thirteen(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (d DATE NULL)")
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (d) VALUES (?)", (datetime.date(13, 1, 2),))
    cursor.execute(f"SELECT d FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == datetime.date(13, 1, 2)


def test_uuid_string_and_uuid_object_compare_equal_in_result(cursor, table_name):
    value = uuid.uuid4()
    cursor.execute(f"CREATE TABLE dbo.{table_name} (g UNIQUEIDENTIFIER NULL)")
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (g) VALUES (?)", (str(value),))
    cursor.execute(f"SELECT g FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == value


def test_stored_procedure_null_outputs_come_back_as_none(cursor):
    name = f"ci_null_{uuid.uuid4().hex[:10]}"
    _drop_proc(cursor, name)
    try:
        cursor.execute(
            f"CREATE PROCEDURE dbo.{name} @out INT OUTPUT AS "
            "BEGIN SET @out = NULL; SELECT CAST(NULL AS NVARCHAR(10)) AS s, NULL AS n END"
        )
        cursor.connection.commit()
        cursor.execute(f"DECLARE @o INT = 5; EXEC dbo.{name} @out = @o OUTPUT; SELECT @o AS o")
        # The procedure's own SELECT is the first result set; the output value follows it.
        cursor.fetchall()
        assert cursor.nextset()
        assert tuple(cursor.fetchone()) == (None,)
    finally:
        _drop_proc(cursor, name)


def test_stored_procedure_null_result_columns_come_back_as_none(cursor):
    name = f"ci_nullset_{uuid.uuid4().hex[:10]}"
    _drop_proc(cursor, name)
    try:
        cursor.execute(
            f"CREATE PROCEDURE dbo.{name} AS "
            "SELECT CAST(NULL AS NVARCHAR(10)) AS s, NULL AS n, CAST(NULL AS DATETIME2) AS d"
        )
        cursor.connection.commit()
        cursor.execute(f"EXEC dbo.{name}")
        assert tuple(cursor.fetchone()) == (None, None, None)
    finally:
        _drop_proc(cursor, name)


def test_real_and_float_values_from_procedure_keep_their_precision(cursor):
    name = f"ci_float_{uuid.uuid4().hex[:10]}"
    _drop_proc(cursor, name)
    try:
        cursor.execute(
            f"CREATE PROCEDURE dbo.{name} AS "
            "SELECT CAST(1.5 AS REAL) AS r, CAST(2.25 AS FLOAT) AS f, "
            "CAST(0.1 AS FLOAT) AS tenth"
        )
        cursor.connection.commit()
        cursor.execute(f"EXEC dbo.{name}")
        r, f, tenth = tuple(cursor.fetchone())
        assert (r, f) == (1.5, 2.25)
        assert isinstance(f, float)
        assert tenth == 0.1
    finally:
        _drop_proc(cursor, name)


def test_repeated_failed_logins_do_not_block_later_good_connect(conn_str):
    bad = re.sub(r"Database=[^;]*", "Database=ci_missing_db_repeat", conn_str, flags=re.I)
    if bad == conn_str:
        bad = conn_str + ";Database=ci_missing_db_repeat"
    for _ in range(5):
        with pytest.raises(OperationalError):
            connect(bad, timeout=5)
    good = connect(conn_str)
    try:
        cursor = good.cursor()
        cursor.execute("SELECT 42")
        assert cursor.fetchone()[0] == 42
    finally:
        good.close()
