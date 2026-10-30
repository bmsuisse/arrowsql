# Adapted from microsoft/mssql-jdbc (MIT), Copyright (c) Microsoft Corporation.
#   src/test/java/com/microsoft/sqlserver/jdbc/datatypes/BigIntegerTest.java
#   src/test/java/com/microsoft/sqlserver/jdbc/datatypes/GuidTest.java
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
# Behaviour ported from the JDBC BigInteger and GUID tests: integers wider than
# BIGINT travel as text or exact numerics, extreme integers round-trip through
# every integer-like column, and UUIDs are bound and compared as uniqueidentifier.
# Python API replaces JDBC setObject(..., Types.X); the SQL type is forced with CAST
# or by the column type. Ground truth is the upstream ODBC driver (.venv-upstream).
import decimal
import uuid

import pytest
from mssql_python import Error

LONG_MAX = 9223372036854775807
LONG_MIN = -9223372036854775808
BIG_POS = "922337203685477580776767676"
BIG_NEG = "-922337203685477580776767676"


def _create(conn, table, ddl):
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table} ({ddl})")
    conn.commit()
    cur.close()


# --- integers ---------------------------------------------------------------


@pytest.mark.parametrize("value", [LONG_MAX, LONG_MIN, 0, 10, -10, 1000, -1000])
def test_extreme_bigint_values_roundtrip_through_bigint_column(conn, cursor, table_name, value):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, v BIGINT NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", [value])
    cursor.execute(f"SELECT v, CAST(v AS VARCHAR(30)) FROM dbo.{table_name}")
    got, text = cursor.fetchone()
    assert got == value
    assert text == str(value)


@pytest.mark.parametrize(
    "col_type, value",
    [
        ("INT", 2147483647),
        ("INT", -2147483648),
        ("SMALLINT", 32767),
        ("SMALLINT", -32768),
        ("TINYINT", 255),
        ("TINYINT", 0),
    ],
)
def test_integer_column_bounds_roundtrip_exactly(conn, cursor, table_name, col_type, value):
    _create(conn, table_name, f"id INT IDENTITY PRIMARY KEY, v {col_type} NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", [value])
    cursor.execute(f"SELECT v FROM dbo.{table_name} WHERE v = ?", [value])
    assert cursor.fetchone()[0] == value


def test_big_integer_text_into_varchar_char_and_max_keeps_every_digit(conn, cursor, table_name):
    _create(
        conn,
        table_name,
        "id INT IDENTITY PRIMARY KEY, c1 VARCHAR(100), c2 CHAR(100), c3 VARCHAR(MAX)",
    )
    for text in (BIG_POS, BIG_NEG):
        cursor.execute(
            f"INSERT INTO dbo.{table_name} (c1, c2, c3) VALUES (?, ?, ?)", [text, text, text]
        )
    cursor.execute(f"SELECT c1, RTRIM(c2), c3 FROM dbo.{table_name} ORDER BY id")
    rows = [tuple(r) for r in cursor.fetchall()]
    assert rows == [(BIG_POS, BIG_POS, BIG_POS), (BIG_NEG, BIG_NEG, BIG_NEG)]


def test_big_integer_as_text_into_numeric_38_0_is_exact(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, n NUMERIC(38,0) NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (n) VALUES (?)", [BIG_POS])
    cursor.execute(f"INSERT INTO dbo.{table_name} (n) VALUES (?)", [BIG_NEG])
    cursor.execute(f"SELECT n FROM dbo.{table_name} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [
        decimal.Decimal(BIG_POS),
        decimal.Decimal(BIG_NEG),
    ]


def test_big_decimal_param_into_numeric_38_0_matches_in_where_clause(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, n NUMERIC(38,0) NOT NULL")
    value = decimal.Decimal(BIG_NEG)
    cursor.execute(f"INSERT INTO dbo.{table_name} (n) VALUES (?)", [value])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE n = ?", [value])
    assert cursor.fetchone()[0] == 1


def test_big_integer_text_into_bigint_column_is_rejected_and_stores_nothing(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, v BIGINT NULL")
    with pytest.raises(Error):
        cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", [BIG_POS])
    conn.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


def test_python_int_beyond_bigint_is_rejected_client_side(cursor):
    with pytest.raises(ValueError):
        cursor.execute("SELECT ?", [int(BIG_POS)])


def test_long_max_into_float_column_keeps_double_magnitude(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, f FLOAT NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (f) VALUES (?)", [LONG_MAX])
    cursor.execute(f"SELECT f FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == float(LONG_MAX)


def test_null_integer_params_into_every_integer_type_fetch_none(conn, cursor, table_name):
    _create(
        conn,
        table_name,
        "id INT IDENTITY PRIMARY KEY, a BIGINT NULL, b INT NULL, c SMALLINT NULL, "
        "d TINYINT NULL, e NUMERIC(38,0) NULL, f VARCHAR(20) NULL",
    )
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
        [None] * 6,
    )
    cursor.execute(f"SELECT a, b, c, d, e, f FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (None,) * 6


# --- GUIDs ------------------------------------------------------------------


def test_uuid_object_roundtrips_through_uniqueidentifier(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, u UNIQUEIDENTIFIER NOT NULL")
    value = uuid.uuid4()
    cursor.execute(f"INSERT INTO dbo.{table_name} (u) VALUES (?)", [value])
    cursor.execute(f"SELECT u FROM dbo.{table_name}")
    got = cursor.fetchone()[0]
    assert str(got).lower() == str(value)


def test_uuid_string_in_upper_and_braced_forms_binds_as_guid(cursor):
    cursor.execute(
        "SELECT CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER)",
        ["0123ABCD-0000-0000-0000-00000000ABCD", "{00000000-0000-0000-0000-000000000001}"],
    )
    upper, braced = cursor.fetchone()
    assert str(upper).lower() == "0123abcd-0000-0000-0000-00000000abcd"
    assert str(braced) == "00000000-0000-0000-0000-000000000001"


def test_nil_uuid_roundtrips_and_is_not_null(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, u UNIQUEIDENTIFIER NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (u) VALUES (?)", [uuid.UUID(int=0)])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE u IS NOT NULL AND u = ?", [uuid.UUID(int=0)])
    assert cursor.fetchone()[0] == 1


def test_uuid_param_in_where_clause_finds_the_row(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, u UNIQUEIDENTIFIER NOT NULL, tag VARCHAR(10)")
    wanted = uuid.uuid4()
    cursor.execute(f"INSERT INTO dbo.{table_name} (u, tag) VALUES (?, ?)", [uuid.uuid4(), "other"])
    cursor.execute(f"INSERT INTO dbo.{table_name} (u, tag) VALUES (?, ?)", [wanted, "wanted"])
    cursor.execute(f"SELECT tag FROM dbo.{table_name} WHERE u = ?", [wanted])
    assert [tuple(r) for r in cursor.fetchall()] == [("wanted",)]


def test_null_uuid_param_matches_is_null_only(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, u UNIQUEIDENTIFIER NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (u) VALUES (?)", [None])
    cursor.execute(f"INSERT INTO dbo.{table_name} (u) VALUES (?)", [uuid.uuid4()])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE u IS NULL")
    assert cursor.fetchone()[0] == 1


def test_garbage_guid_string_raises_database_error_and_stores_nothing(conn, cursor, table_name):
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, u UNIQUEIDENTIFIER NULL")
    with pytest.raises(Error):
        cursor.execute(f"INSERT INTO dbo.{table_name} (u) VALUES (?)", ["garbage"])
    conn.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


def test_insert_then_select_by_id_in_one_batch_returns_inserted_row(conn, cursor, table_name):
    # JDBC pattern: "INSERT ... VALUES(?) SELECT * ... WHERE id = ?" in one execute.
    _create(conn, table_name, "id INT IDENTITY PRIMARY KEY, u UNIQUEIDENTIFIER NOT NULL")
    value = uuid.uuid4()
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (u) VALUES (?) "
        f"SELECT id, u FROM dbo.{table_name} WHERE id = SCOPE_IDENTITY()",
        [value],
    )
    assert cursor.nextset()
    row = tuple(cursor.fetchone())
    assert row is not None
    assert row[0] == 1
    assert str(row[1]).lower() == str(value)
