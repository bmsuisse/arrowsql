"""Datatype round-trips and NULL binding ported from tiberius' query tests.

Adapted from prisma/tiberius tests/query.rs (MIT OR Apache-2.0), Copyright 2021 Tiberius Contributors.
Upstream license texts: https://github.com/prisma/tiberius/blob/main/LICENSE-MIT.txt and LICENSE-APACHE.txt.
The cases were re-expressed against mssql_python (DB-API cursor, qmark parameters); no code was copied.

Covers TINYINT extremes, very long and mixed-script NVARCHAR values, Finnish text,
fixed-width CHAR/NCHAR padding, BINARY/VARBINARY(MAX) sizes, GUID byte order,
boolean and typed-NULL parameters, DECIMAL scale handling and legacy code-page
collations. Tables are permanent so the table_name fixture cleans them up.
"""

import decimal
import uuid

import pytest


def _create(cursor, table, ddl):
    cursor.execute(f"CREATE TABLE dbo.{table} ({ddl})")
    cursor.connection.commit()


def _column(cursor, sql, params=None):
    cursor.execute(sql, params or [])
    return [row[0] for row in cursor.fetchall()]


def test_tinyint_extremes_and_null_round_trip(cursor, table_name):
    _create(cursor, table_name, "content TINYINT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (content) VALUES (?), (?), (?)", [0, 255, None])
    assert cursor.rowcount == 3
    cursor.connection.commit()

    got = _column(cursor, f"SELECT content FROM dbo.{table_name} ORDER BY content")
    assert got == [None, 0, 255]


def test_kanji_nvarchar_max_and_80001_char_value(cursor, table_name):
    _create(cursor, table_name, "content NVARCHAR(MAX) NOT NULL")
    short = "余ったものを後で皆に分けようと思っていただけなのに"
    long_value = "余" * 80001
    cursor.execute(f"INSERT INTO dbo.{table_name} (content) VALUES (?), (?)", [short, long_value])
    assert cursor.rowcount == 2
    cursor.connection.commit()

    got = _column(cursor, f"SELECT content FROM dbo.{table_name} ORDER BY LEN(content)")
    assert got == [short, long_value]
    assert len(got[1]) == 80001


def test_mixed_script_emoji_and_currency_nvarchar(cursor, table_name):
    _create(cursor, table_name, "content NVARCHAR(1000) NOT NULL")
    # Currency symbols, Thai, emoji (surrogate pairs), Devanagari and a combining mark.
    value = "¥฿" + "".join(chr(c) for c in range(0x1F600, 0x1F650)) + "ऀँंःअआइईउऊ€₹₩⃀"
    cursor.execute(f"INSERT INTO dbo.{table_name} (content) VALUES (?)", [value])
    cursor.connection.commit()

    assert _column(cursor, f"SELECT content FROM dbo.{table_name}") == [value]


def test_finnish_nvarchar_max_round_trip(cursor, table_name):
    _create(cursor, table_name, "content NVARCHAR(MAX) NOT NULL")
    text = (
        "Hääpuhetta voi värittää kertomalla vitsejä, aforismeja,\n"
        "sananlaskuja, laulunsäkeitä ja muita lainauksia. Huumori sopii hääpuheeseen."
    )
    cursor.execute(f"INSERT INTO dbo.{table_name} (content) VALUES (?)", [text])
    cursor.connection.commit()

    assert _column(cursor, f"SELECT content FROM dbo.{table_name}") == [text]


def test_char_and_nchar_single_characters(cursor, table_name):
    _create(cursor, table_name, "a CHAR(1) NOT NULL, b NCHAR(1) NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (a, b) VALUES (?, ?)", ["a", "ä"])
    cursor.connection.commit()

    cursor.execute(f"SELECT a, b FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == ("a", "ä")


def test_binary_8000_fixed_length_round_trip(cursor, table_name):
    _create(cursor, table_name, "content BINARY(8000) NOT NULL")
    payload = bytes(7999) + b"\x05"
    cursor.execute(f"INSERT INTO dbo.{table_name} (content) VALUES (?)", [payload])
    cursor.connection.commit()

    got = _column(cursor, f"SELECT content FROM dbo.{table_name}")
    assert len(got[0]) == 8000
    assert bytes(got[0]) == payload


def test_varbinary_max_8001_bytes_round_trip(cursor, table_name):
    _create(cursor, table_name, "content VARBINARY(MAX) NOT NULL")
    payload = bytes(8000) + b"\x05"
    cursor.execute(f"INSERT INTO dbo.{table_name} (content) VALUES (?)", [payload])
    cursor.connection.commit()

    got = _column(cursor, f"SELECT content FROM dbo.{table_name}")
    assert bytes(got[0]) == payload


def test_guid_string_cast_matches_uuid_byte_order(cursor):
    cursor.execute("SELECT CAST('c97dbc01-fb45-4384-a194-e39a4560cf4a' AS UNIQUEIDENTIFIER)")
    assert cursor.fetchone()[0] == uuid.UUID("c97dbc01-fb45-4384-a194-e39a4560cf4a")


def test_none_parameters_bind_as_typed_nulls_in_one_select(cursor):
    guid = uuid.uuid4()
    params = ["foo", None, b"\x01\x06\x02\x00", None, guid, None, 1, None, 3.5, None, True, None]
    cursor.execute("SELECT " + ", ".join(["?"] * len(params)), params)
    row = tuple(cursor.fetchone())

    assert row == (
        "foo", None, b"\x01\x06\x02\x00", None, guid, None, 1, None, 3.5, None, True, None,
    )


def test_boolean_false_and_true_parameters(cursor):
    cursor.execute("SELECT ?, ?, ?", [False, True, None])
    assert tuple(cursor.fetchone()) == (False, True, None)


@pytest.mark.parametrize(
    "value",
    [
        decimal.Decimal("0.2"),
        decimal.Decimal("214748364.8"),
        decimal.Decimal("0.9223372036854775807"),
        decimal.Decimal("1234567890123456789.1234567890"),
    ],
)
def test_decimal_scale_19_round_trip_through_cast(cursor, value):
    cursor.execute("SELECT CAST(? AS DECIMAL(38,19))", [value])
    got = cursor.fetchone()[0]
    assert got == value
    assert got.as_tuple().exponent == -19


def test_legacy_cp437_collation_stores_every_byte_as_the_code_page(cursor, table_name):
    _create(
        cursor,
        table_name,
        "multi VARCHAR(32) COLLATE SQL_Latin1_General_CP437_BIN NOT NULL, "
        "huge VARCHAR(MAX) COLLATE SQL_Latin1_General_CP437_BIN NOT NULL",
    )
    value = "Café α "
    cursor.execute(f"INSERT INTO dbo.{table_name} (multi, huge) VALUES (?, ?)", [value, value * 2000])
    cursor.connection.commit()

    cursor.execute(
        f"SELECT multi, CONVERT(VARBINARY(32), multi), LEN(huge) FROM dbo.{table_name}"
    )
    multi, raw, huge_len = cursor.fetchone()
    assert multi == value
    assert bytes(raw) == b"Caf\x82 \xe0\xff"
    assert huge_len == len(value) * 2000


def test_legacy_cp850_collation_round_trip(cursor, table_name):
    _create(cursor, table_name, "multi VARCHAR(32) COLLATE SQL_1xCompat_CP850_CI_AS NOT NULL")
    value = "Café ø "
    cursor.execute(f"INSERT INTO dbo.{table_name} (multi) VALUES (?)", [value])
    cursor.connection.commit()

    cursor.execute(f"SELECT multi, CONVERT(VARBINARY(32), multi) FROM dbo.{table_name}")
    multi, raw = cursor.fetchone()
    assert multi == value
    assert bytes(raw) == b"Caf\x82 \x9b\xff"
