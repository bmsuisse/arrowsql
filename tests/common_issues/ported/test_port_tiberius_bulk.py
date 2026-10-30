"""Bulk copy value and column-handling cases ported from tiberius' bulk tests.

Adapted from prisma/tiberius tests/bulk.rs (MIT OR Apache-2.0), Copyright 2021 Tiberius Contributors.
Upstream license texts: https://github.com/prisma/tiberius/blob/main/LICENSE-MIT.txt and LICENSE-APACHE.txt.
The cases were re-expressed against mssql_python's cursor.bulkcopy(); no code was copied.

Covers the full TINYINT range with NULLs, exact NUMERIC(28,4) and MONEY values beyond
float precision, identity and computed columns skipped through column mappings, a
bracket-quoted keyword column, and legacy code-page / collation bytes for bulk-inserted text.
Bulk copy may use its own connection, so only permanent tables are used.
"""

import decimal

import pytest


def _create(cursor, table, ddl):
    cursor.execute(f"CREATE TABLE dbo.{table} ({ddl})")
    cursor.connection.commit()


def test_bulk_tinyint_full_range_with_nulls(cursor, table_name):
    _create(cursor, table_name, "content TINYINT NULL")
    data = [(v,) for v in range(256)] + [(None,)]

    result = cursor.bulkcopy(f"dbo.{table_name}", data)

    assert result["rows_copied"] == 257
    cursor.execute(f"SELECT COUNT(*), COUNT(content), SUM(CAST(content AS BIGINT)) FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (257, 256, 255 * 256 // 2)


def test_bulk_numeric_28_4_large_magnitude_is_exact(cursor, table_name):
    # Scaled value exceeds 2**53, so a float detour would lose digits.
    _create(cursor, table_name, "content NUMERIC(28,4) NOT NULL")
    value = decimal.Decimal("12345678901234.5678")

    cursor.bulkcopy(f"dbo.{table_name}", [(value,)])

    cursor.execute(f"SELECT content FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == value


def test_bulk_money_four_decimal_places_are_exact(cursor, table_name):
    _create(cursor, table_name, "content MONEY NOT NULL")
    value = decimal.Decimal("1234.5678")

    cursor.bulkcopy(f"dbo.{table_name}", [(value,)])

    cursor.execute(f"SELECT content FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == value


def test_bulk_text_and_ntext_values_round_trip(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, t TEXT NOT NULL, nt NTEXT NOT NULL")
    data = [(1, "hello bulk text", "héllo bulk ñtext"), (2, "", "Привет")]

    cursor.bulkcopy(f"dbo.{table_name}", data)

    cursor.execute(f"SELECT id, t, nt FROM dbo.{table_name} ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == data


def test_bulk_bracket_quoted_keyword_column(cursor, table_name):
    _create(cursor, table_name, "[End] INT NOT NULL")

    result = cursor.bulkcopy(f"dbo.{table_name}", [(6,), (7,), (8,)])

    assert result["rows_copied"] == 3
    cursor.execute(f"SELECT [End] FROM dbo.{table_name} ORDER BY [End]")
    assert [r[0] for r in cursor.fetchall()] == [6, 7, 8]


def test_bulk_skips_identity_and_computed_columns_via_mapping(cursor, table_name):
    _create(
        cursor,
        table_name,
        "id INT IDENTITY PRIMARY KEY, val INT NOT NULL, c AS (val + 1)",
    )
    data = [(v,) for v in range(10)]

    result = cursor.bulkcopy(f"dbo.{table_name}", data, column_mappings=["val"])

    assert result["rows_copied"] == 10
    cursor.execute(
        f"SELECT COUNT(*), SUM(CASE WHEN c <> val + 1 THEN 1 ELSE 0 END), MIN(id), MAX(id) "
        f"FROM dbo.{table_name}"
    )
    assert tuple(cursor.fetchone()) == (10, 0, 1, 10)


def test_bulk_cyrillic_column_collation_stores_cp1251_bytes(cursor, table_name):
    cursor.execute(
        "SELECT CONVERT(INT, COLLATIONPROPERTY(CONVERT(NVARCHAR(128), "
        "DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage'))"
    )
    assert cursor.fetchone()[0] != 1251
    _create(
        cursor,
        table_name,
        "id INT NOT NULL, "
        "v VARCHAR(20) COLLATE Cyrillic_General_CI_AS NOT NULL, "
        "c CHAR(6) COLLATE Cyrillic_General_CI_AS NOT NULL, "
        "n NVARCHAR(20) COLLATE Cyrillic_General_CI_AS NOT NULL",
    )
    expected = "Привет"

    cursor.bulkcopy(f"dbo.{table_name}", [(1, expected, expected, expected)])

    cursor.execute(
        f"SELECT v, c, n, CONVERT(VARBINARY(20), v), CONVERT(VARBINARY(20), c) FROM dbo.{table_name}"
    )
    v, c, n, raw_v, raw_c = cursor.fetchone()
    cp1251 = bytes([0xCF, 0xF0, 0xE8, 0xE2, 0xE5, 0xF2])
    assert (v, c, n) == (expected, expected, expected)
    assert bytes(raw_v) == cp1251
    assert bytes(raw_c) == cp1251


@pytest.mark.upstream_bug(
    reason=(
        "cursor.bulkcopy ignores the column collation for non-default code pages: text bulk-inserted "
        "into a SQL_Latin1_General_CP437_BIN column is stored as the default code page bytes "
        "(U+00C7 -> 0xC7 instead of 0x80), while parameterised INSERT stores the correct bytes"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_bulk_every_high_byte_of_cp437_stored_verbatim(cursor, table_name):
    cursor.execute("SELECT CONVERT(INT, COLLATIONPROPERTY(CONVERT(NVARCHAR(128), "
                   "DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage'))")
    assert cursor.fetchone()[0] != 437
    collation = "SQL_Latin1_General_CP437_BIN"
    raw = bytes(range(0x80, 0x100))
    hex_literal = raw.hex().upper()

    # Decode the 128 bytes with the server's own collation, then bulk-insert that text.
    cursor.execute(
        f"SET NOCOUNT ON; DECLARE @t TABLE (v VARCHAR(128) COLLATE {collation}); "
        f"INSERT INTO @t VALUES (0x{hex_literal}); SELECT v FROM @t"
    )
    text = cursor.fetchone()[0]
    assert len(text) == 128

    _create(
        cursor,
        table_name,
        f"c CHAR(128) COLLATE {collation} NOT NULL, "
        f"v VARCHAR(128) COLLATE {collation} NOT NULL, "
        f"m VARCHAR(MAX) COLLATE {collation} NOT NULL, "
        f"t TEXT COLLATE {collation} NOT NULL",
    )
    cursor.bulkcopy(f"dbo.{table_name}", [(text, text, text, text)])

    cursor.execute(
        f"SELECT CONVERT(VARBINARY(MAX), c), CONVERT(VARBINARY(MAX), v), "
        f"CONVERT(VARBINARY(MAX), m), CONVERT(VARBINARY(MAX), CONVERT(VARCHAR(MAX), t)) "
        f"FROM dbo.{table_name}"
    )
    stored = [bytes(x) for x in cursor.fetchone()]
    assert stored == [raw, raw, raw, raw]


@pytest.mark.parametrize("batch_size", [0, 1, 7])
def test_bulk_batch_size_keeps_every_row(cursor, table_name, batch_size):
    _create(cursor, table_name, "val INT NOT NULL")
    data = [(i,) for i in range(50)]

    result = cursor.bulkcopy(f"dbo.{table_name}", data, batch_size=batch_size)

    assert result["rows_copied"] == 50
    cursor.execute(f"SELECT COUNT(DISTINCT val) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 50
