"""Catalog introspection and server metadata, ported from SQLAlchemy's mssql tests.

Adapted from sqlalchemy/sqlalchemy test/dialect/mssql/test_reflection.py
(IdentityReflectionTest, ReflectHugeViewTest, the MS_Description comment case
from test/dialect/mssql/test_engine.py MiscTest, and the server-version
probe) (MIT), Copyright 2005-2026 SQLAlchemy authors and contributors. The MIT
notice is in LICENSE_sqlalchemy.txt next to this file. SQLAlchemy reflects
through its Inspector API; here the same catalog views are queried directly
and the driver's Python types for the returned values are checked.
"""

import uuid

import pytest


def _rows(cursor, sql, params=()):
    cursor.execute(sql, params)
    return [tuple(r) for r in cursor.fetchall()]


def test_identity_seed_and_increment_from_sys_identity_columns(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        "  id INT IDENTITY(10,5) PRIMARY KEY, txt NVARCHAR(10))"
    )
    cursor.connection.commit()
    seed, increment = _rows(
        cursor,
        "SELECT CAST(seed_value AS BIGINT), CAST(increment_value AS BIGINT) "
        "FROM sys.identity_columns WHERE object_id = OBJECT_ID(?)",
        (f"dbo.{table_name}",),
    )[0]
    assert (seed, increment) == (10, 5)
    cursor.execute(f"INSERT INTO dbo.{table_name} (txt) VALUES (?)", ("a",))
    cursor.execute(f"INSERT INTO dbo.{table_name} (txt) VALUES (?)", ("b",))
    cursor.connection.commit()
    assert _rows(cursor, f"SELECT id FROM dbo.{table_name} ORDER BY id") == [(10,), (15,)]


@pytest.mark.parametrize(
    "column_type, expected",
    [
        ("NVARCHAR(MAX)", -1),
        ("NVARCHAR(4000)", 4000),
        ("VARBINARY(MAX)", -1),
        ("VARCHAR(7)", 7),
        ("NTEXT", 1073741823),
    ],
)
def test_information_schema_character_maximum_length(cursor, table_name, column_type, expected):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (c {column_type} NULL)")
    cursor.connection.commit()
    value = _rows(
        cursor,
        "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ? AND COLUMN_NAME = 'c'",
        (table_name,),
    )[0][0]
    assert value == expected
    assert type(value) is int


def test_decimal_and_datetime2_precision_metadata(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (a DECIMAL(38,20) NULL, b DATETIME2(7) NULL)"
    )
    cursor.connection.commit()
    rows = _rows(
        cursor,
        "SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        (table_name,),
    )
    assert rows == [("a", 38, 20, None), ("b", None, None, 7)]


def test_composite_primary_key_columns_in_declared_order(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        "  b INT NOT NULL, a INT NOT NULL, c INT NULL,"
        f"  CONSTRAINT PK_{table_name} PRIMARY KEY (b, a))"
    )
    cursor.connection.commit()
    cols = _rows(
        cursor,
        "SELECT k.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t "
        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k "
        "  ON k.CONSTRAINT_NAME = t.CONSTRAINT_NAME AND k.TABLE_NAME = t.TABLE_NAME "
        "WHERE t.TABLE_NAME = ? AND t.CONSTRAINT_TYPE = 'PRIMARY KEY' "
        "ORDER BY k.ORDINAL_POSITION",
        (table_name,),
    )
    assert [r[0] for r in cols] == ["b", "a"]


def test_column_and_table_comments_round_trip_through_extended_properties(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT NOT NULL PRIMARY KEY, has_comment NVARCHAR(20), no_comment INT)"
    )
    cursor.connection.commit()
    cursor.execute(
        "EXEC sp_addextendedproperty @name = N'MS_Description', @value = ?, "
        "@level0type = N'SCHEMA', @level0name = N'dbo', "
        "@level1type = N'TABLE', @level1name = ?",
        ("table comment", table_name),
    )
    cursor.execute(
        "EXEC sp_addextendedproperty @name = N'MS_Description', @value = ?, "
        "@level0type = N'SCHEMA', @level0name = N'dbo', "
        "@level1type = N'TABLE', @level1name = ?, "
        "@level2type = N'COLUMN', @level2name = N'has_comment'",
        ("café 🐍", table_name),
    )
    cursor.connection.commit()

    def comment(column):
        if column is None:
            sql = (
                "SELECT CAST(value AS NVARCHAR(4000)) FROM fn_listextendedproperty("
                "N'MS_Description', N'SCHEMA', N'dbo', N'TABLE', ?, NULL, NULL)"
            )
            params = (table_name,)
        else:
            sql = (
                "SELECT CAST(value AS NVARCHAR(4000)) FROM fn_listextendedproperty("
                "N'MS_Description', N'SCHEMA', N'dbo', N'TABLE', ?, N'COLUMN', ?)"
            )
            params = (table_name, column)
        rows = _rows(cursor, sql, params)
        return rows[0][0] if rows else None

    assert comment(None) == "table comment"
    assert comment("has_comment") == "café 🐍"
    assert comment("no_comment") is None

    cursor.execute(
        "EXEC sp_dropextendedproperty @name = N'MS_Description', "
        "@level0type = N'SCHEMA', @level0name = N'dbo', "
        "@level1type = N'TABLE', @level1name = ?, "
        "@level2type = N'COLUMN', @level2name = N'has_comment'",
        (table_name,),
    )
    cursor.connection.commit()
    assert comment("has_comment") is None


def test_view_definition_longer_than_4000_chars_is_returned_whole(cursor, table_name):
    view = f"v_{table_name}"
    columns = ",\n  ".join(
        f"CAST(N'{'x' * 40}' AS NVARCHAR(100)) AS col_{i:03d}_{'y' * 20}" for i in range(120)
    )
    cursor.execute(f"CREATE VIEW dbo.{view} AS\nSELECT\n  {columns}")
    cursor.connection.commit()
    try:
        definition = _rows(cursor, "SELECT OBJECT_DEFINITION(OBJECT_ID(?))", (f"dbo.{view}",))[0][0]
        assert len(definition) > 4000
        assert "col_119_" in definition
        assert definition.count("col_") == 120
    finally:
        cursor.execute(f"DROP VIEW IF EXISTS dbo.{view}")
        cursor.connection.commit()


def test_server_version_probe_returns_integers_and_text(cursor):
    major = _rows(cursor, "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)")[0][0]
    product_version = _rows(cursor, "SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))")[0][0]
    banner = _rows(cursor, "SELECT @@VERSION")[0][0]
    assert type(major) is int and major > 0
    parts = product_version.split(".")
    assert len(parts) == 4 and all(p.isdigit() for p in parts)
    assert int(parts[0]) == major
    assert banner.startswith("Microsoft SQL")
