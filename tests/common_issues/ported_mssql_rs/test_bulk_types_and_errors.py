# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Behaviour adapted from microsoft/mssql-rs (MIT): mssql-py-core/tests/test_bulkcopy_<type>.py,
# test_bulkcopy_{image,ntext,text,variant,uuid,vector,json,xml,multipart_table_names}.py and
# mssql-tds/tests/test_bulk_copy_{bigint,empty_plp,error_recovery,identifiers,uuid,variant,
# varying_columns,vector,xml}.rs. Re-expressed against Cursor.bulkcopy on the DB-API fixtures.
"""Bulk copy type matrix, empty PLP values, error recovery and identifiers (real SQL Server)."""

import datetime as dt
import decimal
import uuid

import pytest

import mssql_python

D = decimal.Decimal
GUID = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _load(cursor, table, ddl, rows, **opts):
    cursor.execute(f"CREATE TABLE dbo.{table} (id INT NOT NULL, v {ddl})")
    cursor.connection.commit()
    result = cursor.bulkcopy(f"dbo.{table}", [(i, v) for i, v in enumerate(rows)], **opts)
    cursor.execute(f"SELECT v FROM dbo.{table} ORDER BY id")
    return result, [r[0] for r in cursor.fetchall()]


# (sql type, source values, expected read-back values)
TYPE_CASES = {
    "tinyint": ("tinyint", [0, 255, None], [0, 255, None]),
    "smallint": ("smallint", [-32768, 32767, None], [-32768, 32767, None]),
    "int": ("int", [-(2**31), 2**31 - 1, None], [-(2**31), 2**31 - 1, None]),
    "bigint": ("bigint", [-(2**63), 2**63 - 1, 0, None], [-(2**63), 2**63 - 1, 0, None]),
    "bit": ("bit", [True, False, None], [True, False, None]),
    "real": ("real", [1.5, -2.25, None], [1.5, -2.25, None]),
    "float": ("float", [1.5e300, -2.25, None], [1.5e300, -2.25, None]),
    "decimal": ("decimal(18,4)", [D("1.5000"), D("-99999999999999.9999"), None], [D("1.5000"), D("-99999999999999.9999"), None]),
    "numeric": ("numeric(5,0)", [D("12345"), D("-1"), None], [D("12345"), D("-1"), None]),
    "money": ("money", [D("12.3400"), D("-0.5000"), None], [D("12.3400"), D("-0.5000"), None]),
    "smallmoney": ("smallmoney", [D("214748.3647"), D("-1.0000"), None], [D("214748.3647"), D("-1.0000"), None]),
    "char": ("char(5)", ["ab", "abcde", None], ["ab   ", "abcde", None]),
    "varchar": ("varchar(20)", ["", "hello", None], ["", "hello", None]),
    "nchar": ("nchar(4)", ["hé", "日本", None], ["hé  ", "日本  ", None]),
    "nvarchar": ("nvarchar(50)", ["", "héllo \U0001f600", None], ["", "héllo \U0001f600", None]),
    "varchar_max": ("varchar(max)", ["x" * 20000, "", None], ["x" * 20000, "", None]),
    "nvarchar_max": ("nvarchar(max)", ["é" * 20000, "", None], ["é" * 20000, "", None]),
    "binary": ("binary(4)", [b"\x01\x02", b"\xff\xff\xff\xff", None], [b"\x01\x02\x00\x00", b"\xff\xff\xff\xff", None]),
    "varbinary": ("varbinary(16)", [b"", b"\x00\x01", None], [b"", b"\x00\x01", None]),
    "varbinary_max": ("varbinary(max)", [b"\xab" * 30000, b"", None], [b"\xab" * 30000, b"", None]),
    "date": ("date", [dt.date(1, 1, 1), dt.date(9999, 12, 31), None], [dt.date(1, 1, 1), dt.date(9999, 12, 31), None]),
    "time": ("time(7)", [dt.time(0, 0), dt.time(23, 59, 59, 999999), None], [dt.time(0, 0), dt.time(23, 59, 59, 999999), None]),
    "datetime": ("datetime", [dt.datetime(1753, 1, 1), dt.datetime(2024, 2, 29, 12, 30, 45), None], [dt.datetime(1753, 1, 1), dt.datetime(2024, 2, 29, 12, 30, 45), None]),
    "smalldatetime": ("smalldatetime", [dt.datetime(1900, 1, 1), dt.datetime(2079, 6, 6, 23, 59), None], [dt.datetime(1900, 1, 1), dt.datetime(2079, 6, 6, 23, 59), None]),
    "datetime2": ("datetime2(6)", [dt.datetime(1, 1, 1), dt.datetime(2024, 5, 6, 7, 8, 9, 123456), None], [dt.datetime(1, 1, 1), dt.datetime(2024, 5, 6, 7, 8, 9, 123456), None]),
    "datetimeoffset": (
        "datetimeoffset(6)",
        [dt.datetime(2024, 5, 6, 7, 8, 9, 123456, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))), None],
        [dt.datetime(2024, 5, 6, 7, 8, 9, 123456, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))), None],
    ),
    "uniqueidentifier": ("uniqueidentifier", [GUID, uuid.UUID(int=0), None], [GUID, uuid.UUID(int=0), None]),
}


@pytest.mark.parametrize("name", sorted(TYPE_CASES))
def test_bulkcopy_type_round_trip(cursor, table_name, name):
    ddl, source, expected = TYPE_CASES[name]
    result, got = _load(cursor, table_name, ddl, source)
    assert got == expected
    assert result["rows_copied"] == len(source)


def test_bulkcopy_xml_round_trip(cursor, table_name):
    doc = "<root><a>1</a><b attr=\"x\">hé</b></root>"
    _, got = _load(cursor, table_name, "xml", [doc, None])
    assert got[1] is None
    assert "<a>1</a>" in got[0] and "hé" in got[0]


def test_bulkcopy_sql_variant(cursor, table_name):
    _, got = _load(cursor, table_name, "sql_variant", [1, "text", D("1.25"), None])
    assert got[0] == 1 and got[1] == "text" and got[2] == D("1.25") and got[3] is None


@pytest.mark.parametrize("ddl, empty", [
    ("varchar(max)", ""), ("nvarchar(max)", ""), ("varbinary(max)", b""),
    ("varchar(8)", ""), ("nvarchar(8)", ""), ("varbinary(8)", b""),
])
def test_empty_values_are_stored_as_empty_not_null(cursor, table_name, ddl, empty):
    """test_bulk_copy_empty_plp.rs: a zero-length PLP / variable value is not NULL."""
    _, got = _load(cursor, table_name, ddl, [empty, None])
    assert got == [empty, None]


def test_bigint_extremes_in_key_column(cursor, table_name):
    """test_bulk_copy_bigint.rs"""
    cursor.execute(f"CREATE TABLE dbo.{table_name} (k BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
    cursor.connection.commit()
    keys = [-(2**63), -1, 0, 1, 2**63 - 1]
    cursor.bulkcopy(f"dbo.{table_name}", [(k, k) for k in keys])
    cursor.execute(f"SELECT k, v FROM dbo.{table_name} ORDER BY k")
    assert [tuple(r) for r in cursor.fetchall()] == [(k, k) for k in keys]


def test_vector_column_is_either_loaded_or_rejected_cleanly(cursor, table_name):
    """test_bulkcopy_vector.py / test_bulk_copy_vector.rs need SQL Server 2025; elsewhere
    the CREATE fails server-side and must surface as a normal error."""
    try:
        cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, v VECTOR(3))")
    except mssql_python.Error:
        pytest.skip("VECTOR type needs SQL Server 2025")
    cursor.connection.commit()
    cursor.bulkcopy(f"dbo.{table_name}", [(1, "[1.0, 2.0, 3.0]")])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 1


def test_json_column_is_either_loaded_or_rejected_cleanly(cursor, table_name):
    """test_bulkcopy_json.py: the json type needs SQL Server 2025 / Azure SQL."""
    try:
        cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, v JSON)")
    except mssql_python.Error:
        pytest.skip("JSON type needs SQL Server 2025")
    cursor.connection.commit()
    cursor.bulkcopy(f"dbo.{table_name}", [(1, '{"a": 1}')])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 1


# --- error recovery (test_bulk_copy_error_recovery.rs) -----------------------------

def test_failed_bulkcopy_leaves_connection_usable_and_next_load_succeeds(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL PRIMARY KEY, v NVARCHAR(10))")
    cursor.connection.commit()
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, "a"), (1, "duplicate key")], batch_size=0)
    cursor.connection.rollback()
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42
    cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
    cursor.connection.commit()
    result = cursor.bulkcopy(f"dbo.{table_name}", [(1, "a"), (2, "b")])
    assert result["rows_copied"] == 2


def test_string_too_long_for_column_raises_and_connection_recovers(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL, v VARCHAR(3))")
    cursor.connection.commit()
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, "way too long for the column")])
    cursor.connection.rollback()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_bulkcopy_into_missing_table_raises(cursor, table_name):
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}_does_not_exist", [(1,)])
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


# --- identifiers (test_bulk_copy_identifiers.rs, test_bulkcopy_multipart_table_names.py) ---

@pytest.mark.parametrize("fmt", ["{t}", "dbo.{t}", "[dbo].[{t}]", "[{t}]"])
def test_table_name_forms(cursor, table_name, fmt):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, v NVARCHAR(10))")
    cursor.connection.commit()
    cursor.bulkcopy(fmt.format(t=table_name), [(1, "a")])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 1


def test_three_part_table_name_with_current_database(cursor, table_name):
    db = cursor.execute("SELECT DB_NAME()").fetchone()[0]
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT)")
    cursor.connection.commit()
    cursor.bulkcopy(f"[{db}].[dbo].[{table_name}]", [(1,), (2,)])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 2


def test_special_characters_in_table_and_column_names(cursor):
    name = f"ci weird ]] name {uuid.uuid4().hex[:6]}"
    quoted = "[" + name.replace("]", "]]") + "]"
    cursor.execute(f"CREATE TABLE dbo.{quoted} ([a b] INT, [c]]d] NVARCHAR(10))")
    cursor.connection.commit()
    try:
        cursor.bulkcopy(f"dbo.{quoted}", [(1, "x")])
        cursor.execute(f"SELECT [a b], [c]]d] FROM dbo.{quoted}")
        assert tuple(cursor.fetchone()) == (1, "x")
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{quoted}")
        cursor.connection.commit()


# --- varying column counts (test_bulk_copy_varying_columns.rs) ---------------------

@pytest.mark.parametrize("width", [1, 2, 8, 33])
def test_wide_and_narrow_tables(cursor, table_name, width):
    cols = ", ".join(f"c{i} INT" for i in range(width))
    cursor.execute(f"CREATE TABLE dbo.{table_name} ({cols})")
    cursor.connection.commit()
    cursor.bulkcopy(f"dbo.{table_name}", [tuple(range(width)), tuple(range(width, 2 * width))])
    cursor.execute(f"SELECT * FROM dbo.{table_name} ORDER BY c0")
    assert [tuple(r) for r in cursor.fetchall()] == [tuple(range(width)), tuple(range(width, 2 * width))]


# --- collations (test_bulkcopy_char_collations.py, test_bulkcopy_varchar_collations.py) ----

@pytest.mark.parametrize("collation, text", [
    ("Latin1_General_CI_AS", "café üß"),
    ("Cyrillic_General_CI_AS", "Привет"),
    ("Greek_CI_AS", "αβγ"),
    ("Japanese_CI_AS", "日本語"),
    ("Chinese_PRC_CI_AS", "中文"),
    ("Latin1_General_100_CI_AS_SC_UTF8", "héllo \U0001f600"),
])
def test_varchar_collation_encodings(cursor, table_name, collation, text):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, v VARCHAR(40) COLLATE {collation}, c CHAR(10) COLLATE {collation})")
    cursor.connection.commit()
    cursor.bulkcopy(f"dbo.{table_name}", [(1, text, text[:3])])
    cursor.execute(f"SELECT v, RTRIM(c) FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (text, text[:3])
