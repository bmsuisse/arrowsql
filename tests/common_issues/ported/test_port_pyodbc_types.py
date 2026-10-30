"""Type round-trips at buffer fence-posts, decimal precision matrix, and untyped NULL.

Adapted from mkleehammer/pyodbc tests/sqlserver_test.py (MIT-0), Copyright the pyodbc
contributors (LICENSE.txt carries no copyright line). MIT-0 requires no notice to be
reproduced, so none is copied here. The fence-post length ladder and the
decimal precision/scale matrix come from the pyodbc suite's _test_vartype and
test_decimal cases and were re-expressed for mssql_python's qmark parameters.
Upstream ODBC driver is ground truth.
"""

import uuid
from decimal import Decimal

import pytest

# Lengths around the usual read-buffer boundaries (powers of two, +/-1) and the
# 4000-character edge of nvarchar/varchar fixed-size columns.
FENCEPOST_LENGTHS = [
    None, 0, 1, 255, 256, 510, 511, 512, 1023, 1024, 2047, 2048, 4000,
]
SEED = "0123456789-abcdefghijklmnopqrstuvwxyz-"


def _text(length):
    if length is None:
        return None
    # Non-ASCII first so a multi-byte cut would show up near the start of the value.
    value = "á" + SEED * (1 + length // len(SEED))
    return value[:length]


def _binary(length):
    if length is None:
        return None
    return _text(length + 1).encode("utf-8")[:length]


@pytest.mark.parametrize("length", FENCEPOST_LENGTHS)
def test_varchar_fencepost_lengths_roundtrip(cursor, table_name, length):
    cursor.execute(f"create table dbo.{table_name}(c varchar(4000))")
    # varchar is single-byte in this collation; keep only characters that fit it.
    value = _text(length)
    if value is not None:
        value = value.encode("cp1252", errors="replace").decode("cp1252")
    cursor.execute(f"insert into dbo.{table_name} values (?)", value)
    got = cursor.execute(f"select c from dbo.{table_name}").fetchone()[0]
    assert got == value
    if value is not None:
        assert len(got) == len(value)


@pytest.mark.parametrize("length", FENCEPOST_LENGTHS)
def test_nvarchar_fencepost_lengths_roundtrip(cursor, table_name, length):
    cursor.execute(f"create table dbo.{table_name}(c nvarchar(4000))")
    value = _text(length)
    cursor.execute(f"insert into dbo.{table_name} values (?)", value)
    got = cursor.execute(f"select c from dbo.{table_name}").fetchone()[0]
    assert got == value


@pytest.mark.parametrize("length", FENCEPOST_LENGTHS)
def test_varbinary_fencepost_lengths_roundtrip(cursor, table_name, length):
    cursor.execute(f"create table dbo.{table_name}(c varbinary(4000))")
    value = _binary(length)
    cursor.execute(f"insert into dbo.{table_name} values (?)", value)
    got = cursor.execute(f"select c from dbo.{table_name}").fetchone()[0]
    assert got == value


@pytest.mark.parametrize("precision,scale,negative", [
    (1, 0, False), (1, 0, True),
    (6, 0, False), (6, 2, False), (6, 4, True), (6, 6, True),
    (38, 0, False), (38, 10, False), (38, 38, False),
    (38, 0, True), (38, 10, True), (38, 38, True),
])
def test_decimal_maximum_value_for_precision_and_scale(cursor, table_name, precision, scale, negative):
    cursor.execute(f"create table dbo.{table_name}(d decimal({precision}, {scale}))")
    sign = "-" if negative else ""
    before = "9" * (precision - scale)
    after = ("." + "9" * scale) if scale else ""
    value = Decimal(f"{sign}{before}{after}")
    cursor.execute(f"insert into dbo.{table_name} values (?)", value)
    got = cursor.execute(f"select d from dbo.{table_name}").fetchone()[0]
    assert isinstance(got, Decimal)
    assert got == value


def test_untyped_none_select_returns_none(cursor):
    # A bare NULL parameter with no table context must still execute and come back None.
    assert cursor.execute("select ?", None).fetchone()[0] is None


def test_uuid_parameter_roundtrips_through_uniqueidentifier(cursor, table_name):
    value = uuid.uuid4()
    cursor.execute(f"create table dbo.{table_name}(n uniqueidentifier)")
    cursor.execute(f"insert into dbo.{table_name} values (?)", str(value))
    got = cursor.execute(f"select n from dbo.{table_name}").fetchone()[0]
    assert str(got).lower() == str(value).lower()
