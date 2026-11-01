"""Fetch UUID construction, text-shape caching and bulk encoder fast paths."""

import datetime as dt
import decimal
import pickle
import uuid

import pytest

from mssql_python import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="Rust-port internals",
)

D = decimal.Decimal


def test_fetched_uuid_is_a_regular_uuid(cursor):
    cursor.execute("SELECT CAST('12345678-1234-5678-1234-567812345678' AS uniqueidentifier)")
    value = cursor.fetchone()[0]
    expected = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert type(value) is uuid.UUID
    assert value == expected
    assert hash(value) == hash(expected)
    assert value.is_safe is uuid.SafeUUID.unknown
    assert str(value) == "12345678-1234-5678-1234-567812345678"
    assert repr(value) == repr(expected)
    assert value.bytes == expected.bytes
    assert value.version == expected.version
    assert pickle.loads(pickle.dumps(value)) == expected
    with pytest.raises(TypeError):
        value.int = 1


def test_fetchone_char_padding_survives_cached_shape(cursor):
    cursor.execute("SELECT CAST('ab' AS CHAR(4)), CAST(N'cd' AS NCHAR(3)), 1")
    assert tuple(cursor.fetchone()) == ("ab  ", "cd ", 1)
    cursor.execute("SELECT 5, CAST('x' AS CHAR(2))")
    assert tuple(cursor.fetchone()) == (5, "x ")
    cursor.execute("SELECT CAST('y' AS NCHAR(2)), CAST('z' AS CHAR(3))")
    assert [tuple(r) for r in cursor.fetchmany(5)] == [("y ", "z  ")]


def test_decimal_fetchone_distinct_scales(cursor):
    for _ in range(3):
        cursor.execute("SELECT CAST(0 AS DECIMAL(18,2)), CAST(0 AS DECIMAL(18,4)), CAST(0 AS MONEY)")
        row = cursor.fetchone()
        assert [str(v) for v in row] == ["0.00", "0.0000", "0.0000"]
        cursor.execute("SELECT CAST(-1 AS DECIMAL(18,2)), CAST(1 AS DECIMAL(18,4)), CAST(1 AS MONEY)")
        assert [str(v) for v in cursor.fetchone()] == ["-1.00", "1.0000", "1.0000"]


@pytest.fixture
def bulk_table(cursor):
    cursor.execute("DROP TABLE IF EXISTS g3_bulk")
    cursor.execute(
        "CREATE TABLE g3_bulk (id INT, g UNIQUEIDENTIFIER, d DATE, t TIME(3), dt DATETIME2(3), "
        "n DECIMAL(18,2), m MONEY, b VARBINARY(20))"
    )
    yield "g3_bulk"
    cursor.execute("DROP TABLE IF EXISTS g3_bulk")


def test_bulk_row_encoder_fast_paths(cursor, bulk_table):
    u = uuid.UUID("0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9")
    rows = [
        (1, str(u), "2024-02-29", "13:14:15.123", "2024-02-29T13:14:15.123", 5, -7, bytearray(b"\x01\x02")),
        (2, u.hex, dt.date(2000, 1, 2), dt.time(1, 2, 3), dt.datetime(2001, 2, 3, 4, 5, 6), -(10**15), 10**13, b"x"),
        (3, str(u).upper(), "2024-02-29", "00:00:00", "2024-02-29 00:00:00", 0, 0, bytearray()),
        (4, "{" + str(u) + "}", "2024-02-29", "00:00:00", "2024-02-29 00:00:00", 1, 0, b""),
        (5, "urn:uuid:" + str(u), "2024-02-29", "00:00:00", "2024-02-29 00:00:00", D("1.25"), 1.5, b""),
    ]
    cursor.bulkcopy(bulk_table, iter(rows[:3] + rows[3:]))
    cursor.execute(f"SELECT id, g, d, n, m, b FROM {bulk_table} ORDER BY id")
    got = [tuple(r) for r in cursor.fetchall()]
    assert got[0] == (1, u, dt.date(2024, 2, 29), D("5.00"), D("-7.0000"), b"\x01\x02")
    assert got[1][1] == u and got[1][3] == D(-(10**15)) and got[1][4] == D(10**13)
    assert got[2][1] == u and got[2][5] == b""
    assert got[3][1] == u and got[3][3] == D("1.00") and got[3][4] == D("0")
    assert got[4][1] == u and got[4][3] == D("1.25")


@pytest.mark.parametrize("bad", ["12345678-1234-5678-1234-56781234567", "zzzzzzzz-1234-5678-1234-567812345678", "12345678_1234_5678_1234_567812345678x"])
def test_bulk_bad_guid_string_still_errors(cursor, bulk_table, bad):
    with pytest.raises(Exception, match="(?i)GUID|badly formed|uuid"):
        cursor.bulkcopy(bulk_table, iter([(1, bad, None, None, None, None, None, None)]))


def test_bulk_int_overflow_messages_unchanged(cursor, bulk_table):
    with pytest.raises(Exception, match="precision"):
        cursor.bulkcopy(bulk_table, iter([(1, None, None, None, None, 10**20, None, None)]))
    with pytest.raises(Exception, match="out of range"):
        cursor.bulkcopy(bulk_table, iter([(1, None, None, None, None, None, 10**20, None)]))
    with pytest.raises(Exception, match="(?i)invalid decimal|bool|unsupported"):
        cursor.bulkcopy(bulk_table, iter([(1, None, None, None, None, True, None, None)]))
