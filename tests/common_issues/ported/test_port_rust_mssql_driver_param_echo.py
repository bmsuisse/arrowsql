"""Parameter round-trips (``SELECT CAST(? AS T)``) for each scalar type, ported from rust-mssql-driver.

Adapted from praxiomlabs/rust-mssql-driver crates/mssql-client/tests/rpc_roundtrip.rs
(Apache-2.0), Copyright the rust-mssql-driver contributors (praxiomlabs).
Upstream license: Apache License, Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0).
The cases were re-expressed against mssql_python's qmark parameters; no code was copied.

Each value is bound as a parameter, cast server-side to the target type and read
back, so the test exercises the parameter encoding path (RPC) and the result decoding
in one step. Values are chosen at type boundaries: float precision, UTF-16 surrogate
pairs, zero bytes, MAX dates, microsecond time, timezone offsets, money and decimal
scale limits, and NULL for each type.
"""

import datetime
import decimal
import uuid

import pytest


def _one(cursor, sql, params):
    cursor.execute(sql, params)
    return tuple(cursor.fetchone())[0]


def test_float_param_keeps_double_precision(cursor):
    assert _one(cursor, "SELECT CAST(? AS FLOAT)", [0.1]) == 0.1
    assert _one(cursor, "SELECT CAST(? AS FLOAT)", [1e308]) == 1e308
    assert _one(cursor, "SELECT CAST(? AS FLOAT)", [-2.2250738585072014e-308]) == -2.2250738585072014e-308


def test_real_param_rounds_to_single_precision(cursor):
    # 16777217 is not representable as a 24-bit mantissa; REAL rounds it to 16777216.
    assert _one(cursor, "SELECT CAST(? AS REAL)", [16777217.0]) == 16777216.0
    assert _one(cursor, "SELECT CAST(? AS REAL)", [1.5]) == 1.5


@pytest.mark.parametrize(
    "value",
    ["ascii only", "héllo wörld", "日本語のテキスト", "trailing spaces   ", "", "a\tb\nc"],
)
def test_nvarchar_param_round_trip(cursor, value):
    assert _one(cursor, "SELECT CAST(? AS NVARCHAR(100))", [value]) == value


def test_supplementary_plane_param_round_trip(cursor):
    # Each emoji and musical symbol is a UTF-16 surrogate pair on the wire.
    value = "😀𝄞🚀"
    assert _one(cursor, "SELECT CAST(? AS NVARCHAR(20))", [value]) == value


def test_varbinary_param_keeps_zero_bytes(cursor):
    value = b"\x00\x01\x00\xff\x00"
    assert _one(cursor, "SELECT CAST(? AS VARBINARY(16))", [value]) == value
    assert _one(cursor, "SELECT CAST(? AS VARBINARY(16))", [b""]) == b""


def test_uuid_param_round_trip(cursor):
    guid = uuid.UUID("6f1c2a3e-9d4b-4c8a-8e2f-0123456789ab")
    assert _one(cursor, "SELECT CAST(? AS UNIQUEIDENTIFIER)", [guid]) == guid
    assert _one(cursor, "SELECT CAST(? AS UNIQUEIDENTIFIER)", [uuid.UUID(int=0)]) == uuid.UUID(int=0)


@pytest.mark.parametrize("value", [2**63 - 1, -(2**63), 0, -1])
def test_bigint_param_extremes(cursor, value):
    assert _one(cursor, "SELECT CAST(? AS BIGINT)", [value]) == value


def test_bit_param_round_trip(cursor):
    assert _one(cursor, "SELECT CAST(? AS BIT)", [True]) is True
    assert _one(cursor, "SELECT CAST(? AS BIT)", [False]) is False


def test_date_param_max_year(cursor):
    assert _one(cursor, "SELECT CAST(? AS DATE)", [datetime.date(9999, 12, 31)]) == datetime.date(9999, 12, 31)


@pytest.mark.upstream_bug(
    reason="upstream rejects DATE parameters before 1753 with 'Date out of range (1753-9999)'; "
    "SQL Server DATE accepts 0001-01-01 onward"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_date_param_min_year(cursor):
    assert _one(cursor, "SELECT CAST(? AS DATE)", [datetime.date(1, 1, 1)]) == datetime.date(1, 1, 1)


def test_time_param_keeps_microseconds(cursor):
    value = datetime.time(23, 59, 59, 999999)
    assert _one(cursor, "SELECT CAST(? AS TIME(6))", [value]) == value


def test_datetime_param_exact_millisecond(cursor):
    # DATETIME has ~3.33 ms resolution; .500 is exactly representable so it must survive.
    value = datetime.datetime(2020, 1, 1, 0, 0, 0, 500000)
    assert _one(cursor, "SELECT CAST(? AS DATETIME)", [value]) == value


def test_datetime2_param_full_precision(cursor):
    value = datetime.datetime(2024, 2, 29, 12, 34, 56, 123456)
    assert _one(cursor, "SELECT CAST(? AS DATETIME2(6))", [value]) == value


def test_datetimeoffset_param_preserves_instant_and_offset(cursor):
    tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    value = datetime.datetime(2024, 7, 1, 8, 15, 0, 250000, tzinfo=tz)

    got = _one(cursor, "SELECT CAST(? AS DATETIMEOFFSET(6))", [value])

    assert got == value
    assert got.utcoffset() == datetime.timedelta(hours=5, minutes=30)


def test_decimal_param_keeps_scale(cursor):
    value = decimal.Decimal("12345678901234567890.123456789")
    assert _one(cursor, "SELECT CAST(? AS DECIMAL(38,9))", [value]) == value


def test_decimal_param_max_precision_negative(cursor):
    # Build from int: unary minus on a Decimal would round to the 28-digit default context.
    value = decimal.Decimal(-(10**38 - 1))
    assert _one(cursor, "SELECT CAST(? AS DECIMAL(38,0))", [value]) == value


def test_money_param_four_decimal_places(cursor):
    assert _one(cursor, "SELECT CAST(? AS MONEY)", [decimal.Decimal("1.2345")]) == decimal.Decimal("1.2345")
    low = decimal.Decimal("-922337203685477.5808")
    assert _one(cursor, "SELECT CAST(? AS MONEY)", [low]) == low


@pytest.mark.parametrize(
    "sql_type",
    ["INT", "NVARCHAR(10)", "VARBINARY(10)", "DATETIME2(3)", "UNIQUEIDENTIFIER", "DECIMAL(10,2)"],
)
def test_null_param_is_sql_null_for_each_type(cursor, sql_type):
    assert _one(cursor, f"SELECT CAST(? AS {sql_type})", [None]) is None
