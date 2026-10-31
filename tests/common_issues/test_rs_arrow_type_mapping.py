"""Arrow fetch API type mapping: cursor.arrow(), arrow_batch(), arrow_reader().

Sources (research for this suite):

* https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/arrow-integration
  (SQL-to-Arrow type table; datetimeoffset is converted to UTC because Arrow
  timestamp columns need a fixed timezone; sql_variant is not supported by the
  Arrow fetch methods and raises an unsupported-type exception.)
* https://github.com/microsoft/mssql-python/pull/354 ("FEAT: Add arrow fetch
  support": introduces arrow_batch(), arrow(), arrow_reader().)
* https://techcommunity.microsoft.com/blog/sqlserver/mssql-python-1-5-apache-arrow-sql-variant-and-native-uuids/4510363
  (1.5.0 release: Arrow fetch, sql_variant, native UUIDs.)
* https://github.com/microsoft/mssql-python/issues/708 (Arrow type-combination
  pitfalls: Utf8View strings and money columns need pa.decimal128(19, 4) on
  the Arrow side; a Python driver must reject or map Arrow types deliberately.)
* https://github.com/microsoft/mssql-python/issues/855 (1.16.0 bulk-copy/Arrow
  packaging regression; read path not affected, listed for context.)

Focus: exact Arrow type for every SQL type, precision/scale preservation,
time-zone normalisation, truncation of sub-microsecond precision, NULLs,
empty results, multi-batch chunking and large values. Assertions are against
the upstream ODBC driver's observed behaviour, which matches the documented
mapping.
"""

import datetime
import decimal
import struct
import uuid

import pytest

pa = pytest.importorskip("pyarrow")

# Columns that come back as large_string (upstream text mapping for every
# character type, regardless of declared length).
TEXT_TYPE = "large_string"


def _run(cursor, sql, *params):
    if params:
        cursor.execute(sql, *params)
    else:
        cursor.execute(sql)
    return cursor.arrow()


def _schema(table):
    return {field.name: str(field.type) for field in table.schema}


def _row(table, index=0):
    return table.to_pylist()[index]


def test_integer_family_maps_to_exact_arrow_widths(cursor):
    table = _run(
        cursor,
        "SELECT CAST(2147483647 AS INT) AS i, "
        "CAST(9223372036854775807 AS BIGINT) AS bi, "
        "CAST(-32768 AS SMALLINT) AS si, "
        "CAST(255 AS TINYINT) AS ti",
    )
    assert _schema(table) == {
        "i": "int32",
        "bi": "int64",
        "si": "int16",
        "ti": "uint8",
    }
    assert _row(table) == {
        "i": 2147483647,
        "bi": 9223372036854775807,
        "si": -32768,
        "ti": 255,
    }


def test_bit_maps_to_bool_with_null(cursor):
    table = _run(cursor, "SELECT CAST(1 AS BIT) AS b0, CAST(0 AS BIT) AS b1, CAST(NULL AS BIT) AS b2")
    assert _schema(table) == {"b0": "bool", "b1": "bool", "b2": "bool"}
    assert _row(table) == {"b0": True, "b1": False, "b2": None}


def test_decimal_keeps_declared_precision_and_scale(cursor):
    table = _run(
        cursor,
        "SELECT CAST(1.5 AS DECIMAL(10,3)) AS d1, "
        "CAST(-12345.678 AS DECIMAL(10,3)) AS d2, "
        "CAST(NULL AS DECIMAL(10,3)) AS d3",
    )
    assert _schema(table) == {
        "d1": "decimal128(10, 3)",
        "d2": "decimal128(10, 3)",
        "d3": "decimal128(10, 3)",
    }
    row = _row(table)
    # Trailing zero scale is preserved: 1.500, not 1.5.
    assert row["d1"] == decimal.Decimal("1.500")
    assert row["d1"].as_tuple().exponent == -3
    assert row["d2"] == decimal.Decimal("-12345.678")
    assert row["d3"] is None


def test_decimal_38_digits_is_exact(cursor):
    big = decimal.Decimal("12345678901234567890123456789012345678")
    table = _run(cursor, "SELECT CAST(12345678901234567890123456789012345678 AS DECIMAL(38,0)) AS d")
    assert _schema(table) == {"d": "decimal128(38, 0)"}
    assert _row(table) == {"d": big}


def test_money_and_smallmoney_map_to_decimal_4_places(cursor):
    table = _run(
        cursor,
        "SELECT CAST(922337203685477.5807 AS MONEY) AS m, "
        "CAST(-214748.3648 AS SMALLMONEY) AS sm, "
        "CAST(NULL AS MONEY) AS mn",
    )
    assert _schema(table) == {
        "m": "decimal128(19, 4)",
        "sm": "decimal128(10, 4)",
        "mn": "decimal128(19, 4)",
    }
    row = _row(table)
    assert row["m"] == decimal.Decimal("922337203685477.5807")
    assert row["sm"] == decimal.Decimal("-214748.3648")
    assert row["mn"] is None


def test_float_is_double_and_real_is_float32(cursor):
    table = _run(cursor, "SELECT CAST(0.1 AS FLOAT) AS f, CAST(0.1 AS REAL) AS r")
    assert _schema(table) == {"f": "double", "r": "float"}
    row = _row(table)
    # FLOAT is an IEEE double: 0.1 round-trips exactly.
    assert row["f"] == 0.1
    # REAL is an IEEE single: the value must equal the float32 rounding of 0.1.
    expected_real = struct.unpack("f", struct.pack("f", 0.1))[0]
    assert row["r"] == expected_real
    assert row["r"] != 0.1


def test_date_and_time_map_to_date32_and_time64_ns(cursor):
    table = _run(
        cursor,
        "SELECT CAST('2024-01-02' AS DATE) AS dt, "
        "CAST('12:34:56.123' AS TIME(3)) AS tm, "
        "CAST(NULL AS TIME(3)) AS tmn",
    )
    assert _schema(table) == {
        "dt": "date32[day]",
        "tm": "time64[ns]",
        "tmn": "time64[ns]",
    }
    row = _row(table)
    assert row["dt"] == datetime.date(2024, 1, 2)
    assert row["tm"] == datetime.time(12, 34, 56, 123000)
    assert row["tmn"] is None


def test_datetime_maps_to_timestamp_us_without_timezone(cursor):
    table = _run(cursor, "SELECT CAST('2024-01-02 03:04:05' AS DATETIME) AS dtm")
    assert _schema(table) == {"dtm": "timestamp[us]"}
    assert _row(table) == {"dtm": datetime.datetime(2024, 1, 2, 3, 4, 5)}


def test_datetime2_truncates_to_microseconds(cursor):
    # DATETIME2(7) carries 100 ns ticks; Arrow timestamp[us] keeps microseconds.
    # The 7th digit is dropped (truncation, not rounding): .1234567 -> .123456.
    table = _run(
        cursor,
        "SELECT CAST('2024-01-02 03:04:05.1234567' AS DATETIME2(7)) AS dt2, "
        "CAST('0001-01-01' AS DATETIME2(7)) AS dt2min, "
        "CAST('9999-12-31 23:59:59.9999999' AS DATETIME2(7)) AS dt2max",
    )
    assert _schema(table) == {
        "dt2": "timestamp[us]",
        "dt2min": "timestamp[us]",
        "dt2max": "timestamp[us]",
    }
    row = _row(table)
    assert row["dt2"] == datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    assert row["dt2min"] == datetime.datetime(1, 1, 1, 0, 0, 0)
    assert row["dt2max"] == datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)


def test_datetimeoffset_is_normalised_to_utc(cursor):
    # Arrow needs one fixed timezone per column, so the offset is folded into UTC
    # and the original +05:30 offset is not kept in the value.
    table = _run(
        cursor,
        "SELECT CAST('2024-01-02 03:04:05.1234567 +05:30' AS DATETIMEOFFSET(7)) AS dto",
    )
    assert _schema(table) == {"dto": "timestamp[us, tz=+00:00]"}
    value = _row(table)["dto"]
    assert value == datetime.datetime(2024, 1, 1, 21, 34, 5, 123456, tzinfo=datetime.timezone.utc)
    assert value.utcoffset() == datetime.timedelta(0)


def test_uniqueidentifier_is_uppercase_text(cursor):
    guid = "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"
    table = _run(
        cursor,
        f"SELECT CAST('{guid}' AS UNIQUEIDENTIFIER) AS u, CAST(NULL AS UNIQUEIDENTIFIER) AS un",
    )
    assert _schema(table) == {"u": TEXT_TYPE, "un": TEXT_TYPE}
    row = _row(table)
    assert row["u"] == guid
    assert row["u"] == str(uuid.UUID(guid)).upper()
    assert row["un"] is None


def test_char_varchar_nvarchar_are_all_large_string(cursor):
    table = _run(
        cursor,
        "SELECT CAST('ab' AS CHAR(5)) AS c, "
        "CAST('ab' AS VARCHAR(20)) AS vc, "
        "CAST(N'héllo' AS NVARCHAR(20)) AS nv, "
        "CAST(N'big' AS NVARCHAR(MAX)) AS nvm",
    )
    assert _schema(table) == {
        "c": TEXT_TYPE,
        "vc": TEXT_TYPE,
        "nv": TEXT_TYPE,
        "nvm": TEXT_TYPE,
    }
    row = _row(table)
    # CHAR(5) is padded with spaces by SQL Server; the value keeps the padding.
    assert row["c"] == "ab   "
    assert row["vc"] == "ab"
    assert row["nv"] == "héllo"
    assert row["nvm"] == "big"


def test_nvarchar_max_round_trips_large_value(cursor):
    text = ("abcdefghij" * 5000)[:50000]
    table = _run(cursor, "SELECT CAST(? AS NVARCHAR(MAX)) AS big", text)
    assert _schema(table) == {"big": TEXT_TYPE}
    value = _row(table)["big"]
    assert len(value) == 50000
    assert value == text


def test_nvarchar_keeps_non_bmp_characters(cursor):
    emoji = "\U0001F600"  # U+1F600 needs a UTF-16 surrogate pair in SQL Server.
    table = _run(cursor, "SELECT CAST(N'x\U0001F600y' AS NVARCHAR(10)) AS e")
    assert _schema(table) == {"e": TEXT_TYPE}
    assert _row(table)["e"] == "x" + emoji + "y"


def test_varbinary_maps_to_large_binary_with_empty_and_null(cursor):
    table = _run(
        cursor,
        "SELECT CAST(0x000001FF AS VARBINARY(20)) AS vb, "
        "CAST(0x AS VARBINARY(20)) AS empty, "
        "CAST(NULL AS VARBINARY(20)) AS vbn",
    )
    assert _schema(table) == {
        "vb": "large_binary",
        "empty": "large_binary",
        "vbn": "large_binary",
    }
    row = _row(table)
    assert row["vb"] == b"\x00\x00\x01\xff"
    assert row["empty"] == b""
    assert row["vbn"] is None


def test_xml_maps_to_large_string(cursor):
    table = _run(cursor, "SELECT CAST('<a>x</a>' AS XML) AS x, CAST(NULL AS XML) AS xn")
    assert _schema(table) == {"x": TEXT_TYPE, "xn": TEXT_TYPE}
    assert _row(table) == {"x": "<a>x</a>", "xn": None}


def test_all_null_row_keeps_full_schema(cursor):
    table = _run(
        cursor,
        "SELECT CAST(NULL AS INT) AS i, CAST(NULL AS BIGINT) AS bi, "
        "CAST(NULL AS BIT) AS b, CAST(NULL AS DECIMAL(10,3)) AS d, "
        "CAST(NULL AS MONEY) AS m, CAST(NULL AS REAL) AS r, "
        "CAST(NULL AS DATE) AS dt, CAST(NULL AS DATETIME2(7)) AS dt2, "
        "CAST(NULL AS DATETIMEOFFSET(7)) AS dto, CAST(NULL AS NVARCHAR(10)) AS nv, "
        "CAST(NULL AS VARBINARY(10)) AS vb",
    )
    assert _schema(table) == {
        "i": "int32",
        "bi": "int64",
        "b": "bool",
        "d": "decimal128(10, 3)",
        "m": "decimal128(19, 4)",
        "r": "float",
        "dt": "date32[day]",
        "dt2": "timestamp[us]",
        "dto": "timestamp[us, tz=+00:00]",
        "nv": TEXT_TYPE,
        "vb": "large_binary",
    }
    assert all(value is None for value in _row(table).values())
    assert table.num_rows == 1


def test_empty_result_keeps_schema_and_zero_rows(cursor):
    table = _run(
        cursor,
        "SELECT CAST(1 AS INT) AS i, CAST(1.5 AS DECIMAL(10,3)) AS d, "
        "CAST('2024-01-02 03:04:05' AS DATETIME) AS dtm, "
        "CAST(N'x' AS NVARCHAR(10)) AS nv WHERE 1 = 0",
    )
    assert table.num_rows == 0
    assert _schema(table) == {
        "i": "int32",
        "d": "decimal128(10, 3)",
        "dtm": "timestamp[us]",
        "nv": TEXT_TYPE,
    }
    assert table.to_pylist() == []


def _numbered_rows(top):
    return (
        "WITH n AS (SELECT TOP ({top}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS k "
        "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
        "SELECT CAST(k AS INT) AS k, CAST(k AS DECIMAL(10,2)) AS d, "
        "CAST(N'v' + CAST(k AS VARCHAR(10)) AS NVARCHAR(20)) AS s FROM n ORDER BY k"
    ).format(top=top)


def test_arrow_splits_result_into_batches_of_batch_size(cursor):
    cursor.execute(_numbered_rows(2500))
    table = cursor.arrow(1000)
    assert table.num_rows == 2500
    assert [chunk.num_rows for chunk in table.to_batches()] == [1000, 1000, 500]
    assert _schema(table) == {"k": "int32", "d": "decimal128(10, 2)", "s": TEXT_TYPE}
    # Row order is preserved across batch boundaries.
    values = table.column("k").to_pylist()
    assert values == list(range(1, 2501))
    assert table.column("s").to_pylist()[1000] == "v1001"


def test_arrow_batch_returns_one_batch_per_call_then_empty(cursor):
    cursor.execute(_numbered_rows(2500))
    first = cursor.arrow_batch(1000)
    second = cursor.arrow_batch(1000)
    third = cursor.arrow_batch(1000)
    fourth = cursor.arrow_batch(1000)
    assert [first.num_rows, second.num_rows, third.num_rows, fourth.num_rows] == [1000, 1000, 500, 0]
    assert isinstance(first, pa.RecordBatch)
    assert first.schema.equals(third.schema)
    assert first.column("k")[0].as_py() == 1
    assert second.column("k")[0].as_py() == 1001
    assert third.column("k")[-1].as_py() == 2500
    assert fourth.schema.equals(first.schema)


def test_arrow_reader_yields_same_rows_and_schema_as_arrow(cursor):
    cursor.execute(_numbered_rows(2500))
    reader = cursor.arrow_reader(400)
    table = reader.read_all()
    assert table.num_rows == 2500
    assert table.column("k").to_pylist() == list(range(1, 2501))
    assert _schema(table) == {"k": "int32", "d": "decimal128(10, 2)", "s": TEXT_TYPE}

    cursor.execute(_numbered_rows(2500))
    reference = cursor.arrow(400)
    assert reference.schema.equals(table.schema)


def test_sql_variant_is_rejected_by_arrow_fetch(cursor):
    # Documented limitation: sql_variant has no Arrow mapping and must raise
    # instead of silently returning a wrong type.
    cursor.execute("SELECT CAST(7 AS SQL_VARIANT) AS v")
    with pytest.raises(RuntimeError, match="Unsupported data type"):
        cursor.arrow()
