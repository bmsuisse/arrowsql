"""Integer, bit/bool, and float edge cases.

Covers how Python ints, bools and floats bind to and come back from SQL
Server columns: bit normalisation, tinyint/smallint/int/bigint bounds and
overflow, Python ints beyond bigint, non-finite floats, REAL precision loss,
and int parameters landing in decimal columns.
"""

import struct
from decimal import Decimal

import pytest
from mssql_python import DataError, Error


def _fetch_all(cursor):
    # mssql_python returns Row objects that do not compare equal to plain tuples,
    # so normalise to tuples for readable exact-value assertions.
    return [tuple(row) for row in cursor.fetchall()]


def _fetch_one(cursor):
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _as_float32(value):
    """Round a Python float to IEEE-754 single precision, as SQL REAL does."""
    return struct.unpack("f", struct.pack("f", value))[0]


# --- bit / bool -------------------------------------------------------------


def test_bit_column_roundtrips_bool_values(cursor):
    cursor.execute("CREATE TABLE #bits (id int NOT NULL, v bit NULL)")
    cursor.execute("INSERT INTO #bits (id, v) VALUES (1, ?)", [True])
    cursor.execute("INSERT INTO #bits (id, v) VALUES (2, ?)", [False])
    cursor.execute("SELECT id, v FROM #bits ORDER BY id")
    rows = _fetch_all(cursor)
    assert [type(v) for _, v in rows] == [bool, bool]
    assert rows[0][1] is True
    assert rows[1][1] is False


@pytest.mark.parametrize(
    "value, expected",
    [(1, True), (0, False), (2, True), (-1, True)],
)
def test_bit_column_normalises_ints_to_bool(cursor, value, expected):
    # Any non-zero integer is stored as bit 1 and read back as True.
    cursor.execute("CREATE TABLE #bits (v bit NULL)")
    cursor.execute("INSERT INTO #bits (v) VALUES (?)", [value])
    cursor.execute("SELECT v FROM #bits")
    got = _fetch_one(cursor)[0]
    assert got is expected


def test_bit_column_rejects_non_numeric_string(cursor):
    cursor.execute("CREATE TABLE #bits (v bit NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #bits (v) VALUES (?)", ["x"])


def test_bool_parameter_is_sent_as_bit_and_int_column_stores_one_or_zero(cursor):
    cursor.execute("CREATE TABLE #ints (n int NULL)")
    cursor.execute("INSERT INTO #ints (n) VALUES (?)", [True])
    cursor.execute("INSERT INTO #ints (n) VALUES (?)", [False])
    cursor.execute("SELECT n FROM #ints ORDER BY n")
    assert _fetch_all(cursor) == [(0,), (1,)]

    cursor.execute("SELECT ?, ?, CAST(? AS int)", [True, False, True])
    row = _fetch_one(cursor)
    assert row[0] is True
    assert row[1] is False
    assert type(row[2]) is int and row[2] == 1


# --- tinyint ----------------------------------------------------------------


def test_tinyint_bounds_roundtrip(cursor):
    cursor.execute("CREATE TABLE #t (v tinyint NULL)")
    cursor.execute("INSERT INTO #t (v) VALUES (?)", [0])
    cursor.execute("INSERT INTO #t (v) VALUES (?)", [255])
    cursor.execute("SELECT v FROM #t ORDER BY v")
    rows = _fetch_all(cursor)
    assert rows == [(0,), (255,)]
    assert all(type(v) is int for (v,) in rows)


@pytest.mark.parametrize("value", [256, -1])
def test_tinyint_out_of_range_raises_data_error_and_stores_nothing(cursor, value):
    cursor.execute("CREATE TABLE #t (v tinyint NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #t (v) VALUES (?)", [value])
    cursor.execute("SELECT COUNT(*) FROM #t")
    assert _fetch_one(cursor)[0] == 0


def test_tinyint_fractional_float_truncates_toward_zero(cursor):
    # 2.9 is truncated to 2 (not rounded to 3); 255.9 is truncated to 255.
    cursor.execute("CREATE TABLE #t (v tinyint NULL)")
    cursor.execute("INSERT INTO #t (v) VALUES (?)", [2.9])
    cursor.execute("INSERT INTO #t (v) VALUES (?)", [255.9])
    cursor.execute("SELECT v FROM #t ORDER BY v")
    assert _fetch_all(cursor) == [(2,), (255,)]


def test_numeric_string_into_tinyint_is_converted_or_overflows(cursor):
    cursor.execute("CREATE TABLE #t (v tinyint NULL)")
    cursor.execute("INSERT INTO #t (v) VALUES (?)", ["12"])
    cursor.execute("SELECT v FROM #t")
    assert _fetch_one(cursor) == (12,)

    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #t (v) VALUES (?)", ["300"])


# --- smallint / int / bigint ------------------------------------------------


def test_smallint_int_bigint_bounds_roundtrip_exactly(cursor):
    cursor.execute("CREATE TABLE #ints (s smallint NULL, i int NULL, b bigint NULL)")
    cursor.execute(
        "INSERT INTO #ints (s, i, b) VALUES (?, ?, ?)",
        [-32768, -2147483648, -9223372036854775808],
    )
    cursor.execute(
        "INSERT INTO #ints (s, i, b) VALUES (?, ?, ?)",
        [32767, 2147483647, 9223372036854775807],
    )
    cursor.execute("SELECT s, i, b FROM #ints ORDER BY s")
    assert _fetch_all(cursor) == [
        (-32768, -2147483648, -9223372036854775808),
        (32767, 2147483647, 9223372036854775807),
    ]


@pytest.mark.parametrize(
    "column, value",
    [("s", 32768), ("i", 2**31)],
)
def test_one_past_max_raises_data_error_for_small_int_types(cursor, column, value):
    cursor.execute("CREATE TABLE #ints (s smallint NULL, i int NULL)")
    with pytest.raises(DataError):
        cursor.execute(f"INSERT INTO #ints ({column}) VALUES (?)", [value])


def test_bigint_min_and_max_match_in_where_clause(cursor):
    cursor.execute("CREATE TABLE #b (b bigint NOT NULL)")
    cursor.execute("INSERT INTO #b (b) VALUES (?)", [-9223372036854775808])
    cursor.execute("INSERT INTO #b (b) VALUES (?)", [9223372036854775807])
    cursor.execute("SELECT b FROM #b WHERE b = ?", [9223372036854775807])
    assert _fetch_all(cursor) == [(9223372036854775807,)]
    cursor.execute("SELECT b FROM #b WHERE b = ?", [-9223372036854775808])
    assert _fetch_all(cursor) == [(-9223372036854775808,)]


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
def test_python_int_beyond_bigint_is_rejected_before_insert(cursor, value):
    cursor.execute("CREATE TABLE #b (b bigint NULL)")
    with pytest.raises(ValueError, match="out of range for SQL BIGINT"):
        cursor.execute("INSERT INTO #b (b) VALUES (?)", [value])
    cursor.execute("SELECT COUNT(*) FROM #b")
    assert _fetch_one(cursor)[0] == 0


@pytest.mark.upstream_bug(
    reason=(
        "Out-of-range Python int raises a bare ValueError instead of a PEP 249 "
        "DataError, so DB-API callers catching mssql_python.Error miss it"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_python_int_beyond_bigint_is_dbapi_data_error(cursor):
    cursor.execute("CREATE TABLE #b (b bigint NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #b (b) VALUES (?)", [2**63])


# --- non-finite floats ------------------------------------------------------


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_non_finite_float_parameter_is_rejected_and_not_stored(cursor, table_name, value):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (f float NULL)")
    cursor.connection.commit()
    with pytest.raises(Error):
        cursor.execute(f"INSERT INTO dbo.{table_name} (f) VALUES (?)", [value])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert _fetch_one(cursor)[0] == 0


@pytest.mark.upstream_bug(
    reason=(
        "NaN is a data-value problem and should raise DataError; upstream raises "
        "ProgrammingError with a TDS 'Syntax error or access violation' message"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_nan_float_parameter_raises_data_error(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (f float NULL)")
    cursor.connection.commit()
    with pytest.raises(DataError):
        cursor.execute(f"INSERT INTO dbo.{table_name} (f) VALUES (?)", [float("nan")])


# --- real / float precision -------------------------------------------------


def test_real_column_loses_precision_for_python_float(cursor):
    cursor.execute("CREATE TABLE #f (r real NULL, f float NULL)")
    cursor.execute("INSERT INTO #f (r, f) VALUES (?, ?)", [0.1, 0.1])
    cursor.execute("SELECT r, f FROM #f")
    r, f = _fetch_one(cursor)
    assert r == _as_float32(0.1)
    assert r == pytest.approx(0.10000000149011612, rel=0, abs=1e-18)
    assert r != 0.1
    assert f == 0.1


def test_real_cannot_represent_odd_integers_above_2_24(cursor):
    cursor.execute("CREATE TABLE #f (r real NULL)")
    cursor.execute("INSERT INTO #f (r) VALUES (?)", [16777217])
    cursor.execute("SELECT r FROM #f")
    assert _fetch_one(cursor)[0] == 16777216.0


def test_float_column_keeps_double_precision(cursor):
    third = 1 / 3
    cursor.execute("CREATE TABLE #f (f float NULL)")
    cursor.execute("INSERT INTO #f (f) VALUES (?)", [third])
    cursor.execute("SELECT f FROM #f")
    assert _fetch_one(cursor)[0] == third


def test_real_overflow_raises_and_underflow_silently_becomes_zero(cursor):
    cursor.execute("CREATE TABLE #f (r real NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #f (r) VALUES (?)", [1e39])
    cursor.execute("INSERT INTO #f (r) VALUES (?)", [1e-50])
    cursor.execute("SELECT r FROM #f")
    assert _fetch_all(cursor) == [(0.0,)]


# --- int parameter into decimal column --------------------------------------


def test_int_parameter_into_decimal_column_keeps_scale(cursor):
    cursor.execute("CREATE TABLE #d (d decimal(10,2) NULL)")
    for value in (42, -5, 99999999):
        cursor.execute("INSERT INTO #d (d) VALUES (?)", [value])
    cursor.execute("SELECT d FROM #d ORDER BY d")
    rows = _fetch_all(cursor)
    assert [r[0] for r in rows] == [
        Decimal("-5.00"),
        Decimal("42.00"),
        Decimal("99999999.00"),
    ]
    assert all(type(r[0]) is Decimal for r in rows)
    assert str(rows[1][0]) == "42.00"


def test_int_parameter_exceeding_decimal_precision_raises_data_error(cursor):
    # decimal(10,2) holds 8 integer digits; 100000000 needs 9.
    cursor.execute("CREATE TABLE #d (d decimal(10,2) NULL)")
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #d (d) VALUES (?)", [100000000])
    cursor.execute("SELECT COUNT(*) FROM #d")
    assert _fetch_one(cursor)[0] == 0
