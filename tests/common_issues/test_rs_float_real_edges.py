"""FLOAT/REAL edge cases: signed zero, subnormals, extremes, float(24) vs float(53),
exact round-trip of Python floats, REAL rounding and overflow, and REAL equality.

Research sources (issue trackers and docs checked while writing this module):
- https://github.com/mkleehammer/pyodbc/issues/302 (numeric binding, HY104 precision errors)
- https://github.com/pymssql/pymssql/issues/329 (float parameter bound as SQL REAL, SQLFLT4 vs SQLFLT8)
- https://github.com/pymssql/pymssql/issues/623 (float column values rounded on read)
- https://github.com/r-dbi/odbc/issues/300 (Inf written to SQL Server fails with a TDS RPC error)
- https://learn.microsoft.com/de-de/sql/t-sql/data-types/float-and-real-transact-sql
  (en-US canonical page; returned in search as the de-DE, ja-JP and nl-NL variants: REAL range
  ±3.40E+38 with smallest normal 1.18E-38, FLOAT(n) 1-24 maps to REAL and 25-53 to FLOAT)

No issue was found in microsoft/mssql-python or prisma/tiberius specifically about signed zero,
subnormals or REAL rounding. The NaN/Inf rejection and REAL precision/overflow cases are already
covered in test_integers_bool_float.py, so they are not repeated here.
"""

import math
import struct

import pytest
from mssql_python import DataError, Error


def _fetch_one(cursor):
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _fetch_all(cursor):
    return [tuple(row) for row in cursor.fetchall()]


def _bits64(value):
    """IEEE-754 double bit pattern, so -0.0 and 0.0 compare as different values."""
    return struct.pack(">d", value)


def _bits32(value):
    return struct.pack(">f", value)


def _f32(value):
    """Round a Python float to IEEE-754 single precision, as SQL REAL stores it."""
    return struct.unpack(">f", struct.pack(">f", value))[0]


# --- signed zero -------------------------------------------------------------


def test_negative_zero_keeps_sign_bit_in_float_column(cursor):
    cursor.execute("CREATE TABLE #f (f float NULL)")
    cursor.execute("INSERT INTO #f (f) VALUES (?)", [-0.0])
    cursor.execute("SELECT f FROM #f")
    value = _fetch_one(cursor)[0]
    assert value == 0.0
    assert math.copysign(1.0, value) == -1.0
    assert _bits64(value) == _bits64(-0.0)


def test_negative_zero_keeps_sign_bit_in_real_column(cursor):
    cursor.execute("CREATE TABLE #r (r real NULL)")
    cursor.execute("INSERT INTO #r (r) VALUES (?)", [-0.0])
    cursor.execute("SELECT r FROM #r")
    value = _fetch_one(cursor)[0]
    assert value == 0.0
    assert _bits64(value) == _bits64(-0.0)


def test_negative_zero_matches_positive_zero_in_where_clause(cursor):
    # IEEE: -0.0 == 0.0, so SQL equality must treat both as the same value.
    cursor.execute("CREATE TABLE #z (id int NOT NULL, f float NULL)")
    cursor.execute("INSERT INTO #z (id, f) VALUES (1, ?)", [-0.0])
    cursor.execute("INSERT INTO #z (id, f) VALUES (2, ?)", [1 / 3])
    cursor.execute("SELECT id FROM #z WHERE f = ?", [0.0])
    assert _fetch_all(cursor) == [(1,)]
    cursor.execute("SELECT id FROM #z WHERE f = ?", [-0.0])
    assert _fetch_all(cursor) == [(1,)]


# --- subnormals and extremes -------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [5e-324, 2.2250738585072014e-308 / 2, 1e-310, -5e-324],
    ids=["min_double_subnormal", "mid_subnormal", "1e-310", "neg_min_subnormal"],
)
def test_double_subnormals_roundtrip_bit_exact_in_float_column(cursor, value):
    cursor.execute("CREATE TABLE #f (f float NULL)")
    cursor.execute("INSERT INTO #f (f) VALUES (?)", [value])
    cursor.execute("SELECT f FROM #f")
    got = _fetch_one(cursor)[0]
    assert type(got) is float
    assert _bits64(got) == _bits64(value)


def test_double_extremes_roundtrip_bit_exact(cursor):
    values = [1.7976931348623157e308, -1.7976931348623157e308, 2.2250738585072014e-308, 1e308]
    cursor.execute("CREATE TABLE #f (id int NOT NULL, f float NULL)")
    for i, v in enumerate(values):
        cursor.execute("INSERT INTO #f (id, f) VALUES (?, ?)", [i, v])
    cursor.execute("SELECT f FROM #f ORDER BY id")
    got = [row[0] for row in _fetch_all(cursor)]
    assert [type(v) for v in got] == [float] * 4
    assert [_bits64(v) for v in got] == [_bits64(v) for v in values]


def test_double_subnormal_below_real_range_becomes_zero_in_real_column(cursor):
    # 5e-324 is far below the smallest REAL subnormal (~1.4e-45), so it rounds to +0.0.
    cursor.execute("CREATE TABLE #r (r real NULL)")
    cursor.execute("INSERT INTO #r (r) VALUES (?)", [5e-324])
    cursor.execute("SELECT r FROM #r")
    got = _fetch_one(cursor)[0]
    assert got == 0.0
    assert _bits64(got) == _bits64(0.0)


def test_real_subnormals_and_min_normal_roundtrip_exactly(cursor):
    smallest_real_subnormal = 2.0**-149  # 1.401298464324817e-45
    min_normal_real = 2.0**-126  # ~1.1754944e-38, the SQL Server doc's 1.18E-38 bound
    cursor.execute("CREATE TABLE #r (id int NOT NULL, r real NULL)")
    cursor.execute("INSERT INTO #r (id, r) VALUES (1, ?)", [smallest_real_subnormal])
    cursor.execute("INSERT INTO #r (id, r) VALUES (2, ?)", [min_normal_real])
    cursor.execute("SELECT r FROM #r ORDER BY id")
    rows = [row[0] for row in _fetch_all(cursor)]
    assert rows[0] == smallest_real_subnormal
    assert rows[1] == min_normal_real


def test_real_max_rounds_to_flt_max_and_beyond_overflows(cursor):
    flt_max = 3.4028234663852886e38
    cursor.execute("CREATE TABLE #r (id int NOT NULL, r real NULL)")
    cursor.connection.commit()
    # 3.4028235e38 is above FLT_MAX but rounds down to it, so it is accepted.
    cursor.execute("INSERT INTO #r (id, r) VALUES (1, ?)", [3.4028235e38])
    # 3.5e38 is past the rounding threshold and must raise, not store infinity.
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #r (id, r) VALUES (2, ?)", [3.5e38])
    cursor.connection.commit()
    cursor.execute("SELECT id, r FROM #r ORDER BY id")
    assert _fetch_all(cursor) == [(1, flt_max)]


# --- rounding into REAL ------------------------------------------------------


def test_double_to_real_rounds_to_nearest_not_truncates(cursor):
    # 1 + 0.75 * 2**-23 lies between 1 + 2**-23 and 1.0 but is closer to 1 + 2**-23.
    # Round-to-nearest gives 1 + 2**-23; truncation would give 1.0.
    value = 1 + 0.75 * 2.0**-23
    cursor.execute("CREATE TABLE #r (r real NULL)")
    cursor.execute("INSERT INTO #r (r) VALUES (?)", [value])
    cursor.execute("SELECT r FROM #r")
    assert _fetch_one(cursor)[0] == 1 + 2.0**-23


def test_real_equality_only_matches_the_float32_value(cursor):
    # The stored REAL is the float32 value, so comparing with the raw double 0.1 finds
    # nothing, while the float32 rounding of 0.1 finds the row.
    cursor.execute("CREATE TABLE #r (id int NOT NULL, r real NULL)")
    cursor.execute("INSERT INTO #r (id, r) VALUES (1, ?)", [0.1])
    cursor.execute("SELECT id FROM #r WHERE r = ?", [0.1])
    assert _fetch_all(cursor) == []
    cursor.execute("SELECT id FROM #r WHERE r = ?", [_f32(0.1)])
    assert _fetch_all(cursor) == [(1,)]


# --- float(n) mapping and exact round-trips ----------------------------------


def test_float_n_precision_maps_to_real_for_1_to_24_and_float_for_25_to_53(cursor):
    cursor.execute(
        "CREATE TABLE #types (c24 float(24) NULL, c1 float(1) NULL, "
        "c25 float(25) NULL, c53 float(53) NULL, cdef float NULL)"
    )
    cursor.connection.commit()
    cursor.execute(
        "SELECT c.name, t.name FROM tempdb.sys.columns c "
        "JOIN tempdb.sys.types t ON c.user_type_id = t.user_type_id "
        "WHERE c.object_id = OBJECT_ID('tempdb..#types') ORDER BY c.column_id"
    )
    assert _fetch_all(cursor) == [
        ("c24", "real"),
        ("c1", "real"),
        ("c25", "float"),
        ("c53", "float"),
        ("cdef", "float"),
    ]


def test_float24_column_stores_float32_value_and_float53_keeps_double(cursor):
    third = 1 / 3
    cursor.execute("CREATE TABLE #t (id int NOT NULL, f24 float(24) NULL, f53 float(53) NULL)")
    cursor.execute("INSERT INTO #t (id, f24, f53) VALUES (1, ?, ?)", [third, third])
    cursor.execute("SELECT f24, f53 FROM #t WHERE id = 1")
    f24, f53 = _fetch_one(cursor)
    assert f24 == _f32(third)
    assert f24 != third
    assert f53 == third


@pytest.mark.parametrize(
    "value",
    [
        0.1 + 0.2,
        math.pi,
        math.e,
        1e-7,
        123456789.123456789,
        -1.5e-300,
        2.0**53 + 2.0,
        float(2**63),
    ],
)
def test_float_column_roundtrips_python_float_repr_exactly(cursor, value):
    # repr() must round-trip: the value read back has to print the same as the one sent.
    cursor.execute("CREATE TABLE #f (f float NULL)")
    cursor.execute("INSERT INTO #f (f) VALUES (?)", [value])
    cursor.execute("SELECT f FROM #f")
    got = _fetch_one(cursor)[0]
    assert repr(got) == repr(value)
    assert _bits64(got) == _bits64(value)


# --- server-side arithmetic and NaN handling ---------------------------------


def test_float_overflow_in_server_arithmetic_raises_dataerror(cursor):
    # 1e308 * 10 overflows FLOAT on the server. That is a value-range error, not a silent inf.
    cursor.execute("CREATE TABLE #f (f float NULL)")
    cursor.execute("INSERT INTO #f (f) VALUES (?)", [1e308])
    with pytest.raises(DataError):
        cursor.execute("SELECT f * 10 FROM #f")


def test_nan_insert_is_a_dbapi_error_and_connection_stays_usable(cursor):
    cursor.execute("CREATE TABLE #f (id int NOT NULL, f float NULL)")
    cursor.execute("INSERT INTO #f (id, f) VALUES (1, 2.5)")
    cursor.connection.commit()
    # The NaN insert must fail as a DB-API error without corrupting the session.
    with pytest.raises(Error):
        cursor.execute("INSERT INTO #f (id, f) VALUES (2, ?)", [float("nan")])
    cursor.connection.rollback()
    cursor.execute("SELECT id, f FROM #f ORDER BY id")
    assert _fetch_all(cursor) == [(1, 2.5)]
