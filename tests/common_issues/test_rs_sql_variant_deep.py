"""sql_variant deep dive: base types, NULL variants, parameter binding, fetch, comparisons.

Research sources (real reported problems and documented behavior this suite guards against):
- https://github.com/microsoft/mssql-python/pull/446  (sql_variant support added: base type maps to int/float/Decimal/str/bytes/UUID/NULL)
- https://techcommunity.microsoft.com/blog/sqlserver/mssql-python-1-5-apache-arrow-sql-variant-and-native-uuids/4510363  (1.5.0: sql_variant resolved to its base type at fetch time; streaming path)
- https://github.com/mkleehammer/pyodbc/issues/307  ("Deciphering SQL_VARIANT values": a converter receives raw bytes with no base-type metadata)
- https://github.com/mkleehammer/pyodbc/issues/839  (sql_variant reported as an unsupported ODBC type in pyodbc)
- https://github.com/TimelordUK/node-sqlserver-v8/issues/497  (sql_variant column read using the FIRST row's base type: wrong values or failed reads on later rows)
- https://www.red-gate.com/hub/product-learning/sql-prompt/problems-caused-by-use-of-the-sql_variant-datatype/  (implicit conversions, NULL base types, storing data as variant)
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/sql-variant-transact-sql  (NULL variants carry no base type; sql_variant cannot hold sql_variant or MAX types; 8000-byte limit)
- https://learn.microsoft.com/en-us/sql/t-sql/functions/sql-variant-property-transact-sql  (BaseType, MaxLength, Precision, Scale, Collation; NULL for invalid input)
- https://learn.microsoft.com/en-us/sql/relational-databases/native-client-odbc-results/mapping-data-types-odbc  (ODBC has no variant concept; SQL_CA_SS_VARIANT_TYPE is needed to learn the base type)

Ground truth is the upstream ODBC driver (mssql_python 1.16.0, .venv-upstream).
Native-TDS pitfalls this file targets: TDS sql_variant base-type tokens (0x62 + type id + propbytes),
per-row dispatch on the base type, NULL variants with no base type, datetimeoffset carried as
offset-aware values, executemany type inference, and fixed-width NCHAR padding inside a variant.
"""

import datetime as dt
import decimal
import uuid

import pytest
from mssql_python import DataError

GUID_TEXT = "6F9619FF-8B86-D011-B42D-00C04FC964FF"
GUID = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


# (T-SQL expression, expected BaseType, expected Python value)
# Covers every base type a sql_variant can carry.
BASE_TYPE_CASES = [
    ("CAST(42 AS INT)", "int", 42),
    ("CAST(9000000000 AS BIGINT)", "bigint", 9000000000),
    ("CAST(-7 AS SMALLINT)", "smallint", -7),
    ("CAST(3 AS TINYINT)", "tinyint", 3),
    ("CAST(1 AS BIT)", "bit", True),
    ("CAST(1.5 AS DECIMAL(10,3))", "decimal", decimal.Decimal("1.500")),
    ("CAST(12.34 AS MONEY)", "money", decimal.Decimal("12.3400")),
    ("CAST(2.5 AS SMALLMONEY)", "smallmoney", decimal.Decimal("2.5000")),
    ("CAST(1.5 AS FLOAT)", "float", 1.5),
    ("CAST(0.5 AS REAL)", "real", 0.5),
    ("CAST(N'héllo' AS NVARCHAR(20))", "nvarchar", "héllo"),
    ("CAST('abc' AS VARCHAR(5))", "varchar", "abc"),
    ("CAST(N'x' AS NCHAR(3))", "nchar", "x  "),
    ("CAST(0xDEAD AS VARBINARY(4))", "varbinary", b"\xde\xad"),
    (f"CAST('{GUID_TEXT}' AS UNIQUEIDENTIFIER)", "uniqueidentifier", GUID),
    ("CAST('2024-01-02' AS DATE)", "date", dt.date(2024, 1, 2)),
    ("CAST('12:34:56.1234567' AS TIME(7))", "time", dt.time(12, 34, 56, 123456)),
    ("CAST('2024-01-02T03:04:05' AS DATETIME)", "datetime", dt.datetime(2024, 1, 2, 3, 4, 5)),
    ("CAST('2024-01-02 03:04:00' AS SMALLDATETIME)", "smalldatetime", dt.datetime(2024, 1, 2, 3, 4)),
    (
        "CAST('2024-01-02T03:04:05.1234567' AS DATETIME2(7))",
        "datetime2",
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
    ),
]


def _insert_variant(cursor, row_id, expression):
    cursor.execute(f"INSERT INTO #v (id, v) VALUES (?, {expression})", (row_id,))


def _variant_rows_one_by_one(cursor, row_ids):
    """Read (id, BaseType, value) one row per query.

    Multi-row results with BaseType are unreliable upstream (see
    test_basetype_in_multirow_result_is_not_truncated), so value tests read per row.
    """
    rows = []
    for row_id in row_ids:
        cursor.execute(
            "SELECT id, SQL_VARIANT_PROPERTY(v, 'BaseType'), v FROM #v WHERE id = ?", (row_id,)
        )
        rows.append(tuple(cursor.fetchone()))
    return rows


def test_every_base_type_roundtrips_with_its_python_type(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    for row_id, (expression, _, _) in enumerate(BASE_TYPE_CASES, start=1):
        _insert_variant(cursor, row_id, expression)

    rows = _variant_rows_one_by_one(cursor, range(1, len(BASE_TYPE_CASES) + 1))

    for (row_id, base_type, value), (expression, want_base, want_value) in zip(
        rows, BASE_TYPE_CASES
    ):
        assert base_type == want_base, f"{expression}: BaseType"
        assert type(value) is type(want_value), f"{expression}: python type {type(value)}"
        assert repr(value) == repr(want_value), f"{expression}: value"


def test_null_variants_have_no_base_type_and_match_is_null(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST(NULL AS INT)")
    _insert_variant(cursor, 2, "NULL")
    _insert_variant(cursor, 3, "CAST(NULL AS NVARCHAR(10))")
    _insert_variant(cursor, 4, "CAST(N' ' AS NVARCHAR(1))")

    rows = _variant_rows_one_by_one(cursor, [1, 2, 3, 4])
    assert rows[:3] == [(1, None, None), (2, None, None), (3, None, None)]
    # A single space is a real nvarchar value, not a NULL variant.
    assert rows[3] == (4, "nvarchar", " ")

    cursor.execute("SELECT id FROM #v WHERE v IS NULL ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [1, 2, 3]


@pytest.mark.upstream_bug(
    reason=(
        "A sql_variant holding an empty string cannot be fetched: CAST(N'' AS NVARCHAR(1)) raises "
        "'Error fetching LOB for column 3' and CAST('' AS VARCHAR(1)) raises 'SQLGetData failed'. "
        "The value is a real nvarchar/varchar of length 0 and must come back as ''."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_empty_string_variant_fetches_as_empty_string(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST(N'' AS NVARCHAR(1))")
    rows = _variant_rows_one_by_one(cursor, [1])
    assert rows == [(1, "nvarchar", "")]


def test_python_params_bind_to_expected_base_types(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    cases = [
        (1, "tinyint", 1),  # small Python int binds to the smallest integer type
        (2**40, "bigint", 2**40),
        (1.25, "float", 1.25),
        ("text", "varchar", "text"),
        (b"\x01\x02", "varbinary", b"\x01\x02"),
        (decimal.Decimal("3.14"), "numeric", decimal.Decimal("3.14")),
        (True, "bit", True),
        (uuid.UUID(int=5), "uniqueidentifier", uuid.UUID(int=5)),
        (dt.datetime(2024, 5, 6, 7, 8, 9, 123456), "datetime2", dt.datetime(2024, 5, 6, 7, 8, 9, 123456)),
        (dt.date(2024, 1, 2), "date", dt.date(2024, 1, 2)),
        (dt.time(1, 2, 3), "time", dt.time(1, 2, 3)),
        (None, None, None),
    ]
    for row_id, (py_value, _, _) in enumerate(cases, start=1):
        cursor.execute("INSERT INTO #v (id, v) VALUES (?, ?)", (row_id, py_value))

    rows = _variant_rows_one_by_one(cursor, range(1, len(cases) + 1))
    for (row_id, base_type, value), (py_value, want_base, want_value) in zip(rows, cases):
        assert base_type == want_base, f"param {py_value!r}: BaseType"
        assert repr(value) == repr(want_value), f"param {py_value!r}: value"


@pytest.mark.upstream_bug(
    reason=(
        "In a multi-row result, SQL_VARIANT_PROPERTY(v, 'BaseType') for a later row is truncated "
        "when the first row's base type name is shorter: 'uniqueidentifier' comes back as 'er' "
        "(and a value column in the same result can then fail with an LOB fetch error). A plain "
        "nvarchar control column is read correctly."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_basetype_in_multirow_result_is_not_truncated(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST(3 AS TINYINT)")
    _insert_variant(cursor, 2, f"CAST('{GUID_TEXT}' AS UNIQUEIDENTIFIER)")
    cursor.execute("SELECT id, SQL_VARIANT_PROPERTY(v, 'BaseType') FROM #v ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "tinyint"), (2, "uniqueidentifier")]


@pytest.mark.upstream_bug(
    reason=(
        "A datetimeoffset stored in sql_variant comes back as a str such as "
        "'2024-01-02 03:04:05.1234567 +05:30' instead of an offset-aware datetime, "
        "although the same value read from a DATETIMEOFFSET column is a datetime."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_datetimeoffset_in_variant_returns_aware_datetime(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(
        cursor, 1, "CAST('2024-01-02T03:04:05.1234567+05:30' AS DATETIMEOFFSET(7))"
    )
    cursor.execute("SELECT SQL_VARIANT_PROPERTY(v, 'BaseType'), v FROM #v")
    base_type, value = cursor.fetchone()
    assert base_type == "datetimeoffset"
    assert isinstance(value, dt.datetime)
    assert value.utcoffset() == dt.timedelta(hours=5, minutes=30)
    assert value.replace(tzinfo=None) == dt.datetime(2024, 1, 2, 3, 4, 5, 123456)


@pytest.mark.upstream_bug(
    reason=(
        "fetchall() over a sql_variant column raises 'SQLGetData failed for NVARCHAR column' "
        "when a datetimeoffset row follows a row of another base type; the whole result set "
        "should be returned."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_fetchall_with_datetimeoffset_after_other_base_type(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST(1 AS INT)")
    _insert_variant(cursor, 2, "CAST('2024-01-02T03:04:05.1234567+05:30' AS DATETIMEOFFSET(7))")
    cursor.execute("SELECT id, v FROM #v ORDER BY id")
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [1, 2]


def test_fetchall_keeps_per_row_base_types_when_first_row_is_datetimeoffset(cursor):
    # A later row must not be read with the base type of the first row
    # (the node-sqlserver-v8 #497 failure mode).
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST('2024-01-02T03:04:05.1234567+05:30' AS DATETIMEOFFSET(7))")
    _insert_variant(cursor, 2, "CAST(42 AS INT)")
    _insert_variant(cursor, 3, "CAST(N'h' AS NVARCHAR(4))")
    _insert_variant(cursor, 4, "CAST(0xDE AS VARBINARY(4))")
    _insert_variant(cursor, 5, f"CAST('{GUID_TEXT}' AS UNIQUEIDENTIFIER)")
    _insert_variant(cursor, 6, "CAST('2024-01-02' AS DATE)")
    cursor.execute("SELECT id, v FROM #v ORDER BY id")
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6]
    assert [type(r[1]) for r in rows[1:]] == [int, str, bytes, uuid.UUID, dt.date]
    assert rows[1][1] == 42 and rows[2][1] == "h" and rows[3][1] == b"\xde"
    assert rows[4][1] == GUID and rows[5][1] == dt.date(2024, 1, 2)


def test_multirow_values_with_mixed_variant_types_raises_dataerror(cursor):
    # Multi-row VALUES unifies column types on the server, so mixing int and nvarchar
    # in one statement fails. Rows must be inserted with one statement each.
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    with pytest.raises(DataError):
        cursor.execute(
            "INSERT INTO #v (id, v) VALUES (1, CAST(1 AS INT)), (2, CAST(N'h' AS NVARCHAR(4)))"
        )


def test_executemany_same_python_type_preserves_values(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    cursor.executemany("INSERT INTO #v (id, v) VALUES (?, ?)", [(1, 42), (2, 43), (3, None)])
    cursor.execute("SELECT id, v FROM #v ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, 42), (2, 43), (3, None)]


@pytest.mark.upstream_bug(
    reason=(
        "executemany binds a sql_variant parameter column with a fixed type inferred from the "
        "first row; a later str raises a raw RuntimeError ('Unable to cast Python instance of "
        "type str') instead of storing each value with its own base type."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_mixed_python_types_into_variant(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    cursor.executemany(
        "INSERT INTO #v (id, v) VALUES (?, ?)",
        [(1, 42), (2, "x"), (3, 1.5), (4, None), (5, b"\x01")],
    )
    cursor.execute("SELECT id, v FROM #v ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [
        (1, 42), (2, "x"), (3, 1.5), (4, None), (5, b"\x01"),
    ]


def test_where_equality_crosses_integer_and_string_families(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST(42 AS INT)")
    _insert_variant(cursor, 2, "CAST(42 AS BIGINT)")
    _insert_variant(cursor, 3, "CAST(43 AS INT)")
    _insert_variant(cursor, 4, "CAST(N'a' AS NVARCHAR(4))")
    _insert_variant(cursor, 5, "CAST('a' AS VARCHAR(4))")
    _insert_variant(cursor, 6, "NULL")

    cursor.execute("SELECT id FROM #v WHERE v = ? ORDER BY id", (42,))
    assert [r[0] for r in cursor.fetchall()] == [1, 2]

    cursor.execute("SELECT id FROM #v WHERE v = N'a' ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [4, 5]

    # A NULL parameter never matches with '=', not even a NULL variant.
    cursor.execute("SELECT id FROM #v WHERE v = ?", (None,))
    assert cursor.fetchall() == []


def test_order_by_puts_null_variants_first_and_orders_within_one_base_type(cursor):
    cursor.execute("CREATE TABLE #v (id INT NOT NULL PRIMARY KEY, v SQL_VARIANT)")
    _insert_variant(cursor, 1, "CAST(N'b' AS NVARCHAR(4))")
    _insert_variant(cursor, 2, "CAST(N'a' AS NVARCHAR(4))")
    _insert_variant(cursor, 3, "NULL")
    cursor.execute("SELECT id FROM #v ORDER BY v, id")
    assert [r[0] for r in cursor.fetchall()] == [3, 2, 1]


def test_sql_variant_property_max_length_precision_scale(cursor):
    cursor.execute(
        "SELECT SQL_VARIANT_PROPERTY(CAST(N'ab' AS SQL_VARIANT), 'MaxLength'), "
        "SQL_VARIANT_PROPERTY(CAST(1.5 AS DECIMAL(9,2)), 'Precision'), "
        "SQL_VARIANT_PROPERTY(CAST(1.5 AS DECIMAL(9,2)), 'Scale'), "
        "SQL_VARIANT_PROPERTY(CAST(NULL AS INT), 'BaseType'), "
        "SQL_VARIANT_PROPERTY(CAST(1 AS SQL_VARIANT), 'Bogus')"
    )
    # MaxLength counts bytes (UTF-16), Precision/Scale come from the stored decimal,
    # and an untyped NULL or an unknown property name yields NULL.
    assert tuple(cursor.fetchone()) == (4, 9, 2, None, None)


def test_nvarchar_4000_fits_and_nvarchar_max_is_rejected(cursor):
    cursor.execute("CREATE TABLE #v (v SQL_VARIANT)")
    value = "é" * 4000  # 8000 bytes: the sql_variant maximum
    cursor.execute("INSERT INTO #v (v) VALUES (CAST(? AS NVARCHAR(4000)))", (value,))
    cursor.execute("SELECT v, SQL_VARIANT_PROPERTY(v, 'MaxLength') FROM #v")
    got_value, max_length = cursor.fetchone()
    assert got_value == value
    assert max_length == 8000

    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #v (v) VALUES (CAST(N'x' AS NVARCHAR(MAX)))")
