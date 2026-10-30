"""Special SQL Server types: XML, sql_variant, CLR types, hierarchyid, rowversion, computed columns.

Common driver pain points covered here:

* XML must come back as ``str`` with its content intact, including documents
  far larger than 8000 bytes (the classic truncation bug).
* ``sql_variant`` must map each underlying base type to the matching Python
  type, keep NULLs as ``None`` and keep non-ASCII text intact.
* CLR types (geography, geometry, hierarchyid) must come back as their native
  binary serialization (``bytes``), match ``CAST(... AS VARBINARY)``, and be
  readable as text via ``.ToString()``.
* ``rowversion`` / ``timestamp`` come back as 8 raw bytes, change on every
  update, and reject explicit inserts.
* Computed columns are readable but not writable, and recompute on update.
"""

import datetime
import decimal
import uuid

import pytest
from mssql_python.exceptions import IntegrityError, ProgrammingError

# Serialized geography::Point(47.6, -122.3, 4326), as SQL Server stores it.
GEOGRAPHY_POINT_4326 = b'\xe6\x10\x00\x00\x01\x0c\xcd\xcc\xcc\xcc\xcc\xccG@33333\x93^\xc0'

# Serialized geometry::Point(1, 2, 0) (SRID 0).
GEOMETRY_POINT_1_2 = (
    b"\x00\x00\x00\x00\x01\x0c\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
)

# Serialized hierarchyid '/1/' and '/1/2/'.
HIERARCHYID_SLASH_1 = b"\x58"
HIERARCHYID_SLASH_1_2 = b"\x5b\x40"

UPSTREAM_SQL_VARIANT_TEXT_REASON = (
    "upstream corrupts sql_variant text values in multi-row result sets when the "
    "text follows a row whose value has a different length: N'nvarchar' comes back "
    "as 'ar' and N'hello world' as 'helloworld' (single-row reads are correct)"
)

UPSTREAM_TEMP_TABLE_LOST_REASON = (
    "upstream drops the session's temp tables when a parameterized statement fails "
    "(same SPID, the table is gone: 'Invalid object name'). Raw T-SQL keeps the "
    "temp table after the same failure."
)


def _one(cursor):
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _all(cursor):
    return [tuple(row) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------


def test_xml_round_trip_returns_str_with_content_intact(cursor):
    doc = '<root><item id="1">héllo</item><item id="2"/></root>'
    cursor.execute("CREATE TABLE #xml_rt (id INT, doc XML NULL)")
    cursor.execute("INSERT INTO #xml_rt (id, doc) VALUES (?, ?)", (1, doc))
    cursor.execute("SELECT doc FROM #xml_rt WHERE id = 1")
    value = _one(cursor)[0]
    assert type(value) is str
    assert value == doc


def test_xml_null_round_trips_as_none(cursor):
    cursor.execute("CREATE TABLE #xml_null (id INT, doc XML NULL)")
    cursor.execute("INSERT INTO #xml_null (id, doc) VALUES (?, ?)", (1, None))
    cursor.execute("SELECT doc FROM #xml_null")
    assert _one(cursor) == (None,)


def test_xml_large_document_is_not_truncated(cursor):
    # Well past the 8000-byte boundary where naive driver buffers truncate.
    items = "".join(f'<i n="{k}">{"é" * 20}</i>' for k in range(400))
    doc = f"<root>{items}</root>"
    assert len(doc) > 13000
    cursor.execute("CREATE TABLE #xml_big (id INT, doc XML NULL)")
    cursor.execute("INSERT INTO #xml_big (id, doc) VALUES (?, ?)", (1, doc))
    cursor.execute("SELECT doc FROM #xml_big WHERE id = 1")
    value = _one(cursor)[0]
    assert type(value) is str
    assert len(value) == len(doc)
    assert value == doc


def test_xml_declaration_is_accepted_and_stripped_by_server(cursor):
    # SQL Server parses the XML and re-serializes it without the declaration.
    cursor.execute("CREATE TABLE #xml_decl (id INT, doc XML NULL)")
    cursor.execute(
        "INSERT INTO #xml_decl (id, doc) VALUES (?, ?)",
        (1, '<?xml version="1.0" encoding="UTF-8"?><a>x</a>'),
    )
    cursor.execute("SELECT doc FROM #xml_decl WHERE id = 1")
    assert _one(cursor) == ("<a>x</a>",)


def test_xml_malformed_param_raises_programming_error(cursor):
    # Nothing is written to the table in this test, so losing the temp table
    # after the failure (see the upstream_bug test below) does not matter here.
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT CAST(? AS XML)", ("<a><b></a>",))
        cursor.fetchall()


@pytest.mark.upstream_bug(reason=UPSTREAM_TEMP_TABLE_LOST_REASON)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_failed_xml_param_statement_keeps_session_temp_table(cursor):
    cursor.execute("CREATE TABLE #xml_keep (id INT, doc XML NULL)")
    cursor.execute("INSERT INTO #xml_keep (id, doc) VALUES (1, NULL)")
    with pytest.raises(ProgrammingError):
        cursor.execute("INSERT INTO #xml_keep (id, doc) VALUES (2, ?)", ("<a><b></a>",))
    cursor.execute("SELECT COUNT(*) FROM #xml_keep")
    assert _one(cursor) == (1,)


# ---------------------------------------------------------------------------
# sql_variant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "expected", "expected_type"),
    [
        ("CAST(7 AS INT)", 7, int),
        ("CAST(-5 AS SMALLINT)", -5, int),
        ("CAST(3 AS TINYINT)", 3, int),
        ("CAST(42 AS BIGINT)", 42, int),
        ("CAST(1 AS BIT)", True, bool),
        ("CAST(2.25 AS DECIMAL(5,2))", decimal.Decimal("2.25"), decimal.Decimal),
        ("CAST(1.25 AS MONEY)", decimal.Decimal("1.25"), decimal.Decimal),
        ("CAST(12.5 AS FLOAT)", 12.5, float),
        ("CAST(0.5 AS REAL)", 0.5, float),
        ("CAST('2020-01-02' AS DATE)", datetime.date(2020, 1, 2), datetime.date),
        (
            "CAST('2020-01-02T03:04:05.123' AS DATETIME2(3))",
            datetime.datetime(2020, 1, 2, 3, 4, 5, 123000),
            datetime.datetime,
        ),
        (
            "CAST('2020-01-02T03:04:05.123' AS DATETIME)",
            datetime.datetime(2020, 1, 2, 3, 4, 5, 123000),
            datetime.datetime,
        ),
        (
            "CAST('12:30:45.1234567' AS TIME(7))",
            datetime.time(12, 30, 45, 123456),
            datetime.time,
        ),
        (
            "CAST('00000000-0000-0000-0000-000000000001' AS UNIQUEIDENTIFIER)",
            uuid.UUID("00000000-0000-0000-0000-000000000001"),
            uuid.UUID,
        ),
        ("CAST(0x0A AS VARBINARY(10))", b"\n", bytes),
        ("CAST(N'txt' AS NVARCHAR(10))", "txt", str),
        ("CAST('x' AS VARCHAR(5))", "x", str),
        ("NULL", None, type(None)),
    ],
)
def test_sql_variant_base_type_maps_to_python_type(cursor, expression, expected, expected_type):
    cursor.execute(f"SELECT CAST({expression} AS sql_variant)")
    value = _one(cursor)[0]
    assert value == expected
    assert type(value) is expected_type


def test_sql_variant_column_mixed_base_types_in_one_result(cursor):
    cursor.execute("CREATE TABLE #sv_mixed (id INT, v SQL_VARIANT NULL)")
    cursor.execute("INSERT INTO #sv_mixed (id, v) VALUES (1, 7)")
    cursor.execute("INSERT INTO #sv_mixed (id, v) VALUES (2, N'txt')")
    cursor.execute("INSERT INTO #sv_mixed (id, v) VALUES (3, NULL)")
    cursor.execute("INSERT INTO #sv_mixed (id, v) VALUES (4, CAST(2.25 AS DECIMAL(5,2)))")
    cursor.execute("INSERT INTO #sv_mixed (id, v) VALUES (5, 0x0A)")
    cursor.execute("SELECT id, v FROM #sv_mixed ORDER BY id")
    assert _all(cursor) == [
        (1, 7),
        (2, "txt"),
        (3, None),
        (4, decimal.Decimal("2.25")),
        (5, b"\n"),
    ]


@pytest.mark.upstream_bug(reason=UPSTREAM_SQL_VARIANT_TEXT_REASON)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_sql_variant_base_type_property_matches_stored_type(cursor):
    cursor.execute("CREATE TABLE #sv_prop (id INT, v SQL_VARIANT NULL)")
    cursor.execute("INSERT INTO #sv_prop (id, v) VALUES (1, 7)")
    cursor.execute("INSERT INTO #sv_prop (id, v) VALUES (2, N'txt')")
    cursor.execute(
        "SELECT id, SQL_VARIANT_PROPERTY(v, 'BaseType') FROM #sv_prop ORDER BY id"
    )
    assert _all(cursor) == [(1, "int"), (2, "nvarchar")]


@pytest.mark.upstream_bug(reason=UPSTREAM_SQL_VARIANT_TEXT_REASON)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_sql_variant_text_after_other_length_row_keeps_full_value(cursor):
    cursor.execute(
        "SELECT v FROM (VALUES (CAST(1 AS sql_variant)), "
        "(CAST(N'hello world' AS sql_variant))) t(v)"
    )
    assert _all(cursor) == [(1,), ("hello world",)]


def test_sql_variant_nonascii_text_round_trips(cursor):
    cursor.execute("SELECT CAST(N'é' AS sql_variant), CAST(N'中文' AS sql_variant)")
    assert _one(cursor) == ("é", "中文")


def test_sql_variant_supplementary_plane_character_round_trips(cursor):
    # NCHAR(55357) + NCHAR(56832) is the UTF-16 surrogate pair for U+1F600.
    cursor.execute(
        "SELECT CAST(CAST(N'a' + NCHAR(55357) + NCHAR(56832) + N'b' AS NVARCHAR(10)) AS sql_variant)"
    )
    assert _one(cursor) == ("a\U0001f600b",)


# ---------------------------------------------------------------------------
# CLR types: geography, geometry, hierarchyid
# ---------------------------------------------------------------------------


def test_geography_fetched_as_clr_bytes_matching_cast_to_varbinary(cursor):
    cursor.execute("CREATE TABLE #geog (id INT, g GEOGRAPHY NULL)")
    cursor.execute("INSERT INTO #geog (id, g) VALUES (1, geography::Point(47.6, -122.3, 4326))")
    cursor.execute("SELECT g, CAST(g AS VARBINARY(MAX)) FROM #geog WHERE id = 1")
    native, cast_bytes = _one(cursor)
    assert type(native) is bytes
    assert native == GEOGRAPHY_POINT_4326
    assert cast_bytes == native


def test_geography_bytes_param_round_trips_exactly(cursor):
    cursor.execute("CREATE TABLE #geog_rt (id INT, g GEOGRAPHY NULL)")
    cursor.execute("INSERT INTO #geog_rt (id, g) VALUES (1, ?)", (GEOGRAPHY_POINT_4326,))
    cursor.execute("INSERT INTO #geog_rt (id, g) VALUES (2, CAST(? AS geography))", (GEOGRAPHY_POINT_4326,))
    cursor.execute("SELECT id, g FROM #geog_rt ORDER BY id")
    assert _all(cursor) == [(1, GEOGRAPHY_POINT_4326), (2, GEOGRAPHY_POINT_4326)]


def test_geography_tostring_returns_wkt_text(cursor):
    cursor.execute("CREATE TABLE #geog_txt (id INT, g GEOGRAPHY NULL)")
    cursor.execute("INSERT INTO #geog_txt (id, g) VALUES (1, geography::Point(47.6, -122.3, 4326))")
    cursor.execute("SELECT g.ToString(), g.STAsText() FROM #geog_txt WHERE id = 1")
    to_string, as_text = _one(cursor)
    assert to_string == "POINT (-122.3 47.6)"
    assert as_text == to_string
    assert type(to_string) is str


def test_geography_invalid_bytes_raise_programming_error(cursor):
    cursor.execute("CREATE TABLE #geog_bad (id INT, g GEOGRAPHY NULL)")
    with pytest.raises(ProgrammingError):
        cursor.execute("INSERT INTO #geog_bad (id, g) VALUES (1, ?)", (b"\x00\x01",))


def test_geometry_fetched_as_clr_bytes_and_tostring(cursor):
    cursor.execute("CREATE TABLE #geom (id INT, m GEOMETRY NULL)")
    cursor.execute("INSERT INTO #geom (id, m) VALUES (1, geometry::Point(1, 2, 0))")
    cursor.execute("SELECT m, m.ToString(), CAST(m AS VARBINARY(MAX)) FROM #geom WHERE id = 1")
    native, text, cast_bytes = _one(cursor)
    assert type(native) is bytes
    assert native == GEOMETRY_POINT_1_2
    assert text == "POINT (1 2)"
    assert cast_bytes == native


def test_hierarchyid_fetched_as_bytes_and_tostring(cursor):
    cursor.execute("CREATE TABLE #hid (id INT, h HIERARCHYID NULL)")
    cursor.execute("INSERT INTO #hid (id, h) VALUES (1, hierarchyid::Parse('/1/2/'))")
    cursor.execute("SELECT h, h.ToString(), CAST(h AS VARBINARY(892)) FROM #hid WHERE id = 1")
    native, text, cast_bytes = _one(cursor)
    assert type(native) is bytes
    assert native == HIERARCHYID_SLASH_1_2
    assert text == "/1/2/"
    assert cast_bytes == native


def test_hierarchyid_accepts_text_and_binary_params(cursor):
    cursor.execute("CREATE TABLE #hid_in (id INT, h HIERARCHYID NULL)")
    cursor.execute("INSERT INTO #hid_in (id, h) VALUES (1, ?)", ("/1/",))
    cursor.execute("INSERT INTO #hid_in (id, h) VALUES (2, ?)", (HIERARCHYID_SLASH_1,))
    cursor.execute("INSERT INTO #hid_in (id, h) VALUES (3, ?)", (None,))
    cursor.execute("SELECT id, h, h.ToString() FROM #hid_in ORDER BY id")
    rows = _all(cursor)
    assert rows == [
        (1, HIERARCHYID_SLASH_1, "/1/"),
        (2, HIERARCHYID_SLASH_1, "/1/"),
        (3, None, None),
    ]


# ---------------------------------------------------------------------------
# rowversion / timestamp
# ---------------------------------------------------------------------------


def test_rowversion_is_8_bytes_and_increases_on_update(cursor):
    cursor.execute("CREATE TABLE #rv (id INT, rv ROWVERSION)")
    cursor.execute("INSERT INTO #rv (id) VALUES (1)")
    cursor.execute("SELECT rv FROM #rv WHERE id = 1")
    first = _one(cursor)[0]
    cursor.execute("UPDATE #rv SET id = 2 WHERE id = 1")
    cursor.execute("SELECT rv FROM #rv WHERE id = 2")
    second = _one(cursor)[0]
    assert type(first) is bytes
    assert len(first) == 8
    assert second != first
    assert int.from_bytes(second, "big") > int.from_bytes(first, "big")


def test_timestamp_alias_behaves_like_rowversion(cursor):
    cursor.execute("CREATE TABLE #ts_alias (id INT, ts TIMESTAMP)")
    cursor.execute("INSERT INTO #ts_alias (id) VALUES (1)")
    cursor.execute("SELECT ts FROM #ts_alias")
    value = _one(cursor)[0]
    assert type(value) is bytes
    assert len(value) == 8
    assert cursor.description[0][1] is bytes


def test_explicit_rowversion_insert_raises_integrity_error(cursor):
    cursor.execute("CREATE TABLE #rv_explicit (id INT, rv ROWVERSION)")
    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #rv_explicit (id, rv) VALUES (1, 0x0102030405060708)")


# ---------------------------------------------------------------------------
# Computed columns
# ---------------------------------------------------------------------------


def test_computed_columns_return_evaluated_values_and_types(cursor):
    cursor.execute(
        "CREATE TABLE #comp (a INT, b INT, total AS (a + b), "
        "doubled AS (a * 2) PERSISTED, label AS CAST(a AS NVARCHAR(10)) + N'x')"
    )
    cursor.execute("INSERT INTO #comp (a, b) VALUES (2, 3)")
    cursor.execute("SELECT a, b, total, doubled, label FROM #comp")
    assert _one(cursor) == (2, 3, 5, 4, "2x")
    assert [d[1] for d in cursor.description] == [int, int, int, int, str]


def test_computed_column_recomputes_after_update(cursor):
    cursor.execute("CREATE TABLE #comp_upd (a INT, b INT, total AS (a + b))")
    cursor.execute("INSERT INTO #comp_upd (a, b) VALUES (1, 1)")
    cursor.execute("UPDATE #comp_upd SET b = 10")
    cursor.execute("SELECT total FROM #comp_upd")
    assert _one(cursor) == (11,)


def test_insert_into_computed_column_raises_programming_error(cursor):
    cursor.execute("CREATE TABLE #comp_ro (a INT, b INT, total AS (a + b))")
    with pytest.raises(ProgrammingError):
        cursor.execute("INSERT INTO #comp_ro (a, b, total) VALUES (1, 1, 2)")
