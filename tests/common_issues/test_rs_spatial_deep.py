"""Deep geography/geometry behavior for the native TDS driver.

Sources (research, ~10 min):
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/spatial-data?view=sql-server-ver17
  mssql-python spatial guide: geography::STGeomFromText / geography::Point with qmark and %(name)s params,
  STAsText() for reading, STDistance in meters for geography, STIsValid / MakeValid
- https://techcommunity.microsoft.com/blog/sqlserver/mssql-python-1-4-bulk-copy-arrives---load-millions-of-rows-at-native-speed/4503171
  mssql-python 1.4 added spatial type support (geography, geometry, hierarchyid); qmark WKT insert example
- https://github.com/mkleehammer/pyodbc/issues/868
  STGeomFromText / STGeomFromBinary inserts with fast_executemany fail on long binary payloads
  (8000-byte buffer truncation); large spatial values are a known driver pain point
- https://github.com/tiberius-rs/tiberius/issues/257
  "Can't get geography type": CLR UDT columns unsupported by a Rust TDS client; geography/geometry arrive
  as CLR-serialized bytes that must not be mistaken for WKB or text
- https://github.com/panchenkoai/rivet/pull/364
  tiberius 0.13 returns CLR UDTs (hierarchyid, geometry, geography) as raw bytes; parsing is up to the caller
- https://learn.microsoft.com/en-us/sql/t-sql/spatial-geography/stgeomfromtext-geography-data-type
  T-SQL reference: invalid WKT raises; geography polygon ring orientation matters

Failure modes asserted here:
- geography/geometry columns come back as bytes in SQL Server's CLR serialization (SRID header first:
  4326 -> e6 10 00 00, version 01, properties 0c for a point), while STAsText() returns str and
  STAsBinary() returns standard WKB bytes; the driver must not confuse these
- WKB from STAsBinary: byte order 01, type 1 = point, two little-endian doubles
- WKB bytes bound as a parameter round-trip through STGeomFromWKB unchanged
- qmark WKT params, None (SQL NULL) params, and executemany with WKT params
- large polygons (WKT > 8000 chars, CLR value > 8000 bytes) must not be truncated
- geography SRID mismatch raises ProgrammingError (ArgumentException, 24204); geometry SRID mismatch
  silently yields NULL in STDistance/STIntersects instead of raising
- invalid WKT raises ProgrammingError (FormatException)
- Bow-tie geometry is stored, STIsValid() returns Python bool False, MakeValid() repairs it
- geography ring orientation: clockwise ring is interpreted as the complement area (~5.1e14 m^2)
- polygon intersection geometry carries floating-point noise; ordering by STDistance uses float
  results with NULL rows excluded
"""

import struct

import pytest
from mssql_python import ProgrammingError

GEOG_POINT_WKT = "POINT(-122.349 47.651)"
GEOG_POINT_TEXT = "POINT (-122.349 47.651)"


def _strip_polygon_wkt(half_length: int) -> str:
    """Thin closed polygon, counter-clockwise, integer lon/lat in range.

    Bottom edge: (0,0)..(n-1,0) east; top edge: (n-1,1)..(0,1) west; closed at (0,0).
    Total points = 2 * half_length + 1. Valid for geography (lat within -90..90).
    """
    pts = [(x, 0) for x in range(half_length)]
    pts += [(x, 1) for x in range(half_length - 1, -1, -1)]
    pts.append((0, 0))
    body = ", ".join(f"{x} {y}" for x, y in pts)
    return f"POLYGON(({body}))"


def _strip_polygon_text(half_length: int) -> str:
    """Server-side STAsText form of _strip_polygon_wkt (single space after POLYGON)."""
    pts = [(x, 0) for x in range(half_length)]
    pts += [(x, 1) for x in range(half_length - 1, -1, -1)]
    pts.append((0, 0))
    body = ", ".join(f"{x} {y}" for x, y in pts)
    return f"POLYGON (({body}))"


def test_geography_point_from_wkt_param_roundtrip(cursor):
    cursor.execute("CREATE TABLE #g1 (id int NOT NULL, g geography NULL)")
    cursor.execute(
        "INSERT INTO #g1 (id, g) VALUES (?, geography::STGeomFromText(?, 4326))",
        1,
        GEOG_POINT_WKT,
    )
    cursor.execute("SELECT g.STAsText(), g.STSrid FROM #g1 WHERE id = 1")
    text, srid = cursor.fetchone()
    assert type(text) is str
    assert text == GEOG_POINT_TEXT
    assert type(srid) is int
    assert srid == 4326


def test_geometry_polygon_text_area_and_srid_zero(cursor):
    cursor.execute("CREATE TABLE #m1 (id int NOT NULL, m geometry NULL)")
    cursor.execute(
        "INSERT INTO #m1 (id, m) VALUES (?, geometry::STGeomFromText(?, 0))",
        1,
        "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
    )
    cursor.execute("SELECT m.STAsText(), m.STSrid, m.STArea() FROM #m1")
    text, srid, area = cursor.fetchone()
    assert text == "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    assert type(srid) is int and srid == 0
    assert type(area) is float and area == 16.0


def test_geography_column_is_clr_bytes_with_srid_header(cursor):
    """Raw geography column value is SQL Server's CLR serialization, not text or WKB."""
    cursor.execute("CREATE TABLE #g2 (id int NOT NULL, g geography NULL)")
    cursor.execute(
        "INSERT INTO #g2 (id, g) VALUES (1, geography::STGeomFromText(?, 4326))",
        GEOG_POINT_WKT,
    )
    cursor.execute("SELECT g FROM #g2 WHERE id = 1")
    raw = cursor.fetchone()[0]
    assert type(raw) is bytes
    assert raw[:4] == bytes([0xE6, 0x10, 0x00, 0x00])  # SRID 4326, little-endian
    assert raw[4:6] == b"\x01\x0c"  # format version 1, properties 0x0c (valid point)


def test_geometry_column_bytes_srid_zero_header(cursor):
    cursor.execute("CREATE TABLE #m2 (id int NOT NULL, m geometry NULL)")
    cursor.execute(
        "INSERT INTO #m2 (id, m) VALUES (1, geometry::STGeomFromText(?, 0))",
        "POINT(3 4)",
    )
    cursor.execute("SELECT m FROM #m2 WHERE id = 1")
    raw = cursor.fetchone()[0]
    assert type(raw) is bytes
    assert raw[:4] == b"\x00\x00\x00\x00"
    cursor.execute("SELECT m.STAsText() FROM #m2 WHERE id = 1")
    assert cursor.fetchone()[0] == "POINT (3 4)"


def test_stasbinary_returns_standard_wkb_point(cursor):
    cursor.execute(
        "SELECT geography::STGeomFromText(?, 4326).STAsBinary()",
        GEOG_POINT_WKT,
    )
    wkb = cursor.fetchone()[0]
    assert type(wkb) is bytes
    assert len(wkb) == 21  # 1 byte order + 4 type + 2 * 8 doubles
    assert wkb[0:1] == b"\x01"  # little-endian marker
    assert struct.unpack("<I", wkb[1:5])[0] == 1  # WKB type 1 = Point
    x, y = struct.unpack("<dd", wkb[5:21])
    assert (x, y) == (-122.349, 47.651)


def test_wkb_bytes_param_roundtrip_via_stgeomfromwkb(cursor):
    cursor.execute("CREATE TABLE #w1 (id int NOT NULL, g geography NULL)")
    cursor.execute(
        "SELECT geography::STGeomFromText(?, 4326).STAsBinary()",
        "POLYGON((-122.40 47.60, -122.30 47.60, -122.30 47.65, -122.40 47.65, -122.40 47.60))",
    )
    wkb = cursor.fetchone()[0]
    assert type(wkb) is bytes
    cursor.execute(
        "INSERT INTO #w1 (id, g) VALUES (1, geography::STGeomFromWKB(?, 4326))",
        wkb,
    )
    cursor.execute("SELECT g.STAsText(), g.STAsBinary() FROM #w1 WHERE id = 1")
    text, wkb_back = cursor.fetchone()
    assert text == "POLYGON ((-122.4 47.6, -122.3 47.6, -122.3 47.65, -122.4 47.65, -122.4 47.6))"
    assert wkb_back == wkb


def test_null_geography_and_null_wkt_param(cursor):
    cursor.execute("CREATE TABLE #n1 (id int NOT NULL, g geography NULL)")
    cursor.execute("INSERT INTO #n1 (id, g) VALUES (1, NULL)")
    cursor.execute(
        "INSERT INTO #n1 (id, g) VALUES (2, geography::STGeomFromText(?, 4326))",
        None,
    )
    cursor.execute("SELECT id, g, g.STAsText() FROM #n1 ORDER BY id")
    rows = [tuple(r) for r in cursor.fetchall()]
    assert rows == [(1, None, None), (2, None, None)]


def test_executemany_wkt_params_and_order_by_distance(cursor):
    cursor.execute(
        "CREATE TABLE #loc (id int NOT NULL PRIMARY KEY, name nvarchar(20), g geography NULL)"
    )
    cursor.executemany(
        "INSERT INTO #loc (id, name, g) VALUES (?, ?, geography::STGeomFromText(?, 4326))",
        [
            (1, "far", "POINT(-74.006 40.7128)"),  # New York
            (2, "near", "POINT(-122.3321 47.6062)"),  # the reference point itself
            (3, "mid", "POINT(-122.4194 37.7749)"),  # San Francisco
            (4, "unknown", None),
        ],
    )
    cursor.connection.commit()
    cursor.execute(
        """
        DECLARE @ref geography = geography::Point(47.6062, -122.3321, 4326);
        SELECT id, name, g.STDistance(@ref) AS dist
        FROM #loc
        WHERE g IS NOT NULL
        ORDER BY g.STDistance(@ref)
        """
    )
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [2, 3, 1]
    assert type(rows[0][2]) is float
    assert rows[0][2] == 0.0
    assert type(rows[1][2]) is float
    assert 1_000_000 < rows[1][2] < 1_200_000  # SF is roughly 1,100 km from Seattle, in meters


def test_geometry_distance_returns_float_exact(cursor):
    cursor.execute(
        "SELECT geometry::STGeomFromText(?, 0).STDistance(geometry::STGeomFromText(?, 0))",
        "POINT(0 0)",
        "POINT(3 4)",
    )
    value = cursor.fetchone()[0]
    assert type(value) is float
    assert value == 5.0


def test_large_polygon_wkt_param_not_truncated(cursor):
    """Planar geometry strip: WKT > 8000 chars must reach the server intact (no 8000-byte truncation)."""
    half = 700  # 1401 points
    wkt = _strip_polygon_wkt(half)
    assert len(wkt) > 8000
    cursor.execute("CREATE TABLE #big (id int NOT NULL, m geometry NULL)")
    cursor.execute(
        "INSERT INTO #big (id, m) VALUES (1, geometry::STGeomFromText(?, 0))",
        wkt,
    )
    cursor.execute("SELECT m.STNumPoints(), m.STAsText() FROM #big WHERE id = 1")
    num_points, text = cursor.fetchone()
    assert type(num_points) is int
    assert num_points == 2 * half + 1
    assert type(text) is str
    assert text == _strip_polygon_text(half)


def _circle_polygon_wkt(points: int) -> str:
    """Convex geography ring around (lon 10, lat 10), radius 2 degrees, 6-decimal coordinates."""
    import math

    coords = []
    for i in range(points):
        angle = 2 * math.pi * i / points
        coords.append(f"{10 + 2 * math.cos(angle):.6f} {10 + 2 * math.sin(angle):.6f}")
    coords.append(coords[0])
    return "POLYGON((" + ", ".join(coords) + "))"


def test_large_geography_column_value_exceeds_8000_bytes(cursor):
    points = 1400
    wkt = _circle_polygon_wkt(points)
    assert len(wkt) > 8000
    cursor.execute("CREATE TABLE #bigcol (id int NOT NULL, g geography NULL)")
    cursor.execute(
        "INSERT INTO #bigcol (id, g) VALUES (1, geography::STGeomFromText(?, 4326))",
        wkt,
    )
    cursor.execute("SELECT g, g.STNumPoints() FROM #bigcol WHERE id = 1")
    raw, num_points = cursor.fetchone()
    assert type(raw) is bytes
    assert len(raw) > 8000
    assert raw[:4] == bytes([0xE6, 0x10, 0x00, 0x00])
    assert type(num_points) is int
    assert num_points == points + 1
    cursor.execute("SELECT g.STAsBinary() FROM #bigcol WHERE id = 1")
    wkb = cursor.fetchone()[0]
    assert type(wkb) is bytes
    # WKB polygon header: byte order (1) + type (4) + ring count (4) + point count (4)
    assert wkb[0:1] == b"\x01"
    assert struct.unpack("<I", wkb[1:5])[0] == 3  # WKB type 3 = Polygon
    assert struct.unpack("<I", wkb[5:9])[0] == 1  # one ring
    assert struct.unpack("<I", wkb[9:13])[0] == points + 1  # closed ring point count
    assert len(wkb) == 13 + (points + 1) * 16


def test_geography_srid_mismatch_raises_programming_error(cursor):
    with pytest.raises(ProgrammingError) as exc:
        cursor.execute(
            "SELECT geography::STGeomFromText(?, 4326).STDistance(geography::STGeomFromText(?, 3857))",
            "POINT(0 0)",
            "POINT(1 1)",
        )
        cursor.fetchone()
    assert "ArgumentException" in str(exc.value)
    assert "24204" in str(exc.value)


def test_geometry_srid_mismatch_silently_returns_null(cursor):
    """Geometry methods do not raise on SRID mismatch; the result is SQL NULL (None)."""
    cursor.execute(
        "SELECT geometry::STGeomFromText(?, 0).STDistance(geometry::STGeomFromText(?, 4326)), "
        "geometry::STGeomFromText(?, 0).STIntersects(geometry::STGeomFromText(?, 4326))",
        "POINT(0 0)",
        "POINT(1 1)",
        "POINT(0 0)",
        "POINT(0 0)",
    )
    distance, intersects = cursor.fetchone()
    assert distance is None
    assert intersects is None


def test_invalid_wkt_raises_format_exception(cursor):
    with pytest.raises(ProgrammingError) as exc:
        cursor.execute(
            "SELECT geometry::STGeomFromText(?, 0)",
            "POLYGON((0 0, 1 1))",
        )
        cursor.fetchone()
    assert "FormatException" in str(exc.value)


def test_invalid_geography_latitude_raises_format_exception(cursor):
    with pytest.raises(ProgrammingError) as exc:
        cursor.execute(
            "SELECT geography::STGeomFromText(?, 4326)",
            "POINT(0 95)",
        )
        cursor.fetchone()
    assert "FormatException" in str(exc.value)
    assert "Latitude" in str(exc.value)


def test_bowtie_polygon_stored_but_reported_invalid(cursor):
    cursor.execute("CREATE TABLE #bow (id int NOT NULL, m geometry NULL)")
    cursor.execute(
        "INSERT INTO #bow (id, m) VALUES (1, geometry::STGeomFromText(?, 0))",
        "POLYGON((0 0, 4 4, 4 0, 0 4, 0 0))",
    )
    cursor.execute("SELECT m.STIsValid() FROM #bow WHERE id = 1")
    is_valid = cursor.fetchone()[0]
    assert type(is_valid) is bool
    assert is_valid is False
    cursor.execute("SELECT m.MakeValid().STIsValid() FROM #bow WHERE id = 1")
    repaired = cursor.fetchone()[0]
    assert type(repaired) is bool
    assert repaired is True


def test_empty_geometry_text_and_is_empty_flag(cursor):
    cursor.execute(
        "SELECT geometry::STGeomFromText(?, 0).STAsText(), geometry::STGeomFromText(?, 0).STIsEmpty()",
        "POINT EMPTY",
        "POINT EMPTY",
    )
    text, is_empty = cursor.fetchone()
    assert text == "POINT EMPTY"
    assert type(is_empty) is bool
    assert is_empty is True


def test_linestring_length_float_and_numpoints(cursor):
    cursor.execute(
        "SELECT geometry::STGeomFromText(?, 0).STLength(), geometry::STGeomFromText(?, 0).STNumPoints()",
        "LINESTRING(0 0, 3 4, 3 10)",
        "LINESTRING(0 0, 3 4, 3 10)",
    )
    length, num_points = cursor.fetchone()
    assert type(length) is float
    assert length == 11.0  # 5 + 6
    assert type(num_points) is int
    assert num_points == 3


def test_geography_ring_orientation_clockwise_is_complement_area(cursor):
    """Counter-clockwise 1x1 degree ring ~ 12.3e9 m^2; the same ring clockwise is the huge complement."""
    cursor.execute(
        "SELECT geography::STPolyFromText(?, 4326).STArea(), geography::STPolyFromText(?, 4326).STArea()",
        "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
        "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
    )
    ccw_area, cw_area = cursor.fetchone()
    assert type(ccw_area) is float and type(cw_area) is float
    assert 1.20e10 < ccw_area < 1.25e10
    assert cw_area > 5.0e14  # reversed ring encloses the rest of the sphere


def test_geometry_intersection_area_and_null_propagation(cursor):
    cursor.execute(
        "SELECT geometry::STGeomFromText(?, 0).STIntersection(geometry::STGeomFromText(?, 0)).STArea(), "
        "IIF(geometry::STGeomFromText(?, 0).STIntersection(NULL) IS NULL, 1, 0)",
        "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
        "POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))",
        "POINT(0 0)",
    )
    area, null_result = cursor.fetchone()
    assert type(area) is float
    # Overlap is a 2x2 square; the engine returns floating-point noise, so compare approximately.
    assert area == pytest.approx(4.0, abs=1e-9)
    assert type(null_result) is int
    assert null_result == 1
