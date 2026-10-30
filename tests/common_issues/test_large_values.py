"""Large-value (MAX / LOB-ish) round-trips, streaming fetch, and empty-vs-NULL.

Boundaries under test:
- 4000 / 4001 characters (nvarchar) and 8000 / 8001 bytes or characters,
  where the driver and server switch from in-row storage to LOB handling
  for ``(MAX)`` types.
- 1 MB and 10 MB payloads.

Upstream ODBC driver is ground truth. Tests assert exact values, lengths,
Python types and exception classes.
"""

import uuid

import pytest
from mssql_python import DatabaseError, DataError

BOUNDARY_SIZES = [4000, 4001, 8000, 8001]


def _text(n):
    """Deterministic text of exactly n characters; any truncation or shift shows up."""
    pattern = "0123456789abcdefghijklmnopqrstuvwxyz"
    return (pattern * (n // len(pattern) + 1))[:n]


def _blob(n):
    """Deterministic bytes of exactly n bytes, using the full byte range."""
    return bytes(i % 256 for i in range(n))


# --- Boundary round-trips -------------------------------------------------


@pytest.mark.parametrize("n", BOUNDARY_SIZES)
def test_nvarchar_max_roundtrip_at_boundary(cursor, n):
    value = _text(n)
    cursor.execute("CREATE TABLE #nv (id int NOT NULL, s nvarchar(max) NULL)")
    cursor.execute("INSERT INTO #nv (id, s) VALUES (?, ?)", [1, value])
    cursor.execute("SELECT s, LEN(s), DATALENGTH(s) FROM #nv WHERE id = 1")
    row = cursor.fetchone()
    assert isinstance(row[0], str)
    assert row[0] == value
    assert row[1] == n
    assert row[2] == 2 * n  # nvarchar stores UTF-16: 2 bytes per ASCII char


@pytest.mark.parametrize("n", BOUNDARY_SIZES)
def test_varchar_max_roundtrip_at_boundary(cursor, n):
    value = _text(n)
    cursor.execute("CREATE TABLE #v (id int NOT NULL, s varchar(max) NULL)")
    cursor.execute("INSERT INTO #v (id, s) VALUES (?, ?)", [1, value])
    cursor.execute("SELECT s, DATALENGTH(s) FROM #v WHERE id = 1")
    row = cursor.fetchone()
    assert isinstance(row[0], str)
    assert row[0] == value
    assert row[1] == n  # varchar is 1 byte per ASCII char


@pytest.mark.parametrize("n", BOUNDARY_SIZES)
def test_varbinary_max_roundtrip_at_boundary(cursor, n):
    value = _blob(n)
    cursor.execute("CREATE TABLE #vb (id int NOT NULL, b varbinary(max) NULL)")
    cursor.execute("INSERT INTO #vb (id, b) VALUES (?, ?)", [1, value])
    cursor.execute("SELECT b, DATALENGTH(b) FROM #vb WHERE id = 1")
    row = cursor.fetchone()
    assert isinstance(row[0], bytes)
    assert row[0] == value
    assert row[1] == n


@pytest.mark.parametrize("size", [1024 * 1024, 10 * 1024 * 1024], ids=["1MB", "10MB"])
def test_nvarchar_max_roundtrip_megabyte_payloads(cursor, size):
    value = _text(size)
    cursor.execute("CREATE TABLE #nv_big (s nvarchar(max) NOT NULL)")
    cursor.execute("INSERT INTO #nv_big (s) VALUES (?)", [value])
    cursor.execute("SELECT s, LEN(s) FROM #nv_big")
    row = cursor.fetchone()
    assert isinstance(row[0], str)
    assert len(row[0]) == size
    assert row[1] == size
    assert row[0] == value


@pytest.mark.parametrize("size", [1024 * 1024, 10 * 1024 * 1024], ids=["1MB", "10MB"])
def test_varbinary_max_roundtrip_megabyte_payloads(cursor, size):
    value = _blob(size)
    cursor.execute("CREATE TABLE #vb_big (b varbinary(max) NOT NULL)")
    cursor.execute("INSERT INTO #vb_big (b) VALUES (?)", [value])
    cursor.execute("SELECT b, DATALENGTH(b) FROM #vb_big")
    row = cursor.fetchone()
    assert isinstance(row[0], bytes)
    assert len(row[0]) == size
    assert row[1] == size
    assert row[0] == value


def test_nvarchar_max_preserves_astral_characters_across_boundary(cursor):
    # 4001 code points where some are outside the BMP (surrogate pairs in UTF-16).
    # A driver that truncates by UTF-16 units or by bytes would corrupt the tail.
    value = ("aé€\U0001f600" * 1001)[:4001]
    assert len(value) == 4001
    cursor.execute("CREATE TABLE #astral (s nvarchar(max) NOT NULL)")
    cursor.execute("INSERT INTO #astral (s) VALUES (?)", [value])
    cursor.execute("SELECT s FROM #astral")
    got = cursor.fetchone()[0]
    assert got == value
    assert len(got) == 4001


def test_varchar_max_preserves_cp1252_text_above_4000(cursor):
    # Non-ASCII single-byte text in varchar(max) (cp1252 range) above the boundary.
    value = ("Grüße €uro ß çà " * 400)[:4001]
    cursor.execute("CREATE TABLE #v_cp (s varchar(max) NOT NULL)")
    cursor.execute("INSERT INTO #v_cp (s) VALUES (?)", [value])
    cursor.execute("SELECT s, DATALENGTH(s) FROM #v_cp")
    row = cursor.fetchone()
    assert isinstance(row[0], str)
    assert row[0] == value
    # Database default collation is cp1252-based: one byte per character.
    assert row[1] == len(value.encode("cp1252"))


# --- Empty string vs NULL -------------------------------------------------


@pytest.mark.parametrize(
    "ddl, empty, expected_type",
    [
        ("nvarchar(max)", "", str),
        ("varchar(max)", "", str),
        ("varbinary(max)", b"", bytes),
    ],
    ids=["nvarchar_max", "varchar_max", "varbinary_max"],
)
def test_empty_value_is_distinct_from_null(cursor, ddl, empty, expected_type):
    cursor.execute(f"CREATE TABLE #empty (id int NOT NULL, x {ddl} NULL)")
    cursor.execute("INSERT INTO #empty (id, x) VALUES (?, ?)", [1, empty])
    cursor.execute("INSERT INTO #empty (id, x) VALUES (2, NULL)")
    cursor.execute("SELECT id, x, DATALENGTH(x) FROM #empty ORDER BY id")
    rows = cursor.fetchall()
    assert rows[0][1] is not None
    assert isinstance(rows[0][1], expected_type)
    assert rows[0][1] == empty
    assert rows[0][2] == 0
    assert rows[1][1] is None
    assert rows[1][2] is None


def test_empty_and_null_filter_separately_on_nvarchar_max(cursor):
    cursor.execute("CREATE TABLE #filter (id int NOT NULL, s nvarchar(max) NULL)")
    cursor.execute(
        "INSERT INTO #filter (id, s) VALUES (1, ?), (2, ?), (3, ?)",
        ["", None, "x"],
    )
    cursor.execute("SELECT id FROM #filter WHERE s IS NULL")
    assert [r[0] for r in cursor.fetchall()] == [2]
    cursor.execute("SELECT id FROM #filter WHERE DATALENGTH(s) = 0")
    assert [r[0] for r in cursor.fetchall()] == [1]


def test_null_parameter_into_varbinary_max_with_explicit_cast(cursor):
    # Workaround that must always work: an explicit cast on the parameter.
    cursor.execute("CREATE TABLE #vb_null (id int NOT NULL, b varbinary(max) NULL)")
    cursor.execute(
        "INSERT INTO #vb_null (id, b) VALUES (1, CAST(? AS varbinary(max)))", [None]
    )
    cursor.execute("SELECT b FROM #vb_null WHERE id = 1")
    assert cursor.fetchone()[0] is None


@pytest.mark.upstream_bug(
    reason=(
        "Untyped None parameter is bound as varchar, so INSERT of NULL into a "
        "varbinary column fails with 'Implicit conversion from data type varchar "
        "to varbinary(max) is not allowed' (also varbinary(10))."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_null_parameter_into_varbinary_max_plain_insert(cursor):
    cursor.execute("CREATE TABLE #vb_null2 (id int NOT NULL, b varbinary(max) NULL)")
    cursor.execute("INSERT INTO #vb_null2 (id, b) VALUES (?, ?)", [1, None])
    cursor.execute("SELECT b FROM #vb_null2 WHERE id = 1")
    assert cursor.fetchone()[0] is None


# --- Truncation errors ----------------------------------------------------


def test_insert_4001_chars_into_nvarchar_4000_is_rejected_not_silently_truncated(
    cursor,
):
    cursor.execute("CREATE TABLE #trunc (s nvarchar(4000) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #trunc (s) VALUES (?)", [_text(4001)])
    assert "truncated" in str(info.value).lower()
    cursor.execute("SELECT COUNT(*) FROM #trunc")
    assert cursor.fetchone()[0] == 0


def test_insert_8001_bytes_into_varbinary_8000_is_rejected(cursor):
    cursor.execute("CREATE TABLE #trunc_b (b varbinary(8000) NULL)")
    with pytest.raises(DatabaseError) as info:
        cursor.execute("INSERT INTO #trunc_b (b) VALUES (?)", [_blob(8001)])
    assert "truncated" in str(info.value).lower()
    cursor.execute("SELECT COUNT(*) FROM #trunc_b")
    assert cursor.fetchone()[0] == 0


@pytest.mark.upstream_bug(
    reason=(
        "Server reports string truncation (SQLSTATE 22001 class data exception) but "
        "the driver raises ProgrammingError (mapped from SQLSTATE 42000) instead of "
        "DataError, so callers cannot catch data problems by class."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_insert_4001_chars_into_nvarchar_4000_raises_data_error(cursor):
    # A unique table name makes the statement text unique, so the server compiles
    # it fresh. Reused statement text can hit the plan cache and come back as
    # DataError, which hides the bug below.
    name = f"#trunc_dc_{uuid.uuid4().hex[:8]}"
    cursor.execute(f"CREATE TABLE {name} (s nvarchar(4000) NULL)")
    with pytest.raises(DataError):
        cursor.execute(f"INSERT INTO {name} (s) VALUES (?)", [_text(4001)])


def test_insert_4000_chars_into_nvarchar_4000_succeeds_at_exact_limit(cursor):
    value = _text(4000)
    cursor.execute("CREATE TABLE #exact (s nvarchar(4000) NULL)")
    cursor.execute("INSERT INTO #exact (s) VALUES (?)", [value])
    cursor.execute("SELECT s FROM #exact")
    assert cursor.fetchone()[0] == value


# --- Preservation of special content -------------------------------------


def test_nvarchar_max_preserves_embedded_nul_and_trailing_spaces(cursor):
    value = "a\x00b" + " " * 3
    cursor.execute("CREATE TABLE #nul (s nvarchar(max) NOT NULL)")
    cursor.execute("INSERT INTO #nul (s) VALUES (?)", [value])
    cursor.execute("SELECT s, DATALENGTH(s) FROM #nul")
    row = cursor.fetchone()
    assert row[0] == value
    assert row[1] == 2 * len(value)  # UTF-16 storage, NUL and spaces all kept


@pytest.mark.upstream_bug(
    reason=(
        "Trailing NUL characters are stored by the server (DATALENGTH confirms it) "
        "but dropped when a (MAX) string column is fetched. In-row columns keep them."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
@pytest.mark.parametrize("ddl", ["nvarchar(max)", "varchar(max)"])
def test_max_string_keeps_trailing_nul_on_fetch(cursor, ddl):
    value = "x\x00"
    cursor.execute(f"CREATE TABLE #tnul (s {ddl} NOT NULL)")
    cursor.execute("INSERT INTO #tnul (s) VALUES (?)", [value])
    cursor.execute("SELECT s, DATALENGTH(s) FROM #tnul")
    row = cursor.fetchone()
    assert row[1] == len(value) * (2 if ddl.startswith("n") else 1)
    assert row[0] == value


def test_varbinary_max_preserves_leading_and_embedded_zero_bytes(cursor):
    value = b"\x00\x00\x01\x00\xff\x00"
    cursor.execute("CREATE TABLE #zero (b varbinary(max) NOT NULL)")
    cursor.execute("INSERT INTO #zero (b) VALUES (?)", [value])
    cursor.execute("SELECT b FROM #zero")
    assert cursor.fetchone()[0] == value


# --- Streaming fetch of large values -------------------------------------


def test_fetchone_iterates_large_rows_in_order(cursor):
    sizes = [4001, 1024 * 1024, 8001, 10 * 1024 * 1024]
    cursor.execute("CREATE TABLE #stream (id int NOT NULL, s nvarchar(max) NOT NULL)")
    for i, n in enumerate(sizes):
        cursor.execute("INSERT INTO #stream (id, s) VALUES (?, ?)", [i, _text(n)])
    cursor.execute("SELECT id, s FROM #stream ORDER BY id")
    seen = []
    while (row := cursor.fetchone()) is not None:
        seen.append((row[0], len(row[1]), row[1] == _text(sizes[row[0]])))
    assert seen == [(i, n, True) for i, n in enumerate(sizes)]
    assert cursor.fetchone() is None


def test_fetchmany_batches_large_rows_exactly(cursor):
    cursor.execute("CREATE TABLE #batch (id int NOT NULL, s varchar(max) NOT NULL)")
    for i in range(5):
        cursor.execute("INSERT INTO #batch (id, s) VALUES (?, ?)", [i, _text(4001 + i)])
    cursor.execute("SELECT id, s FROM #batch ORDER BY id")
    first = cursor.fetchmany(2)
    second = cursor.fetchmany(2)
    third = cursor.fetchmany(2)
    assert [r[0] for r in first] == [0, 1]
    assert [r[0] for r in second] == [2, 3]
    assert [r[0] for r in third] == [4]
    assert [len(r[1]) for r in first + second + third] == [4001, 4002, 4003, 4004, 4005]
    assert cursor.fetchmany(2) == []


def test_fetchall_returns_all_megabyte_rows_intact(cursor):
    cursor.execute("CREATE TABLE #mb (id int NOT NULL, b varbinary(max) NOT NULL)")
    blobs = [_blob(1024 * 1024 + i) for i in range(3)]
    for i, blob in enumerate(blobs):
        cursor.execute("INSERT INTO #mb (id, b) VALUES (?, ?)", [i, blob])
    cursor.execute("SELECT id, b FROM #mb ORDER BY id")
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [0, 1, 2]
    assert [r[1] for r in rows] == blobs
    assert cursor.fetchall() == []


def test_large_value_alongside_small_columns_in_same_row(cursor):
    # Mixing an in-row column with a LOB column in one row must not shift values.
    cursor.execute(
        "CREATE TABLE #mixed (id int NOT NULL, tag nvarchar(50) NOT NULL, "
        "body nvarchar(max) NOT NULL, n bigint NOT NULL)"
    )
    body = _text(8001)
    cursor.execute(
        "INSERT INTO #mixed (id, tag, body, n) VALUES (?, ?, ?, ?)",
        [7, "tagged", body, 9_000_000_000],
    )
    cursor.execute("SELECT id, tag, body, n FROM #mixed")
    row = cursor.fetchone()
    assert (row[0], row[1], row[3]) == (7, "tagged", 9_000_000_000)
    assert row[2] == body


def test_fetch_large_value_into_nvarchar_max_is_str_not_bytes(cursor):
    # Guards against silently returning bytes for LOB columns.
    cursor.execute("CREATE TABLE #typed (s nvarchar(max) NULL, v varchar(max) NULL)")
    cursor.execute(
        "INSERT INTO #typed (s, v) VALUES (?, ?)", [_text(10_000), _text(10_000)]
    )
    cursor.execute("SELECT s, v FROM #typed")
    s, v = cursor.fetchone()
    assert type(s) is str and type(v) is str
    assert len(s) == 10_000 and len(v) == 10_000
