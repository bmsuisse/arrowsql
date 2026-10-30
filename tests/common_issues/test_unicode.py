"""Unicode handling: emoji/surrogate pairs, CJK, combining marks, RTL, embedded NUL.

Covers NVARCHAR columns, parameters (execute and executemany), column aliases,
and VARCHAR columns under Latin1 and UTF-8 collations, where conversion is lossy.

Expected values were verified against the upstream ODBC driver. Where upstream
returns something other than the correct value, the test asserts the correct
value and is marked ``upstream_bug`` + ``xfail(strict=True)``.
"""

import pytest
from mssql_python import DatabaseError, ProgrammingError

EMOJI = "\U0001F600"  # U+1F600, outside the BMP -> surrogate pair in UTF-16
FAMILY = "\U0001F468‍\U0001F469‍\U0001F467"  # ZWJ emoji sequence
LATIN1_COLLATION = "Latin1_General_CI_AS"
UTF8_COLLATION = "Latin1_General_100_CI_AS_SC_UTF8"


def _fetch_one(cursor, sql, *params):
    cursor.execute(sql, *params)
    return cursor.fetchone()


def test_nvarchar_emoji_surrogate_pair_roundtrip(cursor):
    cursor.execute("CREATE TABLE #u (id INT, n NVARCHAR(50))")
    cursor.execute("INSERT INTO #u VALUES (?, ?)", 1, f"{EMOJI}x")
    cursor.connection.commit()

    row = _fetch_one(cursor, "SELECT n, LEN(n), DATALENGTH(n) FROM #u WHERE id = 1")
    # NVARCHAR counts UTF-16 code units: one emoji is 2 units / 4 bytes.
    assert row[0] == f"{EMOJI}x"
    assert row[1] == 3
    assert row[2] == 6  # 4 bytes for the emoji + 2 for 'x'


def test_emoji_param_roundtrip_in_select(cursor):
    value = _fetch_one(cursor, "SELECT ? AS p", f"{EMOJI}{EMOJI}!")[0]
    assert value == f"{EMOJI}{EMOJI}!"
    assert type(value) is str
    assert len(value) == 3  # Python code points, not UTF-16 units


def test_cjk_nvarchar_roundtrip_with_table_name(cursor, table_name):
    text = "中文字符"
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, n NVARCHAR(40))")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?, ?)", 1, text)
    cursor.connection.commit()

    cursor.execute(f"SELECT n, DATALENGTH(n) FROM dbo.{table_name} WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert value == text
    assert nbytes == 8  # 4 BMP code units, 2 bytes each


def test_combining_mark_kept_as_separate_code_point(cursor):
    decomposed = "é"  # 'e' + COMBINING ACUTE ACCENT
    cursor.execute("CREATE TABLE #c (n NVARCHAR(10))")
    cursor.execute("INSERT INTO #c VALUES (?)", decomposed)
    cursor.connection.commit()

    cursor.execute("SELECT n, LEN(n), CONVERT(VARBINARY(20), n) FROM #c")
    value, length, raw = cursor.fetchone()
    assert value == decomposed
    assert len(value) == 2
    assert length == 2
    assert bytes(raw) == "é".encode("utf-16-le")


def test_rtl_hebrew_and_bidi_controls_roundtrip(cursor):
    hebrew = "שלום"
    bidi = "‮abc‬"  # RIGHT-TO-LEFT OVERRIDE ... POP DIRECTIONAL FORMATTING
    cursor.execute("CREATE TABLE #rtl (id INT, n NVARCHAR(40))")
    cursor.execute("INSERT INTO #rtl VALUES (1, ?), (2, ?)", hebrew, bidi)
    cursor.connection.commit()

    cursor.execute("SELECT n FROM #rtl ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [hebrew, bidi]


def test_zwj_emoji_sequence_roundtrip(cursor):
    cursor.execute("CREATE TABLE #zwj (n NVARCHAR(40))")
    cursor.execute("INSERT INTO #zwj VALUES (?)", FAMILY)
    cursor.connection.commit()

    value, length = _fetch_one(cursor, "SELECT n, LEN(n) FROM #zwj")
    assert value == FAMILY
    assert length == 8  # three 2-unit emoji and two 1-unit ZWJ


def test_nul_inside_nvarchar_param_preserved_on_execute(cursor):
    value = _fetch_one(cursor, "SELECT ? AS p", "a\x00b")[0]
    assert value == "a\x00b"
    assert len(value) == 3


@pytest.mark.upstream_bug(
    reason=(
        "executemany truncates a str parameter at the first embedded NUL "
        "('x\\x00y' is stored as 'x'), while execute() keeps the whole string"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_nul_inside_string_survives_executemany(cursor):
    cursor.execute("CREATE TABLE #nul (id INT, n NVARCHAR(20))")
    cursor.executemany("INSERT INTO #nul VALUES (?, ?)", [(1, "x\x00y")])
    cursor.connection.commit()

    cursor.execute("SELECT n FROM #nul WHERE id = 1")
    assert cursor.fetchone()[0] == "x\x00y"


def test_executemany_uniform_cjk_batch_roundtrips(cursor):
    cursor.execute("CREATE TABLE #em_cjk (id INT, n NVARCHAR(20))")
    cursor.executemany("INSERT INTO #em_cjk VALUES (?, ?)", [(1, "中"), (2, "文字")])
    cursor.connection.commit()

    cursor.execute("SELECT id, n FROM #em_cjk ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "中"), (2, "文字")]


@pytest.mark.upstream_bug(
    reason=(
        "executemany with a batch mixing ASCII-only and non-ASCII strings binds the "
        "column as non-Unicode, so '中' is stored as '?' and emoji as ':-D'"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_mixed_ascii_and_cjk_batch_preserves_text(cursor):
    cursor.execute("CREATE TABLE #mix (id INT, n NVARCHAR(20))")
    cursor.executemany("INSERT INTO #mix VALUES (?, ?)", [(1, "plain"), (2, "中")])
    cursor.connection.commit()

    cursor.execute("SELECT id, n FROM #mix ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "plain"), (2, "中")]


@pytest.mark.upstream_bug(
    reason="executemany with an emoji row next to an ASCII row corrupts the emoji",
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_emoji_with_ascii_row_preserved(cursor):
    cursor.execute("CREATE TABLE #mix_emoji (id INT, n NVARCHAR(20))")
    cursor.executemany("INSERT INTO #mix_emoji VALUES (?, ?)", [(1, f"{EMOJI}a"), (2, "plain")])
    cursor.connection.commit()

    cursor.execute("SELECT id, n FROM #mix_emoji ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, f"{EMOJI}a"), (2, "plain")]


def test_varchar_latin1_replaces_unrepresentable_chars(cursor):
    cursor.execute(f"CREATE TABLE #lat (id INT, v VARCHAR(20) COLLATE {LATIN1_COLLATION})")
    cursor.execute(
        "INSERT INTO #lat VALUES (1, ?), (2, ?), (3, ?), (4, ?)",
        "中",  # CJK: no code page 1252 mapping
        EMOJI,  # non-BMP
        "שלום",  # Hebrew
        "é€",  # both are in cp1252
    )
    cursor.connection.commit()

    cursor.execute("SELECT id, v, DATALENGTH(v) FROM #lat ORDER BY id")
    rows = [tuple(r) for r in cursor.fetchall()]
    # Lossy: each character with no Latin1 mapping becomes one or more '?'.
    assert rows[0] == (1, "?", 1)
    assert rows[1] == (2, "??", 2)
    assert rows[2] == (3, "????", 4)
    assert rows[3] == (4, "é€", 2)


def test_varchar_latin1_combining_mark_degrades_to_spacing_accent(cursor):
    cursor.execute(f"CREATE TABLE #lat_comb (v VARCHAR(10) COLLATE {LATIN1_COLLATION})")
    cursor.execute("INSERT INTO #lat_comb VALUES (?)", "é")
    cursor.connection.commit()

    value = _fetch_one(cursor, "SELECT v FROM #lat_comb")[0]
    # The combining mark has no cp1252 code point; the server maps it to the
    # spacing acute accent U+00B4 and the value no longer has two code points.
    assert value == "e´"


def test_varchar_utf8_collation_stores_full_unicode(cursor):
    cursor.execute(f"CREATE TABLE #u8 (id INT, v VARCHAR(50) COLLATE {UTF8_COLLATION})")
    cursor.execute("INSERT INTO #u8 VALUES (1, ?)", "é€" + EMOJI + "中")
    cursor.connection.commit()

    cursor.execute("SELECT v, LEN(v), DATALENGTH(v), CONVERT(VARBINARY(50), v) FROM #u8")
    value, length, nbytes, raw = cursor.fetchone()
    assert value == "é€" + EMOJI + "中"
    assert length == 4  # SC collations count a surrogate pair as one character
    assert nbytes == 12  # 2 + 3 + 4 + 3 bytes of UTF-8
    assert bytes(raw) == ("é€" + EMOJI + "中").encode("utf-8")


@pytest.mark.upstream_bug(
    reason=(
        "CAST(N'<emoji>' AS NVARCHAR(1)) returns the lone high surrogate (2 bytes, D8 3D); "
        "upstream fetch raises UnicodeDecodeError instead of returning the value "
        "(fetchall returns '' on a first call, then raises on the next fetch)"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_truncated_surrogate_pair_fetch_returns_code_unit(cursor):
    # Truncation splits the pair; the server returns the single code unit 0xD83D.
    value = _fetch_one(cursor, f"SELECT CAST(N'{EMOJI}' AS NVARCHAR(1))")[0]
    assert value == "\ud83d"


def test_nvarchar_max_many_emoji_roundtrip(cursor):
    count = 5000
    cursor.execute("CREATE TABLE #big (n NVARCHAR(MAX))")
    cursor.execute("INSERT INTO #big VALUES (?)", EMOJI * count)
    cursor.connection.commit()

    value = _fetch_one(cursor, "SELECT n FROM #big")[0]
    assert len(value) == count
    assert value == EMOJI * count


def test_column_alias_with_emoji_and_combining_mark(cursor):
    cursor.execute(f"SELECT 1 AS [a{EMOJI}b], 2 AS [é]")
    names = [d[0] for d in cursor.description]
    assert names == [f"a{EMOJI}b", "é"]


def test_column_alias_with_nul_in_sql_text_is_rejected(cursor):
    # The SQL text is truncated at the NUL, leaving an unclosed identifier.
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT 1 AS [a\x00b]")


def test_insert_truncation_of_unicode_raises_and_writes_nothing(cursor):
    cursor.execute("CREATE TABLE #trunc (n NVARCHAR(3))")
    cursor.connection.commit()

    # Upstream intermittently reports this as ProgrammingError or DataError (both
    # DatabaseError), so only the base class is pinned here.
    with pytest.raises(DatabaseError, match="would be truncated"):
        cursor.execute("INSERT INTO #trunc VALUES (?)", EMOJI * 2)  # 4 UTF-16 units
    cursor.connection.rollback()

    assert _fetch_one(cursor, "SELECT COUNT(*) FROM #trunc")[0] == 0


@pytest.mark.upstream_bug(
    reason=(
        "a lone surrogate str parameter fails inside the pybind layer with a generic "
        "RuntimeError instead of UnicodeEncodeError"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_lone_surrogate_param_raises_unicode_encode_error(cursor):
    with pytest.raises(UnicodeEncodeError):
        cursor.execute("SELECT ? AS p", "\ud83d")
