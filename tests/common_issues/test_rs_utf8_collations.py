"""VARCHAR under UTF-8 collations (Latin1_General_100_CI_AS_SC_UTF8): byte vs character semantics.

A VARCHAR column with a ``_UTF8`` collation stores UTF-8 bytes. Its length
``VARCHAR(n)`` counts BYTES, ``LEN`` counts characters (a supplementary-plane
emoji is one character), ``DATALENGTH`` counts bytes, and a 4-byte character
can therefore overflow a column with room for only three more bytes. Failure
modes a native-TDS reimplementation can easily get wrong:

- sizing a VARCHAR(n) parameter or column in characters instead of bytes
- counting a 4-byte character as 2 (UTF-16 surrogate pair) for LEN/length checks
- truncating mid-character (splitting a UTF-8 sequence) instead of at a boundary
- decoding fetched UTF-8 bytes into surrogate halves or U+FFFD
- reporting a UTF-8 varchar column with the NVARCHAR byte/character semantics
- silently inserting a too-long value instead of raising "would be truncated"
- leaving the connection or transaction in a bad state after a truncation error

Sources (research):
- https://github.com/microsoft/mssql-python/issues/531 (VARCHAR with Latin1_General_100_CI_AS_SC_UTF8 not round-tripping)
- https://github.com/microsoft/mssql-python/issues/391 (UTF-8 varchar fetch buffer sized too small for 2-byte characters)
- https://github.com/microsoft/mssql-python/issues/849 (UTF-8 multibyte characters vs VARCHAR bulk copy, error 7339)
- https://learn.microsoft.com/en-us/SQL/connect/python/mssql-python/string-unicode?view=sql-server-2017 (UTF-8 applies to varchar only under a UTF-8 collation)
- https://learn.microsoft.com/en-us/answers/questions/1693889/unexpected-truncation-error-when-inserting-from-ut (truncation sized by the UTF-8 source length)
- https://learn.microsoft.com/en-us/answers/questions/1050699/sql-server-2019-developer-edition-collation-latin1 (DATALENGTH vs the NVARCHAR test table)
- https://sqlquantumleap.com/2018/09/28/native-utf-8-support-in-sql-server-2019-savior-false-prophet-or-both/ (UTF-8 varchar length in bytes, truncation)
- https://sqlquantumleap.com/2019/11/22/how-many-bytes-per-character-in-sql-server-a-completely-complete-guide/ (1 to 4 bytes per character)
- https://sqlsunday.com/2023/11/08/sql-server-utf8-collations/ (VARCHAR(n) bytes under UTF-8 collations, premature truncation)
- https://www.sqlserverscience.com/internals/how-sql-server-stores-strings-utf8/ (storage of UTF-8 varchar vs nvarchar)

Expected values were verified against the upstream ODBC driver. Where upstream
returns something other than the correct value, the test asserts the correct
value and is marked ``upstream_bug`` + ``xfail(strict=True)``.
"""

import pytest
from mssql_python import DatabaseError

UTF8 = "Latin1_General_100_CI_AS_SC_UTF8"
EMOJI = "\U0001F600"  # 4 bytes in UTF-8, one code point outside the BMP
E_ACUTE = "é"  # 2 bytes in UTF-8
EURO = "€"  # 3 bytes in UTF-8


def _one(cursor, sql, *params):
    cursor.execute(sql, *params)
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _all(cursor, sql, *params):
    cursor.execute(sql, *params)
    return [tuple(row) for row in cursor.fetchall()]


def test_len_counts_characters_while_datalength_counts_utf8_bytes(cursor):
    cursor.execute(f"CREATE TABLE #u (v VARCHAR(20) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #u VALUES (?)", f"a{EMOJI}b")
    cursor.connection.commit()

    row = _one(cursor, "SELECT v, LEN(v), DATALENGTH(v) FROM #u")

    assert row == (f"a{EMOJI}b", 3, 6)
    assert type(row[0]) is str
    assert type(row[1]) is int and type(row[2]) is int


def test_mixed_width_characters_fetch_exact_and_have_expected_byte_length(cursor):
    text = "A" + E_ACUTE + EURO + EMOJI  # 1 + 2 + 3 + 4 = 10 bytes, 4 characters
    cursor.execute(f"CREATE TABLE #m (v VARCHAR(20) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #m VALUES (?)", text)
    cursor.connection.commit()

    row = _one(cursor, "SELECT v, LEN(v), DATALENGTH(v) FROM #m")

    assert row == (text, 4, 10)
    assert len(row[0]) == 4


def test_varchar_n_counts_bytes_two_byte_chars_fill_exactly_four(cursor):
    cursor.execute(f"CREATE TABLE #v4 (v VARCHAR(4) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #v4 VALUES (?)", E_ACUTE * 2)  # 4 bytes, 2 chars
    cursor.connection.commit()

    row = _one(cursor, "SELECT v, LEN(v), DATALENGTH(v) FROM #v4")

    assert row == (E_ACUTE * 2, 2, 4)


def test_four_byte_char_fits_varchar4_but_one_more_byte_overflows(cursor):
    cursor.execute(f"CREATE TABLE #e4 (v VARCHAR(4) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #e4 VALUES (?)", EMOJI)  # exactly 4 bytes
    cursor.connection.commit()
    assert _one(cursor, "SELECT v, DATALENGTH(v) FROM #e4") == (EMOJI, 4)

    # 'a' + emoji is 5 bytes: only 2 characters, but over the 4-byte limit.
    with pytest.raises(DatabaseError, match="would be truncated"):
        cursor.execute("INSERT INTO #e4 VALUES (?)", "a" + EMOJI)

    assert _one(cursor, "SELECT COUNT(*) FROM #e4") == (1,)


def test_truncation_error_leaves_connection_and_transaction_usable(cursor):
    cursor.execute(f"CREATE TABLE #t (id INT, v VARCHAR(4) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #t VALUES (1, ?)", E_ACUTE * 2)
    with pytest.raises(DatabaseError, match="would be truncated"):
        cursor.execute("INSERT INTO #t VALUES (2, ?)", E_ACUTE * 3)  # 6 bytes

    # The failed statement inserted nothing and the session still works.
    assert _one(cursor, "SELECT id, v FROM #t") == (1, E_ACUTE * 2)
    cursor.execute("INSERT INTO #t VALUES (3, ?)", "ok")
    assert _one(cursor, "SELECT COUNT(*) FROM #t") == (2,)


def test_explicit_cast_truncates_at_character_boundary_without_error(cursor):
    cursor.execute(f"CREATE TABLE #c (v VARCHAR(10) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #c VALUES (?)", f"a{EMOJI}b")  # 1 + 4 + 1 = 6 bytes
    cursor.connection.commit()

    # 3 bytes: the 4-byte emoji does not fit, so only 'a' survives; no partial bytes.
    assert _one(cursor, "SELECT CAST(v AS VARCHAR(3)), DATALENGTH(CAST(v AS VARCHAR(3))) FROM #c") == ("a", 1)
    # 5 bytes: 'a' plus the whole emoji fits exactly.
    assert _one(cursor, "SELECT CAST(v AS VARCHAR(5)), DATALENGTH(CAST(v AS VARCHAR(5))) FROM #c") == (
        f"a{EMOJI}",
        5,
    )


def test_left_substring_reverse_operate_on_characters_not_bytes(cursor):
    cursor.execute(f"CREATE TABLE #s (v VARCHAR(20) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #s VALUES (?)", f"a{EMOJI}b")
    cursor.connection.commit()

    row = _one(
        cursor,
        "SELECT LEFT(v, 2), DATALENGTH(LEFT(v, 2)), SUBSTRING(v, 2, 1), "
        "DATALENGTH(SUBSTRING(v, 2, 1)), REVERSE(v) FROM #s",
    )

    assert row == (f"a{EMOJI}", 5, EMOJI, 4, f"b{EMOJI}a")


def test_comparison_is_case_insensitive_and_accent_sensitive(cursor):
    cursor.execute(f"CREATE TABLE #cmp (v VARCHAR(20) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #cmp VALUES (?)", f"caf{E_ACUTE}{EMOJI}")
    cursor.connection.commit()

    # Case-insensitive: 'CAFÉ' + emoji matches. Accent-sensitive: plain 'CAFE' does not.
    assert _all(cursor, "SELECT v FROM #cmp WHERE v = ?", f"CAF{E_ACUTE.upper()}{EMOJI}") == [
        (f"caf{E_ACUTE}{EMOJI}",)
    ]
    assert _all(cursor, "SELECT v FROM #cmp WHERE v = ?", f"CAFE{EMOJI}") == []


def test_varchar_max_utf8_roundtrip_preserves_exact_text_and_byte_count(cursor):
    text = (E_ACUTE + EMOJI) * 3000  # 6000 characters, 18000 bytes
    cursor.execute(f"CREATE TABLE #big (v VARCHAR(MAX) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #big VALUES (?)", text)
    cursor.connection.commit()

    row = _one(cursor, "SELECT v, LEN(v), DATALENGTH(v) FROM #big")

    assert row[0] == text
    assert row[1:] == (6000, 18000)


def test_parameter_at_exact_varchar8000_byte_limit_accepted_one_char_more_rejected(cursor):
    cursor.execute(f"CREATE TABLE #lim (v VARCHAR(8000) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #lim VALUES (?)", EMOJI * 2000)  # exactly 8000 bytes
    cursor.connection.commit()
    assert _one(cursor, "SELECT LEN(v), DATALENGTH(v) FROM #lim") == (2000, 8000)

    with pytest.raises(DatabaseError, match="would be truncated"):
        cursor.execute("INSERT INTO #lim VALUES (?)", EMOJI * 2001)  # 8004 bytes


def test_executemany_widest_row_first_stores_every_byte_width(cursor):
    cursor.execute(f"CREATE TABLE #em (id INT, v VARCHAR(10) COLLATE {UTF8})")
    rows = [(1, EMOJI), (2, "a"), (3, E_ACUTE), (4, EURO)]
    cursor.executemany("INSERT INTO #em VALUES (?, ?)", rows)
    cursor.connection.commit()

    result = _all(cursor, "SELECT id, v, DATALENGTH(v) FROM #em ORDER BY id")

    assert result == [(1, EMOJI, 4), (2, "a", 1), (3, E_ACUTE, 2), (4, EURO, 3)]


@pytest.mark.upstream_bug(
    reason="executemany sizes the UTF-8 varchar parameter buffer from the first row; a wider "
    "later value raises RuntimeError('Input string exceeds allowed column size') instead of inserting"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_wider_utf8_value_after_narrow_first_row_is_inserted(cursor):
    cursor.execute(f"CREATE TABLE #em1 (id INT, v VARCHAR(10) COLLATE {UTF8})")
    cursor.executemany("INSERT INTO #em1 VALUES (?, ?)", [(1, "a"), (2, EMOJI)])
    cursor.connection.commit()

    assert _all(cursor, "SELECT id, v, DATALENGTH(v) FROM #em1 ORDER BY id") == [
        (1, "a", 1),
        (2, EMOJI, 4),
    ]


@pytest.mark.upstream_bug(
    reason="executemany truncates a 4-byte UTF-8 value to the first row's buffer size and stores "
    "garbage (b':-') with no error, silently corrupting data"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_emoji_after_narrower_rows_is_not_silently_corrupted(cursor):
    cursor.execute(f"CREATE TABLE #em2 (id INT, v VARCHAR(10) COLLATE {UTF8})")
    cursor.executemany("INSERT INTO #em2 VALUES (?, ?)", [(1, "ab"), (2, EMOJI), (3, "x")])
    cursor.connection.commit()

    assert _all(cursor, "SELECT id, v, DATALENGTH(v) FROM #em2 ORDER BY id") == [
        (1, "ab", 2),
        (2, EMOJI, 4),
        (3, "x", 1),
    ]


def test_null_and_empty_string_in_utf8_column_are_distinct(cursor):
    cursor.execute(f"CREATE TABLE #n (id INT, v VARCHAR(10) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #n VALUES (1, ?)", None)
    cursor.execute("INSERT INTO #n VALUES (2, ?)", "")
    cursor.connection.commit()

    rows = _all(cursor, "SELECT id, v, DATALENGTH(v) FROM #n ORDER BY id")

    assert rows == [(1, None, None), (2, "", 0)]


def test_same_emoji_has_different_length_in_utf8_varchar_and_nvarchar(cursor):
    cursor.execute(f"CREATE TABLE #lens (v VARCHAR(10) COLLATE {UTF8}, nv NVARCHAR(10))")
    cursor.execute("INSERT INTO #lens VALUES (?, ?)", EMOJI, EMOJI)
    cursor.connection.commit()

    row = _one(cursor, "SELECT LEN(v), DATALENGTH(v), LEN(nv), DATALENGTH(nv) FROM #lens")

    # UTF-8: one character, four bytes. UTF-16 (NVARCHAR): two code units, four bytes.
    assert row == (1, 4, 2, 4)


def test_fetched_four_byte_char_is_single_python_character(cursor):
    cursor.execute(f"CREATE TABLE #one (v VARCHAR(4) COLLATE {UTF8})")
    cursor.execute("INSERT INTO #one VALUES (?)", EMOJI)
    cursor.connection.commit()

    value = _one(cursor, "SELECT v FROM #one")[0]

    assert value == EMOJI
    assert len(value) == 1
    assert ord(value) == 0x1F600


def test_cursor_description_reports_str_and_declared_length_for_utf8_varchar(cursor):
    cursor.execute(f"CREATE TABLE #desc (v VARCHAR(10) COLLATE {UTF8})")
    cursor.execute("SELECT v FROM #desc")

    name, type_code, _, _, precision, _, _ = cursor.description[0]

    assert name == "v"
    assert type_code is str
    assert precision == 10


def test_permanent_utf8_varchar_roundtrip_with_byte_length(cursor, table_name):
    text = f"ü{EURO}{EMOJI}"  # 2 + 3 + 4 = 9 bytes, 3 characters
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, v VARCHAR(12) COLLATE {UTF8})")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?, ?)", 1, text)
    cursor.connection.commit()

    row = _one(cursor, f"SELECT v, LEN(v), DATALENGTH(v) FROM dbo.{table_name} WHERE id = ?", 1)

    assert row == (text, 3, 9)
