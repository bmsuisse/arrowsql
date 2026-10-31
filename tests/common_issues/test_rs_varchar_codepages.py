"""VARCHAR with non-Latin1 collations: code-page decoding, parameter round-trip,
and characters that cannot be represented becoming '?'.

A VARCHAR column stores bytes in the code page of its collation, not Unicode.
A driver must decode fetched bytes with that code page, encode parameters into
it (or let the server do so), and count truncation in bytes. Failure modes a
native-TDS reimplementation can easily get wrong:

- decoding with the database default (cp1252) or UTF-8 instead of the column's code page
- sending SQL text or parameters as cp1252/UTF-8 bytes, so the server sees '?'
- treating VARCHAR(n) as n characters instead of n bytes (DBCS: CJK is 2 bytes/char)
- unrepresentable characters must become '?' (lossy), not raise or round-trip
- a literal without the N prefix is converted by the server to the default code page

Sources (research, not all directly applicable):
- https://github.com/microsoft/mssql-python/issues/531 (VARCHAR with UTF-8 collation not round-tripping)
- https://github.com/microsoft/mssql-python/issues/435 (VARCHAR fetch wrong for non-ASCII CP1252 at exact column size)
- https://learn.microsoft.com/en-us/SQL/connect/python/mssql-python/string-unicode?view=sql-server-2017 (varchar decoded with collation's code page)
- https://github.com/mkleehammer/pyodbc/issues/839 (varchar vs. bytes parameters, binary/image clash)
- https://www.sommarskog.se/collations.html (characters outside the code page become ASCII fallbacks / '?')
- https://github.com/meziantou/Meziantou.Framework/pull/3086 (char/varchar values decoded with the collation's sort-ID code page, not UTF-8)

Expected values were verified against the upstream ODBC driver.
"""

import pytest
from mssql_python import DatabaseError, DataError

CYRILLIC = "Привет"  # cp1251: cff0e8e2e5f2
JAPANESE = "日本語テキスト"  # cp932, 7 chars, 14 bytes
CHINESE = "中文字符"  # cp936 (GBK), 4 chars, 8 bytes
HEBREW = "שלום"  # cp1255: f9ece5ed
ARABIC = "مرحبا"  # cp1256: e3d1cdc8c7
GREEK = "Καλημέρα"  # cp1253: cae1ebe7ecddf1e1

CYR_COLL = "Cyrillic_General_CI_AS"
JP_COLL = "Japanese_CI_AS"
ZH_COLL = "Chinese_PRC_CI_AS"
GR_COLL = "Greek_CI_AS"
HE_COLL = "Hebrew_CI_AS"
AR_COLL = "Arabic_CI_AS"


def _create_varchar(cursor, collation, size=50):
    cursor.execute(f"CREATE TABLE #v (id INT, v VARCHAR({size}) COLLATE {collation} NULL)")


def _fetch_scalar(cursor, sql, *params):
    cursor.execute(sql, *params)
    return cursor.fetchone()[0]


@pytest.mark.parametrize(
    "collation, text, expected_bytes",
    [
        (CYR_COLL, CYRILLIC, CYRILLIC.encode("cp1251")),
        (JP_COLL, JAPANESE, JAPANESE.encode("cp932")),
        (ZH_COLL, CHINESE, CHINESE.encode("gbk")),
        (HE_COLL, HEBREW, HEBREW.encode("cp1255")),
        (AR_COLL, ARABIC, ARABIC.encode("cp1256")),
        (GR_COLL, GREEK, GREEK.encode("cp1253")),
    ],
)
def test_param_stored_in_column_codepage_and_fetched_exactly(cursor, collation, text, expected_bytes):
    _create_varchar(cursor, collation)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", text)
    cursor.connection.commit()

    cursor.execute("SELECT v, CONVERT(VARBINARY(100), v) FROM #v WHERE id = 1")
    value, raw = cursor.fetchone()
    assert value == text
    assert type(value) is str
    assert bytes(raw) == expected_bytes


def test_cyrillic_fetch_returns_str_with_datalength_in_bytes(cursor):
    _create_varchar(cursor, CYR_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", CYRILLIC)
    cursor.connection.commit()

    cursor.execute("SELECT v, LEN(v), DATALENGTH(v) FROM #v WHERE id = 1")
    value, chars, nbytes = cursor.fetchone()
    assert value == "Привет"
    assert chars == 6
    assert nbytes == 6  # one byte per cp1251 character


def test_japanese_datalength_is_two_bytes_per_double_byte_char(cursor):
    _create_varchar(cursor, JP_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", JAPANESE)
    cursor.connection.commit()

    cursor.execute("SELECT v, LEN(v), DATALENGTH(v) FROM #v WHERE id = 1")
    value, chars, nbytes = cursor.fetchone()
    assert value == JAPANESE
    assert chars == 7
    assert nbytes == 14


def test_mixed_ascii_and_japanese_in_cp932_column(cursor):
    _create_varchar(cursor, JP_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", "a日b")
    cursor.connection.commit()

    cursor.execute("SELECT v, DATALENGTH(v), CONVERT(VARBINARY(10), v) FROM #v WHERE id = 1")
    value, nbytes, raw = cursor.fetchone()
    assert value == "a日b"
    assert nbytes == 4  # 'a' (1) + 日 (2) + 'b' (1)
    assert bytes(raw) == b"a" + "日".encode("cp932") + b"b"


def test_unrepresentable_chars_become_question_marks_in_greek_column(cursor):
    _create_varchar(cursor, GR_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", CYRILLIC)  # no Cyrillic in cp1253
    cursor.connection.commit()

    cursor.execute("SELECT v, DATALENGTH(v) FROM #v WHERE id = 1")
    value, nbytes = cursor.fetchone()
    assert value == "??????"
    assert nbytes == 6
    assert value != CYRILLIC  # lossy: the original text is not recoverable


def test_unrepresentable_cjk_in_greek_column_is_question_marks_per_char(cursor):
    _create_varchar(cursor, GR_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", "a日b")
    cursor.connection.commit()

    cursor.execute("SELECT v, DATALENGTH(v), CONVERT(VARBINARY(10), v) FROM #v WHERE id = 1")
    value, nbytes, raw = cursor.fetchone()
    assert value == "a?b"
    assert nbytes == 3  # 日 is a single '?' byte in cp1253
    assert bytes(raw) == b"a?b"


def test_lossy_parameter_does_not_match_original_text(cursor):
    _create_varchar(cursor, GR_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", CYRILLIC)
    cursor.connection.commit()

    cursor.execute("SELECT id FROM #v WHERE v = ?", CYRILLIC)
    assert [tuple(r) for r in cursor.fetchall()] == []

    cursor.execute("SELECT id FROM #v WHERE v = ?", "??????")
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]


def test_inline_literal_without_n_prefix_uses_default_codepage(cursor):
    # Database default collation is SQL_Latin1_General_CP1, so an unprefixed
    # literal is converted to cp1252 by the server and loses Cyrillic.
    value = _fetch_scalar(cursor, "SELECT 'Привет'")
    assert value == "??????"


def test_nvarchar_literal_keeps_cyrillic_and_cast_to_column(cursor):
    _create_varchar(cursor, CYR_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, N'Привет')")
    cursor.connection.commit()

    cursor.execute("SELECT v FROM #v WHERE id = 1")
    assert cursor.fetchone()[0] == "Привет"


def test_truncation_counts_bytes_not_characters(cursor):
    # Japanese VARCHAR(4) holds two double-byte chars exactly.
    _create_varchar(cursor, JP_COLL, size=4)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", "日本")
    cursor.connection.commit()

    cursor.execute("SELECT v, DATALENGTH(v) FROM #v WHERE id = 1")
    assert tuple(cursor.fetchone()) == ("日本", 4)

    with pytest.raises(DatabaseError):
        cursor.execute("INSERT INTO #v VALUES (2, ?)", "日本語")  # 6 bytes > 4
    cursor.connection.rollback()


@pytest.mark.upstream_bug(
    reason=(
        "Same truncation (SQLSTATE 22001) raises DataError on the first failing insert "
        "on a connection but ProgrammingError (server error 8152) on later ones; "
        "PEP 249 DataError is the correct class every time"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_truncation_raises_data_error_consistently(cursor):
    _create_varchar(cursor, CYR_COLL, size=4)
    # Commit so the rollback below keeps #v; otherwise the second insert hits a missing table.
    cursor.connection.commit()
    for row_id in (1, 2):
        with pytest.raises(DataError):
            cursor.execute("INSERT INTO #v VALUES (?, ?)", row_id, "Привет")  # 6 bytes > 4
        cursor.connection.rollback()


def test_cyrillic_truncation_at_exact_byte_boundary(cursor):
    _create_varchar(cursor, CYR_COLL, size=4)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", "Прив")  # exactly 4 bytes
    cursor.connection.commit()
    assert _fetch_scalar(cursor, "SELECT v FROM #v WHERE id = 1") == "Прив"

    with pytest.raises(DatabaseError):
        cursor.execute("INSERT INTO #v VALUES (2, ?)", "Приве")  # 5 bytes
    cursor.connection.rollback()


def test_executemany_mixed_codepage_rows(cursor):
    _create_varchar(cursor, CYR_COLL)
    rows = [(1, "Привет"), (2, "Мир"), (3, None), (4, "ё")]
    cursor.executemany("INSERT INTO #v VALUES (?, ?)", rows)
    cursor.connection.commit()

    cursor.execute("SELECT id, v FROM #v ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "Привет"), (2, "Мир"), (3, None), (4, "ё")]


def test_null_varchar_in_non_latin1_column_is_none(cursor):
    _create_varchar(cursor, JP_COLL)
    cursor.execute("INSERT INTO #v (id) VALUES (1)")
    cursor.connection.commit()

    cursor.execute("SELECT v, DATALENGTH(v) FROM #v WHERE id = 1")
    assert tuple(cursor.fetchone()) == (None, None)


def test_permanent_table_chinese_gbk_roundtrip(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, v VARCHAR(40) COLLATE {ZH_COLL})")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1, ?)", CHINESE)
    cursor.connection.commit()

    cursor.execute(f"SELECT v, CONVERT(VARBINARY(20), v) FROM dbo.{table_name} WHERE id = 1")
    value, raw = cursor.fetchone()
    assert value == CHINESE
    assert bytes(raw) == CHINESE.encode("gbk")


def test_hebrew_and_arabic_in_separate_columns_keep_their_codepages(cursor):
    cursor.execute(
        f"CREATE TABLE #multi (id INT, h VARCHAR(20) COLLATE {HE_COLL}, a VARCHAR(20) COLLATE {AR_COLL})"
    )
    cursor.execute("INSERT INTO #multi VALUES (1, ?, ?)", HEBREW, ARABIC)
    cursor.connection.commit()

    cursor.execute("SELECT h, a, CONVERT(VARBINARY(10), h), CONVERT(VARBINARY(10), a) FROM #multi")
    h, a, raw_h, raw_a = cursor.fetchone()
    assert (h, a) == (HEBREW, ARABIC)
    assert bytes(raw_h) == HEBREW.encode("cp1255")
    assert bytes(raw_a) == ARABIC.encode("cp1256")


def test_cursor_description_reports_str_for_codepage_varchar(cursor):
    _create_varchar(cursor, ZH_COLL)
    cursor.execute("INSERT INTO #v VALUES (1, ?)", CHINESE)
    cursor.connection.commit()

    cursor.execute("SELECT v FROM #v WHERE id = 1")
    assert cursor.description[0][0] == "v"
    row = tuple(cursor.fetchone())
    assert row == (CHINESE,)
    assert type(row[0]) is str
