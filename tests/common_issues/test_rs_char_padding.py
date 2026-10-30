"""CHAR/NCHAR fixed-length padding, trailing-space semantics, ANSI_PADDING, and
LEN vs DATALENGTH.

Fixed-length types (char, nchar, binary) are returned by the server padded to
their declared length. Variable-length types (varchar, nvarchar, varbinary)
keep what was inserted, unless ANSI_PADDING is OFF (varchar/varbinary only).
Comparisons with '=' ignore trailing spaces, LIKE does not. A native-TDS
reimplementation can easily get wrong:

- trimming CHAR/NCHAR values on fetch (the server pads them; the driver must not
  "helpfully" strip, and must not pad varchar)
- counting nchar padding in characters instead of the declared length, or
  reporting DATALENGTH in characters instead of bytes (nchar = 2 bytes/char)
- treating '' as NULL for char(n) (it fetches as n spaces)
- binary(n) padding with 0x00 (not spaces) and varbinary keeping trailing zeros
- sending parameters with a different collation/padding than the server expects,
  so trailing-space equality and unique-index collisions behave differently
- ignoring the session ANSI_PADDING setting when the table is created, so
  varchar trailing spaces are stored (ON) or trimmed (OFF) by the server

Sources (research; the expected values were verified against the upstream ODBC
driver in .venv-upstream):
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/string-unicode?view=sql-server-2017
  (mssql-python docs: char(n) pads with trailing spaces; advice is rstrip())
- https://learn.microsoft.com/en-us/sql/t-sql/statements/set-ansi-padding-transact-sql
  (ANSI_PADDING semantics; nchar/nvarchar/varbinary(max) unaffected; always ON
  from SQL Server 2017 and on Azure SQL)
- https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2005/ms187403(v=sql.90)
  (older ANSI_PADDING wording; OFF was slated to become an error)
- https://www.sqlservercentral.com/articles/ansi-options-part-1-ansi_padding
  (CHAR/BINARY padded regardless of setting; variable types depend on it)
- https://sqlstudies.com/2017/06/15/turning-ansi_padding-off-and-why-you-shouldnt/
  (differing ANSI_PADDING per column breaks partition switching)
- https://github.com/hugr-lab/mssql-extension/issues/405
  (char(n) padding reached a consumer on some read paths; joins/GROUP BY disagree
  because 'AB ' = 'AB  ' under SQL Server equality)
- https://github.com/dotnet/ef6/issues/652
  (nchar/char values returned with padding cause equality and display problems)
- https://github.com/mkleehammer/pyodbc/issues/750
  (returned values are whatever the database holds; trimming is the caller's job)
- https://github.com/yugabyte/yugabyte-db/issues/34211
  (CHAR(n) trailing whitespace trimming differs across engines)
- https://github.com/trinodb/trino/issues/31004
  (TRIM/RTRIM on CHAR removed real trailing spaces, not just padding)
"""

import pytest
from mssql_python import DataError, IntegrityError, ProgrammingError


def _fetch_one(cursor, sql, params=()):
    cursor.execute(sql, params)
    return cursor.fetchone()


def test_char_fetch_is_padded_to_declared_length(cursor):
    cursor.execute("CREATE TABLE #c (c char(6))")
    cursor.execute("INSERT INTO #c VALUES (?)", ("ab",))
    value = _fetch_one(cursor, "SELECT c FROM #c")[0]
    assert type(value) is str
    assert value == "ab    "
    assert len(value) == 6


def test_char_empty_string_fetches_as_spaces_not_null(cursor):
    cursor.execute("CREATE TABLE #c (c char(3))")
    cursor.execute("INSERT INTO #c VALUES (?)", ("",))
    value = _fetch_one(cursor, "SELECT c FROM #c")[0]
    assert value is not None
    assert value == "   "


def test_char_null_stays_none(cursor):
    cursor.execute("CREATE TABLE #c (c char(3))")
    cursor.execute("INSERT INTO #c VALUES (?)", (None,))
    assert _fetch_one(cursor, "SELECT c FROM #c")[0] is None


def test_nchar_fetch_is_padded_and_str_typed(cursor):
    cursor.execute("CREATE TABLE #n (nc nchar(4))")
    cursor.execute("INSERT INTO #n VALUES (?)", ("x",))
    value = _fetch_one(cursor, "SELECT nc FROM #n")[0]
    assert type(value) is str
    assert value == "x   "
    assert len(value) == 4


def test_char_insert_with_trailing_spaces_within_width(cursor):
    cursor.execute("CREATE TABLE #c (c char(4))")
    cursor.execute("INSERT INTO #c VALUES (?)", ("ab  ",))
    assert _fetch_one(cursor, "SELECT c FROM #c")[0] == "ab  "


def test_varchar_keeps_inserted_trailing_spaces(cursor):
    cursor.execute("CREATE TABLE #v (v varchar(10), nv nvarchar(10))")
    cursor.execute("INSERT INTO #v VALUES (?, ?)", ("ab  ", "ab  "))
    row = _fetch_one(cursor, "SELECT v, nv FROM #v")
    assert row[0] == "ab  "
    assert row[1] == "ab  "
    assert type(row[0]) is str and type(row[1]) is str


def test_len_ignores_trailing_spaces_but_datalength_does_not(cursor):
    cursor.execute(
        "CREATE TABLE #l (c char(6), nc nchar(4), v varchar(10))"
    )
    cursor.execute("INSERT INTO #l VALUES (?, ?, ?)", ("ab", "x", "ab  "))
    row = _fetch_one(
        cursor,
        "SELECT LEN(c), DATALENGTH(c), LEN(nc), DATALENGTH(nc), LEN(v), DATALENGTH(v) FROM #l",
    )
    # LEN excludes trailing spaces; DATALENGTH counts padding (char: 1 byte per
    # char, nchar: 2 bytes per char) and the stored spaces of varchar.
    assert tuple(row) == (2, 6, 1, 8, 2, 4)
    assert all(type(x) is int for x in row)


def test_datalength_of_max_width_nchar_is_bytes_not_chars(cursor):
    cursor.execute("CREATE TABLE #m (nc nchar(4000))")
    cursor.execute("INSERT INTO #m VALUES (?)", ("",))
    row = _fetch_one(cursor, "SELECT LEN(nc), DATALENGTH(nc) FROM #m")
    assert tuple(row) == (0, 8000)
    value = _fetch_one(cursor, "SELECT nc FROM #m")[0]
    assert value == " " * 4000


def test_char_equality_ignores_trailing_spaces_on_parameter(cursor):
    cursor.execute("CREATE TABLE #e (c char(6), v varchar(10))")
    cursor.execute("INSERT INTO #e VALUES (?, ?)", ("ab", "ab"))
    # '=' is padding-insensitive for varchar too; the bound value's trailing
    # spaces do not stop a match.
    rows_c = cursor.execute("SELECT COUNT(*) FROM #e WHERE c = ?", ("ab  ",)).fetchone()
    rows_v = cursor.execute("SELECT COUNT(*) FROM #e WHERE v = ?", ("ab  ",)).fetchone()
    assert rows_c[0] == 1
    assert rows_v[0] == 1


def test_like_on_varchar_with_bound_pattern_matches_trailing_spaces(cursor):
    # Server ground truth on upstream: a varchar value with trailing spaces
    # matches an un-wildcarded bound pattern without them.
    cursor.execute("CREATE TABLE #k (v varchar(10))")
    cursor.execute("INSERT INTO #k VALUES (?)", ("ab  ",))
    count = _fetch_one(cursor, "SELECT COUNT(*) FROM #k WHERE v LIKE ?", ("ab",))[0]
    assert count == 1


def test_like_with_nvarchar_literal_pattern_is_trailing_space_sensitive(cursor):
    # The same comparison with an N'' pattern does NOT ignore the trailing
    # spaces: the pattern type changes LIKE semantics. A driver that re-types
    # parameters must not silently change which rows match.
    cursor.execute("CREATE TABLE #k2 (v varchar(10))")
    cursor.execute("INSERT INTO #k2 VALUES (?)", ("ab  ",))
    count = _fetch_one(cursor, "SELECT COUNT(*) FROM #k2 WHERE v LIKE N'ab'")[0]
    assert count == 0


def test_unique_index_treats_trailing_space_variants_as_duplicates(cursor):
    cursor.execute("CREATE TABLE #u (v varchar(10) UNIQUE)")
    cursor.execute("INSERT INTO #u VALUES (?)", ("ab",))
    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #u VALUES (?)", ("ab  ",))


def test_char_cast_expression_is_padded(cursor):
    value = _fetch_one(cursor, "SELECT CAST(? AS char(5))", ("ab",))[0]
    assert value == "ab   "
    assert len(value) == 5


def test_rtrim_removes_char_padding_on_fetch(cursor):
    cursor.execute("CREATE TABLE #r (c char(6))")
    cursor.execute("INSERT INTO #r VALUES (?)", ("ab",))
    value = _fetch_one(cursor, "SELECT RTRIM(c) FROM #r")[0]
    assert value == "ab"


def test_binary_is_padded_with_zero_bytes(cursor):
    cursor.execute("CREATE TABLE #b (b binary(3))")
    cursor.execute("INSERT INTO #b VALUES (?)", (b"\x01",))
    value = _fetch_one(cursor, "SELECT b FROM #b")[0]
    assert type(value) is bytes
    assert value == b"\x01\x00\x00"


def test_varbinary_keeps_trailing_zero_bytes(cursor):
    cursor.execute("CREATE TABLE #vb (vb varbinary(4))")
    cursor.execute("INSERT INTO #vb VALUES (?)", (b"\x01\x00",))
    value = _fetch_one(cursor, "SELECT vb FROM #vb")[0]
    assert type(value) is bytes
    assert value == b"\x01\x00"


def test_session_ansi_padding_on_by_default(cursor):
    value = _fetch_one(cursor, "SELECT SESSIONPROPERTY('ANSI_PADDING')")[0]
    assert value == 1


def test_ansi_padding_off_trims_varchar_but_not_char(cursor):
    cursor.execute("SET ANSI_PADDING OFF")
    try:
        cursor.execute("CREATE TABLE #off (c char(4), v varchar(10))")
        cursor.execute("INSERT INTO #off VALUES (?, ?)", ("ab  ", "ab  "))
    finally:
        cursor.execute("SET ANSI_PADDING ON")
    row = _fetch_one(cursor, "SELECT c, v FROM #off")
    # char is padded regardless of ANSI_PADDING; varchar trailing spaces are trimmed.
    assert row[0] == "ab  "
    assert row[1] == "ab"


def test_char_overlong_nonspace_parameter_raises(cursor):
    # Only trailing spaces may be dropped silently; real overflow is an error.
    cursor.execute("CREATE TABLE #t (c char(3))")
    with pytest.raises((DataError, ProgrammingError)):
        cursor.execute("INSERT INTO #t VALUES (?)", ("abcdefgh",))
