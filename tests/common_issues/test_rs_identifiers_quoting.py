"""Identifier quoting, special names, length limits and QUOTED_IDENTIFIER.

Covers how a driver must treat SQL Server identifiers that reach it from user
code: bracket-quoted names with spaces, ``]``, dots and unicode; reserved words
used as names; the 128-character limit (116 for local temp tables); the effect
of SET QUOTED_IDENTIFIER ON/OFF on double quotes; and duplicate or unnamed
column aliases in ``cursor.description``. A native-TDS reimplementation can
easily get wrong:

- reporting the bracket-escaped form (``a]]b``) instead of the unescaped name
  (``a]b``) in ``cursor.description``
- trimming or normalizing spaces inside bracketed or aliased names
- treating double-quoted text as a string literal when QUOTED_IDENTIFIER is OFF
  (and as an identifier when it is ON), and assuming brackets follow the same rule
- counting the 128-character limit in bytes (non-ASCII names are one char each)
  or applying it to the delimiters
- collapsing duplicate column names in ``description`` or in row results
- returning a name for unnamed expressions (SQL Server gives none; the value
  must come back as an empty name, not a generated ``Column1``)

Sources (research; expected values verified against the upstream ODBC driver in
.venv-upstream):
- https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-ver17
  (regular vs delimited identifiers; 1-128 characters; 116 for local temp tables)
- https://learn.microsoft.com/en-us/sql/t-sql/functions/quotename-transact-sql?view=sql-server-ver17
  (``]`` inside a bracketed identifier is escaped by doubling; sysname length)
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/row-objects?view=sql-server-linux-ver15
  (mssql-python row objects keyed by the column names from cursor.description;
  duplicate names collapse in name-based access, so index access is needed)
- https://doc.ispirer.com/sqlways/Output/SQLWays-1-035.html
  (delimited identifiers may contain spaces, keywords and special characters)
- https://www.sqlservercentral.com/forums/topic/the-identifier-that-starts-with-is-too-long-maximum-length-is-128
  (double-quoted text read as an identifier vs a literal depending on
  QUOTED_IDENTIFIER; brackets are unaffected by the setting)
- https://github.com/mkleehammer/pyodbc/issues/194
  (garbled column names in cursor.description after a driver/encoding change)
- https://newreleases.io/project/pypi/pyodbc/release/4.0.5
  (pyodbc 4.0.5 fixed decoding of non-ASCII column aliases in description)
"""

import pytest
from mssql_python import ProgrammingError


def _names(cursor):
    return [col[0] for col in cursor.description]


def _row(cursor):
    return tuple(cursor.fetchone())


def test_bracketed_name_with_space_is_described_verbatim(cursor):
    cursor.execute("CREATE TABLE #sp ([sp ace] int)")
    cursor.execute("INSERT INTO #sp ([sp ace]) VALUES (?)", (7,))
    cursor.execute("SELECT [sp ace] FROM #sp")
    assert _names(cursor) == ["sp ace"]
    assert cursor.fetchone()[0] == 7


def test_escaped_closing_bracket_is_unescaped_in_description(cursor):
    # Inside [...] a literal ']' is written as ']]'. The name the server
    # reports is the unescaped one, 'a]b'.
    cursor.execute("CREATE TABLE #br ([a]]b] int)")
    cursor.execute("INSERT INTO #br ([a]]b]) VALUES (?)", (3,))
    cursor.execute("SELECT [a]]b] FROM #br")
    assert _names(cursor) == ["a]b"]
    assert _row(cursor) == (3,)


def test_bracketed_alias_with_closing_bracket(cursor):
    cursor.execute("SELECT 1 AS [a]]b]")
    assert _names(cursor) == ["a]b"]
    assert _row(cursor) == (1,)


def test_dot_in_bracketed_name_is_not_a_qualifier(cursor):
    cursor.execute("CREATE TABLE #dot ([x.y] int)")
    cursor.execute("INSERT INTO #dot ([x.y]) VALUES (?)", (5,))
    cursor.execute("SELECT [x.y] FROM #dot")
    assert _names(cursor) == ["x.y"]
    assert cursor.fetchone()[0] == 5


def test_reserved_word_column_names(cursor):
    cursor.execute("CREATE TABLE #kw ([select] int, [from] nvarchar(10), [order] int)")
    cursor.execute("INSERT INTO #kw ([select], [from], [order]) VALUES (?, ?, ?)", (1, "x", 2))
    cursor.execute("SELECT [select], [from], [order] FROM #kw")
    assert _names(cursor) == ["select", "from", "order"]
    assert _row(cursor) == (1, "x", 2)


def test_unicode_column_name_roundtrip(cursor):
    cursor.execute("CREATE TABLE #uni ([ünï] nvarchar(10))")
    cursor.execute("INSERT INTO #uni ([ünï]) VALUES (?)", ("Zürich",))
    cursor.execute("SELECT [ünï] FROM #uni")
    assert _names(cursor) == ["ünï"]
    assert cursor.fetchone()[0] == "Zürich"


def test_unicode_alias_is_preserved_exactly(cursor):
    cursor.execute("SELECT 'x' AS [été]")
    assert _names(cursor) == ["été"]
    assert len(_names(cursor)[0]) == 3  # code points, not UTF-8 bytes


def test_supplementary_plane_character_in_alias(cursor):
    # U+1F600 is one code point but two UTF-16 units; the description name
    # must be the single character, not a surrogate-split string.
    cursor.execute("SELECT 3 AS [\U0001F600]")
    names = _names(cursor)
    assert names == ["\U0001F600"]
    assert len(names[0]) == 1
    assert _row(cursor) == (3,)


def test_leading_and_trailing_spaces_in_alias_are_kept(cursor):
    cursor.execute("SELECT 1 AS [ leading], 2 AS [trailing ]")
    assert _names(cursor) == [" leading", "trailing "]
    assert _row(cursor) == (1, 2)


def test_empty_looking_bracket_alias_is_single_space(cursor):
    cursor.execute("SELECT 1 AS [ ]")
    assert _names(cursor) == [" "]


def test_128_char_alias_is_accepted(cursor):
    alias = "a" * 128
    cursor.execute(f"SELECT 1 AS [{alias}]")
    assert _names(cursor) == [alias]
    assert len(cursor.description[0][0]) == 128


def test_129_char_alias_is_rejected_with_programming_error(cursor):
    alias = "a" * 129
    with pytest.raises(ProgrammingError):
        cursor.execute(f"SELECT 1 AS [{alias}]")


def test_116_char_local_temp_table_name_is_accepted(cursor):
    name = "#" + "t" * 115  # 116 characters including '#'
    cursor.execute(f"CREATE TABLE [{name}] (v int)")
    cursor.execute(f"INSERT INTO [{name}] (v) VALUES (?)", (9,))
    cursor.execute(f"SELECT v FROM [{name}]")
    assert _row(cursor) == (9,)


def test_128_char_permanent_table_name_roundtrip(cursor, conn):
    name = "ci_" + "p" * 125  # 128 characters
    cursor.execute(f"CREATE TABLE dbo.[{name}] (v int)")
    try:
        cursor.execute(f"INSERT INTO dbo.[{name}] (v) VALUES (?)", (11,))
        cursor.execute(f"SELECT v FROM dbo.[{name}]")
        assert _row(cursor) == (11,)
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.[{name}]")
        conn.commit()


def test_quoted_identifier_on_double_quotes_are_identifiers(cursor):
    cursor.execute("SET QUOTED_IDENTIFIER ON")
    cursor.execute('SELECT 1 AS "quoted"')
    assert _names(cursor) == ["quoted"]
    assert _row(cursor) == (1,)


def test_quoted_identifier_off_double_quotes_are_string_literals(cursor):
    cursor.execute("SET QUOTED_IDENTIFIER OFF")
    cursor.execute('SELECT "not an ident" AS x')
    assert _row(cursor) == ("not an ident",)


def test_brackets_work_regardless_of_quoted_identifier(cursor):
    cursor.execute("SET QUOTED_IDENTIFIER OFF")
    cursor.execute("SELECT 2 AS [br]")
    assert _names(cursor) == ["br"]
    assert _row(cursor) == (2,)


def test_duplicate_alias_keeps_both_description_entries(cursor):
    cursor.execute("SELECT 1 AS n, 2 AS n")
    assert _names(cursor) == ["n", "n"]
    row = cursor.fetchone()
    assert tuple(row) == (1, 2)
    assert row[0] == 1 and row[1] == 2


def test_alias_names_differing_only_in_case_are_both_described(cursor):
    cursor.execute("SELECT 1 AS a, 2 AS A")
    assert _names(cursor) == ["a", "A"]
    assert _row(cursor) == (1, 2)


def test_same_column_name_from_two_tables_in_join(cursor):
    cursor.execute("CREATE TABLE #l (id int)")
    cursor.execute("CREATE TABLE #r (id int)")
    cursor.execute("INSERT INTO #l VALUES (?)", (1,))
    cursor.execute("INSERT INTO #r VALUES (?)", (2,))
    cursor.execute("SELECT l.id, r.id FROM #l AS l CROSS JOIN #r AS r")
    assert _names(cursor) == ["id", "id"]
    assert _row(cursor) == (1, 2)


def test_unnamed_expression_column_has_empty_description_name(cursor):
    cursor.execute("SELECT 1 + 1")
    assert _names(cursor) == [""]
    assert _row(cursor) == (2,)


def test_description_preserves_alias_case(cursor):
    cursor.execute("SELECT 1 AS MixedCase_Name")
    assert _names(cursor) == ["MixedCase_Name"]
