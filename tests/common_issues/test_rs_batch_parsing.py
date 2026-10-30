"""Batch text edge cases: qmark placeholders vs. literals/comments/identifiers,
GO separator, N-prefixed literals, DECLARE variables, long single-line batches.

Research sources (issue trackers and docs; the rewrite must produce the same
results the ODBC driver produces over TDS):
- https://github.com/microsoft/mssql-python/pull/465 (qmark false-positive fix for
  "?" inside bracketed identifiers, string literals, quoted identifiers and comments;
  tracked as issue #464 and released in mssql-python 1.5.0)
- https://techcommunity.microsoft.com/blog/sqlserver/mssql-python-1-5-apache-arrow-sql-variant-and-native-uuids/4510363
  ("Qmark false positive fix" in the 1.5 announcement; detector ignored ? in comments,
  string literals, bracketed and double-quoted identifiers)
- https://www.sqlservercentral.com/forums/topic/incorrect-syntax-near-go-1
  and https://www.sqlines.com/sql-server/sql_batch_go
  (GO is a batch terminator recognized by SSMS/sqlcmd only; the server rejects it with
  "Incorrect syntax near 'GO'" when a client sends it as T-SQL text)
- https://sqlquantumleap.com/2021/08/31/sqlcmd-prevent-an-entire-batch-from-even-parsing-with-one-magical-character-cruel-joke-4/
  (a bare "?" in a batch is a sqlcmd-only hazard; a native client must not reinterpret it)
- https://github.com/dbeaver/dbeaver/issues/2220 and https://github.com/dbeaver/dbeaver/issues/26870
  (GO sent through a non-SSMS client fails with "Incorrect syntax near 'GO'")
- https://github.com/MicrosoftDocs/sql-docs/blob/live/docs/t-sql/statements/set-quoted-identifier-transact-sql.md
  (double-quoted identifiers require QUOTED_IDENTIFIER ON; "?" inside [..] and "..." is legal)

Behaviors a native-TDS reimplementation can easily get wrong:
- a "?" inside '...', N'...', [...], "..." or -- / /* */ comments must not be bound
- a quote or bracket inside a comment ("don't", "[") must not open a literal/identifier
- doubled quotes ('') and doubled brackets (]]) must not end the token early
- GO is not a T-SQL statement: it must surface as a syntax error, not be stripped
- parameter count mismatch must be a ProgrammingError, not a silent mis-bind
- batch rewriting must survive very long single-line batches and many placeholders
"""

import pytest
from mssql_python import ProgrammingError


def _one(cursor, sql, params=None):
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def test_qmark_inside_string_literal_is_data(cursor):
    row = _one(cursor, "SELECT '?' AS a, ? AS b", (5,))
    assert row == ("?", 5)
    assert type(row[1]) is int


def test_qmark_inside_string_literal_with_multiple_marks(cursor):
    row = _one(cursor, "SELECT 'a?b?c' AS a, ? AS b, ? AS c", (1, 2))
    assert row == ("a?b?c", 1, 2)


def test_qmark_inside_line_comment_is_not_a_placeholder(cursor):
    row = _one(cursor, "SELECT ? AS a -- really a ? here\n", (5,))
    assert row == (5,)


def test_qmark_and_apostrophe_inside_line_comment(cursor):
    # An unbalanced quote inside a comment must not open a string literal.
    row = _one(cursor, "SELECT ? AS a -- don't count this ?\n", (5,))
    assert row == (5,)


def test_qmark_inside_block_comment_is_not_a_placeholder(cursor):
    row = _one(cursor, "SELECT /* ? it's [ ? */ ? AS a", (5,))
    assert row == (5,)


def test_qmark_inside_bracketed_column_alias(cursor):
    cursor.execute("SELECT 1 AS [a?b], ? AS v", (7,))
    assert [d[0] for d in cursor.description] == ["a?b", "v"]
    assert tuple(cursor.fetchone()) == (1, 7)


def test_bracket_inside_string_literal_does_not_open_identifier(cursor):
    row = _one(cursor, "SELECT '[?]' AS a, ? AS b", (9,))
    assert row == ("[?]", 9)


def test_escaped_closing_bracket_in_identifier(cursor):
    # [a]]b?] is one identifier named a]b? ; the trailing ? is a real placeholder.
    cursor.execute("SELECT ? AS [a]]b?]", (3,))
    assert [d[0] for d in cursor.description] == ["a]b?"]
    assert tuple(cursor.fetchone()) == (3,)


def test_qmark_in_temp_table_bracketed_column_insert_and_select(cursor):
    cursor.execute("CREATE TABLE #batch_q ([q?] INT NOT NULL, [v?] NVARCHAR(40) NULL)")
    cursor.execute("INSERT INTO #batch_q ([q?], [v?]) VALUES (?, '?!')", (42,))
    cursor.execute("SELECT [q?], [v?] FROM #batch_q WHERE [q?] = ?", (42,))
    assert tuple(cursor.fetchone()) == (42, "?!")


def test_qmark_in_unicode_literal_is_data(cursor):
    row = _one(cursor, "SELECT N'?x' AS a, ? AS b", (7,))
    assert row == ("?x", 7)
    assert isinstance(row[0], str)


def test_qmark_in_double_quoted_identifier(cursor):
    cursor.execute('SET QUOTED_IDENTIFIER ON; SELECT ? AS "q?"', (7,))
    assert [d[0] for d in cursor.description] == ["q?"]
    assert tuple(cursor.fetchone()) == (7,)


def test_escaped_quote_then_qmark_in_literal(cursor):
    # 'it''s ?' is one literal; the lone ? after it is the only placeholder.
    row = _one(cursor, "SELECT 'it''s ?' AS a, ? AS b", (7,))
    assert row == ("it's ?", 7)


def test_qmark_value_containing_question_marks_is_not_rewritten(cursor):
    row = _one(cursor, "SELECT ? AS q", ("??",))
    assert row == ("??",)
    assert isinstance(row[0], str)


def test_qmark_in_trailing_line_comment_without_newline(cursor):
    row = _one(cursor, "SELECT ? AS a -- ?", (4,))
    assert row == (4,)


def test_go_separator_is_a_syntax_error(cursor):
    with pytest.raises(ProgrammingError) as info:
        cursor.execute("SELECT 1 AS a\nGO\nSELECT 2 AS b")
    assert "GO" in str(info.value)


def test_go_inside_string_literal_is_plain_data(cursor):
    assert _one(cursor, "SELECT 'GO' AS g") == ("GO",)


def test_declare_variable_bound_from_qmark(cursor):
    row = _one(cursor, "DECLARE @x INT = ?; SELECT @x + 1 AS y", (41,))
    assert row == (42,)


def test_multi_statement_batch_with_params_nextset(cursor):
    cursor.execute("SELECT ? AS x; SELECT ? AS y", (4, 5))
    assert [tuple(r) for r in cursor.fetchall()] == [(4,)]
    assert cursor.nextset() is True
    assert [tuple(r) for r in cursor.fetchall()] == [(5,)]


def test_qmark_placeholder_count_mismatch_raises(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT ?, ?", (1,))


def test_long_single_line_batch_with_qmark_literal(cursor):
    literal = "?x" * 30000  # 60,000 characters, all on one line, with ? inside
    cursor.execute("SELECT ? AS a, N'" + literal + "' AS b", (9,))
    row = cursor.fetchone()
    assert row[0] == 9
    assert len(row[1]) == 60000
    assert row[1] == literal


def test_long_single_line_batch_many_placeholders(cursor):
    count = 300
    sql = "SELECT " + "+".join(["CAST(? AS INT)"] * count) + " AS s"
    cursor.execute(sql, tuple(range(count)))
    assert tuple(cursor.fetchone()) == (sum(range(count)),)


def test_dml_with_qmark_literal_and_bound_param(cursor):
    cursor.execute("CREATE TABLE #batch_dml (id INT NOT NULL, note NVARCHAR(20) NOT NULL)")
    cursor.execute("INSERT INTO #batch_dml (id, note) VALUES (?, '?? ? --')", (1,))
    cursor.execute("UPDATE #batch_dml SET note = N'?' + ? WHERE id = ?", ("tail", 1))
    cursor.execute("SELECT id, note FROM #batch_dml")
    assert tuple(cursor.fetchone()) == (1, "?tail")
