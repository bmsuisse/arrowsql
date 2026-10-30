"""Positional (qmark) parameter substitution edge cases.

Test ideas inspired by pymssql's parameter-substitution tests (pymssql/pymssql
tests/test_parameters.py, LGPL-2.1). No code or test text was copied: these
cases were re-derived from the behaviour they describe and are written fresh
against mssql_python's DB-API ``?`` placeholders.

The driver must only treat a ``?`` as a placeholder when it is SQL syntax.
Question marks inside literals, comments and bracketed identifiers, and any
value bound to a placeholder, are data. Expected values come from the upstream
ODBC driver, which is the ground truth for this suite.
"""

import pytest

from mssql_python import ProgrammingError


def _one(cursor, sql, params=None):
    cursor.execute(sql, params) if params is not None else cursor.execute(sql)
    row = cursor.fetchone()
    return tuple(row)


def test_qmark_inside_string_literal_is_not_a_placeholder(cursor):
    assert _one(cursor, "SELECT '?' AS lit, ? AS v", (7,)) == ("?", 7)


def test_qmark_inside_unicode_literal_is_not_a_placeholder(cursor):
    assert _one(cursor, "SELECT N'日本?語' AS s, ? AS v", ("x",)) == ("日本?語", "x")


def test_qmark_inside_line_comment_is_ignored(cursor):
    sql = "SELECT ? AS v -- the ? here is only a comment\n"
    assert _one(cursor, sql, (3,)) == (3,)


def test_qmark_inside_block_comment_is_ignored(cursor):
    sql = "/* ? ? ? */ SELECT ? AS v"
    assert _one(cursor, sql, (4,)) == (4,)


def test_qmark_inside_bracketed_identifier_is_not_a_placeholder(cursor):
    cursor.execute("SELECT ? AS [a?b]", (5,))
    assert cursor.description[0][0] == "a?b"
    assert tuple(cursor.fetchone()) == (5,)


def test_question_marks_in_bound_value_are_data(cursor):
    assert _one(cursor, "SELECT ? AS v", ("what?? ? ?",)) == ("what?? ? ?",)


def test_sql_text_in_bound_value_is_data_and_does_not_run(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v NVARCHAR(200))")
    cursor.connection.commit()
    payload = "x'); DROP TABLE dbo." + table_name + "; --"
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", (payload,))
    cursor.execute(f"SELECT v FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (payload,)


def test_quote_backslash_and_newline_in_value_roundtrip(cursor):
    value = "it's a \\ back\nslash and a 'quote'"
    assert _one(cursor, "SELECT ? AS v", (value,)) == (value,)


def test_percent_in_sql_text_is_untouched_when_params_given(cursor):
    assert _one(cursor, "SELECT ? AS v WHERE 5 % 3 = 2", ("ok",)) == ("ok",)


def test_like_pattern_bound_as_parameter_keeps_wildcard(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (name NVARCHAR(50))")
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (name) VALUES (N'Joe'), (N'Jo%'), (N'Jane'), (N'Bob')"
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE name LIKE ?", ("Jo%",))
    # 'Joe' and the literal 'Jo%' both start with 'Jo'.
    assert cursor.fetchone()[0] == 2


def test_in_list_with_one_placeholder_per_value(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id) VALUES (1), (2), (3), (4), (5)")
    cursor.connection.commit()
    cursor.execute(
        f"SELECT id FROM dbo.{table_name} WHERE id IN (?, ?, ?) ORDER BY id", (1, 3, 9)
    )
    assert [tuple(r) for r in cursor.fetchall()] == [(1,), (3,)]


def test_parameter_values_bind_by_position_not_by_type(cursor):
    assert _one(cursor, "SELECT ? - ?", (1000, 300)) == (700,)
    assert _one(cursor, "SELECT ? - ?", (300, 1000)) == (-700,)


def test_list_and_tuple_parameters_are_equivalent(cursor):
    sql = "SELECT ? AS a, ? AS b"
    assert _one(cursor, sql, [1, "a"]) == _one(cursor, sql, (1, "a"))


def test_none_in_each_position_becomes_sql_null(cursor):
    cursor.execute(
        "SELECT ? AS a, ? AS b, ? AS c, CASE WHEN ? IS NULL THEN 'null' END AS d",
        (None, None, "x", None),
    )
    assert tuple(cursor.fetchone()) == (None, None, "x", "null")


def test_qmark_mixed_with_comment_block_spanning_lines(cursor):
    sql = """
        SELECT
            ?  AS first_col,  -- ? placeholder before this comment is real
            /* ? in block
               comment */ ?   AS second_col
    """
    assert _one(cursor, sql, ("a", "b")) == ("a", "b")


def test_missing_parameter_when_literal_question_mark_is_present_raises(cursor):
    # The ? inside the literal is not a placeholder, so only one value is needed.
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT '?' AS lit, ? AS v, ? AS w", ("only one",))
