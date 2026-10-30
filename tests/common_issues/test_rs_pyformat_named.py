"""Parameter styles: pyformat %(name)s, literal % escaping, style mismatches, LIKE patterns.

Research sources (what was consulted for real failure modes and documented behavior):
- https://devblogs.microsoft.com/python/write-sql-your-way-dual-parameter-style-benefits-in-mssql-python/
  mssql-python picks the style from the parameter type: dict -> pyformat (%(name)s),
  tuple/list -> qmark (?). Mixing styles is documented as an error; repeated named
  placeholders are expanded into a positional tuple internally (values duplicated).
- https://newreleases.io/project/github/microsoft/mssql-python/release/v1.2.0
  v1.2.0 "Parameter as Dictionary - Pyformat Support" (PR #385): adds pyformat next to
  qmark, makes pyformat the default paramstyle, adds a parameter_helper module that
  detects and converts styles in execute/executemany.
- https://github.com/microsoft/mssql-python/wiki/Global-Attributes-&-Functions-Overview
  paramstyle is "pyformat" by default; both %(name)s and ? are auto-detected.
- https://learn.microsoft.com/de-de/sql/connect/python/mssql-python/parameterized-queries?view=sql-server-2017
  pyformat is the default, qmark also works, qmark is needed for ODBC {CALL} escapes.
- https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/like-predicate-escape-character?view=sql-server-ver17
  LIKE treats % and _ as wildcards; a literal match needs an escape character
  (ESCAPE clause or bracket syntax such as [%] and [_]).
- https://github.com/elastic/elasticsearch-sql-odbc/pull/143
  Real-world report: applications passing % and _ in pattern arguments get
  broader matches than intended when they are not escaped.

No pyodbc or mssql-python issue number was verified in the search results for
the %s / %% questions, so those expectations come from probing the upstream driver.

Expected values are what the upstream ODBC driver returns today. Where upstream is
wrong, the test asserts the correct behavior and is an xfail(strict=True) with
@pytest.mark.upstream_bug so the gap is flagged until fixed.
"""

import pytest
from mssql_python.exceptions import ProgrammingError


def _rows(cursor):
    """Fetch all rows as plain tuples so comparisons do not depend on the Row type."""
    return [tuple(row) for row in cursor.fetchall()]


# --- named (pyformat) parameters -------------------------------------------


def test_named_dict_binds_values_in_order_with_types(cursor):
    cursor.execute("SELECT %(a)s AS a, %(b)s AS b", {"a": 1, "b": "x"})
    (a, b) = cursor.fetchone()
    assert (a, b) == (1, "x")
    assert type(a) is int
    assert type(b) is str


def test_named_placeholder_repeated_binds_same_value_twice(cursor):
    cursor.execute("SELECT %(a)s, %(a)s", {"a": 5})
    assert _rows(cursor) == [(5, 5)]



@pytest.mark.upstream_bug(
    reason=(
        "When a real %(a)s placeholder is also present, the pyformat-to-qmark rewrite replaces "
        "the %(a)s inside the string literal N'%(a)s' with '?', so the literal returns '?' "
        "instead of '%(a)s'. The rewrite must skip string literals, as the server does."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_named_placeholder_inside_string_literal_is_not_substituted(cursor):
    cursor.execute("SELECT N'%(a)s' AS x, %(a)s AS y", {"a": 3})
    assert _rows(cursor) == [("%(a)s", 3)]


def test_named_placeholder_inside_line_comment_is_ignored(cursor):
    cursor.execute("SELECT 1 -- %(a)s\n AS x", {"a": 2})
    assert _rows(cursor) == [(1,)]


def test_named_placeholder_inside_block_comment_is_ignored(cursor):
    cursor.execute("SELECT /* %(a)s */ %(b)s AS x", {"a": 2, "b": 9})
    assert _rows(cursor) == [(9,)]



def test_named_dict_none_and_bool_values(cursor):
    cursor.execute("SELECT %(a)s AS a, %(b)s AS b, %(c)s AS c", {"a": None, "b": True, "c": False})
    (a, b, c) = cursor.fetchone()
    assert a is None
    assert b is True
    assert c is False


def test_named_dict_extra_unused_keys_are_ignored(cursor):
    cursor.execute("SELECT %(a)s", {"a": 1, "b": 2, "c": 3})
    assert _rows(cursor) == [(1,)]


def test_named_dict_missing_key_raises_keyerror_naming_the_key(cursor):
    with pytest.raises(KeyError, match="zz"):
        cursor.execute("SELECT %(zz)s", {"a": 1})


def test_named_dict_key_lookup_is_case_sensitive(cursor):
    with pytest.raises(KeyError, match="A"):
        cursor.execute("SELECT %(A)s", {"a": 1})


def test_named_executemany_inserts_all_rows(cursor):
    cursor.execute("CREATE TABLE #pf_many (a INT NOT NULL, b NVARCHAR(10) NULL)")
    cursor.executemany(
        "INSERT INTO #pf_many (a, b) VALUES (%(a)s, %(b)s)",
        [{"a": 1, "b": "one"}, {"a": 2, "b": None}, {"a": 3, "b": "three"}],
    )
    cursor.execute("SELECT a, b FROM #pf_many ORDER BY a")
    assert _rows(cursor) == [(1, "one"), (2, None), (3, "three")]


# --- literal percent handling --------------------------------------------


def test_percent_without_params_is_passed_through_unchanged(cursor):
    cursor.execute("SELECT '100%' AS x")
    assert _rows(cursor) == [("100%",)]


def test_double_percent_is_unescaped_when_named_dict_given(cursor):
    cursor.execute("SELECT '100%%' AS x, %(a)s AS y", {"a": 1})
    assert _rows(cursor) == [("100%", 1)]


def test_double_percent_before_paren_yields_literal_placeholder_text(cursor):
    cursor.execute("SELECT '%%(a)s' AS x, %(a)s AS y", {"a": 1})
    assert _rows(cursor) == [("%(a)s", 1)]


def test_double_percent_stays_literal_under_qmark_tuple(cursor):
    # qmark style does no %-escaping: '%%' reaches the server verbatim.
    cursor.execute("SELECT '100%%' AS x, ? AS y", (1,))
    assert _rows(cursor) == [("100%%", 1)]



def test_percent_s_positional_placeholder_is_not_supported(cursor):
    # Only %(name)s and ? are placeholders; a bare %s is sent to SQL Server as-is.
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT %s", (1,))


# --- style mismatches -----------------------------------------------------


def test_tuple_passed_to_named_query_raises_typeerror(cursor):
    with pytest.raises(TypeError, match="Parameter style mismatch"):
        cursor.execute("SELECT %(a)s", (1,))


def test_named_query_without_params_raises_typeerror(cursor):
    with pytest.raises(TypeError, match="named placeholders"):
        cursor.execute("SELECT %(a)s", ())


def test_dict_passed_to_qmark_query_raises_typeerror(cursor):
    with pytest.raises(TypeError, match="Parameter style mismatch"):
        cursor.execute("SELECT ?", {"a": 1})


def test_mixed_named_and_qmark_in_one_statement_is_an_error(conn, cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT %(a)s, ?", {"a": 1})
    # The connection must remain usable after the failed statement.
    cursor.execute("SELECT 1 AS ok")
    assert _rows(cursor) == [(1,)]


@pytest.mark.upstream_bug(
    reason=(
        "Mixing %(name)s and ? in one statement fails with a server-side ProgrammingError "
        "('COUNT field incorrect'), while every other style mismatch raises the client-side "
        "TypeError 'Parameter style mismatch'. The correct behavior is the same client-side "
        "TypeError."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_mixed_styles_should_raise_client_side_typeerror(cursor):
    with pytest.raises(TypeError, match="Parameter style mismatch"):
        cursor.execute("SELECT %(a)s, ?", {"a": 1})


@pytest.mark.upstream_bug(
    reason=(
        "qmark with more values than markers is accepted silently: 'SELECT ?' with (1, 2) "
        "returns (1,). The correct behavior is ProgrammingError for a parameter-count "
        "mismatch (the fewer-values case already raises ProgrammingError)."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_qmark_extra_values_should_raise_programmingerror(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT ?", (1, 2))


def test_qmark_too_few_values_raises_programmingerror(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT ?, ?", (1,))


# --- LIKE patterns ----------------------------------------------------------


def test_like_wildcard_percent_in_named_param_matches(cursor):
    cursor.execute("SELECT CASE WHEN 'hello' LIKE %(p)s THEN 1 ELSE 0 END", {"p": "h%o"})
    assert _rows(cursor) == [(1,)]



def test_like_literal_underscore_via_bracket_escape(cursor):
    cursor.execute(
        "SELECT CASE WHEN 'a_c' LIKE %(p)s THEN 1 ELSE 0 END, CASE WHEN 'abc' LIKE %(p)s THEN 1 ELSE 0 END",
        {"p": "a[_]c"},
    )
    assert _rows(cursor) == [(1, 0)]


def test_like_literal_percent_via_explicit_escape_clause(conn, cursor):
    cursor.execute("CREATE TABLE #pf_like (v NVARCHAR(50) NOT NULL)")
    cursor.executemany("INSERT INTO #pf_like (v) VALUES (?)", [("a%b",), ("axb",), ("a!b",)])
    cursor.execute(
        "SELECT v FROM #pf_like WHERE v LIKE %(p)s ESCAPE '!' ORDER BY v",
        {"p": "a!%b"},
    )
    assert _rows(cursor) == [("a%b",)]
