"""NULL semantics as seen through the driver: three-valued logic, session-level
NULL options, parameter binding of None, NULL-aware set operators, the
type-precedence difference between ISNULL and COALESCE, NULLIF, and aggregate
NULL elimination with warning 8153.

Failure modes a native-TDS reimplementation can easily get wrong:

* ``None`` must bind as a typed SQL NULL, so ``WHERE col = ?`` with None
  matches nothing and ``WHERE col IS ?`` is a syntax error (the driver must
  not rewrite the SQL).
* SET ANSI_NULLS / CONCAT_NULL_YIELDS_NULL are session state. Changing them
  must reach the server and be undone on the same connection.
* Warning 8153 ("Null value is eliminated by an aggregate or other SET
  operation") is an informational message (SQLSTATE 01003), not an error. It
  must not raise, and it must be available to the caller through
  ``cursor.messages`` when ANSI_WARNINGS is ON and absent when it is OFF.
* ISNULL takes its type from the first argument (truncating or overflowing),
  while COALESCE uses data type precedence. A driver must not "fix" this
  client-side.
* NULL in a NOT IN list or subquery makes the predicate UNKNOWN, so no rows
  come back.
* NULLIF and equality under ANSI padding treat '' and ' ' as equal.

Research sources:

* https://github.com/microsoft/mssql-python/issues/273 (NULL binding into
  nullable columns; the only mssql-python NULL-binding issue found)
* https://github.com/TimelordUK/node-sqlserver-v8/pull/494 (warnings such as
  8153 attached to the final fetch were dropped by a driver built on msodbcsql;
  the same gap could affect a native rewrite that does not read diagnostics at
  end-of-rowset)
* https://github.com/mirek/msduck/pull/98 (SQL Server programs covering
  NULL-elimination semantics across aggregates, with ANSI_WARNINGS ON/OFF)
* https://github.com/mkleehammer/pyodbc/wiki/Binding-Parameters (None binding
  and type-compatibility checks)
* https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/null-handling
  (NULL maps to None, use IS NULL in SQL)
* https://docs.microsoft.com/en-us/sql/t-sql/statements/set-ansi-warnings-transact-sql
  (ANSI_WARNINGS and its effect on aggregate warnings)
* https://www.sqlservercentral.com/forums/topic/null-value-is-eliminated-by-an-aggregate-or-other-set-operation
  (8153 causes: OUTER JOIN NULLs even when columns are NOT NULL)
* https://blog.sqlauthority.com/2015/02/13/sql-server-warning-null-value-is-eliminated-by-an-aggregate-or-other-set-operation/
  (8153 is a warning, SUM still returns the non-NULL total)
* https://michaeljswart.com/tag/null-value-is-eliminated-by-an-aggregate-or-other-set-operation/
  (COUNT(col) vs COUNT(*) under NULLs)

Expected values were checked against the upstream ODBC driver (.venv-upstream)
before being written as assertions.
"""

import pytest
from mssql_python.exceptions import DataError, ProgrammingError

NULL_ELIMINATED = "Null value is eliminated by an aggregate"


def _rows(cursor):
    return [tuple(row) for row in cursor.fetchall()]


def _row(cursor):
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _warning_texts(cursor):
    return [str(m[1]) for m in (cursor.messages or [])]


def _has_8153(cursor):
    return any("8153" in str(m[0]) and NULL_ELIMINATED in str(m[1]) for m in (cursor.messages or []))


@pytest.fixture
def null_table(cursor):
    cursor.execute("CREATE TABLE #nulls (id INT NOT NULL, v INT NULL)")
    cursor.execute("INSERT INTO #nulls (id, v) VALUES (1, NULL), (2, 5), (3, NULL)")
    cursor.connection.commit()
    yield cursor


# ---------------------------------------------------------------------------
# Three-valued logic: = vs IS NULL
# ---------------------------------------------------------------------------


def test_null_equals_null_is_unknown_not_true(cursor):
    cursor.execute("SELECT CASE WHEN NULL = NULL THEN 1 ELSE 0 END")
    assert _row(cursor) == (0,)


def test_is_null_selects_exactly_the_null_rows(null_table):
    null_table.execute("SELECT id FROM #nulls WHERE v IS NULL ORDER BY id")
    assert _rows(null_table) == [(1,), (3,)]




def test_ansi_nulls_off_makes_null_equal_null_for_this_session(conn, cursor):
    cursor.execute("SET ANSI_NULLS OFF")
    try:
        cursor.execute("SELECT CASE WHEN NULL = NULL THEN 1 ELSE 0 END")
        assert _row(cursor) == (1,)
        cursor.execute("SELECT CAST(SESSIONPROPERTY('ANSI_NULLS') AS INT)")
        assert _row(cursor) == (0,)
    finally:
        cursor.execute("SET ANSI_NULLS ON")
        conn.commit()
    cursor.execute("SELECT CASE WHEN NULL = NULL THEN 1 ELSE 0 END")
    assert _row(cursor) == (0,)


# ---------------------------------------------------------------------------
# None as a bound parameter
# ---------------------------------------------------------------------------


def test_none_parameter_with_equality_matches_no_rows(null_table):
    null_table.execute("SELECT id FROM #nulls WHERE v = ?", (None,))
    assert _rows(null_table) == []


def test_none_parameter_with_is_null_or_pattern_matches_nulls(null_table):
    null_table.execute(
        "SELECT id FROM #nulls WHERE (? IS NULL AND v IS NULL) OR v = ? ORDER BY id",
        (None, 5),
    )
    assert _rows(null_table) == [(1,), (2,), (3,)]


def test_is_placeholder_is_a_syntax_error_not_a_null_test(null_table):
    with pytest.raises(ProgrammingError):
        null_table.execute("SELECT id FROM #nulls WHERE v IS ?", (None,))


def test_none_parameter_matches_nulls_when_ansi_nulls_off(conn, null_table):
    null_table.execute("SET ANSI_NULLS OFF")
    try:
        null_table.execute("SELECT id FROM #nulls WHERE v = ? ORDER BY id", (None,))
        assert _rows(null_table) == [(1,), (3,)]
    finally:
        null_table.execute("SET ANSI_NULLS ON")
        conn.commit()


# ---------------------------------------------------------------------------
# String concatenation with NULL
# ---------------------------------------------------------------------------


def test_plus_with_null_yields_null_by_default(cursor):
    cursor.execute("SELECT 'a' + NULL AS r")
    assert _row(cursor) == (None,)


def test_concat_function_skips_null_and_returns_str(cursor):
    cursor.execute("SELECT CONCAT('a', NULL, 'b') AS r")
    row = _row(cursor)
    assert row == ("ab",)
    assert isinstance(row[0], str)


def test_concat_null_yields_null_off_makes_plus_treat_null_as_empty(conn, cursor):
    cursor.execute("SET CONCAT_NULL_YIELDS_NULL OFF")
    try:
        cursor.execute("SELECT 'a' + NULL AS r")
        assert _row(cursor) == ("a",)
    finally:
        cursor.execute("SET CONCAT_NULL_YIELDS_NULL ON")
        conn.commit()


# ---------------------------------------------------------------------------
# NOT IN with NULLs
# ---------------------------------------------------------------------------


def test_not_in_subquery_containing_null_returns_no_rows(null_table):
    null_table.execute("SELECT id FROM #nulls WHERE 7 NOT IN (SELECT v FROM #nulls) ORDER BY id")
    assert _rows(null_table) == []


def test_not_in_subquery_filtered_for_null_returns_rows(null_table):
    null_table.execute(
        "SELECT id FROM #nulls WHERE 7 NOT IN (SELECT v FROM #nulls WHERE v IS NOT NULL) ORDER BY id"
    )
    assert _rows(null_table) == [(1,), (2,), (3,)]


def test_not_exists_is_null_safe_where_not_in_is_not(null_table):
    null_table.execute(
        "SELECT p.id FROM #nulls p WHERE NOT EXISTS (SELECT 1 FROM #nulls q WHERE q.v = 7) ORDER BY p.id"
    )
    assert _rows(null_table) == [(1,), (2,), (3,)]


def test_not_in_literal_list_with_null_is_false(cursor):
    cursor.execute("SELECT CASE WHEN 3 NOT IN (1, NULL) THEN 1 ELSE 0 END AS r")
    assert _row(cursor) == (0,)


def test_null_on_left_of_in_list_is_neither_true_nor_false(cursor):
    cursor.execute(
        "SELECT CASE WHEN NULL NOT IN (1, 2) THEN 'not' "
        "WHEN NULL IN (1, 2) THEN 'in' ELSE 'unknown' END AS r"
    )
    assert _row(cursor) == ("unknown",)


# ---------------------------------------------------------------------------
# ISNULL vs COALESCE: result type
# ---------------------------------------------------------------------------


def test_isnull_takes_type_of_first_argument_and_truncates(cursor):
    cursor.execute("SELECT ISNULL(CAST(NULL AS VARCHAR(3)), 'abcdef') AS r")
    assert _row(cursor) == ("abc",)


def test_coalesce_uses_type_precedence_and_keeps_full_value(cursor):
    cursor.execute("SELECT COALESCE(CAST(NULL AS VARCHAR(3)), 'abcdef') AS r")
    assert _row(cursor) == ("abcdef",)


def test_isnull_tinyint_with_wide_default_raises_data_error(cursor):
    with pytest.raises(DataError):
        cursor.execute("SELECT ISNULL(CAST(NULL AS TINYINT), 300) AS r")
        cursor.fetchall()


def test_coalesce_tinyint_with_wide_default_promotes_to_int(cursor):
    cursor.execute("SELECT COALESCE(CAST(NULL AS TINYINT), 300) AS r")
    row = _row(cursor)
    assert row == (300,)
    assert type(row[0]) is int



# ---------------------------------------------------------------------------
# NULLIF
# ---------------------------------------------------------------------------


def test_nullif_equal_values_return_none_and_distinct_return_first(cursor):
    cursor.execute("SELECT NULLIF(1, 1) AS a, NULLIF(2, 1) AS b")
    row = _row(cursor)
    assert row == (None, 2)
    assert type(row[1]) is int


def test_nullif_trailing_spaces_compare_equal_under_ansi_padding(cursor):
    cursor.execute("SELECT NULLIF('', ' ') AS r")
    assert _row(cursor) == (None,)


# ---------------------------------------------------------------------------
# Aggregates and warning 8153
# ---------------------------------------------------------------------------


def test_aggregates_ignore_null_values(null_table):
    null_table.execute("SELECT SUM(v) AS s, COUNT(v) AS c, COUNT(*) AS n FROM #nulls")
    row = _row(null_table)
    assert row == (5, 1, 3)
    assert all(type(x) is int for x in row)


def test_all_null_aggregates_return_none(cursor):
    cursor.execute("SELECT MIN(v) AS m, SUM(v) AS s, AVG(v) AS a FROM (VALUES (CAST(NULL AS INT))) t(v)")
    assert _row(cursor) == (None, None, None)


def test_warning_8153_surfaces_in_messages_without_raising(null_table):
    # The warning is attached to the end-of-rowset fetch, so the rowset must be
    # drained (fetchall) before cursor.messages is expected to carry it.
    null_table.execute("SET ANSI_WARNINGS ON")
    null_table.execute("SELECT SUM(v) AS s FROM #nulls")
    assert _rows(null_table) == [(5,)]
    assert _has_8153(null_table)


def test_warning_8153_absent_when_ansi_warnings_off(conn, null_table):
    null_table.execute("SET ANSI_WARNINGS OFF")
    try:
        null_table.execute("SELECT SUM(v) AS s FROM #nulls")
        assert _rows(null_table) == [(5,)]
        assert not _has_8153(null_table)
        assert not any(NULL_ELIMINATED in t for t in _warning_texts(null_table))
    finally:
        null_table.execute("SET ANSI_WARNINGS ON")
        conn.commit()
