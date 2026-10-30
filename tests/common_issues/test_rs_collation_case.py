"""Collations: case/accent sensitivity, COLLATE clauses, error 468, Turkish I, binary order.

Failure modes a native-TDS reimplementation can easily get wrong. The driver
itself does not interpret collations; SQL Server does. What the driver must get
right is: binding parameters as text the server can compare with the column's
collation, returning the exact stored text (case, accents, trailing spaces),
surfacing collation errors as the right exception class without poisoning the
connection, and not normalising strings client-side (for example with
Python's ``str.lower()``, which disagrees with Turkish or binary rules).

Topics covered:

- CI vs CS equality, grouping and DISTINCT, and UNIQUE constraints
- AS vs AI accent sensitivity
- COLLATE clauses on a column or literal in WHERE, and parameters on either side
- Collation conflict error 468 (ProgrammingError) and recovery after it
- Column collation from tempdb vs DATABASE_DEFAULT for temp and permanent tables
- Turkish dotless i / dotted capital I equality and ordering
- Binary (BIN2) ordering and equality, and linguistic expansion (sharp s = ss)
- Trailing-space padding in equality under binary collations

Sources (research):
- https://github.com/microsoft/mssql-python/issues (a search for collation issues found no collation-specific driver bug, so the cases below are checked against SQL Server behaviour)
- https://github.com/mkleehammer/pyodbc/issues/835 (parameterised text and a collation-related error; only loosely related to this topic)
- https://www.sqlservercentral.com/articles/collation-error-468 (Msg 468 causes and COLLATE fixes)
- https://www.sqlservercentral.com/forums/topic/collation-error-468 (Msg 468 with temp tables vs permanent tables in CS databases)
- https://sqlserverperformace.blogspot.com/2020/02/error-message-468-cannot-resolve.html (exact Msg 468 text and COLLATE placement)
- https://sqldba.blog/sql-server-collation-case-sensitivity-and-the-cross-database-trap/ (tempdb collation vs user database collation)
- https://www.sqlshack.com/sql-server-collation-introduction-with-collate-sql-casting/ (COLLATE precedence and explicit collation on expressions)
- https://userapps.support.sap.com/sap/support/knowledge/en/2542537 (Turkish collation conflict through ODBC)
- https://infocenter.sybase.com/help/topic/com.sybase.help.sqlanywhere.12.0.0/dbadmin/natlang-s-4263196.html (Turkish I/i case mapping is not ANSI-compatible)
- https://learn.microsoft.com/en-us/sql/t-sql/statements/windows-collation-name-transact-sql (naming of Turkish_CI_AS and Latin1_General_BIN2)
- https://learn.microsoft.com/en-us/SQL/connect/python/mssql-python/string-unicode (text parameters are bound as Unicode)

Expected values were verified against the upstream ODBC driver. Where upstream
returns something wrong, the test asserts the correct value and is marked
``upstream_bug`` + ``xfail(strict=True)``.
"""

import pytest
from mssql_python import IntegrityError, ProgrammingError

CI_AS = "Latin1_General_CI_AS"
CI_AI = "Latin1_General_CI_AI"
CS_AS = "Latin1_General_CS_AS"
BIN2 = "Latin1_General_BIN2"
TURKISH_CI_AS = "Turkish_CI_AS"


def _rows(cursor, sql, *params):
    cursor.execute(sql, *params)
    return [tuple(row) for row in cursor.fetchall()]


def _scalar(cursor, sql, *params):
    cursor.execute(sql, *params)
    row = cursor.fetchone()
    return row[0]


def _make_temp(cursor, name, column_sql):
    cursor.execute(f"CREATE TABLE {name} ({column_sql})")
    cursor.connection.commit()


def test_ci_column_matches_any_case_for_parameter(cursor):
    _make_temp(cursor, "#ci", f"v VARCHAR(20) COLLATE {CI_AS}")
    cursor.execute("INSERT INTO #ci VALUES ('Alpha'), ('alpha'), ('beta')")
    cursor.connection.commit()

    rows = _rows(cursor, "SELECT v FROM #ci WHERE v = ? ORDER BY v COLLATE " + BIN2, "ALPHA")

    assert rows == [("Alpha",), ("alpha",)]
    assert all(isinstance(r[0], str) for r in rows)


def test_cs_column_matches_only_exact_case_for_parameter(cursor):
    _make_temp(cursor, "#cs", f"v VARCHAR(20) COLLATE {CS_AS}")
    cursor.execute("INSERT INTO #cs VALUES ('Alpha'), ('alpha'), ('ALPHA')")
    cursor.connection.commit()

    rows = _rows(cursor, "SELECT v FROM #cs WHERE v = ?", "alpha")

    assert rows == [("alpha",)]


def test_collate_clause_in_where_overrides_column_collation(cursor):
    _make_temp(cursor, "#ov", f"v VARCHAR(20) COLLATE {CS_AS}")
    cursor.execute("INSERT INTO #ov VALUES ('Alpha'), ('alpha')")
    cursor.connection.commit()

    cs_rows = _rows(cursor, "SELECT v FROM #ov WHERE v = ?", "ALPHA")
    ci_rows = _rows(cursor, f"SELECT v FROM #ov WHERE v COLLATE {CI_AS} = ?", "ALPHA")

    assert cs_rows == []
    assert sorted(ci_rows) == [("Alpha",), ("alpha",)]


def test_parameter_on_left_side_of_comparison_uses_column_collation(cursor):
    _make_temp(cursor, "#lhs", f"v VARCHAR(20) COLLATE {CS_AS}")
    cursor.execute("INSERT INTO #lhs VALUES ('Alpha'), ('alpha')")
    cursor.connection.commit()

    rows = _rows(cursor, "SELECT v FROM #lhs WHERE ? = v", "Alpha")

    assert rows == [("Alpha",)]


def test_ci_as_treats_accented_letter_as_different(cursor):
    # AS = accent sensitive: e and e-acute are different characters.
    value = _scalar(cursor, f"SELECT CASE WHEN ? COLLATE {CI_AS} = N'resume' THEN 1 ELSE 0 END", "résumé")

    assert value == 0
    assert type(value) is int


def test_ci_ai_treats_accented_letter_as_equal(cursor):
    # AI = accent insensitive, case insensitive.
    value = _scalar(cursor, f"SELECT CASE WHEN ? COLLATE {CI_AI} = N'RESUME' THEN 1 ELSE 0 END", "résumé")

    assert value == 1
    assert type(value) is int


def test_group_by_and_count_distinct_follow_collation(cursor):
    _make_temp(cursor, "#grp", f"v VARCHAR(10) COLLATE {CI_AS}")
    cursor.execute("INSERT INTO #grp VALUES ('a'), ('A'), ('b')")
    cursor.connection.commit()
    ci_distinct = _scalar(cursor, "SELECT COUNT(DISTINCT v) FROM #grp")

    _make_temp(cursor, "#grp_cs", f"v VARCHAR(10) COLLATE {CS_AS}")
    cursor.execute("INSERT INTO #grp_cs VALUES ('a'), ('A'), ('b')")
    cursor.connection.commit()
    cs_distinct = _scalar(cursor, "SELECT COUNT(DISTINCT v) FROM #grp_cs")

    assert ci_distinct == 2
    assert cs_distinct == 3


def test_unique_constraint_uses_column_collation(cursor):
    _make_temp(cursor, "#uq_ci", f"v VARCHAR(10) COLLATE {CI_AS} UNIQUE")
    cursor.execute("INSERT INTO #uq_ci VALUES ('x')")
    cursor.connection.commit()

    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #uq_ci VALUES (?)", "X")
    cursor.connection.rollback()

    assert _rows(cursor, "SELECT v FROM #uq_ci") == [("x",)]

    _make_temp(cursor, "#uq_cs", f"v VARCHAR(10) COLLATE {CS_AS} UNIQUE")
    cursor.execute("INSERT INTO #uq_cs VALUES ('x'), ('X')")
    cursor.connection.commit()
    assert _scalar(cursor, "SELECT COUNT(*) FROM #uq_cs") == 2


def test_collation_conflict_is_programming_error_and_connection_recovers(cursor):
    _make_temp(cursor, "#conf_a", f"v VARCHAR(10) COLLATE {CS_AS}")
    _make_temp(cursor, "#conf_b", f"v VARCHAR(10) COLLATE {CI_AS}")

    with pytest.raises(ProgrammingError) as exc_info:
        cursor.execute("SELECT a.v FROM #conf_a a JOIN #conf_b b ON a.v = b.v")
    cursor.connection.rollback()

    assert "collation conflict" in str(exc_info.value).lower()
    assert _scalar(cursor, "SELECT 42") == 42


def test_explicit_collate_resolves_collation_conflict_in_join(cursor):
    _make_temp(cursor, "#join_a", f"v VARCHAR(10) COLLATE {CS_AS}")
    _make_temp(cursor, "#join_b", f"v VARCHAR(10) COLLATE {CI_AS}")
    cursor.execute("INSERT INTO #join_a VALUES ('Alpha'), ('beta')")
    cursor.execute("INSERT INTO #join_b VALUES ('alpha'), ('BETA'), ('gamma')")
    cursor.connection.commit()

    rows = _rows(
        cursor,
        f"SELECT a.v, b.v FROM #join_a a JOIN #join_b b ON a.v COLLATE {CI_AS} = b.v "
        "ORDER BY a.v COLLATE " + BIN2,
    )

    assert rows == [("Alpha", "alpha"), ("beta", "BETA")]


def test_temp_column_without_collate_takes_tempdb_collation(cursor):
    _make_temp(cursor, "#dflt", "v VARCHAR(10)")

    column_collation = _scalar(
        cursor,
        "SELECT collation_name FROM tempdb.sys.columns WHERE object_id = OBJECT_ID('tempdb..#dflt')",
    )
    tempdb_collation = _scalar(cursor, "SELECT CONVERT(VARCHAR(128), DATABASEPROPERTYEX('tempdb', 'Collation'))")

    assert column_collation == tempdb_collation
    assert isinstance(column_collation, str)


def test_database_default_collate_on_permanent_table_matches_database(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v VARCHAR(10) COLLATE DATABASE_DEFAULT)")
    cursor.connection.commit()

    column_collation = _scalar(
        cursor,
        "SELECT c.collation_name FROM sys.columns c "
        f"WHERE c.object_id = OBJECT_ID('dbo.{table_name}') AND c.name = 'v'",
    )
    db_collation = _scalar(cursor, "SELECT CONVERT(VARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))")

    assert column_collation == db_collation


def test_turkish_ci_equates_dotless_i_with_capital_i(cursor):
    # Under Turkish rules, dotless i (U+0131) is the lowercase of I.
    turkish = _scalar(cursor, f"SELECT CASE WHEN ? COLLATE {TURKISH_CI_AS} = N'I' THEN 1 ELSE 0 END", "ı")
    latin = _scalar(cursor, f"SELECT CASE WHEN ? COLLATE {CI_AS} = N'I' THEN 1 ELSE 0 END", "ı")

    assert turkish == 1
    assert latin == 0


def test_turkish_ci_equates_i_with_dotted_capital_i(cursor):
    turkish = _scalar(cursor, f"SELECT CASE WHEN ? COLLATE {TURKISH_CI_AS} = N'İ' THEN 1 ELSE 0 END", "i")
    latin = _scalar(cursor, f"SELECT CASE WHEN ? COLLATE {CI_AS} = N'İ' THEN 1 ELSE 0 END", "i")

    assert turkish == 1
    assert latin == 0


def test_turkish_and_latin_order_i_variants_differently(cursor):
    values = "(N'ı'), (N'I'), (N'i'), (N'İ')"
    turkish = _rows(
        cursor,
        f"SELECT v FROM (VALUES {values}) t(v) ORDER BY v COLLATE {TURKISH_CI_AS}, v COLLATE {BIN2}",
    )
    latin = _rows(
        cursor,
        f"SELECT v FROM (VALUES {values}) t(v) ORDER BY v COLLATE {CI_AS}, v COLLATE {BIN2}",
    )

    assert [r[0] for r in turkish] == ["I", "ı", "i", "İ"]
    assert [r[0] for r in latin] == ["I", "i", "ı", "İ"]


def test_binary_collation_orders_by_code_point(cursor):
    rows = _rows(
        cursor,
        "SELECT v FROM (VALUES ('b'), ('A'), ('a'), ('B'), ('Z')) t(v) ORDER BY v COLLATE " + BIN2,
    )

    assert [r[0] for r in rows] == ["A", "B", "Z", "a", "b"]


def test_ci_collation_orders_case_insensitively(cursor):
    rows = _rows(
        cursor,
        f"SELECT v FROM (VALUES ('b'), ('A'), ('a'), ('B'), ('Z')) t(v) "
        f"ORDER BY v COLLATE {CI_AS}, v COLLATE {BIN2}",
    )

    assert [r[0] for r in rows] == ["A", "a", "B", "b", "Z"]


def test_binary_collation_equality_is_case_and_accent_sensitive(cursor):
    _make_temp(cursor, "#bin", f"v NVARCHAR(20) COLLATE {BIN2}")
    cursor.execute("INSERT INTO #bin VALUES (N'Résumé'), (N'résumé'), (N'RESUME'), (N'resume')")
    cursor.connection.commit()

    rows = _rows(cursor, "SELECT v FROM #bin WHERE v = ?", "résumé")

    assert rows == [("résumé",)]


def test_trailing_spaces_are_padded_for_equality_under_binary_collation(cursor):
    # Padding applies to = under every collation, including binary ones.
    _make_temp(cursor, "#pad", f"v VARCHAR(10) COLLATE {BIN2}")
    cursor.execute("INSERT INTO #pad VALUES ('abc ')")
    cursor.connection.commit()

    rows = _rows(cursor, "SELECT v, DATALENGTH(v) FROM #pad WHERE v = ?", "abc")

    assert rows == [("abc ", 4)]


def test_sharp_s_equals_ss_under_ci_as_linguistic_expansion(cursor):
    assert _scalar(cursor, f"SELECT CASE WHEN N'ß' COLLATE {CI_AS} = N'ss' THEN 1 ELSE 0 END") == 1
    assert _scalar(cursor, f"SELECT CASE WHEN N'ß' COLLATE {BIN2} = N'ss' THEN 1 ELSE 0 END") == 0
