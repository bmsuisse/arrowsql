"""Recursive CTEs, MAXRECURSION (error 530) and OPTION clauses for the native-TDS rewrite.

A TDS implementation sends the statement as-is, but several things around it
are driver responsibilities: how server errors map to Python exception classes,
how parameters are bound (qmark ``?`` becomes a typed sp_prepexec parameter, and
its declared length/precision decides whether a recursive CTE type-checks), how
partial result sets interact with a mid-stream error, and whether the batch
text that precedes a ``WITH`` keeps its terminator.

Research (issue trackers and docs, checked for this suite):
- https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql
  (recursive CTE rules: MAXRECURSION 0..32767, server default 100, 0 = no limit,
  one MAXRECURSION per statement, OPTION not allowed inside a CTE definition,
  UNION ALL only between anchor and recursive members, recursive columns are
  all nullable, a ';' is required before WITH when it follows another statement
  in a batch, and the anchor and recursive column types must match)
- https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query
  (query hints, MAXRECURSION in the OPTION clause of SELECT/INSERT/UPDATE/DELETE)
- https://codewithsql.com/errors/530 (error 530 text and the cycle-vs-depth diagnosis)
- https://sqlwithmanoj.com/2011/12/23/recursive-cte-maximum-recursion-100-has-been-exhausted/
  (the 530 message on a depth-101 hierarchy; SQL Server stops at 100 recursions)
- https://database.guide/using-sql-servers-maxrecursion-to-guard-against-infinite-loops-in-a-cte/
  (MAXRECURSION as a guard against cycles; partial results before the error)
- https://www.sqlservercentral.com/forums/topic/why-cte-should-begin-with-semicolon-or-go
  and https://sqlblog.org/2022/04/06/why-ctes-start-with-terminator
  (Msg 319 "Incorrect syntax near the keyword 'with'" when the previous statement is
  not terminated; the reason WITH needs a terminator)
- https://github.com/apache/superset/issues/38650 (pymssql: an OPTION hint breaks
  when the query is wrapped in a subquery; OPTION must sit on the outermost SELECT)
- https://github.com/microsoft/mssql-python/issues/228 (multi-statement batches with
  #temp tables behave differently across cursor.execute calls in mssql-python;
  batches with a CTE are a common way to hit this)
- https://github.com/mkleehammer/pyodbc/issues/588 (batch text split on ';' inside
  a statement; a reason to keep the batch text intact)
- https://github.com/Canner/WrenAI/pull/2566 (trailing ';' handling for MSSQL
  connectors; a lone trailing terminator is accepted by the ODBC driver)

Failure modes covered: exact row values and Python types from a recursive CTE,
the default-100 boundary (101 rows ok, 102 rows fail), the 530 error surfacing as
ProgrammingError with the server message, MAXRECURSION 0/N/out-of-range, a
placeholder inside OPTION (rejected by SQL Server), UNION (distinct) recursion
rejected, Msg 319 without and with the ';' terminator, OPTION on the outer
statement with several hints, OPTION inside a CTE rejected, INSERT ... WITH
rowcount, stored procedures, views, parameters in anchor and recursive members
(including the str/decimal length gotcha), NULL anchors, description nullability,
partial rows before a 530, cursor reuse after an error, and CTE names that shadow
a table.
"""

import uuid

import pytest
from mssql_python import ProgrammingError


@pytest.fixture
def ac_cursor(conn):
    conn.autocommit = True
    cur = conn.cursor()
    yield cur
    cur.close()


def _rows(rows):
    """mssql_python returns Row objects; compare them as plain tuples."""
    return [tuple(r) for r in rows]


def _count_sql(depth: int, option: str = "") -> str:
    """Recursive CTE producing ``depth + 1`` rows (anchor row plus ``depth`` recursions)."""
    return (
        "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < "
        f"{depth + 1}) SELECT n FROM r {option}"
    ).rstrip()


def test_recursive_cte_exact_rows_and_types(ac_cursor):
    ac_cursor.execute(
        "WITH r AS (SELECT 1 AS n, CAST(N'a' AS NVARCHAR(10)) AS s "
        "UNION ALL SELECT n + 1, CAST(s + N'b' AS NVARCHAR(10)) FROM r WHERE n < 4) "
        "SELECT n, s FROM r ORDER BY n"
    )
    rows = _rows(ac_cursor.fetchall())
    assert rows == [(1, "a"), (2, "ab"), (3, "abb"), (4, "abbb")]
    assert type(rows[0][0]) is int
    assert type(rows[0][1]) is str


def test_hierarchy_walk_with_level_and_path(ac_cursor):
    ac_cursor.execute("CREATE TABLE #emp (id INT PRIMARY KEY, mgr INT NULL, name NVARCHAR(20))")
    ac_cursor.execute(
        "INSERT INTO #emp VALUES (1, NULL, N'ceo'), (2, 1, N'vp'), "
        "(3, 2, N'lead'), (4, 2, N'sales'), (5, 3, N'dev')"
    )
    ac_cursor.execute(
        """
        WITH org AS (
            SELECT id, name, 0 AS lvl, CAST(name AS NVARCHAR(200)) AS path
            FROM #emp WHERE mgr IS NULL
            UNION ALL
            SELECT e.id, e.name, o.lvl + 1, CAST(o.path + N'/' + e.name AS NVARCHAR(200))
            FROM #emp AS e INNER JOIN org AS o ON e.mgr = o.id
        )
        SELECT id, lvl, path FROM org ORDER BY id
        """
    )
    assert _rows(ac_cursor.fetchall()) == [
        (1, 0, "ceo"),
        (2, 1, "ceo/vp"),
        (3, 2, "ceo/vp/lead"),
        (4, 2, "ceo/vp/sales"),
        (5, 3, "ceo/vp/lead/dev"),
    ]


def test_default_recursion_limit_allows_100_recursions(ac_cursor):
    ac_cursor.execute(_count_sql(100))
    rows = _rows(ac_cursor.fetchall())
    assert len(rows) == 101
    assert rows[-1] == (101,)


def test_default_recursion_limit_101_recursions_raises_530(ac_cursor):
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(_count_sql(101))
        _rows(ac_cursor.fetchall())
    assert "maximum recursion 100 has been exhausted" in str(exc.value)


def test_maxrecursion_option_raises_the_limit(ac_cursor):
    ac_cursor.execute(_count_sql(150, "OPTION (MAXRECURSION 150)"))
    rows = _rows(ac_cursor.fetchall())
    assert len(rows) == 151
    assert rows[-1] == (151,)


def test_maxrecursion_zero_means_no_limit(ac_cursor):
    ac_cursor.execute(_count_sql(300, "OPTION (MAXRECURSION 0)"))
    rows = _rows(ac_cursor.fetchall())
    assert len(rows) == 301
    assert rows[-1] == (301,)


def test_maxrecursion_stops_infinite_cycle_with_530(ac_cursor):
    # Cycle: the recursive member never stops producing rows.
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(
            "WITH r AS (SELECT 1 AS n UNION ALL SELECT n FROM r) "
            "SELECT n FROM r OPTION (MAXRECURSION 5)"
        )
        _rows(ac_cursor.fetchall())
    assert "maximum recursion 5 has been exhausted" in str(exc.value)


def test_rows_before_530_are_delivered_then_error_on_fetch(ac_cursor):
    ac_cursor.execute(
        "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r) "
        "SELECT n FROM r OPTION (MAXRECURSION 5)"
    )
    assert _rows(ac_cursor.fetchmany(3)) == [(1,), (2,), (3,)]
    with pytest.raises(ProgrammingError) as exc:
        _rows(ac_cursor.fetchall())
    assert "maximum recursion 5" in str(exc.value)


def test_cursor_usable_after_530(ac_cursor):
    with pytest.raises(ProgrammingError):
        ac_cursor.execute(
            "WITH r AS (SELECT 1 AS n UNION ALL SELECT n FROM r) "
            "SELECT n FROM r OPTION (MAXRECURSION 3)"
        )
        _rows(ac_cursor.fetchall())
    ac_cursor.execute("SELECT 42")
    assert (tuple(ac_cursor.fetchone())) == (42,)


def test_maxrecursion_placeholder_is_rejected(ac_cursor):
    # SQL Server requires a literal in MAXRECURSION; a bound parameter is a syntax error.
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(_count_sql(5, "OPTION (MAXRECURSION ?)"), [10])
    assert "Incorrect syntax" in str(exc.value)


def test_maxrecursion_above_32767_is_rejected(ac_cursor):
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(_count_sql(1, "OPTION (MAXRECURSION 32768)"))
    assert "32767" in str(exc.value)


def test_union_distinct_recursion_is_rejected(ac_cursor):
    # UNION (distinct) cannot be used between anchor and recursive member.
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute("WITH r AS (SELECT 1 AS n UNION SELECT n FROM r) SELECT n FROM r")
    assert "top-level UNION ALL" in str(exc.value)


def test_with_after_statement_without_semicolon_is_msg_319(ac_cursor):
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute("SET NOCOUNT ON WITH r AS (SELECT 1 AS n) SELECT n FROM r")
    assert "Incorrect syntax near the keyword 'with'" in str(exc.value)


def test_with_after_semicolon_in_batch_works(ac_cursor):
    ac_cursor.execute("SET NOCOUNT ON; WITH r AS (SELECT 1 AS n) SELECT n FROM r")
    assert _rows(ac_cursor.fetchall()) == [(1,)]


def test_batch_temp_table_then_semicolon_cte_with_option(ac_cursor):
    ac_cursor.execute(
        "CREATE TABLE #x (a INT); INSERT INTO #x VALUES (1); "
        ";WITH r AS (SELECT a FROM #x UNION ALL SELECT a + 1 FROM r WHERE a < 3) "
        "SELECT a FROM r OPTION (MAXRECURSION 10)"
    )
    # The CREATE and INSERT produce no result set; advance to the SELECT's result set.
    while ac_cursor.description is None and ac_cursor.nextset():
        pass
    assert ac_cursor.description is not None
    assert _rows(ac_cursor.fetchall()) == [(1,), (2,), (3,)]


def test_insert_from_recursive_cte_rowcount(ac_cursor):
    ac_cursor.execute("CREATE TABLE #t2 (n INT)")
    ac_cursor.execute(
        ";WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 7) "
        "INSERT INTO #t2 SELECT n FROM r OPTION (MAXRECURSION 10)"
    )
    assert ac_cursor.rowcount == 7
    ac_cursor.execute("SELECT SUM(n), COUNT(*) FROM #t2")
    assert (tuple(ac_cursor.fetchone())) == (28, 7)


def test_option_inside_cte_definition_is_rejected(ac_cursor):
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(
            "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3 "
            "OPTION (MAXRECURSION 3)) SELECT n FROM r"
        )
    assert "'OPTION'" in str(exc.value)


def test_qmark_params_in_anchor_and_recursive_member(ac_cursor):
    ac_cursor.execute(
        "WITH r AS (SELECT CAST(? AS INT) AS n UNION ALL SELECT n + 1 FROM r WHERE n < ?) "
        "SELECT n FROM r OPTION (MAXRECURSION 20)",
        [1, 4],
    )
    rows = _rows(ac_cursor.fetchall())
    assert rows == [(1,), (2,), (3,), (4,)]
    assert all(type(r[0]) is int for r in rows)


def test_unsized_str_param_in_anchor_hits_type_mismatch(ac_cursor):
    # The anchor column takes the parameter's declared length (here 1 char), so the
    # recursive member's longer result type does not match. A native implementation that
    # binds str params with a different length changes whether this query succeeds.
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(
            "WITH r AS (SELECT ? AS s UNION ALL SELECT s + N'x' FROM r WHERE LEN(s) < 3) "
            "SELECT s FROM r OPTION (MAXRECURSION 20)",
            ["a"],
        )
        _rows(ac_cursor.fetchall())
    assert "Types don't match between the anchor and the recursive part" in str(exc.value)


def test_sized_str_param_cast_in_anchor_works(ac_cursor):
    ac_cursor.execute(
        "WITH r AS (SELECT CAST(? AS NVARCHAR(10)) AS s UNION ALL "
        "SELECT CAST(s + N'x' AS NVARCHAR(10)) FROM r WHERE LEN(s) < 3) "
        "SELECT s FROM r OPTION (MAXRECURSION 20)",
        ["a"],
    )
    assert _rows(ac_cursor.fetchall()) == [("a",), ("ax",), ("axx",)]


def test_decimal_growth_needs_cast_in_recursive_member(ac_cursor):
    # amt * 2 widens DECIMAL(10,2) to DECIMAL(11,2); the anchor type must match exactly.
    with pytest.raises(ProgrammingError) as exc:
        ac_cursor.execute(
            "WITH r AS (SELECT CAST(? AS DECIMAL(10,2)) AS amt UNION ALL "
            "SELECT amt * 2 FROM r WHERE amt < 100) SELECT amt FROM r OPTION (MAXRECURSION 20)",
            [1.25],
        )
        _rows(ac_cursor.fetchall())
    assert "Types don't match" in str(exc.value)


def test_decimal_recursion_with_cast_returns_exact_decimals(ac_cursor):
    from decimal import Decimal

    ac_cursor.execute(
        "WITH r AS (SELECT CAST(? AS DECIMAL(10,2)) AS amt UNION ALL "
        "SELECT CAST(amt * 2 AS DECIMAL(10,2)) FROM r WHERE amt < 100) "
        "SELECT amt FROM r OPTION (MAXRECURSION 20)",
        [1.25],
    )
    assert _rows(ac_cursor.fetchall()) == [
        (Decimal("1.25"),),
        (Decimal("2.50"),),
        (Decimal("5.00"),),
        (Decimal("10.00"),),
        (Decimal("20.00"),),
        (Decimal("40.00"),),
        (Decimal("80.00"),),
        (Decimal("160.00"),),
    ]


def test_recursive_cte_description_marks_columns_nullable(ac_cursor):
    ac_cursor.execute(
        "WITH r AS (SELECT CAST(1 AS BIGINT) AS n, CAST(N'a' AS NVARCHAR(5)) AS s "
        "UNION ALL SELECT n + 1, s FROM r WHERE n < 3) SELECT n, s FROM r OPTION (MAXRECURSION 10)"
    )
    desc = ac_cursor.description
    assert [d[0] for d in desc] == ["n", "s"]
    # Documented: every column of a recursive CTE is nullable, even when the anchor is NOT NULL.
    assert desc[0][6] is True
    assert desc[1][6] is True
    assert _rows(ac_cursor.fetchall()) == [(1, "a"), (2, "a"), (3, "a")]


def test_stored_procedure_with_recursive_cte_and_option(ac_cursor):
    name = f"ci_cte_proc_{uuid.uuid4().hex[:10]}"
    ac_cursor.execute(
        f"CREATE PROCEDURE dbo.{name} @n INT AS BEGIN "
        "WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < @n) "
        "SELECT COUNT(*) AS c FROM r OPTION (MAXRECURSION 1000) END"
    )
    try:
        ac_cursor.execute(f"EXEC dbo.{name} ?", [250])
        assert _rows(ac_cursor.fetchall()) == [(250,)]
    finally:
        ac_cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{name}")


def test_cte_name_shadows_permanent_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table_name} (n INT)")
    cur.execute(f"INSERT INTO dbo.{table_name} VALUES (99)")
    conn.commit()
    cur.execute(
        f"WITH {table_name} AS (SELECT 1 AS n UNION ALL "
        f"SELECT n + 1 FROM {table_name} WHERE n < 2) SELECT n FROM {table_name}"
    )
    assert _rows(cur.fetchall()) == [(1,), (2,)]
    cur.close()
