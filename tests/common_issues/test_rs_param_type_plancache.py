"""Parameter typing and plan cache: declared string parameter types, implicit
conversion on indexed columns, plan cache pollution, and setinputsizes.

What a native-TDS reimplementation can easily get wrong:

- The declared TDS parameter type of a Python ``str`` is observable in the
  statement header that SQL Server caches (``(@P1 varchar(3))SELECT ...``).
  ASCII text must go out as varchar, non-ASCII as nvarchar, and the declared
  length must be exact up to 4000 units and ``max`` above that. Sizing the
  parameter by Python ``len()`` for non-ASCII text is wrong: UTF-16 code units
  differ from code points (an emoji is 1 Python char but 2 nvarchar units).
- Declaring ``nvarchar`` for text compared against a ``varchar`` indexed column
  makes SQL Server convert the *column* (CONVERT_IMPLICIT on the column side),
  which turns index seeks into scans. ASCII must not trigger this.
- Per-length declared sizes create one cached plan per distinct length (plan
  cache pollution). ``setinputsizes`` with a fixed size collapses them.
- ``setinputsizes`` sizes are honoured by the declared type, and a value longer
  than the declared size must fail with a data truncation error, not silently
  match a truncated prefix.
- Trailing spaces and empty strings must reach the server unchanged.

Sources (research; WebSearch result snippets, pages not fetched):

- https://github.com/jOOQ/jOOQ/issues/11537 (string params sent as nvarchar(4000) hurt plans on varchar columns)
- https://learn.microsoft.com/en-us/answers/questions/640059/nvarchar-convert-to-varchar-cause-index-scan (CONVERT_IMPLICIT on the column causes index scan)
- https://www.sqlservercentral.com/blogs/impact-on-indexes-when-mixing-varchar-and-nvarchar-types (mixing varchar and nvarchar in indexed predicates)
- https://techcommunity.microsoft.com/t5/azure-database-support-blog/performance-degradation-due-to-implicit-conversion/ba-p/2760732 (implicit conversion performance degradation)
- https://github.com/sqlalchemy/sqlalchemy/discussions/8171 (varchar vs nvarchar parameter causes Scan instead of Seek; setinputsizes fix)
- https://github.com/sqlalchemy/sqlalchemy/issues/5345 (generated query binds nvarchar for varchar column; setinputsizes(SQL_VARCHAR))
- https://github.com/mkleehammer/pyodbc/issues/835 (setinputsizes needed to control declared size of long strings)
- https://consultwithgriff.com/dapper-nvarchar-implicit-conversion-performance-trap (Dapper maps C# strings to nvarchar(4000); implicit conversion)

No mssql-python-specific issue turned up in the search; the ASCII-to-varchar
behaviour and the size boundaries below were checked directly against the
upstream ODBC driver.
"""

import re
import uuid

import pytest

from mssql_python import SQL_VARCHAR, SQL_WVARCHAR, DataError

# Column-side implicit conversion to nvarchar: CONVERT_IMPLICIT(nvarchar(n),[col]).
# Parameter-side conversions show up as [@P1] and are not a problem.
_COLUMN_SIDE_NVARCHAR_CONVERT = re.compile(r"CONVERT_IMPLICIT\(nvarchar\(\d+\),\[(?!@)")


def _setup_table(cursor):
    """Temp table with an index on a varchar and an nvarchar column, ~5000 rows."""
    cursor.execute(
        "CREATE TABLE #t (id int IDENTITY PRIMARY KEY, "
        "code varchar(50) COLLATE Latin1_General_CI_AS NOT NULL, "
        "ncode nvarchar(50) NOT NULL)"
    )
    cursor.execute("CREATE INDEX ix_code ON #t(code)")
    cursor.execute("CREATE INDEX ix_ncode ON #t(ncode)")
    cursor.execute(
        "INSERT INTO #t(code, ncode) "
        "SELECT CONVERT(varchar(50), 'c' + CAST(n AS varchar(10))), "
        "CAST('c' + CAST(n AS varchar(10)) AS nvarchar(50)) "
        "FROM (SELECT TOP 5000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
        "FROM sys.all_objects a CROSS JOIN sys.all_objects b) x"
    )


def _cached_entries(cursor, marker):
    """Return (statement_text, plan_xml) for cached statements containing marker."""
    cursor.execute(
        """
        SELECT st.text, CAST(qp.query_plan AS nvarchar(max))
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE st.text LIKE ? AND st.text NOT LIKE '%dm_exec%'
        """,
        f"%{marker}%",
    )
    return [(text, plan or "") for text, plan in cursor.fetchall()]


def _declared_header(statement_text):
    """The parameter declaration prefix, e.g. '(@P1 varchar(3))'."""
    return statement_text.split("SELECT", 1)[0].strip()


def _run_and_inspect(cursor, sql_with_marker, *params, sizes=None):
    """Execute a query tagged with a unique marker; return its cached entries."""
    marker = "pt" + uuid.uuid4().hex
    if sizes is not None:
        cursor.setinputsizes(sizes)
    cursor.execute(sql_with_marker.replace("MARK", marker), *params)
    cursor.fetchall()
    entries = _cached_entries(cursor, marker)
    assert entries, "expected a cached plan for the executed statement"
    return entries


def _single_header(cursor, sql_with_marker, *params, sizes=None):
    entries = _run_and_inspect(cursor, sql_with_marker, *params, sizes=sizes)
    assert len(entries) == 1, f"expected one cached statement, got {len(entries)}"
    return _declared_header(entries[0][0])


def test_ascii_param_declared_varchar_exact_length(cursor):
    _setup_table(cursor)
    header = _single_header(cursor, "SELECT COUNT(*) FROM #t WHERE code = ? /*MARK*/", "abc")
    assert header == "(@P1 varchar(3))"


def test_non_ascii_param_declared_nvarchar_exact_length(cursor):
    _setup_table(cursor)
    header = _single_header(
        cursor, "SELECT COUNT(*) FROM #t WHERE ncode = ? /*MARK*/", "éèêëàâç"
    )
    assert header == "(@P1 nvarchar(7))"


def test_declared_size_boundary_ascii_4000_is_varchar_4000_and_4001_is_max(cursor):
    _setup_table(cursor)
    at_limit = _single_header(cursor, "SELECT COUNT(*) FROM #t WHERE code = ? /*MARK*/", "a" * 4000)
    over_limit = _single_header(cursor, "SELECT COUNT(*) FROM #t WHERE code = ? /*MARK*/", "a" * 4001)
    assert at_limit == "(@P1 varchar(4000))"
    assert over_limit == "(@P1 varchar(max))"


def test_declared_size_boundary_non_ascii_4000_is_nvarchar_4000_and_4001_is_max(cursor):
    _setup_table(cursor)
    at_limit = _single_header(cursor, "SELECT COUNT(*) FROM #t WHERE ncode = ? /*MARK*/", "é" * 4000)
    over_limit = _single_header(cursor, "SELECT COUNT(*) FROM #t WHERE ncode = ? /*MARK*/", "é" * 4001)
    assert at_limit == "(@P1 nvarchar(4000))"
    assert over_limit == "(@P1 nvarchar(max))"


def test_empty_string_round_trips_as_empty_string(cursor):
    cursor.execute("SELECT ?, DATALENGTH(?)", "", "")
    value, length = cursor.fetchone()
    assert value == ""
    assert type(value) is str
    assert length == 0


def test_large_ascii_5000_round_trips_exactly(cursor):
    text = ("abcdefghij" * 500)
    cursor.execute("SELECT ?, LEN(?), DATALENGTH(?)", text, text, text)
    value, chars, octets = cursor.fetchone()
    assert value == text
    assert chars == 5000
    assert octets == 5000


def test_large_non_ascii_5000_round_trips_with_two_bytes_per_char(cursor):
    text = "é" * 5000
    cursor.execute("SELECT ?, LEN(?), DATALENGTH(?)", text, text, text)
    value, chars, octets = cursor.fetchone()
    assert value == text
    assert chars == 5000
    assert octets == 10000


def test_emoji_declared_as_nvarchar_in_utf16_units_not_python_length(cursor):
    _setup_table(cursor)
    emoji = "\U0001F600"  # one Python code point, two UTF-16 code units
    header = _single_header(cursor, "SELECT COUNT(*) FROM #t WHERE ncode = ? /*MARK*/", emoji)
    assert header == "(@P1 nvarchar(2))"

    cursor.execute("SELECT ?, DATALENGTH(?)", emoji, emoji)
    value, octets = cursor.fetchone()
    assert value == emoji
    assert octets == 4


def test_trailing_spaces_are_sent_unchanged(cursor):
    cursor.execute("SELECT ?, DATALENGTH(?)", "ab  ", "ab  ")
    value, octets = cursor.fetchone()
    assert value == "ab  "
    assert octets == 4


def test_ascii_param_on_varchar_index_does_not_convert_column(cursor):
    _setup_table(cursor)
    entries = _run_and_inspect(cursor, "SELECT id FROM #t WHERE code = ? /*MARK*/", "c42")
    plan = entries[0][1]
    assert plan
    assert not _COLUMN_SIDE_NVARCHAR_CONVERT.search(plan)


def test_non_ascii_param_on_varchar_index_converts_column_and_still_matches(cursor):
    _setup_table(cursor)
    cursor.execute("INSERT INTO #t(code, ncode) VALUES (?, ?)", "café", "café")
    entries = _run_and_inspect(cursor, "SELECT id FROM #t WHERE code = ? /*MARK*/", "café")
    plan = entries[0][1]
    assert _COLUMN_SIDE_NVARCHAR_CONVERT.search(plan), "nvarchar param must convert the varchar column"

    cursor.execute("SELECT COUNT(*) FROM #t WHERE code = ?", "café")
    assert cursor.fetchone()[0] == 1


def test_ascii_param_on_nvarchar_index_does_not_convert_column(cursor):
    _setup_table(cursor)
    entries = _run_and_inspect(cursor, "SELECT id FROM #t WHERE ncode = ? /*MARK*/", "c42")
    assert entries[0][0].split("SELECT", 1)[0].strip() == "(@P1 varchar(3))"
    assert not _COLUMN_SIDE_NVARCHAR_CONVERT.search(entries[0][1])


def test_setinputsizes_sql_varchar_declares_requested_size(cursor):
    _setup_table(cursor)
    header = _single_header(
        cursor,
        "SELECT COUNT(*) FROM #t WHERE code = ? /*MARK*/",
        "abc",
        sizes=[(SQL_VARCHAR, 50, 0)],
    )
    assert header == "(@P1 varchar(50))"


def test_setinputsizes_wvarchar_forces_nvarchar_and_converts_varchar_column(cursor):
    _setup_table(cursor)
    entries = _run_and_inspect(
        cursor,
        "SELECT id FROM #t WHERE code = ? /*MARK*/",
        "c42",
        sizes=[(SQL_WVARCHAR, 50, 0)],
    )
    assert len(entries) == 1
    assert _declared_header(entries[0][0]) == "(@P1 nvarchar(50))"
    assert _COLUMN_SIDE_NVARCHAR_CONVERT.search(entries[0][1])


def test_default_sizing_creates_one_plan_entry_per_distinct_length(cursor):
    _setup_table(cursor)
    marker = "pt" + uuid.uuid4().hex
    sql = f"SELECT COUNT(*) FROM #t WHERE code = ? /*{marker}*/"
    for value in ["a", "ab", "abcde", "abcdefghi", "abcde"]:
        cursor.execute(sql, value)
        cursor.fetchall()
    entries = _cached_entries(cursor, marker)
    headers = sorted(_declared_header(text) for text, _ in entries)
    assert headers == [
        "(@P1 varchar(1))",
        "(@P1 varchar(2))",
        "(@P1 varchar(5))",
        "(@P1 varchar(9))",
    ]


def test_fixed_setinputsizes_shares_one_plan_entry_across_lengths(cursor):
    _setup_table(cursor)
    marker = "pt" + uuid.uuid4().hex
    sql = f"SELECT COUNT(*) FROM #t WHERE code = ? /*{marker}*/"
    for value in ["a", "ab", "abcde", "abcdefghi"]:
        cursor.setinputsizes([(SQL_VARCHAR, 50, 0)])
        cursor.execute(sql, value)
        assert cursor.fetchone()[0] == 0
    entries = _cached_entries(cursor, marker)
    assert len(entries) == 1
    assert _declared_header(entries[0][0]) == "(@P1 varchar(50))"


def test_setinputsizes_applies_only_to_the_next_execute(cursor):
    _setup_table(cursor)
    marker = "pt" + uuid.uuid4().hex
    sql = f"SELECT COUNT(*) FROM #t WHERE code = ? /*{marker}*/"
    cursor.setinputsizes([(SQL_VARCHAR, 50, 0)])
    cursor.execute(sql, "abc")
    cursor.fetchall()
    cursor.execute(sql, "abcde")
    cursor.fetchall()
    headers = sorted(_declared_header(text) for text, _ in _cached_entries(cursor, marker))
    assert headers == ["(@P1 varchar(5))", "(@P1 varchar(50))"]


def test_setinputsizes_too_small_raises_data_error_not_prefix_match(cursor, conn):
    _setup_table(cursor)
    cursor.execute("INSERT INTO #t(code, ncode) VALUES ('abcdefghij', N'abcdefghij')")
    cursor.setinputsizes([(SQL_VARCHAR, 10, 0)])
    with pytest.raises(DataError):
        cursor.execute("SELECT id FROM #t WHERE code = ?", "abcdefghijKLMNO")
        cursor.fetchall()

    # The connection stays usable after the error.
    fresh = conn.cursor()
    fresh.execute("SELECT COUNT(*) FROM #t WHERE code = ?", "abcdefghij")
    assert fresh.fetchone()[0] == 1
    fresh.close()


def test_executemany_mixed_declared_types_round_trip_in_order(cursor):
    cursor.execute("CREATE TABLE #big (id int IDENTITY PRIMARY KEY, v nvarchar(max) NOT NULL)")
    values = [
        "a",
        "é",
        "x" * 4001,
        "y" * 4000,
        "\U0001F600",
        "",
        "tail  ",
    ]
    cursor.executemany("INSERT INTO #big(v) VALUES (?)", [(v,) for v in values])
    cursor.execute("SELECT v FROM #big ORDER BY id")
    assert [row[0] for row in cursor.fetchall()] == values
