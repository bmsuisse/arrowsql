"""cursor.description per SQL type: type_code, display/internal/precision/scale/null_ok.

Research sources (issue trackers and docs; the rewrite has to produce the same
7-tuples that the ODBC driver produces from SQLDescribeCol):
- https://github.com/mkleehammer/pyodbc/issues/521
  ("[Description] Internal size of description gives precision": pyodbc fills
  internal_size with the column size, not a byte count; display_size is always None)
- https://github.com/mkleehammer/pyodbc/wiki/Cursor
  (documents the seven fields; display_size is not set by pyodbc; internal_size is
  documented as bytes; the type_code slot holds a Python type, not an ODBC type code)
- https://github.com/mkleehammer/pyodbc/issues/366
  (type_code/precision/internal_size duplication shown in a float description, and
  output converters changing the type_code slot to str)
- https://github.com/mkleehammer/pyodbc/issues/865
  (cursor.description intermittently empty when a query returns rows; the caller
  then fails on None)
- https://github.com/pymssql/pymssql/issues/254
  (fetchall() after an execute that produced no result set raises instead of
  returning an empty result)
- https://github.com/Teradata/PyTd/issues/54
  (precision/internal_size and scale swapped in description: a classic tuple-slot
  mistake that a reimplementation can easily make)
- https://github.com/oracle/python-cx_Oracle/issues/177
  (precision and scale reported as None for typed numeric columns)
- https://github.com/microsoft/mssql-python/issues/435
  (VARCHAR round-trip at the exact column size; cited in test_rs_varchar_codepages.py)
- https://github.com/tiberius-rs/tiberius/issues/403
  (IDENTITY flag and nvarchar column size decoded wrongly from COLMETADATA flags)
- https://github.com/tiberius-rs/tiberius/issues/397
  (users have to rebuild the column type metadata themselves when the driver does not
  expose it)
- https://github.com/tiberius-rs/tiberius/issues/410
  (a DATE column in bulk COLMETADATA adds a spurious byte that shifts later columns)
- https://peps.python.org/pep-0249/#description
  (seven-item sequence; name and type_code required; the rest None when not meaningful;
  None when no operation has been performed)
- https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql
  (max lengths: varchar(max)/varbinary(max) 2^31-1 bytes, text/image 2^31-1,
  ntext 2^30-1 characters, time(n) fractional scales 0-7, datetimeoffset(n) n 0-7)

Behaviors a native-TDS reimplementation can easily get wrong:
- TDS reports a max length (0xFFFF) for varchar(max); the ODBC column size for it is 0,
  while text/image report 2147483647 and ntext reports 1073741823; mixing these up is
  an easy error
- time(n) and datetimeoffset(n) precision values depend on the fractional scale
  (time(3)=12, time(7)=16; datetimeoffset(7)=34), not a fixed size
- null_ok comes from column nullability (NOT NULL -> False), including IDENTITY and
  computed columns; a plain expression in the SELECT list is nullable
- display_size is None for every column, internal_size is the character count for
  strings and the byte count for binary, not a UTF-16 byte count
- an unaliased expression gets an empty-string name, not None
- WHERE 1=0 must still produce a description (names and types) with no rows
- DDL and DML leave description None, while SELECT sets it
- after nextset(), description describes the new result set, not the previous one
- a failed execute() must not leave the previous statement's description visible
  (see the upstream_bug test)
"""

import datetime
import decimal
import uuid

import pytest
from mssql_python import ProgrammingError


def _describe(cursor, sql):
    cursor.execute(sql)
    return cursor.description


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("CAST(1 AS int)", ("int", int, 10, 10, 0)),
        ("CAST(2 AS bigint)", ("bigint", int, 19, 19, 0)),
        ("CAST(3 AS smallint)", ("smallint", int, 5, 5, 0)),
        ("CAST(4 AS tinyint)", ("tinyint", int, 3, 3, 0)),
    ],
    ids=["int", "bigint", "smallint", "tinyint"],
)
def test_integer_family_type_code_and_sizes(cursor, sql, expected):
    """Integer types map to Python int; precision is the decimal digit count."""
    _, py_type, precision, internal, scale = expected
    d = _describe(cursor, f"SELECT {sql} AS v")
    assert len(d) == 1
    name, type_code, display, internal_size, prec, sc, null_ok = d[0]
    assert name == "v"
    assert type_code is py_type
    assert display is None
    assert internal_size == precision == internal
    assert prec == precision
    assert sc == scale
    assert null_ok is True


def test_bit_maps_to_bool_with_precision_one(cursor):
    d = _describe(cursor, "SELECT CAST(1 AS bit) AS b")
    assert d[0][1] is bool
    assert d[0][2] is None  # display_size
    assert d[0][3] == 1
    assert d[0][4] == 1
    assert d[0][5] == 0


def test_decimal_numeric_and_money_precision_and_scale(cursor):
    d = _describe(
        cursor,
        "SELECT CAST(1.5 AS decimal(10,3)) AS d, CAST(1.5 AS numeric(5,2)) AS n, "
        "CAST(1.5 AS money) AS m, CAST(1.5 AS decimal(38,10)) AS big",
    )
    assert [(c[0], c[1], c[4], c[5]) for c in d] == [
        ("d", decimal.Decimal, 10, 3),
        ("n", decimal.Decimal, 5, 2),
        ("m", decimal.Decimal, 19, 4),
        ("big", decimal.Decimal, 38, 10),
    ]


def test_float_is_53_bits_and_real_is_24(cursor):
    d = _describe(cursor, "SELECT CAST(1.5 AS float) AS f, CAST(1.5 AS real) AS r")
    assert d[0][1] is float and d[0][4] == 53 and d[0][5] == 0
    assert d[1][1] is float and d[1][4] == 24 and d[1][5] == 0


def test_string_types_report_declared_length_and_zero_scale(cursor):
    """varchar/nvarchar/char/nchar: precision and internal_size both equal the declared length."""
    d = _describe(
        cursor,
        "SELECT CAST('ab' AS varchar(10)) AS v, CAST(N'ab' AS nvarchar(10)) AS nv, "
        "CAST('ab' AS char(4)) AS c, CAST(N'ab' AS nchar(4)) AS nc",
    )
    assert [(c[0], c[1], c[3], c[4], c[5]) for c in d] == [
        ("v", str, 10, 10, 0),
        ("nv", str, 10, 10, 0),
        ("c", str, 4, 4, 0),
        ("nc", str, 4, 4, 0),
    ]


def test_max_and_legacy_lob_string_sizes(cursor):
    """varchar(max) reports 0; text/image report 2^31-1; ntext reports 2^30-1."""
    d = _describe(
        cursor,
        "SELECT CAST('x' AS varchar(max)) AS vm, CAST(N'x' AS nvarchar(max)) AS nvm, "
        "CAST(NULL AS text) AS t, CAST(NULL AS ntext) AS nt, CAST(NULL AS image) AS i",
    )
    got = [(c[0], c[1], c[4]) for c in d]
    assert got == [
        ("vm", str, 0),
        ("nvm", str, 0),
        ("t", str, 2147483647),
        ("nt", str, 1073741823),
        ("i", bytes, 2147483647),
    ]


def test_binary_types_are_bytes_with_byte_size(cursor):
    d = _describe(
        cursor,
        "SELECT CAST(0x01 AS binary(3)) AS b, CAST(0x01 AS varbinary(3)) AS vb, "
        "CAST(0x01 AS varbinary(max)) AS vbm",
    )
    assert [(c[0], c[1], c[4], c[5]) for c in d] == [
        ("b", bytes, 3, 0),
        ("vb", bytes, 3, 0),
        ("vbm", bytes, 0, 0),
    ]


def test_date_time_family_precision_and_scale(cursor):
    """Precision for temporal types depends on the fractional scale."""
    d = _describe(
        cursor,
        "SELECT CAST('2020-01-02' AS date) AS dt, CAST('10:11:12.1234567' AS time(7)) AS t7, "
        "CAST('2020-01-02 03:04:05.12' AS datetime) AS dtm, "
        "CAST('2020-01-02 03:04:05.1234567' AS datetime2(7)) AS dtm2, "
        "CAST('2020-01-02 03:04:05 +01:00' AS datetimeoffset(3)) AS dto",
    )
    assert [(c[0], c[1], c[4], c[5]) for c in d] == [
        ("dt", datetime.date, 10, 0),
        ("t7", datetime.time, 16, 7),
        ("dtm", datetime.datetime, 23, 3),
        ("dtm2", datetime.datetime, 27, 7),
        ("dto", datetime.datetime, 30, 3),
    ]


def test_uniqueidentifier_is_uuid_with_36_chars(cursor):
    d = _describe(cursor, "SELECT NEWID() AS g")

    assert d[0][1] is uuid.UUID
    assert d[0][4] == 36
    assert d[0][5] == 0


def test_xml_and_sql_variant_types(cursor):
    d = _describe(cursor, "SELECT CAST(N'<a/>' AS xml) AS x, CAST(1 AS sql_variant) AS sv")
    assert [(c[0], c[1], c[4], c[5]) for c in d] == [("x", str, 0, 0), ("sv", str, 8000, 0)]


def test_rowversion_hierarchyid_and_geography(cursor):
    """Types with no native Python mapping are returned as bytes with their ODBC sizes."""
    cursor.execute("CREATE TABLE #rv (rv rowversion NOT NULL, hid hierarchyid NULL, geo geography NULL)")
    d = _describe(cursor, "SELECT rv, hid, geo FROM #rv")
    assert [(c[0], c[1], c[4], c[6]) for c in d] == [
        ("rv", bytes, 8, False),
        ("hid", bytes, 892, True),
        ("geo", bytes, 0, True),
    ]


def test_null_ok_follows_column_nullability(cursor):
    cursor.execute(
        "CREATE TABLE #nul ("
        " id int IDENTITY(1,1) NOT NULL PRIMARY KEY,"
        " nn varchar(20) NOT NULL,"
        " nv nvarchar(max) NULL,"
        " dec_nn decimal(38,10) NOT NULL,"
        " uid uniqueidentifier NOT NULL,"
        " tm time(3) NOT NULL )"
    )
    cursor.execute("INSERT INTO #nul(nn, dec_nn, uid, tm) VALUES ('a', 1.5, NEWID(), '01:02:03')")
    d = _describe(cursor, "SELECT * FROM #nul")
    assert [(c[0], c[6]) for c in d] == [
        ("id", False),
        ("nn", False),
        ("nv", True),
        ("dec_nn", False),
        ("uid", False),
        ("tm", False),
    ]
    assert d[3][1] is not None and d[3][4] == 38 and d[3][5] == 10


def test_computed_column_nullability_follows_expression(cursor):
    """Computed column over a NOT NULL base is NOT NULL, a nullable base is nullable."""
    cursor.execute(
        "CREATE TABLE #comp (nn varchar(20) NOT NULL, nv varchar(20) NULL,"
        " c1 AS (nn + 'x'), c2 AS (nv + 'x'))"
    )
    d = _describe(cursor, "SELECT c1, c2 FROM #comp")
    assert [(c[0], c[1], c[4], c[6]) for c in d] == [
        ("c1", str, 21, False),
        ("c2", str, 21, True),
    ]


def test_empty_result_still_describes_columns(cursor):
    d = _describe(cursor, "SELECT CAST(1 AS int) AS i, CAST(N'x' AS nvarchar(5)) AS s WHERE 1 = 0")
    assert d is not None
    assert [(c[0], c[1], c[4], c[5]) for c in d] == [("i", int, 10, 0), ("s", str, 5, 0)]
    assert cursor.fetchall() == []


def test_top_zero_describes_table_columns(cursor):
    cursor.execute("CREATE TABLE #top (nn varchar(20) NOT NULL, dec_nn decimal(38,10) NULL)")
    d = _describe(cursor, "SELECT TOP 0 nn, dec_nn FROM #top")
    assert [(c[0], c[1], c[4], c[5], c[6]) for c in d] == [
        ("nn", str, 20, 0, False),
        ("dec_nn", decimal.Decimal, 38, 10, True),
    ]


def test_non_query_statements_leave_description_none(cursor):
    cursor.execute("CREATE TABLE #dml (a int)")
    assert cursor.description is None
    cursor.execute("INSERT INTO #dml VALUES (1)")
    assert cursor.description is None
    assert cursor.rowcount == 1
    cursor.execute("UPDATE #dml SET a = 2")
    assert cursor.description is None
    assert cursor.rowcount == 1


def test_cursor_description_is_none_before_any_execute(cursor):
    assert cursor.description is None


def test_unaliased_expressions_have_empty_string_names(cursor):
    d = _describe(cursor, "SELECT 1, CAST(2 AS int) AS two, 'x' + 'y'")
    assert [c[0] for c in d] == ["", "two", ""]
    assert [c[1] for c in d] == [int, int, str]


def test_duplicate_column_names_are_preserved_in_order(cursor):
    d = _describe(cursor, "SELECT 1 AS a, CAST('z' AS varchar(3)) AS a")
    assert [(c[0], c[1]) for c in d] == [("a", int), ("a", str)]


def test_description_after_nextset_describes_new_result_set(cursor):
    cursor.execute("SELECT 1 AS one; SELECT CAST('a' AS varchar(1)) AS s, CAST(2.5 AS decimal(2,1)) AS x")
    assert [c[0] for c in cursor.description] == ["one"]
    assert cursor.nextset() is True
    assert [(c[0], c[1], c[4], c[5]) for c in cursor.description] == [
        ("s", str, 1, 0),
        ("x", decimal.Decimal, 2, 1),
    ]


def test_description_replaced_not_merged_on_next_execute(cursor):
    _describe(cursor, "SELECT CAST(1 AS int) AS first_col")
    d = _describe(cursor, "SELECT CAST('abc' AS varchar(3)) AS second_col")
    assert [c[0] for c in d] == ["second_col"]
    assert len(d) == 1


@pytest.mark.upstream_bug(
    reason=(
        "after a failed execute() the previous statement's description stays visible; "
        "PEP 249 says description is None when no operation has produced columns"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_failed_execute_clears_previous_description(cursor):
    cursor.execute("SELECT CAST(1 AS int) AS ok_col")
    assert cursor.description is not None
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT * FROM dbo.ci_description_missing_table_xyz")
    assert cursor.description is None
