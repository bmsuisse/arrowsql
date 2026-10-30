"""Sparse columns and column sets for the native TDS driver.

Sources (research, ~10 min):
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/sparse-columns  mssql-python sparse-column guidance:
  SELECT * returns the column set instead of the sparse columns; update through the column set XML
- https://learn.microsoft.com/en-us/sql/relational-databases/tables/use-column-sets  column set semantics: NULL sparse
  columns are omitted from the XML, unspecified sparse columns are NULLed on update, XML values implicitly converted,
  an explicit column list cannot mix with the column set in INSERT
- https://learn.microsoft.com/en-us/sql/relational-databases/native-client/features/sparse-columns-support-in-sql-server-native-client
  column_set behaves like an XML column; SELECT * returns non-sparse columns + the column set
- https://learn.microsoft.com/en-us/sql/connect/jdbc/sparse-columns  column sets up to 30,000 columns; SELECT * shape
- https://www.sqlskills.com/blogs/paul/sql-server-2008-sparse-columns-and-xml-column_set/  SELECT * with 1000+ sparse
  columns; column set materialized only when selected
- https://www.sqlskills.com/blogs/bobb/more-about-sparse-columns-and-column_sets/  column set insert/update semantics
- https://github.com/mkleehammer/pyodbc/issues/741  NULL-heavy inserts (background only; no sparse-specific bug found)

No mssql-python or pyodbc GitHub issue specifically about column sets was found; the failure modes below are the
documented SQL Server behaviors a native-TDS reimplementation has to reproduce on the wire and in result metadata:

- SELECT * on a table with a column set returns only the non-sparse columns plus the column set (as str)
- the column set XML contains only non-NULL sparse columns; all-NULL sparse -> column set is NULL (None)
- empty-string sparse values are kept (XML <s2/>), distinct from NULL (omitted)
- sparse columns are readable by name with their native Python types (int, str, datetime)
- INSERT / UPDATE through the column set XML: unspecified sparse columns become NULL, column set = NULL clears all
- XML-escaped content (&amp;, non-ASCII) round-trips; unescaped '&' is rejected
- invalid column set XML (unknown element, bad conversion, duplicate element, non-str param, mixing with explicit
  sparse columns in the same INSERT) raises the server error mapped to ProgrammingError / DataError
- wide tables (>1024 sparse columns) keep SELECT * narrow and are addressable by name
"""

import datetime

import pytest
from mssql_python import DataError, ProgrammingError


@pytest.fixture
def sparse_temp(cursor):
    """#sp: one plain column set plus three sparse columns (int, nvarchar, datetime2)."""
    cursor.execute(
        """
        CREATE TABLE #sp (
            id int NOT NULL PRIMARY KEY,
            name nvarchar(50) NULL,
            s1 int SPARSE NULL,
            s2 nvarchar(20) SPARSE NULL,
            s3 datetime2 SPARSE NULL,
            props xml COLUMN_SET FOR ALL_SPARSE_COLUMNS NULL
        )
        """
    )
    cursor.connection.commit()
    return cursor


def test_select_star_returns_column_set_instead_of_sparse_columns(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1, s2) VALUES (1, N'a', 5, N'x')")
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT * FROM #sp")
    assert [d[0] for d in sparse_temp.description] == ["id", "name", "props"]
    row = sparse_temp.fetchone()
    assert len(row) == 3
    assert row[0] == 1 and type(row[0]) is int
    assert row[1] == "a" and type(row[1]) is str


def test_column_set_is_str_and_lists_only_non_null_sparse_values(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1, s2) VALUES (1, N'a', 5, N'x')")
    sparse_temp.execute("SELECT props FROM #sp WHERE id = 1")
    value = sparse_temp.fetchone()[0]
    assert type(value) is str
    assert value == "<s1>5</s1><s2>x</s2>"
    assert "<s3>" not in value


def test_column_set_is_none_when_all_sparse_columns_null(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name) VALUES (2, N'b')")
    sparse_temp.execute("SELECT props FROM #sp WHERE id = 2")
    row = sparse_temp.fetchone()
    assert row[0] is None
    assert type(row[0]) is type(None)


def test_empty_string_sparse_value_is_kept_and_differs_from_null(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1, s2, s3) VALUES (1, N'a', 5, N'', NULL)")
    sparse_temp.execute("SELECT s2, s3, props FROM #sp WHERE id = 1")
    s2, s3, props = sparse_temp.fetchone()
    assert s2 == "" and type(s2) is str
    assert s3 is None
    assert props == "<s1>5</s1><s2/>"


def test_explicit_sparse_columns_read_with_native_types(sparse_temp):
    sparse_temp.execute(
        "INSERT INTO #sp (id, name, s1, s2, s3) VALUES (1, N'a', 2147483647, N'zzz', ?)",
        datetime.datetime(2024, 5, 6, 7, 8, 9, 123456),
    )
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT s1, s2, s3 FROM #sp WHERE id = 1")
    s1, s2, s3 = sparse_temp.fetchone()
    assert s1 == 2147483647 and type(s1) is int
    assert s2 == "zzz" and type(s2) is str
    assert s3 == datetime.datetime(2024, 5, 6, 7, 8, 9, 123456)
    assert type(s3) is datetime.datetime


def test_insert_through_column_set_populates_sparse_columns(sparse_temp):
    sparse_temp.execute(
        "INSERT INTO #sp (id, name, props) VALUES (1, N'c', ?)",
        "<s1>7</s1><s2>yy</s2>",
    )
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT s1, s2, s3 FROM #sp WHERE id = 1")
    s1, s2, s3 = sparse_temp.fetchone()
    assert s1 == 7 and type(s1) is int
    assert s2 == "yy"
    assert s3 is None


def test_update_column_set_nulls_sparse_columns_not_mentioned(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1, s2) VALUES (1, N'a', 5, N'x')")
    sparse_temp.connection.commit()
    sparse_temp.execute("UPDATE #sp SET props = ? WHERE id = 1", "<s1>9</s1>")
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT s1, s2, props FROM #sp WHERE id = 1")
    s1, s2, props = sparse_temp.fetchone()
    assert s1 == 9
    assert s2 is None
    assert props == "<s1>9</s1>"


def test_update_column_set_to_null_clears_every_sparse_column(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1, s2, s3) VALUES (1, N'a', 5, N'x', GETDATE())")
    sparse_temp.connection.commit()
    sparse_temp.execute("UPDATE #sp SET props = ? WHERE id = 1", None)
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT s1, s2, s3, props, name FROM #sp WHERE id = 1")
    s1, s2, s3, props, name = sparse_temp.fetchone()
    assert (s1, s2, s3, props) == (None, None, None, None)
    assert name == "a"


def test_column_set_xml_escaping_round_trips(sparse_temp):
    sparse_temp.execute(
        "INSERT INTO #sp (id, name, props) VALUES (1, N'u', ?)",
        "<s2>Müller &amp; € </s2>",
    )
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT s2 FROM #sp WHERE id = 1")
    assert sparse_temp.fetchone()[0] == "Müller & € "


def test_unescaped_ampersand_in_column_set_xml_is_rejected(sparse_temp):
    with pytest.raises(ProgrammingError, match="XML parsing"):
        sparse_temp.execute("INSERT INTO #sp (id, name, props) VALUES (1, N'u', ?)", "<s2>a & b</s2>")


def test_unknown_column_set_element_raises_programming_error(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name) VALUES (1, N'u')")
    sparse_temp.connection.commit()
    with pytest.raises(ProgrammingError, match="nope"):
        sparse_temp.execute("UPDATE #sp SET props = ? WHERE id = 1", "<nope>1</nope>")


def test_column_set_value_that_cannot_convert_raises_programming_error(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name) VALUES (1, N'u')")
    sparse_temp.connection.commit()
    with pytest.raises(ProgrammingError, match="conversion failed"):
        sparse_temp.execute("UPDATE #sp SET props = ? WHERE id = 1", "<s1>abc</s1>")


def test_duplicate_column_set_element_raises_programming_error(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name) VALUES (1, N'u')")
    sparse_temp.connection.commit()
    with pytest.raises(ProgrammingError, match="duplicate references"):
        sparse_temp.execute("UPDATE #sp SET props = ? WHERE id = 1", "<s1>1</s1><s1>2</s1>")


def test_non_string_param_for_xml_column_set_raises_data_error(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name) VALUES (1, N'u')")
    sparse_temp.connection.commit()
    with pytest.raises(DataError):
        sparse_temp.execute("UPDATE #sp SET props = ? WHERE id = 1", 42)


def test_explicit_sparse_column_and_column_set_in_one_insert_rejected(sparse_temp):
    with pytest.raises(ProgrammingError):
        sparse_temp.execute(
            "INSERT INTO #sp (id, name, s1, props) VALUES (1, N'a', 3, ?)",
            "<s1>1</s1>",
        )


def test_executemany_column_set_with_mixed_null_and_values(sparse_temp):
    sparse_temp.executemany(
        "INSERT INTO #sp (id, name, props) VALUES (?, ?, ?)",
        [
            (1, "a", "<s1>1</s1>"),
            (2, "b", None),
            (3, "c", "<s2>three</s2>"),
        ],
    )
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT id, s1, s2 FROM #sp ORDER BY id")
    assert [tuple(r) for r in sparse_temp.fetchall()] == [(1, 1, None), (2, None, None), (3, None, "three")]


def test_sparse_null_filter_and_column_set_null_filter(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1) VALUES (1, N'a', 4)")
    sparse_temp.execute("INSERT INTO #sp (id, name) VALUES (2, N'b')")
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT id FROM #sp WHERE s1 IS NULL")
    assert [tuple(r) for r in sparse_temp.fetchall()] == [(2,)]
    sparse_temp.execute("SELECT id FROM #sp WHERE props IS NULL")
    assert [tuple(r) for r in sparse_temp.fetchall()] == [(2,)]


def test_explicit_select_of_sparse_column_has_its_own_description_entry(sparse_temp):
    sparse_temp.execute("INSERT INTO #sp (id, name, s1) VALUES (1, N'a', 4)")
    sparse_temp.connection.commit()
    sparse_temp.execute("SELECT id, s1, props FROM #sp")
    assert [d[0] for d in sparse_temp.description] == ["id", "s1", "props"]
    assert tuple(sparse_temp.fetchone()) == (1, 4, "<s1>4</s1>")


def test_permanent_table_select_star_hides_sparse_columns(cursor, table_name):
    cursor.execute(
        f"""
        CREATE TABLE dbo.{table_name} (
            id int NOT NULL PRIMARY KEY,
            payload nvarchar(10) NULL,
            code varchar(10) SPARSE NULL,
            amount decimal(10, 2) SPARSE NULL,
            props xml COLUMN_SET FOR ALL_SPARSE_COLUMNS NULL
        )
        """
    )
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (id, payload, code, amount) VALUES (1, N'p', 'ABC', 12.50)"
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT * FROM dbo.{table_name}")
    assert [d[0] for d in cursor.description] == ["id", "payload", "props"]
    row = cursor.fetchone()
    assert tuple(row) == (1, "p", "<code>ABC</code><amount>12.50</amount>")


def test_wide_table_with_1100_sparse_columns_keeps_select_star_narrow(cursor, table_name):
    width = 1100
    sparse_defs = ",\n".join(f"c{i} int SPARSE NULL" for i in range(1, width + 1))
    cursor.execute(
        f"""
        CREATE TABLE dbo.{table_name} (
            id int NOT NULL PRIMARY KEY,
            {sparse_defs},
            props xml COLUMN_SET FOR ALL_SPARSE_COLUMNS NULL
        )
        """
    )
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (id, props) VALUES (1, ?)",
        "<c1000>5</c1000><c1100>6</c1100>",
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT * FROM dbo.{table_name}")
    assert [d[0] for d in cursor.description] == ["id", "props"]
    assert tuple(cursor.fetchone()) == (1, "<c1000>5</c1000><c1100>6</c1100>")
    cursor.execute(f"SELECT c1000, c1100, c1 FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (5, 6, None)
