"""Deep XML-type behavior for the native TDS driver.

Sources (research, ~10 min):
- https://github.com/microsoft/mssql-python/pull/293  XML support PR: XML treated as LOB, streamed in FetchMany/FetchAll
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/xml-data  driver sends/receives XML as str
- https://github.com/mkleehammer/pyodbc/issues/79  long XML rows silently vanish with SQL Server Native Client
- https://github.com/FreeTDS/freetds/issues/115  top-level FOR XML PATH split into several rows by the client
- https://techcommunity.microsoft.com/blog/sqlserver/what-does-server-side-for-xml-return/383062  top-level FOR XML
  without TYPE streams in 2033-character chunks (UCS-2); TYPE or a subquery yields one row
- https://learn.microsoft.com/en-us/answers/questions/142700/for-json-outputs-in-multiple-rows  same split for FOR JSON,
  fixed by subquery / CAST / TYPE

Failure modes asserted here:
- >1 MB XML documents must round-trip byte-exact (no truncation at 8000/65536/2^31 boundaries)
- encoding declarations: utf-8 / ISO-8859-1 accepted on str input, declared utf-16 on str rejected,
  utf-16 bytes with BOM accepted, whitespace before the declaration rejected
- NULL vs empty string vs missing row for xml columns
- typed XML (schema collection): valid, invalid simple type, undeclared root, NULL
- FOR XML PATH: top-level split into 2033-char rows, subquery / TYPE single row, empty -> NULL
"""

import uuid

import pytest
from mssql_python import ProgrammingError

CHUNK = 2033  # SQL Server FOR XML chunk size (UCS-2 code units) for top-level FOR XML without TYPE


def _big_doc(items: int = 12000) -> str:
    return "<r>" + ("<i>" + "a" * 100 + "</i>") * items + "</r>"


def _for_xml_rows(count: int) -> str:
    return "".join(f'<o i="{i}"><v>{"y" * 20}</v></o>' for i in range(1, count + 1))


@pytest.fixture
def big_temp(cursor):
    """#big(i PK, v): 300 rows whose FOR XML PATH output is well over 2033 chars."""
    cursor.execute("CREATE TABLE #big (i int NOT NULL PRIMARY KEY, v varchar(50) NOT NULL)")
    cursor.executemany(
        "INSERT INTO #big (i, v) VALUES (?, ?)",
        [(i, "y" * 20) for i in range(1, 301)],
    )
    cursor.connection.commit()
    return _for_xml_rows(300)


def test_xml_roundtrip_exact_over_1mb(cursor):
    doc = _big_doc()
    assert len(doc) > 1_000_000
    cursor.execute("CREATE TABLE #xbig (id int NOT NULL, x xml NULL)")
    cursor.execute("INSERT INTO #xbig (id, x) VALUES (?, ?)", 1, doc)
    cursor.execute("SELECT x FROM #xbig WHERE id = 1")
    value = cursor.fetchone()[0]
    assert type(value) is str
    assert len(value) == len(doc)
    assert value == doc


@pytest.mark.parametrize("body_len", [1, 2033, 2034, 8000, 8001, 65536])
def test_xml_roundtrip_size_boundaries(cursor, body_len):
    doc = "<r>" + "x" * body_len + "</r>"
    cursor.execute("SELECT CAST(? AS xml)", doc)
    value = cursor.fetchone()[0]
    assert value == doc
    assert len(value) == body_len + len("<r></r>")


def test_xml_executemany_large_documents(cursor):
    docs = [(n, f'<d n="{n}">' + "z" * 300_000 + "</d>") for n in range(3)]
    cursor.execute("CREATE TABLE #xmany (n int NOT NULL, x xml NOT NULL)")
    cursor.executemany("INSERT INTO #xmany (n, x) VALUES (?, ?)", docs)
    cursor.execute("SELECT n, x FROM #xmany ORDER BY n")
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [0, 1, 2]
    assert [r[1] for r in rows] == [d for _, d in docs]


def test_xml_xquery_methods_on_large_document(cursor):
    cursor.execute("CREATE TABLE #xq (x xml NOT NULL)")
    cursor.execute("INSERT INTO #xq (x) VALUES (?)", _big_doc())
    cursor.execute("SELECT x.value('count(/r/i)', 'int') FROM #xq")
    assert cursor.fetchone()[0] == 12000
    cursor.execute("SELECT x.exist('/r/i') FROM #xq")
    assert cursor.fetchone()[0] is True
    cursor.execute("SELECT x.query('/r/i[1]') FROM #xq")
    fragment = cursor.fetchone()[0]
    assert type(fragment) is str
    assert fragment == "<i>" + "a" * 100 + "</i>"


def test_xml_nodes_cross_apply_on_large_document(cursor):
    cursor.execute("CREATE TABLE #xn (x xml NOT NULL)")
    cursor.execute("INSERT INTO #xn (x) VALUES (?)", _big_doc())
    cursor.execute(
        "SELECT n.value('.', 'varchar(200)') FROM #xn "
        "CROSS APPLY x.nodes('/r/i[position() <= 3]') AS T(n)"
    )
    assert [r[0] for r in cursor.fetchall()] == ["a" * 100] * 3


@pytest.mark.parametrize(
    "declared",
    [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<?xml version="1.0" encoding="ISO-8859-1"?>',
    ],
)
def test_xml_ascii_str_with_encoding_declaration_is_accepted(cursor, declared):
    cursor.execute("SELECT CAST(? AS xml)", f"{declared}<a>plain</a>")
    value = cursor.fetchone()[0]
    assert type(value) is str
    assert value == "<a>plain</a>"  # server drops the declaration and returns the infoset text


def test_xml_str_nonascii_with_non_utf16_declaration_is_rejected(cursor):
    # Server re-encodes the nvarchar content to the declared charset and fails when non-ASCII
    # text is present; ASCII-only input (test above) passes. Native drivers must keep this.
    with pytest.raises(ProgrammingError, match="unable to switch the encoding"):
        cursor.execute(
            "SELECT CAST(? AS xml)",
            '<?xml version="1.0" encoding="UTF-8"?><a>\u00e9</a>',
        )


def test_xml_str_nonascii_without_declaration_roundtrips(cursor):
    cursor.execute("SELECT CAST(? AS xml)", "<a>\u00e9\u20ac</a>")
    assert cursor.fetchone()[0] == "<a>\u00e9\u20ac</a>"


def test_xml_str_declared_utf16_is_rejected(cursor):
    # A str is already UTF-16 on the wire; a utf-16 declaration cannot be honoured.
    with pytest.raises(ProgrammingError, match="XML parsing"):
        cursor.execute(
            "SELECT CAST(? AS xml)",
            '<?xml version="1.0" encoding="utf-16"?><a>x</a>',
        )


def test_xml_utf16_bytes_with_declaration_accepted(cursor):
    payload = '<?xml version="1.0" encoding="utf-16"?><a>x</a>'.encode("utf-16")
    cursor.execute("SELECT CAST(? AS xml)", payload)
    assert cursor.fetchone()[0] == "<a>x</a>"


def test_xml_whitespace_before_declaration_is_rejected(cursor):
    with pytest.raises(ProgrammingError, match="XML parsing"):
        cursor.execute(
            "SELECT CAST(? AS xml)",
            '  <?xml version="1.0" encoding="utf-8"?><a>x</a>',
        )


def test_xml_malformed_raises_programming_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT CAST(? AS xml)", "<a>")


def test_xml_non_bmp_character_roundtrip(cursor):
    cursor.execute("CREATE TABLE #xemoji (x xml NOT NULL)")
    cursor.execute("INSERT INTO #xemoji (x) VALUES (?)", "<a>\U0001F600</a>")
    cursor.execute("SELECT x FROM #xemoji")
    assert cursor.fetchone()[0] == "<a>\U0001F600</a>"


def test_xml_null_and_empty_string_are_distinct(cursor):
    cursor.execute("CREATE TABLE #xnull (id int NOT NULL, x xml NULL)")
    cursor.execute("INSERT INTO #xnull (id, x) VALUES (1, ?)", None)
    cursor.execute("INSERT INTO #xnull (id, x) VALUES (2, ?)", "")
    cursor.execute("SELECT id, x, CASE WHEN x IS NULL THEN 1 ELSE 0 END FROM #xnull ORDER BY id")
    rows = cursor.fetchall()
    assert rows[0][1] is None and rows[0][2] == 1
    assert rows[1][1] == "" and rows[1][2] == 0


def test_xml_null_parameter_cast_to_xml_returns_none(cursor):
    cursor.execute("SELECT CAST(? AS xml)", None)
    assert cursor.fetchone()[0] is None


@pytest.mark.upstream_bug(
    reason="NULL parameter bound after a str parameter on the same cursor/statement "
    "fails with 'Implicit conversion from data type xml to varchar'; the NULL must bind as NULL"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_xml_null_param_after_str_param_same_statement(cursor):
    sql = "SELECT CAST(? AS xml)"
    cursor.execute(sql, "<a/>")
    assert cursor.fetchone()[0] == "<a/>"
    cursor.execute(sql, None)
    assert cursor.fetchone()[0] is None


SC_DDL = (
    "CREATE XML SCHEMA COLLECTION dbo.{sc} AS N'"
    '<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
    "<xs:element name=\"a\" type=\"xs:int\"/></xs:schema>'"
)


@pytest.fixture
def int_schema(conn, cursor):
    """Permanent XML schema collection with one global element a (xs:int); dropped after."""
    sc = f"ci_sc_{uuid.uuid4().hex[:10]}"
    cursor.execute(SC_DDL.format(sc=sc))
    conn.commit()
    yield sc
    cursor.execute(f"DROP XML SCHEMA COLLECTION dbo.{sc}")
    conn.commit()


def test_xml_typed_collection_valid_roundtrip(conn, cursor, int_schema, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int NOT NULL, x xml(dbo.{int_schema}) NULL)")
    conn.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, x) VALUES (1, ?)", "<a>7</a>")
    conn.commit()
    cursor.execute(f"SELECT x FROM dbo.{table_name} WHERE id = 1")
    assert cursor.fetchone()[0] == "<a>7</a>"


def test_xml_typed_collection_rejects_invalid_simple_type(conn, cursor, int_schema, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int NOT NULL, x xml(dbo.{int_schema}) NULL)")
    conn.commit()
    with pytest.raises(ProgrammingError, match="Invalid simple type"):
        cursor.execute(f"INSERT INTO dbo.{table_name} (id, x) VALUES (2, ?)", "<a>no</a>")
    conn.rollback()
    with pytest.raises(ProgrammingError, match="Declaration not found"):
        cursor.execute(f"INSERT INTO dbo.{table_name} (id, x) VALUES (3, ?)", "<b>5</b>")
    conn.rollback()


def test_xml_typed_cast_null_stays_null(cursor, int_schema):
    cursor.execute(f"SELECT CAST(NULL AS xml(dbo.{int_schema}))")
    assert cursor.fetchone()[0] is None


def test_for_xml_path_toplevel_splits_into_chunks(cursor, big_temp):
    cursor.execute("SELECT i AS '@i', v FROM #big FOR XML PATH('o')")
    chunks = [r[0] for r in cursor.fetchall()]
    assert len(chunks) > 1
    assert all(len(c) == CHUNK for c in chunks[:-1])
    assert 0 < len(chunks[-1]) <= CHUNK
    assert "".join(chunks) == big_temp


def test_for_xml_path_toplevel_fetchone_yields_first_chunk_only(cursor, big_temp):
    cursor.execute("SELECT i AS '@i', v FROM #big FOR XML PATH('o')")
    first = cursor.fetchone()[0]
    assert len(first) == CHUNK
    assert first == big_temp[:CHUNK]


def test_for_xml_path_subquery_returns_single_full_row(cursor, big_temp):
    cursor.execute("SELECT (SELECT i AS '@i', v FROM #big FOR XML PATH('o')) AS x")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == big_temp


def test_for_xml_path_type_directive_returns_single_row(cursor, big_temp):
    cursor.execute("SELECT i AS '@i', v FROM #big FOR XML PATH('o'), TYPE")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == big_temp


def test_for_xml_path_empty_result_is_null(cursor, big_temp):
    cursor.execute("SELECT (SELECT i AS '@i', v FROM #big WHERE 1 = 0 FOR XML PATH('o')) AS x")
    assert cursor.fetchone()[0] is None
