"""FOR JSON output chunking, OPENJSON with parameters, JSON_VALUE / JSON_QUERY,
ISJSON, and unicode inside JSON.

Failure modes a native-TDS reimplementation can easily get wrong:

- FOR JSON output is delivered as several rows of a single nvarchar column, each
  about 2033 characters; a client that reads only fetchone() gets truncated JSON
- the chunks must be concatenated in order, and a long parameter must travel as
  nvarchar(max), not nvarchar(4000), or OPENJSON sees a truncated document
- JSON_VALUE returns a scalar as nvarchar (42 -> '42', not an int); JSON null,
  missing path and object/array targets all come back as SQL NULL
- JSON_QUERY returns the fragment as text; ISJSON returns int 1/0 and NULL for NULL
- non-ASCII text and emoji (UTF-16 surrogate pairs) must round-trip intact through
  parameters, FOR JSON and JSON_VALUE

Sources (research, not all directly applicable):
- https://learn.microsoft.com/en-us/sql/relational-databases/json/format-query-results-as-json-with-for-json-sql-server
  (FOR JSON output and the client-side concatenation requirement)
- https://learn.microsoft.com/en-us/answers/questions/142700/for-json-outputs-in-multiple-rows
  (rows of ~2033 characters, concatenate on the client)
- https://github.com/microsoft/azuredatastudio/issues/26448 (FOR JSON split into 2033-char rows reported by tooling)
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/json-data
  (JSON stored in nvarchar, sent and received as plain Python strings)
- https://github.com/microsoft/mssql-python/issues/794 (leading U+FEFF / U+FFFE lost in bounded UTF-16 text fetches)
- https://github.com/microsoft/mssql-python/issues/553 (non-Unicode text decoded incorrectly)
- https://github.com/mkleehammer/pyodbc/issues/221 (emoji characters truncated on insert)

Expected values were verified against the upstream ODBC driver.
"""

import json

import pytest
from mssql_python import IntegrityError


def _chunks(cursor, sql, *params):
    cursor.execute(sql, *params)
    return [row[0] for row in cursor.fetchall()]


def _scalar(cursor, sql, *params):
    cursor.execute(sql, *params)
    return cursor.fetchone()[0]


# A deterministic 500-element array: each element ~45 chars, total well over 2033.
BIG_FOR_JSON = (
    "WITH n AS (SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i "
    "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
    "SELECT i, REPLICATE(N'x', 20) AS s FROM n ORDER BY i FOR JSON PATH"
)


def test_for_json_long_output_is_split_into_2033_char_rows(cursor):
    cursor.execute(BIG_FOR_JSON)
    rows = cursor.fetchall()
    assert len(rows) > 1
    pieces = [r[0] for r in rows]
    assert all(type(p) is str for p in pieces)
    # Every row but the last is a full 2033-character chunk.
    assert all(len(p) == 2033 for p in pieces[:-1])
    assert 0 < len(pieces[-1]) <= 2033


def test_for_json_chunks_concatenate_to_valid_json(cursor):
    full = "".join(_chunks(cursor, BIG_FOR_JSON))
    data = json.loads(full)
    assert len(data) == 500
    assert data[0] == {"i": 1, "s": "x" * 20}
    assert data[-1] == {"i": 500, "s": "x" * 20}


def test_fetchone_on_long_for_json_gets_only_first_chunk(cursor):
    cursor.execute(BIG_FOR_JSON)
    first = cursor.fetchone()[0]
    assert len(first) == 2033
    with pytest.raises(ValueError):
        json.loads(first)


def test_for_json_small_result_is_single_row(cursor):
    cursor.execute("SELECT 1 AS a, N'b' AS c FOR JSON PATH")
    rows = [tuple(r) for r in cursor.fetchall()]
    assert rows == [('[{"a":1,"c":"b"}]',)]
    assert type(rows[0][0]) is str


def test_for_json_subquery_returns_one_full_value(cursor):
    sql = (
        "SELECT (SELECT i, REPLICATE(N'x', 20) AS s FROM "
        "(SELECT TOP (500) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i "
        "FROM sys.all_objects a CROSS JOIN sys.all_objects b) n "
        "ORDER BY i FOR JSON PATH) AS j"
    )
    cursor.execute(sql)
    rows = [tuple(r) for r in cursor.fetchall()]
    assert len(rows) == 1
    value = rows[0][0]
    assert type(value) is str
    assert len(value) > 2033
    assert len(json.loads(value)) == 500


def test_for_json_subquery_over_empty_set_is_null(cursor):
    cursor.execute(
        "SELECT (SELECT name FROM sys.databases WHERE 1 = 0 FOR JSON PATH) AS j"
    )
    assert cursor.fetchone()[0] is None


def test_for_json_without_array_wrapper_returns_object(cursor):
    cursor.execute(
        "SELECT 7 AS id, N'seven' AS name FOR JSON PATH, WITHOUT_ARRAY_WRAPPER"
    )
    assert cursor.fetchone()[0] == '{"id":7,"name":"seven"}'


def test_for_json_omits_nulls_unless_include_null_values(cursor):
    cursor.execute(
        "SELECT CAST(1 AS INT) AS a, CAST(NULL AS INT) AS b FOR JSON PATH"
    )
    assert cursor.fetchone()[0] == '[{"a":1}]'
    cursor.execute(
        "SELECT CAST(1 AS INT) AS a, CAST(NULL AS INT) AS b "
        "FOR JSON PATH, INCLUDE_NULL_VALUES"
    )
    assert cursor.fetchone()[0] == '[{"a":1,"b":null}]'


def test_for_json_unicode_and_emoji_survive(cursor):
    text = "日本 é 😀"
    cursor.execute("SELECT N'日本 é 😀' AS t FOR JSON PATH")
    raw = cursor.fetchone()[0]
    assert json.loads(raw) == [{"t": text}]


def test_for_json_unicode_chunk_boundary_keeps_every_character(cursor):
    # Multibyte payload so the 2033-char split lands among non-ASCII characters.
    sql = (
        "WITH n AS (SELECT TOP (300) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i "
        "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
        "SELECT i, REPLICATE(N'日😀', 6) AS s FROM n ORDER BY i FOR JSON PATH"
    )
    full = "".join(_chunks(cursor, sql))
    data = json.loads(full)
    assert len(data) == 300
    assert data[41] == {"i": 42, "s": "日😀" * 6}


def test_openjson_with_parameter_returns_key_value_type(cursor):
    cursor.execute("SELECT [key], value, type FROM OPENJSON(?) ORDER BY [key]", '[{"a":1}, 5]')
    rows = [tuple(r) for r in cursor.fetchall()]
    assert rows == [("0", '{"a":1}', 5), ("1", "5", 2)]
    assert type(rows[0][0]) is str
    assert type(rows[1][2]) is int


def test_openjson_with_schema_returns_typed_columns(cursor):
    cursor.execute(
        "SELECT a, b, c FROM OPENJSON(?) "
        "WITH (a INT '$.a', b NVARCHAR(20) '$.b', c BIT '$.c') ORDER BY a",
        '[{"a":2,"b":"two","c":true},{"a":1,"b":"one","c":false}]',
    )
    rows = [tuple(r) for r in cursor.fetchall()]
    assert rows == [(1, "one", False), (2, "two", True)]
    assert type(rows[0][0]) is int
    assert type(rows[0][2]) is bool


def test_openjson_large_parameter_is_not_truncated(cursor):
    # > 4000 characters: must be bound as nvarchar(max), not nvarchar(4000).
    payload = json.dumps([{"i": i, "s": "y" * 30} for i in range(200)])
    assert len(payload) > 8000
    count, total = cursor.execute(
        "SELECT COUNT(*), SUM(CAST(JSON_VALUE(value, '$.i') AS INT)) "
        "FROM OPENJSON(?)",
        payload,
    ).fetchone()
    assert count == 200
    assert total == sum(range(200))


def test_json_value_scalar_is_returned_as_string(cursor):
    value = _scalar(cursor, "SELECT JSON_VALUE(?, '$.n')", '{"n":42}')
    assert value == "42"
    assert type(value) is str


def test_json_value_null_missing_and_object_are_sql_null(cursor):
    assert _scalar(cursor, "SELECT JSON_VALUE(?, '$.a')", '{"a":null}') is None
    assert _scalar(cursor, "SELECT JSON_VALUE(?, '$.missing')", '{"a":1}') is None
    assert _scalar(cursor, "SELECT JSON_VALUE(?, '$.a')", '{"a":{"b":1}}') is None


def test_json_value_unicode_value_round_trip(cursor):
    value = _scalar(cursor, "SELECT JSON_VALUE(?, '$.n')", '{"n":"日本 😀"}')
    assert value == "日本 😀"


def test_json_query_returns_fragment_text(cursor):
    assert _scalar(cursor, "SELECT JSON_QUERY(?, '$.a')", '{"a":[1,2]}') == "[1,2]"
    assert _scalar(cursor, "SELECT JSON_QUERY(?, '$.a')", '{"a":5}') is None


def test_isjson_returns_int_and_null_for_null(cursor):
    cursor.execute("SELECT ISJSON(?), ISJSON(?), ISJSON(CAST(NULL AS NVARCHAR(10)))", "{}", "x")
    valid, invalid, null = cursor.fetchone()
    assert valid == 1 and type(valid) is int
    assert invalid == 0
    assert null is None


def test_json_column_roundtrips_nvarchar_max_document(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, doc NVARCHAR(MAX) NOT NULL)")
    doc = json.dumps({"items": [{"k": i, "t": "日本😀" * 20} for i in range(120)]}, ensure_ascii=False)
    assert len(doc) > 8000
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, doc) VALUES (?, ?)", 1, doc)
    cursor.connection.commit()
    got = _scalar(cursor, f"SELECT doc FROM dbo.{table_name} WHERE id = ?", 1)
    assert got == doc
    assert json.loads(got)["items"][119]["k"] == 119


def test_isjson_check_constraint_rejects_invalid_json(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, "
        f"doc NVARCHAR(200) CHECK (ISJSON(doc) = 1))"
    )
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, doc) VALUES (?, ?)", 1, '{"ok":true}')
    cursor.connection.commit()
    with pytest.raises(IntegrityError):
        cursor.execute(f"INSERT INTO dbo.{table_name} (id, doc) VALUES (?, ?)", 2, "not json")
        cursor.connection.commit()
    cursor.connection.rollback()
    assert _scalar(cursor, f"SELECT COUNT(*) FROM dbo.{table_name}") == 1
