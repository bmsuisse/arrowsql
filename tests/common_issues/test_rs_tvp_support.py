"""Table-valued parameters (TVPs) and the documented workarounds.

Research (step 1):
- mssql-python support lifecycle lists "Table-valued parameters (TVPs)" as
  "Not supported" (the upstream driver has no TVP binding):
  https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/support-lifecycle
  https://github.com/MicrosoftDocs/sql-docs/blob/live/docs/connect/python/mssql-python/support-lifecycle.md
- Open upstream TVP work (not in the released driver used as ground truth here):
  https://github.com/microsoft/mssql-python/pull/789 (FEAT: Add table-valued parameter support)
  https://github.com/microsoft/mssql-python/pull/814 (FEAT: Add TVP support for async queries)
- pyodbc accepts a list of tuples for a TVP; the common failure modes users hit
  there are the reference for what a native rewrite would need to handle
  (and what it must NOT silently do):
  https://github.com/mkleehammer/pyodbc/wiki/Working-with-Table-Valued-Parameters-(TVPs)
  https://github.com/mkleehammer/pyodbc/issues/595 (type not in dbo schema)
  https://github.com/mkleehammer/pyodbc/issues/596 (None/NULL in TVP cell)
  https://github.com/mkleehammer/pyodbc/issues/659 (Type mismatch between TVP row values)
  https://github.com/mkleehammer/pyodbc/issues/732 (TVP mixed with other params)
  https://github.com/mkleehammer/pyodbc/issues/551 (identity column in TVP type)
- pymssql: no TVP support, open request: https://github.com/pymssql/pymssql/issues/635
- tiberius (Rust) added TVP via RPC API in 0.13.0 (community fork tiberius-ng):
  https://github.com/tiberius-rs/tiberius/pull/328
  https://github.com/tiberius-rs/tiberius/blob/main/CHANGELOG.md
- Microsoft Rust TDS backend TVP work (for the native rewrite):
  https://github.com/microsoft/mssql-rs/pull/715
  https://github.com/microsoft/mssql-rs/pull/708

Ground truth: upstream mssql-python 1.16.0 rejects a list/tuple/dict parameter
with TypeError("Unsupported parameter type: ..."). The supported workarounds are
passing a JSON string and calling OPENJSON, or a delimited string and calling
STRING_SPLIT. The tests below assert the clean error, that nothing partially
executes, and that the workarounds return correctly typed values.
"""

import json
import uuid
from decimal import Decimal

import pytest
from mssql_python import ProgrammingError


def _unique(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# Clean rejection of TVP-shaped parameters
# ---------------------------------------------------------------------------


def test_list_of_tuples_parameter_is_rejected_with_typeerror(cursor):
    with pytest.raises(TypeError, match="Unsupported parameter type"):
        cursor.execute("SELECT ?", ([(1, "a"), (2, "b")],))


def test_tuple_of_tuples_parameter_is_rejected_with_typeerror(cursor):
    with pytest.raises(TypeError, match="Unsupported parameter type"):
        cursor.execute("SELECT ?", (((1, "a"), (2, "b")),))


def test_dict_parameter_is_rejected_with_typeerror(cursor):
    with pytest.raises(TypeError, match="Unsupported parameter type"):
        cursor.execute("SELECT ?", ({"id": 1},))


def test_connection_usable_after_rejected_tvp_parameter(cursor):
    with pytest.raises(TypeError):
        cursor.execute("SELECT ?", ([(1, "a")],))
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    assert row[0] == 1
    assert type(row[0]) is int


def test_rejected_tvp_insert_does_not_partially_execute(cursor):
    cursor.execute("CREATE TABLE #tvp_no_partial (id INT NOT NULL, name NVARCHAR(50) NULL)")
    with pytest.raises(TypeError, match="Unsupported parameter type"):
        cursor.execute("INSERT INTO #tvp_no_partial (id, name) VALUES (?, ?)", (1, [(2, "b")]))
    cursor.execute("SELECT COUNT(*) FROM #tvp_no_partial")
    assert cursor.fetchone()[0] == 0


def test_stored_proc_with_declared_table_type_rejects_list_argument(cursor):
    """Calling a proc that takes a real user-defined table type with a list fails
    cleanly on the client side; the TVP type itself is valid on the server."""
    type_name = _unique("wb_tvp_type")
    proc_name = _unique("wb_tvp_proc")
    cursor.execute(f"CREATE TYPE dbo.{type_name} AS TABLE (id INT NOT NULL, name NVARCHAR(50) NULL)")
    cursor.execute(
        f"CREATE PROCEDURE dbo.{proc_name} @rows dbo.{type_name} READONLY "
        "AS SELECT COUNT(*) AS n FROM @rows"
    )
    cursor.connection.commit()
    try:
        with pytest.raises(TypeError, match="Unsupported parameter type"):
            cursor.execute(f"EXEC dbo.{proc_name} @rows = ?", ([(1, "a"), (2, "b")],))
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{proc_name}")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.{type_name}")
        cursor.connection.commit()


# ---------------------------------------------------------------------------
# Workaround 1: JSON string parameter + OPENJSON
# ---------------------------------------------------------------------------


def test_openjson_roundtrip_returns_python_types(cursor):
    payload = json.dumps(
        [
            {"id": 1, "name": "alpha", "amt": 12.34},
            {"id": 2, "name": None, "amt": 0.5},
        ]
    )
    cursor.execute(
        "SELECT id, name, amt FROM OPENJSON(?) "
        "WITH (id INT '$.id', name NVARCHAR(50) '$.name', amt DECIMAL(10,2) '$.amt') "
        "ORDER BY id",
        (payload,),
    )
    rows = cursor.fetchall()
    assert [tuple(r) for r in rows] == [(1, "alpha", Decimal("12.34")), (2, None, Decimal("0.50"))]
    assert type(rows[0][0]) is int
    assert type(rows[0][1]) is str
    assert type(rows[0][2]) is Decimal
    assert rows[1][1] is None


def test_openjson_bigint_beyond_js_safe_integer_is_exact(cursor):
    # 2**53 + 1 cannot be represented by a JavaScript number; SQL Server must
    # parse it as BIGINT without precision loss.
    cursor.execute(
        "SELECT id FROM OPENJSON(?) WITH (id BIGINT '$.id')",
        ('[{"id":9007199254740993}]',),
    )
    value = cursor.fetchone()[0]
    assert value == 9007199254740993
    assert type(value) is int


def test_openjson_unicode_and_quote_characters_roundtrip(cursor):
    text = 'Ünïcödé 日本 "double" \'single\' back\\slash'
    payload = json.dumps([{"name": text}])
    cursor.execute("SELECT name FROM OPENJSON(?) WITH (name NVARCHAR(200) '$.name')", (payload,))
    assert cursor.fetchone()[0] == text


def test_openjson_empty_array_yields_zero_rows(cursor):
    cursor.execute("SELECT COUNT(*) FROM OPENJSON(?)", ("[]",))
    assert cursor.fetchone()[0] == 0


def test_openjson_malformed_json_raises_database_error(cursor):
    # Malformed JSON is a server-side error, not a client TypeError.
    with pytest.raises(ProgrammingError, match="JSON text is not properly formatted"):
        cursor.execute("SELECT * FROM OPENJSON(?)", ("[{not json",))
        cursor.fetchall()


def test_long_json_payload_is_not_truncated(cursor):
    """A ~200 KB JSON document must reach the server intact (NVARCHAR(MAX) path)."""
    rows = [{"id": i, "name": f"row-{i:06d}-" + "x" * 20} for i in range(5000)]
    payload = json.dumps(rows)
    assert len(payload) > 150_000
    cursor.execute(
        "SELECT COUNT(*), SUM(id), MAX(LEN(name)) FROM OPENJSON(?) "
        "WITH (id INT '$.id', name NVARCHAR(200) '$.name')",
        (payload,),
    )
    count, total, max_len = cursor.fetchone()
    assert count == 5000
    assert total == sum(range(5000))
    assert max_len == len("row-000000-") + 20


def test_json_bulk_insert_into_temp_table_via_openjson(cursor):
    """Realistic TVP replacement: stage many rows in one parameter, insert them."""
    cursor.execute("CREATE TABLE #tvp_bulk (id INT NOT NULL, name NVARCHAR(50) NULL)")
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 301)]
    cursor.execute(
        "INSERT INTO #tvp_bulk (id, name) "
        "SELECT id, name FROM OPENJSON(?) WITH (id INT '$.id', name NVARCHAR(50) '$.name')",
        (json.dumps(rows),),
    )
    cursor.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM #tvp_bulk")
    assert tuple(cursor.fetchone()) == (300, 1, 300)


def test_json_driven_update_join_applies_per_row_values(cursor):
    cursor.execute("CREATE TABLE #tvp_upd (id INT NOT NULL PRIMARY KEY, qty INT NOT NULL)")
    cursor.execute("INSERT INTO #tvp_upd (id, qty) VALUES (1, 10), (2, 20), (3, 30)")
    payload = json.dumps([{"id": 1, "qty": 100}, {"id": 3, "qty": 300}])
    cursor.execute(
        "UPDATE t SET t.qty = j.qty FROM #tvp_upd AS t "
        "JOIN OPENJSON(?) WITH (id INT '$.id', qty INT '$.qty') AS j ON j.id = t.id",
        (payload,),
    )
    cursor.execute("SELECT id, qty FROM #tvp_upd ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, 100), (2, 20), (3, 300)]


def test_proc_taking_json_nvarchar_max_replaces_tvp(cursor):
    proc_name = _unique("wb_json_proc")
    cursor.execute(
        f"CREATE PROCEDURE dbo.{proc_name} @payload NVARCHAR(MAX) AS "
        "SELECT id, name FROM OPENJSON(@payload) WITH (id INT '$.id', name NVARCHAR(50) '$.name') "
        "ORDER BY id"
    )
    cursor.connection.commit()
    try:
        cursor.execute(
            f"EXEC dbo.{proc_name} @payload = ?",
            (json.dumps([{"id": 2, "name": "b"}, {"id": 1, "name": "a"}]),),
        )
        assert [tuple(r) for r in cursor.fetchall()] == [(1, "a"), (2, "b")]
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.{proc_name}")
        cursor.connection.commit()


# ---------------------------------------------------------------------------
# Workaround 2: delimited string + STRING_SPLIT
# ---------------------------------------------------------------------------


def test_string_split_returns_string_rows(cursor):
    cursor.execute("SELECT value FROM STRING_SPLIT(?, ',') ORDER BY value", ("c,a,b",))
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == ["a", "b", "c"]
    assert all(type(r[0]) is str for r in rows)


def test_string_split_empty_string_yields_one_empty_row(cursor):
    # Classic gotcha: an empty list is one empty string, not zero rows.
    cursor.execute("SELECT value FROM STRING_SPLIT(?, ',')", ("",))
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == ""


def test_string_split_does_not_trim_whitespace(cursor):
    cursor.execute("SELECT value FROM STRING_SPLIT(?, ',')", ("a, b",))
    assert [r[0] for r in cursor.fetchall()] == ["a", " b"]
