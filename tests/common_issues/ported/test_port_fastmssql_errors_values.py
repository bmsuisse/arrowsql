"""Error taxonomy, recovery and value edge cases ported from FastMssql's error-handling suite.

Adapted from Rivendael/FastMssql tests/test_error_handling.py (MIT), Copyright (c) 2025 Rivendael.
MIT permission notice: see tests/common_issues/ported/test_port_fastmssql_async_threads.py,
which reproduces the upstream LICENSE text for this same project.

The upstream tests use broad ``pytest.raises(Exception)`` checks and string-concatenated SQL.
These versions assert the specific DB-API exception class for each failure, check that the
connection and any earlier work survive, and bind every value as a ? parameter.
"""

import pytest
from mssql_python.exceptions import DataError, IntegrityError, InterfaceError, ProgrammingError


def _recovered(cursor):
    cursor.execute("SELECT N'still working' AS status")
    return cursor.fetchone()[0]


def test_syntax_error_is_programming_error_and_connection_recovers(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("INVALID SQL STATEMENT")

    assert _recovered(cursor) == "still working"


def test_missing_table_is_programming_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT * FROM dbo.no_such_table_for_fastmssql_port")

    assert _recovered(cursor) == "still working"


def test_insert_into_missing_table_with_params_is_programming_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("INSERT INTO dbo.no_such_table_for_fastmssql_port VALUES (?, ?)", 1, "x")


@pytest.mark.parametrize(
    "ddl, good, bad",
    [
        ("id INT PRIMARY KEY", ("INSERT INTO {t} VALUES (?)", (1,)), ("INSERT INTO {t} VALUES (?)", (1,))),
        (
            "email NVARCHAR(100) UNIQUE NOT NULL",
            ("INSERT INTO {t} VALUES (?)", ("a@x",)),
            ("INSERT INTO {t} VALUES (?)", ("a@x",)),
        ),
        (
            "age INT CHECK (age >= 0 AND age <= 150)",
            ("INSERT INTO {t} VALUES (?)", (25,)),
            ("INSERT INTO {t} VALUES (?)", (200,)),
        ),
        (
            "category NVARCHAR(20) NOT NULL",
            ("INSERT INTO {t} VALUES (?)", ("A",)),
            ("INSERT INTO {t} VALUES (?)", (None,)),
        ),
        (
            "id INT PRIMARY KEY, parent INT NULL REFERENCES {t}(id)",
            ("INSERT INTO {t} (id) VALUES (?)", (1,)),
            ("INSERT INTO {t} (id, parent) VALUES (?, ?)", (2, 999)),
        ),
    ],
    ids=["primary-key", "unique", "check", "not-null", "foreign-key"],
)
def test_constraint_violation_is_integrity_error(cursor, table_name, ddl, good, bad):
    qualified = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {qualified} ({ddl.replace('{t}', qualified)})")
    cursor.connection.commit()
    cursor.execute(good[0].format(t=qualified), *good[1])
    cursor.connection.commit()

    with pytest.raises(IntegrityError):
        cursor.execute(bad[0].format(t=qualified), *bad[1])
    cursor.connection.rollback()

    cursor.execute(f"SELECT COUNT(*) FROM {qualified}")
    assert cursor.fetchone()[0] == 1


def test_statement_error_inside_transaction_keeps_earlier_work_until_commit(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY)")
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?)", 1)
    with pytest.raises(IntegrityError):
        cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?)", 1)
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?)", 2)
    cursor.connection.commit()

    cursor.execute(f"SELECT id FROM dbo.{table_name} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [1, 2]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CAST('invalid-date' AS DATETIME)",
        "SELECT CAST(9999999999999999999999999999999999999 AS INT)",
        "SELECT CAST('not-a-number' AS INT)",
        "SELECT CAST(1 AS INT) / 0",
    ],
    ids=["bad-datetime", "int-overflow", "bad-int", "divide-by-zero"],
)
def test_value_conversion_failure_is_data_error_and_connection_recovers(cursor, sql):
    with pytest.raises(DataError):
        cursor.execute(sql)

    assert _recovered(cursor) == "still working"


def test_parameter_count_mismatch_is_programming_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT ?, ?", 1)


def test_closed_cursor_and_closed_connection_raise_driver_errors(conn_str):
    from mssql_python import connect

    connection = connect(conn_str)
    cursor = connection.cursor()
    cursor.close()
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT 1")

    connection.close()
    with pytest.raises(InterfaceError):
        connection.cursor()
    with pytest.raises(InterfaceError):
        connection.commit()


SPECIAL_STRINGS = [
    "Single 'quotes' in text",
    'Double "quotes" in text',
    "Mixed 'single' and \"double\" quotes",
    "Unicode: café, naïve, résumé, 北京",
    "Newlines\nand\ttabs",
    "Backslashes \\ and forward /slashes/",
    "SQL injection attempt: '; DROP TABLE test; --",
    "Percent % and underscore _ wildcards",
    "XML-like: <tag>content</tag>",
    'JSON-like: {"key": "value"}',
    "Emoji: 😀🎉📊",
]


def test_special_characters_round_trip_as_bound_parameters(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT IDENTITY PRIMARY KEY, text_data NVARCHAR(500))")
    cursor.connection.commit()
    for text in SPECIAL_STRINGS:
        cursor.execute(f"INSERT INTO dbo.{table_name} (text_data) VALUES (?)", text)
    cursor.connection.commit()

    cursor.execute(f"SELECT text_data FROM dbo.{table_name} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == SPECIAL_STRINGS


@pytest.mark.upstream_bug(
    reason=(
        "executemany silently replaces non-ASCII characters with '?' when the batch also "
        "holds another string containing '%' (or quotes): 'Unicode: café, naïve, résumé, 北京' "
        "comes back as '...??'. Each value inserts correctly through execute(), so the bug is in "
        "the batch parameter buffer. No error is raised, so the data is corrupted silently."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_batch_keeps_non_ascii_text_exact(cursor, table_name):
    values = ["Unicode: café, naïve, résumé, 北京", "Percent % and underscore _ wildcards"]
    cursor.execute(f"CREATE TABLE dbo.{table_name} (t NVARCHAR(500))")
    cursor.connection.commit()
    cursor.executemany(f"INSERT INTO dbo.{table_name} (t) VALUES (?)", [(v,) for v in values])
    cursor.connection.commit()

    cursor.execute(f"SELECT t FROM dbo.{table_name} ORDER BY t")
    assert sorted(r[0] for r in cursor.fetchall()) == sorted(values)


@pytest.mark.parametrize(
    "sql_type, value",
    [
        ("INT", -2147483648),
        ("INT", 2147483647),
        ("SMALLINT", -32768),
        ("SMALLINT", 32767),
        ("TINYINT", 0),
        ("TINYINT", 255),
        ("BIGINT", -9223372036854775808),
        ("BIGINT", 9223372036854775807),
    ],
)
def test_integer_boundaries_round_trip(cursor, sql_type, value):
    cursor.execute(f"SELECT CAST(? AS {sql_type})", value)
    assert cursor.fetchone()[0] == value


def test_nvarchar_4000_boundary_round_trips_exact_length(cursor):
    value = "é" * 4000
    cursor.execute("SELECT LEN(CAST(? AS NVARCHAR(4000))), CAST(? AS NVARCHAR(4000))", value, value)
    length, echoed = cursor.fetchone()

    assert length == 4000
    assert echoed == value


def test_empty_result_has_description_and_no_rows(cursor):
    cursor.execute("SELECT 1 AS test WHERE 1 = 0")

    assert [d[0] for d in cursor.description] == ["test"]
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []
