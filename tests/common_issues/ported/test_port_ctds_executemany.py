"""executemany parameter scanning, identity inserts, result sets and error paths.

Adapted from zillow/ctds tests/test_cursor_executemany.py (MIT, Copyright (c) 2016
Zillow); the license text is in LICENSE_ctds.txt next to this file. Escaping
cases use qmark placeholders (mssql_python's paramstyle) instead of ctds'
numeric/named forms, and the ctds COMPUTE-clause case is omitted because
COMPUTE is removed from current SQL Server compatibility levels.
"""

import uuid

import pytest
from mssql_python import InterfaceError, ProgrammingError


def _name(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# Placeholder scanning: '?' inside SQL text is not a parameter
# ---------------------------------------------------------------------------


def test_question_mark_inside_string_literal_is_not_a_placeholder(cursor):
    cursor.executemany("SELECT '?' AS lit, ? AS p", [(1,), (2,)])
    rows = [tuple(r) for r in cursor.fetchall()]
    assert rows == [("?", 1)]


def test_question_mark_inside_line_comment_is_not_a_placeholder(cursor):
    cursor.executemany(
        "SELECT ? AS p -- what is ? here\n",
        [(41,)],
    )
    assert [tuple(r) for r in cursor.fetchall()] == [(41,)]


@pytest.mark.parametrize(
    "value",
    ["what?", "a'b?c", ":0", "'' foo '' ? ''"],
)
def test_parameter_values_containing_placeholder_syntax_are_data(conn, cursor, value):
    name = _name("ctds_exm_lit")
    cursor.execute(f"CREATE TABLE #{name} (s NVARCHAR(100))")
    cursor.executemany(f"INSERT INTO #{name} (s) VALUES (?)", [(value,), (value,)])
    cursor.execute(f"SELECT s FROM #{name}")
    assert [tuple(r) for r in cursor.fetchall()] == [(value,), (value,)]


# ---------------------------------------------------------------------------
# Identity inserts in batches
# ---------------------------------------------------------------------------


def test_identity_insert_batch_keeps_explicit_values(conn, cursor):
    name = _name("ctds_exm_ident")
    try:
        cursor.execute(f"CREATE TABLE dbo.{name} (ident INT IDENTITY(1,1) NOT NULL, v INT)")
        cursor.execute(f"SET IDENTITY_INSERT dbo.{name} ON")
        cursor.executemany(
            f"INSERT INTO dbo.{name} (ident, v) VALUES (?, ?)",
            [(ix, ix * 10) for ix in range(1, 11)],
        )
        cursor.execute(f"SET IDENTITY_INSERT dbo.{name} OFF")
        conn.commit()
        cursor.execute(f"SELECT ident, v FROM dbo.{name} ORDER BY ident")
        rows = [tuple(r) for r in cursor.fetchall()]
        assert rows == [(ix, ix * 10) for ix in range(1, 11)]
    finally:
        cursor.execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name}")
        conn.commit()


def test_rollback_after_identity_insert_batch_removes_rows(conn, cursor):
    name = _name("ctds_exm_idrb")
    try:
        cursor.execute(f"CREATE TABLE dbo.{name} (ident INT IDENTITY(1,1) NOT NULL)")
        conn.commit()
        cursor.execute(f"SET IDENTITY_INSERT dbo.{name} ON")
        cursor.executemany(f"INSERT INTO dbo.{name} (ident) VALUES (?)", [(i,) for i in range(5)])
        cursor.execute(f"SET IDENTITY_INSERT dbo.{name} OFF")
        conn.rollback()
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{name}")
        assert cursor.fetchone()[0] == 0
    finally:
        cursor.execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name}")
        conn.commit()


# ---------------------------------------------------------------------------
# Argument and state errors
# ---------------------------------------------------------------------------


def test_executemany_without_parameter_sequence_is_type_error(cursor):
    with pytest.raises(TypeError):
        cursor.executemany("SELECT ?")


def test_executemany_on_closed_cursor_raises(conn):
    cur = conn.cursor()
    cur.close()
    with pytest.raises((InterfaceError, ProgrammingError)):
        cur.executemany("SELECT ?", [(1,)])


def test_executemany_syntax_error_raises_programming_error_and_connection_survives(cursor):
    with pytest.raises(ProgrammingError):
        cursor.executemany("SELEC ? FROM", [(1,)])
    cursor.execute("SELECT 1")
    assert tuple(cursor.fetchone()) == (1,)


def test_executemany_raiserror_surfaces_as_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.executemany("RAISERROR('ctds port error %d', 16, 1); SELECT ?", [(1,)])
    cursor.execute("SELECT 2")
    assert tuple(cursor.fetchone()) == (2,)


def test_executemany_empty_sequence_executes_nothing(conn, cursor):
    name = _name("ctds_exm_empty")
    cursor.execute(f"CREATE TABLE #{name} (i INT)")
    cursor.executemany(f"INSERT INTO #{name} (i) VALUES (?)", [])
    assert cursor.rowcount == 0
    cursor.execute(f"SELECT COUNT(*) FROM #{name}")
    assert cursor.fetchone()[0] == 0
