"""IDENTITY inserts, sequences and NEXT VALUE FOR, ported from SQLAlchemy's mssql tests.

Adapted from sqlalchemy/sqlalchemy test/dialect/mssql/test_sequence.py,
test/dialect/mssql/test_query.py (IdentityInsertTest) and the autoincrement
cases in test/dialect/mssql/test_types.py (MIT), Copyright 2005-2026 SQLAlchemy
authors and contributors. The MIT notice is in LICENSE_sqlalchemy.txt next to
this file. SQLAlchemy's Sequence() construct is replaced by plain T-SQL
CREATE SEQUENCE statements, since the behaviour under test is the server's.
"""

import uuid
from decimal import Decimal

import pytest
from mssql_python.exceptions import DatabaseError


def _fetch(cursor):
    """All remaining rows as plain tuples (mssql_python Row does not equal a tuple)."""
    return [tuple(r) for r in cursor.fetchall()]


@pytest.fixture
def seq_name(cursor):
    """A unique sequence in dbo, dropped after the test. Caller creates it."""
    name = f"ci_seq_{uuid.uuid4().hex[:10]}"
    yield name
    cursor.execute(f"DROP SEQUENCE IF EXISTS dbo.{name}")
    cursor.connection.commit()


def _scalar(cursor, sql, params=()):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# Sequences (test_sequence.py)
# ---------------------------------------------------------------------------


def test_int_sequence_defaults_to_minimum_int_start(cursor, seq_name):
    cursor.execute(f"CREATE SEQUENCE dbo.{seq_name} AS INT")
    cursor.connection.commit()
    first = _scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}")
    second = _scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}")
    assert first == -(2**31)
    assert second == -(2**31) + 1
    assert isinstance(first, int)


def test_sequence_as_column_default_with_start_and_increment(cursor, table_name, seq_name):
    cursor.execute(f"CREATE SEQUENCE dbo.{seq_name} AS INT START WITH 42 INCREMENT BY 5")
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        f"  id INT NOT NULL DEFAULT (NEXT VALUE FOR dbo.{seq_name}),"
        "  txt NVARCHAR(50))"
    )
    cursor.connection.commit()
    try:
        cursor.executemany(
            f"INSERT INTO dbo.{table_name} (txt) VALUES (?)",
            [("a",), ("b",), ("c",)],
        )
        cursor.connection.commit()
        cursor.execute(f"SELECT id, txt FROM dbo.{table_name} ORDER BY id")
        assert _fetch(cursor) == [(42, "a"), (47, "b"), (52, "c")]
    finally:
        # the column default references the sequence, so the table must go first
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{table_name}")
        cursor.connection.commit()


def test_bigint_sequence_value_beyond_int_range(cursor, seq_name):
    cursor.execute(f"CREATE SEQUENCE dbo.{seq_name} AS BIGINT START WITH 3000000000")
    cursor.connection.commit()
    value = _scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}")
    assert value == 3000000000
    assert type(value) is int


def test_decimal_sequence_returns_decimal(cursor, seq_name):
    cursor.execute(
        f"CREATE SEQUENCE dbo.{seq_name} AS DECIMAL(10,0) START WITH 3000000000"
    )
    cursor.connection.commit()
    value = _scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}")
    assert isinstance(value, Decimal)
    assert value == Decimal("3000000000")


def test_sequence_values_in_one_statement_are_consecutive(cursor, seq_name):
    cursor.execute(f"CREATE SEQUENCE dbo.{seq_name} AS INT START WITH 100")
    cursor.connection.commit()
    cursor.execute(
        f"SELECT NEXT VALUE FOR dbo.{seq_name} OVER (ORDER BY x) "
        "FROM (VALUES (1), (2), (3)) AS v(x) ORDER BY x OPTION (MAXDOP 1)"
    )
    assert [r[0] for r in cursor.fetchall()] == [100, 101, 102]


def test_sequence_cycle_wraps_to_minimum(cursor, seq_name):
    cursor.execute(
        f"CREATE SEQUENCE dbo.{seq_name} AS INT START WITH 1 MINVALUE 1 MAXVALUE 3 CYCLE"
    )
    cursor.connection.commit()
    values = [_scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}") for _ in range(4)]
    assert values == [1, 2, 3, 1]


def test_sequence_exhaustion_without_cycle_raises(cursor, seq_name):
    cursor.execute(
        f"CREATE SEQUENCE dbo.{seq_name} AS TINYINT START WITH 254 MAXVALUE 255 NO CYCLE"
    )
    cursor.connection.commit()
    assert _scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}") == 254
    assert _scalar(cursor, f"SELECT NEXT VALUE FOR dbo.{seq_name}") == 255
    with pytest.raises(DatabaseError):
        cursor.execute(f"SELECT NEXT VALUE FOR dbo.{seq_name}")
        cursor.fetchall()
    assert _scalar(cursor, "SELECT 1") == 1


# ---------------------------------------------------------------------------
# IDENTITY columns (IdentityInsertTest, autoincrement tests in test_types.py)
# ---------------------------------------------------------------------------


def test_identity_insert_executemany_then_auto_values_continue(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        "  id INT IDENTITY(1,1) PRIMARY KEY, description NVARCHAR(50))"
    )
    cursor.connection.commit()
    cursor.execute(f"SET IDENTITY_INSERT dbo.{table_name} ON")
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (id, description) VALUES (?, ?)",
        [(89, "Python"), (8, "Ruby"), (3, "Perl"), (1, "Java")],
    )
    cursor.execute(f"SET IDENTITY_INSERT dbo.{table_name} OFF")
    # identity reseeds to the largest explicit value, so auto values resume at 90
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (description) VALUES (?)",
        [("PHP",), ("Smalltalk",)],
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT id, description FROM dbo.{table_name} ORDER BY id")
    assert _fetch(cursor) == [
        (1, "Java"),
        (3, "Perl"),
        (8, "Ruby"),
        (89, "Python"),
        (90, "PHP"),
        (91, "Smalltalk"),
    ]


def test_identity_insert_is_single_table_per_session(cursor, table_name):
    other = f"{table_name}_b"
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT IDENTITY PRIMARY KEY)")
    cursor.execute(f"CREATE TABLE dbo.{other} (id INT IDENTITY PRIMARY KEY)")
    cursor.connection.commit()
    try:
        cursor.execute(f"SET IDENTITY_INSERT dbo.{table_name} ON")
        # SQL Server allows IDENTITY_INSERT ON for only one table per session
        with pytest.raises(DatabaseError):
            cursor.execute(f"SET IDENTITY_INSERT dbo.{other} ON")
        cursor.execute(f"SET IDENTITY_INSERT dbo.{table_name} OFF")
        cursor.execute(f"SET IDENTITY_INSERT dbo.{other} ON")
        cursor.execute(f"SET IDENTITY_INSERT dbo.{other} OFF")
        cursor.connection.commit()
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{other}")
        cursor.connection.commit()


def test_identity_on_second_column_of_composite_key(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        "  int_n INT NOT NULL DEFAULT 0,"
        "  int_y INT IDENTITY(1,1) NOT NULL,"
        "  txt NVARCHAR(20),"
        f"  PRIMARY KEY (int_n, int_y))"
    )
    cursor.connection.commit()
    cursor.execute(f"INSERT INTO dbo.{table_name} (txt) VALUES (?)", ("first",))
    cursor.execute(f"INSERT INTO dbo.{table_name} (txt) VALUES (?)", ("second",))
    cursor.connection.commit()
    cursor.execute(f"SELECT int_n, int_y, txt FROM dbo.{table_name} ORDER BY int_y")
    assert _fetch(cursor) == [(0, 1, "first"), (0, 2, "second")]


def test_scope_identity_on_numeric_identity_returns_decimal(cursor, table_name):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} ("
        "  id NUMERIC(18,0) IDENTITY(1,1) PRIMARY KEY, description NVARCHAR(50))"
    )
    cursor.connection.commit()
    # SCOPE_IDENTITY is only visible inside the same batch as the INSERT
    cursor.execute(
        f"INSERT INTO dbo.{table_name} (description) VALUES (?); SELECT SCOPE_IDENTITY();",
        ("T-SQL",),
    )
    assert cursor.nextset()
    new_id = cursor.fetchone()[0]
    cursor.connection.commit()
    assert isinstance(new_id, Decimal)
    assert new_id == Decimal("1")
    cursor.execute(f"SELECT id FROM dbo.{table_name}")
    assert _fetch(cursor) == [(Decimal("1"),)]
