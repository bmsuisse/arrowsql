"""Datatype round-trips and errors, ported from SQLAlchemy's mssql type tests.

Adapted from sqlalchemy/sqlalchemy test/dialect/mssql/test_types.py
(TypeRoundTripTest.test_decimal_notation, BinaryTest, the NTEXT and
size-boundary cases of StringRoundTripTest) (MIT), Copyright 2005-2026
SQLAlchemy authors and contributors. The MIT notice is in
LICENSE_sqlalchemy.txt next to this file. Values and expectations were
translated to direct T-SQL and mssql_python calls; the SQLAlchemy fixtures were
not used.
"""

from decimal import Decimal

import pytest
from mssql_python.exceptions import DatabaseError

# A subset of the values from SQLAlchemy's test_decimal_notation; all fit NUMERIC(38,20).
DECIMAL_NOTATION_VALUES = [
    "1500000.00000000000000000000",
    "-1500000.00000000000000000000",
    "0.0000000000000000002",
    "-2E-2",
    "156666.458923543",
    "1E-20",
    "-1203E-20",
    "1.521E+15",
    "-02452E-2",
    "000000000000.32E12",
    "-0.00",
]



def _fetch(cursor):
    """All remaining rows as plain tuples (mssql_python Row does not equal a tuple)."""
    return [tuple(r) for r in cursor.fetchall()]


@pytest.mark.parametrize("text", DECIMAL_NOTATION_VALUES)
def test_numeric_38_20_decimal_notation_round_trip(cursor, table_name, text):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT IDENTITY PRIMARY KEY, numericcol NUMERIC(38,20))"
    )
    cursor.connection.commit()
    value = Decimal(text)
    cursor.execute(f"INSERT INTO dbo.{table_name} (numericcol) VALUES (?)", (value,))
    cursor.connection.commit()
    cursor.execute(f"SELECT numericcol FROM dbo.{table_name} WHERE numericcol = ?", (value,))
    row = cursor.fetchone()
    assert row is not None, f"{text} did not compare equal to itself"
    assert isinstance(row[0], Decimal)
    assert row[0] == value


def test_ntext_round_trip_of_long_text(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, body NTEXT)")
    cursor.connection.commit()
    body = "réve🐍illé " * 1200  # ~10k UTF-16 units, well past NVARCHAR(4000)
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, body) VALUES (?, ?)", (1, body))
    cursor.connection.commit()
    cursor.execute(f"SELECT body FROM dbo.{table_name} WHERE id = ?", (1,))
    assert cursor.fetchone()[0] == body


def test_ntext_cannot_be_used_with_equals(cursor, table_name):
    """NTEXT is not comparable with '='; SQLAlchemy steers users to NVARCHAR(MAX)."""
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, body NTEXT)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, body) VALUES (1, N'abc')")
    cursor.connection.commit()
    with pytest.raises(DatabaseError):
        cursor.execute(f"SELECT id FROM dbo.{table_name} WHERE body = ?", ("abc",))
        cursor.fetchall()
    # the same predicate written with LIKE is the supported form
    cursor.execute(f"SELECT id FROM dbo.{table_name} WHERE body LIKE ?", ("abc",))
    assert _fetch(cursor) == [(1,)]


def test_varbinary_max_round_trip_with_embedded_zero_bytes(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, data VARBINARY(MAX))")
    cursor.connection.commit()
    data = b"\x00\x00\x01" + bytes(range(256)) * 256 + b"\x00"  # ~64 KiB
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, data) VALUES (?, ?)", (1, data))
    cursor.connection.commit()
    cursor.execute(f"SELECT data FROM dbo.{table_name} WHERE id = ?", (1,))
    got = cursor.fetchone()[0]
    assert isinstance(got, bytes)
    assert got == data


def test_varbinary_fixed_size_overflow_raises(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, data VARBINARY(4))")
    cursor.connection.commit()
    with pytest.raises(DatabaseError):
        cursor.execute(
            f"INSERT INTO dbo.{table_name} (id, data) VALUES (?, ?)",
            (1, b"12345"),
        )
        cursor.connection.commit()
    cursor.connection.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


@pytest.mark.parametrize("length", [1999, 2000, 2001, 4000, 4001])
def test_nvarchar_max_boundary_lengths_compare_and_filter(cursor, table_name, length):
    """Lengths around NVARCHAR's 4000-character inline limit round-trip and filter."""
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, data NVARCHAR(MAX))")
    cursor.connection.commit()
    unit = "réve🐍illé"
    data = (unit * (length // len(unit) + 2))[: length - 1] + "Z"
    other = "some other data"
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (id, data) VALUES (?, ?)",
        [(1, data), (2, other)],
    )
    cursor.connection.commit()
    cursor.execute(f"SELECT data FROM dbo.{table_name} WHERE data = ?", (data,))
    assert _fetch(cursor) == [(data,)]
    cursor.execute(f"SELECT id FROM dbo.{table_name} WHERE data <> ? ORDER BY id", (data,))
    assert _fetch(cursor) == [(2,)]
