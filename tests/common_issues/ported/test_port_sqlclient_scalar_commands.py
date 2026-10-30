"""Scalar execution, conversion errors in transactions, and stored-procedure
name and binary-parameter edge cases ported from dotnet/SqlClient manual tests.

Adapted from dotnet/SqlClient src/Microsoft.Data.SqlClient/tests/ManualTests/SQL/
SqlCommand/SqlCommandExecuteScalarTest.cs (GH3736 cases),
SqlCommand/SqlCommandStoredProcTest.cs, and SqlCommand/SqlCommandSetTest.cs
(MIT), Copyright (c) .NET Foundation. All rights reserved. Also Copyright (c) Microsoft Corporation.
The MIT license text is in LICENSE_dotnet_sqlclient.txt next to this file.

ExecuteScalar is expressed as execute() followed by fetchone(). The .NET
ArgumentException for an over-long CommandText is not part of the DB-API surface,
so the stored-procedure case asserts only the not-found error. SqlCommandSet
(an internal batching API) is replaced by a plain EXEC of a procedure with a
varbinary parameter.
"""

import uuid

import pytest
from mssql_python import DataError, ProgrammingError


@pytest.fixture
def scratch(conn):
    """Yield a unique dbo table name and drop it (with its rows) on teardown."""
    cur = conn.cursor()
    names = []

    def make(ddl):
        name = f"ci_sc_{uuid.uuid4().hex[:12]}"
        cur.execute(f"CREATE TABLE dbo.{name} ({ddl})")
        conn.commit()
        names.append(name)
        return f"dbo.{name}"

    yield make
    for name in reversed(names):
        cur.execute(f"DROP TABLE IF EXISTS dbo.{name}")
    conn.commit()
    cur.close()


def _conversion_fixture(cursor, conn, scratch):
    """Two rows: '12345' converts to int, '42-43' does not (Msg 245 when read as int)."""
    src = scratch("Id INT IDENTITY(1,1) NOT NULL, Val VARCHAR(10) NOT NULL")
    cursor.execute(f"INSERT INTO {src} (Val) VALUES ('12345'), ('42-43')")
    conn.commit()
    return src


# ---------------------------------------------------------------------------
# ExecuteScalar: plain values, empty results, NULL
# ---------------------------------------------------------------------------


def test_select_scalar_returns_value(cursor):
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42


def test_scalar_on_empty_result_returns_none(cursor):
    cursor.execute("SELECT 1 WHERE 1 = 0")
    assert cursor.fetchone() is None


def test_scalar_null_value_is_none(cursor):
    cursor.execute("SELECT CAST(NULL AS INT)")
    assert cursor.fetchone()[0] is None


# ---------------------------------------------------------------------------
# GH3736: conversion errors (Msg 245) raised while reading a result
# ---------------------------------------------------------------------------


def test_conversion_error_on_single_row_raises_data_error(cursor, conn, scratch):
    src = _conversion_fixture(cursor, conn, scratch)
    with pytest.raises(DataError, match="Conversion failed"):
        cursor.execute(f"SELECT CONVERT(INT, Val) FROM {src} WHERE Id = 2")
        cursor.fetchone()


def test_conversion_error_on_later_row_surfaces_after_first_row(cursor, conn, scratch):
    src = _conversion_fixture(cursor, conn, scratch)
    cursor.execute(f"SELECT CONVERT(INT, Val) FROM {src} ORDER BY Id")
    assert cursor.fetchone()[0] == 12345
    with pytest.raises(DataError, match="Conversion failed"):
        cursor.fetchall()


def test_conversion_error_in_transaction_rolls_back_all_work(cursor, conn, scratch):
    src = _conversion_fixture(cursor, conn, scratch)
    target = scratch("Id INT IDENTITY(1,1) NOT NULL, Val1 INT NOT NULL, Val2 INT NOT NULL")
    cursor.execute(f"INSERT INTO {target} (Val1, Val2) VALUES (42, 43)")
    with pytest.raises(DataError, match="Conversion failed"):
        cursor.execute(f"SELECT CONVERT(INT, Val) FROM {src} WHERE Id = 2")
        cursor.fetchone()
    cursor.execute(f"INSERT INTO {target} (Val1, Val2) VALUES (100, 200)")
    conn.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM {target}")
    assert cursor.fetchone()[0] == 0


def test_connection_is_usable_after_conversion_error(cursor, conn, scratch):
    src = _conversion_fixture(cursor, conn, scratch)
    with pytest.raises(DataError):
        cursor.execute(f"SELECT CONVERT(INT, Val) FROM {src} WHERE Id = 2")
        cursor.fetchone()
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42


# ---------------------------------------------------------------------------
# SqlCommandStoredProcTest: unknown procedure names, including control characters
# ---------------------------------------------------------------------------

_ODD_PROC_NAME = (
    "random text\x00䀊猀琀愀琀攀洀攀渀琀"
    "\x00\x06ǿऀ\x00ￜ\x01"
)


def test_unknown_stored_procedure_raises_not_found(cursor):
    with pytest.raises(ProgrammingError, match="Could not find stored procedure"):
        cursor.execute("EXEC [dbo].[ci_no_such_proc_" + uuid.uuid4().hex[:10] + "]")


def test_unknown_stored_procedure_with_odd_characters_is_a_programming_error(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("EXEC " + _ODD_PROC_NAME)


def test_unknown_stored_procedure_error_leaves_cursor_usable(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("EXEC [dbo].[ci_no_such_proc_" + uuid.uuid4().hex[:10] + "]")
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# SqlCommandSetTest: a varbinary parameter bound to a stored procedure round-trips
# ---------------------------------------------------------------------------


@pytest.fixture
def bytes_proc(conn):
    cur = conn.cursor()
    token = uuid.uuid4().hex[:10]
    table, proc = f"ci_bin_{token}", f"ci_binp_{token}"
    cur.execute(f"CREATE TABLE dbo.{table} (ByteArrayColumn VARBINARY(MAX) NULL)")
    cur.execute(
        f"CREATE PROCEDURE dbo.{proc} @array VARBINARY(MAX) AS "
        f"BEGIN SET NOCOUNT ON; INSERT INTO dbo.{table} (ByteArrayColumn) VALUES (@array) END"
    )
    conn.commit()
    yield table, proc
    cur.execute(f"DROP PROCEDURE IF EXISTS dbo.{proc}")
    cur.execute(f"DROP TABLE IF EXISTS dbo.{table}")
    conn.commit()
    cur.close()


def test_varbinary_param_through_proc_round_trips(cursor, conn, bytes_proc):
    table, proc = bytes_proc
    payload = bytes([1, 2, 3])
    cursor.execute(f"EXEC dbo.{proc} ?", [payload])
    conn.commit()
    cursor.execute(f"SELECT ByteArrayColumn FROM dbo.{table}")
    rows = cursor.fetchall()
    assert [bytes(r[0]) for r in rows] == [payload]


@pytest.mark.parametrize("size", [0, 1, 8000, 65536], ids=lambda n: f"{n}B")
def test_varbinary_max_param_sizes_round_trip(cursor, conn, bytes_proc, size):
    table, proc = bytes_proc
    payload = bytes((i * 31 + 7) % 256 for i in range(size))
    cursor.execute(f"EXEC dbo.{proc} ?", [payload])
    conn.commit()
    cursor.execute(f"SELECT ByteArrayColumn FROM dbo.{table}")
    assert [bytes(r[0]) for r in cursor.fetchall()] == [payload]
