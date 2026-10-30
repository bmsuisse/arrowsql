"""XACT_ABORT dead-transaction guard and uniqueidentifier edges, ported from microsoft/go-mssqldb.

Adapted from microsoft/go-mssqldb xact_abort_test.go (regression for issue #244)
and uniqueidentifier_test.go (BSD-3-Clause, Copyright (c) 2012 The Go Authors;
Copyright (c) Microsoft Corporation). The license text is in
LICENSE_go_mssqldb.txt next to this file. The Go unit tests of the Go-specific
UniqueIdentifier type (Scan, MarshalText, UnmarshalJSON) are not ported; their
byte-order and parse-failure cases are re-expressed against the server.
"""

import uuid

import pytest
from mssql_python.exceptions import DatabaseError, IntegrityError


def _create_pk_table(cursor, name):
    cursor.execute(f"CREATE TABLE dbo.{name} (id INT PRIMARY KEY, val VARCHAR(50))")
    cursor.execute(f"INSERT INTO dbo.{name} (id, val) VALUES (1, 'existing')")
    cursor.connection.commit()


def _count(cursor, name):
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{name}")
    return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# XACT_ABORT ON (xact_abort_test.go)
# ---------------------------------------------------------------------------


def test_xact_abort_on_duplicate_key_surfaces_integrity_error(conn, cursor, table_name):
    _create_pk_table(cursor, table_name)
    cur = conn.cursor()
    cur.execute("SET XACT_ABORT ON")
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (2, 'ok')")
    with pytest.raises(IntegrityError) as info:
        cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (1, 'dup')")
    assert "PRIMARY KEY" in str(info.value)
    conn.rollback()
    assert _count(cursor, table_name) == 1


@pytest.mark.upstream_bug(
    reason=(
        "After XACT_ABORT rolls the transaction back server-side, the next statement "
        "runs outside the user's transaction and is committed by a later commit(). "
        "go-mssqldb (checkServerAbortedTransaction) rejects it instead."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_xact_abort_on_rejects_statement_after_server_rollback(conn, cursor, table_name):
    _create_pk_table(cursor, table_name)
    cur = conn.cursor()
    cur.execute("SET XACT_ABORT ON")
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (2, 'ok')")
    with pytest.raises(IntegrityError):
        cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (1, 'dup')")
    with pytest.raises(DatabaseError):
        cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (3, 'after')")
    conn.commit()
    assert _count(cursor, table_name) == 1


def test_xact_abort_on_dead_transaction_then_new_transaction_works(conn, cursor, table_name):
    _create_pk_table(cursor, table_name)
    cur = conn.cursor()
    cur.execute("SET XACT_ABORT ON")
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (2, 'ok')")
    with pytest.raises(IntegrityError):
        cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (1, 'dup')")
    conn.rollback()
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (5, 'fresh')")
    conn.commit()
    assert _count(cursor, table_name) == 2


def test_xact_abort_off_keeps_transaction_alive_after_duplicate_key(conn, cursor, table_name):
    # Contrast case: without XACT_ABORT only the failing statement is rolled back.
    _create_pk_table(cursor, table_name)
    cur = conn.cursor()
    cur.execute("SET XACT_ABORT OFF")
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (2, 'ok')")
    with pytest.raises(IntegrityError):
        cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (1, 'dup')")
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (3, 'after')")
    conn.commit()
    cursor.execute(f"SELECT id FROM dbo.{table_name} ORDER BY id")
    assert [r[0] for r in cursor.fetchall()] == [1, 2, 3]


def test_xact_abort_on_with_rollback_on_error_in_explicit_transaction(conn, cursor, table_name):
    _create_pk_table(cursor, table_name)
    cur = conn.cursor()
    cur.execute("SET XACT_ABORT ON")
    cur.execute("BEGIN TRANSACTION")
    cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (7, 'inner')")
    with pytest.raises(IntegrityError):
        cur.execute(f"INSERT INTO dbo.{table_name} (id, val) VALUES (1, 'dup')")
    conn.rollback()
    assert _count(cursor, table_name) == 1


# ---------------------------------------------------------------------------
# uniqueidentifier (uniqueidentifier_test.go)
# ---------------------------------------------------------------------------

# Mixed-endian layout: first three groups little-endian, last two big-endian.
GUID_TEXT = "01234567-89AB-CDEF-0123-456789ABCDEF"
GUID_WIRE = bytes.fromhex("67452301" "AB89" "EFCD" "0123456789ABCDEF")


def test_uniqueidentifier_binary_layout_is_mixed_endian(cursor):
    cursor.execute("SELECT CONVERT(BINARY(16), CAST(? AS UNIQUEIDENTIFIER))", (GUID_TEXT,))
    assert bytes(cursor.fetchone()[0]) == GUID_WIRE


def test_uniqueidentifier_binary_parameter_reads_back_as_same_guid(cursor):
    cursor.execute("SELECT CAST(? AS UNIQUEIDENTIFIER)", (GUID_WIRE,))
    got = cursor.fetchone()[0]
    assert str(got).upper() == GUID_TEXT


@pytest.mark.parametrize(
    "bad",
    [
        "too-short",
        "ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ",
        "0123456789ABCDEF0123456789ABCDEFXX",
    ],
)
def test_uniqueidentifier_invalid_string_raises_and_connection_survives(conn, bad):
    cur = conn.cursor()
    with pytest.raises(DatabaseError):
        cur.execute("SELECT CAST(? AS UNIQUEIDENTIFIER)", (bad,))
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1


def test_uniqueidentifier_null_parameter_roundtrips_as_none(cursor):
    cursor.execute("SELECT CAST(? AS UNIQUEIDENTIFIER)", (None,))
    assert cursor.fetchone()[0] is None


def test_uniqueidentifier_nil_and_max_guid_roundtrip(cursor):
    nil = uuid.UUID(int=0)
    ones = uuid.UUID(int=2**128 - 1)
    cursor.execute("SELECT CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER)", (str(nil), str(ones)))
    got_nil, got_ones = cursor.fetchone()
    assert str(got_nil).lower() == str(nil)
    assert str(got_ones).lower() == str(ones)
    assert str(got_nil).lower() == "00000000-0000-0000-0000-000000000000"


def test_uniqueidentifier_ordering_is_by_server_byte_order(cursor):
    # SQL Server orders GUIDs by their mixed-endian bytes, not by text.
    low = "00000000-0000-0000-0000-000000000001"
    high = "01000000-0000-0000-0000-000000000000"
    cursor.execute(
        "SELECT v FROM (VALUES (CAST(? AS UNIQUEIDENTIFIER)), (CAST(? AS UNIQUEIDENTIFIER))) AS t(v) ORDER BY v",
        (high, low),
    )
    # Bytes 10-15 are compared first, so the all-zero prefix of `high` sorts before `low`.
    assert [str(r[0]).lower() for r in cursor.fetchall()] == [high, low]
