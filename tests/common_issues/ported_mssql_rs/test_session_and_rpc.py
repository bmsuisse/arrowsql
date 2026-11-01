# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Behaviour adapted from microsoft/mssql-rs (MIT): mssql-tds/tests/{test_reset_connection,
# timeout_and_cancel,test_info_messages,test_narrow_param_encoding,test_rpc_datatypes,
# test_rpc_results,test_cursor_ops,test_large_value_read,test_client_transactions,
# test_login_server_name,test_encrypt_for_connections,test_drain_on_error}.rs and
# mssql-py-core/tests/test_{connection,cursor,integration}.py.
"""Session reset, timeouts/cancel, info messages, parameters and results (real SQL Server)."""

import datetime as dt
import decimal
import threading
import time
import uuid

import pytest

import mssql_python

D = decimal.Decimal


@pytest.fixture
def pooled():
    """Explicit pooling for one test; afterwards back to the never-configured state so
    later tests see the default auto-enable behaviour."""
    from mssql_python.pooling import PoolingManager

    mssql_python.pooling(max_size=2, idle_timeout=60)
    yield
    mssql_python.pooling(enabled=False)
    PoolingManager._reset_for_testing()


# --- reset_connection -------------------------------------------------------------------

def test_pooled_connection_is_reset_between_uses(conn_str, pooled):
    """test_reset_connection.rs: session state must not leak across pool checkouts."""
    c1 = mssql_python.connect(conn_str)
    cur = c1.cursor()
    spid = cur.execute("SELECT @@SPID").fetchone()[0]
    cur.execute("SET LANGUAGE German")
    cur.execute("SET DATEFIRST 3")
    cur.execute("CREATE TABLE #leak (x INT)")
    c1.commit()
    c1.close()

    c2 = mssql_python.connect(conn_str)
    cur = c2.cursor()
    row = cur.execute("SELECT @@SPID, @@LANGUAGE, @@DATEFIRST").fetchone()
    cur.execute("SELECT OBJECT_ID('tempdb..#leak')")
    leaked = cur.fetchone()[0]
    c2.close()
    assert row[1] == "us_english", "SET LANGUAGE leaked through the pool"
    assert row[2] == 7, "SET DATEFIRST leaked through the pool"
    assert leaked is None, "temp table leaked through the pool"
    # not asserted: whether the physical session (spid) was reused
    assert isinstance(spid, int)


def test_open_transaction_is_rolled_back_when_connection_returns_to_pool(conn_str, table_name, pooled):
    setup = mssql_python.connect(conn_str, autocommit=True)
    setup.cursor().execute(f"CREATE TABLE dbo.{table_name} (x INT)")
    c1 = mssql_python.connect(conn_str)  # autocommit off
    c1.cursor().execute(f"INSERT INTO dbo.{table_name} VALUES (1)")
    c1.close()  # uncommitted
    c2 = mssql_python.connect(conn_str)
    count = c2.cursor().execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0]
    c2.close()
    assert count == 0
    setup.cursor().execute(f"DROP TABLE dbo.{table_name}")
    setup.close()


# --- timeout and cancel (timeout_and_cancel.rs) ---------------------------------------------

def test_query_timeout_then_connection_reusable(conn_str):
    conn = mssql_python.connect(conn_str)
    conn.timeout = 1  # per-statement query timeout, picked up by cursors created afterwards
    try:
        cur = conn.cursor()
        start = time.monotonic()
        with pytest.raises(mssql_python.OperationalError):
            cur.execute("WAITFOR DELAY '00:00:10'")
        assert time.monotonic() - start < 6
        assert cur.execute("SELECT 7").fetchone()[0] == 7
    finally:
        conn.close()


def test_timeout_applies_to_each_statement_independently(conn_str):
    conn = mssql_python.connect(conn_str)
    conn.timeout = 2
    try:
        cur = conn.cursor()
        for _ in range(3):
            cur.execute("WAITFOR DELAY '00:00:00.300'")
    finally:
        conn.close()


def test_cancel_during_waitfor_then_next_statement_works(conn_str):
    conn = mssql_python.connect(conn_str)
    try:
        cur = conn.cursor()
        threading.Timer(0.5, cur.cancel).start()
        start = time.monotonic()
        with pytest.raises(mssql_python.Error):
            cur.execute("WAITFOR DELAY '00:00:20'")
        assert time.monotonic() - start < 10
        assert cur.execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()


def test_timeout_with_open_transaction_keeps_transaction_state(conn_str, table_name):
    conn = mssql_python.connect(conn_str)
    conn.timeout = 1
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE dbo.{table_name} (x INT)")
        conn.commit()
        cur.execute(f"INSERT INTO dbo.{table_name} VALUES (1)")
        with pytest.raises(mssql_python.OperationalError):
            cur.execute("WAITFOR DELAY '00:00:10'")
        conn.rollback()
        assert cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0] == 0
    finally:
        conn.close()


# --- info messages (test_info_messages.rs) ----------------------------------------------------

def test_print_and_low_severity_raiserror_are_messages(cursor):
    cursor.execute("PRINT 'first'; RAISERROR('second', 10, 1) WITH NOWAIT; SELECT 5")
    seen = [m[1] for m in cursor.messages]
    while cursor.description is None and cursor.nextset():  # message-only statements have no result set
        seen += [m[1] for m in cursor.messages]
    assert cursor.fetchone()[0] == 5
    text = " ".join(seen + [m[1] for m in cursor.messages])
    assert "first" in text and "second" in text


def test_info_messages_between_result_sets(cursor):
    cursor.execute("SELECT 1; PRINT 'between'; SELECT 2")
    assert cursor.fetchone()[0] == 1
    assert cursor.nextset()
    assert cursor.fetchone()[0] == 2
    assert any("between" in m[1] for m in cursor.messages)


def test_high_severity_raiserror_is_an_exception_not_a_message(cursor):
    with pytest.raises(mssql_python.Error, match="boom"):
        cursor.execute("RAISERROR('boom', 16, 1)")
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_error_after_rows_is_raised_and_connection_drains(cursor):
    """test_drain_on_error.rs: the rest of a failed batch is discarded."""
    with pytest.raises(mssql_python.Error):
        cursor.execute("SELECT 1; SELECT 1/0; SELECT 3")
        cursor.fetchall()
        while cursor.nextset():
            cursor.fetchall()
    cursor.execute("SELECT 99")
    assert cursor.fetchone()[0] == 99


# --- cursor operations (test_cursor_ops.rs, mssql-py-core test_cursor.py) -----------------------------

def test_rowcount_for_dml_and_select(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (x INT)")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1),(2),(3)")
    assert cursor.rowcount == 3
    cursor.execute(f"UPDATE dbo.{table_name} SET x = x + 1 WHERE x > 1")
    assert cursor.rowcount == 2
    cursor.execute(f"DELETE FROM dbo.{table_name}")
    assert cursor.rowcount == 3


def test_description_for_each_result_set(cursor):
    cursor.execute("SELECT 1 AS a, 'x' AS b; SELECT CAST(1.5 AS DECIMAL(5,2)) AS c")
    assert [d[0] for d in cursor.description] == ["a", "b"]
    cursor.fetchall()
    assert cursor.nextset()
    assert [d[0] for d in cursor.description] == ["c"]
    assert cursor.nextset() is False or cursor.nextset() is None


def test_fetchmany_arraysize_and_exhaustion(cursor):
    cursor.execute("SELECT TOP 25 object_id FROM sys.all_objects ORDER BY object_id")
    cursor.arraysize = 10
    assert len(cursor.fetchmany()) == 10
    assert len(cursor.fetchmany(10)) == 10
    assert len(cursor.fetchmany(10)) == 5
    assert cursor.fetchmany(10) == []


def test_closing_cursor_mid_result_leaves_connection_usable(conn):
    cur = conn.cursor()
    cur.execute("SELECT TOP 5000 a.object_id FROM sys.all_objects a CROSS JOIN sys.all_columns b")
    cur.fetchone()
    cur.close()
    assert conn.cursor().execute("SELECT 1").fetchone()[0] == 1


def test_many_cursors_on_one_connection_are_sequential(conn):
    for i in range(20):
        cur = conn.cursor()
        assert cur.execute("SELECT ?", (i,)).fetchone()[0] == i
        cur.close()


# --- large values (test_large_value_read.rs) -------------------------------------------------------

@pytest.mark.parametrize("size", [8000, 8001, 65_536, 1_000_000, 8_000_000])
def test_large_varchar_and_varbinary_max_read(cursor, size):
    cursor.execute("SELECT REPLICATE(CAST('a' AS VARCHAR(MAX)), ?), CAST(REPLICATE(CAST(0x7F AS VARBINARY(MAX)), ?) AS VARBINARY(MAX))", (size, size))
    s, b = cursor.fetchone()
    assert len(s) == size and set(s) == {"a"}
    assert len(b) == size and set(bytes(b)) == {0x7F}


def test_large_nvarchar_max_read_with_non_bmp(cursor):
    cursor.execute("SELECT REPLICATE(CAST(N'\U0001f600é' AS NVARCHAR(MAX)), 200000)")
    value = cursor.fetchone()[0]
    assert value == "\U0001f600é" * 200000


# --- parameters: narrow encoding and RPC datatypes (test_narrow_param_encoding.rs, test_rpc_datatypes.rs) ----

@pytest.mark.parametrize("value", [0, 1, 127, 128, 255, 256, -1, -128, -129, 32767, 32768, -32768, -32769,
                                   2**31 - 1, 2**31, -(2**31), -(2**31) - 1, 2**63 - 1, -(2**63)])
def test_integer_parameters_round_trip_at_width_boundaries(cursor, value):
    cursor.execute("SELECT ?, SQL_VARIANT_PROPERTY(?, 'BaseType')", (value, value))
    got, base = cursor.fetchone()
    assert got == value
    assert base in {"tinyint", "smallint", "int", "bigint", "decimal", "numeric"}


@pytest.mark.parametrize("value", [
    True, False, 1.5, -0.0, 1e308, float("inf") if False else 3.25,
    D("0.000001"), D("-12345678901234567890.123456789"),
    "", "ascii", "héllo \U0001f600", "x" * 5000, "y" * 100_000,
    b"", b"\x00\xff" * 50, b"z" * 100_000,
    dt.date(1, 1, 1), dt.date(9999, 12, 31), dt.time(23, 59, 59, 999999),
    dt.datetime(2024, 5, 6, 7, 8, 9, 123456),
    dt.datetime(2024, 5, 6, 7, 8, 9, tzinfo=dt.timezone(dt.timedelta(hours=-3, minutes=-30))),
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
])
def test_parameter_round_trip(cursor, value):
    cursor.execute("SELECT ?", (value,))
    got = cursor.fetchone()[0]
    if isinstance(value, bytes):
        got = bytes(got)
    assert got == value
    assert type(got) is type(value) or isinstance(got, type(value))


def test_none_parameter_in_every_position(cursor):
    cursor.execute("SELECT ?, ?, ?", (None, 1, None))
    assert tuple(cursor.fetchone()) == (None, 1, None)


def test_many_parameters(cursor):
    n = 2000
    cursor.execute("SELECT " + ",".join("?" for _ in range(n)), tuple(range(n)))
    assert tuple(cursor.fetchone()) == tuple(range(n))


def test_parameter_count_mismatch_raises_without_poisoning_connection(cursor):
    with pytest.raises(mssql_python.Error):
        cursor.execute("SELECT ?, ?", (1,))
    cursor.execute("SELECT ?", (1,))
    assert cursor.fetchone()[0] == 1


# --- RPC results (test_rpc_results.rs) ---------------------------------------------------------------

def test_stored_procedure_result_sets_and_return_status(cursor):
    proc = f"dbo.ci_rpc_{uuid.uuid4().hex[:8]}"
    cursor.execute(f"CREATE PROCEDURE {proc} @n INT AS BEGIN SELECT @n AS a; SELECT @n * 2 AS b; RETURN 7; END")
    try:
        cursor.execute(f"DECLARE @r INT; EXEC @r = {proc} @n = ?; SELECT @r AS ret", (21,))
        assert cursor.fetchone()[0] == 21
        assert cursor.nextset()
        assert cursor.fetchone()[0] == 42
        assert cursor.nextset()
        assert cursor.fetchone()[0] == 7
    finally:
        cursor.execute(f"DROP PROCEDURE {proc}")


def test_output_parameter_via_declared_variable(cursor):
    proc = f"dbo.ci_out_{uuid.uuid4().hex[:8]}"
    cursor.execute(f"CREATE PROCEDURE {proc} @n INT, @o INT OUTPUT AS SET @o = @n + 1")
    try:
        cursor.execute(f"DECLARE @o INT; EXEC {proc} ?, @o OUTPUT; SELECT @o", (4,))
        assert cursor.fetchone()[0] == 5
    finally:
        cursor.execute(f"DROP PROCEDURE {proc}")


def test_error_inside_procedure_after_first_result(cursor):
    proc = f"dbo.ci_err_{uuid.uuid4().hex[:8]}"
    cursor.execute(f"CREATE PROCEDURE {proc} AS BEGIN SELECT 1; RAISERROR('late', 16, 1); END")
    try:
        with pytest.raises(mssql_python.Error, match="late"):
            cursor.execute(f"EXEC {proc}")
            cursor.fetchall()
            while cursor.nextset():
                pass
        assert cursor.execute("SELECT 2").fetchone()[0] == 2
    finally:
        cursor.execute(f"DROP PROCEDURE {proc}")


# --- TVP (test_tvp.rs): arrowsql has no table-valued-parameter API ---------------------------------

def test_list_of_tuples_is_not_silently_accepted_as_tvp(cursor):
    with pytest.raises(Exception):
        cursor.execute("SELECT COUNT(*) FROM ?", ([(1,), (2,)],))
    assert cursor.execute("SELECT 1").fetchone()[0] == 1


# --- transactions (test_client_transactions.rs, transaction.rs) -----------------------------------------

def test_commit_rollback_and_savepoint(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table_name} (x INT)")
    conn.commit()
    cur.execute(f"INSERT INTO dbo.{table_name} VALUES (1)")
    cur.execute("SAVE TRANSACTION sp1")
    cur.execute(f"INSERT INTO dbo.{table_name} VALUES (2)")
    cur.execute("ROLLBACK TRANSACTION sp1")
    conn.commit()
    assert [r[0] for r in cur.execute(f"SELECT x FROM dbo.{table_name}").fetchall()] == [1]
    cur.execute(f"INSERT INTO dbo.{table_name} VALUES (3)")
    conn.rollback()
    assert [r[0] for r in cur.execute(f"SELECT x FROM dbo.{table_name}").fetchall()] == [1]


def test_autocommit_toggle_and_trancount(conn_str, table_name):
    conn = mssql_python.connect(conn_str, autocommit=True)
    try:
        cur = conn.cursor()
        assert cur.execute("SELECT @@TRANCOUNT").fetchone()[0] == 0
        conn.autocommit = False
        cur.execute("SELECT 1").fetchall()
        assert cur.execute("SELECT @@TRANCOUNT").fetchone()[0] == 1
        conn.commit()
        conn.autocommit = True
        assert cur.execute("SELECT @@TRANCOUNT").fetchone()[0] == 0
    finally:
        conn.close()


@pytest.mark.parametrize("level", ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE", "SNAPSHOT"])
def test_isolation_level_set_via_batch(conn, level):
    cur = conn.cursor()
    if level == "SNAPSHOT":
        pytest.skip("needs ALLOW_SNAPSHOT_ISOLATION on the database")
    cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
    cur.execute("SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID")
    assert cur.fetchone()[0] in {1, 2, 3, 4}
    conn.rollback()


# --- login / connection options (test_login_server_name.rs, test_encrypt_for_connections.rs) ----------------

def test_application_name_is_set_by_the_driver(conn):
    assert conn.cursor().execute("SELECT APP_NAME()").fetchone()[0]


@pytest.mark.parametrize("encrypt", ["yes", "mandatory", "true", "optional", "no", "false"])
def test_encrypt_keyword_variants_connect_and_report_encryption(conn_str, encrypt):
    base = ";".join(p for p in conn_str.split(";") if not p.lower().startswith(("encrypt=", "trustservercertificate=")))
    conn = mssql_python.connect(base + f";Encrypt={encrypt};TrustServerCertificate=yes")
    try:
        enc = conn.cursor().execute(
            "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID").fetchone()[0]
        if encrypt in {"yes", "mandatory", "true"}:
            assert enc == "TRUE"
    finally:
        conn.close()


def test_connect_to_unreachable_port_fails_fast(conn_str):
    base = ";".join(p for p in conn_str.split(";") if not p.lower().startswith("server="))
    start = time.monotonic()
    with pytest.raises(mssql_python.Error):
        mssql_python.connect(base + ";Server=127.0.0.1,1", timeout=3)
    assert time.monotonic() - start < 10


def test_wrong_password_is_login_failed_error(conn_str):
    parts = [p for p in conn_str.split(";") if not p.lower().startswith(("pwd=", "password="))]
    with pytest.raises(mssql_python.Error) as info:
        mssql_python.connect(";".join(parts) + ";Pwd=definitely-wrong-password")
    assert "18456" in str(info.value) or "Login failed" in str(info.value)
