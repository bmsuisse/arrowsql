"""Regressions for the statement/connection core of the Rust port.

Each test pins one review finding (docs/ARCHITECTURE.md, MARS emulation and
concurrency model) or one behaviour the pooling suite relies on. The
behaviours are driver-level, so the tests go through the public API where
they can and through ``ddbc_bindings`` where the finding is about a native
entry point.
"""

import decimal
import os
import threading
import time

import pytest
from mssql_python import Error, OperationalError, connect, ddbc_bindings as ddbc

SQL_DECIMAL = 3
SQL_ATTR_CURRENT_CATALOG = 109
SQL_ATTR_TXN_ISOLATION = 108
SQL_ATTR_AUTOCOMMIT = 102
SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_TXN_READ_COMMITTED = 2
SQL_TXN_SERIALIZABLE = 8
OK = (0, 1)


def _rows(cursor):
    return [tuple(row) for row in cursor.fetchall()]


def _scalar(native_stmt, sql):
    assert ddbc.DDBCSQLExecDirect(native_stmt, sql) in OK
    row = []
    assert ddbc.DDBCSQLFetchOne(native_stmt, row) in OK
    native_stmt._close_cursor()
    return row[0]


@pytest.fixture
def native(conn_str):
    connection = ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1})
    yield connection
    connection.close()


# ---------------------------------------------------------------------------
# Result cursor
# ---------------------------------------------------------------------------


@pytest.mark.upstream_bug("upstream cannot continue the batch after the error ('Associated statement is not prepared')")
def test_nextset_reports_error_met_while_reading_ahead(cursor):
    # Divide-by-zero ends only its statement, so the second SELECT still runs.
    cursor.execute("SELECT 1 / (n - 2) FROM (VALUES (1), (2), (3)) t(n); SELECT 42 AS answer")
    assert tuple(cursor.fetchone()) == (-1,)

    with pytest.raises(Exception, match="(?i)divide by zero"):
        cursor.nextset()

    assert cursor.nextset() is True
    assert _rows(cursor) == [(42,)]


def test_error_between_results_does_not_hide_later_result(cursor):
    cursor.execute("SELECT 1 AS a; RAISERROR('boom', 16, 1); SELECT 2 AS b")
    assert _rows(cursor) == [(1,)]

    with pytest.raises(Exception, match="boom"):
        cursor.nextset()

    assert cursor.nextset() is True
    assert _rows(cursor) == [(2,)]
    assert not cursor.nextset()


def test_fetchmany_returns_requested_rows_beyond_the_read_cap(cursor):
    # 10 rows of 3 MB exceed the 16 MiB per-read cap several times over.
    cursor.execute(
        "SELECT CAST(REPLICATE(CAST('x' AS VARCHAR(MAX)), 3000000) AS VARCHAR(MAX)) AS blob "
        "FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t(n)"
    )
    rows = cursor.fetchmany(10)
    assert len(rows) == 10
    assert all(len(row[0]) == 3_000_000 for row in rows)


def test_fetchall_survives_rows_larger_than_the_read_cap(cursor):
    cursor.execute(
        "SELECT CAST(REPLICATE(CAST('y' AS VARCHAR(MAX)), 3000000) AS VARCHAR(MAX)) "
        "FROM (VALUES (1),(2),(3),(4),(5),(6)) t(n)"
    )
    assert len(cursor.fetchall()) == 6


# ---------------------------------------------------------------------------
# executemany
# ---------------------------------------------------------------------------


def _output_ids(cursor):
    ids = []
    while True:
        ids.extend(row[0] for row in cursor.fetchall())
        if not cursor.nextset():
            return ids


@pytest.mark.parametrize("count", [3, 2100], ids=["one-chunk", "two-chunks"])
def test_executemany_output_rows_stay_readable(cursor, count):
    cursor.execute("CREATE TABLE #em_out (id INT IDENTITY(1,1), v INT)")
    cursor.executemany(
        "INSERT INTO #em_out (v) OUTPUT INSERTED.id VALUES (?)", [(i,) for i in range(count)]
    )

    assert cursor.description is not None
    assert sorted(_output_ids(cursor)) == list(range(1, count + 1))


def test_executemany_applies_decimal_param_info(cursor):
    cursor.execute("CREATE TABLE #em_dec (v DECIMAL(10, 4))")
    # The Python layer sends the values as strings and declares their type in paramInfos.
    cursor.setinputsizes([(SQL_DECIMAL, 10, 4)])
    cursor.executemany(
        "INSERT INTO #em_dec (v) SELECT ? / 2",
        [(decimal.Decimal("1.25"),), (decimal.Decimal("3.5"),)],
    )
    cursor.execute("SELECT v FROM #em_dec ORDER BY v")
    assert _rows(cursor) == [(decimal.Decimal("0.6250"),), (decimal.Decimal("1.7500"),)]


def test_executemany_large_batch_inserts_every_row(cursor):
    cursor.execute("CREATE TABLE #em_big (n INT)")
    cursor.executemany("INSERT INTO #em_big VALUES (?)", [(i,) for i in range(20_000)])
    assert cursor.rowcount == 20_000
    cursor.execute("SELECT COUNT(*), MIN(n), MAX(n) FROM #em_big")
    assert _rows(cursor) == [(20_000, 0, 19_999)]


def test_executemany_reports_server_errors_in_order(cursor):
    cursor.execute("CREATE TABLE #em_err (n TINYINT)")
    with pytest.raises(Exception) as excinfo:
        cursor.executemany("INSERT INTO #em_err VALUES (?)", [(1,), (300,)])
    assert "tinyint" in str(excinfo.value).lower()


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------


def test_deferred_session_sql_is_retried_after_failure(conn_str):
    connection = ddbc.Connection(
        conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1, SQL_ATTR_CURRENT_CATALOG: "no_such_db_for_rs_core"}
    )
    statement = connection.alloc_statement_handle()
    try:
        # The USE fails on the first statement and must not be forgotten.
        assert ddbc.DDBCSQLExecDirect(statement, "SELECT DB_NAME()") == -1
        assert ddbc.DDBCSQLExecDirect(statement, "SELECT DB_NAME()") == -1
    finally:
        statement.free()
        connection.close()


def test_set_attr_catalog_supersedes_constructor_catalog(conn_str):
    connection = ddbc.Connection(
        conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1, SQL_ATTR_CURRENT_CATALOG: "tempdb"}
    )
    statement = connection.alloc_statement_handle()
    try:
        connection.set_attr(SQL_ATTR_CURRENT_CATALOG, "master")
        assert _scalar(statement, "SELECT DB_NAME()") == "master"
    finally:
        statement.free()
        connection.close()


def test_set_attr_isolation_supersedes_constructor_isolation(conn_str):
    connection = ddbc.Connection(
        conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1, SQL_ATTR_TXN_ISOLATION: SQL_TXN_SERIALIZABLE}
    )
    statement = connection.alloc_statement_handle()
    try:
        connection.set_attr(SQL_ATTR_TXN_ISOLATION, SQL_TXN_READ_COMMITTED)
        level = _scalar(
            statement,
            "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        assert level == 2
    finally:
        statement.free()
        connection.close()


@pytest.mark.parametrize("token", [b"x", b"\x04\x00\x00\x00ab", "not-bytes"], ids=["short", "bad-length", "str"])
def test_malformed_access_token_is_rejected(conn_str, token):
    with pytest.raises(Error, match="(?i)access.token"):
        ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1, SQL_COPT_SS_ACCESS_TOKEN: token})


# ---------------------------------------------------------------------------
# Wire hand-over (MARS emulation)
# ---------------------------------------------------------------------------

BIG_RESULT = (
    "SELECT TOP ({n}) a.object_id AS id FROM sys.all_objects a "
    "CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c"
)


def test_closing_a_connection_discards_a_pending_result_quickly(conn_str):
    connection = ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1})
    statement = connection.alloc_statement_handle()
    assert ddbc.DDBCSQLExecDirect(statement, BIG_RESULT.format(n=50_000_000)) in OK
    row = []
    assert ddbc.DDBCSQLFetchOne(statement, row) in OK

    started = time.monotonic()
    connection.close()
    assert time.monotonic() - started < 10
    statement.free()


def test_closing_a_cursor_discards_a_pending_result_quickly(conn):
    cursor = conn.cursor()
    cursor.execute(BIG_RESULT.format(n=50_000_000))
    cursor.fetchone()

    started = time.monotonic()
    cursor.close()
    assert time.monotonic() - started < 10

    follow_up = conn.cursor()
    follow_up.execute("SELECT 1")
    assert _rows(follow_up) == [(1,)]


@pytest.mark.upstream_bug("without MARS the ODBC driver refuses commit while a result is pending ('Connection is busy')")
def test_commit_buffers_a_huge_pending_result_within_a_bound(conn):
    cursor = conn.cursor()
    cursor.execute(BIG_RESULT.format(n=30_000_000))
    cursor.fetchone()

    started = time.monotonic()
    conn.commit()  # needs the wire: the pending result is moved off it, bounded
    assert time.monotonic() - started < 60

    with pytest.raises(Exception, match="(?i)discarded"):
        cursor.fetchall()


@pytest.mark.upstream_bug("without MARS the ODBC driver refuses a second active statement ('Connection is busy')")
def test_second_cursor_works_while_first_has_pending_rows(conn):
    first = conn.cursor()
    first.execute("SELECT n FROM (VALUES (1),(2),(3)) t(n)")
    assert tuple(first.fetchone()) == (1,)

    second = conn.cursor()
    second.execute("SELECT 'other'")
    assert _rows(second) == [("other",)]
    assert _rows(first) == [(2,), (3,)]


def test_buffering_another_cursor_honours_the_requesters_timeout(conn):
    slow = conn.cursor()
    # NOWAIT flushes the first result to the client before the long wait.
    slow.execute("SELECT 1 AS a; RAISERROR('flush', 0, 1) WITH NOWAIT; WAITFOR DELAY '00:00:20'; SELECT 2 AS b")

    conn.timeout = 1
    requester = conn.cursor()
    started = time.monotonic()
    with pytest.raises(OperationalError):
        requester.execute("SELECT 3")
    assert time.monotonic() - started < 10

    conn.timeout = 0
    check = conn.cursor()
    check.execute("SELECT 4")
    assert _rows(check) == [(4,)]


def test_null_type_inference_honours_the_statement_timeout(conn_str):
    locker = connect(conn_str)
    victim = connect(conn_str)
    try:
        cursor = locker.cursor()
        cursor.execute("CREATE TABLE rs_core_null_lock (x INT)")
        locker.commit()
        try:
            cursor.execute("BEGIN TRAN; ALTER TABLE rs_core_null_lock ADD y INT")  # holds Sch-M

            victim.timeout = 1
            blocked = victim.cursor()
            started = time.monotonic()
            with pytest.raises(OperationalError):
                blocked.execute("INSERT INTO rs_core_null_lock (x) VALUES (?)", (None,))
            assert time.monotonic() - started < 15
        finally:
            locker.rollback()
            cursor.execute("DROP TABLE IF EXISTS rs_core_null_lock")
            locker.commit()
    finally:
        victim.close()
        locker.close()


# ---------------------------------------------------------------------------
# Locks are taken with the GIL released
# ---------------------------------------------------------------------------


def _heartbeat_gap(blocked_calls, seconds=1.5):
    """Longest pause of a pure-Python thread while `blocked_calls` block."""
    gaps = []
    stop = threading.Event()

    def beat():
        last = time.monotonic()
        while not stop.is_set():
            time.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    beater = threading.Thread(target=beat)
    beater.start()
    workers = [threading.Thread(target=call) for call in blocked_calls]
    for worker in workers:
        worker.start()
    time.sleep(seconds)
    stop.set()
    beater.join()
    for worker in workers:
        worker.join()
    return max(gaps)


def test_getters_waiting_for_a_running_query_do_not_hold_the_gil(conn_str):
    connection = ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1})
    statement = connection.alloc_statement_handle()
    runner = threading.Thread(
        target=lambda: ddbc.DDBCSQLExecDirect(statement, "WAITFOR DELAY '00:00:04'")
    )
    runner.start()
    time.sleep(0.5)  # let the query take the connection and statement locks
    try:
        gap = _heartbeat_gap(
            [
                lambda: ddbc.DDBCSQLRowCount(statement),
                lambda: ddbc.DDBCSQLNumResultCols(statement),
                lambda: ddbc.DDBCSQLGetAllDiagRecords(statement),
                lambda: connection.get_info(6),
            ]
        )
        assert gap < 0.5, f"Python threads stalled for {gap:.2f}s behind native locks"
    finally:
        runner.join()
        statement.free()
        connection.close()


# ---------------------------------------------------------------------------
# Handle and connection lifecycle
# ---------------------------------------------------------------------------


def test_free_handle_is_idempotent(native):
    statement = native.alloc_statement_handle()
    assert ddbc.DDBCSQLFreeHandle(3, statement) in OK
    assert ddbc.DDBCSQLFreeHandle(3, statement) == -2
    assert statement.free() is None


def test_failed_disconnect_keeps_the_connection_usable(conn_str):
    connection = ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 0})
    statement = connection.alloc_statement_handle()
    try:
        assert ddbc.DDBCSQLExecDirect(statement, "CREATE TABLE #rs_core_open_txn (n INT)") in OK
        with pytest.raises(OperationalError) as info:
            connection.close()
        assert info.value.sqlstate == "25000"
        assert _scalar(statement, "SELECT 7") == 7
        connection.rollback()
    finally:
        connection.close()
        statement.free()


def test_alloc_after_close_is_rejected(conn_str):
    connection = ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1})
    connection.close()
    with pytest.raises(RuntimeError, match="not initialized"):
        connection.alloc_statement_handle()


def test_concurrent_close_and_allocation_never_raise_borrow_errors(conn_str):
    for _ in range(20):
        connection = ddbc.Connection(conn_str, False, {SQL_ATTR_AUTOCOMMIT: 1})
        errors = []

        def allocate():
            for _ in range(50):
                try:
                    connection.alloc_statement_handle().free()
                except RuntimeError as exc:
                    errors.append(str(exc))

        worker = threading.Thread(target=allocate)
        worker.start()
        connection.close()
        worker.join()
        assert all("not initialized" in message for message in errors), errors


# ---------------------------------------------------------------------------
# Pooling
# ---------------------------------------------------------------------------


@pytest.fixture
def pool():
    ddbc.enable_pooling(100, 600)
    yield
    ddbc.disable_pooling()


def _spid(native_connection):
    statement = native_connection.alloc_statement_handle()
    try:
        return _scalar(statement, "SELECT @@SPID")
    finally:
        statement.free()


def test_pooled_session_is_reused_and_isolation_is_reset(conn_str, pool):
    key = conn_str + "\x00rs_core_isolation"
    attrs = {SQL_ATTR_AUTOCOMMIT: 1}
    first = ddbc.Connection(conn_str, True, dict(attrs), key, None)
    first.set_attr(SQL_ATTR_TXN_ISOLATION, SQL_TXN_SERIALIZABLE)
    spid = _spid(first)
    first.close()

    second = ddbc.Connection(conn_str, True, dict(attrs), key, None)
    try:
        assert _spid(second) == spid
        statement = second.alloc_statement_handle()
        level = _scalar(
            statement,
            "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        statement.free()
        assert level == 2
    finally:
        second.close()


def test_dropped_pooled_connection_gives_its_slot_back(conn_str):
    ddbc.disable_pooling()
    ddbc.enable_pooling(100, 600)
    try:
        key = conn_str + "\x00rs_core_drop"
        for _ in range(150):  # more than the pool's capacity of 100
            dropped = ddbc.Connection(conn_str, True, {SQL_ATTR_AUTOCOMMIT: 1}, key, None)
            del dropped
    finally:
        ddbc.disable_pooling()


# ---------------------------------------------------------------------------
# r2 review: XACT_ABORT guard covers executemany; decimals with no integer digits
# ---------------------------------------------------------------------------


def test_executemany_after_xact_abort_is_rejected_like_execute(conn, cursor, table_name):
    # Upstream (msodbcsql) lets every statement after the server-side rollback run
    # outside the transaction; the port rejects the first write of execute() and
    # must treat executemany the same way. rollback() clears the state.
    from mssql_python import DatabaseError

    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY)")
    conn.commit()
    cursor.execute("SET XACT_ABORT ON")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1)")
    with pytest.raises(DatabaseError):
        cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1)")
    with pytest.raises(DatabaseError):
        cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?)", [(5,), (6,)])
    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0
    cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?)", [(5,), (6,)])
    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 2


@pytest.mark.parametrize(
    "values",
    [
        [decimal.Decimal("1E-38")],
        [decimal.Decimal("0.5")],
        [decimal.Decimal("0")],
        [decimal.Decimal("-0.00001")],
        [decimal.Decimal("0.5"), decimal.Decimal("1E-38")],
        [decimal.Decimal("9" * 38)],
    ],
)
def test_decimal_with_zero_integer_digits_binds(conn, cursor, table_name, values):
    scale = max(0, -min(v.as_tuple().exponent for v in values))
    precision = max(len(v.as_tuple().digits) for v in values) if scale == 0 else 38
    cursor.execute(f"CREATE TABLE dbo.{table_name} (d NUMERIC({precision}, {scale}))")
    cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?)", [(v,) for v in values])
    for v in values:
        cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (?)", (v,))
    conn.commit()
    cursor.execute(f"SELECT d FROM dbo.{table_name}")
    assert sorted(r[0] for r in cursor.fetchall()) == sorted(values * 2)


@pytest.mark.parametrize("other", [0, 0.0, decimal.Decimal("0.5")])
def test_executemany_decimal_mixed_with_other_numbers_allows_zero_integer_digits(
    conn, cursor, table_name, other
):
    # A column mixing a Decimal with an int/float is unified as one DECIMAL; 1E-38
    # has no integer digits, so NUMERIC(38,38) needs precision 38, not 39.
    cursor.execute(f"CREATE TABLE dbo.{table_name} (d NUMERIC(38, 38))")
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} VALUES (?)", [(decimal.Decimal("1E-38"),), (other,)]
    )
    conn.commit()
    cursor.execute(f"SELECT d FROM dbo.{table_name}")
    assert sorted(r[0] for r in cursor.fetchall()) == sorted(
        [decimal.Decimal("1E-38"), decimal.Decimal(str(other))]
    )


@pytest.mark.skipif(not hasattr(os, "register_at_fork"), reason="no os.register_at_fork (Windows)")
def test_module_imports_without_register_at_fork():
    import subprocess
    import sys

    code = (
        "import os\n"
        "del os.register_at_fork\n"
        "import mssql_python.ddbc_bindings\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
