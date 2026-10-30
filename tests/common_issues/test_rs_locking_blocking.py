"""Locking and blocking behavior: LOCK_TIMEOUT / error 1222, two-connection
blocking across threads, NOLOCK and READPAST hints, sp_getapplock, and the
exception classes for lock timeouts and deadlock victims.

Sources (research, ~10 min):
- https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-1222-database-engine-error
  (error 1222 / LK_TIMEOUT: "Lock request time out period exceeded.")
- https://blog.sqlauthority.com/2013/01/28/sql-server-basic-explanation-of-set-lock_timeout-how-to-not-wait-on-locked-query/
  (SET LOCK_TIMEOUT semantics, default -1, 0 behaves like NOWAIT)
- https://github.com/ITHINK-DIGITAL/mmis-v3/pull/22
  (pyodbc surfaces 1222 as ProgrammingError with SQLSTATE 42000 and deadlock 1205
  as a generic DB-API error; error classification keyed off SQLSTATE/native number)
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/python-sql-driver-mssql-python?view=sql-server-ver17
  (mssql-python retry guidance: transient errors such as deadlock victim /
  lock timeout are retried on the same connection)
- https://github.com/microsoft/mssql-python/issues/321
  (driver hang with 3+ concurrent threads in one process: motivates the
  threaded blocking tests here)

Ground truth: upstream ODBC driver in .venv-upstream.
"""

import os
import threading
import time

import mssql_python
import pytest
from mssql_python import connect


def _other(conn_str):
    return connect(conn_str)


def _make_table(cursor, name):
    cursor.execute(f"CREATE TABLE dbo.{name} (id int PRIMARY KEY, v int NOT NULL)")
    cursor.execute(f"INSERT INTO dbo.{name} (id, v) VALUES (1, 10), (2, 20), (3, 30)")
    cursor.connection.commit()


def _scalar(cursor, sql, params=()):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


# ---------------------------------------------------------------- LOCK_TIMEOUT


def test_lock_timeout_default_is_minus_one(conn_str):
    c = _other(conn_str)
    try:
        value = _scalar(c.cursor(), "SELECT @@LOCK_TIMEOUT")
        assert value == -1
        assert type(value) is int
    finally:
        c.close()


def test_lock_timeout_setting_roundtrips(cursor):
    cursor.execute("SET LOCK_TIMEOUT 1500")
    assert _scalar(cursor, "SELECT @@LOCK_TIMEOUT") == 1500


def test_lock_timeout_is_session_scoped(conn, conn_str):
    other = _other(conn_str)
    try:
        conn.cursor().execute("SET LOCK_TIMEOUT 750")
        assert _scalar(other.cursor(), "SELECT @@LOCK_TIMEOUT") == -1
        assert _scalar(conn.cursor(), "SELECT @@LOCK_TIMEOUT") == 750
    finally:
        other.close()


def test_lock_timeout_survives_commit(conn):
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 2222")
    conn.commit()
    assert _scalar(cur, "SELECT @@LOCK_TIMEOUT") == 2222


def test_blocked_select_raises_lock_timeout_error(conn, conn_str, table_name):
    holder_cur = conn.cursor()
    _make_table(holder_cur, table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")
        waiter_cur = conn.cursor()
        waiter_cur.execute("SET LOCK_TIMEOUT 0")
        with pytest.raises(mssql_python.Error) as excinfo:
            waiter_cur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
        assert "Lock request time out period exceeded" in str(excinfo.value)
    finally:
        holder.rollback()
        holder.close()
        conn.rollback()


def test_lock_timeout_waits_approximately_the_configured_time(conn, conn_str, table_name):
    holder_cur = conn.cursor()
    _make_table(holder_cur, table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")
        waiter_cur = conn.cursor()
        waiter_cur.execute("SET LOCK_TIMEOUT 400")
        started = time.monotonic()
        with pytest.raises(mssql_python.Error):
            waiter_cur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
        elapsed = time.monotonic() - started
        assert 0.3 <= elapsed < 10.0
    finally:
        holder.rollback()
        holder.close()
        conn.rollback()


@pytest.mark.upstream_bug(
    reason=(
        "Upstream raises ProgrammingError (SQLSTATE 42000 path) for error 1222. "
        "A lock timeout is a transient operational condition, so PEP 249 "
        "OperationalError is the correct class (deadlock victims already are)."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_lock_timeout_error_class_is_operational(conn, conn_str, table_name):
    holder_cur = conn.cursor()
    _make_table(holder_cur, table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")
        waiter_cur = conn.cursor()
        waiter_cur.execute("SET LOCK_TIMEOUT 0")
        with pytest.raises(mssql_python.OperationalError):
            waiter_cur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
    finally:
        holder.rollback()
        holder.close()
        conn.rollback()


def test_lock_timeout_does_not_abort_explicit_transaction(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 0")
        cur.execute(f"INSERT INTO dbo.{table_name} (id, v) VALUES (4, 40)")
        with pytest.raises(mssql_python.Error):
            cur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
        # The failed statement was cancelled, but the transaction is still open:
        # the earlier INSERT is visible inside it and commit succeeds.
        assert _scalar(cur, f"SELECT COUNT(*) FROM dbo.{table_name} WHERE id = 4") == 1
        conn.commit()
        holder.rollback()
        assert _scalar(holder.cursor(), f"SELECT COUNT(*) FROM dbo.{table_name} WHERE id = 4") == 1
    finally:
        holder.rollback()
        holder.close()


def test_connection_usable_after_lock_timeout(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 0")
        with pytest.raises(mssql_python.Error):
            cur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
        # Unblocked rows are still readable on the same connection.
        assert _scalar(cur, f"SELECT v FROM dbo.{table_name} WHERE id = 2") == 20
        assert type(_scalar(cur, "SELECT 1")) is int
    finally:
        holder.rollback()
        holder.close()
        conn.rollback()


# ------------------------------------------------- blocking across connections


def test_waiter_thread_succeeds_after_blocker_commits(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 99 WHERE id = 3")
    result = {}

    def waiter():
        w = _other(conn_str)
        try:
            w.cursor().execute("SET LOCK_TIMEOUT 10000")
            started = time.monotonic()
            cur = w.cursor()
            cur.execute(f"UPDATE dbo.{table_name} SET v = 31 WHERE id = 3")
            result["rowcount"] = cur.rowcount
            result["elapsed"] = time.monotonic() - started
            w.commit()
        except Exception as exc:  # pragma: no cover - surfaced by assertions below
            result["error"] = exc
        finally:
            w.close()

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.5)
    holder.commit()
    t.join(timeout=30)
    holder.close()

    assert "error" not in result, result.get("error")
    assert result["rowcount"] == 1
    assert result["elapsed"] >= 0.3
    assert _scalar(conn.cursor(), f"SELECT v FROM dbo.{table_name} WHERE id = 3") == 31


def test_waiter_thread_gets_1222_then_retry_succeeds(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 99 WHERE id = 2")
    outcome = {}

    def waiter():
        w = _other(conn_str)
        try:
            cur = w.cursor()
            cur.execute("SET LOCK_TIMEOUT 150")
            try:
                cur.execute(f"UPDATE dbo.{table_name} SET v = 21 WHERE id = 2")
                outcome["first"] = "ok"
            except mssql_python.Error as exc:
                outcome["first"] = "timeout" if "Lock request time out" in str(exc) else str(exc)
            w.rollback()
            # Holder is still open at this point; the second attempt must block
            # again and succeed only once the holder releases.
            cur.execute("SET LOCK_TIMEOUT 10000")
            cur.execute(f"UPDATE dbo.{table_name} SET v = 21 WHERE id = 2")
            outcome["rowcount"] = cur.rowcount
            w.commit()
        except Exception as exc:  # pragma: no cover
            outcome["error"] = exc
        finally:
            w.close()

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.6)
    holder.commit()
    t.join(timeout=30)
    holder.close()

    assert "error" not in outcome, outcome.get("error")
    assert outcome["first"] == "timeout"
    assert outcome["rowcount"] == 1
    assert _scalar(conn.cursor(), f"SELECT v FROM dbo.{table_name} WHERE id = 2") == 21


def test_rollback_releases_lock_for_waiting_connection(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 77 WHERE id = 1")
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 0")
    with pytest.raises(mssql_python.Error):
        cur.execute(f"SELECT v FROM dbo.{table_name} WHERE id = 1")
    holder.rollback()
    holder.close()
    # Rolled back: the committed value is the original one.
    assert _scalar(cur, f"SELECT v FROM dbo.{table_name} WHERE id = 1") == 10


# ------------------------------------------------------------- NOLOCK / READPAST


def test_nolock_reads_uncommitted_value(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 555 WHERE id = 1")
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 0")
        cur.execute(f"SELECT v FROM dbo.{table_name} WITH (NOLOCK) WHERE id = 1")
        rows = cur.fetchall()
        assert [tuple(r) for r in rows] == [(555,)]
        assert type(rows[0][0]) is int
    finally:
        holder.rollback()
        holder.close()
        conn.rollback()


def test_nolock_on_no_matching_rows_returns_empty_list(conn, table_name):
    _make_table(conn.cursor(), table_name)
    cur = conn.cursor()
    cur.execute(f"SELECT v FROM dbo.{table_name} WITH (NOLOCK) WHERE id = 999")
    rows = cur.fetchall()
    assert rows == []
    assert isinstance(rows, list)


def test_readpast_skips_locked_row_without_waiting(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    holder = _other(conn_str)
    try:
        holder.cursor().execute(f"UPDATE dbo.{table_name} SET v = 11 WHERE id = 1")
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 0")
        started = time.monotonic()
        cur.execute(f"SELECT id, v FROM dbo.{table_name} WITH (READPAST) ORDER BY id")
        rows = [tuple(r) for r in cur.fetchall()]
        elapsed = time.monotonic() - started
        assert rows == [(2, 20), (3, 30)]
        assert elapsed < 1.0
    finally:
        holder.rollback()
        holder.close()
        conn.rollback()


# ------------------------------------------------------------------ sp_getapplock


def _applock(cursor, resource, owner, timeout_ms, mode="Exclusive"):
    cursor.execute(
        "DECLARE @r int; "
        "EXEC @r = sp_getapplock @Resource = ?, @LockMode = ?, "
        "@LockOwner = ?, @LockTimeout = ?; SELECT @r",
        (resource, mode, owner, timeout_ms),
    )
    return cursor.fetchone()[0]


def _releaseapplock(cursor, resource, owner):
    cursor.execute(
        "DECLARE @r int; EXEC @r = sp_releaseapplock @Resource = ?, @LockOwner = ?; SELECT @r",
        (resource, owner),
    )
    return cursor.fetchone()[0]


def test_applock_grant_returns_zero(conn, table_name):
    resource = f"wb_{table_name}"
    cur = conn.cursor()
    try:
        value = _applock(cur, resource, "Session", 0)
        assert value == 0
        assert type(value) is int
    finally:
        _releaseapplock(cur, resource, "Session")


def test_applock_conflict_with_zero_timeout_returns_minus_one_not_error(conn, conn_str, table_name):
    resource = f"wb_{table_name}"
    holder = _other(conn_str)
    try:
        assert _applock(holder.cursor(), resource, "Session", 0) == 0
        # sp_getapplock reports a timeout through its return value (-1), not as
        # a driver exception, so the call itself must succeed.
        value = _applock(conn.cursor(), resource, "Session", 0)
        assert value == -1
    finally:
        _releaseapplock(holder.cursor(), resource, "Session")
        holder.close()


def test_applock_release_frees_resource_for_other_session(conn, conn_str, table_name):
    resource = f"wb_{table_name}"
    holder = _other(conn_str)
    try:
        assert _applock(holder.cursor(), resource, "Session", 0) == 0
        assert _releaseapplock(holder.cursor(), resource, "Session") == 0
        assert _applock(conn.cursor(), resource, "Session", 0) == 0
    finally:
        _releaseapplock(conn.cursor(), resource, "Session")
        holder.close()


def test_applock_transaction_owner_released_on_commit(conn, conn_str, table_name):
    resource = f"wb_{table_name}"
    holder = _other(conn_str)
    try:
        assert _applock(holder.cursor(), resource, "Transaction", 0) == 0
        # Still held while the transaction is open.
        assert _applock(conn.cursor(), resource, "Session", 0) == -1
        holder.commit()
        assert _applock(conn.cursor(), resource, "Session", 0) == 0
    finally:
        _releaseapplock(conn.cursor(), resource, "Session")
        holder.close()


# ----------------------------------------------------------------- deadlocks


def test_deadlock_victim_raises_operational_error_and_other_session_commits(conn, conn_str, table_name):
    _make_table(conn.cursor(), table_name)
    a = _other(conn_str)
    b = _other(conn_str)
    ca, cb = a.cursor(), b.cursor()
    try:
        ca.execute(f"UPDATE dbo.{table_name} SET v = 100 WHERE id = 1")
        cb.execute(f"UPDATE dbo.{table_name} SET v = 200 WHERE id = 2")
        cb.execute("SET LOCK_TIMEOUT 5000")
        errors = {}

        def first():
            try:
                ca.execute(f"UPDATE dbo.{table_name} SET v = 101 WHERE id = 2")
            except Exception as exc:
                errors["a"] = exc

        t = threading.Thread(target=first)
        t.start()
        time.sleep(0.3)
        try:
            cb.execute(f"UPDATE dbo.{table_name} SET v = 201 WHERE id = 1")
        except Exception as exc:
            errors["b"] = exc
        t.join(timeout=30)

        # Exactly one session is chosen as the deadlock victim.
        assert len(errors) == 1, errors
        victim_key, victim = next(iter(errors.items()))
        assert isinstance(victim, mssql_python.OperationalError)
        assert "deadlock" in str(victim).lower()

        survivor = b if victim_key == "a" else a
        survivor.commit()
        victim_conn = a if victim_key == "a" else b
        victim_conn.rollback()
        # Victim's work is undone; the survivor's work is committed.
        # a victim: b's (id=1->201, id=2->200) committed, a's id=1->100 undone.
        # b victim: a's (id=1->100, id=2->101) committed, b's writes undone.
        expected = {1: 201, 2: 200} if victim_key == "a" else {1: 100, 2: 101}
        rows = dict(conn.cursor().execute(f"SELECT id, v FROM dbo.{table_name}").fetchall())
        assert {k: rows[k] for k in (1, 2)} == expected
    finally:
        a.close()
        b.close()
