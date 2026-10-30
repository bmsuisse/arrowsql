"""Transaction semantics on the driver: autocommit, visibility, rollback, XACT_ABORT, deadlocks.

Ground truth is the upstream ODBC-backed driver. Tests use a permanent table
(``table_name`` fixture) so that a second connection can observe commit and
rollback. Cross-connection reads use a fresh connection and only run when no
other connection holds uncommitted writes on the table, because READ COMMITTED
readers would otherwise block on the writer's locks.
"""

import threading
import time

import pytest
from mssql_python import (
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    connect,
)


class _Boom(Exception):
    """Sentinel exception used to check that the original error propagates."""


def _create_table(cursor, table):
    cursor.execute(
        f"CREATE TABLE dbo.{table} (id INT NOT NULL PRIMARY KEY, v VARCHAR(20) NOT NULL)"
    )
    cursor.connection.commit()


def _insert(cursor, table, row_id, value):
    cursor.execute(f"INSERT INTO dbo.{table} (id, v) VALUES (?, ?)", (row_id, value))


def _fetch_committed(conn_str, table):
    """Read committed rows on a fresh connection, ordered by id."""
    reader = connect(conn_str)
    try:
        cur = reader.cursor()
        cur.execute(f"SELECT id, v FROM dbo.{table} ORDER BY id")
        return [tuple(row) for row in cur.fetchall()]
    finally:
        reader.close()


def _fetch_dirty(conn_str, table):
    """Read rows including uncommitted changes (READ UNCOMMITTED, never blocks)."""
    reader = connect(conn_str)
    try:
        cur = reader.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cur.execute(f"SELECT id, v FROM dbo.{table} ORDER BY id")
        return [tuple(row) for row in cur.fetchall()]
    finally:
        reader.close()


def _trancount(cursor):
    cursor.execute("SELECT @@TRANCOUNT")
    return cursor.fetchone()[0]


class TestAutocommitMode:
    def test_autocommit_defaults_to_off(self, conn):
        assert conn.autocommit is False
        # With autocommit off, the driver opens an implicit transaction for each batch.
        assert _trancount(conn.cursor()) == 1

    def test_autocommit_on_keeps_trancount_at_zero(self, conn_str):
        conn = connect(conn_str, autocommit=True)
        try:
            assert conn.autocommit is True
            assert _trancount(conn.cursor()) == 0
        finally:
            conn.close()

    def test_autocommit_setter_toggles_trancount(self, conn):
        cur = conn.cursor()
        conn.autocommit = True
        assert conn.autocommit is True
        assert _trancount(cur) == 0
        conn.autocommit = False
        assert conn.autocommit is False
        assert _trancount(cur) == 1

    def test_trancount_is_one_again_after_commit_in_manual_mode(self, conn):
        cur = conn.cursor()
        assert _trancount(cur) == 1
        conn.commit()
        assert _trancount(cur) == 1

    def test_autocommit_insert_is_visible_without_commit(self, conn_str, cursor, table_name):
        _create_table(cursor, table_name)
        conn = connect(conn_str, autocommit=True)
        try:
            _insert(conn.cursor(), table_name, 1, "auto")
            # No commit() call: the statement is durable and visible to other sessions.
            assert _fetch_committed(conn_str, table_name) == [(1, "auto")]
        finally:
            conn.close()

    def test_enabling_autocommit_commits_pending_work(self, conn_str, conn, cursor, table_name):
        _create_table(cursor, table_name)
        _insert(conn.cursor(), table_name, 1, "pending")
        conn.autocommit = True
        assert conn.autocommit is True
        assert _fetch_committed(conn_str, table_name) == [(1, "pending")]


class TestVisibilityAcrossConnections:
    def test_uncommitted_insert_is_dirty_readable_then_visible_after_commit(
        self, conn_str, conn, cursor, table_name
    ):
        _create_table(cursor, table_name)
        _insert(conn.cursor(), table_name, 1, "wip")

        # READ UNCOMMITTED sees the in-flight row.
        assert _fetch_dirty(conn_str, table_name) == [(1, "wip")]

        conn.commit()
        assert _fetch_committed(conn_str, table_name) == [(1, "wip")]

    @pytest.mark.upstream_bug(
        reason=(
            "Lock request timeout (error 1222) is reported with SQLSTATE 42000, which the "
            "driver maps to ProgrammingError ('Syntax error or access violation'). It is an "
            "operational condition and should raise OperationalError."
        )
    )
    @pytest.mark.xfail(strict=True, reason="upstream bug")
    def test_lock_timeout_on_uncommitted_insert_raises_operationalerror(
        self, conn_str, conn, cursor, table_name
    ):
        _create_table(cursor, table_name)
        _insert(conn.cursor(), table_name, 1, "locked")
        # A committed read blocks on the writer's lock; with LOCK_TIMEOUT 0 it fails fast.
        reader = connect(conn_str)
        try:
            rc = reader.cursor()
            rc.execute("SET LOCK_TIMEOUT 0")
            with pytest.raises(OperationalError):
                rc.execute(f"SELECT id, v FROM dbo.{table_name}")
        finally:
            reader.close()
            conn.rollback()

    def test_rollback_discards_insert(self, conn_str, conn, cursor, table_name):
        _create_table(cursor, table_name)
        cur = conn.cursor()
        _insert(cur, table_name, 1, "gone")
        conn.rollback()
        assert _trancount(cur) == 1
        cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        assert cur.fetchone()[0] == 0
        assert _fetch_committed(conn_str, table_name) == []

    def test_commit_makes_insert_visible_to_second_connection(
        self, conn_str, conn, cursor, table_name
    ):
        _create_table(cursor, table_name)
        _insert(conn.cursor(), table_name, 1, "kept")
        conn.commit()
        assert _fetch_committed(conn_str, table_name) == [(1, "kept")]

    def test_close_without_commit_rolls_back(self, conn_str, cursor, table_name):
        _create_table(cursor, table_name)
        conn = connect(conn_str)
        _insert(conn.cursor(), table_name, 1, "orphan")
        conn.close()
        assert conn.closed is True
        assert _fetch_committed(conn_str, table_name) == []


class TestContextManager:
    def test_enter_returns_connection_and_clean_exit_commits(self, conn_str, cursor, table_name):
        _create_table(cursor, table_name)
        with connect(conn_str) as conn:
            entered = conn
            _insert(conn.cursor(), table_name, 1, "cm")
        assert entered is conn
        assert entered.closed is True
        assert _fetch_committed(conn_str, table_name) == [(1, "cm")]

    def test_exception_rolls_back_and_propagates_original_type(self, conn_str, cursor, table_name):
        _create_table(cursor, table_name)
        holder = {}
        with pytest.raises(_Boom):
            with connect(conn_str) as conn:
                holder["conn"] = conn
                _insert(conn.cursor(), table_name, 1, "doomed")
                raise _Boom("user error")
        assert holder["conn"].closed is True
        assert _fetch_committed(conn_str, table_name) == []

    def test_integrity_error_inside_block_rolls_back_whole_block(
        self, conn_str, cursor, table_name
    ):
        _create_table(cursor, table_name)
        _insert(cursor, table_name, 1, "base")
        cursor.connection.commit()
        with pytest.raises(IntegrityError) as info:
            with connect(conn_str) as conn:
                cur = conn.cursor()
                _insert(cur, table_name, 2, "new")
                _insert(cur, table_name, 1, "dup")
        assert info.type is IntegrityError
        assert _fetch_committed(conn_str, table_name) == [(1, "base")]

    def test_autocommit_connection_keeps_work_when_block_raises(self, conn_str, cursor, table_name):
        _create_table(cursor, table_name)
        with pytest.raises(RuntimeError, match="late failure"):
            with connect(conn_str, autocommit=True) as conn:
                _insert(conn.cursor(), table_name, 1, "auto")
                raise RuntimeError("late failure")
        assert _fetch_committed(conn_str, table_name) == [(1, "auto")]


class TestXactAbort:
    def test_xact_abort_off_keeps_earlier_statements_on_error(self, conn_str, conn, cursor, table_name):
        _create_table(cursor, table_name)
        cur = conn.cursor()
        _insert(cur, table_name, 1, "first")
        with pytest.raises(IntegrityError):
            _insert(cur, table_name, 1, "dup")
        conn.commit()
        assert _fetch_committed(conn_str, table_name) == [(1, "first")]

    def test_xact_abort_on_rolls_back_entire_transaction(self, conn_str, conn, cursor, table_name):
        _create_table(cursor, table_name)
        _insert(cursor, table_name, 9, "base")
        cursor.connection.commit()

        cur = conn.cursor()
        cur.execute("SET XACT_ABORT ON")
        _insert(cur, table_name, 1, "a")
        with pytest.raises(IntegrityError) as info:
            _insert(cur, table_name, 9, "dup")
        assert info.type is IntegrityError
        # The transaction was aborted server-side, so commit() succeeds without
        # resurrecting the earlier insert of id 1.
        conn.commit()
        assert _fetch_committed(conn_str, table_name) == [(9, "base")]

    def test_divide_by_zero_is_dataerror_and_connection_stays_usable(self, conn):
        cur = conn.cursor()
        with pytest.raises(DataError) as info:
            cur.execute("SELECT 1/0")
        assert info.type is DataError
        cur.execute("SELECT 42")
        assert tuple(cur.fetchone()) == (42,)
        conn.commit()

    def test_savepoint_rollback_keeps_outer_work(self, conn_str, conn, cursor, table_name):
        _create_table(cursor, table_name)
        cur = conn.cursor()
        _insert(cur, table_name, 1, "outer")
        cur.execute("SAVE TRANSACTION sp1")
        _insert(cur, table_name, 2, "inner")
        cur.execute("ROLLBACK TRANSACTION sp1")
        conn.commit()
        assert _fetch_committed(conn_str, table_name) == [(1, "outer")]

    def test_manual_begin_nests_and_commit_only_decrements_once(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN TRANSACTION")
        assert _trancount(cur) == 2
        # conn.commit() issues COMMIT TRAN, which only decrements the nesting
        # count; the implicit outer transaction stays open.
        conn.commit()
        assert _trancount(cur) == 1
        conn.rollback()
        assert _trancount(cur) == 1


class TestErrorsAndDeadlocks:
    def test_closed_connection_raises_interfaceerror(self, conn_str):
        conn = connect(conn_str)
        conn.close()
        with pytest.raises(InterfaceError) as commit_info:
            conn.commit()
        assert commit_info.type is InterfaceError
        with pytest.raises(InterfaceError) as rollback_info:
            conn.rollback()
        assert rollback_info.type is InterfaceError
        with pytest.raises(InterfaceError):
            conn.cursor()

    def test_deadlock_victim_raises_operationalerror_and_survivor_commits(
        self, conn_str, cursor, table_name
    ):
        _create_table(cursor, table_name)
        _insert(cursor, table_name, 1, "orig")
        _insert(cursor, table_name, 2, "orig")
        cursor.connection.commit()

        a = connect(conn_str)
        b = connect(conn_str)
        try:
            ca, cb = a.cursor(), b.cursor()
            # Safety net so a missed deadlock cannot hang the suite.
            ca.execute("SET LOCK_TIMEOUT 15000")
            cb.execute("SET LOCK_TIMEOUT 15000")

            # Each side locks its own row first, then requests the other's row.
            ca.execute(f"UPDATE dbo.{table_name} SET v = ? WHERE id = ?", ("A", 1))
            cb.execute(f"UPDATE dbo.{table_name} SET v = ? WHERE id = ?", ("B", 2))

            failures = {}

            def attempt_a():
                try:
                    ca.execute(f"UPDATE dbo.{table_name} SET v = ? WHERE id = ?", ("A", 2))
                except Exception as exc:  # noqa: BLE001 - captured for assertion below
                    failures["A"] = exc

            worker = threading.Thread(target=attempt_a)
            worker.start()
            time.sleep(0.5)  # let A block on row 2 before B asks for row 1
            try:
                cb.execute(f"UPDATE dbo.{table_name} SET v = ? WHERE id = ?", ("B", 1))
            except Exception as exc:  # noqa: BLE001 - captured for assertion below
                failures["B"] = exc
            worker.join(timeout=30)
            assert not worker.is_alive()

            # SQL Server picks exactly one victim; which side it is is not fixed.
            assert len(failures) == 1, f"expected one deadlock victim, got {sorted(failures)}"
            (victim_name, victim_exc), = failures.items()
            survivor_name = "B" if victim_name == "A" else "A"
            assert type(victim_exc) is OperationalError
            assert "deadlock victim" in str(victim_exc)

            victim, survivor = (a, b) if victim_name == "A" else (b, a)
            victim.rollback()
            victim_cur = victim.cursor()
            victim_cur.execute("SELECT 1")
            assert tuple(victim_cur.fetchone()) == (1,)
            survivor.commit()
        finally:
            a.close()
            b.close()

        # Only the survivor's updates remain; the victim's work was rolled back.
        assert _fetch_committed(conn_str, table_name) == [
            (1, survivor_name),
            (2, survivor_name),
        ]
