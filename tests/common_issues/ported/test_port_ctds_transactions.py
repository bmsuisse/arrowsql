"""Autocommit switching, commit/rollback semantics and the connection context manager.

Adapted from zillow/ctds tests/test_connection_autocommit.py,
tests/test_connection_commit.py, tests/test_connection___exit__.py and
tests/test_connection_close.py (MIT, Copyright (c) 2016 Zillow). The license
text is in LICENSE_ctds.txt next to this file. Behaviour is translated to
mssql_python's DB-API surface; ctds-specific API (ctds.Connection.spid wrappers,
FreeTDS timeouts, the warnings-based iterator) is not ported.
"""

import uuid

import pytest
from mssql_python import connect, InterfaceError, ProgrammingError


def _new_table_name():
    return f"ci_ctds_tx_{uuid.uuid4().hex[:10]}"


def _trancount(cursor):
    cursor.execute("SELECT @@TRANCOUNT")
    return cursor.fetchone()[0]


@pytest.fixture
def tx_table(conn_str):
    """Permanent table created and committed on an autocommit connection, dropped afterwards."""
    name = _new_table_name()
    admin = connect(conn_str, autocommit=True)
    admin.cursor().execute(f"CREATE TABLE dbo.{name} (ints INT NOT NULL)")
    try:
        yield name
    finally:
        admin.cursor().execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name}")
        admin.close()


def _values(conn_str, name):
    with connect(conn_str, autocommit=True) as check:
        cur = check.cursor()
        cur.execute(f"SELECT ints FROM dbo.{name} ORDER BY ints")
        return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# autocommit property
# ---------------------------------------------------------------------------


def test_autocommit_true_on_connect_reports_true(conn_str):
    with connect(conn_str, autocommit=True) as conn:
        assert conn.autocommit is True


def test_autocommit_false_on_connect_reports_false(conn_str):
    with connect(conn_str, autocommit=False) as conn:
        assert conn.autocommit is False


def test_autocommit_setter_round_trips_bool_values(conn_str):
    with connect(conn_str, autocommit=True) as conn:
        for value in (False, True, False, True):
            conn.autocommit = value
            assert conn.autocommit is value


@pytest.mark.parametrize("bad_value", ["", " ", "True", "False", object()])
def test_autocommit_setter_rejects_non_bool(conn_str, bad_value):
    with connect(conn_str, autocommit=True) as conn:
        with pytest.raises(TypeError):
            conn.autocommit = bad_value
        assert conn.autocommit is True


# ---------------------------------------------------------------------------
# Implicit transactions
# ---------------------------------------------------------------------------


def test_autocommit_off_opens_transaction_on_first_statement(conn_str):
    with connect(conn_str, autocommit=False) as conn:
        cur = conn.cursor()
        assert _trancount(cur) == 1
        conn.commit()
        assert _trancount(cur) == 1  # commit opens the next implicit transaction
        conn.rollback()


def test_autocommit_on_never_leaves_transaction_open(conn_str):
    with connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        assert _trancount(cur) == 0


def test_enabling_autocommit_commits_pending_work(conn_str, tx_table):
    conn = connect(conn_str, autocommit=False)
    try:
        conn.cursor().execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (1), (2), (3)")
        conn.autocommit = True
        assert conn.autocommit is True
        assert _values(conn_str, tx_table) == [1, 2, 3]
        assert _trancount(conn.cursor()) == 0
    finally:
        conn.close()


def test_commit_and_rollback_are_silent_in_autocommit_mode(conn_str):
    with connect(conn_str, autocommit=True) as conn:
        conn.commit()
        conn.rollback()
        assert _trancount(conn.cursor()) == 0


# ---------------------------------------------------------------------------
# commit / rollback visibility
# ---------------------------------------------------------------------------


def test_rollback_discards_only_work_since_last_commit(conn_str, tx_table):
    conn = connect(conn_str, autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (1)")
        conn.commit()
        cur.execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (2)")
        conn.rollback()
        cur.execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (3)")
        conn.commit()
    finally:
        conn.close()
    assert _values(conn_str, tx_table) == [1, 3]


def test_close_with_uncommitted_work_discards_it(conn_str, tx_table):
    conn = connect(conn_str, autocommit=False)
    conn.cursor().execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (7), (8)")
    conn.close()
    assert _values(conn_str, tx_table) == []


# ---------------------------------------------------------------------------
# Context manager (ctds test_connection___exit__)
# ---------------------------------------------------------------------------


def test_context_manager_commits_on_clean_exit(conn_str, tx_table):
    with connect(conn_str, autocommit=False) as conn:
        conn.cursor().execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (1), (2), (3)")
    assert _values(conn_str, tx_table) == [1, 2, 3]


def test_context_manager_rolls_back_on_python_exception(conn_str, tx_table):
    with pytest.raises(RuntimeError, match="something bad"):
        with connect(conn_str, autocommit=False) as conn:
            conn.cursor().execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (1), (2), (3)")
            raise RuntimeError("something bad")
    assert _values(conn_str, tx_table) == []


def test_context_manager_rolls_back_on_sql_error(conn_str, tx_table):
    with pytest.raises(ProgrammingError):
        with connect(conn_str, autocommit=False) as conn:
            cur = conn.cursor()
            cur.execute(f"INSERT INTO dbo.{tx_table} (ints) VALUES (1), (2), (3)")
            cur.execute("EXEC dbo.ctds_port_no_such_procedure")
    assert _values(conn_str, tx_table) == []


def test_context_manager_rolls_back_ddl_as_well(conn_str):
    name = _new_table_name()
    try:
        with pytest.raises(RuntimeError):
            with connect(conn_str, autocommit=False) as conn:
                conn.cursor().execute(f"CREATE TABLE dbo.{name} (ints INT)")
                raise RuntimeError("abort")
        with connect(conn_str, autocommit=True) as check:
            cur = check.cursor()
            cur.execute(f"SELECT OBJECT_ID('dbo.{name}', 'U')")
            assert cur.fetchone()[0] is None
    finally:
        with connect(conn_str, autocommit=True) as check:
            check.cursor().execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name}")


def test_context_manager_closes_connection_and_then_rejects_use(conn_str):
    with connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
    with pytest.raises(InterfaceError):
        conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT 1")
