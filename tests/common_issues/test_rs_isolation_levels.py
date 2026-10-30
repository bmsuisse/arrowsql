"""Transaction isolation levels on the driver.

Research sources (real reported problems / documented behavior this suite guards):
- https://github.com/mkleehammer/pyodbc/issues/137 (SET TRANSACTION ISOLATION LEVEL SNAPSHOT
  with autocommit=True followed by manual BEGIN TRANSACTION: users hit ordering problems)
- https://github.com/mkleehammer/pyodbc/discussions/1372 (pooling and session state with
  unixODBC/ODBC Driver 18, autocommit + pool: state carried between logical connections)
- https://github.com/mkleehammer/pyodbc/issues/1277 (unixODBC pool reset skipped, so SET
  options are not reset to defaults for the next user of a pooled connection)
- https://github.com/sqlalchemy/sqlalchemy/issues/8693 (sp_reset_connection not called on
  reused MSSQL pooled connections; isolation level leaks to next checkout)
- https://github.com/sqlalchemy/sqlalchemy/issues/4058 (autocommit as an isolation level for
  SQL Server pyodbc/pymssql; isolation must be set and reset per connection)
- https://github.com/dotnet/SqlClient/pull/4330 (sp_reset_connection does not reset the
  session isolation level; the driver must reset it on return to pool)
- https://github.com/hugr-lab/mssql-extension/issues/331 (SNAPSHOT update conflict surfaces
  as error 3960 instead of blocking; retries must cover it)
- https://www.sqlserverscience.com/t-sql/snapshot-isolation-msg-3951-adopting-safely/
  (Msg 3951: SNAPSHOT requested after the transaction already started at another level)
- https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-3960-database-engine-error
  (Msg 3960 update conflict)
- https://www.brentozar.com/archive/2013/01/implementing-snapshot-or-read-committed-snapshot-isolation-in-sql-server-a-guide/
  (ALLOW_SNAPSHOT_ISOLATION vs READ_COMMITTED_SNAPSHOT; RCSI readers do not block on writers)

Ground truth is the upstream ODBC-backed driver. Cross-connection tests use a permanent
table (``table_name``) and always roll back the writer before teardown, otherwise the
fixture's DROP TABLE would block on the writer's locks.

Isolation is verified through ``sys.dm_exec_sessions.transaction_isolation_level``
(1=RU, 2=RC, 3=RR, 4=SERIALIZABLE, 5=SNAPSHOT) rather than trusting the client's own
bookkeeping.
"""

import uuid

import pytest
from mssql_python import OperationalError, ProgrammingError, connect
from mssql_python.constants import ConstantsDDBC

ISO_SESSION_SQL = (
    "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
)

SET_ATTR_ISO = ConstantsDDBC.SQL_ATTR_TXN_ISOLATION.value
SQL_TXN_READ_UNCOMMITTED = ConstantsDDBC.SQL_TXN_READ_UNCOMMITTED.value
SQL_TXN_READ_COMMITTED = ConstantsDDBC.SQL_TXN_READ_COMMITTED.value
SQL_TXN_REPEATABLE_READ = ConstantsDDBC.SQL_TXN_REPEATABLE_READ.value
SQL_TXN_SERIALIZABLE = ConstantsDDBC.SQL_TXN_SERIALIZABLE.value

# sys.dm_exec_sessions.transaction_isolation_level codes
DM_UNSPECIFIED = 0
DM_READ_UNCOMMITTED = 1
DM_READ_COMMITTED = 2
DM_REPEATABLE_READ = 3
DM_SERIALIZABLE = 4
DM_SNAPSHOT = 5


def _session_isolation(conn) -> int:
    cur = conn.cursor()
    cur.execute(ISO_SESSION_SQL)
    value = cur.fetchone()[0]
    cur.close()
    return value


def _scalar(conn, sql, params=None):
    cur = conn.cursor()
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)
    value = cur.fetchone()[0]
    cur.close()
    return value


def _admin_conn(conn_str):
    """Autocommit admin connection for DDL and database option changes."""
    return connect(conn_str, autocommit=True)


def _ensure_snapshot_allowed(conn_str):
    admin = _admin_conn(conn_str)
    try:
        cur = admin.cursor()
        cur.execute("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON")
        cur.close()
    finally:
        admin.close()


def _disable_snapshot_allowed(conn_str):
    admin = _admin_conn(conn_str)
    try:
        cur = admin.cursor()
        cur.execute("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF")
        cur.close()
    finally:
        admin.close()


@pytest.fixture
def snapshot_db(conn_str):
    """Enable ALLOW_SNAPSHOT_ISOLATION on the private test database for one test."""
    _ensure_snapshot_allowed(conn_str)
    yield
    _disable_snapshot_allowed(conn_str)


def _create_counter_table(conn_str, table):
    admin = _admin_conn(conn_str)
    try:
        cur = admin.cursor()
        cur.execute(f"CREATE TABLE dbo.{table} (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)")
        cur.execute(f"INSERT INTO dbo.{table} (id, v) VALUES (1, 0)")
        cur.close()
    finally:
        admin.close()


def _drop_table(conn_str, table):
    admin = _admin_conn(conn_str)
    try:
        cur = admin.cursor()
        cur.execute(f"DROP TABLE IF EXISTS dbo.{table}")
        cur.close()
    finally:
        admin.close()


@pytest.fixture
def counter_table(conn_str):
    """Permanent table dbo.<name> holding one row (id=1, v=0); dropped after the test."""
    name = f"ci_iso_{uuid.uuid4().hex[:12]}"
    _create_counter_table(conn_str, name)
    yield name
    _drop_table(conn_str, name)


@pytest.mark.parametrize(
    "sql_level, expected",
    [
        ("READ UNCOMMITTED", DM_READ_UNCOMMITTED),
        ("READ COMMITTED", DM_READ_COMMITTED),
        ("REPEATABLE READ", DM_REPEATABLE_READ),
        ("SERIALIZABLE", DM_SERIALIZABLE),
    ],
)
def test_set_transaction_isolation_level_each_level(conn, sql_level, expected):
    cur = conn.cursor()
    cur.execute(f"SET TRANSACTION ISOLATION LEVEL {sql_level}")
    assert _session_isolation(conn) == expected
    assert type(_session_isolation(conn)) is int


def test_default_isolation_is_read_committed(conn):
    assert _session_isolation(conn) == DM_READ_COMMITTED


@pytest.mark.parametrize(
    "attr_level, expected",
    [
        (SQL_TXN_READ_UNCOMMITTED, DM_READ_UNCOMMITTED),
        (SQL_TXN_READ_COMMITTED, DM_READ_COMMITTED),
        (SQL_TXN_REPEATABLE_READ, DM_REPEATABLE_READ),
        (SQL_TXN_SERIALIZABLE, DM_SERIALIZABLE),
    ],
)
def test_set_attr_isolation_in_autocommit_mode(conn_str, attr_level, expected):
    conn = connect(conn_str, autocommit=True)
    try:
        conn.set_attr(SET_ATTR_ISO, attr_level)
        assert _session_isolation(conn) == expected
    finally:
        conn.close()


def test_set_attr_isolation_bogus_value_raises(conn_str):
    conn = connect(conn_str, autocommit=True)
    try:
        with pytest.raises(ProgrammingError):
            conn.set_attr(SET_ATTR_ISO, 9999)
        # The connection must still work after the rejected attribute.
        assert _session_isolation(conn) == DM_READ_COMMITTED
    finally:
        conn.close()


def test_isolation_survives_commit_and_rollback(conn_str):
    conn = connect(conn_str)
    try:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        conn.commit()
        assert _session_isolation(conn) == DM_SERIALIZABLE
        cur.execute("SELECT 1")
        conn.rollback()
        assert _session_isolation(conn) == DM_SERIALIZABLE
    finally:
        conn.close()


def test_new_connection_after_serializable_gets_read_committed(conn_str):
    # Whether the pool hands back the same physical session is not guaranteed, so the
    # assertion is on the observable contract: a fresh connection starts at READ COMMITTED.
    first = connect(conn_str, autocommit=True)
    first.cursor().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    assert _session_isolation(first) == DM_SERIALIZABLE
    first.close()

    second = connect(conn_str, autocommit=True)
    try:
        assert _session_isolation(second) == DM_READ_COMMITTED
    finally:
        second.close()


def test_read_uncommitted_sees_dirty_row_then_rollback_hides_it(conn_str, counter_table):
    writer = connect(conn_str)
    reader = connect(conn_str, autocommit=True)
    try:
        writer.cursor().execute(f"UPDATE dbo.{counter_table} SET v = 42 WHERE id = 1")
        reader.cursor().execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        dirty = _scalar(reader, f"SELECT v FROM dbo.{counter_table} WHERE id = 1")
        assert dirty == 42
        assert type(dirty) is int
        writer.rollback()
        after = _scalar(reader, f"SELECT v FROM dbo.{counter_table} WHERE id = 1")
        assert after == 0
    finally:
        writer.close()
        reader.close()


def test_repeatable_read_holds_shared_locks_so_writer_times_out(conn_str, counter_table):
    reader = connect(conn_str)
    writer = connect(conn_str, autocommit=True)
    try:
        rc = reader.cursor()
        rc.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        assert _scalar(reader, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 0
        wc = writer.cursor()
        wc.execute("SET LOCK_TIMEOUT 500")
        with pytest.raises(ProgrammingError) as info:
            wc.execute(f"UPDATE dbo.{counter_table} SET v = 1 WHERE id = 1")
        assert "time out" in str(info.value).lower() or "timeout" in str(info.value).lower()
        reader.rollback()
        wc.execute(f"UPDATE dbo.{counter_table} SET v = 1 WHERE id = 1")
        assert _scalar(writer, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 1
    finally:
        reader.close()
        writer.close()


def test_snapshot_reads_consistent_point_in_time(conn_str, counter_table, snapshot_db):
    reader = connect(conn_str, autocommit=True)
    writer = connect(conn_str, autocommit=True)
    try:
        rc = reader.cursor()
        rc.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        rc.execute("BEGIN TRANSACTION")
        assert _session_isolation(reader) == DM_SNAPSHOT
        assert _scalar(reader, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 0
        writer.cursor().execute(f"UPDATE dbo.{counter_table} SET v = 5 WHERE id = 1")
        # Snapshot reader keeps seeing its original version.
        assert _scalar(reader, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 0
        reader.cursor().execute("COMMIT")
        assert _scalar(reader, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 5
    finally:
        reader.close()
        writer.close()


def test_snapshot_update_conflict_raises_3960(conn_str, counter_table, snapshot_db):
    a = connect(conn_str, autocommit=True)
    b = connect(conn_str, autocommit=True)
    try:
        ca = a.cursor()
        ca.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        ca.execute("BEGIN TRANSACTION")
        assert _scalar(a, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 0
        b.cursor().execute(f"UPDATE dbo.{counter_table} SET v = 5 WHERE id = 1")
        with pytest.raises(ProgrammingError) as info:
            ca.execute(f"UPDATE dbo.{counter_table} SET v = 9 WHERE id = 1")
        message = str(info.value).lower()
        assert "snapshot isolation" in message
        assert "update conflict" in message
        # The server aborts the transaction itself: no open transaction remains and an
        # explicit ROLLBACK TRANSACTION now fails with "no corresponding BEGIN".
        assert _scalar(a, "SELECT @@TRANCOUNT") == 0
        with pytest.raises(OperationalError):
            ca.execute("ROLLBACK TRANSACTION")
        assert _scalar(b, f"SELECT v FROM dbo.{counter_table} WHERE id = 1") == 5
    finally:
        a.close()
        b.close()


def test_snapshot_started_after_read_raises_3951(conn_str, counter_table, snapshot_db):
    # Manual-commit connection: the first statement opens a READ COMMITTED transaction.
    conn = connect(conn_str)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        with pytest.raises(ProgrammingError) as info:
            cur.execute(f"SELECT v FROM dbo.{counter_table} WHERE id = 1")
        assert "did not start in snapshot isolation" in str(info.value).lower()
        conn.rollback()
    finally:
        conn.close()


def test_rcsi_reader_does_not_block_on_uncommitted_writer(conn_str, counter_table):
    admin = _admin_conn(conn_str)
    try:
        admin.cursor().execute(
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE"
        )
        assert (
            _scalar(admin, "SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME()")
            == 1
        )
    finally:
        admin.close()

    writer = connect(conn_str)
    reader = connect(conn_str)
    try:
        writer.cursor().execute(f"UPDATE dbo.{counter_table} SET v = 7 WHERE id = 1")
        rc = reader.cursor()
        rc.execute("SET LOCK_TIMEOUT 2000")
        rc.execute(f"SELECT v FROM dbo.{counter_table} WHERE id = 1")
        value = rc.fetchone()[0]
        assert value == 0
        assert type(value) is int
        assert _session_isolation(reader) == DM_READ_COMMITTED
        writer.rollback()
    finally:
        writer.close()
        reader.close()
        admin = _admin_conn(conn_str)
        try:
            admin.cursor().execute(
                "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT OFF WITH ROLLBACK IMMEDIATE"
            )
        finally:
            admin.close()


def test_read_committed_blocks_on_uncommitted_writer_until_lock_timeout(conn_str, counter_table):
    writer = connect(conn_str)
    reader = connect(conn_str, autocommit=True)
    try:
        writer.cursor().execute(f"UPDATE dbo.{counter_table} SET v = 3 WHERE id = 1")
        rc = reader.cursor()
        rc.execute("SET LOCK_TIMEOUT 300")
        with pytest.raises(ProgrammingError) as info:
            rc.execute(f"SELECT v FROM dbo.{counter_table} WHERE id = 1")
        assert "time out" in str(info.value).lower() or "timeout" in str(info.value).lower()
        writer.rollback()
    finally:
        writer.close()
        reader.close()


def test_isolation_applies_to_statements_after_set_in_manual_commit(conn_str):
    conn = connect(conn_str)
    try:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        assert _session_isolation(conn) == DM_REPEATABLE_READ
        conn.commit()
        assert _session_isolation(conn) == DM_REPEATABLE_READ
    finally:
        conn.close()


def test_invalid_isolation_level_sql_raises_programming_error(conn):
    cur = conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.execute("SET TRANSACTION ISOLATION LEVEL NOT_A_LEVEL")
    assert _session_isolation(conn) == DM_READ_COMMITTED
