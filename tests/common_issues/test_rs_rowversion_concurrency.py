"""rowversion / timestamp: optimistic concurrency and the @@DBTS family.

Research sources (driver issue trackers and docs; findings noted per test):

* https://github.com/microsoft/mssql-python/issues/109
  "Is the mapping for timestamp to datetime.datetime correct?" The driver docs
  once mapped SQL Server ``timestamp`` to SQL_TIMESTAMP / ``datetime``. It is an
  alias for ``rowversion`` and is really an 8-byte ``binary(8)`` value. The
  rewrite must return ``bytes``, never ``datetime``.
* https://github.com/microsoft/data-formulator/issues/473
  Preview/import of tables with a TIMESTAMP/ROWVERSION column failed with
  "Explicit conversion of the timestamp data type to nvarchar(max) is not
  allowed". Reading the raw bytes avoids that path, and a client must not
  CAST the column to text.
* https://github.com/sqlalchemy/sqlalchemy/issues/8389
  "SQL Server UPDATE statement using ROWVERSION/TIMESTAMP is not working
  (Linux)". The version-id check depends on the UPDATE's rowcount and on the
  rowversion bytes round-tripping unchanged into the WHERE clause.
* https://bitbucket.org/zzzeek/sqlalchemy_old/issues/4086 (support ms-sql
  timestamp/rowversion) and the older pyodbc/SQLAlchemy ``version_id_col``
  report: rowcount from UPDATE must be trustworthy for stale-state detection.
* https://learn.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql
  rowversion is database-scoped, auto-generated, non-nullable, cannot be
  inserted explicitly, and changes on every write (even ``SET c = c``).
  ``@@DBTS`` returns the last value generated in the database.
* https://learn.microsoft.com/en-us/sql/t-sql/functions/min-active-rowversion-transact-sql
  MIN_ACTIVE_ROWVERSION() returns binary(8) and is the lowest rowversion still
  used by an active transaction (or the next value when none is active).
* https://github.com/mirek/msduck/issues/382 (capturing a rowversion contract)
  Reinforces: treat rowversion as opaque 8-byte value; do not interpret it as a
  timestamp.
"""

import pytest
from mssql_python.exceptions import IntegrityError


def _rv_int(cursor, rv_bytes):
    """Big-endian integer value of a rowversion, computed by SQL Server."""
    cursor.execute("SELECT CAST(? AS bigint)", (rv_bytes,))
    return cursor.fetchone()[0]


@pytest.fixture
def rv_table(cursor, conn):
    cursor.execute(
        "CREATE TABLE #orders (id int NOT NULL PRIMARY KEY, "
        "status nvarchar(20) NOT NULL, rv rowversion NOT NULL)"
    )
    cursor.execute("INSERT INTO #orders (id, status) VALUES (1, N'new'), (2, N'new'), (3, N'paid')")
    conn.commit()
    yield cursor


def _rv_of(cursor, order_id):
    cursor.execute("SELECT rv FROM #orders WHERE id = ?", (order_id,))
    return cursor.fetchone()[0]


def test_rowversion_fetched_as_8_byte_bytes(rv_table):
    """rowversion must surface as raw ``bytes`` of length 8, not datetime or str."""
    rv = _rv_of(rv_table, 1)
    assert type(rv) is bytes
    assert len(rv) == 8


def test_rowversion_values_unique_per_row(rv_table):
    values = []
    for order_id in (1, 2, 3):
        values.append(_rv_of(rv_table, order_id))
    assert len(set(values)) == 3


def test_rowversion_auto_generated_and_increasing_on_insert(rv_table, conn):
    rv_table.execute("INSERT INTO #orders (id, status) VALUES (10, N'x')")
    conn.commit()
    first = _rv_int(rv_table, _rv_of(rv_table, 1))
    later = _rv_int(rv_table, _rv_of(rv_table, 10))
    assert later > first


def test_update_bumps_rowversion(rv_table, conn):
    before = _rv_of(rv_table, 1)
    rv_table.execute("UPDATE #orders SET status = ? WHERE id = ?", ("paid", 1))
    conn.commit()
    after = _rv_of(rv_table, 1)
    assert after != before
    assert _rv_int(rv_table, after) > _rv_int(rv_table, before)


def test_noop_update_still_bumps_rowversion(rv_table, conn):
    """``SET status = status`` still writes the row, so rowversion must change."""
    before = _rv_of(rv_table, 2)
    rv_table.execute("UPDATE #orders SET status = status WHERE id = 2")
    conn.commit()
    assert _rv_of(rv_table, 2) != before


def test_select_does_not_change_rowversion(rv_table):
    first = _rv_of(rv_table, 3)
    second = _rv_of(rv_table, 3)
    assert first == second


def test_rowversion_update_matches_zero_rows_when_stale(rv_table, conn):
    """Classic optimistic-concurrency check: the second writer's rowcount is 0."""
    stale = _rv_of(rv_table, 1)
    # Another "client" updates first.
    rv_table.execute("UPDATE #orders SET status = N'shipped' WHERE id = 1")
    conn.commit()

    rv_table.execute(
        "UPDATE #orders SET status = N'cancelled' WHERE id = ? AND rv = ?",
        (1, stale),
    )
    assert rv_table.rowcount == 0
    conn.commit()
    rv_table.execute("SELECT status FROM #orders WHERE id = 1")
    assert rv_table.fetchone()[0] == "shipped"


def test_rowversion_update_matches_one_row_when_fresh(rv_table, conn):
    current = _rv_of(rv_table, 2)
    rv_table.execute(
        "UPDATE #orders SET status = N'paid' WHERE id = ? AND rv = ?",
        (2, current),
    )
    assert rv_table.rowcount == 1
    conn.commit()
    rv_table.execute("SELECT status FROM #orders WHERE id = 2")
    assert rv_table.fetchone()[0] == "paid"


def test_bytes_param_equals_fetched_rowversion_in_where(rv_table):
    rv = _rv_of(rv_table, 3)
    rv_table.execute("SELECT COUNT(*) FROM #orders WHERE rv = ?", (rv,))
    assert rv_table.fetchone()[0] == 1


def test_bytes_param_with_cast_to_binary8_matches(rv_table):
    rv = _rv_of(rv_table, 3)
    rv_table.execute("SELECT COUNT(*) FROM #orders WHERE rv = CAST(? AS binary(8))", (rv,))
    assert rv_table.fetchone()[0] == 1


def test_hex_string_param_converted_to_binary8_matches(rv_table):
    """Hex text round-trip: CONVERT(binary(8), '0x...', 1) must equal the stored value."""
    rv = _rv_of(rv_table, 3)
    hex_text = "0x" + rv.hex().upper()
    rv_table.execute(
        "SELECT COUNT(*) FROM #orders WHERE rv = CONVERT(binary(8), ?, 1)", (hex_text,)
    )
    assert rv_table.fetchone()[0] == 1


def test_explicit_insert_into_rowversion_rejected(rv_table, conn):
    with pytest.raises(IntegrityError):
        rv_table.execute(
            "INSERT INTO #orders (id, status, rv) VALUES (?, ?, ?)",
            (99, "x", b"\x00\x00\x00\x00\x00\x00\x00\x01"),
        )
    conn.rollback()


def test_rollback_restores_previous_rowversion(rv_table, conn):
    before = _rv_of(rv_table, 1)
    rv_table.execute("UPDATE #orders SET status = N'void' WHERE id = 1")
    assert _rv_of(rv_table, 1) != before
    conn.rollback()
    assert _rv_of(rv_table, 1) == before


@pytest.fixture
def dbts_table(cursor, conn, table_name):
    """@@DBTS tracks the current database, so DBTS tests need a permanent table."""
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id int NOT NULL PRIMARY KEY, "
        "status nvarchar(20) NOT NULL, rv rowversion NOT NULL)"
    )
    cursor.execute(f"INSERT INTO dbo.{table_name} (id, status) VALUES (1, 'new'), (3, 'paid')")
    conn.commit()
    yield table_name


def test_dbts_equals_rowversion_of_last_write(cursor, conn, dbts_table):
    """@@DBTS is the most recently generated rowversion in the database."""
    cursor.execute(f"UPDATE dbo.{dbts_table} SET status = 'done' WHERE id = 3")
    cursor.execute("SELECT @@DBTS")
    dbts = cursor.fetchone()[0]
    assert type(dbts) is bytes
    cursor.execute(f"SELECT rv FROM dbo.{dbts_table} WHERE id = 3")
    assert dbts == cursor.fetchone()[0]
    conn.commit()


def test_dbts_is_binary8_and_advances_with_writes(cursor, conn, dbts_table):
    cursor.execute("SELECT @@DBTS")
    before = cursor.fetchone()[0]
    cursor.execute(f"UPDATE dbo.{dbts_table} SET status = 'a' WHERE id = 1")
    cursor.execute("SELECT @@DBTS")
    after = cursor.fetchone()[0]
    assert len(after) == 8
    assert _rv_int(cursor, after) > _rv_int(cursor, before)
    conn.commit()


def test_min_active_rowversion_bounded_by_dbts(rv_table, conn):
    """MIN_ACTIVE_ROWVERSION() is at most one past @@DBTS and never negative."""
    rv_table.execute("SELECT MIN_ACTIVE_ROWVERSION(), @@DBTS")
    min_active, dbts = rv_table.fetchone()
    assert type(min_active) is bytes and len(min_active) == 8
    assert 0 <= _rv_int(rv_table, min_active) <= _rv_int(rv_table, dbts) + 1
    conn.commit()


def test_timestamp_alias_behaves_as_rowversion(cursor, conn):
    """The deprecated ``timestamp`` spelling is the same type as ``rowversion``."""
    cursor.execute("CREATE TABLE #ts_alias (id int PRIMARY KEY, ts timestamp NOT NULL)")
    cursor.execute("INSERT INTO #ts_alias (id) VALUES (1)")
    conn.commit()
    cursor.execute("SELECT ts FROM #ts_alias WHERE id = 1")
    ts = cursor.fetchone()[0]
    assert type(ts) is bytes
    assert len(ts) == 8
    cursor.execute("SELECT COUNT(*) FROM #ts_alias WHERE ts = ?", (ts,))
    assert cursor.fetchone()[0] == 1


def test_executemany_update_with_rowversion_guard(rv_table, conn):
    """Batch optimistic update: each row guarded by its own rowversion."""
    guards = [(1, _rv_of(rv_table, 1)), (2, _rv_of(rv_table, 2))]
    rv_table.execute("UPDATE #orders SET status = N'bumped' WHERE id = 2")
    conn.commit()
    updated = 0
    for order_id, rv in guards:
        rv_table.execute(
            "UPDATE #orders SET status = N'batch' WHERE id = ? AND rv = ?",
            (order_id, rv),
        )
        updated += rv_table.rowcount
    conn.commit()
    assert updated == 1  # id 1 still matches its guard, id 2 was stale
    rv_table.execute("SELECT id, status FROM #orders WHERE id IN (1, 2) ORDER BY id")
    assert [tuple(r) for r in rv_table.fetchall()] == [(1, "batch"), (2, "bumped")]
