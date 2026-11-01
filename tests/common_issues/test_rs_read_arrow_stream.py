"""read_arrow_stream: streaming parallel reads, typed bounds, cancellation, errors.

Rust-port only (native ArrowMergeStream), so the module skips upstream.
"""

import datetime
import os
import signal
import threading
import time
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")

import mssql_python  # noqa: E402
from mssql_python import ddbc_bindings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "ArrowMergeStream"), reason="native Arrow API is Rust-port only"
)

ROWS = 90_000


def _fill(cursor, name, nullable=True, rows=ROWS, nulls=0):
    null = "NULL" if nullable else "NOT NULL"
    cursor.execute(
        f"CREATE TABLE dbo.{name} (id bigint {null}, d date {null}, s nvarchar(20) {null}, "
        "n decimal(12,2) NULL, v int NOT NULL)"
    )
    cursor.execute(
        f"INSERT INTO dbo.{name} SELECT TOP ({rows}) r, DATEADD(day, r % 1000, '2020-01-01'), "
        "RIGHT('00000' + CAST(r % 5000 AS varchar(10)), 5), r / 7.0, r % 97 FROM "
        "(SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS r FROM sys.all_columns a CROSS JOIN sys.all_columns b) t"
    )
    for _ in range(nulls):
        cursor.execute(f"INSERT INTO dbo.{name} VALUES (NULL, NULL, NULL, NULL, -1)")
    cursor.connection.commit()


def _sorted(table):
    return pa.table(table).sort_by([("id", "ascending"), ("v", "ascending")])


def _sessions(cursor):
    cursor.execute(
        "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE host_process_id = ? AND is_user_process = 1",
        (os.getpid(),),
    )
    return cursor.fetchone()[0]


def _wait_sessions(cursor, expected, timeout=10.0):
    end = time.time() + timeout
    while _sessions(cursor) != expected and time.time() < end:
        time.sleep(0.1)
    return _sessions(cursor)


def _single(conn_str, query):
    return pa.table(mssql_python.read_arrow(conn_str, query))


@pytest.mark.parametrize("partitions", [1, 2, 8])
def test_stream_matches_single_with_nulls(conn_str, cursor, table_name, partitions):
    _fill(cursor, table_name, nulls=5)
    q = f"SELECT id, v FROM dbo.{table_name}"
    with mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partitions=partitions) as s:
        got = pa.table(s)
    assert got.num_rows == ROWS + 5
    assert got.column("id").null_count == 5
    assert _sorted(got).equals(_sorted(_single(conn_str, q)))


def test_read_arrow_built_on_stream_matches(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    q = f"SELECT id, v FROM dbo.{table_name}"
    got = pa.table(mssql_python.read_arrow(conn_str, q, partition_on="id", partitions=4))
    assert _sorted(got).equals(_sorted(_single(conn_str, q)))


def test_skewed_key(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id bigint NOT NULL, v int NOT NULL)")
    cursor.execute(
        f"INSERT INTO dbo.{table_name} SELECT CASE WHEN r % 10 = 0 THEN r ELSE 7 END, r % 97 FROM "
        "(SELECT TOP (60000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS r "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b) t"
    )
    cursor.connection.commit()
    q = f"SELECT id, v FROM dbo.{table_name}"
    got = pa.table(mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partitions=4))
    assert _sorted(got).equals(_sorted(_single(conn_str, q)))


def test_int_bounds_with_nulls_and_no_probe(conn_str, cursor, table_name):
    _fill(cursor, table_name, nulls=3)
    q = f"SELECT id, v FROM dbo.{table_name}"
    bounds = [1, 100, 30_000, 60_000]  # first slice < 1 is empty; last >= 60000
    got = pa.table(
        mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partitions=3, partition_bounds=bounds)
    )
    assert got.num_rows == ROWS + 3
    assert _sorted(got).equals(_sorted(_single(conn_str, q)))


def test_bounds_not_nullable_skips_null_slice(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False)
    q = f"SELECT id, v FROM dbo.{table_name}"
    got = mssql_python.read_arrow(
        conn_str, q, partition_on="id", partitions=2, partition_bounds=[45_000], partition_nullable=False
    )
    assert got.num_rows == ROWS


def test_date_bounds(conn_str, cursor, table_name):
    _fill(cursor, table_name, nulls=2)
    q = f"SELECT id, d, v FROM dbo.{table_name}"
    bounds = [datetime.date(2020, 3, 1), datetime.date(2021, 1, 1), datetime.date(2022, 1, 1)]
    got = pa.table(mssql_python.read_arrow(conn_str, q, partition_on="d", partitions=4, partition_bounds=bounds))
    assert got.num_rows == ROWS + 2
    assert _sorted(got).equals(_sorted(_single(conn_str, q)))


def test_datetime_and_decimal_bounds(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    q = f"SELECT id, d, n, v FROM dbo.{table_name}"
    got = mssql_python.read_arrow(
        conn_str, q, partition_on="d", partitions=2,
        partition_bounds=[datetime.datetime(2020, 6, 1, 12, 0)],
    )
    assert got.num_rows == ROWS
    got = mssql_python.read_arrow(
        conn_str, q, partition_on="n", partitions=3,
        partition_bounds=[Decimal("1000.50"), Decimal("5000.00")],
    )
    assert got.num_rows == ROWS


def test_string_bounds(conn_str, cursor, table_name):
    _fill(cursor, table_name, nulls=4)
    q = f"SELECT id, s, v FROM dbo.{table_name}"
    got = pa.table(
        mssql_python.read_arrow(
            conn_str, q, partition_on="s", partitions=3, partition_bounds=["01000", "02500", "04000"]
        )
    )
    assert got.num_rows == ROWS + 4
    assert _sorted(got).equals(_sorted(_single(conn_str, q)))


def test_bad_bounds(conn_str):
    q = "SELECT 1 AS id"
    with pytest.raises(ValueError):
        mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partition_bounds=[5, 5])
    with pytest.raises(ValueError):
        mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partition_bounds=[9, 1])
    with pytest.raises(TypeError):
        mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partition_bounds=[1, "a"])
    with pytest.raises(TypeError):
        mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partition_bounds=[None, 2])
    with pytest.raises(ValueError):
        mssql_python.read_arrow_stream(conn_str, q, partition_bounds=[1])


def test_bounded_buffer_and_backpressure(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    q = f"SELECT id, v FROM dbo.{table_name}"
    stream = mssql_python.read_arrow_stream(
        conn_str, q, partition_on="id", partitions=4, batch_size=1000, max_buffered_batches=2
    )
    try:
        time.sleep(1.0)  # workers fill the buffer and block; nothing runs away
        assert not stream.closed
        got = pa.table(stream)
        assert got.num_rows == ROWS
    finally:
        stream.close()


def test_early_close_cancels_workers_no_leaked_sessions(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    base = _sessions(cursor)
    q = f"SELECT id, v FROM dbo.{table_name}"
    stream = mssql_python.read_arrow_stream(
        conn_str, q, partition_on="id", partitions=4, batch_size=500, max_buffered_batches=2
    )
    reader = pa.RecordBatchReader.from_stream(stream)
    reader.read_next_batch()
    reader.close()
    stream.close()
    assert _wait_sessions(cursor, base) == base


def test_close_unread_stream_and_context_exit(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    base = _sessions(cursor)
    q = f"SELECT id, v FROM dbo.{table_name}"
    with mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partitions=4) as s:
        assert s.column_names == ["id", "v"]
    assert s.closed
    assert _wait_sessions(cursor, base) == base


def test_dropped_stream_releases_sessions(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    base = _sessions(cursor)
    s = mssql_python.read_arrow_stream(
        conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=4
    )
    del s
    assert _wait_sessions(cursor, base) == base


def test_consumer_stops_early_via_capsule_drop(conn_str, cursor, table_name):
    duckdb = pytest.importorskip("duckdb")
    _fill(cursor, table_name)
    base = _sessions(cursor)
    stream = mssql_python.read_arrow_stream(
        conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=4, batch_size=1000
    )
    assert duckdb.sql("SELECT id FROM stream LIMIT 5").fetchall()
    assert _wait_sessions(cursor, base) == base
    stream.close()


def test_duckdb_aggregate_over_stream(conn_str, cursor, table_name):
    duckdb = pytest.importorskip("duckdb")
    _fill(cursor, table_name, nulls=2)
    stream = mssql_python.read_arrow_stream(
        conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=4
    )
    n, total = duckdb.sql("SELECT count(*), sum(v) FROM stream").fetchall()[0]
    cursor.execute(f"SELECT count(*), sum(v) FROM dbo.{table_name}")
    assert (n, total) == tuple(cursor.fetchone())


def test_worker_error_surfaces(conn_str, cursor, table_name):
    # The last slice divides by zero: it fails on a worker thread, not in the call.
    _fill(cursor, table_name)
    cursor.execute(f"UPDATE dbo.{table_name} SET v = -1 WHERE id = {ROWS}")
    cursor.connection.commit()
    base = _sessions(cursor)
    q = f"SELECT id, 1 / (v + 1) AS x FROM dbo.{table_name}"
    stream = mssql_python.read_arrow_stream(conn_str, q, partition_on="id", partitions=4)
    with pytest.raises(Exception, match="(?i)divide by zero|arithmetic"):
        pa.table(stream)
    stream.close()
    assert _wait_sessions(cursor, base) == base


def test_read_arrow_worker_error_keeps_exception_type(conn_str, cursor, table_name):
    _fill(cursor, table_name)
    cursor.execute(f"UPDATE dbo.{table_name} SET v = -1 WHERE id = {ROWS}")
    cursor.connection.commit()
    q = f"SELECT id, 1 / (v + 1) AS x FROM dbo.{table_name}"
    with pytest.raises(mssql_python.Error):
        mssql_python.read_arrow(conn_str, q, partition_on="id", partitions=4)


def test_error_in_first_statement(conn_str):
    with pytest.raises(mssql_python.Error):
        mssql_python.read_arrow_stream(conn_str, "SELECT * FROM dbo.no_such_table_xyz", partition_on="id", partitions=4)


@pytest.mark.skipif(os.name == "nt", reason="needs SIGINT")
def test_ctrl_c_interrupts_read_all_and_cancels(conn_str, cursor, table_name):
    _fill(cursor, table_name, nullable=False)
    locker = mssql_python.connect(conn_str, autocommit=False)
    try:
        locker.cursor().execute(f"UPDATE dbo.{table_name} SET v = v WHERE id = {ROWS - 1}")
        base = _sessions(cursor)
        # The slice holding the locked row blocks its worker; the other one ends.
        stream = mssql_python.read_arrow_stream(
            conn_str, f"SELECT id, v FROM dbo.{table_name}", partition_on="id", partitions=2,
            partition_bounds=[ROWS // 2], partition_nullable=False,
        )
        timer = threading.Timer(1.0, lambda: os.kill(os.getpid(), signal.SIGINT))
        timer.start()
        started = time.time()
        try:
            with pytest.raises(KeyboardInterrupt):
                stream.read_all()
        finally:
            timer.cancel()
        assert time.time() - started < 10
        assert stream.closed
        assert _wait_sessions(cursor, base - 0) <= base
    finally:
        locker.rollback()
        locker.close()
