"""Closing an Arrow stream from another thread while a reader is blocked.

A reader parked in the stream (waiting for the producer's next batch) must
not keep `close()` waiting: close cancels the producer first, the reader ends
promptly and the connection stays usable. The producer is stalled the way a
slow server stalls it: a row lock held by another connection.
"""

import threading
import time

import pytest

pa = pytest.importorskip("pyarrow")

from mssql_python import connect, ddbc_bindings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "ArrowStream"), reason="native Arrow API is Rust-port only"
)

NUMBERS = "SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b"
CLOSE_DEADLINE = 2.0


def _read_all(stream):
    stream.read_all()


def _read_arrow_reader(stream):
    for _ in pa.RecordBatchReader.from_stream(stream):
        pass


def _read_batches(stream):
    reader = pa.RecordBatchReader.from_stream(stream)
    while True:
        try:
            reader.read_next_batch()
        except StopIteration:
            return


def _locked_stream(conn, conn_str, cursor, table_name, rows, batch_size):
    """A stream whose last row waits on a lock held by the returned connection."""
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id int PRIMARY KEY)")
    cursor.execute(f"INSERT INTO {table} VALUES (1)")
    conn.commit()
    holder = connect(conn_str)
    holder.autocommit = False
    holder.cursor().execute(f"UPDATE {table} SET id = id WHERE id = 1")  # X lock, stays open
    cursor.execute(
        f"SELECT n FROM ({NUMBERS.format(n=rows)}) q UNION ALL "
        f"SELECT id FROM {table} WITH (READCOMMITTEDLOCK) OPTION (MAXDOP 1)"
    )
    return holder, cursor.arrow_stream(batch_size=batch_size)


@pytest.mark.parametrize("reader", [_read_all, _read_arrow_reader, _read_batches])
def test_close_from_another_thread_while_reader_blocked(
    conn, conn_str, cursor, table_name, reader
):
    holder, stream = _locked_stream(conn, conn_str, cursor, table_name, 30000, 10_000)
    try:
        outcome = {}

        def read():
            try:
                reader(stream)
                outcome["end"] = "clean"
            except BaseException as exc:  # noqa: BLE001 - checked below
                outcome["end"] = exc

        thread_a = threading.Thread(target=read, daemon=True)
        thread_a.start()
        time.sleep(1.5)  # reader took the ready batches and now waits on the lock
        assert thread_a.is_alive()

        started = time.perf_counter()
        thread_b = threading.Thread(target=stream.close, daemon=True)
        thread_b.start()
        thread_b.join(10)
        elapsed = time.perf_counter() - started
        assert not thread_b.is_alive(), "close() never returned"
        assert elapsed < CLOSE_DEADLINE, f"close() took {elapsed:.1f}s"

        thread_a.join(CLOSE_DEADLINE)
        assert not thread_a.is_alive(), "reader still blocked after close()"
        end = outcome["end"]
        assert end == "clean" or isinstance(end, Exception), end
        assert stream.closed

        holder.rollback()
        cursor.execute("SELECT 42")
        assert cursor.fetchone()[0] == 42
        conn.cursor().execute("SELECT 1").fetchone()
    finally:
        holder.close()


def test_closed_flag_does_not_wait_for_a_blocked_reader(conn, conn_str, cursor, table_name):
    holder, stream = _locked_stream(conn, conn_str, cursor, table_name, 30000, 10_000)
    try:

        def swallow():
            try:
                stream.read_all()
            except Exception:  # noqa: BLE001
                pass

        reader = threading.Thread(target=swallow, daemon=True)
        reader.start()
        time.sleep(1)
        probe = threading.Thread(target=lambda: (stream.closed, repr(stream)), daemon=True)
        probe.start()
        probe.join(CLOSE_DEADLINE)
        assert not probe.is_alive(), "`closed` waits for the blocked reader"
        stream.close()
        reader.join(CLOSE_DEADLINE)
        assert not reader.is_alive()
        holder.rollback()
    finally:
        holder.close()


def test_closing_an_old_stream_does_not_cancel_a_newer_query(conn, conn_str, cursor, table_name):
    """The cursor re-executes while the old stream's producer is still stuck in
    a read; closing the old stream must cancel only that old execution."""
    holder, stream = _locked_stream(conn, conn_str, cursor, table_name, 30000, 10_000)
    try:
        reader = threading.Thread(target=lambda: _swallow(stream), daemon=True)
        reader.start()
        time.sleep(1)
        outcome = {}

        def rerun():
            try:
                # The raw binding: cursor.execute() would first close the cursor,
                # which cancels the old execution by itself.
                outcome["ret"] = ddbc_bindings.DDBCSQLExecDirect(
                    cursor.hstmt, "WAITFOR DELAY '00:00:03'; SELECT 11"
                )
            except Exception as exc:  # noqa: BLE001
                outcome["error"] = exc

        runner = threading.Thread(target=rerun, daemon=True)
        runner.start()
        time.sleep(1)  # the new execute is waiting for the wire behind the producer
        stream.close()
        runner.join(30)
        reader.join(CLOSE_DEADLINE)
        assert outcome.get("ret") == 0, outcome  # SQL_SUCCESS, not HY008
        row = []
        assert ddbc_bindings.DDBCSQLFetchOne(cursor.hstmt, row) == 0 and row[0] == 11
        holder.rollback()
    finally:
        holder.close()


def _swallow(stream):
    try:
        stream.read_all()
    except Exception:  # noqa: BLE001
        pass
