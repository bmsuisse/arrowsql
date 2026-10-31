"""RUST-PORT: partitioned parallel Arrow reads.

One TDS session delivers a result at whatever rate SQL Server serializes it
(about 1.4M rows/s for a 20-column table on a dev box), well below what the
Rust decoder can take. ``read_arrow`` splits the query on an integer column
into ranges and reads them over separate connections at once; decoding runs
without the GIL, so plain threads scale. The parts are concatenated into one
native ``ArrowTable`` (row order across partitions is not preserved).

The range is cut into more slices than workers; each worker holds one
connection and pulls slices from a queue, so one slow connection does not set
the wall time. The MIN/MAX probe runs on the first worker's connection.
"""

import math
import queue
import re
import threading
from typing import Any, List, Optional, Tuple

from mssql_python import ddbc_bindings
from mssql_python.db_connection import connect

# Slices per worker, and the fewest rows worth a slice (when the row count is known).
_SLICES_PER_WORKER = 2
_MIN_SLICE_ROWS = 8 * 1024
# A result of at most this many rows (catalog estimate) is read over the probe
# connection alone (measured: parallel wins from about 20k rows up).
_MIN_PARALLEL_ROWS = 20_000
_NAME = r"(?:\[[^\]]+\]|\w+)"
_PLAIN_TABLE = re.compile(
    rf"^\s*SELECT\s+(?:\*|[\w\s,.\[\]]+?)\s+FROM\s+({_NAME}(?:\.{_NAME}){{0,2}})\s*;?\s*$",
    re.IGNORECASE,
)


def _quote_identifier(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def read_arrow(
    connection_str: str,
    query: str,
    partition_on: Optional[str] = None,
    partitions: int = 1,
    batch_size: int = 65536,
    **connect_kwargs: Any,
) -> "ddbc_bindings.ArrowTable":
    """Run ``query`` and return its whole result as a native ``ArrowTable``.

    With ``partition_on`` (an integer column of the result) and
    ``partitions`` > 1, the column's [min, max] range is cut into equal-width
    slices (about twice ``partitions``, never more than the number of distinct
    key values in the range) that ``partitions`` connections read in parallel.
    Rows whose partition value is NULL are read as a slice of their own. A
    result known to be small (estimated from the catalog when ``query`` is a
    plain table) is read over a single connection. ``query`` becomes a derived
    table, so it may not end in ORDER BY (without TOP) or contain ``?``
    parameters.

    Slices are equal in key width, not in rows: a skewed key (a few hot values,
    big gaps) is not rebalanced, and a column with fewer distinct values than
    ``partitions`` cannot use all connections. The partitions are separate
    statements on separate connections, so they read at different points in
    time: the result is not one snapshot, and rows changed concurrently may be
    missed or read twice.
    """
    if partitions <= 1 or not partition_on:
        return _read(connection_str, query, (), batch_size, connect_kwargs)

    column = _quote_identifier(partition_on)
    with connect(connection_str, autocommit=True, **connect_kwargs) as probe_conn:
        work = _plan(probe_conn, query, column, partition_on, partitions)
        if not work:
            return _fetch(probe_conn, query, (), batch_size)
        tables = _run_slices(
            probe_conn, connection_str, connect_kwargs, work, partitions, batch_size
        )
    return ddbc_bindings.ArrowTable.concat(tables)


def _run_slices(probe_conn, connection_str, connect_kwargs, work, partitions, batch_size):
    """Read ``work`` over the probe connection plus up to partitions-1 extra ones.

    Extra workers connect in the background and start pulling from the queue
    when ready; if the queue is already empty by then they leave, and the call
    does not wait for them. Slices are returned in plan order.
    """
    pending: "queue.Queue[Tuple[int, str, tuple]]" = queue.Queue()
    for index, (sql, params) in enumerate(work):
        pending.put((index, sql, params))
    results: List[Any] = [None] * len(work)
    state = threading.Condition()
    left = len(work)
    error: List[BaseException] = []

    def fail(exc: BaseException) -> None:
        with state:
            error.append(exc)
            state.notify_all()

    def drain(conn) -> None:
        nonlocal left
        while not error:
            try:
                index, sql, params = pending.get_nowait()
            except queue.Empty:
                return
            table = _fetch(conn, sql, params, batch_size)
            with state:
                results[index] = table
                left -= 1
                state.notify_all()

    def extra_worker() -> None:
        try:
            if pending.empty() or error:
                return
            with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
                drain(conn)
        except BaseException as exc:
            fail(exc)

    threads = [
        threading.Thread(target=extra_worker, name="mssql-read-arrow", daemon=True)
        for _ in range(min(partitions, len(work)) - 1)
    ]
    for thread in threads:
        thread.start()
    try:
        drain(probe_conn)
        with state:
            while left and not error:
                state.wait()
    except BaseException as exc:
        fail(exc)
        raise
    finally:
        if error:
            # Stop the others; they finish the slice in flight and exit.
            for thread in threads:
                thread.join()
    if error:
        raise error[0]
    return results


_NULLABLE_SQL = (
    "(SELECT TOP 1 is_nullable FROM sys.dm_exec_describe_first_result_set(?, NULL, 0))"
)
_ROWS_SQL = (
    "(SELECT SUM(row_count) FROM sys.dm_db_partition_stats "
    "WHERE object_id = OBJECT_ID(?) AND index_id IN (0, 1))"
)


def _probe_meta(cursor, query, column) -> Tuple[Optional[int], bool]:
    """(estimated row count or None, column may hold NULL) in one round trip.

    The row count comes from the catalog and only when ``query`` is a plain
    ``SELECT cols FROM table`` (a count scan is not worth it just to size the
    plan); filters, joins, views or missing permissions give None. A failed
    nullability check counts as nullable.
    """
    describe = (f"SELECT {column} FROM ({query}) AS _q",)
    match = _PLAIN_TABLE.match(query)
    if match:
        try:
            cursor.execute(f"SELECT {_ROWS_SQL}, {_NULLABLE_SQL}", (match.group(1),) + describe)
            rows, nullable = cursor.fetchone()
            return (None if rows is None else int(rows)), nullable is None or bool(nullable)
        except Exception:
            pass
    try:
        cursor.execute(f"SELECT {_NULLABLE_SQL}", describe)
        row = cursor.fetchone()
        return None, row is None or row[0] is None or bool(row[0])
    except Exception:
        return None, True


def _plan(conn, query, column, name, partitions) -> List[Tuple[str, tuple]]:
    """Probe [min, max]; return the slices, or [] to read in one stream."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM ({query}) AS _q")
        low, high = cursor.fetchone()
        if low is None:
            return []
        if not isinstance(low, int) or not isinstance(high, int):
            raise TypeError(f"partition_on column {name!r} must be an integer column")
        span = high - low + 1
        count = partitions * _SLICES_PER_WORKER
        rows, has_null = _probe_meta(cursor, query, column)
        if rows is not None:
            if rows <= _MIN_PARALLEL_ROWS:
                return []
            count = min(count, max(rows // _MIN_SLICE_ROWS, 2))
        # Cannot cut finer than one key value per slice. The size of the key
        # range says nothing about the row count (many rows can share a key),
        # so it never makes the read single-stream except for a single value.
        count = min(count, span)
        if count < 2:
            return []
    finally:
        cursor.close()
    step = math.ceil(span / count)
    edges = [low + i * step for i in range(1, count) if low + i * step <= high]
    base = f"SELECT * FROM ({query}) AS _q WHERE "
    work: List[Tuple[str, tuple]] = []
    if has_null:
        work.append((base + f"{column} IS NULL", ()))
    # Open-ended first and last ranges so out-of-range rows cannot be lost.
    work.append((base + f"{column} < ?", (edges[0],)))
    for a, b in zip(edges, edges[1:]):
        work.append((base + f"{column} >= ? AND {column} < ?", (a, b)))
    work.append((base + f"{column} >= ?", (edges[-1],)))
    return work


def _fetch(conn, sql, params, batch_size):
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params) if params else cursor.execute(sql)
        return cursor.fetchall_arrow(batch_size)
    finally:
        cursor.close()


def _read(connection_str, sql, params, batch_size, connect_kwargs):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        return _fetch(conn, sql, params, batch_size)
