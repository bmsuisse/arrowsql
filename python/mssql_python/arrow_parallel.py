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
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Tuple

from mssql_python import ddbc_bindings
from mssql_python.db_connection import connect

# Slices per worker, and the fewest rows (by key-range estimate) worth a slice.
_SLICES_PER_WORKER = 2
_MIN_SLICE_ROWS = 16 * 1024
# A key range up to this size is read over the probe connection alone (measured:
# parallel wins from about 20k rows up).
_MIN_PARALLEL_ROWS = 20_000


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
    ``partitions`` > 1, the column's [min, max] range is cut into slices that
    ``partitions`` connections read in parallel. Rows whose partition value is
    NULL are read as a slice of their own. A small or empty result is read
    over a single connection. ``query`` becomes a derived table, so it may not
    end in ORDER BY (without TOP) or contain ``?`` parameters.

    The partitions are separate statements on separate connections, so they
    read at different points in time: the result is not one snapshot, and rows
    changed concurrently may be missed or read twice.
    """
    if partitions <= 1 or not partition_on:
        return _read(connection_str, query, (), batch_size, connect_kwargs)

    column = _quote_identifier(partition_on)
    with connect(connection_str, autocommit=True, **connect_kwargs) as probe_conn:
        work = _plan(probe_conn, query, column, partition_on, partitions)
        if not work:
            return _fetch(probe_conn, query, (), batch_size)

        pending: "queue.Queue[Tuple[str, tuple]]" = queue.Queue()
        for item in work:
            pending.put(item)
        abort = threading.Event()

        def drain(conn) -> List[Any]:
            tables = []
            try:
                while not abort.is_set():
                    try:
                        sql, params = pending.get_nowait()
                    except queue.Empty:
                        break
                    tables.append(_fetch(conn, sql, params, batch_size))
            except BaseException:
                abort.set()
                raise
            return tables

        def extra_worker() -> List[Any]:
            try:
                with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
                    return drain(conn)
            except BaseException:
                abort.set()
                raise

        extra = min(partitions, len(work)) - 1
        with ThreadPoolExecutor(max_workers=max(extra, 1), thread_name_prefix="mssql-read-arrow") as pool:
            futures = [pool.submit(extra_worker) for _ in range(extra)]
            try:
                tables = drain(probe_conn)
            finally:
                for future in futures:
                    future.exception()
            for future in futures:
                tables.extend(future.result())
    return ddbc_bindings.ArrowTable.concat(tables)


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
        count = min(partitions * _SLICES_PER_WORKER, span // _MIN_SLICE_ROWS)
        if span <= _MIN_PARALLEL_ROWS or count < 2:
            return []
        has_null = _is_nullable(cursor, query, column)
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


def _is_nullable(cursor, query, column) -> bool:
    """Whether the column can hold NULL; unknown counts as nullable."""
    try:
        cursor.execute(
            "SELECT is_nullable FROM sys.dm_exec_describe_first_result_set(?, NULL, 0)",
            (f"SELECT {column} FROM ({query}) AS _q",),
        )
        row = cursor.fetchone()
        return row is None or row[0] is None or bool(row[0])
    except Exception:
        return True


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
