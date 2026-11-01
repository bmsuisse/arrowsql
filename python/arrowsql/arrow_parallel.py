"""RUST-PORT: partitioned parallel Arrow reads.

One TDS session delivers a result at whatever rate SQL Server serializes it
(about 1.4M rows/s for a 20-column table on a dev box), well below what the
Rust decoder can take. ``read_arrow`` / ``read_arrow_stream`` split the query
on a column into ranges and read them over separate connections at once;
decoding runs without the GIL, so plain threads scale.

The range is cut into more slices than workers; each worker holds one
connection and pulls slices from a queue, so one slow connection does not set
the wall time. The MIN/MAX probe runs on the first worker's connection.

``read_arrow_stream`` merges the workers' batches (native
``ddbc_bindings.ArrowMergeStream``, bounded buffer, unordered) so a consumer
overlaps with the read; ``read_arrow`` is that stream read into one
``ArrowTable``.
"""

import datetime
import math
from contextlib import closing
from decimal import Decimal
import queue
import re
import threading
import weakref
from typing import Any, List, Optional, Sequence, Tuple

from arrowsql import ddbc_bindings
from arrowsql.db_connection import connect
from arrowsql._utf8_strings import utf8_query
from arrowsql.exceptions import raise_exception

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
_STREAM_ERROR = re.compile(r"\[(\w{5})\] (.*)", re.DOTALL)
_BOUND_TYPES = (int, float, Decimal, str, datetime.date, datetime.datetime, datetime.time, bytes)


def _quote_identifier(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def read_arrow(
    connection_str: str,
    query: str,
    partition_on: Optional[str] = None,
    partitions: int = 1,
    batch_size: int = 65536,
    utf8_strings: bool = True,
    partition_bounds: Optional[Sequence[Any]] = None,
    partition_nullable: Optional[bool] = None,
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

    ``partition_bounds`` (typed boundaries, any column type) replaces the
    planning; see ``read_arrow_stream``.

    Slices are equal in key width, not in rows: a skewed key (a few hot values,
    big gaps) is not rebalanced, and a column with fewer distinct values than
    ``partitions`` cannot use all connections. The partitions are separate
    statements on separate connections, so they read at different points in
    time: the result is not one snapshot, and rows changed concurrently may be
    missed or read twice. Row order across partitions is not preserved.

    With ``utf8_strings`` (the default) nvarchar/nchar/ntext columns are cast
    to UTF-8 varchar on the server, which halves their bytes on the wire for
    mostly-ASCII text; the Arrow result is the same. It needs SQL Server 2019
    or Azure SQL and a ``query`` without ORDER BY; otherwise, or when the
    query cannot be described, the query runs unchanged. Pass False to
    disable. Costs two extra round trips per call.
    """
    if not _partitioned(partition_on, partitions, partition_bounds):
        return _read(connection_str, query, (), batch_size, connect_kwargs, utf8_strings)
    probe_conn, work = _open_plan(
        connection_str, query, partition_on, partitions, partition_bounds,
        partition_nullable, connect_kwargs, utf8_strings,
    )
    if len(work) == 1:
        with closing(probe_conn):
            return _fetch(probe_conn, work[0][0], work[0][1], batch_size)
    stream = _start(probe_conn, connection_str, connect_kwargs, work, partitions, batch_size, None)
    try:
        return stream.read_all()
    finally:
        stream.close()


def read_arrow_stream(
    connection_str: str,
    query: str,
    partition_on: Optional[str] = None,
    partitions: int = 1,
    batch_size: int = 65536,
    partition_bounds: Optional[Sequence[Any]] = None,
    max_buffered_batches: Optional[int] = None,
    partition_nullable: Optional[bool] = None,
    utf8_strings: bool = True,
    **connect_kwargs: Any,
) -> "ParallelArrowStream":
    """Like ``read_arrow``, but stream the batches while the slices are read.

    Returns a ``ParallelArrowStream`` (``__arrow_c_stream__``, so DuckDB,
    polars, pyarrow and deltalake read it directly): up to ``partitions``
    connections decode slices without the GIL and feed one bounded buffer of
    ``max_buffered_batches`` batches (default ``2 * partitions``, at least 4),
    so the consumer works while the server is still sending and memory stays
    bounded. Batches arrive unordered. The stream is one-shot. ``close()``
    (also on leaving a ``with`` block, or when a consumer stops early) cancels
    the running statements, stops every worker and closes their connections.
    The first worker error reaches the consumer and stops the others.

    ``partition_bounds``: ascending boundaries of the ``partition_on`` column
    (``int``, ``date``/``datetime``, ``Decimal``, ``str``; one type). ``k``
    values give the disjoint half-open slices ``< b1``, ``[b1, b2)``, ...,
    ``>= bk`` (plus an ``IS NULL`` slice when the column is nullable), each
    bound as a typed parameter. No MIN/MAX or size probe is run, so the first
    batch comes sooner, and skew can be handled by passing quantiles. Bounds
    must ascend in the column's own ordering (for strings: its collation); a
    ``str`` bound is sent as nvarchar, so a ``varchar`` column is compared
    after an implicit conversion (no index seek). ``partition_nullable`` says
    whether the column may hold NULL (default: asked from the server, one
    metadata call; pass False to skip it). ``utf8_strings``: as for
    ``read_arrow``.
    """
    if not _partitioned(partition_on, partitions, partition_bounds):
        probe_conn = connect(connection_str, autocommit=True, **connect_kwargs)
        try:
            if utf8_strings:
                query = utf8_query(probe_conn, query)
        except BaseException:
            probe_conn.close()
            raise
        return _start(probe_conn, connection_str, connect_kwargs, [(query, ())], 1, batch_size, max_buffered_batches)
    probe_conn, work = _open_plan(
        connection_str, query, partition_on, partitions, partition_bounds,
        partition_nullable, connect_kwargs, utf8_strings,
    )
    return _start(
        probe_conn, connection_str, connect_kwargs, work, partitions, batch_size, max_buffered_batches
    )


def _partitioned(partition_on, partitions, partition_bounds) -> bool:
    if partition_bounds is not None and not partition_on:
        raise ValueError("partition_bounds needs partition_on")
    return bool(partition_on) and (partitions > 1 or partition_bounds is not None)


def _open_plan(connection_str, query, partition_on, partitions, partition_bounds, nullable, kwargs, utf8_strings):
    """Open the probe connection and plan the slices: (connection, [(sql, params)...])."""
    column = _quote_identifier(partition_on)
    conn = connect(connection_str, autocommit=True, **kwargs)
    try:
        plain = query  # the catalog row estimate needs the user's own text
        if utf8_strings:
            query = utf8_query(conn, query)
        if partition_bounds is not None:
            work = _bounds_plan(conn, query, column, partition_bounds, nullable)
        else:
            work = _plan(conn, query, column, partition_on, partitions, plain)
        return conn, work or [(query, ())]
    except BaseException:
        conn.close()
        raise


def _bounds_plan(conn, query, column, bounds, nullable) -> List[Tuple[str, tuple]]:
    """Slices for caller-supplied boundaries (no data probe)."""
    bounds = list(bounds)
    if not bounds:
        return []
    kind = type(bounds[0])
    for b in bounds:
        if isinstance(b, bool) or not isinstance(b, _BOUND_TYPES) or type(b) is not kind:
            raise TypeError(
                "partition_bounds must be non-NULL values of one type "
                f"(int, Decimal, str, date, datetime); got {b!r}"
            )
    for a, b in zip(bounds, bounds[1:]):
        if not a < b:
            raise ValueError(f"partition_bounds must be strictly ascending ({a!r} !< {b!r})")
    if nullable is None:
        with closing(conn.cursor()) as cursor:
            _, nullable = _probe_meta(cursor, query, column, with_rows=False)
    base = f"SELECT * FROM ({query}) AS _q WHERE "
    work: List[Tuple[str, tuple]] = []
    if nullable:
        work.append((base + f"{column} IS NULL", ()))
    work.append((base + f"{column} < ?", (bounds[0],)))
    for a, b in zip(bounds, bounds[1:]):
        work.append((base + f"{column} >= ? AND {column} < ?", (a, b)))
    work.append((base + f"{column} >= ?", (bounds[-1],)))
    return work


class _Workers:
    """Threads and open statements of one parallel stream."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.cursors: set = set()
        self.threads: List[threading.Thread] = []
        self.error: List[BaseException] = []

    def cancel_all(self) -> None:
        with self.lock:
            cursors = list(self.cursors)
        for cursor in cursors:
            try:
                cursor.cancel()
            except Exception:
                pass

    def stop(self, merge, wait: bool) -> None:
        """Close the merge, cancel statements (repeatedly: a cancel can precede its execute)."""
        merge.close()
        budget = 30.0
        while True:
            self.cancel_all()
            alive = [t for t in self.threads if t.is_alive() and t is not threading.current_thread()]
            if not alive or not wait or budget <= 0:
                return
            alive[0].join(0.05)
            budget -= 0.05


def _release(merge, workers) -> None:
    # Finalizer for a stream nobody read. One handed to a consumer stops itself.
    if not merge.in_use:
        workers.stop(merge, wait=False)


class ParallelArrowStream:
    """Batches of a partitioned read, arriving while the slices are read.

    Created by ``read_arrow_stream``. One-shot, unordered, bounded buffer.
    """

    def __init__(self, merge, workers: _Workers) -> None:
        self._merge = merge
        self._workers = workers
        self._finalizer = weakref.finalize(self, _release, merge, workers)

    @property
    def column_names(self) -> List[str]:
        return self._merge.column_names

    @property
    def num_columns(self) -> int:
        return self._merge.num_columns

    @property
    def closed(self) -> bool:
        return self._merge.closed

    def __arrow_c_schema__(self) -> Any:
        return self._merge.__arrow_c_schema__()

    def __arrow_c_stream__(self, requested_schema: Optional[Any] = None) -> Any:
        return self._merge.__arrow_c_stream__(requested_schema)

    def read_all(self) -> "ddbc_bindings.ArrowTable":
        """Read every remaining batch into an ``ArrowTable`` (Ctrl-C interrupts)."""
        try:
            return self._merge.read_all()
        except RuntimeError as exc:
            self.close()
            if self._workers.error:
                raise self._workers.error[0] from None
            # A read that failed mid-statement arrives as "[SQLSTATE] message".
            found = _STREAM_ERROR.search(str(exc))
            if found:
                raise_exception(found.group(1), found.group(2))
            raise
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Cancel running statements, stop the workers, close their connections."""
        self._workers.stop(self._merge, wait=True)

    def __enter__(self) -> "ParallelArrowStream":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"ParallelArrowStream(columns={self.column_names!r}, closed={self.closed})"


def _start(probe_conn, connection_str, connect_kwargs, work, partitions, batch_size, depth):
    """Run work[0] on the probe connection (for the schema), then start the workers."""
    try:
        cursor = probe_conn.cursor()
        sql, params = work[0]
        cursor.execute(sql, params) if params else cursor.execute(sql)
        first = cursor.arrow_stream(batch_size, 1)
    except BaseException:
        probe_conn.close()
        raise
    workers = _Workers()
    count = max(1, min(partitions, len(work)))
    merge = ddbc_bindings.ArrowMergeStream(first, count, depth or max(2 * count, 4))
    pending: "queue.Queue[Tuple[str, tuple]]" = queue.Queue()
    for item in work[1:]:
        pending.put(item)

    def drain(conn) -> None:
        while not merge.stopped:
            try:
                sql, params = pending.get_nowait()
            except queue.Empty:
                return
            cur = conn.cursor()
            with workers.lock:
                workers.cursors.add(cur)
            try:
                cur.execute(sql, params) if params else cur.execute(sql)
                merge.pump(cur.arrow_stream(batch_size, 1))
            finally:
                with workers.lock:
                    workers.cursors.discard(cur)
                cur.close()

    def run(first_stream, first_cursor, conn) -> None:
        try:
            if first_stream is not None:
                with workers.lock:
                    workers.cursors.add(first_cursor)
                try:
                    merge.pump(first_stream)
                finally:
                    with workers.lock:
                        workers.cursors.discard(first_cursor)
                    first_cursor.close()
                drain(conn)
            elif not (merge.stopped or pending.empty()):
                with connect(connection_str, autocommit=True, **connect_kwargs) as own:
                    drain(own)
        except BaseException as exc:
            if not merge.stopped:
                workers.error.append(exc)
                merge.fail(f"{type(exc).__name__}: {exc}")
        finally:
            try:
                if conn is not None:
                    conn.close()
            finally:
                merge.worker_done()

    jobs = [(first, cursor, probe_conn)] + [(None, None, None)] * (count - 1)
    for first_stream, first_cursor, conn in jobs:
        thread = threading.Thread(
            target=run, args=(first_stream, first_cursor, conn), name="mssql-read-arrow", daemon=True
        )
        workers.threads.append(thread)
        thread.start()
    return ParallelArrowStream(merge, workers)


_NULLABLE_SQL = (
    "(SELECT TOP 1 is_nullable FROM sys.dm_exec_describe_first_result_set(?, NULL, 0))"
)
_ROWS_SQL = (
    "(SELECT SUM(row_count) FROM sys.dm_db_partition_stats "
    "WHERE object_id = OBJECT_ID(?) AND index_id IN (0, 1))"
)


def _probe_meta(cursor, query, column, plain=None, with_rows: bool = True) -> Tuple[Optional[int], bool]:
    """(estimated row count or None, column may hold NULL) in one round trip.

    The row count comes from the catalog and only when ``query`` is a plain
    ``SELECT cols FROM table`` (a count scan is not worth it just to size the
    plan); filters, joins, views or missing permissions give None. A failed
    nullability check counts as nullable.
    """
    describe = (f"SELECT {column} FROM ({query}) AS _q",)
    match = _PLAIN_TABLE.match(plain if plain is not None else query) if with_rows else None
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


def _plan(conn, query, column, name, partitions, plain=None) -> List[Tuple[str, tuple]]:
    """Probe [min, max]; return the slices, or [] to read in one stream."""
    with closing(conn.cursor()) as cursor:
        cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM ({query}) AS _q")
        low, high = cursor.fetchone()
        if low is None:
            return []
        if not isinstance(low, int) or not isinstance(high, int):
            raise TypeError(f"partition_on column {name!r} must be an integer column")
        span = high - low + 1
        count = partitions * _SLICES_PER_WORKER
        rows, has_null = _probe_meta(cursor, query, column, plain)
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
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, params) if params else cursor.execute(sql)
        return cursor.fetchall_arrow(batch_size)


def _read(connection_str, sql, params, batch_size, connect_kwargs, utf8_strings=False):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        if utf8_strings and not params:
            sql = utf8_query(conn, sql)
        return _fetch(conn, sql, params, batch_size)
