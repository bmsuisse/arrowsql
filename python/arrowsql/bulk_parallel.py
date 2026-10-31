"""RUST-PORT: parallel bulk load.

One bulk-copy stream is limited by the client-side encoder, not by the server.
``write_arrow`` (Arrow sources) and ``write_rows`` (Python row tuples) cut the
source into slices, hand them to N connections, and run one bulk copy per
connection.

The target decides the policy (``_TARGET_QUERY`` detects it):

* heap: TABLOCK on every worker. Bulk-update locks are compatible with each
  other, so the streams load the same heap at once.
* clustered columnstore: parallel, without TABLOCK (a table lock serializes
  the loads; concurrent loads without it are the documented way to scale a
  columnstore load) and with batches of at least 102,400 rows so full
  rowgroups compress directly instead of landing in the delta store. Each
  worker's last partial batch ends up in an open delta rowgroup that the tuple
  mover compresses later.
* heap with nonclustered indexes, or a clustered rowstore index: one worker
  and a ``RuntimeWarning`` (measured: TABLOCK workers into a heap with a
  nonclustered index were slower than one).

Row order is not kept. Rows committed by a worker before another worker fails
stay in the table; the first error is raised.
"""

import itertools
import queue
import threading
import time
import warnings
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Optional

from arrowsql.db_connection import connect

# Cap on rows per slice. Smaller slices spread work across workers; larger
# ones amortize per-batch overhead.
_MAX_SLICE_ROWS = 65_536
_SLICES_PER_WORKER = 4
_POLL_SECONDS = 0.1
_END = object()

# One round trip: the target kind and the database recovery model. Index types:
# 1 clustered, 2 nonclustered, 5 clustered columnstore, 6 nonclustered columnstore.
_TARGET_QUERY = """
SELECT
    CASE WHEN OBJECT_ID(?) IS NULL THEN NULL
         WHEN EXISTS (SELECT 1 FROM sys.indexes
                      WHERE object_id = OBJECT_ID(?) AND type = 5) THEN 'columnstore'
         WHEN EXISTS (SELECT 1 FROM sys.indexes
                      WHERE object_id = OBJECT_ID(?) AND type = 1) THEN 'clustered'
         WHEN EXISTS (SELECT 1 FROM sys.indexes
                      WHERE object_id = OBJECT_ID(?) AND type IN (2, 6)) THEN 'heap_nci'
         ELSE 'heap' END,
    (SELECT recovery_model_desc FROM sys.databases WHERE database_id = DB_ID())
"""

# Fewest rows per batch for a columnstore load: smaller batches land in the delta store.
_COLUMNSTORE_MIN_BATCH = 102_400

_SINGLE_WORKER_REASON = {
    "clustered": "it has a clustered index",
    "heap_nci": "it is a heap with nonclustered indexes, where parallel TABLOCK loads are slower",
}


def write_arrow(
    connection_str: str,
    table: str,
    source: Any,
    workers: int = 4,
    batch_size: int = 0,
    table_lock: bool = True,
    timeout: int = 30,
    column_mappings: Optional[list] = None,
    keep_identity: bool = False,
    check_constraints: bool = False,
    keep_nulls: bool = False,
    fire_triggers: bool = False,
    **connect_kwargs: Any,
) -> dict:
    """Bulk-load an Arrow source into ``table`` over ``workers`` connections.

    ``source`` takes the same forms as :meth:`Cursor.bulkcopy_arrow`. With
    ``workers > 1`` the source is cut into slices that workers pull from a
    shared queue, so faster connections take more of the load. With
    ``workers > 1`` the target decides the lock: TABLOCK for a heap, none for a
    clustered columnstore (batches are raised to at least 102,400 rows), and
    ``workers`` falls back to 1 with a warning for other indexed targets
    (``table_lock`` then applies as given).

    Returns a dict with ``rows_copied``, ``batch_count``, ``elapsed_time``,
    ``rows_per_second`` and ``workers`` (connections used).

    Raises the first error from any worker or from reading the source. The
    other workers stop taking new slices. ``workers > 1`` needs pyarrow to cut
    the source.
    """
    _check_args(workers, batch_size, timeout)
    bulk_options = {
        "batch_size": batch_size,
        "timeout": timeout,
        "column_mappings": column_mappings,
        "keep_identity": keep_identity,
        "check_constraints": check_constraints,
        "keep_nulls": keep_nulls,
        "fire_triggers": fire_triggers,
    }
    started = time.perf_counter()

    if workers > 1:
        workers, table_lock, bulk_options = _apply_target_policy(
            "write_arrow", connection_str, table, workers, table_lock, bulk_options, connect_kwargs
        )
    if workers == 1:
        result = _load_single(connection_str, table, source, table_lock, bulk_options, connect_kwargs)
        return _summarize([result], 1, started)

    slice_rows = _slice_rows(source, workers)
    results = _load_parallel(
        connection_str,
        workers,
        _slices(source, slice_rows),
        lambda cursor, first, pieces: _load_arrow_pieces(
            cursor, table, first, pieces, table_lock, bulk_options
        ),
        connect_kwargs,
    )
    return _summarize(results, workers, started)


def write_rows(
    connection_str: str,
    table: str,
    rows: Any,
    workers: int = 4,
    batch_size: int = 0,
    timeout: int = 30,
    column_mappings: Optional[list] = None,
    keep_identity: bool = False,
    check_constraints: bool = False,
    keep_nulls: bool = False,
    fire_triggers: bool = False,
    **connect_kwargs: Any,
) -> dict:
    """Bulk-load row tuples into ``table`` over ``workers`` connections.

    ``rows`` is an iterable of tuples (the :meth:`Cursor.bulkcopy` input). A
    list or tuple is cut with plain slicing; any other iterable is read by a
    feeder thread in chunks. Workers pull chunks from a bounded queue and each
    runs one ``cursor.bulkcopy`` with TABLOCK.

    Converting Python values holds the GIL, so the speed-up is bounded by the
    share of a bulk copy spent on the network and the server. Row order is not
    kept. If a worker fails, the others stop taking new rows and the first
    error is raised; rows already committed stay in the table.

    Returns a dict with ``rows_copied``, ``batch_count``, ``elapsed_time``,
    ``rows_per_second`` and ``workers`` (connections used).
    """
    _check_args(workers, batch_size, timeout)
    bulk_options = {
        "batch_size": batch_size,
        "timeout": timeout,
        "column_mappings": column_mappings,
        "keep_identity": keep_identity,
        "check_constraints": check_constraints,
        "keep_nulls": keep_nulls,
        "fire_triggers": fire_triggers,
    }
    started = time.perf_counter()

    table_lock = True
    if workers > 1:
        workers, table_lock, bulk_options = _apply_target_policy(
            "write_rows", connection_str, table, workers, True, bulk_options, connect_kwargs
        )
    if workers == 1:
        with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
            with closing(conn.cursor()) as cursor:
                result = cursor.bulkcopy(table, rows, table_lock=table_lock, **bulk_options)
        return _summarize([result], 1, started)

    results = _load_parallel(
        connection_str,
        workers,
        _row_slices(rows, workers),
        lambda cursor, first, pieces: _load_row_pieces(
            cursor, table, first, pieces, table_lock, bulk_options
        ),
        connect_kwargs,
    )
    return _summarize(results, workers, started)


def _check_args(workers, batch_size, timeout):
    if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
        raise ValueError(f"workers must be a positive integer, got {workers!r}")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0:
        raise ValueError(f"batch_size must be a non-negative integer, got {batch_size!r}")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout < 0:
        raise ValueError(f"timeout must be a non-negative integer, got {timeout!r}")


def _apply_target_policy(caller, connection_str, table, workers, table_lock, bulk_options, connect_kwargs):
    """Return ``(workers, table_lock, bulk_options)`` for the target's kind."""
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(_TARGET_QUERY, (table, table, table, table))
            kind, recovery_model = cursor.fetchone()
    if recovery_model == "FULL":
        warnings.warn(
            f"{caller}: the database uses the FULL recovery model, so bulk loads are fully logged; "
            "BULK_LOGGED or SIMPLE is much faster for large loads",
            RuntimeWarning,
            stacklevel=3,
        )
    if kind in _SINGLE_WORKER_REASON:
        warnings.warn(
            f"{caller}: loading {table!r} with 1 worker because {_SINGLE_WORKER_REASON[kind]}",
            RuntimeWarning,
            stacklevel=3,
        )
        return 1, table_lock, bulk_options
    if kind == "columnstore":
        if bulk_options["batch_size"] < _COLUMNSTORE_MIN_BATCH:
            bulk_options = {**bulk_options, "batch_size": _COLUMNSTORE_MIN_BATCH}
        return workers, False, bulk_options
    return workers, True, bulk_options


def _slice_size(count, workers):
    """Slice size that gives each worker several slices to pull."""
    return max(1, min(_MAX_SLICE_ROWS, -(-count // (workers * _SLICES_PER_WORKER))))


def _slice_rows(source, workers):
    num_rows = getattr(source, "num_rows", None)
    if isinstance(num_rows, int) and num_rows > 0:
        return _slice_size(num_rows, workers)
    return _MAX_SLICE_ROWS


def _load_single(connection_str, table, source, table_lock, bulk_options, connect_kwargs):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        with closing(conn.cursor()) as cursor:
            return cursor.bulkcopy_arrow(table, source, table_lock=table_lock, **bulk_options)


def _load_parallel(connection_str, workers, pieces, load, connect_kwargs):
    """Feed ``pieces`` to ``workers`` connections. ``load(cursor, first, pieces)``
    runs one bulk copy over the pieces a worker pulls and returns its result."""
    stop = threading.Event()
    failures: list = []
    pending = queue.Queue(maxsize=workers * 4)

    def worker():
        try:
            return _load_worker(connection_str, pending, stop, load, connect_kwargs)
        except BaseException as exc:
            failures.append(exc)
            stop.set()
            raise

    def feed():
        try:
            for piece in pieces:
                if not _put(pending, piece, stop):
                    return
        except BaseException as exc:
            failures.append(exc)
            stop.set()
        finally:
            for _ in range(workers):
                _put(pending, _END, stop)

    with ThreadPoolExecutor(max_workers=workers + 1, thread_name_prefix="mssql-bulk") as pool:
        futures = [pool.submit(worker) for _ in range(workers)]
        wait([pool.submit(feed), *futures])
    if failures:
        raise failures[0]
    results = [future.result() for future in futures]
    return [r for r in results if r]


def _load_worker(connection_str, pending, stop, load, connect_kwargs):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        with closing(conn.cursor()) as cursor:
            first = _next(pending, stop)
            if first is _END:
                return None

            def pieces():
                piece = first
                while piece is not _END:
                    yield piece
                    piece = _next(pending, stop)

            return load(cursor, first, pieces())


def _load_arrow_pieces(cursor, table, first, pieces, table_lock, bulk_options):
    import pyarrow as pa

    # A RecordBatchReader is an Arrow C stream, which the native bulk
    # copy consumes directly; a bare generator would need mssql_py_core.
    reader = pa.RecordBatchReader.from_batches(first.schema, pieces)
    return cursor.bulkcopy_arrow(table, reader, table_lock=table_lock, **bulk_options)


def _load_row_pieces(cursor, table, first, pieces, table_lock, bulk_options):
    rows = itertools.chain.from_iterable(pieces)
    return cursor.bulkcopy(table, rows, table_lock=table_lock, **bulk_options)


def _row_slices(rows, workers):
    """Yield lists of rows: plain slices of a list or tuple, else islice chunks."""
    if isinstance(rows, (list, tuple)):
        count = len(rows)
        size = _slice_size(count, workers)
        for offset in range(0, count, size):
            yield rows[offset : offset + size]
        return
    iterator = iter(rows)
    while True:
        chunk = list(itertools.islice(iterator, _MAX_SLICE_ROWS))
        if not chunk:
            return
        yield chunk


def _slices(source, slice_rows):
    """Yield non-empty RecordBatch slices of at most ``slice_rows`` rows."""
    for batch in _record_batches(source):
        for offset in range(0, batch.num_rows, slice_rows):
            yield batch.slice(offset, slice_rows)


def _record_batches(source):
    import pyarrow as pa

    if isinstance(source, pa.Table):
        return source.to_batches()
    if isinstance(source, pa.RecordBatch):
        return [source]
    if isinstance(source, pa.RecordBatchReader) or hasattr(source, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(source)
    if hasattr(source, "__arrow_c_array__"):
        return [pa.record_batch(source)]
    return source


def _put(pending, item, stop):
    """Put ``item`` unless the load was stopped. Returns False if stopped."""
    while not stop.is_set():
        try:
            pending.put(item, timeout=_POLL_SECONDS)
            return True
        except queue.Full:
            continue
    return False


def _next(pending, stop):
    """Next slice, or ``_END`` when the feed is done or the load was stopped."""
    while not stop.is_set():
        try:
            return pending.get(timeout=_POLL_SECONDS)
        except queue.Empty:
            continue
    return _END


def _summarize(results, workers, started):
    rows = sum(r.get("rows_copied", 0) for r in results)
    batches = sum(r.get("batch_count", 0) for r in results)
    elapsed = time.perf_counter() - started
    return {
        "rows_copied": rows,
        "batch_count": batches,
        "elapsed_time": elapsed,
        "rows_per_second": rows / elapsed if elapsed > 0 else 0.0,
        "workers": workers,
    }
