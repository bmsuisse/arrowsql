"""RUST-PORT: parallel Arrow bulk load into a heap.

One bulk-copy stream is limited by the client-side Arrow-to-TDS encoder, not by
the server. ``write_arrow`` cuts the source into slices, hands them to N
connections, and runs one ``cursor.bulkcopy_arrow`` per connection with
TABLOCK. Bulk-update locks are compatible with each other, so the streams load
the same heap at once.

Parallel loads need a heap. For a clustered or columnstore target the load runs
on one worker and a ``RuntimeWarning`` says why. Rows committed by a worker
before another worker fails stay in the table; the first error is raised.
"""

import queue
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Optional

from mssql_python.db_connection import connect

# Cap on rows per slice. Smaller slices spread work across workers; larger
# ones amortize per-batch overhead.
_MAX_SLICE_ROWS = 65_536
_SLICES_PER_WORKER = 4
_POLL_SECONDS = 0.1
_END = object()

_HEAP_QUERY = """
SELECT
    CASE WHEN OBJECT_ID(?) IS NULL THEN NULL
         WHEN EXISTS (SELECT 1 FROM sys.indexes
                      WHERE object_id = OBJECT_ID(?) AND type IN (1, 5)) THEN 0
         ELSE 1 END
"""


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
    shared queue, so faster connections take more of the load. ``table_lock``
    is forced on when ``workers > 1``.

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
        workers = _effective_workers(connection_str, table, workers, connect_kwargs)
    if workers == 1:
        result = _load_single(connection_str, table, source, table_lock, bulk_options, connect_kwargs)
        return _summarize([result], 1, started)

    slice_rows = _slice_rows(source, workers)
    results = _load_parallel(
        connection_str, table, source, workers, slice_rows, bulk_options, connect_kwargs
    )
    return _summarize(results, workers, started)


def _check_args(workers, batch_size, timeout):
    if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
        raise ValueError(f"workers must be a positive integer, got {workers!r}")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0:
        raise ValueError(f"batch_size must be a non-negative integer, got {batch_size!r}")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout < 0:
        raise ValueError(f"timeout must be a non-negative integer, got {timeout!r}")


def _effective_workers(connection_str, table, workers, connect_kwargs):
    """Return ``workers`` for a heap target, else 1 with a warning."""
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_HEAP_QUERY, (table, table))
            is_heap = cursor.fetchone()[0]
        finally:
            cursor.close()
    if is_heap == 0:
        warnings.warn(
            f"write_arrow: {table!r} is not a heap; loading with 1 worker "
            "(parallel loads need a heap)",
            RuntimeWarning,
            stacklevel=3,
        )
        return 1
    return workers


def _slice_rows(source, workers):
    """Pick a slice size that gives each worker several slices to pull."""
    num_rows = getattr(source, "num_rows", None)
    if isinstance(num_rows, int) and num_rows > 0:
        per_slice = -(-num_rows // (workers * _SLICES_PER_WORKER))
        return max(1, min(_MAX_SLICE_ROWS, per_slice))
    return _MAX_SLICE_ROWS


def _load_single(connection_str, table, source, table_lock, bulk_options, connect_kwargs):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        cursor = conn.cursor()
        try:
            return cursor.bulkcopy_arrow(table, source, table_lock=table_lock, **bulk_options)
        finally:
            cursor.close()


def _load_parallel(connection_str, table, source, workers, slice_rows, bulk_options, connect_kwargs):
    stop = threading.Event()
    failures: list = []
    pending = queue.Queue(maxsize=workers * 4)

    def worker():
        try:
            return _load_worker(connection_str, table, pending, stop, bulk_options, connect_kwargs)
        except BaseException as exc:
            failures.append(exc)
            stop.set()
            raise

    def feed():
        try:
            for piece in _slices(source, slice_rows):
                if not _put(pending, piece, stop):
                    return
        except BaseException as exc:
            failures.append(exc)
            stop.set()
        finally:
            for _ in range(workers):
                _put(pending, _END, stop)

    with ThreadPoolExecutor(max_workers=workers + 1, thread_name_prefix="mssql-write-arrow") as pool:
        futures = [pool.submit(worker) for _ in range(workers)]
        wait([pool.submit(feed), *futures])
    if failures:
        raise failures[0]
    results = [future.result() for future in futures]
    return [r for r in results if r]


def _load_worker(connection_str, table, pending, stop, bulk_options, connect_kwargs):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        cursor = conn.cursor()
        try:
            first = _next(pending, stop)
            if first is _END:
                return None

            def slices():
                piece = first
                while piece is not _END:
                    yield piece
                    piece = _next(pending, stop)

            return cursor.bulkcopy_arrow(table, slices(), table_lock=True, **bulk_options)
        finally:
            cursor.close()


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
