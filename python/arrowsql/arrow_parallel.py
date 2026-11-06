"""RUST-PORT: partitioned parallel Arrow reads.

One TDS session delivers a result at whatever rate SQL Server serializes it
(about 1.4M rows/s for a 20-column table on a dev box), well below what the
Rust decoder can take. ``read_arrow`` / ``read_arrow_stream`` split the query
on a column into ranges and read them over separate connections at once.

The planning (MIN/MAX and catalog probes, slices), the worker threads and the
merge of their batches run in Rust (``arrowsql_core::read``) without the GIL.
This module checks the arguments and hands Rust a ``connect()`` closure for
the connections, so connection keywords, pooling and authentication work as
for any connection.
"""

import datetime
from decimal import Decimal
from typing import Any, Callable, List, Optional, Sequence, Tuple

from arrowsql import ddbc_bindings
from arrowsql.db_connection import connect

_BOUND_TYPES = (int, float, Decimal, str, datetime.date, datetime.datetime, datetime.time, bytes)

ParallelArrowStream = ddbc_bindings.ParallelArrowStream


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
    column, bounds = _split(partition_on, partitions, partition_bounds)
    return ddbc_bindings.read_arrow(
        _connector(connection_str, connect_kwargs), query, column, partitions, batch_size,
        utf8_strings, bounds, partition_nullable,
    )


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
    must ascend in the column's own ordering (for strings: its collation). Only
    Python code-point order is checked here, not the server collation: a list
    that ascends in Python but not in the collation yields overlapping or empty
    slices (duplicate or missing rows), so order string bounds by the collation; a
    ``str`` bound is sent as nvarchar, so a ``varchar`` column is compared
    after an implicit conversion (no index seek). ``partition_nullable`` says
    whether the column may hold NULL (default: asked from the server, one
    metadata call; pass False to skip it). ``utf8_strings``: as for
    ``read_arrow``.
    """
    column, bounds = _split(partition_on, partitions, partition_bounds)
    return ddbc_bindings.read_arrow_stream(
        _connector(connection_str, connect_kwargs), query, column, partitions, batch_size,
        bounds, max_buffered_batches, partition_nullable, utf8_strings,
    )


def _connector(connection_str: str, connect_kwargs: dict) -> Callable[[], Any]:
    """The connections of a read (the probe connection and one per extra worker)."""
    return lambda: connect(connection_str, autocommit=True, **connect_kwargs)


def _partitioned(partition_on, partitions, partition_bounds) -> bool:
    if partition_bounds is not None and not partition_on:
        raise ValueError("partition_bounds needs partition_on")
    return bool(partition_on) and (partitions > 1 or partition_bounds is not None)


def _split(partition_on, partitions, partition_bounds) -> Tuple[Optional[str], Optional[list]]:
    """(partition column or None for a single stream, checked bounds or None)."""
    if not _partitioned(partition_on, partitions, partition_bounds):
        return None, None
    return partition_on, None if partition_bounds is None else _check_bounds(partition_bounds)


def _check_bounds(bounds: Sequence[Any]) -> list:
    """``bounds`` as a list, if they are non-NULL values of one type, strictly ascending."""
    bounds = list(bounds)
    if not bounds:
        return bounds
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
    return bounds


def _quote_identifier(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _plan_read(
    connection_str: str,
    query: str,
    partition_on: Optional[str] = None,
    partitions: int = 1,
    partition_bounds: Optional[Sequence[Any]] = None,
    partition_nullable: Optional[bool] = None,
    utf8_strings: bool = True,
    **connect_kwargs: Any,
) -> List[Tuple[str, tuple]]:
    """The statements a read would run, ``[(sql, params), ...]`` (nothing is read)."""
    column, bounds = _split(partition_on, partitions, partition_bounds)
    return ddbc_bindings._plan_read(
        _connector(connection_str, connect_kwargs), query, column, partitions, bounds,
        partition_nullable, utf8_strings,
    )


def _plan(conn, query, column, name, partitions, plain=None) -> List[Tuple[str, tuple]]:
    """Probe [min, max] on ``conn``; return the slices, or [] to read in one stream."""
    return ddbc_bindings._range_plan(conn, query, column, name, partitions, plain)


def _bounds_plan(conn, query, column, bounds, nullable) -> List[Tuple[str, tuple]]:
    """Slices for caller-supplied boundaries (no data probe)."""
    return ddbc_bindings._bounds_plan(conn, query, column, _check_bounds(bounds), nullable)


def _probe_meta(conn, query, column, plain=None, with_rows: bool = True) -> Tuple[Optional[int], bool]:
    """(estimated row count or None, column may hold NULL) in one round trip."""
    return ddbc_bindings._probe_meta(conn, query, column, plain, with_rows)
