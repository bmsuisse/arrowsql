"""RUST-PORT: partitioned parallel Arrow reads.

One TDS session delivers a result at whatever rate SQL Server serializes it
(about 1.4M rows/s for a 20-column table on a dev box), well below what the
Rust decoder can take. ``read_arrow`` splits the query on an integer column
into ranges and reads them over separate connections at once; decoding runs
without the GIL, so plain threads scale. The parts are concatenated into one
native ``ArrowTable`` (row order across partitions is not preserved).
"""

import math
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from mssql_python import ddbc_bindings
from mssql_python.db_connection import connect


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
    ``partitions`` > 1, the column's [min, max] range is split into equal
    ranges, each read over its own connection in parallel. Rows whose
    partition value is NULL go to the first partition. ``query`` becomes a
    derived table, so it may not end in ORDER BY (without TOP) or contain
    ``?`` parameters.
    """
    if partitions <= 1 or not partition_on:
        return _read(connection_str, query, (), batch_size, connect_kwargs)

    column = _quote_identifier(partition_on)
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM ({query}) AS _q")
        low, high = cursor.fetchone()
    if low is None:
        return _read(connection_str, query, (), batch_size, connect_kwargs)
    if not isinstance(low, int) or not isinstance(high, int):
        raise TypeError(f"partition_on column {partition_on!r} must be an integer column")

    step = max(math.ceil((high - low + 1) / partitions), 1)
    bounds = [(low + i * step, low + (i + 1) * step) for i in range(partitions)]
    bounds = [(start, end) for start, end in bounds if start <= high]
    parts = []
    for i, (start, end) in enumerate(bounds):
        last = i == len(bounds) - 1
        condition = f"{column} >= ? AND {column} {'<=' if last else '<'} ?"
        if i == 0:
            condition = f"({condition} OR {column} IS NULL)"
        sql = f"SELECT * FROM ({query}) AS _q WHERE {condition}"
        parts.append((sql, (start, high if last else end)))

    with ThreadPoolExecutor(max_workers=len(parts), thread_name_prefix="mssql-read-arrow") as pool:
        tables = list(
            pool.map(lambda part: _read(connection_str, *part, batch_size, connect_kwargs), parts)
        )
    return ddbc_bindings.ArrowTable.concat(tables)


def _read(connection_str, sql, params, batch_size, connect_kwargs):
    with connect(connection_str, autocommit=True, **connect_kwargs) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params) if params else cursor.execute(sql)
        return cursor.fetchall_arrow(batch_size)
