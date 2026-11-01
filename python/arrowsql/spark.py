"""Spark Python data source for SQL Server: ``spark.read.format("arrowsql")``.

Needs ``pyspark>=4.0`` (``pip install "arrowsql[spark]"``) and arrowsql on every
executor (on Databricks: a notebook ``%pip install arrowsql``). Importing
``arrowsql`` never imports pyspark; only this module does.

    from arrowsql.spark import register
    register(spark)
    df = (spark.read.format("arrowsql")
          .option("connection_string", conn_str).option("dbtable", "dbo.orders")
          .option("partitionColumn", "id").option("numPartitions", 8).load())

Each Spark task opens its own connection, reads its slice with
``arrowsql.read_arrow_stream`` (Arrow record batches, no rows, no pandas) and
hands the batches to Spark. All data work happens in arrowsql's Rust core; this
module only plans slices and adapts types.

Options (names follow Spark JDBC where one exists):

* ``connection_string`` (alias ``url``): ODBC connection string. Required.
* ``dbtable`` or ``query``: exactly one. ``dbtable`` is a (schema-qualified)
  table or view name; ``query`` a SELECT without a trailing ORDER BY.
* ``partitionColumn``, ``numPartitions``, ``lowerBound``, ``upperBound``: range
  partitioning on a numeric, date or datetime column. Slices are half-open and
  the first one also reads the rows whose column is NULL, so no row is lost.
  Without bounds, MIN/MAX are read in one query on the driver.
* ``hashColumns`` + ``numPartitions``: slice ``i`` reads the rows with
  ``(CHECKSUM(cols) & 2147483647) % numPartitions = i``. Use key columns, never
  columns that change between queries (every slice is its own query).
* default: one slice (one task, one connection).
* ``batchSize``: rows per fetched Arrow batch (default 65536).
* ``utf8Strings``: ``true`` (default) transfers nvarchar as UTF-8, see ``read_arrow``.
* ``timestampType``: ``ntz`` (default) maps datetime/datetime2 to Spark
  ``TimestampNTZType`` (the exact wall-clock value); ``timestamp`` maps them to
  ``TimestampType`` like Spark JDBC, reading the wall-clock value as a time in
  the zone ``timeZone`` (default UTC). Spark does not tell a data source its session
  time zone, so set ``timeZone`` to ``spark.sql.session.timeZone`` when that is not UTC
  to get exactly what Spark JDBC returns.

Write (``df.write.format("arrowsql").option("dbtable", ...).mode(...).save()``):
append bulk-copies each task's Arrow batches over its own connection; the DataFrame
column names must be column names of the table. A Spark ``TimestampType`` column is an
instant: by default it is sent with its zone and only a datetimeoffset column takes it;
with ``timestampType=timestamp`` it is written as the wall-clock value in ``timeZone``
in ``timeZone`` (default UTC; like Spark JDBC) into datetime/datetime2. ``TimestampNTZType`` always writes as is. ``overwrite`` runs
``TRUNCATE TABLE`` once on the driver before the tasks start. The load is not
transactional across tasks: rows of tasks that finished stay when another task
fails, and a truncated table stays empty if the job then fails.
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceArrowWriter,
        DataSourceReader,
        InputPartition,
        WriterCommitMessage,
    )
except ImportError as exc:  # pragma: no cover - exercised only without pyspark
    raise ImportError(
        'arrowsql.spark needs pyspark>=4.0: pip install "arrowsql[spark]"'
    ) from exc

__all__ = ["ArrowSqlDataSource", "register"]

_WIDEN = {"uint8": "int16", "uint16": "int32", "uint32": "int64"}
_NAME_PART = re.compile(r"\[(?:[^\]]|\]\])+\]|[\w$#@]+")
_FALSE = {"false", "0", "no"}
_TRUE = {"true", "1", "yes"}


def register(spark: Any) -> None:
    """Register the ``arrowsql`` format with a SparkSession."""
    spark.dataSource.register(ArrowSqlDataSource)


# --- options -----------------------------------------------------------------------


def _quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _table_name(value: str) -> str:
    """Normalize ``a.b.c`` / ``[a].[b]`` to quoted parts; anything else is an error."""
    parts, pos = [], 0
    while True:
        match = _NAME_PART.match(value, pos)
        if not match:
            raise ValueError("dbtable must be a table or view name such as schema.table")
        part = match.group()
        parts.append(part if part.startswith("[") else _quote(part))
        pos = match.end()
        if pos == len(value):
            break
        if value[pos] != "." or len(parts) == 4:
            raise ValueError("dbtable must be a table or view name such as schema.table")
        pos += 1
    return ".".join(parts)


def _column_list(value: str, option: str) -> List[str]:
    names = [n.strip() for n in value.split(",")]
    if not all(names):
        raise ValueError(f"{option} must be a comma separated list of column names")
    return names


def _int_option(opts: Dict[str, str], key: str, label: str, default: int, minimum: int) -> int:
    raw = opts.get(key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{label} must be an integer, got {raw!r}") from None
    if value < minimum:
        raise ValueError(f"{label} must be >= {minimum}, got {value}")
    return value


@dataclass
class _Options:
    connection_string: str
    dbtable: Optional[str]
    query: Optional[str]
    partition_column: Optional[str]
    lower_bound: Optional[str]
    upper_bound: Optional[str]
    num_partitions: int
    hash_columns: List[str]
    batch_size: int
    utf8_strings: bool
    timestamp_ntz: bool
    time_zone: Optional[str]

    @classmethod
    def parse(cls, raw: Any) -> "_Options":
        opts = {str(k).lower(): str(v) for k, v in dict(raw).items()}
        conn = opts.get("connection_string") or opts.get("url")
        if not conn:
            raise ValueError("option 'connection_string' (or 'url') is required")
        dbtable, query = opts.get("dbtable"), opts.get("query")
        if bool(dbtable) == bool(query):
            raise ValueError("set exactly one of the options 'dbtable' and 'query'")
        if dbtable:
            dbtable = _table_name(dbtable.strip())
        utf8 = opts.get("utf8strings", "true").strip().lower()
        if utf8 not in _TRUE | _FALSE:
            raise ValueError(f"utf8Strings must be true or false, got {utf8!r}")
        ts = opts.get("timestamptype", "ntz").strip().lower()
        if ts not in ("ntz", "timestamp"):
            raise ValueError(f"timestampType must be 'ntz' or 'timestamp', got {ts!r}")
        out = cls(
            connection_string=conn,
            dbtable=dbtable,
            query=query,
            partition_column=opts.get("partitioncolumn"),
            lower_bound=opts.get("lowerbound"),
            upper_bound=opts.get("upperbound"),
            num_partitions=_int_option(opts, "numpartitions", "numPartitions", 1, 1),
            hash_columns=_column_list(opts["hashcolumns"], "hashColumns") if opts.get("hashcolumns") else [],
            batch_size=_int_option(opts, "batchsize", "batchSize", 65536, 1),
            utf8_strings=utf8 in _TRUE,
            timestamp_ntz=ts == "ntz",
            time_zone=opts.get("timezone"),
        )
        out._check_partitioning()
        return out

    def _check_partitioning(self) -> None:
        if self.partition_column and self.hash_columns:
            raise ValueError("set either partitionColumn or hashColumns, not both")
        bounds = (self.lower_bound, self.upper_bound)
        if self.partition_column:
            if (self.lower_bound is None) != (self.upper_bound is None):
                raise ValueError("lowerBound and upperBound must be set together")
        elif any(b is not None for b in bounds):
            raise ValueError("lowerBound and upperBound need partitionColumn")
        if self.num_partitions > 1 and not (self.partition_column or self.hash_columns):
            raise ValueError("numPartitions > 1 needs partitionColumn or hashColumns")

    @property
    def source_query(self) -> str:
        return self.query if self.query else f"SELECT * FROM {self.dbtable}"


# --- types ---------------------------------------------------------------------------


def _widen(schema: Any) -> Any:
    """Spark has no unsigned integers: widen them like the JDBC source does."""
    import pyarrow as pa

    return pa.schema(
        [f.with_type(pa.type_for_alias(_WIDEN[str(f.type)])) if str(f.type) in _WIDEN else f for f in schema]
    )


def _source_schema(opts: _Options) -> Any:
    """Arrow schema of the query, from a zero-row read on the driver."""
    import pyarrow as pa

    from arrowsql import read_arrow

    probe = f"SELECT TOP 0 * FROM ({opts.source_query}) AS q"
    return pa.table(read_arrow(opts.connection_string, probe, utf8_strings=opts.utf8_strings)).schema


def _spark_schema(opts: _Options) -> Any:
    from pyspark.sql.pandas.types import from_arrow_schema
    from pyspark.sql.types import StructField, StructType, TimestampNTZType, TimestampType

    struct = from_arrow_schema(_widen(_source_schema(opts)))
    if opts.timestamp_ntz:
        return struct
    return StructType(
        [
            StructField(f.name, TimestampType(), f.nullable) if isinstance(f.dataType, TimestampNTZType) else f
            for f in struct.fields
        ]
    )


def _arrow_schema(schema: Any) -> Any:
    """Spark's Arrow schema for ``schema`` (timestamps in UTC); the ``timezone`` keyword is pyspark >= 4.2."""
    from pyspark.sql.pandas.types import to_arrow_schema

    try:
        return to_arrow_schema(schema, timezone="UTC")
    except TypeError:
        return to_arrow_schema(schema)


def _conform(batch: Any, target: Any, time_zone: str) -> Any:
    """Cast an arrow batch to Spark's expected arrow schema (positional).

    A zone-less timestamp column going into a ``TimestampType`` field is read as
    wall-clock time in ``time_zone``.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    columns = []
    for column, field in zip(batch.columns, target):
        if column.type != field.type:
            if pa.types.is_timestamp(column.type) and column.type.tz is None and field.type.tz is not None:
                column = pc.assume_timezone(column, time_zone, ambiguous="earliest", nonexistent="earliest")
            column = column.cast(field.type, safe=False)
        columns.append(column)
    return pa.RecordBatch.from_arrays(columns, schema=target)


def _wall_clock(batch: Any, time_zone: str) -> Any:
    """Spark ``TimestampType`` (an instant) -> the zone-less wall-clock value in ``time_zone``."""
    import pyarrow as pa
    import pyarrow.compute as pc

    columns = []
    for column in batch.columns:
        if pa.types.is_timestamp(column.type) and column.type.tz is not None:
            column = pc.local_timestamp(column.cast(pa.timestamp(column.type.unit, tz=time_zone)))
        columns.append(column)
    return pa.RecordBatch.from_arrays(columns, names=batch.schema.names)


# --- reading -------------------------------------------------------------------------


class _Slice(InputPartition):
    """One Spark task's share of the query (picklable)."""

    def __init__(self, kind: str, index: int = 0, count: int = 1, bounds: Optional[list] = None):
        self.kind, self.index, self.count, self.bounds = kind, index, count, bounds or []


def _parse_bound(text: str, spark_type: Any) -> Any:
    import datetime
    from decimal import Decimal

    from pyspark.sql.types import DateType, DecimalType, FractionalType, IntegralType, TimestampNTZType, TimestampType

    try:
        if isinstance(spark_type, IntegralType):
            return int(text)
        if isinstance(spark_type, DecimalType):
            return Decimal(text)
        if isinstance(spark_type, FractionalType):
            return float(text)
        if isinstance(spark_type, DateType):
            return datetime.date.fromisoformat(text.strip())
        if isinstance(spark_type, (TimestampType, TimestampNTZType)):
            return datetime.datetime.fromisoformat(text.strip().replace(" ", "T", 1))
    except (ValueError, ArithmeticError):
        raise ValueError(f"cannot parse bound {text!r} as {spark_type.simpleString()}") from None
    raise ValueError(f"partitionColumn must be numeric, date or timestamp, not {spark_type.simpleString()}")


def _range_bounds(low: Any, high: Any, count: int) -> list:
    """Strictly ascending interior boundaries cutting [low, high] into ``count`` slices."""
    import datetime

    if isinstance(low, datetime.datetime):
        cuts = [low + (high - low) * k // count for k in range(1, count)]
    elif isinstance(low, datetime.date):
        cuts = [low + datetime.timedelta(days=(high - low).days * k // count) for k in range(1, count)]
    elif isinstance(low, int):
        cuts = [low + (high - low) * k // count for k in range(1, count)]
    else:
        cuts = [low + (high - low) * k / count for k in range(1, count)]
    # Fewer distinct values than slices: drop duplicates and cuts outside the range.
    return sorted({c for c in cuts if low < c <= high})


class _ArrowSqlReader(DataSourceReader):
    def __init__(self, options: Any, schema: Any):
        self.opts = _Options.parse(options)
        self.schema = schema

    def partitions(self) -> List[InputPartition]:
        opts = self.opts
        if opts.hash_columns:
            return [_Slice("hash", i, opts.num_partitions) for i in range(opts.num_partitions)]
        if not opts.partition_column or opts.num_partitions == 1:
            return [_Slice("full")]
        field = self._field(opts.partition_column)
        if opts.lower_bound is not None:
            low = _parse_bound(opts.lower_bound, field.dataType)
            high = _parse_bound(opts.upper_bound, field.dataType)
            if low > high:
                raise ValueError("lowerBound must not be greater than upperBound")
        else:
            low, high = self._probe_range(field.name)
        bounds = [] if low is None else _range_bounds(low, high, opts.num_partitions)
        if not bounds:
            return [_Slice("full")]
        # k bounds -> k + 1 slices; the NULL slice runs with the first one.
        return [_Slice("range", i, len(bounds) + 1, bounds) for i in range(len(bounds) + 1)]

    def _field(self, column: str) -> Any:
        for field in self.schema.fields:
            if field.name.lower() == column.lower():
                return field
        raise ValueError(f"partitionColumn {column!r} is not a column of the query")

    def _probe_range(self, column: str) -> Any:
        from contextlib import closing

        from arrowsql import connect

        sql = f"SELECT MIN({_quote(column)}), MAX({_quote(column)}) FROM ({self.opts.source_query}) AS q"
        with connect(self.opts.connection_string, autocommit=True) as conn, closing(conn.cursor()) as cursor:
            cursor.execute(sql)
            low, high = cursor.fetchone()
        return low, high

    def read(self, partition: InputPartition):  # type: ignore[override]
        import pyarrow as pa

        opts, target = self.opts, _arrow_schema(self.schema)
        time_zone = opts.time_zone or "UTC"
        stream = self._open(partition)
        try:
            for batch in pa.RecordBatchReader.from_stream(stream):
                yield _conform(batch, target, time_zone)
        finally:
            stream.close()

    def _open(self, part: Any) -> Any:
        from arrowsql import connect, read_arrow_stream

        opts, query = self.opts, self.opts.source_query
        if part.kind == "full":
            return read_arrow_stream(opts.connection_string, query, batch_size=opts.batch_size, utf8_strings=opts.utf8_strings)
        if part.kind == "hash":
            cols = ", ".join(_quote(c) for c in opts.hash_columns)
            sql = (
                f"SELECT * FROM ({query}) AS q "
                f"WHERE (CHECKSUM({cols}) & 2147483647) % {part.count} = {part.index}"
            )
            return read_arrow_stream(opts.connection_string, sql, batch_size=opts.batch_size, utf8_strings=opts.utf8_strings)
        # Range slice: the same half-open predicates read_arrow_stream(partition_bounds=...)
        # plans, one slice per task instead of one stream per query.
        from arrowsql._utf8_strings import utf8_query
        from arrowsql.arrow_parallel import _bounds_plan, _start

        column = _quote(self._field(opts.partition_column).name)
        conn = connect(opts.connection_string, autocommit=True)
        try:
            if opts.utf8_strings:
                query = utf8_query(conn, query)
            work = _bounds_plan(conn, query, column, part.bounds, True)  # [IS NULL, < b1, ..., >= bk]
        except BaseException:
            conn.close()
            raise
        mine = work[:2] if part.index == 0 else [work[part.index + 1]]
        return _start(conn, opts.connection_string, {}, mine, 1, opts.batch_size, None)


# --- writing -------------------------------------------------------------------------


@dataclass
class _CommitMessage(WriterCommitMessage):
    rows: int


class _ArrowSqlWriter(DataSourceArrowWriter):
    def __init__(self, options: Any, overwrite: bool):
        self.opts = _write_options(options)
        if overwrite:
            from contextlib import closing

            from arrowsql import connect

            with connect(self.opts.connection_string, autocommit=True) as conn, closing(conn.cursor()) as cursor:
                cursor.execute(f"TRUNCATE TABLE {self.opts.dbtable}")

    def write(self, iterator):  # type: ignore[override]
        import itertools

        import pyarrow as pa

        from arrowsql import write_arrow

        first = next(iterator, None)
        if first is None:
            return _CommitMessage(0)
        if not self.opts.timestamp_ntz:
            time_zone = self.opts.time_zone or "UTC"
            first, iterator = _wall_clock(first, time_zone), (_wall_clock(b, time_zone) for b in iterator)
        source = pa.RecordBatchReader.from_batches(first.schema, itertools.chain([first], iterator))
        result = write_arrow(
            self.opts.connection_string,
            self.opts.dbtable,
            source,
            workers=1,
            timeout=0,
            column_mappings=list(first.schema.names),
        )
        return _CommitMessage(int(result["rows_copied"]))


def _write_options(raw: Any) -> _Options:
    opts = {str(k).lower(): str(v) for k, v in dict(raw).items()}
    if opts.get("query"):
        raise ValueError("'query' is a read option; write needs 'dbtable'")
    if not opts.get("dbtable"):
        raise ValueError("option 'dbtable' is required to write")
    return _Options.parse(opts)


class ArrowSqlDataSource(DataSource):
    """``spark.read.format("arrowsql")`` / ``df.write.format("arrowsql")``; see the module docs."""

    @classmethod
    def name(cls) -> str:
        return "arrowsql"

    def schema(self) -> Any:
        return _spark_schema(_Options.parse(self.options))

    def reader(self, schema: Any) -> DataSourceReader:
        return _ArrowSqlReader(self.options, schema)

    def writer(self, schema: Any, overwrite: bool) -> _ArrowSqlWriter:
        return _ArrowSqlWriter(self.options, overwrite)
