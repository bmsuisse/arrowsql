"""Native (zero-dependency) Arrow API of the Rust port.

cursor.fetchall_arrow() / fetchmany_arrow() return a native ArrowTable and
cursor.arrow_stream() a pipelined ArrowStream; both implement the Arrow
PyCapsule protocol, so DuckDB, polars and pyarrow read them without copying.
These methods do not exist upstream, so the module skips there.
"""

import datetime
import decimal
import gc
import uuid

import pytest

pa = pytest.importorskip("pyarrow")
duckdb = pytest.importorskip("duckdb")
pl = pytest.importorskip("polars")

from mssql_python import ddbc_bindings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "ArrowTable"), reason="native Arrow API is Rust-port only"
)

NUMBERS = "SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b"


def numbers(n):
    return NUMBERS.format(n=n)


def test_fetchall_arrow_basic_shape(cursor):
    cursor.execute("SELECT 1 AS a, N'x' AS b UNION ALL SELECT 2, NULL")
    table = cursor.fetchall_arrow()
    assert isinstance(table, ddbc_bindings.ArrowTable)
    assert table.num_rows == 2 == len(table)
    assert table.num_columns == 2
    assert table.column_names == ["a", "b"]
    assert pa.table(table).to_pydict() == {"a": [1, 2], "b": ["x", None]}


def test_table_exports_repeatedly(cursor):
    """DuckDB's register path calls __arrow_c_stream__ more than once."""
    cursor.execute(numbers(1000))
    table = cursor.fetchall_arrow()
    assert pa.table(table).num_rows == 1000
    assert pa.table(table).num_rows == 1000
    con = duckdb.connect()
    con.register("t", table)
    assert con.sql("SELECT count(*), sum(n) FROM t").fetchall()[0] == (1000, 500500)
    assert con.sql("SELECT max(n) FROM t").fetchall()[0] == (1000,)


def test_duckdb_replacement_scan_on_table(cursor):
    cursor.execute(numbers(5000))
    t = cursor.fetchall_arrow(batch_size=1024)
    assert t.num_batches == 5
    assert duckdb.sql("SELECT count(*) FROM t WHERE n % 2 = 0").fetchall()[0] == (2500,)


def test_polars_from_arrow_table(cursor):
    cursor.execute("SELECT CAST(1.5 AS decimal(10,2)) AS d, CAST('2024-01-02' AS date) AS dt")
    df = pl.DataFrame(cursor.fetchall_arrow())  # pl.from_arrow() of a capsule stream gives a struct Series
    assert df.shape == (1, 2)
    assert df["d"][0] == decimal.Decimal("1.50")
    assert df["dt"][0] == datetime.date(2024, 1, 2)


def test_fetchmany_arrow_and_rownumber(cursor):
    cursor.execute(numbers(25))
    first = cursor.fetchmany_arrow(10)
    assert pa.table(first).column("n").to_pylist() == list(range(1, 11))
    assert cursor.rownumber == 9
    row = cursor.fetchone()
    assert row[0] == 11
    rest = cursor.fetchall_arrow()
    assert pa.table(rest).column("n").to_pylist() == list(range(12, 26))
    assert cursor.rownumber == 24


def test_fetchmany_arrow_defaults_to_arraysize(cursor):
    cursor.arraysize = 7
    cursor.execute(numbers(20))
    assert cursor.fetchmany_arrow().num_rows == 7


def test_empty_result_keeps_schema(cursor):
    cursor.execute("SELECT CAST(1 AS int) AS a, N'' AS b WHERE 1 = 0")
    table = cursor.fetchall_arrow()
    assert table.num_rows == 0
    schema = pa.table(table).schema
    assert schema.names == ["a", "b"]
    assert schema.field("a").type == pa.int32()
    assert schema.field("b").type == pa.large_string()


def test_no_result_set_raises(cursor):
    cursor.execute("DECLARE @a int")
    with pytest.raises(Exception, match="No active result set"):
        cursor.fetchall_arrow()
    with pytest.raises(Exception, match="No active result set"):
        cursor.arrow_stream()


def test_type_fidelity_every_type(cursor):
    g = uuid.UUID("58185e0d-3a91-44d8-bc46-7107217e0a6d")
    cursor.execute(
        """
        SELECT CAST(255 AS tinyint) AS ti, CAST(-32768 AS smallint) AS si, CAST(-2147483648 AS int) AS i,
               CAST(9223372036854775807 AS bigint) AS bi, CAST(1 AS bit) AS b,
               CAST(1.25 AS real) AS r, CAST(2.5 AS float) AS f,
               CAST(-12345.6789 AS decimal(18,4)) AS d, CAST(922337203685477.5807 AS money) AS m,
               CAST(-214748.3648 AS smallmoney) AS sm,
               CAST('2024-02-29' AS date) AS dt, CAST('23:59:59.1234567' AS time(7)) AS tm,
               CAST('2024-02-29 12:34:56.789' AS datetime) AS dtm,
               CAST('2024-02-29 12:34:00' AS smalldatetime) AS sdt,
               CAST('2024-02-29 12:34:56.1234567' AS datetime2(7)) AS dt2,
               CAST('2024-02-29 12:34:56 +02:00' AS datetimeoffset(0)) AS dto,
               CAST('abc' AS varchar(10)) AS vc, CAST(N'äöü😀' AS nvarchar(10)) AS nvc,
               CAST('ab' AS char(4)) AS ch, CAST(0xDEADBEEF AS varbinary(10)) AS vb,
               CAST(? AS uniqueidentifier) AS g, CAST('<a>1</a>' AS xml) AS x,
               CAST(REPLICATE(CAST(N'z' AS nvarchar(max)), 10000) AS nvarchar(max)) AS nmax
        """,
        (str(g),),
    )
    t = pa.table(cursor.fetchall_arrow())
    types = {f.name: f.type for f in t.schema}
    assert types["ti"] == pa.uint8()
    assert types["si"] == pa.int16()
    assert types["i"] == pa.int32()
    assert types["bi"] == pa.int64()
    assert types["b"] == pa.bool_()
    assert types["r"] == pa.float32()
    assert types["f"] == pa.float64()
    assert types["d"] == pa.decimal128(18, 4)
    assert types["m"] == pa.decimal128(19, 4)
    assert types["sm"] == pa.decimal128(10, 4)
    assert types["dt"] == pa.date32()
    assert types["tm"] == pa.time64("ns")
    assert types["dtm"] == pa.timestamp("us")
    assert types["dto"] == pa.timestamp("us", tz="+00:00")
    assert types["vb"] == pa.large_binary()
    for name in ("vc", "nvc", "ch", "g", "x", "nmax"):
        assert types[name] == pa.large_string(), name
    row = t.to_pylist()[0]
    assert row["ti"] == 255 and row["si"] == -32768 and row["i"] == -(2**31)
    assert row["bi"] == 2**63 - 1 and row["b"] is True
    assert row["r"] == 1.25 and row["f"] == 2.5
    assert row["d"] == decimal.Decimal("-12345.6789")
    assert row["m"] == decimal.Decimal("922337203685477.5807")
    assert row["sm"] == decimal.Decimal("-214748.3648")
    assert row["dt"] == datetime.date(2024, 2, 29)
    assert row["tm"] == datetime.time(23, 59, 59, 123456)
    assert row["dtm"] == datetime.datetime(2024, 2, 29, 12, 34, 56, 790000)
    assert row["sdt"] == datetime.datetime(2024, 2, 29, 12, 34)
    assert row["dt2"] == datetime.datetime(2024, 2, 29, 12, 34, 56, 123456)
    assert row["dto"] == datetime.datetime(2024, 2, 29, 10, 34, 56, tzinfo=datetime.timezone.utc)
    assert row["vc"] == "abc" and row["nvc"] == "äöü😀" and row["ch"] == "ab  "
    assert row["vb"] == b"\xde\xad\xbe\xef"
    assert row["g"] == str(g).upper()
    assert row["x"] == "<a>1</a>"
    assert row["nmax"] == "z" * 10000


def test_nulls_in_every_type(cursor):
    types = [
        "tinyint", "smallint", "int", "bigint", "bit", "real", "float", "decimal(10,2)",
        "money", "date", "time", "datetime", "datetime2", "datetimeoffset",
        "nvarchar(10)", "varchar(max)", "varbinary(max)", "uniqueidentifier",
    ]
    cols = ", ".join(f"CAST(NULL AS {t}) AS c{i}" for i, t in enumerate(types))
    cursor.execute(f"SELECT {cols} UNION ALL SELECT {cols}")
    t = pa.table(cursor.fetchall_arrow())
    assert t.num_rows == 2
    for column in t.columns:
        assert column.null_count == 2


def test_matches_upstream_arrow_path(cursor):
    """The native table and cursor.arrow() decode identically."""
    sql = "SELECT TOP 500 name, object_id, create_date, CAST(object_id AS decimal(20,3)) / 7 AS d FROM sys.all_objects ORDER BY object_id"
    expected = cursor.execute(sql).arrow()
    actual = pa.table(cursor.execute(sql).fetchall_arrow())
    assert actual.equals(expected)


def test_interleaves_with_row_fetches(cursor):
    cursor.execute(numbers(100))
    assert cursor.fetchone()[0] == 1  # reads rows ahead into the row buffer
    t = cursor.fetchmany_arrow(50)  # taken from the read-ahead buffer, then the wire
    assert pa.table(t).column("n").to_pylist() == list(range(2, 52))
    assert [r[0] for r in cursor.fetchmany(3)] == [52, 53, 54]


def test_two_cursors_share_connection(conn):
    a, b = conn.cursor(), conn.cursor()
    a.execute(numbers(3000))
    first = a.fetchmany_arrow(1000)
    b.execute("SELECT 42")  # forces a's remaining rows off the wire
    assert b.fetchone()[0] == 42
    rest = a.fetchall_arrow()
    assert first.num_rows + rest.num_rows == 3000
    assert pa.table(rest).column("n").to_pylist()[-1] == 3000


def test_arrow_stream_into_duckdb(cursor):
    cursor.execute(numbers(100_000))
    stream = cursor.arrow_stream(batch_size=8192)
    assert stream.column_names == ["n"]
    result = duckdb.sql("SELECT count(*), sum(n) FROM stream").fetchall()[0]
    assert result == (100_000, 100_000 * 100_001 // 2)


def test_arrow_stream_schema_peek_does_not_consume(cursor):
    cursor.execute(numbers(10))
    stream = cursor.arrow_stream()
    capsule = stream.__arrow_c_stream__()  # exported, nothing read
    del capsule
    gc.collect()
    assert pa.RecordBatchReader.from_stream(stream).read_all().num_rows == 10


def test_arrow_stream_is_one_shot(cursor):
    cursor.execute(numbers(10))
    stream = cursor.arrow_stream()
    assert pa.RecordBatchReader.from_stream(stream).read_all().num_rows == 10
    with pytest.raises(ValueError, match="consumed|closed"):
        stream.__arrow_c_stream__()


def test_arrow_stream_read_all_and_polars(cursor):
    cursor.execute(numbers(20_000))
    table = cursor.arrow_stream(batch_size=3000).read_all()
    assert table.num_rows == 20_000
    assert table.num_batches == 7
    assert pl.DataFrame(table)["n"].sum() == 20_000 * 20_001 // 2


def test_arrow_stream_dropped_midway_leaves_cursor_usable(cursor):
    cursor.execute(numbers(200_000))
    reader = pa.RecordBatchReader.from_stream(cursor.arrow_stream(batch_size=1000))
    assert reader.read_next_batch().num_rows == 1000
    del reader
    gc.collect()
    cursor.execute("SELECT 7")
    assert cursor.fetchone()[0] == 7


def test_arrow_stream_close_and_context_manager(cursor):
    cursor.execute(numbers(50_000))
    with cursor.arrow_stream(batch_size=1000) as stream:
        assert not stream.closed
    assert stream.closed
    with pytest.raises(ValueError, match="closed"):
        stream.__arrow_c_stream__()
    cursor.execute("SELECT 8")
    assert cursor.fetchone()[0] == 8


def test_cursor_reexecuted_while_stream_alive(cursor):
    cursor.execute(numbers(100_000))
    stream = cursor.arrow_stream(batch_size=1000)
    cursor.execute("SELECT 9")  # the stream's result is gone
    assert cursor.fetchone()[0] == 9
    reader = pa.RecordBatchReader.from_stream(stream)
    total = sum(batch.num_rows for batch in reader)
    assert total < 100_000  # only batches decoded before the re-execute
    cursor.execute("SELECT 10")
    assert cursor.fetchone()[0] == 10


def test_stream_error_mid_result_surfaces(cursor):
    cursor.execute(
        "SELECT n, 1 / (n - 5000) AS boom FROM (" + numbers(10_000) + ") t ORDER BY n"
    )
    reader = pa.RecordBatchReader.from_stream(cursor.arrow_stream(batch_size=1000))
    with pytest.raises(Exception, match="(?i)divide by zero"):
        for _ in reader:
            pass


def test_large_stream_bounded_memory(cursor):
    """3M rows stream through with memory bounded by the prefetch window."""
    psutil = pytest.importorskip("psutil")
    proc = psutil.Process()
    cursor.execute(
        "SELECT TOP (3000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n, "
        "CAST(N'payload-payload-payload' AS nvarchar(40)) AS s "
        "FROM sys.all_columns a CROSS JOIN sys.all_columns b"
    )
    before = proc.memory_info().rss
    peak = before
    rows = 0
    for batch in pa.RecordBatchReader.from_stream(cursor.arrow_stream(batch_size=65536)):
        rows += batch.num_rows
        peak = max(peak, proc.memory_info().rss)
    assert rows == 3_000_000
    # Full materialization would be ~3M * (8 + 8 + 23) bytes ≈ 120 MB.
    assert peak - before < 80 * 1024 * 1024
