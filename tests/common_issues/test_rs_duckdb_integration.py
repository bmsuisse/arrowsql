"""DuckDB / Arrow integration: duckdb and polars over mssql_python Arrow results.

Research sources (issue trackers and docs consulted for real failure modes):
- https://github.com/microsoft/mssql-python/pull/354
  Arrow fetch support (cursor.arrow, arrow_batch, arrow_reader). Review notes
  call out interop with DuckDB and Polars; a Windows build broke on a 128-bit
  integer type that MSVC does not support.
- https://github.com/microsoft/mssql-python/discussions/669
  Arrow bulk-copy API review: users who already hold data in Arrow / DuckDB /
  Polars must materialize it into Python objects before a bulk load.
- https://github.com/microsoft/mssql-python/pull/857 (fixes issue #856)
  Flaky arrow reader cancel-from-another-thread test; cancellation surfaces as
  OperationalError("Operation canceled").
- https://github.com/apache/arrow/issues/18126
  Arrow infers decimal precision from the first element; later, wider values
  fail with ArrowInvalid. Relevant to any Decimal column built from Python rows.
- https://github.com/mkleehammer/pyodbc/issues/463
  Money/decimal values truncated on read by the ODBC driver (CAST(0.1234 AS
  money) came back as 0.12 with FreeTDS).
- https://github.com/pandas-dev/pandas/issues/61641
  to_numeric on a pyarrow decimal Series that contains NA values loses the
  index alignment.
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/arrow-integration
  cursor.arrow() / arrow_reader() as zero-copy hand-off to Polars, pandas, DuckDB.
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/duckdb-integration
  duckdb.sql over a cursor.arrow() table.

Expected values are what the upstream ODBC driver returns today, except where a
test is marked upstream_bug: then it asserts the correct value and is an
xfail(strict=True) so the suite flags the gap until upstream is fixed.
"""

import datetime
import decimal

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pl = pytest.importorskip("polars")


def test_arrow_result_is_pyarrow_table_with_expected_types(cursor):
    cursor.execute(
        "SELECT CAST(1 AS INT) i, CAST(2 AS BIGINT) b, CAST(N'x' AS NVARCHAR(10)) s, "
        "CAST(1 AS BIT) bt, CAST(12.5000 AS DECIMAL(18,4)) d, "
        "CAST(1.5 AS FLOAT) f, CAST(2.5 AS REAL) r"
    )
    t = cursor.arrow()
    assert isinstance(t, pa.Table)
    assert t.schema.field("i").type == pa.int32()
    assert t.schema.field("b").type == pa.int64()
    assert t.schema.field("s").type == pa.large_string()
    assert t.schema.field("bt").type == pa.bool_()
    assert t.schema.field("d").type == pa.decimal128(18, 4)
    assert t.schema.field("f").type == pa.float64()
    assert t.schema.field("r").type == pa.float32()
    assert t.to_pylist() == [
        {
            "i": 1,
            "b": 2,
            "s": "x",
            "bt": True,
            "d": decimal.Decimal("12.5000"),
            "f": 1.5,
            "r": 2.5,
        }
    ]


def test_duckdb_aggregates_over_arrow_table_respect_null_semantics(cursor):
    cursor.execute("SELECT v FROM (VALUES (1), (2), (3), (NULL)) AS x(v)")
    t = cursor.arrow()
    con = duckdb.connect()
    con.register("t", t)
    total, cnt_star, cnt_col = con.execute(
        "SELECT sum(v), count(*), count(v) FROM t"
    ).fetchone()
    assert (total, cnt_star, cnt_col) == (6, 4, 3)


def test_join_sqlserver_result_with_local_duckdb_table(cursor):
    cursor.execute(
        "SELECT id, name FROM (VALUES (1, N'alpha'), (2, N'beta'), (4, N'delta')) AS x(id, name)"
    )
    left = cursor.arrow()
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE local_prices AS SELECT * FROM (VALUES "
        "(1, 9.50::DECIMAL(10,2)), (2, 0.25::DECIMAL(10,2)), (3, 7.00::DECIMAL(10,2))"
        ") v(id, price)"
    )
    con.register("sql_rows", left)
    rows = con.execute(
        "SELECT s.id, s.name, p.price FROM sql_rows s "
        "LEFT JOIN local_prices p USING (id) ORDER BY s.id"
    ).fetchall()
    assert rows == [
        (1, "alpha", decimal.Decimal("9.50")),
        (2, "beta", decimal.Decimal("0.25")),
        (4, "delta", None),
    ]


def test_money_and_decimal_scale_survive_duckdb_arithmetic(cursor):
    cursor.execute("SELECT CAST(1234567890.12 AS MONEY) m, CAST(12345.6789 AS DECIMAL(18,4)) d")
    t = cursor.arrow()
    assert t.schema.field("m").type == pa.decimal128(19, 4)
    con = duckdb.connect()
    con.register("t", t)
    money, doubled = con.execute("SELECT m, d * 2 FROM t").fetchone()
    assert money == decimal.Decimal("1234567890.1200")
    assert doubled == decimal.Decimal("24691.3578")


def test_decimal38_extreme_value_roundtrip_through_duckdb(cursor):
    cursor.execute("SELECT CAST(-12345678901234567890123456789012345678 AS DECIMAL(38,0)) big")
    t = cursor.arrow()
    assert t.schema.field("big").type == pa.decimal128(38, 0)
    con = duckdb.connect()
    con.register("t", t)
    assert con.execute("SELECT big FROM t").fetchone() == (
        decimal.Decimal("-12345678901234567890123456789012345678"),
    )


def test_nvarchar_non_bmp_character_length_in_duckdb(cursor):
    # U+1F600 is a surrogate pair in UTF-16 (the TDS wire encoding for NVARCHAR).
    cursor.execute("SELECT N'a\U0001F600b' AS s")
    t = cursor.arrow()
    assert t.column("s").to_pylist() == ["a\U0001F600b"]
    con = duckdb.connect()
    con.register("t", t)
    # length() counts code points, octet_length() counts UTF-8 bytes (1 + 4 + 1).
    assert con.execute("SELECT length(s), octet_length(encode(s)) FROM t").fetchone() == (3, 6)


def test_empty_result_keeps_column_names_and_types(cursor):
    cursor.execute("SELECT CAST(1 AS INT) AS a, CAST(N'' AS NVARCHAR(5)) AS b WHERE 1 = 0")
    t = cursor.arrow()
    assert t.num_rows == 0
    assert t.schema.names == ["a", "b"]
    assert t.schema.field("a").type == pa.int32()
    assert t.schema.field("b").type == pa.large_string()
    con = duckdb.connect()
    con.register("t", t)
    assert con.execute("SELECT count(*) FROM t").fetchone() == (0,)
    assert [d[0] for d in con.execute("SELECT * FROM t").description] == ["a", "b"]


def test_all_null_columns_arrow_types_and_duckdb_counts(cursor):
    cursor.execute("SELECT CAST(NULL AS NVARCHAR(10)) s, CAST(NULL AS INT) n")
    t = cursor.arrow()
    assert t.schema.field("s").type == pa.large_string()
    assert t.schema.field("n").type == pa.int32()
    assert t.to_pylist() == [{"s": None, "n": None}]
    con = duckdb.connect()
    con.register("t", t)
    assert con.execute("SELECT count(*), count(n), count(s) FROM t").fetchone() == (1, 0, 0)


def test_registered_arrow_reader_stream_is_scannable_by_duckdb(cursor):
    cursor.execute("SELECT v FROM (VALUES (1), (2), (3)) AS x(v)")
    reader = cursor.arrow_reader()
    assert hasattr(reader, "__arrow_c_stream__")
    con = duckdb.connect()
    con.register("strm", reader)
    assert con.execute("SELECT sum(v) FROM strm").fetchone() == (6,)


def test_arrow_reader_is_single_pass_second_scan_is_empty(cursor):
    # Documents upstream behavior: the stream is consumed once; a second scan
    # returns zero rows without raising.
    cursor.execute("SELECT v FROM (VALUES (1), (2)) AS x(v)")
    reader = cursor.arrow_reader()
    con = duckdb.connect()
    con.register("strm", reader)
    assert con.execute("SELECT count(*) FROM strm").fetchone() == (2,)
    con2 = duckdb.connect()
    con2.register("strm", reader)
    assert con2.execute("SELECT count(*) FROM strm").fetchone() == (0,)


def test_cursor_arrow_second_call_returns_empty_table(cursor):
    # Documents upstream behavior: the result set is consumed by the first arrow().
    cursor.execute("SELECT 1 AS a, 2 AS b")
    first = cursor.arrow()
    second = cursor.arrow()
    assert first.num_rows == 1
    assert second.num_rows == 0


def test_polars_from_arrow_reader_two_columns(cursor):
    # pl.from_arrow on a stream returns a single struct Series, not a DataFrame;
    # callers must unnest it.
    cursor.execute(
        "SELECT * FROM (VALUES (CAST(1 AS INT), CAST(N'x' AS NVARCHAR(3))), "
        "(CAST(3 AS INT), CAST(N'y' AS NVARCHAR(3)))) AS x(a, b)"
    )
    series = pl.from_arrow(cursor.arrow_reader())
    assert isinstance(series, pl.Series)
    assert series.dtype == pl.Struct
    df = series.struct.unnest()
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["a", "b"]
    assert df.schema["a"] == pl.Int32
    assert df.schema["b"] == pl.String
    assert df.rows() == [(1, "x"), (3, "y")]


def test_polars_from_arrow_table_decimal_dtype(cursor):
    cursor.execute("SELECT CAST(1.50 AS DECIMAL(10,2)) d")
    df = pl.from_arrow(cursor.arrow())
    assert df.schema["d"] == pl.Decimal(10, 2)
    assert df["d"].to_list() == [decimal.Decimal("1.50")]


def test_time7_arrow_keeps_nanoseconds_duckdb_truncates_to_micro(cursor):
    cursor.execute("SELECT CAST('12:34:56.1234567' AS TIME(7)) tm")
    t = cursor.arrow()
    assert t.schema.field("tm").type == pa.time64("ns")
    # 12:34:56.1234567 expressed as nanoseconds since midnight.
    assert t.column("tm").cast(pa.int64()).to_pylist() == [45296123456700]
    con = duckdb.connect()
    con.register("t", t)
    # DuckDB TIME has microsecond precision, so the trailing 00/7 digits are lost there.
    assert con.execute("SELECT tm FROM t").fetchone() == (datetime.time(12, 34, 56, 123456),)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "documented contract: DATETIME2(7) maps to timestamp[us] in Arrow (100ns digits "
        "truncated); both upstream and the port keep it for compatibility"
    ),
)
def test_datetime2_7_keeps_100ns_in_arrow(cursor):
    cursor.execute("SELECT CAST('2024-01-02 03:04:05.1234567' AS DATETIME2(7)) ts")
    t = cursor.arrow()
    assert t.schema.field("ts").type == pa.timestamp("ns")
    assert t.column("ts").cast(pa.int64()).to_pylist() == [1704164645123456700]


def test_datetimeoffset_arrow_is_utc_normalized(cursor):
    cursor.execute("SELECT CAST('2024-01-02 03:04:05 +05:30' AS DATETIMEOFFSET(3)) o")
    t = cursor.arrow()
    assert t.schema.field("o").type == pa.timestamp("us", tz="+00:00")
    assert t.to_pylist() == [
        {"o": datetime.datetime(2024, 1, 1, 21, 34, 5, tzinfo=datetime.timezone.utc)}
    ]


def test_temp_table_group_by_result_scanned_by_duckdb(cursor):
    cursor.execute("CREATE TABLE #sales (region NVARCHAR(10), amount DECIMAL(10,2))")
    cursor.execute(
        "INSERT INTO #sales (region, amount) VALUES "
        "(N'eu', 10.00), (N'eu', 2.50), (N'us', 7.25), (N'us', NULL)"
    )
    cursor.execute(
        "SELECT region, SUM(amount) AS total, COUNT(amount) AS n FROM #sales GROUP BY region"
    )
    t = cursor.arrow()
    con = duckdb.connect()
    con.register("t", t)
    rows = con.execute("SELECT region, total, n FROM t ORDER BY region").fetchall()
    assert rows == [
        ("eu", decimal.Decimal("12.50"), 2),
        ("us", decimal.Decimal("7.25"), 1),
    ]
