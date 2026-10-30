"""Native bulk copy (cursor.bulkcopy_arrow / bulkcopy on the cursor's own connection).

The wire encoders write TDS bytes directly, so the main check is that what
lands in the table equals what Microsoft's `mssql_py_core` engine stores for
the same source (when it is installed), alongside explicit expected values.
"""

import datetime as dt
import decimal
import importlib.util
import uuid

import pytest

pa = pytest.importorskip("pyarrow")

from mssql_python import ddbc_bindings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="native bulk copy is a Rust-port feature",
)

HAS_CORE = importlib.util.find_spec("mssql_py_core") is not None
D = decimal.Decimal


def _fetch(cursor, table):
    cursor.execute(f"SELECT * FROM {table} ORDER BY 1")
    return [tuple(row) for row in cursor.fetchall()]


def _load_with_core(cursor, table, source, **options):
    """Run bulkcopy_arrow through mssql_py_core instead of the native engine."""
    cursor._native_bulk_handle = lambda _name: None
    try:
        return cursor.bulkcopy_arrow(table, source, **options)
    finally:
        del cursor._native_bulk_handle


# (SQL type, Arrow array, expected values). The first column of each table is a
# bigint key; the type under test is the second.
CASES = {
    "bit": ("bit", pa.array([True, False, None]), [True, False, None]),
    "tinyint": ("tinyint", pa.array([0, 255, None], pa.int16()), [0, 255, None]),
    "smallint": ("smallint", pa.array([-32768, 32767, None], pa.int32()), [-32768, 32767, None]),
    "int": ("int", pa.array([-(2**31), 2**31 - 1, None], pa.int64()), [-(2**31), 2**31 - 1, None]),
    "bigint": ("bigint", pa.array([-(2**63), 2**63 - 1, None], pa.int64()), [-(2**63), 2**63 - 1, None]),
    "bigint_uint32": ("bigint", pa.array([0, 4_000_000_000, None], pa.uint32()), [0, 4_000_000_000, None]),
    "real": ("real", pa.array([1.5, -2.25, None], pa.float32()), [1.5, -2.25, None]),
    "float": ("float", pa.array([1.5e300, -2.25, None], pa.float64()), [1.5e300, -2.25, None]),
    "float_from_f32": ("float", pa.array([0.5, 2.0, None], pa.float32()), [0.5, 2.0, None]),
    "decimal_10_2": (
        "decimal(10,2)",
        pa.array([D("1.50"), D("-99999999.99"), None], pa.decimal128(10, 2)),
        [D("1.50"), D("-99999999.99"), None],
    ),
    "decimal_rescaled": (
        "decimal(12,4)",
        pa.array([D("1.5"), D("-0.25"), None], pa.decimal128(5, 2)),
        [D("1.5000"), D("-0.2500"), None],
    ),
    "decimal_38": (
        "decimal(38,10)",
        pa.array([D("1234567890123456789012345678.1234567890"), D("0"), None], pa.decimal128(38, 10)),
        [D("1234567890123456789012345678.1234567890"), D("0E-10"), None],
    ),
    "numeric_5_0": ("numeric(5,0)", pa.array([D("12345"), D("-1"), None], pa.decimal128(5, 0)), [D("12345"), D("-1"), None]),
    "money": ("money", pa.array([D("12.34"), D("-0.5"), None], pa.decimal128(10, 2)), [D("12.3400"), D("-0.5000"), None]),
    "date": (
        "date",
        pa.array([dt.date(1, 1, 1), dt.date(2024, 2, 29), None], pa.date32()),
        [dt.date(1, 1, 1), dt.date(2024, 2, 29), None],
    ),
    "datetime2_6": (
        "datetime2(6)",
        pa.array([dt.datetime(2020, 1, 2, 3, 4, 5, 123456), dt.datetime(1969, 12, 31, 23, 59, 59), None], pa.timestamp("us")),
        [dt.datetime(2020, 1, 2, 3, 4, 5, 123456), dt.datetime(1969, 12, 31, 23, 59, 59), None],
    ),
    "datetime2_0": (
        "datetime2(0)",
        pa.array([dt.datetime(2020, 1, 2, 3, 4, 5), None, dt.datetime(9999, 12, 31, 23, 59, 59)], pa.timestamp("s")),
        [dt.datetime(2020, 1, 2, 3, 4, 5), None, dt.datetime(9999, 12, 31, 23, 59, 59)],
    ),
    "datetime2_3_ns": (
        "datetime2(3)",
        pa.array([dt.datetime(2001, 2, 3, 4, 5, 6, 789000), None, dt.datetime(1900, 1, 1)], pa.timestamp("ns")),
        [dt.datetime(2001, 2, 3, 4, 5, 6, 789000), None, dt.datetime(1900, 1, 1)],
    ),
    "datetimeoffset": (
        "datetimeoffset(7)",
        pa.array([dt.datetime(2020, 5, 6, 7, 8, 9, 123456), None, dt.datetime(2000, 1, 1)], pa.timestamp("us", tz="UTC")),
        [
            dt.datetime(2020, 5, 6, 7, 8, 9, 123456, tzinfo=dt.timezone.utc),
            None,
            dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
        ],
    ),
    "datetime": (
        "datetime",
        pa.array([dt.datetime(2020, 1, 2, 3, 4, 5, 123000), None, dt.datetime(1753, 1, 1)], pa.timestamp("us")),
        [dt.datetime(2020, 1, 2, 3, 4, 5, 123000), None, dt.datetime(1753, 1, 1)],
    ),
    "smalldatetime": (
        "smalldatetime",
        pa.array([dt.datetime(2020, 1, 2, 3, 4, 31), None, dt.datetime(1900, 1, 1)], pa.timestamp("us")),
        [dt.datetime(2020, 1, 2, 3, 5), None, dt.datetime(1900, 1, 1)],
    ),
    "time_7": (
        "time(7)",
        pa.array([dt.time(1, 2, 3, 123456), None, dt.time(23, 59, 59)], pa.time64("us")),
        [dt.time(1, 2, 3, 123456), None, dt.time(23, 59, 59)],
    ),
    "time_0_ms": (
        "time(0)",
        pa.array([dt.time(1, 2, 3), None, dt.time(0, 0, 0)], pa.time32("ms")),
        [dt.time(1, 2, 3), None, dt.time(0, 0, 0)],
    ),
    "nvarchar": (
        "nvarchar(20)",
        pa.array(["plain", "héllo wörld \U0001F600", None], pa.string()),
        ["plain", "héllo wörld \U0001F600", None],
    ),
    "nvarchar_large_string": (
        "nvarchar(20)",
        pa.array(["", "abc", None], pa.large_string()),
        ["", "abc", None],
    ),
    "nvarchar_view": (
        "nvarchar(20)",
        pa.array(["x", "yz", None], pa.string_view()),
        ["x", "yz", None],
    ),
    "nvarchar_max": (
        "nvarchar(max)",
        pa.array(["", "m" * 20_000, None], pa.string()),
        ["", "m" * 20_000, None],
    ),
    "nchar": ("nchar(6)", pa.array(["ab", "abcdef", None], pa.string()), ["ab    ", "abcdef", None]),
    "varchar_ascii": ("varchar(20)", pa.array(["plain", "", None], pa.string()), ["plain", "", None]),
    "varchar_non_ascii": ("varchar(20)", pa.array(["café", "ascii", None], pa.string()), ["café", "ascii", None]),
    "varchar_max": ("varchar(max)", pa.array(["", "v" * 20_000, None], pa.string()), ["", "v" * 20_000, None]),
    "char": ("char(5)", pa.array(["ab", "abcde", None], pa.string()), ["ab   ", "abcde", None]),
    "varbinary": ("varbinary(20)", pa.array([b"\x00\x01", b"", None], pa.binary()), [b"\x00\x01", b"", None]),
    "uniqueidentifier": (
        "uniqueidentifier",
        pa.array(
            [uuid.UUID("12345678-1234-5678-1234-567812345678").bytes, uuid.UUID(int=0).bytes, None],
            pa.binary(16),
        ),
        [uuid.UUID("12345678-1234-5678-1234-567812345678"), uuid.UUID(int=0), None],
    ),
}


def _case_params():
    return [pytest.param(*case, id=name) for name, case in CASES.items()]


def _normalize(value):
    return str(value).upper() if isinstance(value, uuid.UUID) else value


@pytest.mark.parametrize("sql_type, column, expected", _case_params())
def test_type_roundtrip(cursor, table_name, sql_type, column, expected):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k bigint NOT NULL, v {sql_type} NULL)")
    source = pa.table({"k": pa.array(range(len(column)), pa.int64()), "v": column})

    result = cursor.bulkcopy_arrow(table, source, table_lock=True)

    assert result["rows_copied"] == len(column)
    got = [row[1] for row in _fetch(cursor, table)]
    if isinstance(got[0], uuid.UUID):
        assert [_normalize(v) for v in got] == [_normalize(v) for v in expected]
    else:
        assert got == expected


@pytest.mark.skipif(not HAS_CORE, reason="mssql_py_core not installed")
@pytest.mark.parametrize("sql_type, column, expected", _case_params())
def test_native_matches_core(cursor, table_name, sql_type, column, expected):
    native, core = f"dbo.{table_name}", f"dbo.{table_name}_core"
    for table in (native, core):
        cursor.execute(f"CREATE TABLE {table} (k bigint NOT NULL, v {sql_type} NULL)")
    source = pa.table({"k": pa.array(range(len(column)), pa.int64()), "v": column})
    try:
        cursor.bulkcopy_arrow(native, source)
        cursor.connection.commit()
        _load_with_core(cursor, core, source)
        assert _fetch(cursor, native) == _fetch(cursor, core)
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {core}")
        cursor.connection.commit()


@pytest.mark.parametrize(
    "sql_type, column, expected",
    [
        ("bigint", pa.array([1, 2, 3], pa.int64()), [1, 2, 3]),
        ("int", pa.array([1, 2, 3], pa.int32()), [1, 2, 3]),
        ("smallint", pa.array([1, 2, 3], pa.int16()), [1, 2, 3]),
        ("tinyint", pa.array([1, 2, 3], pa.uint8()), [1, 2, 3]),
        ("bit", pa.array([True, False, True]), [True, False, True]),
        ("float", pa.array([1.0, 2.0, 3.0]), [1.0, 2.0, 3.0]),
        ("real", pa.array([1.0, 2.0, 3.0], pa.float32()), [1.0, 2.0, 3.0]),
        ("decimal(9,3)", pa.array([D("1.001"), D("2.5"), D("-3")], pa.decimal128(9, 3)), [D("1.001"), D("2.500"), D("-3.000")]),
        ("nvarchar(10)", pa.array(["a", "b", "c"]), ["a", "b", "c"]),
        ("datetime2(7)", pa.array([dt.datetime(2020, 1, 1)] * 3, pa.timestamp("us")), [dt.datetime(2020, 1, 1)] * 3),
    ],
)
def test_not_null_columns(cursor, table_name, sql_type, column, expected):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int NOT NULL, v {sql_type} NOT NULL)")
    source = pa.table({"k": pa.array([0, 1, 2], pa.int32()), "v": column})

    cursor.bulkcopy_arrow(table, source)

    assert [row[1] for row in _fetch(cursor, table)] == expected


def test_null_into_not_null_column_fails_and_connection_survives(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int NOT NULL, b int NOT NULL)")
    source = pa.table({"a": pa.array([1, 2], pa.int32()), "b": pa.array([1, None], pa.int32())})

    with pytest.raises(Exception, match="(?i)non-nullable"):
        cursor.bulkcopy_arrow(table, source)

    cursor.connection.rollback()
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42


@pytest.mark.parametrize(
    "sql_type, column, message",
    [
        ("tinyint", pa.array([1, 300], pa.int64()), "TINYINT"),
        ("smallint", pa.array([1, 40000], pa.int64()), "SMALLINT"),
        ("int", pa.array([1, 2**31], pa.int64()), "INT"),
        ("bigint", pa.array([1, 2**63], pa.uint64()), "BIGINT"),
        ("decimal(5,2)", pa.array([D("1.00"), D("1000.00")], pa.decimal128(7, 2)), "precision"),
        ("decimal(5,2)", pa.array([D("1.001"), D("1.00")], pa.decimal128(7, 3)), "scale"),
        ("nvarchar(3)", pa.array(["abc", "abcd"]), "exceeds"),
        ("varchar(3)", pa.array(["abc", "abcd"]), "(?i)exceeds|too long|truncat"),
        ("float", pa.array([1.0, float("nan")]), "(?i)non-finite"),
        ("smalldatetime", pa.array([dt.datetime(2000, 1, 1), dt.datetime(2100, 1, 1)], pa.timestamp("us")), "SMALLDATETIME"),
    ],
)
def test_bad_values_are_rejected_before_anything_commits(cursor, table_name, sql_type, column, message):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (v {sql_type} NULL)")
    cursor.connection.commit()

    with pytest.raises(Exception, match=message):
        cursor.bulkcopy_arrow(table, pa.table({"v": column}))

    cursor.connection.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    assert cursor.fetchone()[0] == 0


def test_bulk_into_temp_table(cursor):
    """Upstream issue #492: the load must see a #temp table of its own session."""
    cursor.execute("CREATE TABLE #bulk_native_tmp (a int, b nvarchar(10))")

    cursor.bulkcopy_arrow("#bulk_native_tmp", pa.table({"a": [1, 2], "b": ["x", "y"]}))
    cursor.bulkcopy("#bulk_native_tmp", [(3, "z")])

    cursor.execute("SELECT a, b FROM #bulk_native_tmp ORDER BY a")
    assert [tuple(row) for row in cursor.fetchall()] == [(1, "x"), (2, "y"), (3, "z")]


def test_load_takes_part_in_the_open_transaction(conn, table_name):
    cursor = conn.cursor()
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")
    conn.commit()

    cursor.bulkcopy_arrow(table, pa.table({"a": pa.array([1, 2, 3], pa.int32())}))
    conn.rollback()

    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    assert cursor.fetchone()[0] == 0


def test_autocommit_load_is_durable(conn_str, table_name):
    from mssql_python import connect

    with connect(conn_str, autocommit=True) as writer:
        cursor = writer.cursor()
        table = f"dbo.{table_name}"
        cursor.execute(f"CREATE TABLE {table} (a int)")
        cursor.bulkcopy_arrow(table, pa.table({"a": pa.array([1, 2, 3], pa.int32())}))
        with connect(conn_str) as reader:
            other = reader.cursor()
            other.execute(f"SELECT COUNT(*) FROM {table}")
            assert other.fetchone()[0] == 3


def test_batches_are_streamed_and_counted(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a bigint, s nvarchar(20))")
    batches = [
        pa.record_batch([pa.array(range(i * 5000, (i + 1) * 5000), pa.int64()), pa.array([f"r{i}"] * 5000)], names=["a", "s"])
        for i in range(4)
    ]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, iter(batches))

    result = cursor.bulkcopy_arrow(table, reader, batch_size=7000)

    assert result["rows_copied"] == 20_000
    assert result["batch_count"] == 3
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(a) FROM {table}")
    assert tuple(cursor.fetchone()) == (20_000, sum(range(20_000)))


def test_wide_rows_span_many_packets(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(4000), t varchar(8000))")
    text = "w" * 4000
    source = pa.table(
        {"a": pa.array(range(300), pa.int32()), "s": [text] * 300, "t": ["t" * 8000] * 300}
    )

    cursor.bulkcopy_arrow(table, source)

    cursor.execute(f"SELECT COUNT(*), MIN(LEN(s)), MIN(LEN(t)) FROM {table}")
    assert tuple(cursor.fetchone()) == (300, 4000, 8000)


def test_column_mapping_by_name_and_skipped_identity(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id int IDENTITY(1,1), a int, b nvarchar(5) NULL)")
    source = pa.table({"x": pa.array([10, 20], pa.int32()), "y": ["p", "q"]})

    cursor.bulkcopy_arrow(table, source)  # positional: identity is skipped
    cursor.bulkcopy_arrow(table, source, column_mappings=[("y", "b"), ("x", "a")])

    cursor.execute(f"SELECT id, a, b FROM {table} ORDER BY id")
    assert [tuple(row) for row in cursor.fetchall()] == [(1, 10, "p"), (2, 20, "q"), (3, 10, "p"), (4, 20, "q")]


def test_keep_identity(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id int IDENTITY(1,1), a int)")

    cursor.bulkcopy_arrow(
        table, pa.table({"id": pa.array([100, 200], pa.int32()), "a": pa.array([1, 2], pa.int32())}), keep_identity=True
    )

    cursor.execute(f"SELECT id, a FROM {table} ORDER BY id")
    assert [tuple(row) for row in cursor.fetchall()] == [(100, 1), (200, 2)]


def test_empty_source_copies_nothing(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")

    result = cursor.bulkcopy_arrow(table, pa.table({"a": pa.array([], pa.int32())}))

    assert result["rows_copied"] == 0


def test_missing_table_is_a_clear_error(cursor):
    with pytest.raises(Exception, match="(?i)invalid object name|not found|does not exist"):
        cursor.bulkcopy_arrow("dbo.no_such_bulk_native_table", pa.table({"a": [1]}))


def test_unsupported_pairing_raises_value_error(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")

    with pytest.raises(ValueError, match="(?i)cannot map"):
        cursor.bulkcopy_arrow(table, pa.table({"a": ["text"]}))


def test_result_dictionary_shape(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")

    result = cursor.bulkcopy_arrow(table, pa.table({"a": pa.array([1, 2], pa.int32())}))

    assert set(result) == {"rows_copied", "batch_count", "elapsed_time", "rows_per_second"}
    assert result["rows_copied"] == 2
