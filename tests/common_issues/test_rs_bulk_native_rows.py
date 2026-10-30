"""Native row-tuple bulk copy (cursor.bulkcopy on the cursor's own connection).

Same approach as test_rs_bulk_native.py: explicit expected values plus, when
`mssql_py_core` is installed, equality with what that engine stores.
"""

import datetime as dt
import decimal
import importlib.util
import uuid

import pytest

from mssql_python import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="native bulk copy is a Rust-port feature",
)

HAS_CORE = importlib.util.find_spec("mssql_py_core") is not None
D = decimal.Decimal


def _fetch(cursor, table):
    cursor.execute(f"SELECT * FROM {table} ORDER BY 1")
    return [tuple(row) for row in cursor.fetchall()]


CASES = {
    "bit": ("bit", [True, False, None], [True, False, None]),
    "bit_from_int_str": ("bit", [1, "true", 0], [True, True, False]),
    "tinyint": ("tinyint", [0, 255, None], [0, 255, None]),
    "smallint_from_str": ("smallint", ["-5", 32767, None], [-5, 32767, None]),
    "int": ("int", [-(2**31), 2**31 - 1, None], [-(2**31), 2**31 - 1, None]),
    "bigint": ("bigint", [-(2**63), 2**63 - 1, None], [-(2**63), 2**63 - 1, None]),
    "real": ("real", [1.5, -2, None], [1.5, -2.0, None]),
    "float": ("float", [1.5e300, 2, "3.5"], [1.5e300, 2.0, 3.5]),
    "decimal": ("decimal(10,2)", [D("1.50"), 3, D("-99999999.99")], [D("1.50"), D("3.00"), D("-99999999.99")]),
    "decimal_from_float_and_str": (
        "decimal(10,3)",
        [0.1, "2.25", D("1E+2")],
        [D("0.100"), D("2.250"), D("100.000")],
    ),
    "decimal_38": (
        "decimal(38,10)",
        [D("1234567890123456789012345678.1234567890"), D("0"), None],
        [D("1234567890123456789012345678.1234567890"), D("0E-10"), None],
    ),
    "money": ("money", [D("12.34"), 5, None], [D("12.3400"), D("5.0000"), None]),
    "date": (
        "date",
        [dt.date(1, 1, 1), dt.datetime(2024, 2, 29, 13, 0), "2020-01-02"],
        [dt.date(1, 1, 1), dt.date(2024, 2, 29), dt.date(2020, 1, 2)],
    ),
    "time": (
        "time(6)",
        [dt.time(1, 2, 3, 123456), dt.time(23, 59, 59), "04:05:06"],
        [dt.time(1, 2, 3, 123456), dt.time(23, 59, 59), dt.time(4, 5, 6)],
    ),
    "datetime2": (
        "datetime2(6)",
        [dt.datetime(2020, 1, 2, 3, 4, 5, 123456), dt.date(1999, 12, 31), "2001-02-03T04:05:06"],
        [dt.datetime(2020, 1, 2, 3, 4, 5, 123456), dt.datetime(1999, 12, 31), dt.datetime(2001, 2, 3, 4, 5, 6)],
    ),
    "datetimeoffset": (
        "datetimeoffset(3)",
        [
            dt.datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            dt.datetime(2020, 1, 2, 3, 4, 5),
            None,
        ],
        [
            dt.datetime(2020, 1, 2, 3, 4, 5, 123000, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc),
            None,
        ],
    ),
    "datetime": (
        "datetime",
        [dt.datetime(2020, 1, 2, 3, 4, 5, 123000), None, dt.datetime(1753, 1, 1)],
        [dt.datetime(2020, 1, 2, 3, 4, 5, 123000), None, dt.datetime(1753, 1, 1)],
    ),
    "smalldatetime": (
        "smalldatetime",
        [dt.datetime(2020, 1, 2, 3, 4, 31), None, dt.datetime(1900, 1, 1)],
        [dt.datetime(2020, 1, 2, 3, 5), None, dt.datetime(1900, 1, 1)],
    ),
    "nvarchar": ("nvarchar(20)", ["plain", "héllo \U0001F600", None], ["plain", "héllo \U0001F600", None]),
    "nvarchar_from_numbers": ("nvarchar(20)", [12, 1.5, D("2.50")], ["12", "1.5", "2.50"]),
    "nvarchar_max": ("nvarchar(max)", ["", "m" * 20_000, None], ["", "m" * 20_000, None]),
    "nchar": ("nchar(4)", ["ab", "abcd", None], ["ab  ", "abcd", None]),
    "varchar": ("varchar(20)", ["plain", "café", None], ["plain", "café", None]),
    "varchar_max": ("varchar(max)", ["", "v" * 20_000, None], ["", "v" * 20_000, None]),
    "varbinary": ("varbinary(20)", [b"\x00\x01", bytearray(b"ab"), None], [b"\x00\x01", b"ab", None]),
    "uniqueidentifier": (
        "uniqueidentifier",
        [uuid.UUID("12345678-1234-5678-1234-567812345678"), "00000000-0000-0000-0000-000000000001", None],
        [uuid.UUID("12345678-1234-5678-1234-567812345678"), uuid.UUID(int=1), None],
    ),
}


CASES["time_plain"] = (
    "time(6)",
    [dt.time(1, 2, 3, 123456), dt.time(23, 59, 59), None],
    [dt.time(1, 2, 3, 123456), dt.time(23, 59, 59), None],
)
CASES["datetime2_plain"] = (
    "datetime2(6)",
    [dt.datetime(2020, 1, 2, 3, 4, 5, 123456), dt.datetime(1999, 12, 31), None],
    [dt.datetime(2020, 1, 2, 3, 4, 5, 123456), dt.datetime(1999, 12, 31), None],
)

# mssql_py_core rejects these inputs (date or ISO text for datetime2/time) or stores
# an aware datetimeoffset shifted by its offset, so only the explicit values apply.
NATIVE_ONLY = {"time", "datetime2", "datetimeoffset"}


def _params():
    return [pytest.param(*case, id=name) for name, case in CASES.items()]


def _core_params():
    return [pytest.param(*case, id=name) for name, case in CASES.items() if name not in NATIVE_ONLY]


@pytest.mark.parametrize("sql_type, values, expected", _params())
def test_row_type_roundtrip(cursor, table_name, sql_type, values, expected):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int NOT NULL, v {sql_type} NULL)")

    result = cursor.bulkcopy(table, [(i, value) for i, value in enumerate(values)])

    assert result["rows_copied"] == len(values)
    assert [row[1] for row in _fetch(cursor, table)] == expected


@pytest.mark.skipif(not HAS_CORE, reason="mssql_py_core not installed")
@pytest.mark.parametrize("sql_type, values, expected", _core_params())
def test_row_native_matches_core(cursor, table_name, sql_type, values, expected):
    native, core = f"dbo.{table_name}", f"dbo.{table_name}_core"
    for table in (native, core):
        cursor.execute(f"CREATE TABLE {table} (k int NOT NULL, v {sql_type} NULL)")
    rows = [(i, value) for i, value in enumerate(values)]
    try:
        cursor.bulkcopy(native, rows)
        cursor.connection.commit()
        cursor._native_bulk_handle = lambda _name: None
        try:
            cursor.bulkcopy(core, rows)
        finally:
            del cursor._native_bulk_handle
        assert _fetch(cursor, native) == _fetch(cursor, core)
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {core}")
        cursor.connection.commit()


@pytest.mark.parametrize(
    "sql_type, value, message",
    [
        ("tinyint", 300, "TINYINT"),
        ("int", 2**31, "INT"),
        ("bigint", 2**63, "(?i)out of range"),
        ("int", "abc", "(?i)cannot convert"),
        ("decimal(5,2)", D("1000.00"), "precision"),
        ("decimal(5,2)", D("1.001"), "scale"),
        ("nvarchar(3)", "abcd", "exceeds"),
        ("float", float("nan"), "(?i)non-finite"),
        ("date", "not-a-date", "(?i)invalid isoformat"),
    ],
)
def test_row_bad_values_abort_the_load(cursor, table_name, sql_type, value, message):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (v {sql_type} NULL)")
    cursor.connection.commit()

    with pytest.raises(Exception, match=message):
        cursor.bulkcopy(table, [(None,), (value,)])

    cursor.connection.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    assert cursor.fetchone()[0] == 0


def test_row_null_into_not_null_column(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int NOT NULL, b nvarchar(5) NOT NULL)")

    with pytest.raises(Exception, match="(?i)non-nullable"):
        cursor.bulkcopy(table, [(1, "x"), (2, None)])

    cursor.connection.rollback()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_rows_with_the_wrong_width_are_rejected(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, b int)")

    with pytest.raises(Exception, match="(?i)columns"):
        cursor.bulkcopy(table, [(1, 2), (3,)])


def test_exception_in_the_source_is_raised_as_is(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")

    class Boom(Exception):
        pass

    def rows():
        yield (1,)
        raise Boom("source failed")

    with pytest.raises(Boom, match="source failed"):
        cursor.bulkcopy(table, rows())

    cursor.connection.rollback()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_row_objects_and_generators(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, b varchar(10))")
    cursor.execute(f"INSERT INTO {table} VALUES (1, 'x'), (2, 'y')")
    cursor.execute(f"SELECT a, b FROM {table}")
    fetched = cursor.fetchall()

    cursor.bulkcopy(table, fetched)
    cursor.bulkcopy(table, ((i, str(i)) for i in range(10, 13)))

    cursor.execute(f"SELECT COUNT(*), SUM(a) FROM {table}")
    assert tuple(cursor.fetchone()) == (7, 1 + 2 + 1 + 2 + 10 + 11 + 12)


def test_source_cursor_of_the_same_connection(conn, table_name):
    source, target = f"dbo.{table_name}_src", f"dbo.{table_name}"
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE {source} (a int, b varchar(10))")
    cursor.execute(f"INSERT INTO {source} VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    cursor.execute(f"CREATE TABLE {target} (a int, b varchar(10))")
    try:
        reader = conn.cursor()
        reader.execute(f"SELECT a, b FROM {source} ORDER BY a")

        result = cursor.bulkcopy(target, reader)

        assert result["rows_copied"] == 3
        assert _fetch(cursor, target) == [(1, "x"), (2, "y"), (3, "z")]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {source}")
        conn.commit()


def test_many_rows_across_windows_and_batches(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(30), f float)")
    n = 25_000

    result = cursor.bulkcopy(table, ((i, f"row {i}", i / 4) for i in range(n)), batch_size=10_000)

    assert result["rows_copied"] == n
    assert result["batch_count"] == 3
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(CAST(a AS bigint)), MAX(s) FROM {table}")
    assert tuple(cursor.fetchone()) == (n, sum(range(n)), "row 9999")


def test_row_column_mappings(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id int IDENTITY(1,1), a int, b nvarchar(5) NULL)")

    cursor.bulkcopy(table, [(10, "p"), (20, "q")])  # positional, identity skipped
    cursor.bulkcopy(table, [("r", 30)], column_mappings=[(1, "a"), (0, "b")])

    cursor.execute(f"SELECT id, a, b FROM {table} ORDER BY id")
    assert [tuple(row) for row in cursor.fetchall()] == [(1, 10, "p"), (2, 20, "q"), (3, 30, "r")]


def test_empty_row_source(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")

    assert cursor.bulkcopy(table, [])["rows_copied"] == 0
    assert cursor.bulkcopy(table, iter(()))["rows_copied"] == 0
