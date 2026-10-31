"""time(7) precision: what each path keeps of the 100 ns digit.

A Python ``datetime.time`` holds microseconds, so a row fetch of time(7) drops
the 7th fractional digit (truncation, as upstream mssql-python and pyodbc with
ODBC Driver 18 do). The Arrow paths keep it (time64[ns]), and binding a
``datetime.time`` keeps all six digits it has (pyodbc with ODBC Driver 18
binds it without fractional seconds).
"""

import datetime as dt

import pytest

from mssql_python import ddbc_bindings

T7 = "12:34:56.1234567"
T7_NS = ((12 * 60 + 34) * 60 + 56) * 1_000_000_000 + 123_456_700


def _fetch_text(cursor, table):
    cursor.execute(f"SELECT CONVERT(varchar(16), t) FROM {table} ORDER BY id")
    return [row[0] for row in cursor.fetchall()]


@pytest.mark.parametrize(
    "literal, expected",
    [
        (T7, dt.time(12, 34, 56, 123456)),
        # Truncated, not rounded: rounding would carry into the next day.
        ("23:59:59.9999999", dt.time(23, 59, 59, 999999)),
        ("00:00:00.0000001", dt.time(0, 0, 0, 0)),
    ],
)
def test_row_fetch_truncates_to_microseconds(cursor, literal, expected):
    cursor.execute(f"SELECT CAST('{literal}' AS time(7))")
    assert cursor.fetchone()[0] == expected


def test_full_precision_as_text(cursor):
    cursor.execute(f"SELECT CONVERT(varchar(16), CAST('{T7}' AS time(7)))")
    assert cursor.fetchone()[0] == T7


def test_bound_time_keeps_microseconds(cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (id int, t time(7))")
    cursor.execute(f"INSERT INTO {table_name} VALUES (1, ?)", (dt.time(12, 34, 56, 123456),))
    assert _fetch_text(cursor, table_name) == ["12:34:56.1234560"]


def test_executemany_time_keeps_microseconds(cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (id int, t time(7))")
    cursor.executemany(
        f"INSERT INTO {table_name} VALUES (?, ?)",
        [(1, dt.time(12, 34, 56, 123456)), (2, dt.time(23, 59, 59, 999999))],
    )
    assert _fetch_text(cursor, table_name) == ["12:34:56.1234560", "23:59:59.9999990"]


def test_arrow_keeps_100ns(cursor):
    pa = pytest.importorskip("pyarrow")
    table = cursor.execute(f"SELECT CAST('{T7}' AS time(7)) AS t").arrow()
    assert table.schema.field("t").type == pa.time64("ns")
    assert table.column("t").cast(pa.int64()).to_pylist() == [T7_NS]


@pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False), reason="fetchall_arrow is a Rust-port API"
)
def test_native_arrow_keeps_100ns(cursor):
    pa = pytest.importorskip("pyarrow")
    native = cursor.execute(f"SELECT CAST('{T7}' AS time(7)) AS t").fetchall_arrow()
    table = pa.table(native)
    assert table.schema.field("t").type == pa.time64("ns")
    assert table.column("t").cast(pa.int64()).to_pylist() == [T7_NS]


def test_arrow_bulk_copy_writes_100ns(cursor, table_name):
    pa = pytest.importorskip("pyarrow")
    cursor.execute(f"CREATE TABLE {table_name} (id int, t time(7))")
    cursor.connection.commit()
    source = pa.table({"id": pa.array([1], pa.int32()), "t": pa.array([T7_NS], pa.time64("ns"))})
    cursor.bulkcopy_arrow(table_name, source)
    assert _fetch_text(cursor, table_name) == [T7]
