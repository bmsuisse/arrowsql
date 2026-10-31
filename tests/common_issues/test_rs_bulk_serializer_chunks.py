"""Bulk-serializer output must survive chunked writes (stack buffers / static pad chunks).

Strings are sized so UTF-16 surrogate pairs straddle the 512-byte (256 unit)
streaming chunk and padding crosses the 256-byte pad chunk.
"""

import pytest

from mssql_python import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="native bulk copy is a Rust-port feature",
)

EMOJI = "\U0001F600"
STRINGS = [("a" * n) + EMOJI + "x" + EMOJI + EMOJI + "é" for n in (0, 1, 254, 255, 256, 257, 300, 1000)]


@pytest.mark.parametrize("n", [0, 1, 5, 16])
def test_binary_padding(cursor, table_name, n):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, b binary(16), v varbinary(64))")
    data = bytes(range(n))
    cursor.bulkcopy(table, [(1, data, data)])
    cursor.execute(f"SELECT b, v FROM {table}")
    b, v = cursor.fetchone()
    assert bytes(b) == data + b"\x00" * (16 - n)
    assert bytes(v) == data


def test_nvarchar_max_and_nchar_padding_bulkcopy(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, m nvarchar(max), c nchar(20), d char(20))")
    rows = [(i, s, "héllo", "abc") for i, s in enumerate(STRINGS)]
    cursor.bulkcopy(table, rows)
    cursor.execute(f"SELECT k, m, c, d FROM {table} ORDER BY k")
    got = cursor.fetchall()
    assert [r[1] for r in got] == STRINGS
    assert all(r[2] == "héllo".ljust(20) and r[3] == "abc".ljust(20) for r in got)


def test_executemany_strings(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, m nvarchar(max))")
    cursor.executemany(f"INSERT INTO {table} VALUES (?, ?)", list(enumerate(STRINGS)))
    cursor.execute(f"SELECT m FROM {table} ORDER BY k")
    assert [r[0] for r in cursor.fetchall()] == STRINGS
