"""Bulk-loaded decimals are written at the shortest legal TDS magnitude width per value.

SQL Server's bulk receiver reads the per-value length byte, so a decimal(38,2)
cell holding a small number may travel as 5 bytes instead of 17. These tests
pin exact stored values around every width boundary (2^32, 2^64, 2^96, the
precision limit) on both the Arrow and the row path, with NULLs mixed in.
"""

import decimal

import pytest

pa = pytest.importorskip("pyarrow")

from mssql_python import ddbc_bindings  # noqa: E402

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="native bulk copy is a Rust-port feature",
)

D = decimal.Decimal
BOUNDARIES = [0, 1, 99, 100, 10**9 - 1, 10**9, 10**18 - 1, 10**18, 2**32 - 1, 2**32,
              2**64 - 1, 2**64, 2**96 - 1, 2**96, 10**38 - 1]


def _expected(prec, scale):
    limit = 10**prec - 1
    unscaled = sorted({s * v for v in BOUNDARIES if v <= limit for s in (1, -1)})
    ctx = decimal.Context(prec=60)
    return [D(v).scaleb(-scale, ctx) for v in unscaled]


@pytest.mark.parametrize("prec,scale", [(38, 2), (38, 0), (20, 2), (12, 4), (9, 2), (28, 10)])
@pytest.mark.parametrize("path", ["arrow", "rows"])
def test_boundary_values_round_trip_exactly(cursor, table_name, prec, scale, path):
    table = f"dbo.{table_name}"
    values = _expected(prec, scale)
    cursor.execute(f"CREATE TABLE {table} (i int NOT NULL, d decimal({prec},{scale}) NULL)")
    cursor.connection.commit()
    column = values + [None, None]
    if path == "arrow":
        data = pa.table({"i": pa.array(range(len(column)), pa.int32()),
                         "d": pa.array(column, pa.decimal128(prec, scale))})
        cursor.bulkcopy_arrow(table, data, timeout=0)
    else:
        cursor.bulkcopy(table, [(i, v) for i, v in enumerate(column)], timeout=0)
    cursor.execute(f"SELECT CAST(d AS varchar(60)) FROM {table} ORDER BY i")
    got = [r[0] for r in cursor.fetchall()]
    assert got == [None if v is None else format(v, "f") for v in column]


def test_negative_zero_is_stored_as_zero(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (d decimal(38,2) NULL)")
    cursor.connection.commit()
    cursor.bulkcopy(table, [(D("-0.00"),), (D("0.00"),)], timeout=0)
    cursor.execute(f"SELECT CAST(d AS varchar(10)), SIGN(d) FROM {table}")
    assert [tuple(r) for r in cursor.fetchall()] == [("0.00", 0), ("0.00", 0)]
