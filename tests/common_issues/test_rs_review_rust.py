"""Regression tests for the r4 full-sweep review (Rust core): B1, S3, S4, S9, S10, S11."""

import decimal
import os

import pytest
from arrowsql import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False), reason="Rust-port only"
)

pa = pytest.importorskip("pyarrow")


# ------------------------------------------------------------------ B1
@pytest.mark.parametrize("collation", ["SQL_Latin1_General_CP437_CI_AS", "SQL_Latin1_General_CP850_CI_AS"])
def test_b1_oem_varchar_decodes_the_same_on_every_path(cursor, conn_str, table_name, collation):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int, v varchar(20) COLLATE {collation})")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1, CAST(0xE9FC41 AS varchar(20)) COLLATE {collation})")
    cursor.connection.commit()
    sql = f"SELECT v FROM dbo.{table_name}"
    want = cursor.execute(sql).fetchall()[0][0]
    assert "�" not in want and want.endswith("A") and len(want) == 3

    assert pa.table(cursor.execute(sql).fetchall_arrow()).column(0).to_pylist() == [want]
    assert cursor.execute(sql).arrow().column(0).to_pylist() == [want]
    for utf8 in (True, False):
        got = pa.table(cursor.execute_arrow(sql, utf8_strings=utf8)).column(0).to_pylist()
        assert got == [want]
    from arrowsql.agent import query

    assert want in query(conn_str, sql)


# ------------------------------------------------------------------ S4 / S6
def test_s4_bytearray_cell_loads_through_row_bulkcopy(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id int, b varbinary(20))")
    cursor.bulkcopy(table, [(1, bytearray(b"\x01\x02\xff")), (2, bytearray())])
    cursor.connection.commit()
    cursor.execute(f"SELECT b FROM {table} ORDER BY id")
    assert [bytes(r[0]) for r in cursor.fetchall()] == [b"\x01\x02\xff", b""]


def test_s6_description_stays_shared_between_executions(cursor):
    cursor.execute("SELECT 1 AS a, 'x' AS b")
    first = cursor.description
    cursor.fetchall()
    cursor.execute("SELECT 2 AS a, 'y' AS b")
    assert [c[0] for c in cursor.description] == ["a", "b"]
    assert [c[1] for c in cursor.description] == [c[1] for c in first]
    cursor.fetchall()


# ------------------------------------------------------------------ S9
def test_s9_ragged_executemany_rows_raise_a_dbapi_error_not_a_panic(cursor):
    cursor.execute("CREATE TABLE #rag (a int, b int)")
    with pytest.raises((IndexError, ValueError, TypeError)):
        cursor.executemany("INSERT INTO #rag VALUES (?, ?)", [(1, 2), (3,)])
    cursor.execute("SELECT COUNT(*) FROM #rag")
    assert cursor.fetchone()[0] == 0


# ------------------------------------------------------------------ S10
SQL_DECIMAL = 3
SQL_MONEY = 60
D = decimal.Decimal


@pytest.mark.parametrize("value, want", [(0.00005, D("0.00")), (0.005, D("0.01")), (1e-05, D("0.00")), (2.5e-3, D("0.00")), (5e-3, D("0.01"))])
def test_s10_decimal_hint_rounds_exponent_text(cursor, value, want):
    cursor.setinputsizes([(SQL_DECIMAL, 10, 2)])
    cursor.execute("SELECT CAST(? AS decimal(10, 2))", (value,))
    assert cursor.fetchone()[0] == want


@pytest.mark.parametrize("value, want", [
    (D("2.00005"), D("2.0001")), (D("-2.00005"), D("-2.0001")), (D("2.00015"), D("2.0002")),
    (D("2.00004"), D("2.0000")), (D("0.00005"), D("0.0001")),
])
def test_s10_money_rounds_half_away_on_every_path(cursor, table_name, value, want):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (m money)")

    # Hinted bind, under an ambient decimal context that would round half-even at prec 3.
    # setinputsizes() rejects MONEY, so install the hint the native binding reads directly.
    cursor._inputsizes = [(SQL_MONEY, 1, 19, 4)]
    ctx = decimal.getcontext()
    saved = ctx.prec, ctx.rounding
    ctx.prec, ctx.rounding = 3, decimal.ROUND_HALF_EVEN
    try:
        cursor.execute(f"INSERT INTO {table} VALUES (?)", (value,))
    finally:
        ctx.prec, ctx.rounding = saved
    cursor._inputsizes = []
    # Row bulk copy.
    cursor.bulkcopy(table, [(value,)])
    cursor.connection.commit()
    cursor.execute(f"SELECT m FROM {table}")
    assert [r[0] for r in cursor.fetchall()] == [want, want]


def test_s10_arrow_bulk_money_matches_row_bulk(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (m money)")
    cursor.bulkcopy(table, [(D("2.00005"),), (D("2.00015"),), (D("-2.00005"),)])
    cursor.connection.commit()
    cursor.execute(f"SELECT m FROM {table} ORDER BY m")
    assert [r[0] for r in cursor.fetchall()] == [D("-2.0001"), D("2.0001"), D("2.0002")]


# ------------------------------------------------------------------ S11
def test_s11_bulkcopy_without_mappings_rejects_extra_source_columns(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, b int)")
    cursor.connection.commit()
    with pytest.raises(Exception, match=r"source has 3 columns, destination has 2"):
        cursor.bulkcopy(table, [(1, 2, 3)])
    cursor.connection.rollback()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    assert cursor.fetchone()[0] == 0


# ------------------------------------------------------------------ cancel / close
def test_closing_a_partly_read_result_does_not_wait_for_a_locked_row(conn_str, table_name):
    """Skimming the rest of the result on close must not wait for rows the server cannot send."""
    import threading

    from mssql_python import connect

    holder = connect(conn_str)
    reader = connect(conn_str)
    try:
        h = holder.cursor()
        h.execute(f"CREATE TABLE dbo.{table_name} (id int primary key, v int)")
        h.execute(
            f"INSERT INTO dbo.{table_name} SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 0 "
            "FROM sys.all_columns a, sys.all_columns b"
        )
        holder.commit()
        h.execute(f"UPDATE dbo.{table_name} SET v = 1 WHERE id = 20000")  # the last row stays locked

        r = reader.cursor()
        r.timeout = 0
        r.execute(f"SELECT id, v FROM dbo.{table_name} ORDER BY id")
        assert r.fetchone()[0] == 1
        done = threading.Event()
        threading.Thread(target=lambda: (r.close(), done.set()), daemon=True).start()
        finished = done.wait(20)
        holder.rollback()  # unblocks a hung close so the thread and fixtures can end
        assert finished, "closing the cursor blocked on a locked row"
    finally:
        reader.close()
        c = holder.cursor()
        c.execute(f"DROP TABLE IF EXISTS dbo.{table_name}")
        holder.commit()
        holder.close()
