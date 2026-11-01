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
