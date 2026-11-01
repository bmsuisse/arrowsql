# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Behaviour adapted from microsoft/mssql-rs (MIT): mssql-tds/tests/test_bulk_copy_timeout.rs,
# test_bulk_copy_udt.rs, test_bulk_copy_with_metadata.rs and
# mssql-py-core/tests/test_bulkcopy_errors_timeouts.py.
"""Bulk copy timeout against a blocked table, UDT (hierarchyid) payloads, metadata mapping."""

import time

import pytest

import mssql_python


def test_bulkcopy_times_out_when_table_is_locked_and_connection_recovers(conn_str, table_name):
    owner = mssql_python.connect(conn_str)
    loader = mssql_python.connect(conn_str)
    try:
        oc = owner.cursor()
        oc.execute(f"CREATE TABLE dbo.{table_name} (id INT)")
        owner.commit()
        oc.execute(f"INSERT INTO dbo.{table_name} WITH (TABLOCKX) VALUES (0)")  # holds X lock, uncommitted
        lc = loader.cursor()
        start = time.monotonic()
        with pytest.raises(Exception):
            lc.bulkcopy(f"dbo.{table_name}", [(1,), (2,)], timeout=1, table_lock=True)
        assert time.monotonic() - start < 15
        loader.rollback()
        assert lc.execute("SELECT 1").fetchone()[0] == 1
        owner.rollback()
        result = lc.bulkcopy(f"dbo.{table_name}", [(1,), (2,)])
        assert result["rows_copied"] == 2
    finally:
        loader.close()  # releases anything it still holds before the table is dropped
        owner.rollback()
        owner.cursor().execute(f"DROP TABLE IF EXISTS dbo.{table_name}")
        owner.commit()
        owner.close()


def test_hierarchyid_udt_bulkcopy_as_bytes(cursor, table_name):
    """A CLR UDT is bulk-copied as its serialized varbinary form."""
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT, h HIERARCHYID)")
    cursor.connection.commit()
    cursor.execute("SELECT CAST('/1/2/' AS HIERARCHYID), CAST('/' AS HIERARCHYID)")
    first, root = [bytes(v) for v in cursor.fetchone()]
    cursor.bulkcopy(f"dbo.{table_name}", [(1, first), (2, root), (3, None)])
    cursor.execute(f"SELECT id, h.ToString() FROM dbo.{table_name} ORDER BY id")
    assert [tuple(r) for r in cursor.fetchall()] == [(1, "/1/2/"), (2, "/"), (3, None)]


def test_column_mappings_by_name_and_by_ordinal(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (a INT, b NVARCHAR(10), c INT)")
    cursor.connection.commit()
    cursor.bulkcopy(f"dbo.{table_name}", [("x", 1), ("y", 2)], column_mappings=[(0, "b"), (1, "c")])
    cursor.execute(f"SELECT a, b, c FROM dbo.{table_name} ORDER BY c")
    assert [tuple(r) for r in cursor.fetchall()] == [(None, "x", 1), (None, "y", 2)]


def test_column_mapping_to_unknown_column_raises(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (a INT)")
    cursor.connection.commit()
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}", [(1,)], column_mappings=[(0, "no_such_column")])
    cursor.connection.rollback()
    assert cursor.execute("SELECT 1").fetchone()[0] == 1
