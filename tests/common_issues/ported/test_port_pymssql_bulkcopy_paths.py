"""Bulk copy batching, error paths and column naming.

Test ideas inspired by pymssql's bulk copy tests (pymssql/pymssql
tests/test_bulk_copy.py, LGPL-2.1). No code or test text was copied: the
cases below were re-derived from the behaviours they check and are written
fresh against mssql_python's ``cursor.bulkcopy()``.

Temporary-table bulk copy is deliberately not covered here: bulk copy can run
on its own connection, so session temp tables are not visible to it (see
test_port_rust_mssql_driver_bulkcopy.py).
"""

import pytest


def _create(cursor, table, ddl):
    cursor.execute(f"CREATE TABLE dbo.{table} ({ddl})")
    cursor.connection.commit()


def _count(cursor, table):
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table}")
    return cursor.fetchone()[0]


def test_generator_source_copies_every_row(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    rows = ((i, i * 2, i * 3) for i in range(1000))
    result = cursor.bulkcopy(f"dbo.{table_name}", rows, batch_size=1000)
    assert result["rows_copied"] == 1000
    assert _count(cursor, table_name) == 1000


def test_exact_batch_boundary_reports_two_batches(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    result = cursor.bulkcopy(f"dbo.{table_name}", [(1, 2, 3)] * 2000, batch_size=1000)
    assert result["rows_copied"] == 2000
    assert result["batch_count"] == 2
    assert _count(cursor, table_name) == 2000


def test_batch_larger_than_data_is_one_batch(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    result = cursor.bulkcopy(f"dbo.{table_name}", [(1, 1, 1), (2, 2, 2), (3, 3, 3)],
                             batch_size=10000)
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1


def test_no_rows_lost_or_duplicated_across_many_batches(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b BIGINT NOT NULL")
    n = 20000
    cursor.bulkcopy(f"dbo.{table_name}", [(i, i * 1000) for i in range(n)], batch_size=1000)
    cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT a), SUM(b) FROM dbo.{table_name}")
    count, distinct, total = tuple(cursor.fetchone())
    assert (count, distinct) == (n, n)
    assert total == sum(i * 1000 for i in range(n))


def test_repeated_bulkcopy_calls_report_their_own_row_counts(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    first = cursor.bulkcopy(f"dbo.{table_name}", [(1, 1, 1)] * 5)
    second = cursor.bulkcopy(f"dbo.{table_name}", [(2, 2, 2)] * 7)
    assert first["rows_copied"] == 5
    assert second["rows_copied"] == 7
    assert _count(cursor, table_name) == 12


def test_nulls_in_every_seventh_row_across_batches(cursor, table_name):
    _create(cursor, table_name, "a INT NULL, b NVARCHAR(20) NULL")
    rows = [(None, None) if i % 7 == 0 else (i, f"r{i}") for i in range(2500)]
    cursor.bulkcopy(f"dbo.{table_name}", rows, batch_size=1000)
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE a IS NULL AND b IS NULL")
    expected_nulls = sum(1 for i in range(2500) if i % 7 == 0)
    assert cursor.fetchone()[0] == expected_nulls
    assert _count(cursor, table_name) == 2500


def test_value_conversion_failure_raises_valueerror(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    with pytest.raises(ValueError):
        cursor.bulkcopy(f"dbo.{table_name}", [("Hello", 7, 7)])


def test_cursor_and_connection_usable_after_failed_bulkcopy(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    with pytest.raises(ValueError):
        cursor.bulkcopy(f"dbo.{table_name}", [("Hello", 7, 7)])
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
    result = cursor.bulkcopy(f"dbo.{table_name}", [(4, 5, 6)])
    assert result["rows_copied"] == 1


def test_column_mapping_index_beyond_source_tuple_raises(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b INT NOT NULL, c INT NOT NULL")
    with pytest.raises(ValueError, match="out of bounds"):
        cursor.bulkcopy(
            f"dbo.{table_name}",
            [(1, 2, 3)],
            column_mappings=[(1, "a"), (2, "b"), (9, "c")],
        )


def test_unicode_destination_column_names_roundtrip(cursor, table_name):
    _create(cursor, table_name, "[名前] NVARCHAR(50) NOT NULL, [Größe] INT NOT NULL")
    data = [("Renée", 1), ("Привет", 2), ("スワップ", 3)]
    cursor.bulkcopy(f"dbo.{table_name}", data)
    cursor.execute(f"SELECT [名前], [Größe] FROM dbo.{table_name} ORDER BY [Größe]")
    assert [tuple(r) for r in cursor.fetchall()] == data
