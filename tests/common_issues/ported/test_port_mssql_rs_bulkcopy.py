# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Adapted from microsoft/mssql-rs mssql-py-core/tests/test_bulkcopy_rows_copied_accuracy.py,
# test_bulkcopy_timeout_zero.py, test_bulkcopy_transactions.py and
# test_bulkcopy_column_mismatch.py (MIT, Copyright (c) Microsoft Corporation).
# The MIT notice above is retained as required. Re-expressed against
# mssql_python.Cursor.bulkcopy (tuple/Row sources) on the DB-API cursor fixtures.
#
# Negative-timeout validation is covered by tests/upstream/test_019_bulkcopy.py and is not repeated.

import pytest


def _create(cursor, name, columns_ddl):
    cursor.execute(f"CREATE TABLE dbo.{name} ({columns_ddl})")
    cursor.connection.commit()


def _rows(cursor, name, order_by="1"):
    cursor.execute(f"SELECT * FROM dbo.{name} ORDER BY {order_by}")
    return [tuple(r) for r in cursor.fetchall()]


def _count(cursor, name):
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{name}")
    return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# rows_copied / batch_count accuracy (issue #209 regression shape)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "row_count,batch_size,expected_batches",
    [(1, 0, 1), (1, 1000, 1), (1000, 0, 1), (5000, 1000, 5), (101, 25, 5)],
)
def test_rows_copied_matches_server_count(cursor, table_name, row_count, batch_size, expected_batches):
    _create(cursor, table_name, "id BIGINT NOT NULL")

    result = cursor.bulkcopy(
        f"dbo.{table_name}", ((i,) for i in range(row_count)), batch_size=batch_size, timeout=60
    )

    actual = _count(cursor, table_name)
    assert actual == row_count
    assert result["rows_copied"] == actual
    assert result["batch_count"] == expected_batches


def test_empty_iterable_copies_zero_rows(cursor, table_name):
    _create(cursor, table_name, "id BIGINT NOT NULL")

    result = cursor.bulkcopy(f"dbo.{table_name}", iter([]))

    assert result["rows_copied"] == 0
    assert _count(cursor, table_name) == 0


def test_batch_size_larger_than_data_is_one_batch(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, value INT NOT NULL")

    result = cursor.bulkcopy(
        f"dbo.{table_name}",
        [(i, i * 10) for i in range(100)],
        batch_size=10_000,
        timeout=0,
    )

    assert result["rows_copied"] == 100
    assert result["batch_count"] == 1
    assert _count(cursor, table_name) == 100


def test_timeout_zero_means_no_timeout_and_completes(cursor, table_name):
    _create(cursor, table_name, "id BIGINT NOT NULL, value BIGINT NOT NULL")

    result = cursor.bulkcopy(
        f"dbo.{table_name}",
        [(i, i * 10) for i in range(100)],
        batch_size=50,
        timeout=0,
        column_mappings=[(0, "id"), (1, "value")],
    )

    assert result["rows_copied"] == 100
    assert result["batch_count"] == 2
    assert _rows(cursor, table_name, "id")[-1] == (99, 990)


def test_multi_batch_preserves_row_order(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, label NVARCHAR(20) NOT NULL")

    cursor.bulkcopy(
        f"dbo.{table_name}",
        [(i, f"row-{i}") for i in range(250)],
        batch_size=60,
    )

    rows = _rows(cursor, table_name, "id")
    assert len(rows) == 250
    assert rows == [(i, f"row-{i}") for i in range(250)]


# ---------------------------------------------------------------------------
# Column mismatch with tuple sources (test_bulkcopy_column_mismatch.py)
# ---------------------------------------------------------------------------


def test_tuple_source_with_explicit_mapping_skipping_column(cursor, table_name):
    _create(cursor, table_name, "id INT PRIMARY KEY, value1 INT NULL, value2 INT NOT NULL")

    result = cursor.bulkcopy(
        f"dbo.{table_name}",
        [(1, 30), (2, 25)],
        column_mappings=[(0, "id"), (1, "value2")],
    )

    assert result["rows_copied"] == 2
    assert _rows(cursor, table_name, "id") == [(1, None, 30), (2, None, 25)]


def test_tuple_source_with_positional_names_and_defaults(cursor, table_name):
    _create(cursor, table_name, "id INT PRIMARY KEY, value1 INT, status INT DEFAULT 999")

    cursor.bulkcopy(f"dbo.{table_name}", [(1, 100), (2, 200)], column_mappings=["id", "value1"])

    assert _rows(cursor, table_name, "id") == [(1, 100, 999), (2, 200, 999)]


def test_row_objects_from_fetch_can_be_bulk_copied(cursor, table_name):
    _create(cursor, f"{table_name}_src", "id INT NOT NULL, label NVARCHAR(20) NULL")
    _create(cursor, table_name, "id INT NOT NULL, label NVARCHAR(20) NULL")
    try:
        cursor.execute(f"INSERT INTO dbo.{table_name}_src VALUES (1, N'a'), (2, NULL), (3, N'c')")
        cursor.connection.commit()
        cursor.execute(f"SELECT id, label FROM dbo.{table_name}_src ORDER BY id")
        rows = cursor.fetchall()

        result = cursor.bulkcopy(f"dbo.{table_name}", rows)

        assert result["rows_copied"] == 3
        assert _rows(cursor, table_name, "id") == [(1, "a"), (2, None), (3, "c")]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{table_name}_src")
        cursor.connection.commit()


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


@pytest.mark.upstream_bug(
    reason="bulkcopy runs on its own connection, so rows bypass the caller's open transaction and survive rollback()"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_bulkcopy_rolled_back_with_open_transaction(conn, cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    conn.autocommit = False

    cursor.bulkcopy(f"dbo.{table_name}", [(i,) for i in range(1, 6)])
    conn.rollback()

    assert _count(cursor, table_name) == 0


def test_bulkcopy_committed_with_explicit_commit(conn, cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    conn.autocommit = False

    cursor.bulkcopy(f"dbo.{table_name}", [(i,) for i in range(1, 6)])
    conn.commit()

    assert _rows(cursor, table_name, "id") == [(1,), (2,), (3,), (4,), (5,)]


def test_internal_transaction_true_commits_multi_batch(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")

    result = cursor.bulkcopy(
        f"dbo.{table_name}",
        [(i,) for i in range(1, 13)],
        batch_size=5,
        use_internal_transaction=True,
    )

    assert result["rows_copied"] == 12
    assert result["batch_count"] == 3
    assert _count(cursor, table_name) == 12


@pytest.mark.upstream_bug(
    reason="tuple bulkcopy returns rows_copied=2 after a mid-stream generator error instead of raising (the Arrow path raises)"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_generator_error_raises_instead_of_truncating(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")

    def rows():
        yield (1,)
        yield (2,)
        raise RuntimeError("generator failed")

    with pytest.raises(Exception, match="generator failed"):
        cursor.bulkcopy(f"dbo.{table_name}", rows(), batch_size=1000)


# ---------------------------------------------------------------------------
# Value errors (test_bulkcopy_errors_timeouts.py)
# ---------------------------------------------------------------------------


def test_null_into_not_null_column_raises_value_error(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, value INT NOT NULL")

    with pytest.raises(ValueError):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, 10), (2, None)])


def test_invalid_type_conversion_raises_value_error(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")

    with pytest.raises(ValueError):
        cursor.bulkcopy(f"dbo.{table_name}", [(1,), ("not-a-number",)])
