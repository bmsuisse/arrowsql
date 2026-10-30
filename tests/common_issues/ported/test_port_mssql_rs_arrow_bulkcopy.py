# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Adapted from microsoft/mssql-rs mssql-py-core/tests/test_bulkcopy_arrow_column_mismatch.py,
# test_bulkcopy_arrow_transactions.py and test_bulkcopy_rows_copied_accuracy.py (MIT,
# Copyright (c) Microsoft Corporation). The MIT notice above is retained as required.
# Re-expressed against mssql_python.Cursor.bulkcopy_arrow (DB-API cursor, dbo.<table>
# names, cursor.arrow() read-back) instead of mssql_py_core.PyCoreConnection.
#
# Cases already covered by tests/upstream/test_024_bulkcopy_arrow.py (positional and
# by-name column mappings, empty source, timeout=0, NULL into NOT NULL) are not repeated.

import datetime as dt
import uuid
from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")


def _create(cursor, name, columns_ddl):
    cursor.execute(f"CREATE TABLE dbo.{name} ({columns_ddl})")
    cursor.connection.commit()


def _rows(cursor, name, order_by="1"):
    cursor.execute(f"SELECT * FROM dbo.{name} ORDER BY {order_by}")
    return [tuple(r) for r in cursor.fetchall()]


def _count(cursor, name):
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{name}")
    return cursor.fetchone()[0]


def _int32_table(**columns):
    return pa.table({k: pa.array(v, type=pa.int32()) for k, v in columns.items()})


# ---------------------------------------------------------------------------
# use_internal_transaction (test_bulkcopy_arrow_transactions.py)
# ---------------------------------------------------------------------------


def test_internal_transaction_true_commits_all_rows(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, value INT NOT NULL")
    source = _int32_table(id=[1, 2, 3], value=[100, 200, 300])

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", source, use_internal_transaction=True)

    assert result["rows_copied"] == 3
    assert _rows(cursor, table_name) == [(1, 100), (2, 200), (3, 300)]


def test_internal_transaction_default_commits_all_rows(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, value INT NOT NULL")
    source = _int32_table(id=[1, 2, 3], value=[100, 200, 300])

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", source)

    assert result["rows_copied"] == 3
    assert _count(cursor, table_name) == 3


@pytest.mark.upstream_bug(
    reason="bulkcopy runs on its own connection, so rows bypass the caller's open transaction and survive rollback()"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_arrow_bulkcopy_rolled_back_with_open_transaction(conn, cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    conn.autocommit = False

    cursor.bulkcopy_arrow(f"dbo.{table_name}", _int32_table(id=[1, 2, 3, 4]))
    conn.rollback()

    assert _count(cursor, table_name) == 0


def test_arrow_bulkcopy_committed_with_explicit_commit(conn, cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    conn.autocommit = False

    cursor.bulkcopy_arrow(f"dbo.{table_name}", _int32_table(id=[1, 2, 3, 4]))
    conn.commit()

    assert _rows(cursor, table_name) == [(1,), (2,), (3,), (4,)]


# ---------------------------------------------------------------------------
# batch_size and rows_copied accuracy (decoupled from Arrow RecordBatch layout)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "row_count,batch_size,expected_batches",
    [(1, 0, 1), (1, 1000, 1), (10, 3, 4), (10, 5, 2), (10, 100, 1), (5000, 1000, 5)],
)
def test_rows_copied_and_batch_count_match_server(cursor, table_name, row_count, batch_size, expected_batches):
    _create(cursor, table_name, "id BIGINT NOT NULL")
    source = pa.table({"id": pa.array(range(row_count), type=pa.int64())})

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", source, batch_size=batch_size, timeout=60)

    actual = _count(cursor, table_name)
    assert actual == row_count
    assert result["rows_copied"] == actual
    assert result["batch_count"] == expected_batches


def test_batch_size_zero_reports_single_batch(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    source = _int32_table(id=list(range(1, 6)))

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", source, batch_size=0)

    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1


def test_many_arrow_batches_keep_row_order(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, label NVARCHAR(20) NOT NULL")
    schema = pa.schema([("id", pa.int32()), ("label", pa.string())])
    batches = [
        pa.record_batch(
            [pa.array(range(i * 4, i * 4 + 4), type=pa.int32()),
             pa.array([f"r{j}" for j in range(i * 4, i * 4 + 4)], type=pa.string())],
            schema=schema,
        )
        for i in range(5)
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", reader, batch_size=7)

    assert result["rows_copied"] == 20
    assert _rows(cursor, table_name, "id") == [(i, f"r{i}") for i in range(20)]


def test_mid_stream_generator_error_raises_instead_of_truncating(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    schema = pa.schema([("id", pa.int32())])

    def batches():
        yield pa.record_batch([pa.array([1, 2], type=pa.int32())], schema=schema)
        raise RuntimeError("boom mid-stream")

    with pytest.raises(Exception, match="boom mid-stream"):
        cursor.bulkcopy_arrow(f"dbo.{table_name}", batches(), batch_size=1000, timeout=30)


# ---------------------------------------------------------------------------
# Column count mismatch (test_bulkcopy_arrow_column_mismatch.py)
# ---------------------------------------------------------------------------


def test_auto_mapping_ignores_extra_source_columns(cursor, table_name):
    _create(cursor, table_name, "id INT, value1 INT, value2 INT")
    source = _int32_table(
        id=[1, 2, 3], value1=[100, 200, 300], value2=[30, 25, 35], extra=[777, 777, 777]
    )

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", source)

    assert result["rows_copied"] == 3
    assert _rows(cursor, table_name, "id") == [(1, 100, 30), (2, 200, 25), (3, 300, 35)]


def test_auto_mapping_fewer_source_columns_leaves_nullable_null(cursor, table_name):
    _create(cursor, table_name, "id INT, value1 INT, value2 INT NULL")
    source = _int32_table(id=[1, 2, 3], value1=[100, 200, 300])

    cursor.bulkcopy_arrow(f"dbo.{table_name}", source)

    assert _rows(cursor, table_name, "id") == [(1, 100, None), (2, 200, None), (3, 300, None)]


def test_explicit_mapping_skipping_middle_column_yields_null(cursor, table_name):
    _create(cursor, table_name, "id INT PRIMARY KEY, value1 INT NULL, value2 INT")
    source = _int32_table(id=[1, 2, 3], v2=[30, 25, 35])

    result = cursor.bulkcopy_arrow(
        f"dbo.{table_name}", source, column_mappings=[(0, "id"), (1, "value2")]
    )

    assert result["rows_copied"] == 3
    assert _rows(cursor, table_name, "id") == [(1, None, 30), (2, None, 25), (3, None, 35)]


def test_unmapped_columns_take_table_defaults(cursor, table_name):
    _create(
        cursor,
        table_name,
        "id INT PRIMARY KEY, value1 INT, status INT DEFAULT 999, counter INT DEFAULT 0",
    )
    source = _int32_table(id=[1, 2, 3], value1=[100, 200, 300])

    cursor.bulkcopy_arrow(f"dbo.{table_name}", source, column_mappings=[(0, "id"), (1, "value1")])

    assert _rows(cursor, table_name, "id") == [
        (1, 100, 999, 0),
        (2, 200, 999, 0),
        (3, 300, 999, 0),
    ]


def test_keep_nulls_false_applies_column_default_to_source_null(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, status INT NULL DEFAULT 5")
    source = pa.table(
        {"id": pa.array([1, 2], type=pa.int32()), "status": pa.array([None, 7], type=pa.int32())}
    )

    cursor.bulkcopy_arrow(f"dbo.{table_name}", source)

    assert _rows(cursor, table_name, "id") == [(1, 5), (2, 7)]


def test_keep_nulls_true_stores_source_null_over_default(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, status INT NULL DEFAULT 5")
    source = pa.table(
        {"id": pa.array([1, 2], type=pa.int32()), "status": pa.array([None, 7], type=pa.int32())}
    )

    cursor.bulkcopy_arrow(f"dbo.{table_name}", source, keep_nulls=True)

    assert _rows(cursor, table_name, "id") == [(1, None), (2, 7)]


def test_keep_identity_true_preserves_source_ids(cursor, table_name):
    _create(cursor, table_name, "id INT IDENTITY(1,1) PRIMARY KEY, label NVARCHAR(10) NOT NULL")
    schema = pa.schema([("id", pa.int32()), ("label", pa.string())])
    source = pa.table(
        {"id": pa.array([100, 200], type=pa.int32()), "label": pa.array(["a", "b"])},
        schema=schema,
    )

    cursor.bulkcopy_arrow(f"dbo.{table_name}", source, keep_identity=True)

    assert _rows(cursor, table_name, "id") == [(100, "a"), (200, "b")]


def test_fire_triggers_true_runs_insert_trigger(cursor, table_name):
    audit = f"ci_audit_{uuid.uuid4().hex[:12]}"
    _create(cursor, table_name, "id INT NOT NULL")
    _create(cursor, audit, "src_id INT NOT NULL")
    cursor.execute(
        f"CREATE TRIGGER dbo.trg_{table_name} ON dbo.{table_name} AFTER INSERT AS "
        f"INSERT INTO dbo.{audit} (src_id) SELECT id FROM inserted"
    )
    cursor.connection.commit()
    try:
        cursor.bulkcopy_arrow(f"dbo.{table_name}", _int32_table(id=[1, 2]), fire_triggers=True)
        assert _rows(cursor, audit, "src_id") == [(1,), (2,)]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{audit}")
        cursor.connection.commit()


def test_fire_triggers_false_skips_insert_trigger(cursor, table_name):
    audit = f"ci_audit_{uuid.uuid4().hex[:12]}"
    _create(cursor, table_name, "id INT NOT NULL")
    _create(cursor, audit, "src_id INT NOT NULL")
    cursor.execute(
        f"CREATE TRIGGER dbo.trg_{table_name} ON dbo.{table_name} AFTER INSERT AS "
        f"INSERT INTO dbo.{audit} (src_id) SELECT id FROM inserted"
    )
    cursor.connection.commit()
    try:
        cursor.bulkcopy_arrow(f"dbo.{table_name}", _int32_table(id=[1, 2]))
        assert _count(cursor, audit) == 0
        assert _count(cursor, table_name) == 2
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{audit}")
        cursor.connection.commit()


# ---------------------------------------------------------------------------
# Value round trips through Arrow bulk copy and Arrow reads
# ---------------------------------------------------------------------------


def test_bulkcopy_arrow_round_trip_read_back_as_arrow(cursor, table_name):
    _create(
        cursor,
        table_name,
        "id INT NOT NULL, amount DECIMAL(10,2) NULL, label NVARCHAR(40) NULL, "
        "d DATE NULL, ts DATETIME2(6) NULL",
    )
    d = dt.date(2024, 2, 29)
    ts = dt.datetime(2024, 2, 29, 23, 59, 59, 123456)
    source = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "amount": pa.array([Decimal("12.34"), None, Decimal("-0.01")], type=pa.decimal128(10, 2)),
            "label": pa.array(["héllo ✓", None, ""], type=pa.string()),
            "d": pa.array([d, None, dt.date(1900, 1, 1)], type=pa.date32()),
            "ts": pa.array([ts, None, dt.datetime(1, 1, 1)], type=pa.timestamp("us")),
        }
    )

    result = cursor.bulkcopy_arrow(f"dbo.{table_name}", source)
    assert result["rows_copied"] == 3

    read_back = cursor.execute(f"SELECT * FROM dbo.{table_name} ORDER BY id").arrow()
    assert read_back.column("id").to_pylist() == [1, 2, 3]
    assert read_back.column("amount").to_pylist() == [Decimal("12.34"), None, Decimal("-0.01")]
    assert read_back.column("label").to_pylist() == ["héllo ✓", None, ""]
    assert read_back.column("d").to_pylist() == [d, None, dt.date(1900, 1, 1)]
    assert read_back.column("ts").to_pylist() == [ts, None, dt.datetime(1, 1, 1)]
