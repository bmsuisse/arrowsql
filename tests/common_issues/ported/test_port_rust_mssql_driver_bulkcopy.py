"""Bulk copy behaviour (cursor.bulkcopy) ported from rust-mssql-driver's bulk insert tests.

Adapted from praxiomlabs/rust-mssql-driver crates/mssql-client/tests/bulk_insert.rs
(Apache-2.0), Copyright the rust-mssql-driver contributors (praxiomlabs).
Upstream license: Apache License, Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0).
The cases were re-expressed against mssql_python's cursor.bulkcopy() API; no code was copied.

Covers NULLs, mixed types, binary/GUID, MAX types, large batches, table lock,
column mappings, identity handling, trigger firing, transactions and argument
validation. Tables are permanent (bulk copy may use its own connection, so
session temp tables are not used).
"""

import datetime
import decimal
import uuid

import pytest

import mssql_python


def _create(cursor, table, ddl):
    cursor.execute(f"CREATE TABLE dbo.{table} ({ddl})")
    cursor.connection.commit()


def _rows(cursor, sql):
    cursor.execute(sql)
    return [tuple(r) for r in cursor.fetchall()]


def _drop(cursor, *tables):
    for t in tables:
        cursor.execute(f"IF OBJECT_ID('dbo.{t}', 'U') IS NOT NULL DROP TABLE dbo.{t}")
    cursor.connection.commit()


def test_null_values_round_trip(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, name NVARCHAR(100) NULL, age INT NULL")
    data = [(1, "Alice", 30), (2, None, 25), (3, "Carol", None)]

    result = cursor.bulkcopy(f"dbo.{table_name}", data)

    assert result["rows_copied"] == 3
    got = _rows(cursor, f"SELECT id, name, age FROM dbo.{table_name} ORDER BY id")
    assert got == data


def test_mixed_types_round_trip(cursor, table_name):
    _create(
        cursor,
        table_name,
        "i INT NOT NULL, big BIGINT NULL, flag BIT NULL, f FLOAT NULL, "
        "dec DECIMAL(18,4) NULL, dt2 DATETIME2(7) NULL, d DATE NULL, s NVARCHAR(50) NULL",
    )
    data = [
        (1, 9_223_372_036_854_775_807, True, 1.5, decimal.Decimal("12345.6789"),
         datetime.datetime(2024, 2, 29, 23, 59, 59, 123456), datetime.date(2024, 2, 29), "x"),
        (-2, -5, False, -0.25, decimal.Decimal("-0.0001"),
         datetime.datetime(1900, 1, 1, 0, 0, 0), datetime.date(1, 1, 1), ""),
        (3, None, None, None, None, None, None, None),
    ]

    cursor.bulkcopy(f"dbo.{table_name}", data)

    got = _rows(cursor, f"SELECT i, big, flag, f, dec, dt2, d, s FROM dbo.{table_name} ORDER BY i")
    # ORDER BY i sorts -2 before 1 and 3.
    assert got[0][:3] == (-2, -5, False)
    assert got[0][3] == -0.25
    assert got[0][4] == decimal.Decimal("-0.0001")
    assert got[0][6] == datetime.date(1, 1, 1)
    assert got[0][7] == ""
    assert got[1] == (1, 9_223_372_036_854_775_807, True, 1.5, decimal.Decimal("12345.6789"),
                       datetime.datetime(2024, 2, 29, 23, 59, 59, 123456), datetime.date(2024, 2, 29), "x")
    assert got[2] == (3, None, None, None, None, None, None, None)


def test_binary_and_guid_round_trip(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, b VARBINARY(16) NULL, g UNIQUEIDENTIFIER NULL")
    guid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    data = [
        (1, bytes([0, 1, 2, 0xFF, 0]), guid),
        (2, b"", None),
        (3, bytes(16), uuid.UUID(int=0)),
    ]

    cursor.bulkcopy(f"dbo.{table_name}", data)

    got = _rows(cursor, f"SELECT id, b, g FROM dbo.{table_name} ORDER BY id")
    assert got == [
        (1, bytes([0, 1, 2, 0xFF, 0]), guid),
        (2, b"", None),
        (3, bytes(16), uuid.UUID(int=0)),
    ]


def test_unicode_and_supplementary_plane_round_trip(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, s NVARCHAR(100) NULL")
    values = ["café", "日本語テキスト", "😀🚀 emoji", "mixed ✓ ü 中文", "x" * 10]
    data = list(enumerate(values, start=1))

    cursor.bulkcopy(f"dbo.{table_name}", data)

    got = _rows(cursor, f"SELECT id, s FROM dbo.{table_name} ORDER BY id")
    assert got == data
    assert [len(s) for _, s in got] == [len(v) for v in values]


def test_empty_string_is_not_null(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, s NVARCHAR(10) NULL")

    cursor.bulkcopy(f"dbo.{table_name}", [(1, ""), (2, None)])

    got = _rows(cursor, f"SELECT id, s, CASE WHEN s IS NULL THEN 1 ELSE 0 END FROM dbo.{table_name} ORDER BY id")
    assert got == [(1, "", 0), (2, None, 1)]


def test_nvarchar_max_large_value(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, body NVARCHAR(MAX) NULL")
    big = ("abcé日😀" * 40_000)  # ~200k code points, several PLP chunks

    cursor.bulkcopy(f"dbo.{table_name}", [(1, big)])

    cursor.execute(f"SELECT body FROM dbo.{table_name} WHERE id = 1")
    assert cursor.fetchone()[0] == big


def test_varbinary_max_large_payload(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, payload VARBINARY(MAX) NULL")
    payload = bytes(range(256)) * 4000  # ~1 MB

    cursor.bulkcopy(f"dbo.{table_name}", [(1, payload), (2, b"\x00" * 10)])

    got = _rows(cursor, f"SELECT id, payload FROM dbo.{table_name} ORDER BY id")
    assert got[0][1] == payload
    assert got[1][1] == b"\x00" * 10


def test_decimal_precision_boundaries(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, wide DECIMAL(38,0) NULL, money2 DECIMAL(10,2) NULL")
    # Build the 38-digit values from ints: unary minus on a Decimal would round to
    # the default 28-digit context and produce a 39-digit value.
    max38 = decimal.Decimal(10**38 - 1)
    data = [
        (1, max38, decimal.Decimal("-99999999.99")),
        (2, decimal.Decimal(-(10**38 - 1)), decimal.Decimal("0.01")),
    ]

    cursor.bulkcopy(f"dbo.{table_name}", data)

    got = _rows(cursor, f"SELECT id, wide, money2 FROM dbo.{table_name} ORDER BY id")
    assert got == data


def test_datetime2_fraction_precision(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, ts DATETIME2(6) NULL")
    stamps = [
        datetime.datetime(2020, 1, 1, 0, 0, 0, 1),
        datetime.datetime(2023, 6, 15, 12, 30, 45, 999999),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 123456),
    ]

    cursor.bulkcopy(f"dbo.{table_name}", list(enumerate(stamps, start=1)))

    got = _rows(cursor, f"SELECT id, ts FROM dbo.{table_name} ORDER BY id")
    assert [ts for _, ts in got] == stamps


def test_zero_rows_copies_nothing(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")

    result = cursor.bulkcopy(f"dbo.{table_name}", [])

    assert result["rows_copied"] == 0
    assert _rows(cursor, f"SELECT COUNT(*) FROM dbo.{table_name}") == [(0,)]


def test_large_batch_with_explicit_batch_size(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, v NVARCHAR(20) NOT NULL")
    n = 50_000

    result = cursor.bulkcopy(
        f"dbo.{table_name}",
        ((i, f"v{i}") for i in range(n)),
        batch_size=5_000,
    )

    assert result["rows_copied"] == n
    assert _rows(cursor, f"SELECT COUNT_BIG(*), SUM(CAST(id AS BIGINT)) FROM dbo.{table_name}") == [
        (n, sum(range(n)))
    ]


def test_table_lock_option_loads_all_rows(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, val NVARCHAR(50) NOT NULL")
    data = [(i, f"val_{i}") for i in range(1, 11)]

    result = cursor.bulkcopy(f"dbo.{table_name}", data, table_lock=True)

    assert result["rows_copied"] == 10
    assert _rows(cursor, f"SELECT id, val FROM dbo.{table_name} ORDER BY id") == data


def test_bulk_appends_to_existing_rows(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    cursor.execute(f"INSERT INTO dbo.{table_name} (id) VALUES (?), (?)", [100, 101])
    cursor.connection.commit()

    cursor.bulkcopy(f"dbo.{table_name}", [(1,), (2,), (3,)])

    assert _rows(cursor, f"SELECT id FROM dbo.{table_name} ORDER BY id") == [
        (1,), (2,), (3,), (100,), (101,)
    ]


def test_column_mappings_simple_list_matches_by_position(cursor, table_name):
    _create(cursor, table_name, "a INT NOT NULL, b NVARCHAR(10) NOT NULL")

    # Source index 0 goes to column b, source index 1 goes to column a.
    cursor.bulkcopy(f"dbo.{table_name}", [("x", 1), ("y", 2)], column_mappings=["b", "a"])

    assert _rows(cursor, f"SELECT a, b FROM dbo.{table_name} ORDER BY a") == [(1, "x"), (2, "y")]


def test_column_mappings_advanced_skips_source_column(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, name NVARCHAR(20) NOT NULL, note NVARCHAR(20) NULL")

    # Source index 1 is deliberately skipped; the unmapped target column stays NULL.
    cursor.bulkcopy(
        f"dbo.{table_name}",
        [(7, "ignored", "Alice")],
        column_mappings=[(0, "id"), (2, "name")],
    )

    assert _rows(cursor, f"SELECT id, name, note FROM dbo.{table_name}") == [(7, "Alice", None)]


def test_keep_identity_preserves_explicit_values(cursor, table_name):
    _create(cursor, table_name, "id INT IDENTITY(1,1) NOT NULL, name NVARCHAR(10) NOT NULL")

    cursor.bulkcopy(
        f"dbo.{table_name}",
        [(100, "a"), (200, "b")],
        keep_identity=True,
    )

    assert _rows(cursor, f"SELECT id, name FROM dbo.{table_name} ORDER BY id") == [(100, "a"), (200, "b")]


def test_identity_assigned_when_not_kept(cursor, table_name):
    _create(cursor, table_name, "id INT IDENTITY(1,1) NOT NULL, name NVARCHAR(10) NOT NULL")

    cursor.bulkcopy(f"dbo.{table_name}", [("a",), ("b",)], column_mappings=["name"])

    assert _rows(cursor, f"SELECT id, name FROM dbo.{table_name} ORDER BY id") == [(1, "a"), (2, "b")]


def test_insert_trigger_not_fired_by_default(cursor, table_name):
    log = f"{table_name}_log"
    _create(cursor, table_name, "id INT NOT NULL")
    _create(cursor, log, "src_id INT NOT NULL")
    try:
        cursor.execute(
            f"CREATE TRIGGER dbo.trg_{table_name} ON dbo.{table_name} AFTER INSERT AS "
            f"INSERT INTO dbo.{log} (src_id) SELECT id FROM inserted"
        )
        cursor.connection.commit()

        cursor.bulkcopy(f"dbo.{table_name}", [(1,), (2,), (3,)])

        assert _rows(cursor, f"SELECT COUNT(*) FROM dbo.{table_name}") == [(3,)]
        assert _rows(cursor, f"SELECT COUNT(*) FROM dbo.{log}") == [(0,)]
    finally:
        _drop(cursor, log)


def test_insert_trigger_fires_when_requested(cursor, table_name):
    log = f"{table_name}_log"
    _create(cursor, table_name, "id INT NOT NULL")
    _create(cursor, log, "src_id INT NOT NULL")
    try:
        cursor.execute(
            f"CREATE TRIGGER dbo.trg_{table_name} ON dbo.{table_name} AFTER INSERT AS "
            f"INSERT INTO dbo.{log} (src_id) SELECT id FROM inserted"
        )
        cursor.connection.commit()

        cursor.bulkcopy(f"dbo.{table_name}", [(1,), (2,), (3,)], fire_triggers=True)

        assert _rows(cursor, f"SELECT src_id FROM dbo.{log} ORDER BY src_id") == [(1,), (2,), (3,)]
    finally:
        _drop(cursor, log)


def test_not_null_violation_raises_and_loads_nothing(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, name NVARCHAR(10) NOT NULL")

    # The driver rejects NULL for a NOT NULL column before sending it (ValueError upstream).
    with pytest.raises((mssql_python.Error, ValueError)):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, "ok"), (2, None)])

    # The connection stays usable after the failed copy.
    assert _rows(cursor, "SELECT 1") == [(1,)]


def test_rows_from_fetchall_can_be_copied_into_another_table(cursor, table_name):
    target = f"{table_name}_copy"
    _create(cursor, table_name, "id INT NOT NULL, s NVARCHAR(20) NULL")
    _create(cursor, target, "id INT NOT NULL, s NVARCHAR(20) NULL")
    try:
        cursor.execute(f"INSERT INTO dbo.{table_name} (id, s) VALUES (?, ?), (?, ?)", [1, "one", 2, None])
        cursor.connection.commit()
        cursor.execute(f"SELECT id, s FROM dbo.{table_name} ORDER BY id")
        rows = cursor.fetchall()

        result = cursor.bulkcopy(f"dbo.{target}", rows)

        assert result["rows_copied"] == 2
        assert _rows(cursor, f"SELECT id, s FROM dbo.{target} ORDER BY id") == [(1, "one"), (2, None)]
    finally:
        _drop(cursor, target)


def test_internal_transaction_survives_rollback(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    cursor.connection.commit()

    cursor.bulkcopy(f"dbo.{table_name}", [(1,), (2,)], use_internal_transaction=True)
    cursor.connection.rollback()

    # use_internal_transaction commits each batch itself, so rollback cannot undo it.
    assert _rows(cursor, f"SELECT COUNT(*) FROM dbo.{table_name}") == [(2,)]


def test_bulkcopy_then_commit_is_visible_to_new_connection(cursor, table_name, conn_str):
    _create(cursor, table_name, "id INT NOT NULL")

    cursor.bulkcopy(f"dbo.{table_name}", [(1,), (2,), (3,)])
    cursor.connection.commit()

    other = mssql_python.connect(conn_str)
    try:
        cur = other.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        assert cur.fetchone()[0] == 3
    finally:
        other.close()


def test_connection_usable_after_bulkcopy(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    cursor.bulkcopy(f"dbo.{table_name}", [(1,)])

    assert _rows(cursor, f"SELECT id FROM dbo.{table_name} WHERE id = 1") == [(1,)]


def test_string_source_is_rejected(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    with pytest.raises(TypeError):
        cursor.bulkcopy(f"dbo.{table_name}", "abc")


def test_none_source_is_rejected(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    with pytest.raises(TypeError):
        cursor.bulkcopy(f"dbo.{table_name}", None)
