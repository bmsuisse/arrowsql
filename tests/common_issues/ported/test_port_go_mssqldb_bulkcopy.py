"""Bulk copy behaviour ported from microsoft/go-mssqldb.

Adapted from microsoft/go-mssqldb bulkcopy_test.go, bulkcopy_midnight_test.go
and money_test.go (BSD-3-Clause, Copyright (c) 2012 The Go Authors;
Copyright (c) Microsoft Corporation). The license text is in
LICENSE_go_mssqldb.txt next to this file. Go's API (Bulk, CopyIn, BulkOptions,
makeParam) is not ported; the cases are re-expressed through cursor.bulkcopy
and checked against server round trips.
"""

import datetime as dt
import uuid
from decimal import Decimal

import pytest


def _create(cursor, name, columns_ddl):
    cursor.execute(f"CREATE TABLE dbo.{name} ({columns_ddl})")
    cursor.connection.commit()


def _rows(cursor, name, order_by="1"):
    cursor.execute(f"SELECT * FROM dbo.{name} ORDER BY {order_by}")
    return [tuple(r) for r in cursor.fetchall()]


# ---------------------------------------------------------------------------
# datetime near midnight through the bulk path (bulkcopy_midnight_test.go)
# ---------------------------------------------------------------------------

_ONE_TICK_TOLERANCE = dt.timedelta(microseconds=3334)  # datetime precision is 1/300 s

MIDNIGHT_CASES = [
    dt.datetime(2025, 1, 1, 23, 59, 59, 998350),  # the value from the go-mssqldb issue report
    dt.datetime(2025, 1, 1, 23, 59, 59, 999000),
    dt.datetime(2025, 1, 1, 23, 59, 59, 999999),
    dt.datetime(2025, 1, 1, 23, 59, 59, 997000),
    dt.datetime(2025, 1, 1, 23, 59, 59, 996000),
    dt.datetime(2025, 1, 2, 0, 0, 0, 0),
]


@pytest.mark.parametrize("value", MIDNIGHT_CASES, ids=lambda v: v.isoformat())
def test_bulkcopy_datetime_near_midnight_stays_within_one_tick(cursor, table_name, value):
    _create(cursor, table_name, "id INT NOT NULL, ts DATETIME NOT NULL")
    result = cursor.bulkcopy(f"dbo.{table_name}", [(1, value)])
    assert result["rows_copied"] == 1

    (_, stored), = _rows(cursor, table_name)
    assert abs(stored - value) <= _ONE_TICK_TOLERANCE
    # Rounding up at 23:59:59.99x may roll into the next day, never further.
    assert stored.date() in (value.date(), value.date() + dt.timedelta(days=1))


def test_bulkcopy_datetime_near_midnight_batch_keeps_order(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, ts DATETIME NOT NULL")
    rows = [(i, v) for i, v in enumerate(MIDNIGHT_CASES)]
    cursor.bulkcopy(f"dbo.{table_name}", rows, batch_size=2)
    stored = _rows(cursor, table_name, "id")
    assert [r[0] for r in stored] == list(range(len(MIDNIGHT_CASES)))
    for (_, got), want in zip(stored, MIDNIGHT_CASES):
        assert abs(got - want) <= _ONE_TICK_TOLERANCE


# ---------------------------------------------------------------------------
# NULLs and typed round trips (bulkcopy_test.go, setupNullableTypeTable)
# ---------------------------------------------------------------------------

NULLABLE_DDL = (
    "id INT IDENTITY(1,1) NOT NULL PRIMARY KEY, "
    "f FLOAT NULL, s NVARCHAR(50) NULL, t TINYINT NULL, b BIT NULL, "
    "bi BIGINT NULL, i INT NULL, si SMALLINT NULL, dtm DATETIME NULL, "
    "u UNIQUEIDENTIFIER NULL, d DECIMAL(18,4) NULL, m MONEY NULL"
)
NULLABLE_COLS = ["f", "s", "t", "b", "bi", "i", "si", "dtm", "u", "d", "m"]


def test_bulkcopy_all_null_row_reads_back_as_none(cursor, table_name):
    _create(cursor, table_name, NULLABLE_DDL)
    cursor.bulkcopy(
        f"dbo.{table_name}",
        [(None,) * len(NULLABLE_COLS)],
        column_mappings=NULLABLE_COLS,
    )
    cursor.execute(f"SELECT {', '.join(NULLABLE_COLS)} FROM dbo.{table_name}")
    assert [tuple(r) for r in cursor.fetchall()] == [(None,) * len(NULLABLE_COLS)]


def test_bulkcopy_mixed_null_and_value_rows_per_type(cursor, table_name):
    _create(cursor, table_name, NULLABLE_DDL)
    guid = uuid.UUID("01234567-89ab-cdef-0123-456789abcdef")
    full = (
        1.5,
        "naïve ✓",
        255,
        True,
        2**63 - 1,
        -(2**31),
        -32768,
        dt.datetime(2024, 2, 29, 12, 0, 0),
        guid,
        Decimal("12345678901234.5678"),
        Decimal("922337203685477.5807"),
    )
    nulls = (None,) * len(NULLABLE_COLS)
    cursor.bulkcopy(f"dbo.{table_name}", [nulls, full], column_mappings=NULLABLE_COLS)

    cursor.execute(f"SELECT {', '.join(NULLABLE_COLS)} FROM dbo.{table_name} WHERE f IS NULL")
    assert [tuple(r) for r in cursor.fetchall()] == [nulls]

    cursor.execute(f"SELECT {', '.join(NULLABLE_COLS)} FROM dbo.{table_name} WHERE f IS NOT NULL")
    (got,) = cursor.fetchall()
    assert got[0] == 1.5
    assert got[1] == "naïve ✓"
    assert got[2] == 255 and got[3] is True
    assert got[4] == 2**63 - 1 and got[5] == -(2**31) and got[6] == -32768
    assert got[7] == dt.datetime(2024, 2, 29, 12, 0, 0)
    assert str(got[8]).lower() == str(guid)
    assert Decimal(str(got[9])) == Decimal("12345678901234.5678")
    assert Decimal(str(got[10])) == Decimal("922337203685477.5807")


# ---------------------------------------------------------------------------
# Money and uniqueidentifier edges (money_test.go, uniqueidentifier_test.go)
# ---------------------------------------------------------------------------

MONEY_VALUES = [
    Decimal("922337203685477.5807"),
    Decimal("-922337203685477.5808"),
    Decimal("0.0001"),
    Decimal("-0.0001"),
    Decimal("0.0000"),
]


def test_bulkcopy_money_extremes_round_trip_exactly(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, m MONEY NOT NULL, sm SMALLMONEY NULL")
    rows = [(i, v, None) for i, v in enumerate(MONEY_VALUES)]
    result = cursor.bulkcopy(f"dbo.{table_name}", rows)
    assert result["rows_copied"] == len(MONEY_VALUES)
    stored = _rows(cursor, table_name, "id")
    assert [r[1] for r in stored] == MONEY_VALUES
    assert all(r[2] is None for r in stored)


def test_bulkcopy_smallmoney_out_of_range_raises(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, sm SMALLMONEY NULL")
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, Decimal("300000.0000"))])


def test_bulkcopy_uniqueidentifier_values_round_trip(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, u UNIQUEIDENTIFIER NOT NULL")
    guids = [uuid.UUID(int=0), uuid.UUID(int=2**128 - 1), uuid.uuid4()]
    cursor.bulkcopy(f"dbo.{table_name}", [(i, g) for i, g in enumerate(guids)])
    stored = _rows(cursor, table_name, "id")
    assert [str(r[1]).lower() for r in stored] == [str(g) for g in guids]


def test_bulkcopy_uniqueidentifier_join_matches_regardless_of_case(cursor, table_name):
    _create(cursor, table_name, "u UNIQUEIDENTIFIER NOT NULL")
    guid = uuid.UUID("0123abcd-89ab-cdef-0123-456789ABCDEF")
    cursor.bulkcopy(f"dbo.{table_name}", [(guid,)])
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE u = ?", (str(guid).upper(),))
    assert cursor.fetchone()[0] == 1
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name} WHERE u = ?", (str(guid).lower(),))
    assert cursor.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Large and wide values
# ---------------------------------------------------------------------------


def test_bulkcopy_unicode_nvarchar_max_round_trip(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, body NVARCHAR(MAX) NOT NULL")
    body = ("漢字😀ü" * 10000) + "END"  # 50k chars incl. surrogate pairs
    cursor.bulkcopy(f"dbo.{table_name}", [(1, body)])
    cursor.execute(f"SELECT body, LEN(body) FROM dbo.{table_name}")
    got, length = cursor.fetchone()
    assert got == body
    # LEN counts UTF-16 code units, so each emoji (surrogate pair) counts twice.
    assert length == len(body.encode("utf-16-le")) // 2


def test_bulkcopy_datetime2_7_keeps_microseconds(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, ts DATETIME2(7) NOT NULL")
    value = dt.datetime(2025, 6, 30, 23, 59, 59, 123456)
    cursor.bulkcopy(f"dbo.{table_name}", [(1, value)])
    (_, stored), = _rows(cursor, table_name)
    assert stored == value


# ---------------------------------------------------------------------------
# Column mapping, identifiers, defaults
# ---------------------------------------------------------------------------


def test_bulkcopy_column_mapping_skips_and_reorders(cursor, table_name):
    # [select] is a reserved word and "a]b c" needs bracket escaping.
    _create(
        cursor,
        table_name,
        "[id] INT NOT NULL, [select] INT NULL, [a]]b c] NVARCHAR(20) NULL, "
        "[skipped] NVARCHAR(20) NULL",
    )
    cursor.bulkcopy(
        f"dbo.{table_name}",
        [(7, "drop-me", 42, "name-one")],
        column_mappings=[(0, "id"), (2, "select"), (3, "a]b c")],
    )
    cursor.execute(f"SELECT [id], [select], [a]]b c], [skipped] FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (7, 42, "name-one", None)


def test_bulkcopy_keep_nulls_false_applies_column_default(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, v NVARCHAR(20) NULL DEFAULT N'dflt'")
    cursor.bulkcopy(f"dbo.{table_name}", [(1, None)], keep_nulls=False)
    assert _rows(cursor, table_name) == [(1, "dflt")]


def test_bulkcopy_keep_nulls_true_preserves_null_over_default(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, v NVARCHAR(20) NULL DEFAULT N'dflt'")
    cursor.bulkcopy(f"dbo.{table_name}", [(1, None)], keep_nulls=True)
    assert _rows(cursor, table_name) == [(1, None)]


# ---------------------------------------------------------------------------
# Counts, batches, empty input
# ---------------------------------------------------------------------------


def test_bulkcopy_thousand_rows_in_small_batches(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL PRIMARY KEY, v NVARCHAR(10) NOT NULL")
    rows = [(i, f"r{i}") for i in range(1000)]
    result = cursor.bulkcopy(f"dbo.{table_name}", rows, batch_size=100)
    assert result["rows_copied"] == 1000
    assert result["batch_count"] >= 1
    cursor.execute(f"SELECT COUNT(*), SUM(id) FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (1000, sum(range(1000)))


def test_bulkcopy_empty_iterable_copies_zero_rows(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL")
    result = cursor.bulkcopy(f"dbo.{table_name}", [])
    assert result["rows_copied"] == 0
    assert _rows(cursor, table_name) == []


# ---------------------------------------------------------------------------
# Failures (TestBulkcopyFailure, TestBulkcopyWithInvalidNullableType)
# ---------------------------------------------------------------------------


def test_bulkcopy_missing_table_raises_and_connection_recovers(cursor, table_name):
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}_does_not_exist", [(1,)])
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
    cursor.execute("CREATE TABLE #tmp (id INT)")
    cursor.execute("SELECT * FROM #tmp")
    assert cursor.fetchall() == []


def test_bulkcopy_text_into_int_column_raises(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, n INT NULL")
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, "not-a-number")])
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_bulkcopy_null_into_not_null_column_raises(cursor, table_name):
    _create(cursor, table_name, "id INT NOT NULL, v NVARCHAR(10) NOT NULL")
    with pytest.raises(Exception):
        cursor.bulkcopy(f"dbo.{table_name}", [(1, None)])
    assert _rows(cursor, table_name) == []
