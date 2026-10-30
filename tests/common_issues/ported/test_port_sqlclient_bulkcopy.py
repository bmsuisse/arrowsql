"""Bulk copy edge cases ported from dotnet/SqlClient manual tests (BulkCopy/).

Adapted from dotnet/SqlClient src/Microsoft.Data.SqlClient/tests/ManualTests/BulkCopy/
(KeepNulls.cs, CheckConstraints.cs, SpecialCharacterNames.cs,
DestinationTableNameWithSpecialChar.cs, CopyWidenNullInexactNumerics.cs,
MissingTargetColumn.cs, MissingTargetTable.cs, TableLock.cs, HiddenTargetColumn.cs,
CopyVariants.cs, ColumnCollation.cs), (MIT), Copyright (c) .NET Foundation. All rights reserved.
Also Copyright (c) Microsoft Corporation. The MIT license text is in
LICENSE_dotnet_sqlclient.txt next to this file.

Translated, not transliterated: SqlBulkCopy options map to cursor.bulkcopy keyword
arguments, ColumnMappings map to column_mappings, and SqlException numbers are
checked through the message text where the upstream exception classes differ.
Where the .NET source expects a specific SqlException class that upstream does not
raise for a bulk path, the test asserts the DB-API class and is marked upstream_bug.
"""

import uuid
from decimal import Decimal

import pytest
from mssql_python import IntegrityError, ProgrammingError


def _q(name):
    """Bracket-quote an identifier, doubling any ']' (as SpecialCharacterNames.cs does)."""
    return "[" + name.replace("]", "]]") + "]"


def _rows(cursor, qualified, order_by="1"):
    cursor.execute(f"SELECT * FROM {qualified} ORDER BY {order_by}")
    return [tuple(r) for r in cursor.fetchall()]


@pytest.fixture
def make_table(conn):
    """Create permanent dbo tables; drop them (and nothing else) on teardown."""
    cur = conn.cursor()
    made = []

    def make(ddl):
        name = f"ci_{uuid.uuid4().hex[:12]}"
        cur.execute(f"CREATE TABLE dbo.{name} ({ddl})")
        conn.commit()
        made.append(name)
        return f"dbo.{name}"

    yield make
    for name in reversed(made):
        cur.execute(f"DROP TABLE IF EXISTS dbo.{name}")
    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# KeepNulls.cs: NULL in the source is kept as NULL instead of the column default
# ---------------------------------------------------------------------------

_KEEP_NULLS_SOURCE_ROWS = [(1, None, "Michael"), (2, "Quark", "Astrid"), (66, "Käse", None)]


def _seed_keep_nulls(cursor, conn, make_table):
    src = make_table("col1 INT NOT NULL, col2 NVARCHAR(50) NULL, col3 NVARCHAR(50) NULL")
    dst = make_table("col1 INT NOT NULL, col2 NVARCHAR(50) NULL DEFAULT N'Jogurt', col3 NVARCHAR(50) NULL")
    for row in _KEEP_NULLS_SOURCE_ROWS:
        cursor.execute(f"INSERT INTO {src} (col1, col2, col3) VALUES (?, ?, ?)", list(row))
    conn.commit()
    return src, dst, _rows(cursor, src)


def test_keep_nulls_preserves_null_over_column_default(cursor, conn, make_table):
    _, dst, source_rows = _seed_keep_nulls(cursor, conn, make_table)
    cursor.bulkcopy(dst, source_rows, column_mappings=["col1", "col2", "col3"], keep_nulls=True)
    assert _rows(cursor, dst) == _KEEP_NULLS_SOURCE_ROWS


def test_without_keep_nulls_null_is_replaced_by_column_default(cursor, conn, make_table):
    _, dst, source_rows = _seed_keep_nulls(cursor, conn, make_table)
    cursor.bulkcopy(dst, source_rows, column_mappings=["col1", "col2", "col3"])
    got = _rows(cursor, dst)
    assert got[0] == (1, "Jogurt", "Michael")
    assert got[1:] == _KEEP_NULLS_SOURCE_ROWS[1:]


# ---------------------------------------------------------------------------
# CheckConstraints.cs: CHECK constraints are enforced only with check_constraints=True
# ---------------------------------------------------------------------------

_CHECK_SOURCE_ROWS = [(33, 498, "Michael"), (34, 499, "Astrid"), (65, 500, "alles Käse")]


def _check_table(make_table):
    return make_table(
        "col1 INT NOT NULL PRIMARY KEY, col2 INT NULL CHECK (col2 < 500), col3 NVARCHAR(50) NULL"
    )


def test_check_constraints_option_rejects_violating_row(cursor, conn, make_table):
    dst = _check_table(make_table)
    # Error 547: the INSERT statement conflicted with the CHECK constraint.
    with pytest.raises(Exception, match="547"):
        cursor.bulkcopy(dst, _CHECK_SOURCE_ROWS, check_constraints=True)
    assert _rows(cursor, dst) == []


def test_without_check_constraints_option_violating_row_is_loaded(cursor, conn, make_table):
    dst = _check_table(make_table)
    result = cursor.bulkcopy(dst, _CHECK_SOURCE_ROWS)
    assert result["rows_copied"] == len(_CHECK_SOURCE_ROWS)
    assert _rows(cursor, dst) == _CHECK_SOURCE_ROWS


@pytest.mark.upstream_bug(
    reason="bulkcopy surfaces SQL errors as RuntimeError; a CHECK violation (547) is an IntegrityError (SQLSTATE 23000) as on the INSERT path"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_check_violation_in_bulkcopy_raises_integrity_error(cursor, conn, make_table):
    dst = _check_table(make_table)
    with pytest.raises(IntegrityError):
        cursor.bulkcopy(dst, _CHECK_SOURCE_ROWS, check_constraints=True)


@pytest.mark.upstream_bug(
    reason="bulkcopy surfaces SQL errors as RuntimeError; a PRIMARY KEY violation (2627) is an IntegrityError on the INSERT path"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_duplicate_key_in_bulkcopy_raises_integrity_error(cursor, conn, make_table):
    dst = _check_table(make_table)
    with pytest.raises(IntegrityError):
        cursor.bulkcopy(dst, [(1, 1, "a"), (1, 2, "b")])


# ---------------------------------------------------------------------------
# DestinationTableNameWithSpecialChar.cs: names that need brackets, including a
# leading '@', are accepted in every qualified/unqualified bracket combination.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "form",
    ["@{t}", "[@{t}]", "dbo.@{t}", "[dbo].[@{t}]"],
    ids=["bare", "bracketed", "schema-bare", "schema-bracketed"],
)
def test_destination_name_forms_with_leading_at_sign(cursor, conn, form):
    t = f"ci_at_{uuid.uuid4().hex[:10]}"
    cursor.execute(f"CREATE TABLE [dbo].[@{t}] ([col1] INT NOT NULL)")
    conn.commit()
    try:
        destination = form.format(t=t)
        cursor.bulkcopy(destination, [(1,), (2,)])
        cursor.execute(f"SELECT COUNT(*) FROM [dbo].[@{t}]")
        assert cursor.fetchone()[0] == 2
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS [dbo].[@{t}]")
        conn.commit()


# ---------------------------------------------------------------------------
# SpecialCharacterNames.cs: schema and table names with quotes and ']' characters.
# ---------------------------------------------------------------------------


def test_schema_and_table_with_quote_and_bracket_characters(cursor, conn):
    token = uuid.uuid4().hex[:8]
    schema = f"ci_{token}_Schema'-][''"
    table = f"ci_{token}_Table'-][''"
    qualified = f"{_q(schema)}.{_q(table)}"
    cursor.execute(f"CREATE SCHEMA {_q(schema)}")
    cursor.execute(f"CREATE TABLE {qualified} (orderid INT NOT NULL, customerid NCHAR(5) NOT NULL)")
    conn.commit()
    try:
        cursor.bulkcopy(qualified, [(1, "AAAAA"), (2, "BB")])
        # nchar(5) pads on the way in; the copy must not truncate or reject the short value.
        assert _rows(cursor, qualified, "orderid") == [(1, "AAAAA"), (2, "BB   ")]
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {qualified}")
        cursor.execute(f"DROP SCHEMA IF EXISTS {_q(schema)}")
        conn.commit()


# ---------------------------------------------------------------------------
# CopyWidenNullInexactNumerics.cs: float source values into decimal(18,10) columns.
# ---------------------------------------------------------------------------


def test_float_source_widens_into_decimal_columns_with_nulls(cursor, conn, make_table):
    dst = make_table("floatVal DECIMAL(18,10) NULL, realVal DECIMAL(18,10) NULL")
    cursor.bulkcopy(
        dst,
        [(1.0, 1.0), (2.0, 2.0), (None, None), (0.00000000000001, 0.00000000000001)],
    )
    # ORDER BY puts NULL first; 1e-14 rounds to zero at scale 10.
    assert _rows(cursor, dst, "floatVal") == [
        (None, None),
        (Decimal("0"), Decimal("0")),
        (Decimal("1"), Decimal("1")),
        (Decimal("2"), Decimal("2")),
    ]


# ---------------------------------------------------------------------------
# TableLock.cs: the TABLE LOCK hint does not change the loaded data.
# ---------------------------------------------------------------------------


def test_table_lock_option_loads_all_rows(cursor, conn, make_table):
    dst = make_table("col1 INT NOT NULL, col2 NVARCHAR(20) NULL DEFAULT N'Jogurt', col3 NVARCHAR(20) NULL")
    rows = [(1, "Quark", "Michael"), (2, None, "Astrid"), (66, "Käse", None)]
    result = cursor.bulkcopy(dst, rows, table_lock=True)
    assert result["rows_copied"] == 3
    # Without KEEP_NULLS the NULL in col2 takes the column default.
    assert _rows(cursor, dst) == [(1, "Quark", "Michael"), (2, "Jogurt", "Astrid"), (66, "Käse", None)]


# ---------------------------------------------------------------------------
# MissingTargetColumn.cs / MissingTargetTable.cs: bad destination metadata.
# ---------------------------------------------------------------------------


def test_unmapped_destination_column_raises_naming_the_column(cursor, conn, make_table):
    dst = make_table("col1 INT NOT NULL, col3 NVARCHAR(10) NULL")
    with pytest.raises(Exception, match="col2"):
        cursor.bulkcopy(
            dst,
            [(1, "Michael", "Quark")],
            column_mappings=[(0, "col1"), (1, "col2"), (2, "col3")],
        )
    assert _rows(cursor, dst) == []


def test_missing_destination_table_error_names_the_object(cursor, conn):
    missing = f"dbo.ci_missing_{uuid.uuid4().hex[:12]}"
    with pytest.raises(Exception, match="Invalid object name"):
        cursor.bulkcopy(missing, [(1,)])


@pytest.mark.upstream_bug(
    reason="bulkcopy surfaces the missing-table SQL error (208) as RuntimeError; the DB-API class is ProgrammingError"
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_missing_destination_table_raises_programming_error(cursor, conn):
    missing = f"dbo.ci_missing_{uuid.uuid4().hex[:12]}"
    with pytest.raises(ProgrammingError):
        cursor.bulkcopy(missing, [(1,)])


# ---------------------------------------------------------------------------
# HiddenTargetColumn.cs: GENERATED ALWAYS columns reject an explicit value (13536).
# ---------------------------------------------------------------------------


def test_explicit_value_into_generated_always_column_is_rejected(cursor, conn):
    t = f"ci_gen_{uuid.uuid4().hex[:10]}"
    hist = f"{t}_hist"
    cursor.execute(
        f"""CREATE TABLE dbo.{t} (
            Column1 INT PRIMARY KEY NOT NULL,
            Column2 NVARCHAR(10) NOT NULL,
            ValidFrom DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
            ValidTo DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo))
        WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.{hist}))"""
    )
    conn.commit()
    try:
        with pytest.raises(Exception, match="GENERATED ALWAYS|13536"):
            cursor.bulkcopy(
                f"dbo.{t}",
                [(1, "a", None, None)],
                column_mappings=[(0, "Column1"), (1, "Column2"), (2, "ValidFrom"), (3, "ValidTo")],
            )
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{t}")
        assert cursor.fetchone()[0] == 0
    finally:
        cursor.execute(f"ALTER TABLE dbo.{t} SET (SYSTEM_VERSIONING = OFF)")
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{t}")
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{hist}")
        conn.commit()


# ---------------------------------------------------------------------------
# CopyVariants.cs: sql_variant source values keep their base type and value.
# ---------------------------------------------------------------------------

_VARIANT_VALUES = [
    "NULL",
    "CONVERT(int, 0)",
    "CONVERT(smallint, -32768)",
    "CONVERT(real, 2.2)",
    "CONVERT(float, -3303.33303)",
    "CONVERT(decimal(28,4), 44404.4404)",
    "CONVERT(money, 555505.5505)",
    "CONVERT(bit, 1)",
    "CONVERT(tinyint, 8)",
    "CONVERT(uniqueidentifier, '00000000-0000-0000-0000-000000000009')",
    "CONVERT(varchar(20), 'abc')",
    "CONVERT(datetime, '2003-01-11 12:54:01.133')",
    "CONVERT(bigint, 444444444444404)",
]


def test_sql_variant_values_copy_with_their_base_types(cursor, conn, make_table):
    src = make_table("col_1 INT PRIMARY KEY, col_2 SQL_VARIANT NULL")
    dst = make_table("col_1 INT PRIMARY KEY, col_2 SQL_VARIANT NULL")
    for i, expr in enumerate(_VARIANT_VALUES):
        cursor.execute(f"INSERT INTO {src} (col_1, col_2) VALUES ({i}, {expr})")
    conn.commit()
    source_rows = _rows(cursor, src)
    assert len(source_rows) == len(_VARIANT_VALUES)

    cursor.bulkcopy(dst, source_rows)

    assert _rows(cursor, dst) == source_rows


# ---------------------------------------------------------------------------
# ColumnCollation.cs: Japanese and Cyrillic collations in varchar columns.
# ---------------------------------------------------------------------------


def test_varchar_columns_with_different_collations_round_trip(cursor, conn, make_table):
    dst = make_table(
        "name_jp VARCHAR(20) COLLATE Japanese_CI_AS NULL, "
        "name_ru VARCHAR(20) COLLATE Cyrillic_General_CI_AS NULL"
    )
    s_jp, s_ru = "江戸糸あやつり人形", "проверка"
    cursor.bulkcopy(dst, [(s_jp, s_ru)], column_mappings=["name_jp", "name_ru"])
    assert _rows(cursor, dst) == [(s_jp, s_ru)]
