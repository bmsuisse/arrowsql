"""NULL handling across SQL Server types, None parameters, and cursor metadata.

Common driver pain points covered here:

* NULL must come back as ``None`` for every common type (never ``0``, ``""``,
  ``False`` or ``b""``), and an empty string / empty bytes must stay non-NULL.
* ``None`` bound as a parameter must work against typed columns. The classic
  pyodbc bug is ``None`` into a VARBINARY column failing with an implicit
  conversion error because the NULL is bound as a character type.
* ``executemany`` with a column that is ``None`` in every row.
* ``cursor.description`` must report nullability (``null_ok``) correctly.
"""

import datetime
import decimal
import uuid

import pytest
from mssql_python.exceptions import IntegrityError

VARBINARY_TYPES = ["VARBINARY(10)", "VARBINARY(MAX)"]


def _row(cursor):
    """Fetch one row as a plain tuple (upstream Row objects do not == tuples)."""
    row = cursor.fetchone()
    return None if row is None else tuple(row)


def _rows(cursor):
    return [tuple(row) for row in cursor.fetchall()]


def _rows_many(cursor, size):
    return [tuple(row) for row in cursor.fetchmany(size)]


# ---------------------------------------------------------------------------
# NULL round-trip per type
# ---------------------------------------------------------------------------


def test_null_int_round_trips_as_none_not_zero(cursor):
    cursor.execute("CREATE TABLE #t_int (v INT NULL)")
    cursor.execute("INSERT INTO #t_int (v) VALUES (?)", (None,))
    cursor.execute("SELECT v FROM #t_int")
    value = _row(cursor)[0]
    assert value is None
    assert value != 0


def test_null_text_round_trips_as_none_and_empty_string_stays_empty(cursor):
    cursor.execute("CREATE TABLE #t_text (n NVARCHAR(20) NULL, v VARCHAR(20) NULL)")
    cursor.execute("INSERT INTO #t_text (n, v) VALUES (?, ?)", (None, None))
    cursor.execute("INSERT INTO #t_text (n, v) VALUES (?, ?)", ("", ""))
    cursor.execute("SELECT n, v FROM #t_text")
    rows = _rows(cursor)
    assert rows == [(None, None), ("", "")]
    assert rows[0][0] is None and rows[1][0] == ""


def test_null_datetime_and_date_round_trip_as_none(cursor):
    cursor.execute("CREATE TABLE #t_dt (ts DATETIME2 NULL, d DATE NULL, t TIME NULL)")
    cursor.execute("INSERT INTO #t_dt (ts, d, t) VALUES (?, ?, ?)", (None, None, None))
    cursor.execute("SELECT ts, d, t FROM #t_dt")
    assert _row(cursor) == (None, None, None)


def test_null_numeric_types_round_trip_as_none(cursor):
    cursor.execute(
        "CREATE TABLE #t_num (dec_ DECIMAL(10,2) NULL, flt FLOAT NULL, "
        "rl REAL NULL, big BIGINT NULL, small SMALLINT NULL, tiny TINYINT NULL, "
        "money_ MONEY NULL)"
    )
    cursor.execute(
        "INSERT INTO #t_num VALUES (?, ?, ?, ?, ?, ?, ?)",
        (None, None, None, None, None, None, None),
    )
    cursor.execute("SELECT dec_, flt, rl, big, small, tiny, money_ FROM #t_num")
    assert _row(cursor) == (None, None, None, None, None, None, None)


def test_null_bit_is_none_not_false(cursor):
    cursor.execute("CREATE TABLE #t_bit (v BIT NULL)")
    cursor.execute("INSERT INTO #t_bit (v) VALUES (?), (?), (?)", (None, False, True))
    cursor.execute("SELECT v FROM #t_bit")
    values = [row[0] for row in _rows(cursor)]
    assert values == [None, False, True]
    assert values[0] is None


def test_null_uuid_xml_and_datetimeoffset_round_trip_as_none(cursor):
    cursor.execute(
        "CREATE TABLE #t_misc (g UNIQUEIDENTIFIER NULL, x XML NULL, "
        "dto DATETIMEOFFSET NULL)"
    )
    cursor.execute("INSERT INTO #t_misc (g, x, dto) VALUES (?, ?, ?)", (None, None, None))
    cursor.execute("SELECT g, x, dto FROM #t_misc")
    assert _row(cursor) == (None, None, None)


def test_non_null_values_are_not_mistaken_for_null(cursor):
    cursor.execute("CREATE TABLE #t_nonnull (i INT NULL, d DECIMAL(10,2) NULL, g UNIQUEIDENTIFIER NULL)")
    guid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    cursor.execute(
        "INSERT INTO #t_nonnull (i, d, g) VALUES (?, ?, ?)",
        (0, decimal.Decimal("0.00"), guid),
    )
    cursor.execute("SELECT i, d, g FROM #t_nonnull")
    row = _row(cursor)
    assert row == (0, decimal.Decimal("0.00"), guid)
    assert row[0] is not None and row[1] is not None and row[2] is not None


# ---------------------------------------------------------------------------
# None parameters against typed columns (VARBINARY is the classic failure)
# ---------------------------------------------------------------------------


def test_bytes_into_varbinary_round_trips(cursor):
    """Positive control: non-NULL bytes work, including the empty value."""
    cursor.execute("CREATE TABLE #t_vb (v VARBINARY(10) NULL)")
    cursor.execute("INSERT INTO #t_vb (v) VALUES (?), (?)", (b"\x00\x01", b""))
    cursor.execute("SELECT v FROM #t_vb")
    assert [row[0] for row in _rows(cursor)] == [b"\x00\x01", b""]


def test_empty_bytes_is_not_null(cursor):
    cursor.execute("CREATE TABLE #t_vb_empty (v VARBINARY(10) NULL)")
    cursor.execute("INSERT INTO #t_vb_empty (v) VALUES (?)", (b"",))
    cursor.execute("SELECT v, CASE WHEN v IS NULL THEN 1 ELSE 0 END FROM #t_vb_empty")
    value, is_null = _row(cursor)
    assert value == b""
    assert is_null == 0


def test_sql_null_literal_into_varbinary_works(cursor):
    """Contrast case: a SQL NULL literal (not a bound parameter) always works."""
    cursor.execute("CREATE TABLE #t_vb_lit (v VARBINARY(10) NULL)")
    cursor.execute("INSERT INTO #t_vb_lit (v) VALUES (NULL)")
    cursor.execute("SELECT v FROM #t_vb_lit")
    assert _row(cursor)[0] is None


# None into VARBINARY needs the server to describe the parameter, which it can
# only do for permanent tables. For #temp tables upstream pins a ProgrammingError
# (tests/upstream/test_004_cursor.py test_gh627_temp_table_null_varbinary_raises,
# test_gh627_executemany_temp_table_raises) and the port matches it, so these use
# permanent tables.
@pytest.mark.parametrize("varbinary_type", VARBINARY_TYPES)
def test_none_param_into_varbinary_column_inserts_null(cursor, table_name, varbinary_type):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (v {varbinary_type} NULL)")
    cursor.execute(f"INSERT INTO dbo.{table_name} (v) VALUES (?)", (None,))
    cursor.execute(f"SELECT v FROM dbo.{table_name}")
    assert _row(cursor)[0] is None


def test_none_param_into_mixed_row_insert_keeps_other_columns(cursor):
    """None for a nullable column next to typed non-NULL values in the same row."""
    cursor.execute(
        "CREATE TABLE #t_mixed (id INT NOT NULL, name NVARCHAR(20) NULL, "
        "amount DECIMAL(10,2) NULL, created DATETIME2 NULL)"
    )
    created = datetime.datetime(2024, 3, 15, 10, 30, 0)
    cursor.execute(
        "INSERT INTO #t_mixed (id, name, amount, created) VALUES (?, ?, ?, ?)",
        (7, None, decimal.Decimal("12.50"), created),
    )
    cursor.execute("SELECT id, name, amount, created FROM #t_mixed")
    assert _row(cursor) == (7, None, decimal.Decimal("12.50"), created)


def test_none_param_in_where_equals_matches_nothing_but_is_null_matches(cursor):
    """SQL three-valued logic: ``col = NULL`` is never true, ``IS NULL`` is."""
    cursor.execute("CREATE TABLE #t_where (id INT NOT NULL, v NVARCHAR(10) NULL)")
    cursor.execute("INSERT INTO #t_where (id, v) VALUES (1, NULL), (2, 'x')")

    cursor.execute("SELECT id FROM #t_where WHERE v = ?", (None,))
    assert _rows(cursor) == []

    cursor.execute("SELECT id FROM #t_where WHERE v IS NULL")
    assert _rows(cursor) == [(1,)]


# ---------------------------------------------------------------------------
# executemany with all-None columns
# ---------------------------------------------------------------------------


def test_executemany_all_none_column_inserts_nulls(cursor):
    cursor.execute("CREATE TABLE #t_em (id INT NOT NULL, note NVARCHAR(20) NULL, qty INT NULL)")
    rows = [(1, None, None), (2, None, None), (3, None, None)]
    cursor.executemany("INSERT INTO #t_em (id, note, qty) VALUES (?, ?, ?)", rows)
    assert cursor.rowcount == 3

    cursor.execute("SELECT id, note, qty FROM #t_em ORDER BY id")
    assert _rows(cursor) == [(1, None, None), (2, None, None), (3, None, None)]


def test_executemany_all_none_varbinary_column_inserts_nulls(cursor, table_name):
    # Permanent table: see the comment above test_none_param_into_varbinary_column_inserts_null.
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL, payload VARBINARY(10) NULL)")
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (id, payload) VALUES (?, ?)",
        [(1, None), (2, None)],
    )
    cursor.execute(f"SELECT id, payload FROM dbo.{table_name} ORDER BY id")
    assert _rows(cursor) == [(1, None), (2, None)]


def test_executemany_mixed_none_and_bytes_into_varbinary(cursor):
    """Works upstream when at least one row supplies real bytes for the column."""
    cursor.execute("CREATE TABLE #t_em_vb_mix (id INT NOT NULL, payload VARBINARY(10) NULL)")
    cursor.executemany(
        "INSERT INTO #t_em_vb_mix (id, payload) VALUES (?, ?)",
        [(1, None), (2, b"ab")],
    )
    cursor.execute("SELECT id, payload FROM #t_em_vb_mix ORDER BY id")
    assert _rows(cursor) == [(1, None), (2, b"ab")]


# ---------------------------------------------------------------------------
# Fetch paths and aggregates
# ---------------------------------------------------------------------------


def test_null_is_none_across_fetchone_fetchmany_and_fetchall(cursor):
    cursor.execute("CREATE TABLE #t_fetch (id INT NOT NULL, v NVARCHAR(10) NULL)")
    cursor.execute("INSERT INTO #t_fetch (id, v) VALUES (1, NULL), (2, NULL), (3, 'z')")

    cursor.execute("SELECT v FROM #t_fetch ORDER BY id")
    first = _row(cursor)
    many = _rows_many(cursor, 1)
    rest = _rows(cursor)
    assert first == (None,)
    assert many == [(None,)]
    assert rest == [("z",)]


def test_aggregates_over_null_values(cursor):
    cursor.execute("CREATE TABLE #t_agg (v INT NULL)")
    cursor.execute("INSERT INTO #t_agg (v) VALUES (NULL), (NULL), (4), (6)")
    cursor.execute("SELECT COUNT(*), COUNT(v), SUM(v), AVG(v), MAX(v) FROM #t_agg")
    assert _row(cursor) == (4, 2, 10, 5, 6)

    cursor.execute("CREATE TABLE #t_agg_null (v INT NULL)")
    cursor.execute("INSERT INTO #t_agg_null (v) VALUES (NULL), (NULL)")
    cursor.execute("SELECT COUNT(v), SUM(v), AVG(v) FROM #t_agg_null")
    assert _row(cursor) == (0, None, None)


# ---------------------------------------------------------------------------
# cursor.description nullability
# ---------------------------------------------------------------------------


def test_description_null_ok_reflects_column_nullability(cursor):
    cursor.execute("CREATE TABLE #t_desc (id INT NOT NULL, name NVARCHAR(20) NULL)")
    cursor.execute("SELECT id, name FROM #t_desc")
    null_ok = {desc[0]: desc[6] for desc in cursor.description}
    assert null_ok == {"id": False, "name": True}


def test_description_null_ok_for_nullable_varbinary_and_expression(cursor):
    cursor.execute("CREATE TABLE #t_desc_vb (payload VARBINARY(10) NULL)")
    cursor.execute("SELECT payload FROM #t_desc_vb")
    assert cursor.description[0][6] is True
    assert cursor.description[0][1] is bytes

    cursor.execute("SELECT CAST(NULL AS INT) AS n")
    assert cursor.description[0][0] == "n"
    assert cursor.description[0][6] is True


def test_not_null_constraint_violation_raises_integrity_error(cursor):
    cursor.execute("CREATE TABLE #t_notnull (id INT NOT NULL)")
    with pytest.raises(IntegrityError):
        cursor.execute("INSERT INTO #t_notnull (id) VALUES (?)", (None,))
