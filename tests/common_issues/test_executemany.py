"""executemany: NULL handling, varying lengths, large batches, rowcount, and partial failure.

Common-issue coverage for the mssql-python driver's executemany path. Each test
asserts exact values, types and exception classes, and the expectations were
checked against the upstream ODBC driver.
"""

import datetime
import decimal
import uuid

import pytest
from mssql_python import DataError, IntegrityError, ProgrammingError, connect


def _rows(cursor, sql):
    cursor.execute(sql)
    return [tuple(r) for r in cursor.fetchall()]


# --- NULL handling ----------------------------------------------------------


def test_mixed_none_per_column_round_trips_exact_values(cursor):
    cursor.execute(
        "CREATE TABLE #mixed (s NVARCHAR(50) NULL, i INT NULL, f FLOAT NULL, "
        "d DECIMAL(10,2) NULL, dt DATETIME2 NULL, b VARBINARY(20) NULL, id INT NOT NULL)"
    )
    stamp = datetime.datetime(2020, 1, 2, 3, 4, 5)
    rows = [
        ("abc", 1, 1.5, decimal.Decimal("1.25"), stamp, b"\x01", 1),
        (None, 2, None, None, None, None, 2),
        ("xyz", None, 2.5, decimal.Decimal("-3.00"), None, b"\x02\x03", 3),
        (None, None, None, None, None, None, 4),
    ]
    cursor.executemany("INSERT INTO #mixed VALUES (?,?,?,?,?,?,?)", rows)
    got = _rows(cursor, "SELECT s,i,f,d,dt,b,id FROM #mixed ORDER BY id")
    assert got == rows
    # NULL must come back as None, never as an empty string or zero.
    assert got[1][0] is None and got[1][2] is None and got[1][3] is None
    assert isinstance(got[0][3], decimal.Decimal)
    assert isinstance(got[0][4], datetime.datetime)
    assert isinstance(got[0][5], bytes)


def test_first_row_all_none_then_typed_values_keep_their_types(cursor):
    # The driver must not lock a column's type from a NULL first row.
    cursor.execute(
        "CREATE TABLE #typed (s NVARCHAR(200) NULL, i INT NULL, dt DATETIME2 NULL, "
        "f FLOAT NULL, id INT NOT NULL)"
    )
    stamp = datetime.datetime(2024, 6, 30, 23, 59, 59)
    rows = [
        (None, None, None, None, 1),
        (None, None, None, None, 2),
        ("late value " + "q" * 150, 42, stamp, 0.125, 3),
    ]
    cursor.executemany("INSERT INTO #typed VALUES (?,?,?,?,?)", rows)
    got = _rows(cursor, "SELECT s,i,dt,f,id FROM #typed ORDER BY id")
    assert got == rows
    assert got[2][0] == "late value " + "q" * 150
    assert isinstance(got[2][1], int) and got[2][1] == 42
    assert isinstance(got[2][2], datetime.datetime)


def test_empty_string_is_not_stored_as_null(cursor):
    cursor.execute("CREATE TABLE #empty (s NVARCHAR(10) NULL, id INT NOT NULL)")
    cursor.executemany(
        "INSERT INTO #empty VALUES (?,?)", [("", 1), (None, 2), ("x", 3)]
    )
    got = _rows(cursor, "SELECT s, id FROM #empty ORDER BY id")
    assert got == [("", 1), (None, 2), ("x", 3)]


# --- Varying string lengths (truncation regression) -------------------------


def test_varying_lengths_short_first_row_no_truncation(cursor):
    # A short first row must not size the buffer for the whole batch.
    cursor.execute("CREATE TABLE #len (s NVARCHAR(200) NULL, id INT NOT NULL)")
    values = ["a", "b" * 150, "", "c" * 50, None, "d" * 199, "e"]
    cursor.executemany(
        "INSERT INTO #len VALUES (?,?)", [(v, i) for i, v in enumerate(values)]
    )
    got = _rows(cursor, "SELECT s, id FROM #len ORDER BY id")
    assert [r[0] for r in got] == values
    assert [len(r[0]) if r[0] is not None else None for r in got] == [
        len(v) if v is not None else None for v in values
    ]


def test_none_first_then_long_string_is_not_truncated(cursor):
    cursor.execute("CREATE TABLE #nonefirst (s NVARCHAR(200) NULL, id INT NOT NULL)")
    long_value = "L" * 180
    cursor.executemany(
        "INSERT INTO #nonefirst VALUES (?,?)", [(None, 1), (long_value, 2), ("s", 3)]
    )
    got = _rows(cursor, "SELECT s, id FROM #nonefirst ORDER BY id")
    assert got[0][0] is None
    assert got[1][0] == long_value
    assert len(got[1][0]) == 180
    assert got[2][0] == "s"


def test_nvarchar_max_long_strings_round_trip(cursor):
    cursor.execute("CREATE TABLE #max (s NVARCHAR(MAX) NULL, id INT NOT NULL)")
    values = ["x" * 10000, "y" * 4001, "", None, "z" * 8000]
    cursor.executemany(
        "INSERT INTO #max VALUES (?,?)", [(v, i) for i, v in enumerate(values)]
    )
    got = _rows(cursor, "SELECT s, id FROM #max ORDER BY id")
    assert [r[0] for r in got] == values


def test_unicode_including_surrogate_pairs_round_trips(cursor):
    # NVARCHAR(n) counts UTF-16 units, so each emoji takes 2 units.
    cursor.execute("CREATE TABLE #uni (s NVARCHAR(20) NULL, id INT NOT NULL)")
    values = ["é", "😀😀", None, "a😀b"]
    cursor.executemany(
        "INSERT INTO #uni VALUES (?,?)", [(v, i) for i, v in enumerate(values)]
    )
    got = _rows(cursor, "SELECT s, id FROM #uni ORDER BY id")
    assert [r[0] for r in got] == values


@pytest.mark.upstream_bug(
    reason=(
        "A batch mixing a CJK string and an emoji raises RuntimeError ('Input string "
        "exceeds allowed column size') even though every value fits NVARCHAR(20) and "
        "each value inserts fine on its own. The client sizing is wrong for mixed "
        "multi-byte batches, and the error is not a DB-API exception."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_mixed_cjk_and_emoji_batch_fits_column(cursor):
    cursor.execute("CREATE TABLE #cjk (s NVARCHAR(20) NULL)")
    values = ["日本語", "😀😀"]
    cursor.executemany("INSERT INTO #cjk VALUES (?)", [(v,) for v in values])
    assert sorted(_rows(cursor, "SELECT s FROM #cjk")) == sorted((v,) for v in values)


# --- Large batches ----------------------------------------------------------


def test_10k_rows_exact_count_and_sum(cursor):
    cursor.execute("CREATE TABLE #big (id INT NOT NULL)")
    cursor.executemany("INSERT INTO #big VALUES (?)", [(i,) for i in range(10_000)])
    got = _rows(cursor, "SELECT COUNT(*), SUM(CAST(id AS BIGINT)) FROM #big")
    assert tuple(got[0]) == (10_000, sum(range(10_000)))


def test_10k_rows_mixed_none_and_varying_length(cursor):
    cursor.execute("CREATE TABLE #bigmix (id INT NOT NULL, s NVARCHAR(100) NULL)")

    def value_for(i):
        # Every third row is NULL; the rest vary between 0 and 96 characters.
        return None if i % 3 == 0 else "v" * (i % 97)

    rows = [(i, value_for(i)) for i in range(10_000)]
    cursor.executemany("INSERT INTO #bigmix VALUES (?,?)", rows)

    got = _rows(
        cursor,
        "SELECT COUNT(*), COUNT(s), SUM(CAST(LEN(s) AS BIGINT)) FROM #bigmix",
    )
    expected_nulls = sum(1 for i in range(10_000) if i % 3 == 0)
    expected_total_len = sum(len(value_for(i)) for i in range(10_000) if value_for(i) is not None)
    assert tuple(got[0]) == (10_000, 10_000 - expected_nulls, expected_total_len)

    # Spot-check boundary rows: empty string at i=97 (not NULL), NULL at i=0.
    spot = dict(_rows(cursor, "SELECT id, s FROM #bigmix WHERE id IN (0, 1, 97, 98, 9999)"))
    assert spot[0] is None
    assert spot[1] == "v"
    assert spot[97] == ""
    assert spot[98] == "v"  # 98 % 97 == 1
    assert spot[9999] is None  # 9999 % 3 == 0


# --- Empty input and rowcount ------------------------------------------------


def test_empty_list_inserts_nothing_and_rowcount_is_zero(cursor):
    cursor.execute("CREATE TABLE #none (id INT NOT NULL)")
    cursor.executemany("INSERT INTO #none VALUES (?)", [])
    assert cursor.rowcount == 0
    assert _rows(cursor, "SELECT COUNT(*) FROM #none") == [(0,)]


def test_rowcount_after_insert_equals_number_of_rows(cursor):
    cursor.execute("CREATE TABLE #rc (id INT NOT NULL)")
    cursor.executemany("INSERT INTO #rc VALUES (?)", [(i,) for i in range(37)])
    assert cursor.rowcount == 37


def test_rowcount_after_update_sums_matched_rows_per_parameter_set(cursor):
    cursor.execute("CREATE TABLE #upd (id INT NOT NULL, v INT NOT NULL)")
    cursor.executemany("INSERT INTO #upd VALUES (?,?)", [(i, 0) for i in range(5)])
    # Parameter set 3 matches no row, so the total is 2, not 3.
    cursor.executemany(
        "UPDATE #upd SET v = ? WHERE id = ?", [(10, 0), (11, 1), (12, 999)]
    )
    assert cursor.rowcount == 2
    got = _rows(cursor, "SELECT id, v FROM #upd ORDER BY id")
    assert got == [(0, 10), (1, 11), (2, 0), (3, 0), (4, 0)]


def test_rowcount_after_delete_sums_deleted_rows(cursor):
    cursor.execute("CREATE TABLE #del (id INT NOT NULL)")
    cursor.executemany("INSERT INTO #del VALUES (?)", [(i,) for i in range(10)])
    cursor.executemany("DELETE FROM #del WHERE id = ?", [(1,), (2,), (3,), (500,)])
    assert cursor.rowcount == 3
    assert _rows(cursor, "SELECT COUNT(*) FROM #del") == [(7,)]


# --- Errors mid-batch --------------------------------------------------------


def test_not_null_violation_raises_integrity_error(cursor):
    cursor.execute("CREATE TABLE #nn (k INT NOT NULL, s NVARCHAR(5) NULL)")
    with pytest.raises(IntegrityError):
        cursor.executemany(
            "INSERT INTO #nn VALUES (?,?)", [(1, "a"), (2, "b"), (None, "c")]
        )


def test_error_mid_batch_later_rows_are_still_applied_upstream(cursor):
    # Characterization, not a spec requirement. Upstream applies rows around the
    # failing one and raises after the batch, so row 4 is still inserted. The
    # Rust port must reproduce which rows land.
    cursor.execute("CREATE TABLE #partial (k INT NOT NULL, s NVARCHAR(5) NULL)")
    with pytest.raises(IntegrityError):
        cursor.executemany(
            "INSERT INTO #partial VALUES (?,?)",
            [(1, "a"), (2, "b"), (None, "c"), (4, "d")],
        )
    got = _rows(cursor, "SELECT k FROM #partial ORDER BY k")
    assert got == [(1,), (2,), (4,)]


def test_truncation_mid_batch_keeps_rows_around_the_failure(cursor):
    cursor.execute("CREATE TABLE #tr (s NVARCHAR(5) NULL)")
    # The error class is unstable upstream (see the strict xfail below), so accept both.
    with pytest.raises((DataError, ProgrammingError)):
        cursor.executemany("INSERT INTO #tr VALUES (?)", [("a",), ("toolong!",), ("b",)])
    got = _rows(cursor, "SELECT s FROM #tr ORDER BY s")
    assert got == [("a",), ("b",)]


@pytest.mark.upstream_bug(
    reason=(
        "String truncation (SQLSTATE 22001) raises ProgrammingError upstream. PEP 249 "
        "puts data-related errors under DataError, and numeric overflow in the same "
        "driver already raises DataError."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_truncation_raises_data_error(cursor):
    # Unique temp-table name: upstream's error class changes for a name it has already
    # executed in this process, so a fixed name would make the strict xfail flaky.
    table = f"#trc_{uuid.uuid4().hex[:8]}"
    cursor.execute(f"CREATE TABLE {table} (s NVARCHAR(5) NULL)")
    with pytest.raises(DataError):
        cursor.executemany(f"INSERT INTO {table} VALUES (?)", [("a",), ("toolong!",)])


def test_numeric_overflow_in_batch_raises_data_error(cursor):
    cursor.execute("CREATE TABLE #ovf (i INT NULL)")
    with pytest.raises(DataError):
        cursor.executemany("INSERT INTO #ovf VALUES (?)", [(1,), (99_999_999_999,)])


def test_rollback_after_failed_batch_leaves_no_rows(conn, cursor):
    cursor.execute("CREATE TABLE #rb (k INT NOT NULL, s NVARCHAR(5) NULL)")
    conn.commit()
    with pytest.raises(IntegrityError):
        cursor.executemany("INSERT INTO #rb VALUES (?,?)", [(1, "a"), (None, "b")])
    conn.rollback()
    assert _rows(cursor, "SELECT COUNT(*) FROM #rb") == [(0,)]


def test_param_count_mismatch_with_sql_raises_programming_error_and_inserts_nothing(cursor):
    cursor.execute("CREATE TABLE #pc (id INT NOT NULL)")
    with pytest.raises(ProgrammingError):
        cursor.executemany("INSERT INTO #pc VALUES (?,?)", [(1, 2), (3, 4)])
    assert _rows(cursor, "SELECT COUNT(*) FROM #pc") == [(0,)]


def test_ragged_parameter_rows_rejected_before_any_row_is_inserted(cursor):
    # Rows with different parameter counts fail before anything is sent.
    # Upstream raises IndexError or ValueError depending on which row is ragged.
    cursor.execute("CREATE TABLE #rag (id INT NOT NULL)")
    with pytest.raises((IndexError, ValueError)):
        cursor.executemany("INSERT INTO #rag VALUES (?)", [(1,), (2, 3)])
    assert _rows(cursor, "SELECT COUNT(*) FROM #rag") == [(0,)]


def test_tuple_and_list_rows_are_both_accepted(cursor):
    cursor.execute("CREATE TABLE #seq (id INT NOT NULL, s NVARCHAR(5) NULL)")
    cursor.executemany("INSERT INTO #seq VALUES (?,?)", [(1, "a"), [2, None]])
    got = _rows(cursor, "SELECT id, s FROM #seq ORDER BY id")
    assert got == [(1, "a"), (2, None)]


# --- Permanent table and commit visibility -----------------------------------


def test_permanent_table_committed_batch_visible_from_second_connection(
    conn, conn_str, cursor, table_name
):
    cursor.execute(
        f"CREATE TABLE dbo.{table_name} (id INT NOT NULL, s NVARCHAR(120) NULL)"
    )
    conn.commit()
    values = [(1, "short"), (2, None), (3, "w" * 110)]
    cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?,?)", values)
    assert cursor.rowcount == 3
    conn.commit()

    other = connect(conn_str)
    try:
        other_cur = other.cursor()
        other_cur.execute(f"SELECT id, s FROM dbo.{table_name} ORDER BY id")
        assert [tuple(r) for r in other_cur.fetchall()] == values
    finally:
        other.close()


def test_permanent_table_uncommitted_batch_invisible_to_second_connection(
    conn, conn_str, cursor, table_name
):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT NOT NULL)")
    conn.commit()
    cursor.executemany(f"INSERT INTO dbo.{table_name} VALUES (?)", [(i,) for i in range(5)])
    conn.rollback()

    other = connect(conn_str)
    try:
        other_cur = other.cursor()
        other_cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        assert [tuple(r) for r in other_cur.fetchall()] == [(0,)]
    finally:
        other.close()
