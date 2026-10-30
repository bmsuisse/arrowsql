"""cursor.setinputsizes: per-parameter SQL types and sizes, executemany, invalid
sizes, reset after execute, and interplay with None values.

Research sources (issue trackers and release notes consulted):
- https://github.com/mkleehammer/pyodbc/issues/380
  fast_executemany "String data, right truncation" errors; setinputsizes was the
  documented workaround.
- https://github.com/mkleehammer/pyodbc/issues/845
  Negative decimal(m,n) strings under fast_executemany raise right-truncation;
  setinputsizes did not fix it.
- https://github.com/mkleehammer/pyodbc/issues/867
  Long str/bytes values are not bound as (max) types under fast_executemany;
  workaround is setinputsizes with size 0 (varchar(max)/varbinary(max)).
- https://github.com/mkleehammer/pyodbc/issues/480
  Strings longer than the column were silently truncated on insert under
  fast_executemany (the opposite failure mode: data loss without error).
- https://github.com/mkleehammer/pyodbc/issues/940
  Truncation errors under fast_executemany can mask the real error message.
- https://github.com/mkleehammer/pyodbc/issues/835 (via search summary)
  Strings over 2000 chars bound as LONGVARCHAR/text; the fix is
  setinputsizes([(SQL_WVARCHAR, 0, 0)]) to send nvarchar(max).
- https://github.com/microsoft/mssql-python/issues/745
  Money-range Decimal binding still takes a VARCHAR shortcut on the
  executemany path, so sizing and type choice can differ between execute and
  executemany.
- https://github.com/microsoft/mssql-python/issues/816
  UDT parameters: setinputsizes accepts SQL_SS_UDT but cannot carry the type
  name, so the type is not fully expressible through the tuple.
- https://github.com/microsoft/mssql-python/releases/tag/v1.15.0
  setinputsizes() execution routed through the native pipeline (PR #736), so
  behavior on the native path must match the legacy Python path.
- https://github.com/microsoft/mssql-python/wiki/Known-问题-and-Unsupported-Features
  Known limitations list (consulted; setinputsizes-related entries noted above).

Expected values are what the upstream ODBC driver returns today. Where the
upstream driver is genuinely wrong, the test asserts the correct behavior and
is marked upstream_bug + xfail(strict=True).
"""

import datetime
import decimal
import uuid

import mssql_python as m
import pytest
from mssql_python import DataError, ProgrammingError

W = m.SQL_WVARCHAR
VB = m.SQL_VARBINARY
I = m.SQL_INTEGER
BIG = m.SQL_BIGINT
DEC = m.SQL_DECIMAL
BIT = m.SQL_BIT


def _rows(cursor, sql, params=None):
    cursor.execute(sql, params) if params is not None else cursor.execute(sql)
    return [tuple(r) for r in cursor.fetchall()]


# --- sizing and type hints for single execute ------------------------------


def test_setinputsizes_returns_none_and_empty_list_is_accepted(cursor):
    assert cursor.setinputsizes([]) is None
    assert _rows(cursor, "SELECT ?", ("ok",)) == [("ok",)]


def test_sized_nvarchar_round_trips_unicode_exactly(cursor):
    value = "héllo ✓ 日本"
    cursor.setinputsizes([(W, 100, 0)])
    got = _rows(cursor, "SELECT ?", (value,))
    assert got == [(value,)]
    assert isinstance(got[0][0], str)


def test_nvarchar_max_size_zero_sends_long_string_intact(cursor):
    # size 0 means (max): a 5000-char value must not be cut to a default size.
    value = "y" * 5000
    cursor.setinputsizes([(W, 0, 0)])
    assert _rows(cursor, "SELECT LEN(?)", (value,)) == [(5000,)]


def test_varbinary_max_size_zero_preserves_all_bytes(cursor):
    payload = bytes(range(256)) * 36  # 9216 bytes
    cursor.setinputsizes([(VB, 0, 0)])
    got = _rows(cursor, "SELECT DATALENGTH(?)", (payload,))
    assert got == [(len(payload),)]
    cursor.setinputsizes([(VB, 0, 0)])
    got = _rows(cursor, "SELECT CAST(? AS VARBINARY(MAX))", (payload,))
    assert got == [(payload,)]
    assert isinstance(got[0][0], bytes)


def test_bigint_size_handles_value_above_int32(cursor):
    cursor.setinputsizes([(BIG, 0, 0)])
    got = _rows(cursor, "SELECT ?", (2**40,))
    assert got == [(2**40,)]
    assert isinstance(got[0][0], int)


def test_bit_size_round_trips_bool(cursor):
    cursor.setinputsizes([(BIT, 0, 0)])
    got = _rows(cursor, "SELECT ?", (True,))
    assert got == [(True,)]
    assert isinstance(got[0][0], bool)


def test_none_under_integer_size_becomes_sql_null(cursor):
    cursor.setinputsizes([(I, 0, 0)])
    got = _rows(cursor, "SELECT ?", (None,))
    assert got == [(None,)]


@pytest.mark.upstream_bug(
    reason=(
        "None entries in the sizes list mean 'no hint for this position' in pyodbc "
        "(pyodbc issue #867 uses setinputsizes([None, None, (SQL_VARBINARY, 0)])), "
        "but upstream raises ValueError('Invalid SQL type: None')"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_none_entry_in_sizes_list_leaves_that_parameter_untyped(cursor):
    # None in the sizes list means "no hint" for that position only.
    cursor.setinputsizes([None, (I, 0, 0)])
    got = _rows(cursor, "SELECT ?, ?", ("text", 7))
    assert got == [("text", 7)]
    assert isinstance(got[0][0], str) and isinstance(got[0][1], int)


def test_decimal_size_with_fitting_value_round_trips(cursor):
    cursor.setinputsizes([(DEC, 5, 2)])
    got = _rows(cursor, "SELECT ?", (decimal.Decimal("12.34"),))
    assert got == [(decimal.Decimal("12.34"),)]
    assert isinstance(got[0][0], decimal.Decimal)


# --- undersized / oversized hints and error classes ------------------------


def test_undersized_size_hint_raises_dataerror_on_execute(cursor):
    cursor.execute("CREATE TABLE #small (v NVARCHAR(10) NULL)")
    cursor.setinputsizes([(W, 5, 0)])
    with pytest.raises(DataError):
        cursor.execute("INSERT INTO #small (v) VALUES (?)", ("a" * 50,))
    assert _rows(cursor, "SELECT COUNT(*) FROM #small") == [(0,)]


def test_string_too_long_for_column_without_hint_raises_programmingerror(cursor):
    cursor.execute("CREATE TABLE #small2 (v NVARCHAR(10) NULL)")
    cursor.setinputsizes([])
    with pytest.raises(ProgrammingError):
        cursor.execute("INSERT INTO #small2 (v) VALUES (?)", ("a" * 50,))


def test_wrong_tuple_arity_raises_valueerror(cursor):
    with pytest.raises(ValueError):
        cursor.setinputsizes([(W, 10, 0, 9)])


def test_invalid_sql_type_constant_raises_valueerror(cursor):
    with pytest.raises(ValueError):
        cursor.setinputsizes([(999999, 10, 0)])


# --- reset semantics -------------------------------------------------------


def test_sizes_apply_only_to_next_execute(cursor):
    # Sizes are consumed by the next execute, even one with no parameters.
    # A later string parameter must then be typed by value, not by the old
    # SQL_INTEGER hint (which fails with a RuntimeError while sizes are set).
    cursor.setinputsizes([(I, 0, 0)])
    assert _rows(cursor, "SELECT 1") == [(1,)]
    assert _rows(cursor, "SELECT ?", ("abc",)) == [("abc",)]


def test_setinputsizes_with_mismatched_count_warns_and_still_executes(cursor):
    cursor.setinputsizes([(W, 10, 0), (I, 0, 0), (W, 10, 0)])
    with pytest.warns(Warning, match=r"Number of input sizes \(3\) does not match"):
        got = _rows(cursor, "SELECT ?", ("x",))
    assert got == [("x",)]


def test_sizes_for_single_execute_do_not_leak_into_executemany(cursor):
    cursor.execute("CREATE TABLE #leak (v NVARCHAR(10) NULL, i INT NULL)")
    cursor.setinputsizes([(W, 10, 0), (I, 0, 0)])
    cursor.execute("INSERT INTO #leak (v, i) VALUES (?, ?)", ("a", 1))
    cursor.executemany("INSERT INTO #leak (v, i) VALUES (?, ?)", [("b", 2), ("c", 3)])
    got = _rows(cursor, "SELECT v, i FROM #leak ORDER BY i")
    assert got == [("a", 1), ("b", 2), ("c", 3)]


# --- executemany ------------------------------------------------------------


def test_executemany_sized_with_none_rows_round_trips(cursor):
    cursor.execute("CREATE TABLE #em (v NVARCHAR(10) NULL, i INT NULL)")
    cursor.setinputsizes([(W, 10, 0), (I, 0, 0)])
    rows = [(None, None), ("q", 1), (None, 2)]
    cursor.executemany("INSERT INTO #em (v, i) VALUES (?, ?)", rows)
    got = _rows(cursor, "SELECT v, i FROM #em ORDER BY i")
    assert got == [(None, None), ("q", 1), (None, 2)]


def test_executemany_sizes_cover_whole_batch_of_many_rows(cursor):
    cursor.execute("CREATE TABLE #batch (v NVARCHAR(40) NULL, id INT NOT NULL)")
    cursor.setinputsizes([(W, 40, 0), (I, 0, 0)])
    rows = [(f"row-{n:04d}", n) for n in range(500)]
    cursor.executemany("INSERT INTO #batch (v, id) VALUES (?, ?)", rows)
    assert _rows(cursor, "SELECT COUNT(*) FROM #batch") == [(500,)]
    assert _rows(cursor, "SELECT v FROM #batch WHERE id = 499") == [("row-0499",)]


def test_executemany_sized_bigint_column_keeps_large_values(cursor):
    cursor.execute("CREATE TABLE #embig (b BIGINT NOT NULL)")
    cursor.setinputsizes([(BIG, 0, 0)])
    values = [(1,), (2**40,), (2**62,)]
    cursor.executemany("INSERT INTO #embig (b) VALUES (?)", values)
    got = _rows(cursor, "SELECT b FROM #embig ORDER BY b")
    assert got == [(1,), (2**40,), (2**62,)]


# --- correct behavior that upstream currently gets wrong -------------------


@pytest.mark.upstream_bug(
    reason=(
        "executemany with an undersized string size hint raises RuntimeError "
        "('Input string exceeds allowed column size') instead of DataError, "
        "although execute() with the same hint raises DataError (PEP 249 data error)"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_executemany_undersized_hint_raises_dataerror(cursor):
    cursor.execute("CREATE TABLE #em_small (v NVARCHAR(10) NULL, id INT NOT NULL)")
    cursor.setinputsizes([(W, 3, 0), (I, 0, 0)])
    with pytest.raises(DataError):
        cursor.executemany(
            "INSERT INTO #em_small (v, id) VALUES (?, ?)",
            [("ab", 1), ("abcdef", 2)],
        )
    assert _rows(cursor, "SELECT COUNT(*) FROM #em_small") == [(0,)]


@pytest.mark.upstream_bug(
    reason=(
        "SQL_INTEGER size hint with an out-of-range Python int (2**40) silently "
        "sends 0 instead of raising or sending the exact value"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_out_of_range_int_under_integer_size_is_not_silently_zeroed(cursor):
    cursor.execute("CREATE TABLE #ovf (v BIGINT NULL)")
    cursor.setinputsizes([(I, 0, 0)])
    try:
        cursor.execute("INSERT INTO #ovf (v) VALUES (?)", (2**40,))
    except (DataError, ProgrammingError, OverflowError, ValueError):
        return  # raising is acceptable; silently storing 0 is not
    got = _rows(cursor, "SELECT v FROM #ovf")
    assert got == [(2**40,)]


@pytest.mark.upstream_bug(
    reason=(
        "DECIMAL(5,2) size hint with a value having more fractional digits "
        "(123.456) raises 'right-truncated' DataError; SQL Server rounds it to "
        "123.46 on conversion, and the hint's precision/scale should drive that"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_decimal_size_hint_rounds_like_sql_server(cursor):
    cursor.setinputsizes([(DEC, 5, 2)])
    got = _rows(cursor, "SELECT ?", (decimal.Decimal("123.456"),))
    assert got == [(decimal.Decimal("123.46"),)]
