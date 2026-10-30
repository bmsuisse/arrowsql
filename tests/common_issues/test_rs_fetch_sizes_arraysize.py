"""fetchmany(size) and cursor.arraysize edge cases.

Covers: size 0, size 1, negative sizes, size larger than the result, the
default arraysize (1) and changes to it, fetchmany after exhaustion,
fetchone/fetchall after a fetchall/fetchmany, and iteration after a partial
fetch. Also keyword-argument calls, non-integer sizes, and re-execution after
a partial fetch.

Failure modes a native-TDS reimplementation can easily get wrong:

* fetchmany(0) and fetchmany(<0) must return [] and must NOT consume rows. A
  naive loop that fetches "at most size" rows, or one that treats a negative
  size as "unbounded", gets this wrong (the CPython sqlite3 fix for gh-139283
  is the reference case where size <= 0 fetched everything).
* fetchmany() with no argument uses cursor.arraysize, which defaults to 1. The
  default must be read at call time, so changing arraysize between calls must
  take effect on the next call.
* A partial fetch must leave a cursor position that fetchone, fetchall, next()
  and iteration all agree on. A reimplementation with a separate row buffer per
  method can drop or duplicate rows.
* Once the result is exhausted, fetchmany returns [] and fetchone returns None.
  It must not raise, and it must not restart the result.
* Re-executing on the same cursor after a partial fetch must discard the old
  buffered rows, not serve leftovers from the previous statement.
* Size arguments must be Python ints. The driver should reject str and float
  with TypeError and keep the cursor usable afterwards. Sizes beyond the
  int32 range should return all remaining rows instead of raising (see the
  upstream_bug test below).
* Passing size by keyword, fetchmany(size=2), must work. pyodbc issue #860
  reported that the keyword form raised "fetchmany() takes no keyword
  arguments".

Research sources (fetchmany and arraysize issues and docs):

* https://github.com/microsoft/mssql-python/issues/345 (fetchmany ignores the
  requested row count when the table has an LOB column)
* https://github.com/microsoft/mssql-python/pull/792 (async fetchmany: size <= 0
  must return [] and fetchmany(-1) must not reach the native code with a
  negative size)
* https://github.com/microsoft/mssql-python/pull/806 (native fetchmany rejects
  non-positive sizes internally and errors if ODBC changes the row-array size;
  buffer reuse across calls)
* https://github.com/microsoft/mssql-python/pull/858 (fetchmany row-level
  handling of SQL NULL)
* https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/retrieving-data
  (cursor.arraysize is the default batch size for fetchmany(), default 1)
* https://github.com/mkleehammer/pyodbc/issues/860 (fetchmany(size=...) keyword
  call raised TypeError in pyodbc 4.0.28)
* https://github.com/python/cpython/commit/cde02ae7825c09ded31e253bc58839716a943795
  (gh-139283: sqlite3 fetchmany(size <= 0) returned all rows; fixed to reject
  negative sizes)
* https://github.com/cubrid-lab/pycubrid/pull/876 (fetchmany(size <= 0) returns
  an empty list silently; deprecated in favour of an error)
* https://github.com/matrixorigin/matrixone/issues/31314 (negative fetchmany
  size moved the result cursor backwards)
* https://readthedocs.org/projects/pymssql/ (pymssql fetchmany(size=None):
  returns [] when no data is left; arraysize defaults to 1)

Expected values were checked against the upstream ODBC driver (.venv-upstream)
before being written as assertions.
"""

import pytest
from mssql_python.exceptions import ProgrammingError

ROW_COUNT = 5


def _seed(cursor, n=ROW_COUNT):
    """Create a #temp table with ids 1..n and leave the cursor ready to query it."""
    cursor.execute("IF OBJECT_ID('tempdb..#fs') IS NOT NULL DROP TABLE #fs")
    cursor.execute("CREATE TABLE #fs (id int NOT NULL PRIMARY KEY)")
    cursor.executemany("INSERT INTO #fs (id) VALUES (?)", [(i,) for i in range(1, n + 1)])


def _ids(rows):
    return [int(r[0]) for r in rows]


@pytest.fixture
def seeded(cursor):
    _seed(cursor)
    cursor.connection.commit()
    cursor.execute("SELECT id FROM #fs ORDER BY id")
    return cursor


def test_fetchmany_zero_returns_empty_list_and_consumes_nothing(seeded):
    assert seeded.fetchmany(0) == []
    # The rows must still be there.
    assert _ids(seeded.fetchall()) == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("size", [-1, -2, -(10**6)])
def test_fetchmany_negative_returns_empty_list_and_consumes_nothing(seeded, size):
    result = seeded.fetchmany(size)
    assert type(result) is list
    assert result == []
    assert _ids(seeded.fetchall()) == [1, 2, 3, 4, 5]


def test_fetchmany_one_returns_single_row_list(seeded):
    result = seeded.fetchmany(1)
    assert type(result) is list
    assert len(result) == 1
    assert int(result[0][0]) == 1
    assert _ids(seeded.fetchall()) == [2, 3, 4, 5]


def test_fetchmany_size_larger_than_result_returns_all_then_empty(seeded):
    first = seeded.fetchmany(100)
    assert _ids(first) == [1, 2, 3, 4, 5]
    assert seeded.fetchmany(100) == []


def test_fetchmany_exact_size_then_fetchone_returns_none(seeded):
    assert _ids(seeded.fetchmany(5)) == [1, 2, 3, 4, 5]
    assert seeded.fetchone() is None


def test_default_arraysize_is_one_and_no_arg_fetchmany_returns_one_row(seeded):
    assert seeded.arraysize == 1
    result = seeded.fetchmany()
    assert type(result) is list
    assert _ids(result) == [1]


def test_changing_arraysize_changes_next_fetchmany_without_argument(seeded):
    seeded.arraysize = 2
    assert _ids(seeded.fetchmany()) == [1, 2]
    seeded.arraysize = 3
    assert _ids(seeded.fetchmany()) == [3, 4, 5]
    assert seeded.fetchmany() == []


def test_explicit_size_ignores_arraysize(seeded):
    seeded.arraysize = 4
    assert _ids(seeded.fetchmany(1)) == [1]
    assert _ids(seeded.fetchmany(2)) == [2, 3]
    assert seeded.arraysize == 4


def test_fetchmany_size_by_keyword(seeded):
    assert _ids(seeded.fetchmany(size=2)) == [1, 2]
    assert _ids(seeded.fetchmany(size=10)) == [3, 4, 5]


def test_fetchmany_after_exhaustion_returns_empty_list_repeatedly(seeded):
    assert _ids(seeded.fetchall()) == [1, 2, 3, 4, 5]
    for _ in range(3):
        assert seeded.fetchmany(2) == []
        assert seeded.fetchmany() == []
        assert seeded.fetchone() is None


def test_fetchone_after_fetchall_returns_none(seeded):
    assert len(seeded.fetchall()) == 5
    assert seeded.fetchone() is None
    assert seeded.fetchmany(10) == []


def test_fetchall_after_partial_fetchmany_returns_remaining_rows(seeded):
    assert _ids(seeded.fetchmany(2)) == [1, 2]
    assert _ids(seeded.fetchall()) == [3, 4, 5]
    assert seeded.fetchall() == []


def test_iteration_after_partial_fetchmany_yields_only_remaining_rows(seeded):
    assert _ids(seeded.fetchmany(2)) == [1, 2]
    assert [int(r[0]) for r in seeded] == [3, 4, 5]


def test_fetchone_and_fetchmany_share_one_position(seeded):
    assert int(seeded.fetchone()[0]) == 1
    assert _ids(seeded.fetchmany(2)) == [2, 3]
    assert int(seeded.fetchone()[0]) == 4
    assert _ids(seeded.fetchmany(5)) == [5]
    assert seeded.fetchone() is None


def test_fetchmany_in_chunks_covers_every_row_once_in_order(cursor):
    _seed(cursor, n=2500)
    cursor.connection.commit()
    cursor.execute("SELECT id FROM #fs ORDER BY id")
    chunks = []
    while True:
        chunk = cursor.fetchmany(1000)
        if not chunk:
            break
        chunks.append(len(chunk))
        ids = _ids(chunk)
        assert ids == list(range(ids[0], ids[0] + len(ids)))
    assert chunks == [1000, 1000, 500]


def test_fetchmany_on_empty_result_returns_empty_list(cursor):
    _seed(cursor)
    cursor.connection.commit()
    cursor.execute("SELECT id FROM #fs WHERE id < 0")
    assert cursor.fetchmany(5) == []
    assert cursor.fetchmany() == []
    assert cursor.fetchone() is None


def test_reexecute_after_partial_fetch_discards_old_rows(cursor):
    _seed(cursor)
    cursor.connection.commit()
    cursor.execute("SELECT id FROM #fs ORDER BY id")
    assert _ids(cursor.fetchmany(2)) == [1, 2]
    cursor.execute("SELECT id FROM #fs WHERE id > 3 ORDER BY id")
    assert _ids(cursor.fetchmany(10)) == [4, 5]
    assert cursor.fetchmany(10) == []


@pytest.mark.parametrize("bad_size", ["2", 2.5])
def test_fetchmany_non_int_size_raises_type_error_and_cursor_survives(seeded, bad_size):
    with pytest.raises(TypeError):
        seeded.fetchmany(bad_size)
    assert _ids(seeded.fetchmany(1)) == [1]


def test_fetchmany_bool_size_is_treated_as_int_one(seeded):
    assert _ids(seeded.fetchmany(True)) == [1]


def test_fetchmany_after_close_raises_programming_error(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1 AS a")
    cur.close()
    with pytest.raises(ProgrammingError):
        cur.fetchmany(1)


def test_fetchmany_without_result_set_raises_programming_error(conn):
    cur = conn.cursor()
    with pytest.raises(ProgrammingError):
        cur.fetchmany(2)


def test_fetchmany_with_lob_column_returns_requested_row_count(table_name, cursor):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int NOT NULL, body nvarchar(max) NOT NULL)")
    for i in range(1, 6):
        cursor.execute(f"INSERT INTO dbo.{table_name} (id, body) VALUES (?, ?)", i, "x" * (i * 1000))
    cursor.connection.commit()
    cursor.execute(f"SELECT id, body FROM dbo.{table_name} ORDER BY id")
    first = cursor.fetchmany(3)
    assert len(first) == 3
    assert [int(r[0]) for r in first] == [1, 2, 3]
    assert [len(r[1]) for r in first] == [1000, 2000, 3000]
    rest = cursor.fetchmany(3)
    assert [int(r[0]) for r in rest] == [4, 5]


@pytest.mark.upstream_bug(
    reason=(
        "fetchmany(size) with a Python int above the int32 range raises TypeError "
        "from the pybind11 binding (argument overflow) instead of returning all "
        "remaining rows"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_fetchmany_size_beyond_int32_returns_all_rows(seeded):
    assert _ids(seeded.fetchmany(2**40)) == [1, 2, 3, 4, 5]
    assert seeded.fetchmany(2**40) == []


def test_fetchmany_large_size_returns_all_rows_without_stalling(seeded):
    # 2**31 - 1 stalls upstream (driver buffer scales with the requested size),
    # so this test uses 2**24, which returns immediately.
    assert _ids(seeded.fetchmany(2**24)) == [1, 2, 3, 4, 5]
