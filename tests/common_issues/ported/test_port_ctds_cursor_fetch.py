"""Cursor fetch state, iteration, arraysize, DB-API no-op hooks and binary parameters.

Adapted from zillow/ctds tests/test_cursor_fetchmany.py, tests/test_cursor_arraysize.py,
tests/test_cursor___iter__.py, tests/test_cursor_setinputsizes.py,
tests/test_cursor_setoutputsize.py, tests/test_cursor_description.py and
tests/test_tds_binary.py (MIT, Copyright (c) 2016 Zillow); the license text is in
LICENSE_ctds.txt next to this file. ctds' DB-API extension warnings
(cursor.__iter__, cursor.rownumber) and ctds.Binary wrappers are not ported.
"""

import pytest
from mssql_python import InterfaceError, ProgrammingError


def _rows(cursor):
    return [tuple(r) for r in cursor.fetchall()]


# ---------------------------------------------------------------------------
# DB-API hooks that are documented no-ops
# ---------------------------------------------------------------------------


def test_setoutputsize_accepts_all_call_shapes_and_returns_none(cursor):
    assert cursor.setoutputsize(100, 1) is None
    assert cursor.setoutputsize(100, None) is None
    assert cursor.setoutputsize(100) is None


def test_setoutputsize_does_not_truncate_fetched_values(cursor):
    cursor.setoutputsize(10, 0)
    value = "x" * 3000
    cursor.execute("SELECT CAST(? AS NVARCHAR(MAX)) AS v", [value])
    assert cursor.fetchone()[0] == value


def test_setinputsizes_with_empty_sizes_returns_none_and_keeps_values(cursor):
    assert cursor.setinputsizes([]) is None
    cursor.execute("SELECT ? AS v", ["z" * 500])
    assert cursor.fetchone()[0] == "z" * 500


# ---------------------------------------------------------------------------
# arraysize
# ---------------------------------------------------------------------------


def test_arraysize_defaults_to_one(cursor):
    assert cursor.arraysize == 1


def test_arraysize_round_trips_integers(cursor):
    for size in (3, 1, 250):
        cursor.arraysize = size
        assert cursor.arraysize == size


_ARRAYSIZE_BUG = "arraysize accepts None/str/bytes/negative values silently; DB-API requires an int and a later fetchmany() then fails"


@pytest.mark.upstream_bug(reason=_ARRAYSIZE_BUG)
@pytest.mark.xfail(strict=True, reason="upstream bug")
@pytest.mark.parametrize("bad_value", [None, "", b"1234"])
def test_arraysize_rejects_non_integers_and_keeps_previous_value(cursor, bad_value):
    cursor.arraysize = 7
    with pytest.raises(TypeError):
        cursor.arraysize = bad_value
    assert cursor.arraysize == 7


@pytest.mark.upstream_bug(reason=_ARRAYSIZE_BUG)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_arraysize_rejects_negative_and_keeps_previous_value(cursor):
    cursor.arraysize = 4
    with pytest.raises((OverflowError, ValueError)):
        cursor.arraysize = -1
    assert cursor.arraysize == 4


# ---------------------------------------------------------------------------
# fetchmany across result sets
# ---------------------------------------------------------------------------


def test_fetchmany_walks_sets_and_returns_empty_when_exhausted(cursor):
    cursor.execute(
        """
        SET NOCOUNT ON;
        DECLARE @t TABLE (i INT);
        INSERT INTO @t (i) VALUES (1),(2),(3);
        SELECT i FROM @t;
        SELECT i * 2 FROM @t;
        """
    )
    assert [tuple(r) for r in cursor.fetchmany(3)] == [(1,), (2,), (3,)]
    assert list(cursor.fetchmany()) == []
    assert cursor.nextset()
    assert [tuple(r) for r in cursor.fetchmany(3)] == [(2,), (4,), (6,)]
    assert list(cursor.fetchmany()) == []
    assert not cursor.nextset()


def test_fetchmany_on_empty_result_set_returns_empty_list(cursor):
    cursor.execute(
        """
        SET NOCOUNT ON;
        DECLARE @t TABLE (i INT);
        INSERT INTO @t (i) VALUES (1),(2),(3);
        SELECT i FROM @t WHERE i < 0;
        """
    )
    assert list(cursor.fetchmany()) == []
    assert not cursor.nextset()


def test_fetchmany_respects_size_across_partial_reads(cursor):
    cursor.execute(
        "SET NOCOUNT ON; DECLARE @t TABLE (i INT); INSERT INTO @t (i) VALUES (1),(2),(3),(4),(5); SELECT i FROM @t;"
    )
    assert [r[0] for r in cursor.fetchmany(2)] == [1, 2]
    assert [r[0] for r in cursor.fetchmany(2)] == [3, 4]
    assert [r[0] for r in cursor.fetchmany(2)] == [5]
    assert list(cursor.fetchmany(2)) == []


def test_fetchmany_after_result_set_is_gone_raises(cursor):
    cursor.execute("SELECT 1 AS one")
    assert not cursor.nextset()
    with pytest.raises((InterfaceError, ProgrammingError)):
        cursor.fetchmany()


@pytest.mark.upstream_bug(
    reason=(
        "execute() of a batch whose first statement is DML leaves the cursor on the "
        "row-count with description None; the SELECT result set is only reachable "
        "after nextset(), so fetchone/fetchall raise 'Invalid cursor state'. ctds "
        "(FreeTDS) positions on the first result set."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_execute_positions_on_first_result_set_after_leading_dml(cursor):
    cursor.execute("DECLARE @t TABLE (i INT); INSERT INTO @t (i) VALUES (1); SELECT i FROM @t;")
    assert cursor.description is not None
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]


# ---------------------------------------------------------------------------
# Iteration protocol (ctds test_cursor___iter__, minus the warnings)
# ---------------------------------------------------------------------------


def test_iterating_cursor_yields_rows_of_each_set_in_turn(cursor):
    cursor.execute(
        """
        SET NOCOUNT ON;
        DECLARE @t TABLE (i INT);
        INSERT INTO @t (i) VALUES (1),(2),(3);
        SELECT i FROM @t;
        SELECT i * 2 FROM @t;
        """
    )
    assert [tuple(r) for r in cursor] == [(1,), (2,), (3,)]
    assert cursor.nextset()
    assert [tuple(r) for r in cursor] == [(2,), (4,), (6,)]
    assert not cursor.nextset()


def test_iterating_exhausted_cursor_yields_nothing(cursor):
    cursor.execute("SELECT 1 AS one")
    assert [tuple(r) for r in cursor] == [(1,)]
    assert list(cursor) == []


def test_fetchone_after_exhaustion_keeps_returning_none(cursor):
    cursor.execute("SELECT 1 AS one")
    assert cursor.fetchone() is not None
    for _ in range(3):
        assert cursor.fetchone() is None
    assert _rows(cursor) == []


# ---------------------------------------------------------------------------
# Description and closed-cursor behaviour
# ---------------------------------------------------------------------------


def test_description_is_none_for_statements_without_result_set(cursor):
    cursor.execute("DECLARE @x INT = 1")
    assert cursor.description is None


def test_description_present_for_zero_row_select(cursor):
    cursor.execute("SELECT CAST(1 AS INT) AS n WHERE 1 = 0")
    assert cursor.description is not None
    assert cursor.description[0][0] == "n"
    assert _rows(cursor) == []


def test_fetch_on_closed_cursor_raises(conn):
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.close()
    with pytest.raises((InterfaceError, ProgrammingError)):
        cur.fetchone()


# ---------------------------------------------------------------------------
# Binary parameters (ctds test_tds_binary / test_tds_types.test_varbinary)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [b"", b"\x00", b"1234567890", b"\x00\x01\x00\xff", bytes(range(256)), bytearray(b"ba")],
)
def test_binary_parameter_round_trips_exact_bytes(cursor, value):
    cursor.execute("SELECT CAST(? AS VARBINARY(MAX)) AS b", [value])
    got = cursor.fetchone()[0]
    assert bytes(got) == bytes(value)


def test_binary_parameter_none_is_sql_null(cursor):
    cursor.execute("SELECT CAST(? AS VARBINARY(MAX)) AS b", [None])
    assert cursor.fetchone()[0] is None


def test_binary_parameter_longer_than_varbinary_default_is_not_truncated(cursor):
    value = bytes(range(256)) * 40  # 10240 bytes
    cursor.execute("SELECT CAST(? AS VARBINARY(MAX)) AS b, DATALENGTH(CAST(? AS VARBINARY(MAX))) AS n", [value, value])
    row = cursor.fetchone()
    assert bytes(row[0]) == value
    assert row[1] == len(value)
