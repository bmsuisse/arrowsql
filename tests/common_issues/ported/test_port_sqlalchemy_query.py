"""T-SQL row limiting and paging behaviour, ported from SQLAlchemy's mssql dialect tests.

Adapted from sqlalchemy/sqlalchemy test/dialect/mssql/test_compiler.py (the
TOP / OFFSET-FETCH / ROW_NUMBER cases) and test/dialect/mssql/test_query.py
(MIT), Copyright 2005-2026 SQLAlchemy authors and contributors. The MIT notice
is in LICENSE_sqlalchemy.txt next to this file. SQLAlchemy asserts the compiled
SQL text; here the same clauses are executed against SQL Server with qmark
parameters and the returned rows are checked.
"""

import pytest
from mssql_python.exceptions import DatabaseError


def _make_numbers(cursor, name, n):
    """Table dbo.<name> with id 1..n and grp = id % 3."""
    cursor.execute(f"CREATE TABLE dbo.{name} (id INT PRIMARY KEY, grp INT NOT NULL)")
    cursor.executemany(
        f"INSERT INTO dbo.{name} (id, grp) VALUES (?, ?)",
        [(i, i % 3) for i in range(1, n + 1)],
    )
    cursor.connection.commit()


def _ids(cursor, sql, params=()):
    cursor.execute(sql, params)
    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# TOP (compiler: "TOP __[POSTCOMPILE_param_1]", "TOP ... PERCENT ... WITH TIES")
# ---------------------------------------------------------------------------


def test_top_qmark_parameter_limits_rows(cursor, table_name):
    _make_numbers(cursor, table_name, 10)
    ids = _ids(cursor, f"SELECT TOP (?) id FROM dbo.{table_name} ORDER BY id", (3,))
    assert ids == [1, 2, 3]


def test_top_zero_returns_no_rows(cursor, table_name):
    _make_numbers(cursor, table_name, 10)
    assert _ids(cursor, f"SELECT TOP (?) id FROM dbo.{table_name} ORDER BY id", (0,)) == []


def test_top_larger_than_table_returns_everything(cursor, table_name):
    _make_numbers(cursor, table_name, 10)
    ids = _ids(cursor, f"SELECT TOP (?) id FROM dbo.{table_name} ORDER BY id", (1000,))
    assert ids == list(range(1, 11))


def test_top_percent_qmark_parameter(cursor, table_name):
    _make_numbers(cursor, table_name, 100)
    ids = _ids(
        cursor,
        f"SELECT TOP (?) PERCENT id FROM dbo.{table_name} ORDER BY id",
        (25,),
    )
    assert ids == list(range(1, 26))


def test_top_with_ties_keeps_every_tied_row(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, score INT NOT NULL)")
    cursor.executemany(
        f"INSERT INTO dbo.{table_name} (id, score) VALUES (?, ?)",
        [(1, 1), (2, 2), (3, 2), (4, 2), (5, 3)],
    )
    cursor.connection.commit()
    ids = _ids(
        cursor,
        f"SELECT TOP (?) WITH TIES id FROM dbo.{table_name} ORDER BY score",
        (2,),
    )
    # TOP 2 lands on the first score=2 row; WITH TIES pulls in the other two.
    assert sorted(ids) == [1, 2, 3, 4]


def test_top_non_integer_parameter_is_rejected(cursor, table_name):
    _make_numbers(cursor, table_name, 3)
    with pytest.raises(DatabaseError):
        cursor.execute(f"SELECT TOP (?) id FROM dbo.{table_name} ORDER BY id", ("three",))
        cursor.fetchall()


# ---------------------------------------------------------------------------
# OFFSET / FETCH (compiler: "OFFSET ... ROWS FETCH FIRST ... ROWS ONLY")
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "offset, fetch, expected",
    [
        (0, 3, [1, 2, 3]),
        (3, 4, [4, 5, 6, 7]),
        (9, 5, [10]),
        (10, 2, []),
        (4, 100, [5, 6, 7, 8, 9, 10]),
    ],
)
def test_offset_fetch_qmark_window(cursor, table_name, offset, fetch, expected):
    _make_numbers(cursor, table_name, 10)
    ids = _ids(
        cursor,
        f"SELECT id FROM dbo.{table_name} ORDER BY id "
        "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        (offset, fetch),
    )
    assert ids == expected


def test_offset_without_fetch_returns_the_tail(cursor, table_name):
    _make_numbers(cursor, table_name, 10)
    ids = _ids(
        cursor,
        f"SELECT id FROM dbo.{table_name} ORDER BY id OFFSET ? ROWS",
        (7,),
    )
    assert ids == [8, 9, 10]


def test_offset_fetch_accepts_parenthesised_expressions(cursor, table_name):
    _make_numbers(cursor, table_name, 10)
    ids = _ids(
        cursor,
        f"SELECT id FROM dbo.{table_name} ORDER BY id "
        "OFFSET (? + ?) ROWS FETCH NEXT (? * 2) ROWS ONLY",
        (2, 1, 1),
    )
    # offset 3 -> first row is id 4; fetch 2 -> ids 4 and 5
    assert ids == [4, 5]


def test_offset_fetch_without_order_by_is_a_syntax_error(cursor, table_name):
    _make_numbers(cursor, table_name, 5)
    with pytest.raises(DatabaseError):
        cursor.execute(
            f"SELECT id FROM dbo.{table_name} OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            (0, 2),
        )
        cursor.fetchall()


def test_fetch_zero_rows_is_an_error_and_connection_stays_usable(cursor, table_name):
    _make_numbers(cursor, table_name, 5)
    with pytest.raises(DatabaseError):
        cursor.execute(
            f"SELECT id FROM dbo.{table_name} ORDER BY id "
            "OFFSET 0 ROWS FETCH NEXT 0 ROWS ONLY",
        )
        cursor.fetchall()
    # the failed statement must not poison the session
    assert _ids(cursor, "SELECT 1 AS one") == [1]


def test_negative_offset_is_an_error(cursor, table_name):
    _make_numbers(cursor, table_name, 5)
    with pytest.raises(DatabaseError):
        cursor.execute(
            f"SELECT id FROM dbo.{table_name} ORDER BY id OFFSET ? ROWS",
            (-1,),
        )
        cursor.fetchall()


def test_offset_fetch_pages_visit_every_row_exactly_once(cursor, table_name):
    _make_numbers(cursor, table_name, 50)
    seen = []
    page = 0
    while True:
        rows = _ids(
            cursor,
            f"SELECT id FROM dbo.{table_name} ORDER BY grp, id "
            "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
            (page * 7, 7),
        )
        if not rows:
            break
        seen.extend(rows)
        page += 1
    assert page == 8  # 50 rows in pages of 7
    assert len(seen) == len(set(seen)) == 50
    assert sorted(seen) == list(range(1, 51))


# ---------------------------------------------------------------------------
# Pre-2012 paging (compiler: "mssql_rn > :param_1 AND mssql_rn <= ...")
# ---------------------------------------------------------------------------


def test_row_number_window_matches_offset_fetch(cursor, table_name):
    _make_numbers(cursor, table_name, 10)
    legacy = _ids(
        cursor,
        "SELECT id FROM ("
        f"  SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM dbo.{table_name}"
        ") AS q WHERE rn > ? AND rn <= ? ORDER BY rn",
        (3, 6),
    )
    modern = _ids(
        cursor,
        f"SELECT id FROM dbo.{table_name} ORDER BY id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY",
        (3, 3),
    )
    assert legacy == modern == [4, 5, 6]
