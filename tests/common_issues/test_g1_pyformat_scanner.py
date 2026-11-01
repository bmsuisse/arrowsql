"""Parity of the native pyformat scanner with the original Python implementation."""

import random

import pytest

from arrowsql import parameter_helper as ph
from arrowsql.row import Row

_CLOSERS = {"'": "'", '"': '"', "[": "]"}


def _oracle_skip(sql, i, length):
    ch = sql[i]
    if ch == "-" and i + 1 < length and sql[i + 1] == "-":
        i += 2
        while i < length and sql[i] != "\n":
            i += 1
        return i
    if ch == "/" and i + 1 < length and sql[i + 1] == "*":
        i += 2
        depth = 1
        while i < length and depth > 0:
            if i + 1 < length and sql[i] == "/" and sql[i + 1] == "*":
                depth += 1
                i += 2
            elif i + 1 < length and sql[i] == "*" and sql[i + 1] == "/":
                depth -= 1
                i += 2
            else:
                i += 1
        return min(i, length)
    close = _CLOSERS.get(ch)
    if close:
        i += 1
        while True:
            i = sql.find(close, i)
            if i == -1:
                return length
            if i + 1 < length and sql[i + 1] == close:
                i += 2
                continue
            return i + 1
    return -1


def _oracle_qmark(sql):
    i, length = 0, len(sql)
    while i < length:
        s = _oracle_skip(sql, i, length)
        if s >= 0:
            i = s
            continue
        if sql[i] == "?":
            return True
        i += 1
    return False


def _oracle_scan(sql):
    params, i, length = [], 0, len(sql)
    while i < length:
        s = _oracle_skip(sql, i, length)
        if s >= 0:
            i = s
            continue
        if i + 2 < length and sql[i] == "%" and sql[i + 1] == "(":
            j = i + 2
            while j < length and sql[j] != ")":
                j += 1
            if j < length and sql[j] == ")":
                if j + 1 < length and sql[j + 1] == "s":
                    params.append((sql[i + 2 : j], i, j + 2))
                    i = j + 2
                    continue
        i += 1
    return params


CASES = [
    "SELECT * FROM users WHERE id = %(id)s",
    "WHERE name = %(name)s OR email = %(name)s",
    "SELECT '%(x)s', \"%(y)s\", [%(z)s] FROM t WHERE a = %(a)s",
    "-- %(c)s\nSELECT %(d)s /* %(e) /* nested %(f)s */ %(g)s */ %(h)s",
    "SELECT 'it''s %(q)s' , %(r)s",
    "SELECT [a]]b %(s)s] , %(t)s",
    "SELECT 'unterminated %(u)s",
    "SELECT %(a",
    "SELECT %(a)x %(b)s",
    "SELECT %(it's)s, %(b)s",
    "SELECT 'x?' , [y?], ?, -- ?",
    "SELECT \U0001f600 %(emoji\U0001f600)s, é %(b)s ?",
    "%(a)s",
    "%(",
    "",
]


@pytest.mark.parametrize("sql", CASES)
def test_cases(sql):
    assert ph._scan_pyformat(sql) == _oracle_scan(sql)
    assert ph._has_unquoted_question_marks(sql) == _oracle_qmark(sql)


def test_fuzz():
    rng = random.Random(1234)
    atoms = ["'", '"', "[", "]", "--", "/*", "*/", "\n", "%(", ")s", ")", "s", "?", "%", "(",
             " ", "a", "\U0001f600", "é", "%(x)s", "''", "]]"]
    for _ in range(4000):
        sql = "".join(rng.choice(atoms) for _ in range(rng.randint(0, 25)))
        assert ph._scan_pyformat(sql) == _oracle_scan(sql), sql
        assert ph._has_unquoted_question_marks(sql) == _oracle_qmark(sql), sql


def test_tuple_with_percent_paren_still_rejected():
    with pytest.raises(TypeError):
        ph.detect_and_convert_parameters("select %(a)s", (1,))
    assert ph.detect_and_convert_parameters("select ?", (1,)) == ("select ?", (1,))
    assert ph.detect_and_convert_parameters("select '%(a)s', ?", (1,))[1] == (1,)


def test_convert_with_names():
    sql, params, names = ph.convert_pyformat_to_qmark_with_names(
        "a = %(x)s AND b = %(y)s AND c = %(x)s AND d LIKE '100%%'", {"x": 1, "y": 2}
    )
    assert sql == "a = ? AND b = ? AND c = ? AND d LIKE '100%'"
    assert params == (1, 2, 1) and names == ["x", "y", "x"]


def test_row_mapping_duplicates_last_wins():
    names = ("a", "b", "a")
    row = Row([1, 2, 3], {"a": 2, "b": 1}, None, None, None, None, names)
    m = row._mapping
    assert list(m) == ["a", "b"] and len(m) == 2
    assert dict(m) == {"a": 3, "b": 2}
    assert "A" not in m and "a" in m
    with pytest.raises(KeyError):
        m["A"]
    # a second row sharing the names tuple hits the cache and stays correct
    row2 = Row([7, 8, 9], {"a": 2, "b": 1}, None, None, None, None, names)
    assert dict(row2._mapping) == {"a": 9, "b": 8}


def test_fetch_with_output_converter_all_paths(conn):
    from arrowsql.constants import ConstantsDDBC as K

    conn.add_output_converter(K.SQL_INTEGER.value, lambda v: v * 10)
    try:
        cur = conn.cursor()
        sql = "SELECT 1 AS a, 'x' AS s, CAST(NULL AS INT) AS n, 3 AS a"
        cur.execute(sql)
        one = cur.fetchone()
        cur.execute(sql)
        many = cur.fetchmany(5)[0]
        cur.execute(sql)
        everything = cur.fetchall()[0]
        for row in (one, many, everything):
            assert list(row) == [10, "x", None, 30]
            assert dict(row._mapping) == {"a": 30, "s": "x", "n": None}
        cur.close()
    finally:
        conn.clear_output_converters()


def test_executemany_slow_path_decimal_null_and_ragged(conn):
    import decimal

    cur = conn.cursor()
    cur.execute("CREATE TABLE #g1 (i INT, d DECIMAL(18,4), s NVARCHAR(10))")
    rows = [(1, decimal.Decimal("1.5"), "a"), (2, None, "b"), (3, decimal.Decimal("-2.25"), None)]
    cur.executemany("INSERT INTO #g1 VALUES (?, ?, ?)", rows)
    cur.execute("SELECT i, d, s FROM #g1 ORDER BY i")
    assert [tuple(r) for r in cur.fetchall()] == [
        (1, decimal.Decimal("1.5000"), "a"),
        (2, None, "b"),
        (3, decimal.Decimal("-2.2500"), None),
    ]
    with pytest.raises(ValueError, match="Inconsistent parameter row size"):
        cur.executemany("INSERT INTO #g1 VALUES (?, ?, ?)", [(4, None, "x"), (5, None, "y", "extra")])
    cur.executemany(
        "INSERT INTO #g1 VALUES (%(i)s, %(d)s, %(s)s)",
        [{"i": 6, "d": decimal.Decimal("3.5"), "s": "z"}, {"i": 7, "d": None, "s": "y"}],
    )
    cur.execute("SELECT COUNT(*) FROM #g1")
    assert cur.fetchone()[0] == 5
    cur.close()
