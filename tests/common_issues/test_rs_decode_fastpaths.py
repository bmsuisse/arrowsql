"""Row decode fast paths: ASCII UTF-16 narrowing, the direct-mapped Decimal
cache, and fetchone/partial-fetchmany row slicing.

Values are inserted from Python and read back, so every expectation comes from
Python, not from a second server round trip.
"""

import os
import subprocess
import sys
from decimal import Decimal

import pytest

TEXTS = [
    "",
    "a",
    "abc",
    "abcd",
    "abcde",
    "abcdefg",
    "abcdefgh",
    "abcdefghi",
    "x" * 31,
    "x" * 32,
    "x" * 33,
    "\x00",
    "a\x00b",
    "\x7f",
    "\u0080",
    "café",
    "étude",
    "abšcd",
    "abcdefgš",
    "abcdefghš",
    "snow ☃ man",
    "emoji \U0001f600 end",
    "\U0001f600",
    "tab\tnl\ncr\r",
    " leading and trailing ",
]


@pytest.fixture
def texts_table(cursor):
    cursor.execute("CREATE TABLE #t (id INT, v NVARCHAR(100) NULL, c NCHAR(6) NULL, n NVARCHAR(MAX) NULL)")
    rows = [(i, t, t[:6], t) for i, t in enumerate(TEXTS)]
    rows.append((len(rows), None, None, None))
    cursor.executemany("INSERT INTO #t VALUES (?, ?, ?, ?)", rows)
    return rows


def _pad6(value):
    if value is None:
        return None
    units = len(value.encode("utf-16-le")) // 2
    return value + " " * (6 - units)


def _expected(rows):
    return [(i, v, _pad6(c), n) for i, v, c, n in rows]


def test_ascii_and_non_ascii_text_roundtrip(cursor, texts_table):
    cursor.execute("SELECT id, v, c, n FROM #t ORDER BY id")
    got = [tuple(r) for r in cursor.fetchall()]
    assert got == _expected(texts_table)


def test_text_matches_across_fetch_styles(cursor, texts_table):
    sql = "SELECT id, v, c, n FROM #t ORDER BY id"
    cursor.execute(sql)
    all_rows = [tuple(r) for r in cursor.fetchall()]
    cursor.execute(sql)
    one = []
    while (r := cursor.fetchone()) is not None:
        one.append(tuple(r))
    cursor.execute(sql)
    many = []
    while chunk := cursor.fetchmany(3):
        many.extend(tuple(r) for r in chunk)
    assert one == all_rows
    assert many == all_rows


def test_mixed_partial_fetches_keep_rows_intact(cursor):
    cursor.execute("CREATE TABLE #m (id INT, s NVARCHAR(40), d DECIMAL(18, 4), b VARBINARY(20))")
    rows = [(i, f"row-{i}-{'z' * (i % 17)}", Decimal(i) / 8, bytes([i % 256]) * (i % 11)) for i in range(2000)]
    cursor.executemany("INSERT INTO #m VALUES (?, ?, ?, ?)", rows)
    expected = [(i, s, d.quantize(Decimal("0.0001")), b) for i, s, d, b in rows]
    cursor.execute("SELECT id, s, d, b FROM #m ORDER BY id")
    got = [tuple(cursor.fetchone()), *map(tuple, cursor.fetchmany(3)), tuple(cursor.fetchone())]
    got.extend(tuple(r) for r in cursor.fetchmany(1000))
    got.extend(tuple(r) for r in cursor.fetchall())
    assert got == expected


def _decimal_values():
    values = []
    for i in range(6000):
        values.append(Decimal(i) / Decimal(10_000))
        values.append(-Decimal(i * 37) / Decimal(10_000))
    values += [Decimal("0.0000"), Decimal("-0.0001"), Decimal("99999999999999.9999"), Decimal("-99999999999999.9999")]
    return values


def _check_decimals(cursor, values):
    cursor.execute("CREATE TABLE #d (id INT, a DECIMAL(18, 4), b DECIMAL(38, 10), c DECIMAL(10, 0))")
    rows = [(i, v, v * Decimal("1.5"), Decimal(i % 7)) for i, v in enumerate(values)]
    cursor.executemany("INSERT INTO #d VALUES (?, ?, ?, ?)", rows)
    cursor.execute("SELECT id, a, b, c FROM #d ORDER BY id")
    got = cursor.fetchall()
    assert len(got) == len(rows)
    for (i, a, b, c), (_, ea, eb, ec) in zip(got, rows):
        assert str(a) == str(ea.quantize(Decimal("0.0001"))), i
        assert str(b) == str(eb.quantize(Decimal("0.0000000001"))), i
        assert str(c) == str(ec), i
    cursor.execute("SELECT id, a, b, c FROM #d ORDER BY id")
    first = cursor.fetchone()
    assert tuple(first) == tuple(got[0])


def test_decimal_values_distinct_and_repeated(cursor):
    # More distinct values than the cache has slots, plus heavy repetition.
    _check_decimals(cursor, _decimal_values())


def test_decimal_scale_and_sign_are_part_of_the_key(cursor):
    cursor.execute("SELECT CAST(1 AS DECIMAL(10,0)), CAST(1 AS DECIMAL(10,2)), CAST(-1 AS DECIMAL(10,2)), CAST(1.00 AS DECIMAL(10,2))")
    row = cursor.fetchone()
    assert [str(v) for v in row] == ["1", "1.00", "-1.00", "1.00"]
    for _ in range(3):  # repeats hit the single-row cache
        cursor.execute("SELECT CAST(1 AS DECIMAL(10,0)), CAST(1 AS DECIMAL(10,2)), CAST(-1 AS DECIMAL(10,2))")
        assert [str(v) for v in cursor.fetchone()] == ["1", "1.00", "-1.00"]
        cursor.execute("SELECT CAST(1 AS DECIMAL(10,0)), CAST(1 AS DECIMAL(10,2)), CAST(-1 AS DECIMAL(10,2))")
        assert [str(v) for v in cursor.fetchall()[0]] == ["1", "1.00", "-1.00"]


def test_decimal_cache_with_forced_collisions(conn_str):
    """A 1-slot cache collides on every distinct value; results must not change."""
    script = (
        "import os, sys\n"
        "sys.path.insert(0, os.environ['TEST_DIR'])\n"
        "import mssql_python\n"
        "from test_rs_decode_fastpaths import _check_decimals, _decimal_values\n"
        "c = mssql_python.connect(os.environ['DB_CONNECTION_STRING']); cur = c.cursor()\n"
        "_check_decimals(cur, _decimal_values())\n"
        "print('ok')\n"
    )
    env = dict(os.environ, MSSQL_RS_DECIMAL_CACHE_SLOTS="1", TEST_DIR=os.path.dirname(__file__))
    out = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0 and out.stdout.strip() == "ok", out.stderr[-2000:]


def test_lone_surrogate_text_does_not_disturb_neighbours(cursor):
    sql = "SELECT CAST(N'ab' + NCHAR(55357) + N'cd' AS NVARCHAR(20)) AS v, CAST(N'plain' AS NVARCHAR(20)) AS w"
    cursor.execute(sql)
    bulk = cursor.fetchall()[0]
    assert bulk[1] == "plain"
    cursor.execute(sql)
    assert tuple(cursor.fetchmany(5)[0]) == tuple(bulk)
    cursor.execute(sql)
    try:
        single = tuple(cursor.fetchone())
    except UnicodeDecodeError:
        return  # single-row fetches decode strictly
    assert single[1] == "plain"


def test_padded_char_column_with_fetchone_and_fetchmany(cursor):
    """ANSI_PADDING OFF stores a nullable char(n) trimmed; the driver pads it on fetch."""
    cursor.execute("SET ANSI_PADDING OFF")
    try:
        cursor.execute("CREATE TABLE #p (id INT, c CHAR(12) NULL, v NVARCHAR(30), d DECIMAL(10, 2))")
    finally:
        cursor.execute("SET ANSI_PADDING ON")
    n = 3000
    rows = [(i, None if i % 50 == 0 else "x" * (i % 9), f"text {i}", Decimal(i) / 4) for i in range(n)]
    cursor.executemany("INSERT INTO #p VALUES (?, ?, ?, ?)", rows)
    expected = [(i, None if c is None else c.ljust(12), v, d) for i, c, v, d in rows]

    cursor.execute("SELECT id, c, v, d FROM #p ORDER BY id")
    got = [tuple(cursor.fetchone()) for _ in range(5)]
    got.extend(tuple(r) for r in cursor.fetchmany(7))
    got.append(tuple(cursor.fetchone()))
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        got.append(tuple(row))
    assert got == expected

    cursor.execute("SELECT id, c, v, d FROM #p ORDER BY id")
    assert [tuple(r) for r in cursor.fetchmany(1000)] == expected[:1000]
    assert [tuple(r) for r in cursor.fetchall()] == expected[1000:]
