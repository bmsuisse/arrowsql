"""executemany column unification: mixed-type cells in one column round-trip exactly.

Every test binds a batch whose cells mix Python types within a single column
and reads the stored values back. Each batch runs in both orders because the
server declares each parameter from the first row, so the first row must not
decide the precision of the rest.
"""

import decimal

import pytest

BIG = 9007199254740993  # 2**53 + 1: the first integer an f64 cannot hold


def _rows(cursor, sql):
    cursor.execute(sql)
    return [tuple(r) for r in cursor.fetchall()]


def _both_orders(values):
    return [list(values), list(reversed(values))]


def _insert(cursor, table, values):
    cursor.executemany(
        f"INSERT INTO {table} VALUES (?, ?)", [(i, v) for i, v in enumerate(values)]
    )


@pytest.mark.parametrize("values", _both_orders([BIG, 0.0, None, -BIG, 2.0]))
def test_bigint_column_keeps_integers_beyond_2_53_next_to_floats(cursor, values):
    cursor.execute("CREATE TABLE #big (id INT NOT NULL, v BIGINT NULL)")
    _insert(cursor, "#big", values)
    got = _rows(cursor, "SELECT v FROM #big ORDER BY id")
    assert got == [(None if v is None else int(v),) for v in values]


@pytest.mark.parametrize("values", _both_orders([1, 1.5, None, 2**53, 70000]))
def test_float_column_mixed_with_small_ints_is_exact(cursor, values):
    cursor.execute("CREATE TABLE #flt (id INT NOT NULL, f FLOAT NULL)")
    _insert(cursor, "#flt", values)
    got = _rows(cursor, "SELECT f FROM #flt ORDER BY id")
    assert got == [(None if v is None else float(v),) for v in values]


@pytest.mark.parametrize(
    "values",
    _both_orders(
        [
            decimal.Decimal("12345678901234567890.1234567890"),
            7,
            0.5,
            None,
            decimal.Decimal("-0.0000000001"),
            BIG,
        ]
    ),
)
def test_numeric_column_mixes_decimal_int_float_and_none_exactly(cursor, values):
    cursor.execute("CREATE TABLE #num (id INT NOT NULL, d NUMERIC(38,10) NULL)")
    _insert(cursor, "#num", values)
    got = _rows(cursor, "SELECT d FROM #num ORDER BY id")
    assert got == [(None if v is None else decimal.Decimal(v),) for v in values]


def test_int_sample_with_decimals_keeps_every_fraction_digit(cursor):
    # An integer sample types the column as an integer, so the Decimals reach the
    # Rust unification raw and must share one DECIMAL scale with the integer.
    cursor.execute("CREATE TABLE #dec (id INT NOT NULL, d NUMERIC(38,10) NULL)")
    values = [7, decimal.Decimal("0.1"), decimal.Decimal("-12.3456")]
    _insert(cursor, "#dec", values)
    assert _rows(cursor, "SELECT d FROM #dec ORDER BY id") == [
        (decimal.Decimal("7"),),
        (decimal.Decimal("0.1"),),
        (decimal.Decimal("-12.3456"),),
    ]


def test_int_widths_mixed_with_none_round_trip(cursor):
    cursor.execute("CREATE TABLE #ints (id INT NOT NULL, v BIGINT NULL)")
    values = [1, 300, 70000, 2**40, None, -(2**31)]
    _insert(cursor, "#ints", values)
    assert _rows(cursor, "SELECT v FROM #ints ORDER BY id") == [(v,) for v in values]


def test_nvarchar_varying_lengths_and_none_round_trip(cursor):
    cursor.execute("CREATE TABLE #nv (id INT NOT NULL, s NVARCHAR(200) NULL)")
    values = ["a", "日本語", None, "😀😀", "x" * 150, ""]
    _insert(cursor, "#nv", values)
    assert _rows(cursor, "SELECT s FROM #nv ORDER BY id") == [(v,) for v in values]


def test_varchar_varying_lengths_and_none_round_trip(cursor):
    cursor.execute("CREATE TABLE #vc (id INT NOT NULL, s VARCHAR(120) NULL)")
    values = ["", "abc", None, "x" * 100]
    _insert(cursor, "#vc", values)
    assert _rows(cursor, "SELECT s FROM #vc ORDER BY id") == [(v,) for v in values]


def test_varbinary_varying_lengths_and_none_round_trip(cursor):
    cursor.execute("CREATE TABLE #vb (id INT NOT NULL, b VARBINARY(64) NULL)")
    values = [b"", b"\x00" * 40, None, b"ab"]
    _insert(cursor, "#vb", values)
    assert _rows(cursor, "SELECT b FROM #vb ORDER BY id") == [(v,) for v in values]


def test_decimal_needing_more_than_38_digits_is_rejected_and_cursor_survives(cursor):
    # 37 integer digits plus 10 fraction digits need precision 47 for one column.
    cursor.execute("CREATE TABLE #over (id INT NOT NULL, d NUMERIC(38,0) NULL)")
    values = [1, decimal.Decimal("9" * 37), decimal.Decimal("0.0000000001")]
    with pytest.raises(ValueError, match="precision"):
        _insert(cursor, "#over", values)
    assert _rows(cursor, "SELECT 1") == [(1,)]
