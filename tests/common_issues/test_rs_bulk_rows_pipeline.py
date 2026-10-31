"""cursor.bulkcopy row-tuple pipeline: windowed conversion with the GIL held on
the calling thread, buffer reuse, and decimal / uuid conversion edge cases.

The row iterator must run on the thread that called bulkcopy, one window at a
time, with no read-ahead.
"""

import contextvars
import decimal
import random
import sqlite3
import threading
import uuid

import pytest

from mssql_python import ddbc_bindings

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="native bulk copy is a Rust-port feature",
)

D = decimal.Decimal


class Boom(Exception):
    pass


def test_iterator_raising_in_a_late_window_is_raised_as_is(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(30))")

    def rows():
        for i in range(30_000):
            yield (i, f"row {i}")
        raise Boom("late failure")

    with pytest.raises(Boom, match="^late failure$"):
        cursor.bulkcopy(table, rows())

    cursor.connection.rollback()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1


def test_conversion_error_in_a_late_window(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(30))")

    def rows():
        for i in range(30_000):
            yield (i, f"row {i}")
        yield ("not a number", "x")
        yield (1, "never read")

    with pytest.raises(Exception, match=r"Row 30001: .*not a number"):
        cursor.bulkcopy(table, rows())


def test_wrong_width_in_a_late_window(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(30))")

    def rows():
        for i in range(20_000):
            yield (i, "x")
        yield (1,)

    with pytest.raises(Exception, match="Row 20001 has 1 columns"):
        cursor.bulkcopy(table, rows())


def test_rows_load_in_order_across_many_windows(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(30), d decimal(10, 2))")
    n = 60_000

    result = cursor.bulkcopy(table, [(i, f"row {i}", D(i % 1000) / 4) for i in range(n)], batch_size=0)

    assert result["rows_copied"] == n
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(CAST(a AS bigint)), MAX(a), SUM(d) FROM {table}")
    count, total, last, dsum = cursor.fetchone()
    assert (count, total, last) == (n, sum(range(n)), n - 1)
    assert dsum == sum(D(i % 1000) / 4 for i in range(n))


def test_decimal_edge_cases(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int NOT NULL, v decimal(10, 2) NULL, w decimal(38, 10) NULL)")
    rows = [
        (0, D("-0"), D("-0.0")),
        (1, D("1E+2"), D("1E+20")),
        (2, D("99999999.99"), D("9999999999999999999999999999.9999999999")),
        (3, D("-99999999.99"), D("-9999999999999999999999999999.9999999999")),
        (4, D("1.10"), D("0.0000000005")),
        (5, 0.125, 0.125),
        (6, 2.675, 1e-11),
        (7, "12.5", "1.5E3"),
        (8, 7, 7),
        (9, None, None),
    ]
    cursor.bulkcopy(table, rows)
    cursor.execute(f"SELECT k, v, w FROM {table} ORDER BY k")
    got = [tuple(r) for r in cursor.fetchall()]
    assert got[0][1] == 0 and got[0][2] == 0
    assert got[1][1:] == (D("100.00"), D("100000000000000000000.0000000000"))
    assert got[2][1:] == (D("99999999.99"), D("9999999999999999999999999999.9999999999"))
    assert got[3][1:] == (D("-99999999.99"), D("-9999999999999999999999999999.9999999999"))
    assert got[4][1:] == (D("1.10"), D("0.0000000005"))
    assert got[5][1:] == (D("0.12"), D("0.1250000000"))  # float: half to even
    assert got[6][1:] == (D("2.68"), D("0E-10"))  # the float's repr "2.675" rounds half to even
    assert got[7][1:] == (D("12.50"), D("1500.0000000000"))
    assert got[8][1:] == (D("7.00"), D("7.0000000000"))
    assert got[9][1:] == (None, None)


@pytest.mark.parametrize(
    "value, message",
    [
        (D("1.234"), r"Input decimal scale exceeds target scale 2 of column 'v'"),
        (D("100000000.00"), r"Decimal value exceeds target precision 10 of column 'v'"),
        ("1.2.3", r"Invalid decimal value '1\.2\.3' for column 'v'"),
    ],
)
def test_decimal_errors_keep_their_messages(cursor, table_name, value, message):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (v decimal(10, 2) NULL)")
    with pytest.raises(Exception, match=message):
        cursor.bulkcopy(table, [(D("1.00"),), (value,)])


def test_decimal_subclass_and_repeated_values(cursor, table_name):
    class MyDecimal(D):
        pass

    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, v decimal(10, 2))")
    values = [MyDecimal("3.50"), D("3.50"), MyDecimal("3.5"), D("-3.50")] * 5000
    cursor.bulkcopy(table, [(i, v) for i, v in enumerate(values)])
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(v) FROM {table}")
    count, total = cursor.fetchone()
    assert (count, total) == (20_000, D("3.50") * 15_000 - D("3.50") * 5_000)


def test_random_uuids_roundtrip(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, g uniqueidentifier)")
    rng = random.Random(7)
    guids = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(2000)]
    guids[0] = uuid.UUID(int=0)
    guids[1] = uuid.UUID(int=2**128 - 1)

    cursor.bulkcopy(table, [(i, g if i % 2 else str(g)) for i, g in enumerate(guids)])

    cursor.execute(f"SELECT k, g FROM {table} ORDER BY k")
    got = [uuid.UUID(str(g)) for _, g in cursor.fetchall()]
    assert got == guids



def test_iterator_runs_on_the_calling_thread(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")
    var = contextvars.ContextVar("bulk_marker", default="unset")
    var.set("caller")
    caller = threading.current_thread()
    seen = set()

    def rows():
        for i in range(12_000):
            seen.add((threading.current_thread(), var.get(), threading.get_ident()))
            yield (i,)

    cursor.bulkcopy(table, rows())

    assert {(t, v) for t, v, _ in seen} == {(caller, "caller")}
    assert {ident for _, _, ident in seen} == {threading.get_ident()}


def test_thread_bound_state_is_visible_to_the_iterator(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int)")
    local = threading.local()
    local.base = 100
    lock = threading.RLock()

    def rows():
        for i in range(5000):
            with lock:  # re-entrant: only works on the thread that holds it
                yield (local.base + i,)

    with lock:
        cursor.bulkcopy(table, rows())
    cursor.execute(f"SELECT COUNT_BIG(*), MIN(a) FROM {table}")
    assert tuple(cursor.fetchone()) == (5000, 100)


def test_sqlite3_cursor_as_row_source(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s nvarchar(20), f float)")
    db = sqlite3.connect(":memory:")  # check_same_thread=True: any other thread raises
    db.execute("CREATE TABLE t (a INTEGER, s TEXT, f REAL)")
    db.executemany("INSERT INTO t VALUES (?, ?, ?)", [(i, f"s{i}", i / 2) for i in range(20_000)])
    result = cursor.bulkcopy(table, db.execute("SELECT a, s, f FROM t ORDER BY a"))
    assert result["rows_copied"] == 20_000
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(CAST(a AS bigint)), MAX(a) FROM {table}")
    assert tuple(cursor.fetchone()) == (20_000, sum(range(20_000)), 19_999)
    db.close()


def test_no_rows_consumed_beyond_a_window_after_a_server_failure(cursor, table_name):
    """A duplicate key fails the load at the server; the source is not drained further."""
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int NOT NULL PRIMARY KEY)")
    consumed = []

    def rows():
        for i in range(5_000_000):
            consumed.append(i)
            yield (0 if i == 1 else i,)

    with pytest.raises(Exception, match="(?i)duplicate|primary key"):
        cursor.bulkcopy(table, rows(), batch_size=1000)

    # One window is 4096 rows; the first window already holds the duplicate. (Without a
    # batch size the server reports the failure only when the stream ends.)
    assert len(consumed) <= 2 * 4096, len(consumed)


def test_buffers_are_reused_across_many_windows(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (a int, s varchar(40), d decimal(12, 3), g uniqueidentifier)")
    n = 100_000
    g = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
    cursor.bulkcopy(table, ((i, "v" * (i % 40), D(i) / 8, g) for i in range(n)), batch_size=0)
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(CAST(a AS bigint)), SUM(LEN(s)), SUM(d) FROM {table}")
    count, total, lens, dsum = cursor.fetchone()
    assert (count, total) == (n, sum(range(n)))
    assert lens == sum(i % 40 for i in range(n))
    assert dsum == sum(D(i) / 8 for i in range(n))


def test_uuid_edge_values_and_forms(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, g uniqueidentifier)")
    base = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
    forms = [
        base,
        str(base),
        str(base).upper(),
        "{" + str(base) + "}",
        base.hex,
        uuid.UUID(int=0),
        uuid.UUID(int=2**128 - 1),
        uuid.UUID(int=1),
        uuid.UUID(int=1 << 127),
    ]
    cursor.bulkcopy(table, list(enumerate(forms)))
    cursor.execute(f"SELECT k, g FROM {table} ORDER BY k")
    got = [uuid.UUID(str(g)) for _, g in cursor.fetchall()]
    want = [base] * 5 + [uuid.UUID(int=0), uuid.UUID(int=2**128 - 1), uuid.UUID(int=1), uuid.UUID(int=1 << 127)]
    assert got == want


def test_uuid_rejects_wrong_types(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (g uniqueidentifier)")
    with pytest.raises(Exception, match="(?i)uniqueidentifier|guid"):
        cursor.bulkcopy(table, [(12345,)])


def test_decimal_negative_zero_and_exponent_forms(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, v decimal(10, 2), w decimal(38, 0))")
    rows = [
        (0, D("-0"), D("-0")),
        (1, D("-0.00"), D("0E+5")),
        (2, D("1E+2"), D("1E+37")),
        (3, D("1.5E+1"), D("-1E+37")),
        (4, D("0.1E+1"), D("12345678901234567890123456789012345678")),
    ]
    cursor.bulkcopy(table, rows)
    cursor.execute(f"SELECT k, v, w FROM {table} ORDER BY k")
    got = [tuple(r) for r in cursor.fetchall()]
    assert got[0][1:] == (0, 0)
    assert got[1][1:] == (0, 0)
    assert got[2][1:] == (D("100.00"), D(10) ** 37)
    assert got[3][1:] == (D("15.00"), -(D(10) ** 37))
    assert got[4][1:] == (D("1.00"), D("12345678901234567890123456789012345678"))


def test_decimal_max_precision_and_overflow(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (v decimal(38, 0))")
    top = D("9" * 38)
    bottom = D("-" + "9" * 38)  # unary minus would round to the context's 28 digits
    cursor.bulkcopy(table, [(top,), (bottom,)])
    cursor.execute(f"SELECT v FROM {table} ORDER BY v")
    assert [r[0] for r in cursor.fetchall()] == [bottom, top]
    with pytest.raises(Exception, match=r"Decimal value exceeds target precision 38 of column 'v'"):
        cursor.bulkcopy(table, [(D("1" + "0" * 38),)])


def test_decimal_extra_scale_message_is_unchanged(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (v decimal(10, 2))")
    for value in (D("1.234"), "1.234", D("0.001")):
        with pytest.raises(Exception, match=r"Input decimal scale exceeds target scale 2 of column 'v'"):
            cursor.bulkcopy(table, [(value,)])
    # trailing zeros beyond the scale carry no information and are accepted
    cursor.bulkcopy(table, [(D("1.2300"),), ("4.5000",)])
    cursor.execute(f"SELECT v FROM {table} ORDER BY v")
    assert [r[0] for r in cursor.fetchall()] == [D("1.23"), D("4.50")]


def test_decimal_float_rounds_half_to_even(cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (k int, v decimal(10, 2))")
    values = [0.125, 0.375, 2.675, 1.005, -0.125, -2.5e-3, 1e-9, 12345.675, 7.0]
    cursor.bulkcopy(table, list(enumerate(values)))
    cursor.execute(f"SELECT v FROM {table} ORDER BY k")
    got = [r[0] for r in cursor.fetchall()]
    want = [D(repr(v)).quantize(D("0.01"), rounding=decimal.ROUND_HALF_EVEN) for v in values]
    assert got == want
