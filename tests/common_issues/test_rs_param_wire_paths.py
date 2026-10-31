"""Parameters of the common types survive the direct-to-packet encoding.

The port writes non-NULL ints, floats, decimals, datetime2(7) and short
strings straight into the packet buffer, and declares them without building a
string per parameter. Whatever path a value takes, it must arrive unchanged,
including when a statement spans several packets.
"""

import datetime as dt
import random
from decimal import Decimal

COLUMNS = "i int, b bigint, f float, d decimal(38,10), t datetime2(7), v varchar(8000), n nvarchar(4000)"

INTS = [0, 1, 255, 256, -1, -128, 32767, -32768, 32768, True, False, 2**31 - 1, -(2**31)]
BIGS = [2**40, -(2**40), 2**63 - 1, -(2**63), 5]
FLOATS = [0.0, 1.5, -1e300, 3.14159, 1e-300]
DECIMALS = ["0", "-0.5", "123456789.1234567890", "-99999999999999999999.99", "0.0000000001", "1"]
VARCHARS = ["", "a", "hello world", "x" * 255, "x" * 256, "y" * 257, "z" * 3000, "é", "naïve", "tab\tq'uote"]
NVARCHARS = ["", "a", "日本語", "ü" * 100, "w" * 4000, "\U0001f600" * 50]


def _row(rng):
    return (
        rng.choice(INTS),
        rng.choice(BIGS),
        rng.choice(FLOATS),
        Decimal(rng.choice(DECIMALS)),
        dt.datetime(rng.randint(1, 9999), rng.randint(1, 12), rng.randint(1, 28), rng.randint(0, 23),
                    rng.randint(0, 59), rng.randint(0, 59), rng.randint(0, 999999)),
        rng.choice(VARCHARS),
        rng.choice(NVARCHARS),
    )


def test_multirow_insert_round_trips(cursor):
    cursor.execute(f"CREATE TABLE #wire ({COLUMNS})")
    rng = random.Random(1)
    for count in (1, 2, 7, 30, 120):
        rows = [_row(rng) for _ in range(count)]
        cursor.execute("TRUNCATE TABLE #wire")
        flat = [value for row in rows for value in row]
        cursor.execute("INSERT INTO #wire VALUES " + ",".join(["(?,?,?,?,?,?,?)"] * count), flat)
        cursor.execute("SELECT i,b,f,d,t,v,n FROM #wire")
        got = sorted((tuple(r) for r in cursor.fetchall()), key=repr)
        expected = sorted(((int(r[0]),) + tuple(r[1:]) for r in rows), key=repr)
        assert got == expected


def test_repeated_statement_with_changing_types(cursor):
    """The same SQL text with different parameter types re-declares them."""
    cursor.execute("CREATE TABLE #wire2 (a sql_variant, b sql_variant)")
    sql = "INSERT INTO #wire2 VALUES (?, ?)" + ", (?, ?)" * 8
    for values in ((1, "x") * 9, (2**40, "ü") * 9, (1.5, Decimal("1.25")) * 9, (1, "x") * 9):
        cursor.execute("TRUNCATE TABLE #wire2")
        cursor.execute(sql, values)
        cursor.execute("SELECT COUNT(*) FROM #wire2")
        assert cursor.fetchone()[0] == 9
