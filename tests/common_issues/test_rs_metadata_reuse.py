"""A result of the same shape as the previous one reuses its parsed column metadata.

The transport remembers the last COLMETADATA token by its wire bytes and the
statement keeps the column list derived from it. Any difference in the wire
bytes (name, type, length, nullability) must produce fresh metadata.
"""


def _desc(cursor):
    return [(d[0], d[1], d[3], d[4], d[5], d[6]) for d in cursor.description]


def test_repeated_query_keeps_description_and_rows(cursor):
    sql = "SELECT CAST(? AS int) AS a, CAST('x' AS nvarchar(5)) AS b, CAST(1.5 AS decimal(10,2)) AS c"
    seen = None
    for i in range(20):
        cursor.execute(sql, (i,))
        row = cursor.fetchone()
        assert tuple(row) == (i, "x", __import__("decimal").Decimal("1.50"))
        assert row.a == i and row.b == "x"
        if seen is None:
            seen = _desc(cursor)
        assert _desc(cursor) == seen
        assert cursor.fetchone() is None


def test_same_names_different_types_are_not_confused(cursor):
    cursor.execute("SELECT CAST(1 AS int) AS a, CAST('x' AS varchar(3)) AS b")
    first = _desc(cursor)
    assert tuple(cursor.fetchone()) == (1, "x")
    cursor.execute("SELECT CAST(1 AS bigint) AS a, CAST('x' AS varchar(4)) AS b")
    second = _desc(cursor)
    assert tuple(cursor.fetchone()) == (1, "x")
    assert first != second
    cursor.execute("SELECT CAST(1 AS int) AS a, CAST('x' AS varchar(3)) AS b")
    assert _desc(cursor) == first


def test_same_types_different_names_are_not_confused(cursor):
    cursor.execute("SELECT 1 AS a")
    assert cursor.description[0][0] == "a"
    cursor.execute("SELECT 1 AS b")
    assert cursor.description[0][0] == "b"
    assert cursor.fetchone().b == 1
    cursor.execute("SELECT 1 AS a")
    assert cursor.description[0][0] == "a"
    assert cursor.fetchone().a == 1


def test_nullability_difference_is_seen(cursor):
    cursor.execute("CREATE TABLE #m (a int NOT NULL, b int NULL)")
    cursor.execute("INSERT INTO #m VALUES (1, 2)")
    cursor.execute("SELECT a FROM #m")
    nullable_a = cursor.description[0][6]
    cursor.execute("SELECT b AS a FROM #m")
    nullable_b = cursor.description[0][6]
    assert nullable_a != nullable_b


def test_two_cursors_alternating_shapes(conn):
    one, two = conn.cursor(), conn.cursor()
    for i in range(10):
        one.execute("SELECT 1 AS x, 'a' AS y")
        two.execute("SELECT 2.5 AS z")
        assert tuple(one.fetchone()) == (1, "a")
        assert [d[0] for d in two.description] == ["z"]
        assert float(two.fetchone()[0]) == 2.5
        assert [d[0] for d in one.description] == ["x", "y"]
    one.close()
    two.close()


def test_wide_result_over_a_packet_boundary(cursor):
    columns = ", ".join(f"CAST({i} AS int) AS column_with_a_long_name_{i}" for i in range(160))
    for _ in range(3):
        cursor.execute(f"SELECT {columns}")
        row = cursor.fetchone()
        assert list(row) == list(range(160))
        assert len(cursor.description) == 160
