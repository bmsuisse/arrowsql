"""Scale edges: wide result sets, near-8060-byte rows, the 2100-parameter ceiling,
1 MB query text, and streaming 100k rows with bounded memory.

Upstream ODBC driver is ground truth. Where the upstream ceiling differs from
the naive "2100 server parameters" number, the test asserts the boundary that
upstream actually enforces and the notes in the report explain why.
"""

import gc

import psutil
import pytest

from mssql_python import ProgrammingError

# --- Wide result sets (1024 columns) --------------------------------------

WIDE = 1024


def test_select_1024_literal_columns_returns_every_column(cursor):
    select_list = ", ".join(f"{i} AS c{i}" for i in range(WIDE))
    cursor.execute(f"SELECT {select_list}")
    assert len(cursor.description) == WIDE
    assert [d[0] for d in cursor.description] == [f"c{i}" for i in range(WIDE)]
    row = cursor.fetchone()
    assert len(row) == WIDE
    assert tuple(row) == tuple(range(WIDE))
    assert all(isinstance(v, int) for v in row)


def test_1024_column_table_roundtrip_preserves_column_order(cursor):
    cursor.execute(
        "CREATE TABLE #wide (" + ", ".join(f"c{i} int NULL" for i in range(WIDE)) + ")"
    )
    values = [i * 7 - 3 for i in range(WIDE)]
    placeholders = ", ".join("?" for _ in range(WIDE))
    cursor.execute(f"INSERT INTO #wide VALUES ({placeholders})", values)
    cursor.execute("SELECT * FROM #wide")
    row = cursor.fetchone()
    assert tuple(row) == tuple(values)
    assert row[0] == -3
    assert row[-1] == (WIDE - 1) * 7 - 3


def test_1024_columns_mixed_types_and_nulls(cursor):
    # Alternate int / nvarchar / NULL so a per-column type or offset bug shows up.
    cols = []
    expected = []
    for i in range(WIDE):
        kind = i % 3
        if kind == 0:
            cols.append(f"SELECT {i} AS v{i}")
            expected.append(i)
        elif kind == 1:
            cols.append(f"SELECT N'v{i}' AS v{i}")
            expected.append(f"v{i}")
        else:
            cols.append(f"SELECT CAST(NULL AS int) AS v{i}")
            expected.append(None)
    select_list = ", ".join(c.replace("SELECT ", "", 1) for c in cols)
    cursor.execute(f"SELECT {select_list}")
    row = cursor.fetchone()
    assert len(row) == WIDE
    assert list(row) == expected


def test_1025_column_table_fails_with_programming_error_and_connection_survives(conn):
    cursor = conn.cursor()
    ddl = "CREATE TABLE #too_wide (" + ", ".join(f"c{i} int NULL" for i in range(WIDE + 1)) + ")"
    with pytest.raises(ProgrammingError, match="1024 columns"):
        cursor.execute(ddl)
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42


# --- Rows near the 8060-byte in-row limit ---------------------------------


def test_row_of_8049_data_bytes_plus_overhead_roundtrips_exactly(cursor):
    # id int (4) + varchar(4000) 4000 + varchar(4000) 4000 + varchar(45) 45 = 8049 data bytes.
    cursor.execute(
        "CREATE TABLE #near (id int NOT NULL, a varchar(4000) NULL, "
        "b varchar(4000) NULL, c varchar(45) NULL)"
    )
    a = ("a" * 4000)
    b = ("b" * 3999) + "B"
    c = "c" * 45
    cursor.execute("INSERT INTO #near (id, a, b, c) VALUES (?, ?, ?, ?)", [1, a, b, c])
    cursor.execute("SELECT id, a, b, c, DATALENGTH(a) + DATALENGTH(b) + DATALENGTH(c) FROM #near")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == a
    assert row[2] == b
    assert row[3] == c
    assert row[4] == 8045  # 4000 + 4000 + 45 bytes of varchar payload


def test_varchar_row_over_8060_bytes_roundtrips_via_row_overflow(cursor):
    # Variable-length columns may overflow off-row, so a total row above 8060 bytes
    # is legal as long as each column stays within its own 8000-byte limit.
    cursor.execute(
        "CREATE TABLE #overflow (id int NOT NULL, a varchar(4000) NULL, "
        "b varchar(4000) NULL, c varchar(100) NULL)"
    )
    a = "A" * 4000
    b = "B" * 4000
    c = "C" * 100
    cursor.execute("INSERT INTO #overflow (id, a, b, c) VALUES (?, ?, ?, ?)", [9, a, b, c])
    cursor.execute("SELECT a, b, c, DATALENGTH(a) + DATALENGTH(b) + DATALENGTH(c) FROM #overflow")
    row = cursor.fetchone()
    assert row[0] == a
    assert row[1] == b
    assert row[2] == c
    assert row[3] == 8100


def test_fixed_length_row_over_8060_bytes_fails_at_create_table(conn):
    # Fixed-length columns cannot overflow: min row size is 8071 (8064 data + 7 overhead).
    cursor = conn.cursor()
    ddl = "CREATE TABLE #fixed_too_big (id int NOT NULL, a char(4000) NULL, b char(4000) NULL, c char(60) NULL)"
    with pytest.raises(ProgrammingError, match="8060"):
        cursor.execute(ddl)
    cursor.execute("SELECT 7")
    assert cursor.fetchone()[0] == 7


# --- 2100-parameter ceiling -----------------------------------------------

# Upstream accepts 2097 qmark parameters in one execute and rejects 2098. That is
# below the server's 2100 limit, so the spec's 2099 expectation does not hold here.
MAX_OK_PARAMS = 2097


def test_2097_qmark_parameters_roundtrip_exactly(cursor):
    values = list(range(MAX_OK_PARAMS))
    cursor.execute("SELECT " + ", ".join("?" for _ in values), values)
    row = cursor.fetchone()
    assert len(row) == MAX_OK_PARAMS
    assert tuple(row) == tuple(values)


def test_2101_qmark_parameters_raise_and_connection_survives(conn):
    cursor = conn.cursor()
    # 2098 is msodbcsql's own cap (it adds handle parameters); the port talks TDS directly and
    # the server accepts it, so only 2101 must fail. Either way the connection stays usable.
    values = list(range(2101))
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT " + ", ".join("?" for _ in values), values)
    cursor.execute("SELECT ?", [5])
    assert cursor.fetchone()[0] == 5


def test_2101_qmark_parameters_raise_programming_error(cursor):
    values = list(range(2101))
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT " + ", ".join("?" for _ in values), values)


@pytest.mark.upstream_bug(
    reason=(
        "Upstream reports 2101 parameters as 'COUNT field incorrect or syntax error', "
        "a driver-side string that names no cause. The server's own text is "
        "'The incoming request has too many parameters. The server supports a "
        "maximum of 2100 parameters.'. Upstream also garbles the 2098 text "
        "(word-shuffled per process), so 2101 is the deterministic case."
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_2101_parameter_error_message_names_the_cause(cursor):
    with pytest.raises(ProgrammingError) as info:
        cursor.execute("SELECT " + ", ".join("?" for _ in range(2101)), list(range(2101)))
    message = str(info.value).lower()
    assert "too many parameters" in message
    assert "2100" in message


# --- 1 MB query text ------------------------------------------------------

ONE_MB = 1024 * 1024


def test_1mb_query_text_with_trailing_comment_executes(cursor):
    prefix, suffix = "SELECT 1 AS n /*", "*/"
    sql = prefix + ("x" * ONE_MB) + suffix
    assert len(sql) == ONE_MB + len(prefix) + len(suffix)
    cursor.execute(sql)
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]


def test_1mb_string_literal_inside_query_text_has_exact_length(cursor):
    text = ("0123456789abcdefghijklmnopqrstuvwxyz" * (ONE_MB // 36 + 1))[:ONE_MB]
    assert len(text) == ONE_MB
    sql = f"SELECT LEN(N'{text}') AS n, CAST(RIGHT(N'{text}', 4) AS nvarchar(4)) AS tail"
    assert len(sql) > ONE_MB
    cursor.execute(sql)
    n, tail = cursor.fetchone()
    assert n == ONE_MB
    assert tail == text[-4:]


def test_1mb_parameter_value_roundtrips_and_matches_query_text_length(cursor):
    text = ("ab" * (ONE_MB // 2))
    assert len(text) == ONE_MB
    cursor.execute("SELECT ? AS v, LEN(?) AS n", [text, text])
    v, n = cursor.fetchone()
    assert isinstance(v, str)
    assert v == text
    assert n == ONE_MB


# --- Streaming 100k rows --------------------------------------------------

STREAM_ROWS = 100_000

_STREAM_SQL = (
    "WITH n AS (SELECT 0 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 99999) "
    "SELECT i, CAST(REPLICATE(N'x', 400) AS nvarchar(400)) + CAST(i AS nvarchar(10)) AS s "
    "FROM n OPTION (MAXRECURSION 0)"
)


def test_fetchmany_100k_rows_exact_counts_sums_and_end_of_stream(cursor):
    cursor.execute(_STREAM_SQL)
    batch_sizes = []
    total = 0
    checksum = 0
    first = None
    last = None
    while True:
        batch = cursor.fetchmany(1000)
        if not batch:
            break
        assert len(batch) <= 1000
        batch_sizes.append(len(batch))
        for i, s in batch:
            if first is None:
                first = (i, s)
            last = (i, s)
            checksum += i
            total += 1
    assert total == STREAM_ROWS
    assert batch_sizes == [1000] * 100
    assert checksum == sum(range(STREAM_ROWS))
    assert first == (0, "x" * 400 + "0")
    assert last == (99_999, "x" * 400 + "99999")
    assert cursor.fetchone() is None
    assert cursor.fetchmany(10) == []


def test_fetchmany_100k_rows_memory_growth_is_bounded(cursor):
    # Streaming must not materialize the result set: 100k rows of ~400-char
    # strings would cost well over 100 MB as Python str objects.
    process = psutil.Process()
    cursor.execute(_STREAM_SQL)
    gc.collect()
    baseline = process.memory_info().rss
    peak_growth = 0
    seen = 0
    while True:
        batch = cursor.fetchmany(1000)
        if not batch:
            break
        seen += len(batch)
        del batch
        if seen % 10_000 == 0:
            peak_growth = max(peak_growth, process.memory_info().rss - baseline)
    assert seen == STREAM_ROWS
    assert peak_growth < 50 * 1024 * 1024, f"RSS grew by {peak_growth / 1e6:.1f} MB while streaming"
