"""Agent mode (``mssql_python.agent``): bounded typed text previews and JSON errors.

Rust-port only: the renderer is ``ddbc_bindings.AgentRender``. Covers the output
contract (typed header, strict byte ceiling, row/cell limits, escaping, footer
that never claims an unknown total), every SQL Server type, multiple result
sets, DML row counts, server messages, error categories, and the CLI.
"""

import json
import os
import re
import subprocess
import sys

import pytest
from mssql_python import ddbc_bindings
from mssql_python.agent import error_json, query, query_cursor, render_cursor
from mssql_python.exceptions import DatabaseError, OperationalError, ProgrammingError

pytestmark = pytest.mark.skipif(
    not hasattr(ddbc_bindings, "AgentRender"), reason="agent mode is Rust-port only"
)

NUMBERS = (
    "SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
    "FROM sys.all_columns a CROSS JOIN sys.all_columns b"
)


def numbers(n):
    return NUMBERS.format(n=n)


def footer(text):
    """The footer is the last line (a one-column result of empty strings has blank data lines)."""
    return text.rstrip("\n").rsplit("\n", 1)[-1]


def lines(text):
    return text.split("\n")


def is_error(text):
    try:
        return isinstance(json.loads(text), dict)
    except ValueError:
        return False


# --- output contract --------------------------------------------------------


def test_typed_tsv_header_rows_and_footer(conn_str):
    out = query(conn_str, "SELECT 1 AS answer, N'x' AS label UNION ALL SELECT 2, N'y'")
    assert out == (
        "answer:int\tlabel:nvarchar(1)\n1\tx\n2\ty\n\nrows_shown=2; complete=true; total_rows=2"
    )


def test_markdown_table_quotes_text(conn_str):
    out = query(conn_str, "SELECT 1 AS a, N'x' AS b, NULL AS c", format="markdown")
    assert lines(out)[:3] == ["|a (int)|b (nvarchar(1))|c (int)|", "|-|-|-|", '|1|"x"|NULL|']
    assert footer(out) == "rows_shown=1; complete=true; total_rows=1"


def test_null_differs_from_the_string_null_in_both_layouts(conn_str):
    sql = "SELECT CAST(NULL AS nvarchar(10)) AS s UNION ALL SELECT N'NULL' UNION ALL SELECT N'\\N'"
    tsv = lines(query(conn_str, sql))
    assert tsv[1:4] == ["\\N", "NULL", "\\\\N"]
    md = lines(query(conn_str, sql, format="markdown"))
    assert md[2:5] == ["|NULL|", '|"NULL"|', '|"\\\\N"|']


def test_empty_result_keeps_the_typed_header(conn_str):
    out = query(conn_str, "SELECT 1 AS a, N'x' AS b WHERE 1 = 0")
    assert out == "a:int\tb:nvarchar(1)\n\nrows_shown=0; complete=true; total_rows=0"


def test_duplicate_and_unnamed_columns_stay_in_position(conn_str):
    out = query(conn_str, "SELECT 1 AS a, 2 AS a, 3")
    assert lines(out)[0] == "a:int\ta:int\t:int"
    assert lines(out)[1] == "1\t2\t3"


def test_tsv_cells_cannot_break_rows(conn_str):
    value = "a\tb\nc\rd\\e|f`g\x01h\x7fi"
    out = query(conn_str, "SELECT ? AS v, 1 AS k", [value])
    body = lines(out)
    assert body[0] == "v:nvarchar(4000)\tk:int" or body[0].startswith("v:")
    assert body[1] == "a\\tb\\nc\\rd\\\\e|f`g\\u0001h\\u007fi\t1"
    assert len(body) == 4  # header, row, blank, footer


def test_markdown_escapes_pipes_and_html(conn_str):
    out = query(conn_str, "SELECT N'a|b <i>&*_[x]~`' AS v", format="markdown")
    assert lines(out)[2] == '|"a\\|b &lt;i&gt;&amp;\\*\\_\\[x\\]\\~\\`"|'


def test_empty_string_is_an_empty_field(conn_str):
    out = query(conn_str, "SELECT N'' AS a, N'x' AS b")
    assert lines(out)[1] == "\tx"


def test_unicode_cells_are_clipped_on_character_boundaries(conn_str):
    out = query(conn_str, "SELECT REPLICATE(N'é🦀', 50) AS v", max_cell_chars=5)
    row = lines(out)[1]
    assert row == "é🦀é🦀é…[truncated]"
    row.encode("utf-8")
    assert "cells_truncated=true" in footer(out) and "complete=false" in footer(out)


def test_clipping_by_bytes_never_splits_a_character(conn_str):
    for budget in range(256, 420):
        out = query(conn_str, "SELECT REPLICATE(N'🦀', 400) AS v", max_bytes=budget, max_cell_chars=10_000)
        out.encode("utf-8")
        assert len(out.encode("utf-8")) <= budget


@pytest.mark.parametrize("fmt", ["tsv", "markdown"])
@pytest.mark.parametrize("budget", [256, 300, 512, 1000, 2500, 10_000])
def test_whole_output_fits_the_byte_ceiling(conn_str, fmt, budget):
    sql = "SELECT n, REPLICATE(N'é|\t🦀', n % 20) AS txt, CAST(n AS decimal(18,4)) AS d FROM (" + numbers(500) + ") t"
    out = query(conn_str, sql, format=fmt, max_bytes=budget)
    assert len(out.encode("utf-8")) <= budget
    if not is_error(out) and "Result schema exceeds" not in out:
        assert "rows_shown=" in footer(out)


def test_row_cap_stops_without_claiming_a_total(conn_str):
    out = query(conn_str, numbers(100), max_rows=5)
    assert lines(out)[1:6] == ["1", "2", "3", "4", "5"]
    assert footer(out) == "rows_shown=5; complete=false; total_rows>=5; next=filter/aggregate SQL or raise budgets"


def test_exact_row_boundary_is_complete_only_after_exhaustion(conn_str):
    out = query(conn_str, numbers(5), max_rows=5)
    assert footer(out) == "rows_shown=5; complete=true; total_rows=5"
    out = query(conn_str, numbers(6), max_rows=5)
    assert "complete=false" in footer(out) and "total_rows>=5" in footer(out)


def test_byte_cap_drops_whole_rows_and_marks_incomplete(conn_str):
    out = query(conn_str, numbers(1000), max_bytes=400)
    assert len(out.encode("utf-8")) <= 400
    rows = [x for x in lines(out)[1:] if x.isdigit()]
    assert 0 < len(rows) < 1000
    assert rows == [str(i) for i in range(1, len(rows) + 1)]
    assert "complete=false" in footer(out) and "total_rows>=" in footer(out)


def test_head_and_tail_sample(conn_str):
    out = query(conn_str, numbers(100), max_rows=10, tail_rows=3)
    body = lines(out)
    assert body[1:8] == ["1", "2", "3", "4", "5", "6", "7"]
    assert body[8] == "… 90 rows omitted …"
    assert body[9:12] == ["98", "99", "100"]
    assert footer(out) == (
        "rows_shown=10; complete=false; total_rows=100; sample=head+tail; rows_omitted=90"
        "; next=filter/aggregate SQL or raise budgets"
    )


def test_tail_sample_of_a_short_result_is_the_plain_result(conn_str):
    out = query(conn_str, numbers(5), max_rows=10, tail_rows=3)
    assert footer(out) == "rows_shown=5; complete=true; total_rows=5"


def test_huge_cells_stay_bounded(conn_str):
    out = query(conn_str, "SELECT REPLICATE(CAST(N'ab' AS nvarchar(max)), 2000000) AS v, CAST(REPLICATE(CAST('x' AS varchar(max)), 3000000) AS varbinary(max)) AS b")
    assert len(out.encode("utf-8")) <= 10_000
    row = lines(out)[1]
    assert row.count("…[truncated]") == 2
    assert row.split("\t")[1].startswith("0x78787878")


def test_budgets_are_validated_without_running_sql(conn_str):
    for kwargs in [{"max_bytes": 255}, {"max_rows": 0}, {"max_cell_chars": 0}, {"format": "csv"}, {"timeout_s": -1}]:
        out = query(conn_str, "SELECT 1/0", **kwargs)
        assert json.loads(out)["category"] == "configuration"


# --- types -------------------------------------------------------------------

TYPES = [
    ("CAST(255 AS tinyint)", "tinyint", "255"),
    ("CAST(-32768 AS smallint)", "smallint", "-32768"),
    ("CAST(-2147483648 AS int)", "int", "-2147483648"),
    ("CAST(-9223372036854775808 AS bigint)", "bigint", "-9223372036854775808"),
    ("CAST(1 AS bit)", "bit", "1"),
    ("CAST(0 AS bit)", "bit", "0"),
    ("CAST(1.5 AS real)", "real", "1.5"),
    ("CAST(0.1 AS float)", "float", "0.1"),
    ("CAST(12345.6789 AS decimal(18,4))", "decimal(18,4)", "12345.6789"),
    ("CAST(-0.05 AS decimal(5,2))", "decimal(5,2)", "-0.05"),
    ("CAST(7 AS numeric(10,0))", "numeric(10,0)", "7"),
    ("CAST(-123.4500 AS money)", "money", "-123.4500"),
    ("CAST(5.25 AS smallmoney)", "smallmoney", "5.2500"),
    ("CAST('2024-02-29' AS date)", "date", "2024-02-29"),
    ("CAST('0001-01-01' AS date)", "date", "0001-01-01"),
    ("CAST('13:14:15.123' AS time(3))", "time(3)", "13:14:15.123"),
    ("CAST('13:14:15' AS time(0))", "time(0)", "13:14:15"),
    ("CAST('2024-03-04T05:06:07.123' AS datetime)", "datetime", "2024-03-04 05:06:07.123"),
    ("CAST('2024-03-04T05:07:00' AS smalldatetime)", "smalldatetime", "2024-03-04 05:07:00"),
    ("CAST('2024-03-04T05:06:07.123456' AS datetime2(6))", "datetime2(6)", "2024-03-04 05:06:07.123456"),
    ("CAST('2024-03-04T05:06:07' AS datetime2(0))", "datetime2(0)", "2024-03-04 05:06:07"),
    (
        "CAST('2024-03-04T05:06:07.5+02:00' AS datetimeoffset(1))",
        "datetimeoffset(1)",
        "2024-03-04 03:06:07.5+00:00",  # normalised to UTC
    ),
    ("CAST('ab' AS char(4))", "char(4)", "ab  "),
    ("CAST('ab' AS varchar(10))", "varchar(10)", "ab"),
    ("CAST('ab' AS varchar(max))", "varchar(max)", "ab"),
    ("CAST(N'é🦀' AS nchar(5))", "nchar(5)", "é🦀  "),
    ("CAST(N'é🦀' AS nvarchar(10))", "nvarchar(10)", "é🦀"),
    ("CAST(N'é🦀' AS nvarchar(max))", "nvarchar(max)", "é🦀"),
    ("CAST(0xCAFE AS binary(4))", "binary(4)", "0xcafe0000"),
    ("CAST(0xCAFE AS varbinary(8))", "varbinary(8)", "0xcafe"),
    ("CAST(0xCAFE AS varbinary(max))", "varbinary(max)", "0xcafe"),
    ("CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS uniqueidentifier)", "uniqueidentifier", "6F9619FF-8B86-D011-B42D-00C04FC964FF"),
    ("CAST('<r a=\"1\"><c/></r>' AS xml)", "xml", '<r a="1"><c/></r>'),
    ("CAST(NULL AS int)", "int", "\\N"),
    ("CAST(NULL AS datetimeoffset)", "datetimeoffset(7)", "\\N"),
    ("CAST(NULL AS varbinary(4))", "varbinary(4)", "\\N"),
    # sql_variant renders the value by its own base type.
    ("CAST(42 AS sql_variant)", "sql_variant", "42"),
    ("CAST(N'é' AS sql_variant)", "sql_variant", "é"),
    ("CAST('abc' AS sql_variant)", "sql_variant", "abc"),
    ("CAST(12.50 AS sql_variant)", "sql_variant", "12.50"),
    ("CAST(CAST('2024-03-04' AS date) AS sql_variant)", "sql_variant", "2024-03-04"),
    ("CAST(CAST('2024-03-04T05:06:07' AS datetime2(3)) AS sql_variant)", "sql_variant", "2024-03-04 05:06:07"),
    ("CAST(CAST('05:06:07.5' AS time(1)) AS sql_variant)", "sql_variant", "05:06:07.500"),
    ("CAST(CAST(0xBEEF AS varbinary(4)) AS sql_variant)", "sql_variant", "0xbeef"),
    ("CAST(CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS uniqueidentifier) AS sql_variant)", "sql_variant", "6F9619FF-8B86-D011-B42D-00C04FC964FF"),
    ("CAST(NULL AS sql_variant)", "sql_variant", "\\N"),
]


@pytest.mark.parametrize("expr,type_name,text", TYPES, ids=[t[0][:48] for t in TYPES])
def test_every_sql_server_type_renders_sensibly(conn_str, expr, type_name, text):
    out = query(conn_str, f"SELECT {expr} AS v")
    assert lines(out)[0] == f"v:{type_name}", out
    assert lines(out)[1] == text


def test_text_and_ntext_legacy_types(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (t text, n ntext, i image)")
    cursor.execute(f"INSERT INTO {table_name} VALUES ('plain', N'wide', 0x0102)")
    cursor.connection.commit()
    out = query(cursor.connection, f"SELECT t, n, i FROM {table_name}")
    assert lines(out)[:2] == ["t:text\tn:ntext\ti:image", "plain\twide\t0x0102"]


def test_float_edge_values(conn_str):
    out = query(conn_str, "SELECT CAST(1e300 AS float) AS big, CAST(-0.0 AS float) AS z, CAST(3.4e38 AS real) AS r")
    assert lines(out)[0] == "big:float\tz:float\tr:real"
    row = lines(out)[1].split("\t")
    assert float(row[0]) == 1e300 and float(row[1]) == 0.0 and float(row[2]) > 3e38


# --- batches, rowcounts, messages ---------------------------------------------


def test_multiple_result_sets_share_one_budget(conn_str):
    out = query(conn_str, "SELECT 1 AS a; SELECT N'x' AS b, 2 AS c; SELECT 3 AS d")
    assert out == (
        "a:int\n1\n\nrows_shown=1; complete=true; total_rows=1\n"
        "b:nvarchar(1)\tc:int\nx\t2\n\nresult_set=2; rows_shown=1; complete=true; total_rows=1\n"
        "d:int\n3\n\nresult_set=3; rows_shown=1; complete=true; total_rows=1"
    )


def test_many_result_sets_stay_inside_the_ceiling(conn_str):
    sql = "; ".join(f"SELECT {i} AS v" for i in range(60))
    out = query(conn_str, sql, max_bytes=1500)
    assert len(out.encode("utf-8")) <= 1500
    assert "more parts omitted" in lines(out)[-1]


def test_dml_reports_rows_affected(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (x int)")
    cursor.connection.commit()
    out = query(cursor.connection, f"INSERT INTO {table_name} VALUES (1), (2), (3)")
    assert out == "-- rows_affected=3"
    out = query(cursor.connection, f"UPDATE {table_name} SET x = x + 1 WHERE x > 1; SELECT COUNT(*) AS n FROM {table_name}")
    assert lines(out)[0] == "-- rows_affected=2"
    assert lines(out)[1:3] == ["n:int", "3"]
    assert query(cursor.connection, f"DELETE FROM {table_name} WHERE 1 = 0") == "-- rows_affected=0"


def test_statement_without_result_or_count(conn_str):
    assert query(conn_str, "SET NOCOUNT ON; DECLARE @x int = 1") == "-- no result set"


def test_print_and_low_severity_raiserror_are_reported(conn_str):
    out = query(conn_str, "PRINT 'hello'; RAISERROR('careful %d', 10, 1, 7) WITH NOWAIT; SELECT 1 AS a")
    assert lines(out)[0] == "-- message: hello"
    assert lines(out)[1] == "-- message: (50000) careful 7"
    assert lines(out)[2:4] == ["a:int", "1"]


def test_trailing_print_after_the_last_result_set(conn_str):
    out = query(conn_str, "SELECT 1 AS a; PRINT 'bye'")
    assert lines(out)[-1] == "-- message: bye"


def test_long_messages_are_clipped_and_escaped(conn_str):
    out = query(conn_str, "PRINT REPLICATE(CAST('x\ty' AS varchar(8000)), 1000)")
    assert len(out) < 700 and out.endswith("…[truncated]") and "\t" not in out


def test_params_bound_with_question_marks(conn_str):
    out = query(conn_str, "SELECT ? AS a, ? AS b", (7, "seven"))
    assert lines(out)[1] == "7\tseven"


def test_accepts_connections_and_cursors(conn):
    assert lines(query(conn, "SELECT 1 AS a"))[1] == "1"
    cur = conn.cursor()
    assert lines(query_cursor(cur, "SELECT 2 AS a"))[1] == "2"
    cur.execute("SELECT 3 AS a")
    assert lines(render_cursor(cur))[1] == "3"
    cur.execute("SELECT 4 AS a")  # the cursor stays usable
    assert cur.fetchone()[0] == 4
    cur.close()


def test_a_truncated_first_result_leaves_later_ones_unread(conn_str):
    out = query(conn_str, numbers(50) + "; SELECT 99 AS later", max_rows=3)
    assert "later" not in out and "complete=false" in footer(out)


# --- errors ------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql,number,category",
    [
        ("SELECT * FROM no_such_table_xyz", 208, "missing_table"),
        ("SELECT no_such_col FROM sys.objects", 207, "missing_column"),
        ("SELECT FROM sys.objects", 156, "syntax_error"),
        ("SELECT 1 FROM", 102, "syntax_error"),
        ("EXEC no_such_proc_xyz", 2812, "missing_function"),
        ("SELECT dbo.no_such_fn_xyz(1)", 4121, "execution_error"),
        ("SELECT 1/0", 8134, "arithmetic_error"),
        ("SELECT CAST('x' AS int)", 245, "conversion_error"),
        ("SELECT CAST(300 AS tinyint)", 220, "conversion_error"),
        ("SELECT CAST(1e10 AS int)", 232, "conversion_error"),
        ("RAISERROR('nope', 16, 1)", 50000, "execution_error"),
        ("USE no_such_db_xyz", 911, "missing_database"),
        ("DROP TABLE no_such_table_xyz", 3701, "missing_table"),
    ],
)
def test_sql_errors_have_categories_and_keep_the_server_message(conn_str, sql, number, category):
    out = query(conn_str, sql)
    value = json.loads(out)
    assert value["category"] == category, out
    assert value["native_error"] == number
    assert value["next_step"] and value["retryable"] in (True, False)
    assert value["sqlstate"]
    assert "[Microsoft]" not in value["message"] and value["message"]


def test_server_message_is_preserved_verbatim(conn_str):
    value = json.loads(query(conn_str, "SELECT * FROM dbo.Orders_typo"))
    assert value["message"] == "Invalid object name 'dbo.Orders_typo'."


def test_failure_after_earlier_rows_gives_error_not_success(conn_str):
    out = query(conn_str, "SELECT 1 AS a UNION ALL SELECT 1/0")
    assert is_error(out) and "rows_shown" not in out
    assert json.loads(out)["native_error"] == 8134
    out = query(conn_str, "SELECT 1 AS a; SELECT 1/0 AS b")
    assert is_error(out) and "rows_shown" not in out


def test_failed_statement_is_never_rewritten_or_retried(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE {table_name} (x int)")
    cursor.connection.commit()
    out = query(conn_str, f"INSERT INTO {table_name}(x) VALUES (1); SELECT * FROM {table_name}_typo")
    assert json.loads(out)["category"] == "missing_table"
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert cursor.fetchone()[0] == 1  # exactly one execution


def test_login_failure_is_authentication(conn_str):
    bad = re.sub(r"(?i)(pwd|password)=[^;]*", r"\1=definitely-not-the-password", conn_str)
    value = json.loads(query(bad, "SELECT 1"))
    assert value["category"] == "authentication"
    assert value["native_error"] == 18456
    assert "definitely-not-the-password" not in json.dumps(value)


def test_query_timeout_is_a_timeout_error(conn_str):
    value = json.loads(query(conn_str, "WAITFOR DELAY '00:00:10'", timeout_s=1))
    assert value["category"] == "timeout" and value["retryable"] is True


def test_deadlock_between_two_sessions(conn_str, cursor, table_name):
    import threading

    cursor.execute(f"CREATE TABLE {table_name} (id int primary key, v int)")
    cursor.execute(f"INSERT INTO {table_name} VALUES (1, 0), (2, 0)")
    cursor.connection.commit()
    ready = threading.Barrier(2)
    results = []

    def worker(first, second):
        sql = (
            "SET LOCK_TIMEOUT 20000; BEGIN TRAN; "
            f"UPDATE {table_name} SET v = 1 WHERE id = {first}; WAITFOR DELAY '00:00:01'; "
            f"UPDATE {table_name} SET v = 1 WHERE id = {second}; COMMIT"
        )
        ready.wait()
        results.append(query(conn_str, sql, timeout_s=30))

    threads = [threading.Thread(target=worker, args=ids) for ids in [(1, 2), (2, 1)]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    errors = [json.loads(r) for r in results if is_error(r)]
    assert [e["category"] for e in errors] == ["deadlock"]
    assert errors[0]["retryable"] is True and errors[0]["native_error"] == 1205


class Fake(DatabaseError):
    def __init__(self, message, sqlstate=None, native_error=None):
        super().__init__("Driver Error", message, sqlstate, native_error)


@pytest.mark.parametrize(
    "number,state,category",
    [
        (208, "42S02", "missing_table"),
        (207, "42S22", "missing_column"),
        (102, "42000", "syntax_error"),
        (156, "42000", "syntax_error"),
        (18456, "28000", "authentication"),
        (1205, "40001", "deadlock"),
        (1222, "42000", "lock_timeout"),
        (229, "42000", "permission_denied"),
        (262, "42000", "permission_denied"),
        (2627, "23000", "constraint_violation"),
        (547, "23000", "constraint_violation"),
        (515, "23000", "constraint_violation"),
        (8152, "22001", "data_truncation"),
        (2714, "42S01", "already_exists"),
        (40613, "42000", "transient"),
        (None, "HYT00", "timeout"),
        (None, "08S01", "connection"),
        (None, "08001", "connection"),
        (9999, "42000", "execution_error"),
        (None, None, "execution_error"),
    ],
)
def test_error_categories_by_number_and_sqlstate(number, state, category):
    exc = Fake("[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]server text", state, number)
    value = json.loads(error_json(exc))
    assert value["category"] == category
    assert value["message"] == "server text"
    assert value.get("native_error") == number
    assert value.get("sqlstate") == state


def test_error_json_respects_the_byte_ceiling_and_keeps_the_prefix():
    message = "Invalid object name '" + "é🦀" * 5000 + "'."
    for budget in (256, 300, 1000, 10_000):
        out = error_json(Fake(message, "42S02", 208), budget)
        assert len(out.encode("utf-8")) <= budget
        value = json.loads(out)
        assert message.startswith(value["message"])
        assert value["truncated"] is True and value["category"] == "missing_table"


def test_driver_errors_are_actionable():
    value = json.loads(error_json(ProgrammingError("Driver Error", "", None, None)))
    assert value["category"] == "execution_error" and value["message"]
    value = json.loads(error_json(OperationalError("Communication link failure", "", "08S01", None)))
    assert value["category"] == "connection"


# --- CLI ---------------------------------------------------------------------


def run_cli(conn_str, *args, stdin=None, env_name="MSSQL_CONNECTION_STRING", module=True):
    env = {k: v for k, v in os.environ.items() if k not in ("MSSQL_CONNECTION_STRING", "DB_CONNECTION_STRING")}
    if env_name:
        env[env_name] = conn_str
    command = [sys.executable, "-m", "mssql_python.agent"] if module else list(args)
    return subprocess.run(
        command + (list(args) if module else []),
        input=stdin,
        capture_output=True,
        env=env,
        timeout=120,
    )


def test_cli_default_is_agent_tsv(conn_str):
    done = run_cli(conn_str, "-c", "SELECT 1 AS answer")
    assert done.returncode == 0 and done.stderr == b""
    assert done.stdout == b"answer:int\n1\n\nrows_shown=1; complete=true; total_rows=1"


def test_cli_accepts_sql_as_argument_flags_alias_and_markdown(conn_str):
    for args in (["SELECT 1 AS a"], ["--sql", "SELECT 1 AS a"], ["--command", "SELECT 1 AS a", "--agent"]):
        assert run_cli(conn_str, *args).stdout.startswith(b"a:int\n1")
    out = run_cli(conn_str, "-c", "SELECT 1 AS a", "--format", "markdown").stdout
    assert out.startswith(b"|a (int)|\n|-|\n|1|")


def test_cli_reads_sql_files_and_stdin(conn_str, tmp_path):
    path = tmp_path / "q.sql"
    path.write_bytes("SELECT N'é🦀' AS v".encode("utf-8"))
    assert run_cli(conn_str, "--file", str(path)).stdout.startswith("v:nvarchar(3)\né🦀".encode("utf-8"))
    assert run_cli(conn_str, stdin=b"SELECT 5 AS five").stdout.startswith(b"five:int\n5")
    assert run_cli(conn_str, "-f", "-", stdin=b"SELECT 6 AS six").stdout.startswith(b"six:int\n6")


def test_cli_connection_string_from_option_or_either_env_var(conn_str):
    assert run_cli(conn_str, "-c", "SELECT 1 AS a", env_name="DB_CONNECTION_STRING").returncode == 0
    done = run_cli(conn_str, "--connection-string", conn_str, "-c", "SELECT 1 AS a", env_name=None)
    assert done.returncode == 0 and done.stdout.startswith(b"a:int")


def test_cli_caps_stdout_bytes_and_row_count(conn_str):
    done = run_cli(conn_str, "-c", numbers(5000), "--max-bytes", "600", "--max-rows", "50")
    assert done.returncode == 0 and len(done.stdout) <= 600
    assert b"complete=false" in done.stdout
    done = run_cli(conn_str, "-c", numbers(100), "--max-rows", "10", "--tail-rows", "2")
    assert b"sample=head+tail" in done.stdout


def test_cli_sql_error_is_json_on_stderr_exit_1(conn_str):
    done = run_cli(conn_str, "-c", "SELECT FROM sys.objects")
    assert done.returncode == 1 and done.stdout == b""
    assert json.loads(done.stderr)["category"] == "syntax_error"


def test_cli_error_respects_the_byte_budget(conn_str):
    done = run_cli(conn_str, "-c", "SELECT * FROM " + "x" * 120, "--max-bytes", "256")
    assert done.returncode == 1 and len(done.stderr) <= 256
    json.loads(done.stderr)


@pytest.mark.parametrize(
    "args",
    [[], ["-c", "  "], ["-c", "SELECT 1", "--max-bytes", "10"], ["-c", "SELECT 1", "--format", "csv"], ["-c", "SELECT 1", "--max-rows", "x"]],
)
def test_cli_configuration_errors_exit_2_with_json(conn_str, args):
    done = run_cli(conn_str, *args)
    assert done.returncode == 2 and done.stdout == b""
    assert json.loads(done.stderr)["category"] == "configuration"


def test_cli_without_a_connection_string_is_a_configuration_error(conn_str):
    done = run_cli(conn_str, "-c", "SELECT 1", env_name=None)
    assert done.returncode == 2
    assert "MSSQL_CONNECTION_STRING" in json.loads(done.stderr)["message"]


def test_cli_timeout_is_json(conn_str):
    done = run_cli(conn_str, "-c", "WAITFOR DELAY '00:00:10'", "--timeout", "1")
    assert done.returncode == 1 and json.loads(done.stderr)["category"] == "timeout"


def test_console_script_is_installed(conn_str):
    script = os.path.join(os.path.dirname(sys.executable), "mssql-rs-agent")
    if not os.path.exists(script):
        pytest.skip("package not installed with its console script")
    done = run_cli(conn_str, script, "-c", "SELECT 1 AS a", module=False)
    assert done.returncode == 0 and done.stdout.startswith(b"a:int")
