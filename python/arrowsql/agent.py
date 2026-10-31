"""Agent mode: compact, bounded, typed query output for LLM agents and terminals.

``query()`` runs one SQL batch and returns one string: typed TSV (or a Markdown
table) with explicit truncation on success, or a JSON object with a category,
the server's own message and a recovery hint on failure. Rows are fetched
lazily and rendered to text in Rust (``ddbc_bindings.AgentRender``) without
building Python row objects; fetching stops once the row or byte limit is
reached, so a capped preview of a huge table stays cheap.

    from arrowsql.agent import query
    print(query(conn_str, "SELECT TOP 5 name, create_date FROM sys.objects"))

CLI: ``python -m arrowsql.agent -c "SELECT 1 AS answer"`` or ``arrowsql-agent``.
Success goes to stdout (exit 0), error JSON to stderr (exit 1), bad
arguments to stderr as JSON (exit 2). SQL is sent unchanged and a failed
statement is never rewritten or retried.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Literal, NoReturn, Optional, Sequence, Union

from . import ddbc_bindings
from .constants import ConstantsDDBC
from .helpers import check_error

AgentFormat = Literal["tsv", "markdown"]
AGENT_FORMATS = ("tsv", "markdown")
DEFAULT_FORMAT: AgentFormat = "tsv"
DEFAULT_MAX_ROWS = 1000
DEFAULT_MAX_BYTES = 10_000
DEFAULT_MAX_CELL_CHARS = 500
DEFAULT_TIMEOUT_S = 60.0
ENV_CONNECTION_STRINGS = ("MSSQL_CONNECTION_STRING", "DB_CONNECTION_STRING")

__all__ = [
    "AGENT_FORMATS",
    "error_json",
    "main",
    "query",
    "query_cursor",
    "render_cursor",
]

_SQL_HANDLE_STMT = ConstantsDDBC.SQL_HANDLE_STMT.value

# (category, recovery hint, retryable) by SQL Server error number.
_Category = tuple[str, str, bool]
_MISSING_TABLE: _Category = (
    "missing_table",
    "Verify the table or view name, schema and database, e.g. "
    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES, and your access to it.",
    False,
)
_MISSING_COLUMN: _Category = (
    "missing_column",
    "Check column names against the table schema, e.g. "
    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '<table>'.",
    False,
)
_SYNTAX: _Category = (
    "syntax_error",
    "Correct the SQL at the position the server names before submitting again.",
    False,
)
_PERMISSION: _Category = (
    "permission_denied",
    "Request access to the referenced objects or use an authorized object.",
    False,
)
_AUTH: _Category = (
    "authentication",
    "Check the login, password or token and that the login can reach the database.",
    False,
)
_DEADLOCK: _Category = (
    "deadlock",
    "The server chose this statement as a deadlock victim and rolled it back; retry it.",
    True,
)
_TIMEOUT: _Category = (
    "timeout",
    "Increase the timeout or narrow the query (filter, TOP, aggregate) before retrying.",
    True,
)
_LOCK_TIMEOUT: _Category = (
    "lock_timeout",
    "Another session holds a conflicting lock; retry later or find the blocker in sys.dm_exec_requests.",
    True,
)
_CONSTRAINT: _Category = (
    "constraint_violation",
    "The data violates a constraint named in the message; change the values, not the constraint.",
    False,
)
_CONVERSION: _Category = (
    "conversion_error",
    "Check the value and target type; use TRY_CAST/TRY_CONVERT to find the offending rows.",
    False,
)
_TRUNCATION: _Category = (
    "data_truncation",
    "A value is longer than its column; shorten it or widen the column.",
    False,
)
_EXISTS: _Category = (
    "already_exists",
    "An object with that name exists; pick another name or check it first.",
    False,
)
_MISSING_PROC: _Category = (
    "missing_function",
    "Check the procedure or function name and schema, e.g. "
    "SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES.",
    False,
)
_MISSING_DB: _Category = (
    "missing_database",
    "Verify the database name and that the login has access: SELECT name FROM sys.databases.",
    False,
)
_TRANSIENT: _Category = (
    "transient",
    "The server or network was briefly unavailable; retry after a short wait.",
    True,
)
_CONNECTION: _Category = (
    "connection",
    "Check the server name, port, network access and TLS settings in the connection string.",
    True,
)
_DIVIDE: _Category = (
    "arithmetic_error",
    "Guard the expression (NULLIF for divisions) or filter out the offending rows.",
    False,
)

_BY_NUMBER: dict[int, _Category] = {
    208: _MISSING_TABLE,
    3701: _MISSING_TABLE,
    207: _MISSING_COLUMN,
    4104: _MISSING_COLUMN,
    102: _SYNTAX,
    105: _SYNTAX,
    156: _SYNTAX,
    170: _SYNTAX,
    174: _SYNTAX,
    128: _SYNTAX,
    4145: _SYNTAX,
    8120: _SYNTAX,
    8127: _SYNTAX,
    195: _MISSING_PROC,
    2812: _MISSING_PROC,
    4060: _MISSING_DB,
    911: _MISSING_DB,
    229: _PERMISSION,
    230: _PERMISSION,
    262: _PERMISSION,
    297: _PERMISSION,
    300: _PERMISSION,
    916: _PERMISSION,
    18456: _AUTH,
    18452: _AUTH,
    18487: _AUTH,
    18488: _AUTH,
    18470: _AUTH,
    1205: _DEADLOCK,
    1222: _LOCK_TIMEOUT,
    515: _CONSTRAINT,
    547: _CONSTRAINT,
    2601: _CONSTRAINT,
    2627: _CONSTRAINT,
    241: _CONVERSION,
    242: _CONVERSION,
    245: _CONVERSION,
    8114: _CONVERSION,
    8115: _CONVERSION,
    232: _CONVERSION,
    248: _CONVERSION,
    220: _CONVERSION,
    8134: _DIVIDE,
    8152: _TRUNCATION,
    2628: _TRUNCATION,
    2714: _EXISTS,
    1801: _EXISTS,
    40613: _TRANSIENT,
    40501: _TRANSIENT,
    40197: _TRANSIENT,
    49918: _TRANSIENT,
    10928: _TRANSIENT,
    10929: _TRANSIENT,
}

# Used when the server error number is missing (driver-side errors).
_BY_SQLSTATE: dict[str, _Category] = {
    "42S02": _MISSING_TABLE,
    "42S22": _MISSING_COLUMN,
    "28000": _AUTH,
    "28P01": _AUTH,
    "40001": _DEADLOCK,
    "HYT00": _TIMEOUT,
    "HYT01": _TIMEOUT,
    "08001": _CONNECTION,
    "08004": _CONNECTION,
    "08S01": _CONNECTION,
    "08003": _CONNECTION,
    "08007": _CONNECTION,
    "23000": _CONSTRAINT,
    "22001": _TRUNCATION,
    "22003": _CONVERSION,
    "22007": _CONVERSION,
    "22018": _CONVERSION,
    "22012": _DIVIDE,
}

_GENERIC: _Category = (
    "execution_error",
    "Inspect the server's message and verify the statement; do not retry unchanged.",
    False,
)
_DRIVER_PREFIX = re.compile(r"^(?:\[[^\]]*\])+")


def _server_message(exc: BaseException) -> str:
    """The server's own text, without the ODBC driver prefix."""
    text = getattr(exc, "ddbc_error", None) or getattr(exc, "driver_error", None) or str(exc)
    text = str(text)
    marker = text.find("[SQL Server]")
    if marker >= 0:
        return text[marker + len("[SQL Server]") :].strip()
    return _DRIVER_PREFIX.sub("", text, count=1).strip() or text


def _classify(exc: BaseException, timeout: bool) -> _Category:
    if timeout or isinstance(exc, TimeoutError):
        return _TIMEOUT
    number = getattr(exc, "native_error", None)
    state = getattr(exc, "sqlstate", None)
    if number in _BY_NUMBER:
        return _BY_NUMBER[number]
    if state in _BY_SQLSTATE:
        return _BY_SQLSTATE[state]
    if isinstance(exc, ValueError):
        return (
            "configuration",
            "Check the budgets (max_bytes >= 256, max_rows >= 1, max_cell_chars >= 1) and arguments.",
            False,
        )
    if state == "42000":
        # Generic "syntax error or access violation" with an unmapped number.
        return _SYNTAX if "syntax" in _server_message(exc).lower() else _GENERIC
    return _GENERIC


def error_json(exc: BaseException, max_bytes: int = DEFAULT_MAX_BYTES, timeout: bool = False) -> str:
    """One JSON object describing ``exc``, at most ``max_bytes`` UTF-8 bytes.

    Keys: ``category``, ``message`` (the server's text, clipped from the end if
    needed, with ``truncated: true``), ``next_step`` (advice only; nothing is
    executed), ``retryable``, and ``sqlstate`` / ``native_error`` (the SQL
    Server error number) when known.
    """
    category, next_step, retryable = _classify(exc, timeout)
    message = _server_message(exc) if not isinstance(exc, TimeoutError) else str(exc)
    value: dict[str, Any] = {
        "category": category,
        "message": message,
        "next_step": next_step,
        "retryable": retryable,
    }
    state = getattr(exc, "sqlstate", None)
    number = getattr(exc, "native_error", None)
    if state:
        value["sqlstate"] = state
    if number:
        value["native_error"] = number
    max_bytes = max(int(max_bytes), 256)

    def size() -> int:
        return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))

    if size() <= max_bytes:
        return json.dumps(value, ensure_ascii=False)
    value["truncated"] = True
    low, high = 0, len(message)
    while low < high:
        middle = (low + high + 1) // 2
        value["message"] = message[:middle]
        if size() <= max_bytes:
            low = middle
        else:
            high = middle - 1
    value["message"] = message[:low]
    if size() > max_bytes:  # the fixed fields alone are too big
        value["next_step"] = "Inspect the server's message before retrying."
        value.pop("sqlstate", None)
    return json.dumps(value, ensure_ascii=False)


def _split_message(item: Any) -> str:
    """Text of a ``cursor.messages`` entry, with the SQL Server number when present."""
    label, text = item if isinstance(item, (tuple, list)) and len(item) == 2 else ("", str(item))
    text = str(text)
    marker = text.find("[SQL Server]")
    if marker >= 0:
        text = text[marker + len("[SQL Server]") :]
    match = re.search(r"\((\d+)\)", str(label))
    prefix = f"({match.group(1)}) " if match and match.group(1) != "0" else ""
    return prefix + text.strip()


def _flush_messages(render: Any, cursor: Any, seen: int) -> int:
    messages = cursor.messages
    for item in messages[seen:]:
        render.message(_split_message(item))
    return len(messages)


def _render_all(cursor: Any, render: Any) -> None:
    """Walk the cursor's result sets and parts into ``render``."""
    seen = 0
    while True:
        seen = _flush_messages(render, cursor, seen)
        exhausted = True
        if cursor.description:
            ret, exhausted = render.result_set(cursor.hstmt, cursor.messages)
            check_error(_SQL_HANDLE_STMT, cursor.hstmt, ret)
            seen = _flush_messages(render, cursor, seen)
        elif cursor.rowcount is not None and cursor.rowcount >= 0:
            render.rows_affected(cursor.rowcount)
        # Past a truncated result set (or the budget) later ones stay unread:
        # skipping to them would read the rest of this one.
        if not exhausted or not cursor.nextset():
            break
        if not render.has_room():
            render.omit(1)
            break
        seen = 0


def _make_render(
    format: str,
    max_bytes: int,
    max_rows: int,
    max_cell_chars: int,
    tail_rows: int,
    timeout_s: Optional[float],
) -> Any:
    if format not in AGENT_FORMATS:
        raise ValueError("format must be 'tsv' or 'markdown'")
    if timeout_s is not None and (not math.isfinite(timeout_s) or timeout_s < 0):
        raise ValueError("timeout_s must be a finite, non-negative number of seconds")
    return ddbc_bindings.AgentRender(format, max_bytes, max_rows, max_cell_chars, tail_rows, timeout_s)


def render_cursor(
    cursor: Any,
    *,
    format: AgentFormat = DEFAULT_FORMAT,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_cell_chars: int = DEFAULT_MAX_CELL_CHARS,
    tail_rows: int = 0,
    timeout_s: Optional[float] = DEFAULT_TIMEOUT_S,
) -> str:
    """Render the results of a cursor that has just executed, raising on errors.

    Reads every result set of the batch (stopping after one that was cut short)
    and returns the text. Raises the driver's exception when the statement
    fails, so no success footer is ever produced for a failed statement.
    """
    render = _make_render(format, max_bytes, max_rows, max_cell_chars, tail_rows, timeout_s)
    _render_all(cursor, render)
    return render.finish()


def _execute(cursor: Any, sql: str, params: Any) -> None:
    if params is None:
        cursor.execute(sql)
    elif isinstance(params, (list, tuple)):
        cursor.execute(sql, *params)
    else:
        cursor.execute(sql, params)


def _run(
    conn_or_str: Any,
    sql: str,
    params: Any,
    *,
    format: str,
    max_bytes: int,
    max_rows: int,
    max_cell_chars: int,
    tail_rows: int,
    timeout_s: Optional[float],
    cursor: Any = None,
) -> tuple[str, bool]:
    """``(text, ok)``: the preview, or the error JSON with ``ok`` False."""
    try:
        render = _make_render(format, max_bytes, max_rows, max_cell_chars, tail_rows, timeout_s)
    except (ValueError, OverflowError, TypeError) as exc:
        return error_json(exc, max(max_bytes, 256) if isinstance(max_bytes, int) else 256), False
    owned = None
    own_cursor = None
    previous_timeout = None
    try:
        if cursor is None:
            if isinstance(conn_or_str, str):
                from .db_connection import connect

                owned = conn = connect(conn_or_str, autocommit=True)
            else:
                conn = conn_or_str
            if timeout_s:
                previous_timeout = conn.timeout
                conn.timeout = max(1, math.ceil(timeout_s))
            cursor = own_cursor = conn.cursor()
        _execute(cursor, sql, params)
        _render_all(cursor, render)
        return render.finish(), True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return error_json(exc, max_bytes), False
    finally:
        if own_cursor is not None:
            try:
                own_cursor.close()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        if previous_timeout is not None and owned is None:
            try:
                conn_or_str.timeout = previous_timeout
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        if owned is not None:
            try:
                owned.close()
            except Exception:  # pylint: disable=broad-exception-caught
                pass


def query(
    conn_or_str: Any,
    sql: str,
    params: Union[Sequence[Any], dict, None] = None,
    *,
    format: AgentFormat = DEFAULT_FORMAT,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_cell_chars: int = DEFAULT_MAX_CELL_CHARS,
    tail_rows: int = 0,
    timeout_s: Optional[float] = DEFAULT_TIMEOUT_S,
) -> str:
    """Run ``sql`` and return its bounded, typed preview as one string.

    ``conn_or_str`` is a connection or a connection string (opened with
    autocommit and closed afterwards). On success the text is typed TSV (or a
    Markdown table) ending with a ``rows_shown=...; complete=...`` footer per
    result set; ``-- rows_affected=N`` and ``-- message: ...`` lines report DML
    and PRINT/RAISERROR output. On any failure the result is a JSON object with
    ``category``, ``message`` (the server's own text), ``next_step`` and
    ``retryable``; tell the two apart by whether the text parses as a JSON
    object. The whole string, footer or JSON, is at most ``max_bytes`` bytes.

    ``tail_rows`` > 0 keeps the last rows of a longer result next to the first
    ones (head and tail sample) at the cost of reading the whole result.
    ``timeout_s`` bounds the statement and the fetching.
    """
    return _run(
        conn_or_str,
        sql,
        params,
        format=format,
        max_bytes=max_bytes,
        max_rows=max_rows,
        max_cell_chars=max_cell_chars,
        tail_rows=tail_rows,
        timeout_s=timeout_s,
    )[0]


def query_cursor(
    cursor: Any,
    sql: str,
    params: Union[Sequence[Any], dict, None] = None,
    *,
    format: AgentFormat = DEFAULT_FORMAT,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_rows: int = DEFAULT_MAX_ROWS,
    max_cell_chars: int = DEFAULT_MAX_CELL_CHARS,
    tail_rows: int = 0,
    timeout_s: Optional[float] = DEFAULT_TIMEOUT_S,
) -> str:
    """Like :func:`query` on an existing cursor (which is left open and usable)."""
    return _run(
        None,
        sql,
        params,
        format=format,
        max_bytes=max_bytes,
        max_rows=max_rows,
        max_cell_chars=max_cell_chars,
        tail_rows=tail_rows,
        timeout_s=timeout_s,
        cursor=cursor,
    )[0]


# --- command line -----------------------------------------------------------


def _configuration_error(message: str) -> None:
    print(
        json.dumps(
            {
                "category": "configuration",
                "message": message,
                "next_step": "Check --help, the connection string, SQL input and positive budgets "
                "(max_bytes >= 256).",
                "retryable": False,
            }
        ),
        file=sys.stderr,
    )


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        _configuration_error(message)
        raise SystemExit(2)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="arrowsql-agent",
        description="SQL Server query to a bounded typed preview (TSV by default); errors are JSON on stderr.",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("sql_text", nargs="?", help="SQL statement, sent unchanged")
    source.add_argument("-c", "--sql", "--command", dest="sql", help="SQL statement, sent unchanged")
    source.add_argument("-f", "--file", help="UTF-8 SQL file, or - for stdin; defaults to piped stdin")
    parser.add_argument("--agent", "-agent", action="store_true", help="agent output (the default)")
    parser.add_argument(
        "--connection-string",
        "--conn",
        dest="connection_string",
        default=None,
        help="ODBC-style connection string (or set MSSQL_CONNECTION_STRING / DB_CONNECTION_STRING)",
    )
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--max-cell-chars", type=int, default=DEFAULT_MAX_CELL_CHARS)
    parser.add_argument("--tail-rows", type=int, default=0, help="also show the last N rows (reads the whole result)")
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=AGENT_FORMATS,
        default=DEFAULT_FORMAT,
        help="tab-separated values (default) or a Markdown pipe table",
    )
    parser.add_argument("--total-timeout-s", "--timeout", dest="timeout_s", type=float, default=DEFAULT_TIMEOUT_S)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """Run one SQL batch; return 0 for a preview, 1 for query errors, 2 for bad arguments."""
    parser = _parser()
    args = parser.parse_args(argv)
    connection_string = args.connection_string
    for name in ENV_CONNECTION_STRINGS:
        connection_string = connection_string or os.environ.get(name)
    if not connection_string:
        parser.error("set MSSQL_CONNECTION_STRING or DB_CONNECTION_STRING, or pass --connection-string")
    try:
        if args.sql is not None:
            sql = args.sql
        elif args.sql_text is not None:
            sql = args.sql_text
        elif args.file and args.file != "-":
            sql = Path(args.file).read_bytes().decode("utf-8")
        else:
            if sys.stdin.isatty():
                parser.error("supply SQL as an argument, --sql, --file, or pipe it on stdin")
            sql = sys.stdin.buffer.read().decode("utf-8")
    except (OSError, UnicodeError) as exc:
        parser.error(f"cannot read SQL input: {exc}")
    if not sql.strip():
        parser.error("SQL input is empty; supply an argument, --sql, --file or pipe SQL on stdin")
    try:
        text, ok = _run(
            connection_string,
            sql,
            None,
            format=args.output_format,
            max_bytes=args.max_bytes,
            max_rows=args.max_rows,
            max_cell_chars=args.max_cell_chars,
            tail_rows=args.tail_rows,
            timeout_s=args.timeout_s,
        )
    except KeyboardInterrupt:
        print(
            json.dumps(
                {
                    "category": "interrupted",
                    "message": "Query interrupted.",
                    "next_step": "Retry only if you still need the result.",
                    "retryable": False,
                }
            ),
            file=sys.stderr,
        )
        return 130
    parsed = _is_configuration(text) if not ok else False
    # Locale encoding and newline translation must not change the byte budget.
    stream = sys.stdout if ok else sys.stderr
    stream.buffer.write(text.encode("utf-8"))
    stream.buffer.flush()
    return 2 if parsed else (0 if ok else 1)


def _is_configuration(text: str) -> bool:
    try:
        return json.loads(text).get("category") == "configuration"
    except ValueError:
        return False


if __name__ == "__main__":
    sys.exit(main())
