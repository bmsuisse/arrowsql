"""Informational message volume: PRINT floods, RAISERROR progress messages,
messages interleaved with result sets, cursor.messages content and order, and
bounded memory for very large message counts.

Research sources (issue trackers and docs, consulted via web search; summaries
were taken from search snippets, not from full page reads):
- https://github.com/microsoft/mssql-python/issues/612
  (open report: only the first PRINT reached cursor.messages when a batch
  printed several messages; the reporter expects every message to be available
  through cursor.messages/nextset() before nextset() returns False)
- https://github.com/microsoft/mssql-python/pull/618 (fix for #612: PRINT
  messages after the first one were lost because nextset() did not capture the
  SQL_SUCCESS_WITH_INFO diagnostics; the fix collects messages per result set
  and clears them on each nextset(), and its regression tests assert totals
  across nextset() calls)
- https://github.com/microsoft/mssql-python/pull/181 (added cursor.messages and
  clears it before each non-fetch operation)
- https://learn.microsoft.com/en-us/sql/connect/python/mssql-python/cursor-management?view=sql-server-ver17
  (cursor.messages holds PRINT output and RAISERROR with severity below 11 as
  (sqlstate, text) tuples)
- https://github.com/mkleehammer/pyodbc/issues/673 (RAISERROR ... WITH NOWAIT
  before a SELECT inside a procedure: the message is a separate result step and
  the caller must advance with nextset() before the rows are visible)
- https://github.com/mkleehammer/pyodbc/issues/583 (PRINT output not visible in
  cursor.messages for some callers; the message stream must be drained with
  nextset())
- https://github.com/mkleehammer/pyodbc/issues/747 (long diagnostic text
  truncated by a fixed-size error buffer; relevant to how much text a
  diagnostic record may carry)
- https://sqltheater.com/blog/stop-using-print/ (PRINT is buffered and flushed
  late; RAISERROR ... WITH NOWAIT flushes immediately, so ordering between the
  two can differ on the client)
- https://chadbaldwin.net/2021/01/15/raiserror-cheatsheet.html (severity 0-10
  are informational, 11+ raise an error that aborts the batch or statement)

Behaviors a native-TDS reimplementation can easily get wrong:
- every INFO token in a batch must be surfaced, not only the first one, and
  messages must stay in batch order relative to result sets
- messages must be attached to the result step they belong to; the list must be
  reset on every nextset() and on every execute()
- the message tuple shape is (sqlstate-like string, text); text must keep
  brackets and Unicode (surrogate pairs) intact
- severity 0-10 must not raise; severity 11+ must raise, with the message text
  present in the exception
- a long PRINT must not be cut at an internal buffer size smaller than the
  server limit, and must not overflow the server limit
- very large message counts must stream without an unbounded internal buffer
"""

import time

import pytest
from mssql_python import ProgrammingError


_PREFIX_MARK = "[SQL Server]"


def _text(message):
    """Return the server text of one cursor.messages entry, without driver prefix."""
    return message[1].split(_PREFIX_MARK, 1)[1]


def _drain(cursor):
    """Consume a whole batch, returning ordered ('m', text) and ('r', rows) events.

    Each step may carry messages and/or rows. Messages of a step are emitted
    before its rows, which matches the order the server sends them in.
    """
    events = []
    while True:
        for message in cursor.messages:
            events.append(("m", _text(message)))
        if cursor.description is not None:
            events.append(("r", [tuple(row) for row in cursor.fetchall()]))
        if not cursor.nextset():
            break
    return events


def _messages_only(events):
    return [value for kind, value in events if kind == "m"]


def test_print_message_tuple_shape(cursor):
    cursor.execute("PRINT 'hello'")
    assert len(cursor.messages) == 1
    message = cursor.messages[0]
    assert isinstance(message, tuple) and len(message) == 2
    assert isinstance(message[0], str) and isinstance(message[1], str)
    assert message[0] == "[01000] (0)"
    assert _text(message) == "hello"


def test_print_unicode_bmp_and_surrogate_pairs(cursor):
    cursor.execute("PRINT N'héllo 😀 €'")
    assert _messages_only(_drain(cursor)) == ["héllo 😀 €"]


def test_print_brackets_are_not_parsed_away(cursor):
    cursor.execute("PRINT 'a]b[c [SQL Server] d'")
    assert _messages_only(_drain(cursor)) == ["a]b[c [SQL Server] d"]


def test_print_empty_string_yields_one_blank_message(cursor):
    cursor.execute("PRINT ''")
    values = _messages_only(_drain(cursor))
    assert len(values) == 1
    assert values[0].strip() == ""


def test_print_varchar_max_is_capped_at_8000_chars_without_error(cursor):
    cursor.execute("PRINT REPLICATE(CAST('x' AS varchar(max)), 9000)")
    values = _messages_only(_drain(cursor))
    assert len(values) == 1
    assert len(values[0]) == 8000
    assert set(values[0]) == {"x"}


def test_print_nvarchar_below_limit_is_not_truncated(cursor):
    cursor.execute("PRINT REPLICATE(CAST(N'é' AS nvarchar(max)), 3000)")
    values = _messages_only(_drain(cursor))
    assert values == ["é" * 3000]


def test_two_thousand_prints_all_delivered_in_order(cursor):
    cursor.execute(
        "DECLARE @i int = 0; WHILE @i < 2000 BEGIN PRINT CONCAT('m', @i); SET @i += 1; END"
    )
    events = _drain(cursor)
    assert all(kind == "m" for kind, _ in events)
    assert _messages_only(events) == [f"m{i}" for i in range(2000)]


def test_twenty_thousand_prints_count_exact_and_fast(cursor):
    started = time.monotonic()
    cursor.execute(
        "DECLARE @i int = 0; WHILE @i < 20000 BEGIN PRINT CONCAT('p', @i); SET @i += 1; END"
    )
    count = 0
    first = None
    last = None
    while True:
        for message in cursor.messages:
            text = _text(message)
            first = text if first is None else first
            last = text
            count += 1
        if not cursor.nextset():
            break
    elapsed = time.monotonic() - started
    assert count == 20000
    assert first == "p0"
    assert last == "p19999"
    assert elapsed < 30


def test_messages_visible_in_order_across_steps(cursor):
    cursor.execute("PRINT 'a'; PRINT 'b'; PRINT 'c'")
    assert [_text(m) for m in cursor.messages] == ["a"]
    assert cursor.nextset() is True
    assert [_text(m) for m in cursor.messages] == ["b"]
    assert cursor.nextset() is True
    assert [_text(m) for m in cursor.messages] == ["c"]
    assert cursor.nextset() is False


def test_print_interleaved_with_result_sets_keeps_order(cursor):
    cursor.execute("PRINT 'p1'; SELECT 1 AS a; PRINT 'p2'; SELECT 2 AS b; PRINT 'p3'")
    events = _drain(cursor)
    assert [e for e in events if e[0] == "m"] == [("m", "p1"), ("m", "p2"), ("m", "p3")]
    rows = [e[1] for e in events if e[0] == "r"]
    assert rows == [[(1,)], [(2,)]]
    assert [kind for kind, _ in events] == ["m", "r", "m", "r", "m"]


def test_print_with_nocount_on_still_reports_messages(cursor):
    cursor.execute("SET NOCOUNT ON; PRINT 'x'; SELECT 5 AS v")
    events = _drain(cursor)
    assert ("m", "x") in events
    assert ("r", [(5,)]) in events
    assert events.index(("m", "x")) < events.index(("r", [(5,)]))


def test_raiserror_severity_0_to_10_nowait_progress_in_order(cursor):
    sql = "".join(
        f"RAISERROR('sev{sev}', {sev}, 1) WITH NOWAIT; " for sev in range(0, 11)
    ) + "SELECT 42 AS answer"
    cursor.execute(sql)
    events = _drain(cursor)
    assert _messages_only(events) == [f"sev{sev}" for sev in range(0, 11)]
    assert ("r", [(42,)]) in events
    assert events[-1] == ("r", [(42,)])


def test_raiserror_nowait_twenty_thousand_messages_with_arguments(cursor):
    cursor.execute(
        "DECLARE @i int = 0; "
        "WHILE @i < 20000 BEGIN RAISERROR('step %d', 0, 1, @i) WITH NOWAIT; SET @i += 1; END"
    )
    values = _messages_only(_drain(cursor))
    assert len(values) == 20000
    assert values[0] == "step 0"
    assert values[-1] == "step 19999"


def test_raiserror_severity_11_raises_programming_error(cursor):
    with pytest.raises(ProgrammingError) as info:
        cursor.execute("RAISERROR('sev11-boom', 11, 1)")
    assert "sev11-boom" in str(info.value)


def test_messages_cleared_between_executes(cursor):
    cursor.execute("PRINT 'stale'")
    assert len(cursor.messages) == 1
    cursor.execute("SELECT 1 AS one")
    assert cursor.messages == []
    assert [tuple(row) for row in cursor.fetchall()] == [(1,)]


def test_messages_from_rolled_back_transaction_still_delivered(cursor):
    cursor.execute(
        "BEGIN TRANSACTION; PRINT 'in-tran'; ROLLBACK TRANSACTION; SELECT 7 AS v"
    )
    events = _drain(cursor)
    assert _messages_only(events) == ["in-tran"]
    assert ("r", [(7,)]) in events


def test_print_inside_loop_with_temp_table_counts_match(cursor):
    cursor.execute(
        "CREATE TABLE #flood (i int NOT NULL); "
        "DECLARE @i int = 0; "
        "WHILE @i < 500 BEGIN "
        "  INSERT INTO #flood (i) VALUES (@i); "
        "  PRINT CONCAT('row ', @i); "
        "  SET @i += 1; "
        "END; "
        "SELECT COUNT(*) AS n, MAX(i) AS hi FROM #flood"
    )
    events = _drain(cursor)
    values = _messages_only(events)
    assert len(values) == 500
    assert values[0] == "row 0" and values[-1] == "row 499"
    assert ("r", [(500, 499)]) in events
