"""Agent mode: bounded, typed text previews and actionable JSON errors for LLM agents.

    python examples/agent_mode.py

The same from a shell:

    export MSSQL_CONNECTION_STRING='Server=...;Database=...;Uid=...;Pwd=...'
    arrowsql-agent -c "SELECT TOP 5 * FROM dbo.arrowsql_example_trips"
    arrowsql-agent --format markdown --max-rows 20 -f query.sql
    echo "SELECT 1 AS answer" | arrowsql-agent
"""

import json

import arrowsql
from arrowsql.agent import query, query_cursor
from _demo import TABLE, connection_string, create_demo_table, drop_demo_table

conn_str = connection_string()
create_demo_table(conn_str)

# 1. A capped preview: the header carries SQL types, the footer says whether the
#    result is complete, and the whole string never exceeds max_bytes.
print(query(conn_str, f"SELECT * FROM {TABLE} WHERE fare > ?", [70], max_rows=5))

# 2. Markdown for chat UIs, plus the last rows of the result (reads it all).
print(query(conn_str, f"SELECT trip_id, zone, fare FROM {TABLE} ORDER BY trip_id",
            format="markdown", max_rows=3, tail_rows=2))

# 3. A batch: every result set, row counts for DML, and PRINT messages.
print(query(conn_str, f"PRINT 'counting'; SELECT count(*) AS n FROM {TABLE}; "
                      f"UPDATE {TABLE} SET passengers = 1 WHERE trip_id <= 10"))

# 4. A failure is not raised: the result is one JSON object an agent can act on
#    (category, the server's message, next_step, retryable). arrowsql-agent prints
#    the same object to stderr and exits 1.
out = query(conn_str, "SELECT * FROM dbo.no_such_table")
err = json.loads(out)
print(err["category"], "|", err["next_step"], "| retryable:", err["retryable"])

# 5. On an existing connection (e.g. inside a transaction your agent controls).
with arrowsql.connect(conn_str) as conn:
    print(query_cursor(conn.cursor(), f"SELECT TOP 2 zone FROM {TABLE} ORDER BY trip_id"))

drop_demo_table(conn_str)
