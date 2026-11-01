# Agent mode


A CLI and Python API for LLM agents and terminals (ported from
[arrowbricks](https://github.com/bmsuisse/arrowbricks)): compact typed TSV (or a
Markdown table), explicit truncation, and actionable JSON errors. Rows are
fetched lazily and formatted in Rust, without building Python row objects, so a
capped preview of a huge table costs little more than the statement's start.

```bash
export MSSQL_CONNECTION_STRING='Server=...;Database=...;Uid=...;Pwd=...'   # or DB_CONNECTION_STRING
arrowsql-agent -c "SELECT 1 AS answer, N'x' AS label"      # python -m arrowsql.agent works too
```

```text
answer:int	label:nvarchar(1)
1	x

rows_shown=1; complete=true; total_rows=1
```

```python
from arrowsql.agent import query

text = query(conn_or_connection_string, "SELECT TOP 100 * FROM dbo.orders WHERE id > ?", [42],
             format="tsv",          # or "markdown"
             max_bytes=10_000,      # strict ceiling for the whole string, footer included
             max_rows=1000, max_cell_chars=500, timeout_s=60,
             tail_rows=0)           # N > 0: also keep the last N rows (reads the whole result)
```

- The header carries SQL types (`name:decimal(18,2)`). Tab, newline, CR and `\`
  are backslash-escaped, control characters become `\uXXXX`, NULL is `\N`
  (`NULL` in Markdown, where text is quoted so `"NULL"` differs), binary is `0x...`,
  `datetimeoffset` is shown in UTC, `sql_variant` by its value.
- The footer is `rows_shown=N; complete=true|false; total_rows=N` and says
  `total_rows>=N` unless every row was read. Clipped cells end in
  `…[truncated]` and set `cells_truncated=true`. With `tail_rows`, the footer
  reads `sample=head+tail; rows_omitted=K` and the total is exact.
- A batch prints every result set (footers from the second on start with
  `result_set=k`), `-- rows_affected=N` for DML and `-- message: ...` for
  PRINT / low-severity RAISERROR. After a truncated result set the rest of the
  batch is left unread. Output beyond the byte budget is dropped whole and noted.
- Failures return (CLI: stderr, exit 1) one JSON object: `category`
  (`missing_table`, `missing_column`, `syntax_error`, `authentication`,
  `permission_denied`, `deadlock`, `timeout`, `lock_timeout`, `constraint_violation`,
  `conversion_error`, `connection`, `transient`, ...), `message` (the server's own text),
  `next_step`, `retryable`, `sqlstate`, `native_error`. Advice only: SQL is never
  rewritten, repaired or retried. A failure after earlier rows gives the error, not
  a partial result with a success footer. Bad arguments exit 2, Ctrl-C exits 130.
- `query_cursor(cursor, sql)` runs on an existing cursor; `render_cursor(cursor)`
  renders one that has just executed. String connections use autocommit.

## Credentials and least privilege

- The CLI accepts a full connection string with `--connection-string` / `--conn`.
  Anything on the command line is visible to other local users (`ps`, `/proc`) and ends up
  in shell history and agent transcripts, so do **not** put the password there. Pass it
  through the environment instead: `MSSQL_CONNECTION_STRING` or `DB_CONNECTION_STRING`
  (read when `--connection-string` is absent), set from a secret store or an untracked
  `.env` file the agent never prints.
- Give the agent its own SQL login or Entra principal with the least privilege that the
  task needs, ideally read-only (`db_datareader`, `VIEW DEFINITION`) on one database. The
  tool runs the SQL it is given unchanged and does not make an unsafe statement safe.
- Prefer `Authentication=ActiveDirectoryServicePrincipal`-style or token credentials with
  short lifetimes over long-lived `sa` passwords; never use `sa`.
