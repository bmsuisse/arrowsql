# Examples

Each script creates its own demo table (`dbo.arrowsql_example_trips`) in the
database of `DB_CONNECTION_STRING` and drops it at the end.

```bash
pip install arrowsql duckdb polars pyarrow pandas
export DB_CONNECTION_STRING='Server=localhost,1433;Database=tempdb;Uid=sa;Pwd=...;Encrypt=yes;TrustServerCertificate=yes'
python examples/arrow_duckdb.py
```

| Script | Shows |
|---|---|
| [arrow_duckdb.py](arrow_duckdb.py) | Results as Arrow: `fetchall_arrow()` into DuckDB, `arrow_stream()` for larger-than-memory scans, `arrow()` for pyarrow, polars and pandas, and `read_arrow(partitions=...)` over parallel connections |
| [write_arrow.py](write_arrow.py) | Arrow into SQL Server: `cursor.bulkcopy_arrow()` from a DuckDB result, and `write_arrow(workers=4)` over parallel connections |
| [agent_mode.py](agent_mode.py) | Agent mode for LLM agents and terminals: typed TSV or Markdown previews with a byte budget, head-and-tail samples, multi-statement batches, JSON errors, and the `arrowsql-agent` CLI |
| [odbc2deltalake_sync.py](odbc2deltalake_sync.py) | Sync a table into Delta Lake with [odbc2deltalake](https://github.com/bmsuisse/odbc2deltalake) (SCD2, incremental merges), reading through arrowsql instead of ODBC |

`TrustServerCertificate=yes` is for local test servers only.
