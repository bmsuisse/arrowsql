"""RUST-PORT: UTF-8 transfer of national character columns for Arrow reads.

SQL Server sends nvarchar/nchar/ntext as UTF-16 (two bytes per character,
even ASCII) and TDS has no compression. ``utf8_query`` rewrites a query so
those columns travel as ``varchar`` in a UTF-8 collation, about half the
bytes for mostly-ASCII text; the decoded Arrow type is the same string type.
The describe round trip, the size probe and the SQL text are Rust
(``arrowsql_core::utf8``).

Every failure path returns the original query: the rewrite is an
optimization, never a reason for a read to fail.
"""

from typing import Any

from arrowsql import ddbc_bindings


def utf8_query(conn: Any, query: str) -> str:
    """``query`` rewritten to transfer nvarchar/nchar/ntext columns as UTF-8.

    Returns ``query`` itself when the server predates UTF-8 collations
    (SQL Server 2019 / Azure SQL), the query has an ORDER BY, no column would
    change, the column names are empty or duplicated, the describe or the
    sample probe fails, or the first 256 rows are not at least 10% smaller in
    UTF-8 (CJK text grows: 3 bytes per character against 2).
    """
    return ddbc_bindings.utf8_query(conn, query)
