"""RUST-PORT: UTF-8 transfer of national character columns for Arrow reads.

SQL Server sends nvarchar/nchar/ntext as UTF-16 (two bytes per character,
even ASCII) and TDS has no compression. ``utf8_query`` rewrites a query so
those columns travel as ``varchar`` in a UTF-8 collation, about half the
bytes for mostly-ASCII text; the decoded Arrow type is the same string type.
The SQL text is built in Rust (``ddbc_bindings.utf8_wrap_query``); this module
only runs the describe round trip and decides when to skip.

Every failure path returns the original query: the rewrite is an
optimization, never a reason for a read to fail.
"""

import re
import weakref
from contextlib import closing
from typing import Any, Optional

from arrowsql import ddbc_bindings
from arrowsql.logging import logger

# One round trip: server capability plus the described, visible columns.
_DESCRIBE_SQL = (
    "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int), "
    "CAST(SERVERPROPERTY('EngineEdition') AS int), "
    "d.name, d.system_type_id, d.max_length, d.is_nullable "
    "FROM sys.dm_exec_describe_first_result_set(?, NULL, 0) AS d "
    "WHERE d.is_hidden = 0 ORDER BY d.column_ordinal"
)
_CHECK_SQL = "SELECT TOP 1 d.name FROM sys.dm_exec_describe_first_result_set(?, NULL, 0) AS d"
# The rewrite puts the query in a derived table, which drops ORDER BY.
_ORDER_BY = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)

# connection -> server accepts UTF-8 collations (learned from the first describe).
_server_support: "weakref.WeakKeyDictionary[Any, bool]" = weakref.WeakKeyDictionary()


def utf8_query(conn: Any, query: str) -> str:
    """``query`` rewritten to transfer nvarchar/nchar/ntext columns as UTF-8.

    Returns ``query`` itself when the server predates UTF-8 collations
    (SQL Server 2019 / Azure SQL), the query has an ORDER BY, no column would
    change, the column names are empty or duplicated, or the describe fails.
    """
    if _ORDER_BY.search(query) or _server_support.get(conn) is False:
        return query
    try:
        with closing(conn.cursor()) as cursor:
            cursor.execute(_DESCRIBE_SQL, (query,))
            rows = cursor.fetchall()
            if not rows:
                return query
            supported = ddbc_bindings.utf8_collations_supported(rows[0][0], rows[0][1])
            try:
                _server_support[conn] = supported
            except TypeError:
                pass
            if not supported:
                return query
            wrapped = ddbc_bindings.utf8_wrap_query(query, [(r[2], r[3], r[4], r[5]) for r in rows])
            if wrapped is None:
                return query
            # The rewrite is invalid for batches, EXEC and the like; find out here.
            cursor.execute(_CHECK_SQL, (wrapped,))
            check = cursor.fetchall()
            if not check or check[0][0] is None:
                return query
            return wrapped
    except Exception as exc:
        logger.debug("utf8_strings: keeping the original query (%s)", exc)
        return query
