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
# Rewrite only when UTF-8 is at most this share of the UTF-16 bytes of the
# sampled rows. CJK is 3 bytes per character in UTF-8 against 2 in UTF-16.
_MAX_SIZE_RATIO = 0.9
# The rewrite puts the query in a derived table, which drops ORDER BY.
_ORDER_BY = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)

# connection -> server accepts UTF-8 collations (learned from the first describe).
_server_support: "weakref.WeakKeyDictionary[Any, bool]" = weakref.WeakKeyDictionary()


def utf8_query(conn: Any, query: str) -> str:
    """``query`` rewritten to transfer nvarchar/nchar/ntext columns as UTF-8.

    Returns ``query`` itself when the server predates UTF-8 collations
    (SQL Server 2019 / Azure SQL), the query has an ORDER BY, no column would
    change, the column names are empty or duplicated, the describe or the
    sample probe fails, or the first 256 rows are not at least 10% smaller in
    UTF-8 (CJK text grows: 3 bytes per character against 2).
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
            plan = ddbc_bindings.utf8_wrap_query(query, [(r[2], r[3], r[4], r[5]) for r in rows])
            if plan is None:
                return query
            wrapped, sample_sql = plan
            # Runs the query's first rows: also finds out that the rewrite is
            # invalid (batches, EXEC, ...) and how big the strings are in UTF-8.
            cursor.execute(sample_sql)
            utf8_bytes, utf16_bytes = cursor.fetchone()
            if not utf16_bytes or utf8_bytes > _MAX_SIZE_RATIO * utf16_bytes:
                logger.debug("utf8_strings: no saving in sample (%s vs %s bytes)", utf8_bytes, utf16_bytes)
                return query
            return wrapped
    except Exception as exc:
        logger.debug("utf8_strings: keeping the original query (%s)", exc)
        return query
