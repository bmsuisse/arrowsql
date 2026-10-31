"""
SQL Server engine error number -> ODBC 3.x SQLSTATE.

The table lives in the native layer (src/diag.rs); this module re-exports it
for the async py-core path, so exceptions still carry the SQLSTATE that
msodbcsql would report (#581).
"""

from arrowsql.ddbc_bindings import sqlstate_for_server_error

__all__ = ["sqlstate_for_server_error"]
