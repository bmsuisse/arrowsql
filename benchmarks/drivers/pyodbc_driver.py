"""pyodbc + Microsoft ODBC Driver 18 (the .so bundled in mssql-python's wheel).

Needs unixODBC: the runner sets LD_LIBRARY_PATH/ODBCSYSINI to <main>/.bench-odbc
(see benchmarks/setup_drivers.sh), whose odbcinst.ini points at the bundled
libmsodbcsql-18 and enables driver-manager connection pooling.
"""

from __future__ import annotations

from drivers.base import DbApiDriver
from drivers.registry import SPECS

DRIVER_NAME = "ODBC Driver 18 for SQL Server"


class PyodbcDriver(DbApiDriver):
    name = "pyodbc"
    supports = SPECS["pyodbc"].caps
    fast = False

    @classmethod
    def version(cls):
        import pyodbc

        return f"pyodbc {pyodbc.version}"

    def _connect(self, autocommit):
        import pyodbc

        pyodbc.pooling = self.pooled  # process-global, honoured at the first connect
        return pyodbc.connect(self.info.odbc(DRIVER_NAME), autocommit=autocommit)

    def _cursor(self):
        cur = self.conn.cursor()
        cur.fast_executemany = self.fast
        return cur

    def executemany(self, sql, rows):
        self.set_autocommit(False)
        try:
            cur = self._cursor()
            cur.executemany(sql, rows)
            self.conn.commit()
            cur.close()
        finally:
            self.set_autocommit(True)
        return len(rows)

    def pooled_connect_close(self, n):
        return self.connect_close(n)


class PyodbcFastDriver(PyodbcDriver):
    name = "pyodbc_fast"
    supports = SPECS["pyodbc_fast"].caps
    fast = True
