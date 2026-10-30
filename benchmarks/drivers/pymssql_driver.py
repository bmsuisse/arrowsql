"""pymssql (FreeTDS). DB-API rows plus Connection.bulk_copy for bulk loads."""

from __future__ import annotations

from drivers.base import DbApiDriver
from drivers.registry import SPECS


class PymssqlDriver(DbApiDriver):
    name = "pymssql"
    supports = SPECS["pymssql"].caps

    @classmethod
    def version(cls):
        import pymssql

        return f"pymssql {pymssql.__version__}"

    def ph(self, sql):
        return sql.replace("?", "%s")

    def _connect(self, autocommit):
        import pymssql

        return pymssql.connect(server=self.info.host, port=self.info.port, user=self.info.user,
                               password=self.info.password, database=self.info.database,
                               autocommit=autocommit)

    def set_autocommit(self, flag):
        self.conn.autocommit(flag)

    def bulk_insert_rows(self, table, payload, batch, tablock):
        # bulk_copy wants a materialized sequence: feed it one batch at a time. Each call is
        # its own bulk operation (TABLOCK taken per call).
        size = batch or 100_000
        pending: list = []
        for chunk in payload.row_chunks(10_000):
            pending.extend(chunk)
            if len(pending) >= size:
                self.conn.bulk_copy(table, pending, batch_size=size, tablock=tablock)
                pending = []
        if pending:
            self.conn.bulk_copy(table, pending, batch_size=size, tablock=tablock)
        return payload.n
