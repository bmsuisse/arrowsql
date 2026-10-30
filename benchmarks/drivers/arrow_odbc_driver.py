"""arrow-odbc: Rust odbc-api bound to Python. Arrow reads and Arrow inserts; no row API."""

from __future__ import annotations

from drivers.base import Driver, duckdb_aggregate
from drivers.pyodbc_driver import DRIVER_NAME
from drivers.registry import SPECS

MAX_TEXT = 100_000  # bytes; bounds nvarchar(max) buffers for the LOB dataset (values are ~50 KB)


class ArrowOdbcDriver(Driver):
    name = "arrow_odbc"
    supports = SPECS["arrow_odbc"].caps

    @classmethod
    def version(cls):
        import importlib.metadata as m

        return f"arrow-odbc {m.version('arrow-odbc')}"

    def connect(self):
        import arrow_odbc

        self.mod = arrow_odbc
        self.conn = arrow_odbc.connect(self.info.odbc(DRIVER_NAME))

    def close(self):
        self.conn = None

    def execute(self, sql, params=()):
        self.conn.execute(sql)

    def scalar(self, sql):
        for batch in self.conn.read_arrow_batches(sql, batch_size=1):
            return batch.column(0)[0].as_py()

    def _reader(self, sql, batch_size):
        return self.conn.read_arrow_batches(sql, batch_size=batch_size, max_text_size=MAX_TEXT)

    def fetch_arrow(self, sql, batch_size=65_536):
        import pyarrow as pa

        return pa.Table.from_batches(list(self._reader(sql, batch_size))).num_rows

    def fetch_arrow_duckdb(self, sql, batch_size=65_536):
        import pyarrow as pa

        reader = self._reader(sql, batch_size)
        return duckdb_aggregate(pa.RecordBatchReader.from_batches(reader.schema, reader))

    def connect_close(self, n):
        for _ in range(n):
            self.mod.connect(self.info.odbc(DRIVER_NAME))  # closed when garbage collected
        return n

    def bulk_insert_arrow(self, table, payload, batch, tablock):
        import pyarrow as pa

        t = payload.arrow()
        size = batch or 65_536
        reader = pa.RecordBatchReader.from_batches(t.schema, t.to_batches(max_chunksize=size))
        self.conn.insert_into_table(reader, table, chunk_size=size)
        return payload.n
