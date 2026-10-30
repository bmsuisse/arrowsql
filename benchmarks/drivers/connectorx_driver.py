"""connectorx: Rust, tiberius-based, Arrow-native, read-only (no write path)."""

from __future__ import annotations

from drivers.base import Driver, duckdb_aggregate
from drivers.registry import SPECS


class ConnectorXDriver(Driver):
    name = "connectorx"
    supports = SPECS["connectorx"].caps

    @classmethod
    def version(cls):
        import connectorx

        return f"connectorx {connectorx.__version__}"

    def connect(self):
        import connectorx

        self.cx = connectorx
        self.uri = self.info.uri()
        self.conn = True
        self.cx.read_sql(self.uri, "SELECT 1", return_type="arrow")  # connectivity check, untimed

    def close(self):
        self.conn = None

    def scalar(self, sql):
        return self.cx.read_sql(self.uri, sql, return_type="arrow").column(0)[0].as_py()

    def fetch_arrow(self, sql, batch_size=65_536):
        return self.cx.read_sql(self.uri, sql, return_type="arrow").num_rows

    def fetch_arrow_duckdb(self, sql, batch_size=65_536):
        reader = self.cx.read_sql(self.uri, sql, return_type="arrow_stream", batch_size=batch_size)
        return duckdb_aggregate(reader)

    def fetch_arrow_partitioned(self, sql, part_col, parts):
        return self.cx.read_sql(self.uri, sql, partition_on=part_col, partition_num=parts,
                                return_type="arrow").num_rows
