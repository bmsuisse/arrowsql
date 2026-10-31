"""Adapters for upstream mssql-python and the Rust port (same `mssql_python` import name).

Which one is measured depends on the interpreter (.venv-upstream vs .venv). The
port additionally exposes fetchall_arrow()/arrow_stream(); the adapter prefers
them when present and falls back to the upstream-compatible cursor.arrow() API.
"""

from __future__ import annotations

import os

from drivers.base import DbApiDriver, duckdb_aggregate
from drivers.registry import SPECS


class UpstreamDriver(DbApiDriver):
    name = "mssql_python"
    supports = SPECS["mssql_python"].caps

    @classmethod
    def version(cls):
        import importlib.metadata as m

        import mssql_python

        try:
            v = m.version("mssql-python")
        except Exception:
            v = getattr(mssql_python, "__version__", "?")
        where = "in-repo port" if "site-packages" not in mssql_python.__file__ else "PyPI wheel"
        return f"mssql-python {v} ({where})"

    def _connect(self, autocommit):
        import mssql_python

        # BENCH_CONN_EXTRA (e.g. "Packet Size=32767;") lets A/B runs vary connection options.
        return mssql_python.connect(self.info.odbc() + os.environ.get("BENCH_CONN_EXTRA", ""),
                                    autocommit=autocommit)

    # -- arrow ---------------------------------------------------------------
    def _arrow_table(self, cur, batch_size):
        if hasattr(cur, "fetchall_arrow"):
            return cur.fetchall_arrow()
        return cur.arrow(batch_size=batch_size)

    def fetch_arrow(self, sql, batch_size=65_536):
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return self._arrow_table(cur, batch_size).num_rows
        finally:
            cur.close()

    def fetch_arrow_duckdb(self, sql, batch_size=65_536):
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            stream = cur.arrow_stream(batch_size) if hasattr(cur, "arrow_stream") else cur.arrow_reader(batch_size)
            return duckdb_aggregate(stream)
        finally:
            cur.close()

    # -- pooling ---------------------------------------------------------------
    def connect_close(self, n):
        import mssql_python

        mssql_python.pooling(enabled=False)  # upstream auto-enables pooling; measure raw connects
        return super().connect_close(n)

    def pooled_connect_close(self, n):
        import mssql_python

        mssql_python.pooling(max_size=20, idle_timeout=600, enabled=True)
        return super().connect_close(n)

    # -- bulk ------------------------------------------------------------------
    def bulk_insert_rows(self, table, payload, batch, tablock):
        cur = self.conn.cursor()
        try:
            res = cur.bulkcopy(table, payload.rows(), batch_size=batch, timeout=0, table_lock=tablock)
        finally:
            cur.close()
        return payload.n

    def bulk_insert_arrow(self, table, payload, batch, tablock):
        t = payload.arrow()
        cur = self.conn.cursor()
        try:
            cur.bulkcopy_arrow(table, t, batch_size=batch, timeout=0, table_lock=tablock,
                               column_mappings=list(enumerate(t.column_names)))
        finally:
            cur.close()
        return payload.n


class RustPortDriver(UpstreamDriver):
    name = "rust_port"
    supports = SPECS["rust_port"].caps
