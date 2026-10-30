"""Common driver interface used by every workload.

Adapters live in benchmarks/drivers/<module>.py, are registered in
registry.SPECS, and only implement what they can do (the `supports` set).
Workloads never touch a client library directly.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from common import ConnInfo
from drivers.registry import SPECS

POINT_SQL = "SELECT * FROM dbo.yellow_trips WHERE trip_id = ?"
DUCK_AGG = "SELECT PULocationID, count(*) AS n, avg(total_amount) AS avg_total FROM t GROUP BY 1"


class Unsupported(Exception):
    """Raised by an adapter for an operation it cannot perform; the runner records a skip."""


def duckdb_aggregate(source) -> int:
    """Register an Arrow stream/table in DuckDB and run the taxi aggregate; returns group count."""
    import duckdb

    con = duckdb.connect()
    try:
        con.register("t", source)
        return len(con.execute(DUCK_AGG).fetchall())
    finally:
        con.close()


class Driver:
    name = ""
    supports: frozenset = frozenset()
    max_fetch_rows = None  # set by drivers that materialize whole results

    def __init__(self, info: ConnInfo, pooled: bool = False):
        self.info = info
        self.pooled = pooled  # set before the first connect for pooled-connect workloads
        self.conn = None

    # -- meta ------------------------------------------------------------
    @classmethod
    def version(cls) -> str:
        return "?"

    # -- lifecycle -------------------------------------------------------
    def connect(self) -> None:
        raise Unsupported

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    # -- statements (setup/verification; also used by write workloads) ------
    def execute(self, sql: str, params: tuple = ()) -> None:
        raise Unsupported

    def scalar(self, sql: str):
        raise Unsupported

    def set_autocommit(self, flag: bool) -> None:
        raise Unsupported

    def commit(self) -> None:
        raise Unsupported

    # -- reads -------------------------------------------------------------
    def fetch_rows(self, sql: str, params: tuple = (), mode: str = "fetchmany", batch: int = 10_000) -> int:
        raise Unsupported

    def fetch_rows_retained(self, sql: str) -> int:
        """fetchall() keeping every row alive until return (memory-peak workload)."""
        raise Unsupported

    def fetch_arrow(self, sql: str, batch_size: int = 65_536) -> int:
        raise Unsupported

    def fetch_arrow_duckdb(self, sql: str, batch_size: int = 65_536) -> int:
        raise Unsupported

    def fetch_arrow_partitioned(self, sql: str, part_col: str, parts: int) -> int:
        raise Unsupported

    def export_native(self, sql: str) -> int:
        raise Unsupported

    # -- latency -----------------------------------------------------------
    def connect_close(self, n: int) -> int:
        raise Unsupported

    def pooled_connect_close(self, n: int) -> int:
        raise Unsupported

    def select1(self, n: int) -> int:
        raise Unsupported

    def point_lookups(self, ids) -> int:
        raise Unsupported

    def open_workers(self, n: int) -> None:
        raise Unsupported

    def run_workers(self, ids_per_worker: list) -> int:
        raise Unsupported

    def close_workers(self) -> None:
        pass

    # -- writes --------------------------------------------------------------
    def insert_single(self, sql: str, rows: list) -> int:
        """One INSERT per row, autocommit off, one commit at the end."""
        raise Unsupported

    def executemany(self, sql: str, rows: list) -> int:
        raise Unsupported

    def insert_multirow(self, table: str, rows: list, rows_per_stmt: int) -> int:
        """INSERT .. VALUES (?,..),(?,..).. with parameters, N rows per statement."""
        raise Unsupported

    def prepare_payload(self, payload) -> None:
        """Untimed hook before the first timed bulk write (bcp stages its native file here)."""

    def bulk_insert_rows(self, table: str, payload, batch: int, tablock: bool) -> int:
        raise Unsupported

    def bulk_insert_arrow(self, table: str, payload, batch: int, tablock: bool) -> int:
        raise Unsupported


class DbApiDriver(Driver):
    """Shared implementation for PEP-249 style drivers (mssql_python, pyodbc, pymssql)."""

    #: set by subclasses
    def _connect(self, autocommit: bool):
        raise NotImplementedError

    def ph(self, sql: str) -> str:
        """Translate '?' placeholders to the driver's paramstyle."""
        return sql

    def connect(self) -> None:
        self.conn = self._connect(True)

    def set_autocommit(self, flag: bool) -> None:
        self.conn.autocommit = flag

    def commit(self) -> None:
        self.conn.commit()

    def execute(self, sql, params=()):
        cur = self.conn.cursor()
        try:
            cur.execute(self.ph(sql), params) if params else cur.execute(sql)
        finally:
            cur.close()

    def scalar(self, sql):
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchone()[0]
        finally:
            cur.close()

    def fetch_rows(self, sql, params=(), mode="fetchmany", batch=10_000):
        cur = self.conn.cursor()
        try:
            cur.execute(self.ph(sql), params) if params else cur.execute(sql)
            if mode == "fetchall":
                return len(cur.fetchall())
            n = 0
            if mode == "fetchone":
                while cur.fetchone() is not None:
                    n += 1
                return n
            while True:
                chunk = cur.fetchmany(batch)
                if not chunk:
                    return n
                n += len(chunk)
        finally:
            cur.close()

    def fetch_rows_retained(self, sql):
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return len(rows)
        finally:
            cur.close()

    def select1(self, n):
        cur = self.conn.cursor()
        for _ in range(n):
            cur.execute("SELECT 1")
            cur.fetchone()
        cur.close()
        return n

    def point_lookups(self, ids):
        cur = self.conn.cursor()
        sql = self.ph(POINT_SQL)
        n = 0
        for i in ids:
            cur.execute(sql, (i,))
            cur.fetchall()
            n += 1
        cur.close()
        return n

    def connect_close(self, n):
        for _ in range(n):
            self._connect(True).close()
        return n

    # threads: one connection per worker, opened untimed
    def open_workers(self, n):
        self._workers = [self._connect(True) for _ in range(n)]

    def run_workers(self, ids_per_worker):
        sql = self.ph(POINT_SQL)
        barrier = threading.Barrier(len(self._workers))

        def work(args):
            conn, ids = args
            cur = conn.cursor()
            barrier.wait()
            for i in ids:
                cur.execute(sql, (i,))
                cur.fetchall()
            cur.close()
            return len(ids)

        with ThreadPoolExecutor(len(self._workers)) as pool:
            return sum(pool.map(work, zip(self._workers, ids_per_worker)))

    def close_workers(self):
        for c in getattr(self, "_workers", []):
            try:
                c.close()
            except Exception:
                pass
        self._workers = []

    def insert_single(self, sql, rows):
        self.set_autocommit(False)
        try:
            cur = self.conn.cursor()
            sql = self.ph(sql)
            for r in rows:
                cur.execute(sql, r)
            self.conn.commit()
            cur.close()
        finally:
            self.set_autocommit(True)
        return len(rows)

    def executemany(self, sql, rows):
        self.set_autocommit(False)
        try:
            cur = self.conn.cursor()
            cur.executemany(self.ph(sql), rows)
            self.conn.commit()
            cur.close()
        finally:
            self.set_autocommit(True)
        return len(rows)

    def insert_multirow(self, table, rows, rows_per_stmt):
        width = len(rows[0])
        group = "(" + ",".join("?" * width) + ")"
        full_sql = self.ph(f"INSERT INTO {table} VALUES " + ",".join([group] * rows_per_stmt))
        self.set_autocommit(False)
        try:
            cur = self.conn.cursor()
            for start in range(0, len(rows), rows_per_stmt):
                chunk = rows[start:start + rows_per_stmt]
                if len(chunk) == rows_per_stmt:
                    sql = full_sql
                else:
                    sql = self.ph(f"INSERT INTO {table} VALUES " + ",".join([group] * len(chunk)))
                cur.execute(sql, tuple(v for r in chunk for v in r))
            self.conn.commit()
            cur.close()
        finally:
            self.set_autocommit(True)
        return len(rows)
