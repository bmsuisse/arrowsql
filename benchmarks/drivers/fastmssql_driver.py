"""fastmssql: async tiberius wrapper. Driven from a private event loop so workloads stay synchronous."""

from __future__ import annotations

import asyncio
from decimal import Decimal

from drivers.base import POINT_SQL, Driver
from drivers.registry import SPECS


def _v(value):
    """fastmssql has no Decimal binding: pass decimals as float (documented in the README)."""
    return float(value) if isinstance(value, Decimal) else value


def _p(sql: str) -> str:
    """'?' -> @P1, @P2, ..."""
    out, n = [], 0
    for ch in sql:
        if ch == "?":
            n += 1
            out.append(f"@P{n}")
        else:
            out.append(ch)
    return "".join(out)


class FastMssqlDriver(Driver):
    name = "fastmssql"
    supports = SPECS["fastmssql"].caps
    max_fetch_rows = 400_000  # QueryStream buffers the whole result set

    @classmethod
    def version(cls):
        import fastmssql

        return f"fastmssql {fastmssql.version()}"

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    async def _enter(self):
        await self.conn.__aenter__()

    def connect(self):
        from fastmssql import Connection

        self.loop = asyncio.new_event_loop()
        self.cs = self.info.ado()
        self.conn = Connection(self.cs)
        self._run(self._enter())

    async def _exit(self):
        await self.conn.__aexit__(None, None, None)

    def close(self):
        if self.conn is not None:
            try:
                self._run(self._exit())
            finally:
                self.conn = None
        self.loop.close()

    def execute(self, sql, params=()):
        async def go():
            await self.conn.execute(_p(sql), list(params) or None)

        self._run(go())

    def scalar(self, sql):
        async def go():
            return (await self.conn.query(sql)).rows()[0][0]

        return self._run(go())

    def fetch_rows(self, sql, params=(), mode="fetchmany", batch=10_000):
        async def go():
            s = await self.conn.query(_p(sql), list(params) or None)
            if mode == "fetchall":
                return len(s.rows())
            n = 0
            if mode == "fetchone":
                while s.fetchone() is not None:
                    n += 1
                return n
            while chunk := s.fetchmany(batch):
                n += len(chunk)
            return n

        return self._run(go())

    def fetch_rows_retained(self, sql):
        async def go():
            return len((await self.conn.query(sql)).rows())

        return self._run(go())

    def select1(self, n):
        async def go():
            for _ in range(n):
                (await self.conn.query("SELECT 1")).fetchone()

        self._run(go())
        return n

    def point_lookups(self, ids):
        sql = _p(POINT_SQL)

        async def go():
            for i in ids:
                (await self.conn.query(sql, [i])).rows()

        self._run(go())
        return len(ids)

    def connect_close(self, n):
        from fastmssql import Connection

        async def go():
            for _ in range(n):
                c = Connection(self.cs)
                await c.__aenter__()
                await c.query("SELECT 1")  # the pool connects lazily
                await c.__aexit__(None, None, None)

        self._run(go())
        return n

    def open_workers(self, n):
        self._nworkers = n  # async tasks share the pool (max 20 connections)

    def run_workers(self, ids_per_worker):
        sql = _p(POINT_SQL)

        async def worker(ids):
            for i in ids:
                (await self.conn.query(sql, [i])).rows()
            return len(ids)

        async def go():
            return sum(await asyncio.gather(*(worker(ids) for ids in ids_per_worker)))

        return self._run(go())

    def insert_single(self, sql, rows):
        from fastmssql import Transaction

        async def go():
            async with Transaction(self.cs) as t:
                s = _p(sql)
                for r in rows:
                    await t.execute(s, [_v(x) for x in r])

        self._run(go())
        return len(rows)

    def insert_multirow(self, table, rows, rows_per_stmt):
        from fastmssql import Transaction

        width = len(rows[0])
        group = "(" + ",".join("?" * width) + ")"

        async def go():
            async with Transaction(self.cs) as t:
                for start in range(0, len(rows), rows_per_stmt):
                    chunk = rows[start:start + rows_per_stmt]
                    sql = _p(f"INSERT INTO {table} VALUES " + ",".join([group] * len(chunk)))
                    await t.execute(sql, [_v(v) for r in chunk for v in r])

        self._run(go())
        return len(rows)

    def bulk_insert_rows(self, table, payload, batch, tablock):
        size = batch or 100_000
        pending: list = []

        def flush():
            async def go():
                await self.conn.bulk_insert(table, payload.columns, [[_v(x) for x in r] for r in pending])

            self._run(go())
            pending.clear()

        for chunk in payload.row_chunks(10_000):
            pending.extend(chunk)
            if len(pending) >= size:
                flush()
        if pending:
            flush()
        return payload.n
