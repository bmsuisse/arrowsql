"""bcp CLI inside the SQL Server container, native format (-n): the server-local speed-of-light.

Not a Python client: it ignores the Python rows/Arrow data it is handed and moves the
same data from server-side files, so it bounds what any client library can reach.
  * "export": `bcp "<query>" queryout file -n` (read side)
  * bulk_*  : `bcp table in file -n [-b batch] [-h TABLOCK]`, the file staged untimed from
              the same source rows (prepare_payload).
"""

from __future__ import annotations

import re
import subprocess

from common import CONTAINER, SQLTOOLS, sqlcmd
from drivers.base import Driver
from drivers.registry import SPECS

EXPORT_FILE = "/tmp/bench_export.bcp"


class BcpDriver(Driver):
    name = "bcp"
    supports = SPECS["bcp"].caps

    @classmethod
    def version(cls):
        p = subprocess.run(["podman", "exec", CONTAINER, f"{SQLTOOLS}/bcp", "-v"], capture_output=True, text=True)
        return " ".join(p.stdout.split())[:80] or "bcp"

    def connect(self):
        self.conn = True
        self._files: set[str] = set()
        sqlcmd("SELECT 1")  # connectivity check

    def close(self):
        if self.conn:
            if self._files:
                subprocess.run(["podman", "exec", CONTAINER, "rm", "-f", *sorted(self._files)])
            self.conn = None

    def execute(self, sql, params=()):
        sqlcmd(sql)

    def scalar(self, sql):
        return sqlcmd("SET NOCOUNT ON; " + sql).strip().splitlines()[0]

    def _bcp(self, *args) -> int:
        i = self.info
        cmd = ["podman", "exec", CONTAINER, f"{SQLTOOLS}/bcp", *args, "-S", "localhost", "-U", i.user,
               "-P", i.password, "-u", "-a", "16384"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        m = re.search(r"(\d+) rows copied", p.stdout)
        if p.returncode != 0 or not m:
            raise RuntimeError(f"bcp failed: {(p.stdout + p.stderr).strip()[-400:]}")
        return int(m.group(1))

    def export_native(self, sql):
        self._files.add(EXPORT_FILE)
        return self._bcp(sql, "queryout", EXPORT_FILE, "-n", "-d", self.info.database)

    def prepare_payload(self, payload):
        payload.bcp_file = f"/tmp/bench_payload_{payload.n}.bcp"
        self._files.add(payload.bcp_file)
        self._bcp(payload.source_sql, "queryout", payload.bcp_file, "-n", "-d", self.info.database)

    def _bulk_in(self, table, payload, batch, tablock):
        args = [table, "in", payload.bcp_file, "-n", "-d", self.info.database]
        if batch:
            args += ["-b", str(batch)]
        if tablock:
            args += ["-h", "TABLOCK"]
        n = self._bcp(*args)
        assert n == payload.n, (n, payload.n)
        return n

    bulk_insert_rows = _bulk_in
    bulk_insert_arrow = _bulk_in
