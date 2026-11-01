"""bcp CLI inside the SQL Server container, native format (-n): the server-local speed-of-light.

Not a Python client: it ignores the Python rows/Arrow data it is handed and moves the
same data from server-side files, so it bounds what any client library can reach.
  * "export": `bcp "<query>" queryout file -n` (read side)
  * bulk_*  : `bcp table in file -n [-b batch] [-h TABLOCK]`, the file staged untimed from
              the same source rows (prepare_payload).

Timing caveats: every bcp call includes `podman exec` + process start + login (the fixed cost;
measure it with workload write.bcp_fixed_cost.noop or `noop()`), so small loads
overstate bcp; `last_clock_ms` is bcp's own "Clock Time (ms.) Total" (excludes podman exec and
process start). `-h TABLOCK` is documented as Windows-only for bcp, but the Linux build also
takes the hint: sys.dm_tran_locks shows a table-level BU lock during a hinted load and
row/page-level IX locks only without it (see benchmarks/RESULTS.md, "bcp baseline").
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from common import CONTAINER, ENGINE, SQLTOOLS, sqlcmd
from drivers.base import Driver
from drivers.registry import SPECS

# BCP_HOST_BIN=<dir with bcp> switches to a host-side bcp talking to localhost:1433 over the
# published port (files in BCP_HOST_DIR); for when `podman exec` into the container is unavailable.
# Host mode adds the port-forward hop that the Python clients also pay, in-container mode does not.
HOST_BIN = os.environ.get("BCP_HOST_BIN")
HOST_DIR = os.environ.get("BCP_HOST_DIR", "/tmp")
EXPORT_FILE = f"{HOST_DIR}/bench_export.bcp" if HOST_BIN else "/tmp/bench_export.bcp"


class BcpDriver(Driver):
    name = "bcp"
    supports = SPECS["bcp"].caps

    @classmethod
    def version(cls):
        p = subprocess.run(cls._prefix() + ["-v"], capture_output=True, text=True, env=cls._env())
        return " ".join(p.stdout.split())[:80] or "bcp"

    @staticmethod
    def _prefix():
        return [f"{HOST_BIN}/bcp"] if HOST_BIN else [ENGINE, "exec", CONTAINER, f"{SQLTOOLS}/bcp"]

    @staticmethod
    def _env():
        if not HOST_BIN:
            return None
        odbc = Path(os.environ.get("BCP_ODBC_DIR", str(Path.home() / "devops/mssql-python-rs/.bench-odbc")))
        return {**os.environ, "LD_LIBRARY_PATH": str(odbc / "lib"), "ODBCSYSINI": str(odbc / "etc")}

    def connect(self):
        self.conn = True
        self._files: set[str] = set()
        if HOST_BIN:  # DDL/scalar queries go through the interpreter's mssql_python instead of sqlcmd
            import mssql_python

            self._db = mssql_python.connect(self.info.odbc(), autocommit=True)
        else:
            sqlcmd("SELECT 1")  # connectivity check

    def close(self):
        if self.conn:
            if self._files and HOST_BIN:
                for f in self._files:
                    Path(f).unlink(missing_ok=True)
            elif self._files:
                subprocess.run([ENGINE, "exec", CONTAINER, "rm", "-f", *sorted(self._files)])
            self.conn = None

    def execute(self, sql, params=()):
        if HOST_BIN:
            cur = self._db.cursor()
            cur.execute(sql)
            cur.close()
            return
        sqlcmd(sql)

    def scalar(self, sql):
        if HOST_BIN:
            cur = self._db.cursor()
            cur.execute(sql)
            v = cur.fetchone()[0]
            cur.close()
            return v
        return sqlcmd("SET NOCOUNT ON; " + sql).strip().splitlines()[0]

    def _bcp(self, *args) -> int:
        i = self.info
        server = f"localhost,{i.port}" if HOST_BIN else "localhost"
        cmd = [*self._prefix(), *args, "-S", server, "-U", i.user, "-P", i.password, "-u", "-a", "16384"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=3600, env=self._env())
        m = re.search(r"(\d+) rows copied", p.stdout)
        c = re.search(r"Total\s*:\s*(\d+)", p.stdout)
        self.last_clock_ms = int(c.group(1)) if c else None
        if p.returncode != 0 or not m:
            raise RuntimeError(f"bcp failed: {(p.stdout + p.stderr).strip()[-400:]}")
        return int(m.group(1))

    def noop(self):
        """Fixed cost of one bcp call: podman exec + process start + login + a one-row query."""
        self._files.add(EXPORT_FILE)
        return self._bcp("SELECT 1", "queryout", EXPORT_FILE, "-n", "-d", self.info.database)

    def export_native(self, sql):
        self._files.add(EXPORT_FILE)
        return self._bcp(sql, "queryout", EXPORT_FILE, "-n", "-d", self.info.database)

    def prepare_payload(self, payload):
        payload.bcp_file = f"{HOST_DIR}/bench_payload_{payload.n}.bcp"
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
