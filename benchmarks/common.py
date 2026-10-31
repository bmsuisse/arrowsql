"""Shared helpers: paths, connection info, statistics. Stdlib only."""

from __future__ import annotations

import os
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

BENCH = Path(__file__).resolve().parent
ROOT = BENCH.parent
RESULTS = BENCH / "results"
DATASETS_JSON = RESULTS / "datasets.json"
CONTAINER = "mssql-rs"
SQLTOOLS = "/opt/mssql-tools18/bin"


def main_repo() -> Path:
    """The primary checkout (first `git worktree list` entry); holds .env.local, venvs, data."""
    if os.environ.get("BENCH_MAIN"):
        return Path(os.environ["BENCH_MAIN"])
    out = subprocess.run(
        ["git", "-C", str(ROOT), "worktree", "list", "--porcelain"],
        capture_output=True, text=True, check=True,
    ).stdout
    return Path(out.splitlines()[0].split(" ", 1)[1])


def load_env() -> None:
    """Populate os.environ from <main>/.env.local unless DB_CONNECTION_STRING is already set."""
    if os.environ.get("DB_CONNECTION_STRING"):
        return
    for line in (main_repo() / ".env.local").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.removeprefix("export ").strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class ConnInfo:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls, database: str = "nyctaxi") -> "ConnInfo":
        load_env()
        parts = {}
        for item in os.environ["DB_CONNECTION_STRING"].split(";"):
            if "=" in item:
                k, v = item.split("=", 1)
                parts[k.strip().lower()] = v.strip()
        server = parts.get("server", "localhost")
        host, _, port = server.partition(",")
        return cls(host, int(port or 1433), parts.get("uid", parts.get("user id", "sa")),
                   parts.get("pwd", parts.get("password", "")), database)

    def odbc(self, driver: str | None = None) -> str:
        head = f"Driver={{{driver}}};" if driver else ""
        return (f"{head}Server={self.host},{self.port};Database={self.database};Uid={self.user};"
                f"Pwd={self.password};Encrypt=yes;TrustServerCertificate=yes")

    def ado(self) -> str:
        return (f"Server={self.host},{self.port};Database={self.database};User Id={self.user};"
                f"Password={self.password};Encrypt=true;TrustServerCertificate=true")

    def uri(self) -> str:
        return (f"mssql://{quote(self.user, safe='')}:{quote(self.password, safe='')}@{self.host}:"
                f"{self.port}/{self.database}?encrypt=true&trust_server_certificate=true")


def summarize(samples: list[float]) -> dict:
    s = sorted(samples)
    p95 = s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))] if s else None
    return {"median": statistics.median(s) if s else None, "min": s[0] if s else None, "p95": p95}


def sqlcmd(sql: str, database: str = "nyctaxi", timeout: int = 900) -> str:
    """Run T-SQL through sqlcmd inside the container (driver-independent setup path)."""
    info = ConnInfo.from_env(database)
    args = ["-C", "-U", info.user, "-P", info.password, "-d", database, "-b", "-h", "-1", "-W", "-s", "|",
            "-i", "/dev/stdin"]
    # BCP_HOST_BIN (see drivers/bcp_driver.py) also holds a host-side sqlcmd, used over
    # the published port when `podman exec` into the container is unavailable.
    host_bin = os.environ.get("BCP_HOST_BIN")
    if host_bin:
        odbc = Path(os.environ.get("BCP_ODBC_DIR", str(Path.home() / "devops/mssql-python-rs/.bench-odbc")))
        cmd = [f"{host_bin}/sqlcmd", "-S", f"{info.host},{info.port}", *args]
        env = {**os.environ, "LD_LIBRARY_PATH": str(odbc / "lib"), "ODBCSYSINI": str(odbc / "etc")}
    else:
        cmd = ["podman", "exec", "-i", CONTAINER, f"{SQLTOOLS}/sqlcmd", "-S", "localhost", *args]
        env = None
    p = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=timeout, env=env)
    if p.returncode != 0:
        raise RuntimeError(f"sqlcmd failed: {(p.stdout + p.stderr).strip()[:500]}")
    return p.stdout
