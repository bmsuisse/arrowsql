"""Static driver metadata (stdlib only, importable from any interpreter).

`caps` is the single source of truth for what a driver can run; the runner uses
it to skip unsupported (driver, workload) pairs without spawning a process, and
adapters expose it as `Driver.supports`.

Capabilities: rows, arrow, duckdb, partitioned, insert (single-row INSERTs),
executemany, multirow (INSERT .. VALUES batches), bulk_rows, bulk_arrow,
tablock (bulk copy honours TABLOCK), pool, threads, connect, select1,
export (server-side file export; bcp only).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DBAPI = {"rows", "insert", "executemany", "multirow", "threads", "connect", "select1"}


@dataclass(frozen=True)
class Spec:
    name: str
    venv: str  # directory under the main repo ("" = no client library needed)
    module: str
    cls: str
    caps: frozenset
    note: str = ""
    odbc: bool = False  # needs unixODBC (libodbc) from <main>/.bench-odbc


SPECS: dict[str, Spec] = {s.name: s for s in [
    Spec("mssql_python", ".venv-upstream", "mssql_python_driver", "UpstreamDriver",
         frozenset(DBAPI | {"arrow", "duckdb", "bulk_rows", "bulk_arrow", "tablock", "pool"}),
         "upstream Microsoft mssql-python (C++/ODBC)"),
    Spec("rust_port", ".venv", "mssql_python_driver", "RustPortDriver",
         frozenset(DBAPI | {"arrow", "duckdb", "bulk_rows", "bulk_arrow", "tablock", "pool"}),
         "this repo: Rust/PyO3 port"),
    Spec("pyodbc", ".venv-bench-pyodbc", "pyodbc_driver", "PyodbcDriver",
         frozenset(DBAPI | {"pool"}), "pyodbc + msodbcsql18", odbc=True),
    Spec("pyodbc_fast", ".venv-bench-pyodbc", "pyodbc_driver", "PyodbcFastDriver",
         frozenset(DBAPI | {"pool"}), "pyodbc with fast_executemany=True", odbc=True),
    Spec("connectorx", ".venv-bench-connectorx", "connectorx_driver", "ConnectorXDriver",
         frozenset({"arrow", "duckdb", "partitioned"}), "connectorx (Rust, tiberius)"),
    Spec("arrow_odbc", ".venv-bench-arrow-odbc", "arrow_odbc_driver", "ArrowOdbcDriver",
         frozenset({"arrow", "duckdb", "connect"}),
         "arrow-odbc (Rust odbc-api); insert_into_table excluded: 20k taxi rows took 130 s (~150 rows/s)", odbc=True),
    Spec("pymssql", ".venv-bench-pymssql", "pymssql_driver", "PymssqlDriver",
         frozenset(DBAPI | {"bulk_rows", "tablock"}), "pymssql (FreeTDS)"),
    Spec("fastmssql", ".venv-fastmssql", "fastmssql_driver", "FastMssqlDriver",
         frozenset({"rows", "insert", "multirow", "threads", "connect", "select1"}),
         "fastmssql (tiberius, async); bulk_insert excluded: 20k rows took 16 s / 900 MB RSS"),
    Spec("bcp", "", "bcp_driver", "BcpDriver",
         frozenset({"export", "bulk_rows", "bulk_arrow", "tablock"}),
         "bcp CLI inside the container, native format: server-local speed-of-light reference"),
]}


def python_for(spec: Spec, main: Path) -> Path:
    # RUST_PORT_VENV points the rust_port driver at a worktree's own venv; without it
    # the driver runs the main checkout's build.
    override = os.environ.get("RUST_PORT_VENV") if spec.name == "rust_port" else None
    if override:
        return Path(override) / "bin" / "python"
    return main / (spec.venv or ".venv-upstream") / "bin" / "python"


def env_for(spec: Spec, main: Path, pooled: bool = False) -> dict:
    if not spec.odbc:
        return {}
    base = main / ".bench-odbc"
    return {"LD_LIBRARY_PATH": str(base / "lib"), "ODBCSYSINI": str(base / ("etc-pool" if pooled else "etc"))}
