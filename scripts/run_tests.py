"""Run pytest against a private database without sourcing scripts/env.sh.

    python scripts/run_tests.py <db-id> <pytest args...>
    python scripts/run_tests.py sync_io tests/upstream/test_004_cursor.py -x
    RUN_TESTS_PYTHON=.venv-upstream/bin/python python scripts/run_tests.py me tests/common_issues/test_x.py

Uses this worktree's .venv, loads DB_CONNECTION_STRING from the main
checkout's .env.local and points it at database rs_agent_<db-id> (created if
missing). Test paths are relative to the worktree root.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main_checkout() -> Path:
    out = subprocess.run(["git", "-C", str(ROOT), "worktree", "list", "--porcelain"],
                         capture_output=True, text=True, check=True).stdout
    return Path(out.splitlines()[0].split(" ", 1)[1])


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"')
    return env


def ensure_database(python: Path, conn_str: str, name: str) -> None:
    code = (
        "import sys, mssql_python\n"
        "c = mssql_python.connect(sys.argv[1], autocommit=True)\n"
        f"c.cursor().execute(\"IF DB_ID('{name}') IS NULL CREATE DATABASE [{name}]\")\n"
    )
    subprocess.run([str(python), "-I", "-c", code, conn_str], check=True)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    db = "rs_agent_" + re.sub(r"\W", "_", sys.argv[1])
    env = {**os.environ, **load_env(main_checkout() / ".env.local")}
    # RUN_TESTS_PYTHON selects another interpreter, e.g. <main>/.venv-upstream/bin/python for ground truth.
    python = Path(os.environ.get("RUN_TESTS_PYTHON", ROOT / ".venv" / "bin" / "python"))
    ensure_database(python, env["DB_CONNECTION_STRING"], db)
    env["DB_CONNECTION_STRING"] = env["DB_CONNECTION_STRING"].replace("Database=master", f"Database={db}")
    args = [str(ROOT / a) if (ROOT / a.split("::")[0]).exists() else a for a in sys.argv[2:]]
    cmd = [str(python), "-m", "pytest", "-q", "-p", "no:cacheprovider", "--timeout", "120", *args]
    return subprocess.run(cmd, env=env, cwd=ROOT / "tests" / "upstream").returncode


if __name__ == "__main__":
    sys.exit(main())
