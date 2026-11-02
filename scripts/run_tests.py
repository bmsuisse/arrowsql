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


_DB_KEYS = {"database", "initial catalog"}
_SECRET_KEYS = {"pwd", "password"}


def split_attributes(conn_str: str) -> list[str]:
    """Split a connection string at its top-level ';'.

    A value that starts with '{' runs to the matching '}' ('}}' is an escaped
    '}') and may hold ';'. The segments joined with ';' give back conn_str.
    """
    segments: list[str] = []
    start = i = 0
    n = len(conn_str)
    in_value = False  # past the first '=' of the segment
    while i < n:
        ch = conn_str[i]
        if ch == ";":
            segments.append(conn_str[start:i])
            start = i = i + 1
            in_value = False
        elif ch == "=" and not in_value:
            in_value = True
            i += 1
            while i < n and conn_str[i] in " \t":
                i += 1
            if i < n and conn_str[i] == "{":
                i += 1
                while i < n:
                    if conn_str[i] == "}":
                        if conn_str[i + 1 : i + 2] == "}":
                            i += 2
                            continue
                        i += 1
                        break
                    i += 1
        else:
            i += 1
    segments.append(conn_str[start:])
    return segments


def _attr_key(segment: str) -> str:
    if "=" not in segment:
        return ""
    return re.sub(r"\s+", " ", segment.split("=", 1)[0].strip().lower())


def with_database(conn_str: str, name: str) -> str:
    """Return conn_str with its Database / Initial Catalog key set to ``name``.

    Keys are matched case-insensitively and only at a top-level key position
    (a braced value may hold ';' and text that looks like a key). When neither
    key is present ``Database=<name>`` is appended.
    """
    segments = split_attributes(conn_str)
    found = False
    for idx, seg in enumerate(segments):
        if _attr_key(seg) in _DB_KEYS:
            segments[idx] = seg.split("=", 1)[0] + "=" + name
            found = True
    if found:
        return ";".join(segments)
    return conn_str.rstrip(";") + f";Database={name}"


def _plain_value(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("{"):
        return raw[1:].rsplit("}", 1)[0].replace("}}", "}")
    return raw


def redact(text: str, conn_str: str) -> str:
    """Remove the connection string and its password values from ``text``."""
    for seg in split_attributes(conn_str):
        if _attr_key(seg) in _SECRET_KEYS:
            secret = _plain_value(seg.split("=", 1)[1])
            if secret:
                text = text.replace(secret, "***")
    return text.replace(conn_str, "<connection string>")


def ensure_database(python: Path, conn_str: str, name: str) -> None:
    """Create database ``name``.

    The connection string reaches the child through its environment, not argv,
    and a failure prints a redacted message only.
    """
    code = (
        "import os, mssql_python\n"
        "c = mssql_python.connect(os.environ['RUN_TESTS_SETUP_CONN'], autocommit=True)\n"
        f"c.cursor().execute(\"IF DB_ID('{name}') IS NULL CREATE DATABASE [{name}]\")\n"
    )
    proc = subprocess.run(
        [str(python), "-I", "-c", code],
        env={**os.environ, "RUN_TESTS_SETUP_CONN": conn_str},
        capture_output=True,
        text=True,
    )
    if proc.returncode:
        tail = redact(proc.stderr, conn_str).strip().splitlines()[-5:]
        sys.exit(f"database setup for {name} failed (exit {proc.returncode}):\n" + "\n".join(tail))


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    db = "rs_agent_" + re.sub(r"\W", "_", sys.argv[1])
    env = {**os.environ, **load_env(main_checkout() / ".env.local")}
    # RUN_TESTS_PYTHON selects another interpreter, e.g. <main>/.venv-upstream/bin/python for ground truth.
    python = Path(os.environ.get("RUN_TESTS_PYTHON", ROOT / ".venv" / "bin" / "python"))
    ensure_database(python, env["DB_CONNECTION_STRING"], db)
    env["DB_CONNECTION_STRING"] = with_database(env["DB_CONNECTION_STRING"], db)
    args = [str(ROOT / a) if (ROOT / a.split("::")[0]).exists() else a for a in sys.argv[2:]]
    cmd = [str(python), "-m", "pytest", "-q", "-p", "no:cacheprovider", "--timeout", "120", *args]
    return subprocess.run(cmd, env=env, cwd=ROOT / "tests" / "upstream").returncode


if __name__ == "__main__":
    sys.exit(main())
