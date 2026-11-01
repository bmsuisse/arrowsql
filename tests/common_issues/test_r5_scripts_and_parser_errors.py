"""r5 F2 / F3 / F10: error text never echoes password fragments; dev scripts handle braces and spaces.

No database needed.
"""

import importlib.util
import subprocess
from pathlib import Path

import pytest
from mssql_python.connection_string_parser import _ConnectionStringParser
from mssql_python.exceptions import ConnectionStringParseError

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def _load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


run_tests = _load("run_tests")
check_baseline = _load("check_upstream_baseline")


def _parse_error(conn_str, validate=True):
    with pytest.raises(ConnectionStringParseError) as exc:
        _ConnectionStringParser(validate_keywords=validate)._parse(conn_str)
    return str(exc.value)


@pytest.mark.parametrize(
    "conn_str, leaked",
    [
        ("PWD=abc;SecretToken=;UID=u", "secrettoken"),
        ("PWD=abc;SecretToken=x;UID=u", "secrettoken"),
        ("PWD=abc;Server SecretTail;UID=u", "SecretTail"),
        ("PWD=abc;SecretTail;UID=u", "SecretTail"),
        ("PWD=abc;SecretToken=x;SecretToken=y", "secrettoken"),
        ("Password=abc;SecretToken=x", "secrettoken"),
    ],
)
def test_parse_errors_do_not_echo_password_fragments(conn_str, leaked):
    assert leaked.lower() not in _parse_error(conn_str).lower()


def test_unknown_keyword_still_named_without_a_password():
    msg = _parse_error("Server=localhost;UnknownParam=value")
    assert "Unknown keyword 'unknownparam'" in msg


def test_documented_unsupported_keyword_named_even_with_a_password():
    assert "'application name'" in _parse_error("PWD=abc;Application Name=x").lower()


def test_known_keyword_is_still_named_with_a_password():
    assert "Duplicate keyword 'server'" in _parse_error("PWD=abc;Server=a;Server=b")


@pytest.mark.parametrize(
    "conn_str, expected",
    [
        ("Server=s;Database=old;UID=u", "Server=s;Database=new;UID=u"),
        ("Server=s;Initial  Catalog=old", "Server=s;Initial  Catalog=new"),
        ("Server=s", "Server=s;Database=new"),
        ("APP={x;Database=master;y};Database=old", "APP={x;Database=master;y};Database=new"),
        ("PWD={a;Database=b};Server=s", "PWD={a;Database=b};Server=s;Database=new"),
        ("PWD={a}};Database=b};Database=old", "PWD={a}};Database=b};Database=new"),
        ("PWD={a;Database=b}", "PWD={a;Database=b};Database=new"),
    ],
)
def test_with_database_respects_braced_values(conn_str, expected):
    assert run_tests.with_database(conn_str, "new") == expected


def test_redact_removes_connection_string_and_password():
    conn = "Server=s;PWD={Top;Secret}};x};UID=u"
    text = f"failed: {conn} and Top;Secret}};x"
    out = run_tests.redact(text, conn)
    assert "Secret" not in out and "Server=s" not in out


def test_ensure_database_failure_is_redacted(monkeypatch):
    conn = "Server=s;PWD=hunter2-secret;UID=u"
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd
        seen["env"] = kwargs["env"]
        return subprocess.CompletedProcess(cmd, 3, "", f"boom {conn} hunter2-secret")

    monkeypatch.setattr(run_tests.subprocess, "run", fake_run)
    with pytest.raises(SystemExit) as exc:
        run_tests.ensure_database(Path("python"), conn, "rs_agent_x")
    assert "hunter2-secret" not in str(exc.value)
    assert "exit 3" in str(exc.value)
    assert not any("hunter2-secret" in str(a) for a in seen["cmd"])
    assert seen["env"]["RUN_TESTS_SETUP_CONN"] == conn


NODE = "test_004_cursor.py::test_x[values0-Small decimals with scientific notation]"


@pytest.mark.parametrize(
    "line, expected",
    [
        (f"ERROR {NODE}", NODE),
        (f"ERROR {NODE} - Failed: teardown boom", NODE),
        ("ERROR test_a.py::test_b - RuntimeError: x", "test_a.py::test_b"),
        ("PASSED test_a.py::test_b", None),
    ],
)
def test_parse_error_line_handles_node_ids_with_spaces(line, expected):
    assert check_baseline.parse_error_line(line, {NODE}) == expected
