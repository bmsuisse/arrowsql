"""Review r4: test-harness helper hardening."""

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def _load(name):
    spec = importlib.util.spec_from_file_location(name, _ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_with_database_replaces_key_case_insensitively():
    f = _load("run_tests").with_database
    assert f("Server=x;database=master;UID=u", "d") == "Server=x;database=d;UID=u"
    assert f("Server=x;Initial Catalog = prod ;PWD=p", "d") == "Server=x;Initial Catalog = d;PWD=p"
    assert f("Server=x;DATABASE={a;b};PWD=p", "d") == "Server=x;DATABASE=d;PWD=p"
    assert f("Server=x;PWD=p", "d") == "Server=x;PWD=p;Database=d"


def test_baseline_error_line_regex():
    rx = _load("check_upstream_baseline").ERROR_LINE
    m = rx.match("ERROR tests/x.py::T::test_a - RuntimeError: boom")
    assert m and m.group(1) == "tests/x.py::T::test_a"
    assert rx.match("ERROR tests/x.py::T::test_a[p1]").group(1) == "tests/x.py::T::test_a[p1]"
    assert not rx.match("PASSED tests/x.py::T::test_a")


def test_conn_str_repr_redacted():
    sys_path_conftest = _load  # noqa: F841 - conftest is imported by pytest itself
    from conftest import _RedactedStr

    s = _RedactedStr("Server=x;PWD=hunter2")
    assert "hunter2" not in repr(s) and s == "Server=x;PWD=hunter2"
