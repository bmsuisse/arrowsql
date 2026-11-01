"""Review r4: test-harness helper hardening."""

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def _load(name):
    return _load_path(_ROOT / "scripts" / f"{name}.py", name)


def _load_path(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_with_database_replaces_key_case_insensitively():
    f = _load("run_tests").with_database
    assert f("Server=x;database=master;UID=u", "d") == "Server=x;database=d;UID=u"
    assert f("Server=x;Initial Catalog = prod ;PWD=p", "d") == "Server=x;Initial Catalog =d;PWD=p"
    assert f("Server=x;DATABASE={a;b};PWD=p", "d") == "Server=x;DATABASE=d;PWD=p"
    assert f("Server=x;PWD=p", "d") == "Server=x;PWD=p;Database=d"


def test_baseline_error_line_parsing():
    f = _load("check_upstream_baseline").parse_error_line
    assert f("ERROR tests/x.py::T::test_a - RuntimeError: boom", set()) == "tests/x.py::T::test_a"
    assert f("ERROR tests/x.py::T::test_a[p1]", set()) == "tests/x.py::T::test_a[p1]"
    assert f("PASSED tests/x.py::T::test_a", set()) is None


def test_conn_str_repr_redacted():
    sys_path_conftest = _load  # noqa: F841 - conftest is imported by pytest itself
    conftest = _load_path(_ROOT / "tests" / "common_issues" / "conftest.py", "ci_conftest")
    _RedactedStr = conftest._RedactedStr

    s = _RedactedStr("Server=x;PWD=hunter2")
    assert "hunter2" not in repr(s) and s == "Server=x;PWD=hunter2"


def test_logging_module_has_no_bare_except():
    import ast

    src = (_ROOT / "python" / "arrowsql" / "logging.py").read_text(encoding="utf-8")
    bare = [n.lineno for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ExceptHandler) and n.type is None]
    assert bare == []


def test_parallel_probe_failure_is_logged(tmp_path):
    from unittest import mock

    from arrowsql import arrow_parallel

    class _Cur:
        def execute(self, *a, **k):
            raise RuntimeError("boom")

    with mock.patch.object(arrow_parallel.logger, "debug") as dbg:
        assert arrow_parallel._probe_meta(_Cur(), "SELECT a FROM t", "a") == (None, True)
    assert dbg.called


def test_workflows_pin_actions_and_restrict_publish():
    import re

    wf = _ROOT / ".github" / "workflows"
    for f in wf.glob("*.yml"):
        for line in f.read_text(encoding="utf-8").splitlines():
            m = re.search(r"uses:\s*([\w.-]+/[\w./-]+)@(\S+)", line)
            if m:
                assert re.fullmatch(r"[0-9a-f]{40}", m.group(2)), f"{f.name}: {line.strip()}"
    release = (wf / "workflow.yml").read_text(encoding="utf-8")
    assert "github.ref == 'refs/heads/main'" in release
    for line in release.splitlines():
        if "needs.plan.outputs.version" in line:
            assert line.strip().startswith(("VERSION:", "version:")), line
