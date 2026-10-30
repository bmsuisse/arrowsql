# Free-threaded CPython

The extension module `ddbc_bindings` is declared with `#[pymodule(gil_used = false)]`
(src/lib.rs). On a free-threaded interpreter (PEP 703, `3.13t`/`3.14t`/`3.15t`)
importing it does not re-enable the GIL. On a GIL build the attribute is a no-op.

## Build and test

    uv venv -p 3.15t .venv-ft
    uv pip install -p .venv-ft maturin pytest pytest-timeout pytest-asyncio psutil pyarrow
    .venv-ft/bin/maturin build --release --auditwheel skip -i .venv-ft/bin/python -o .venv-ft/dist
    uv pip install -p .venv-ft --no-deps --reinstall .venv-ft/dist/*.whl

Notes for 3.15t:

- `maturin develop` fails there: pyproject pins `mssql-python-rs==0.3.0`, which
  only has abi3 (GIL) wheels. Build the wheel and install it with `--no-deps`.
- `--auditwheel skip`: the box has no `patchelf`, and an unrepaired wheel is fine
  for a local build.
- `pyarrow==25.0.1` (requirements-dev.txt) has no 3.15t wheel and fails to build
  from source; unpinned pyarrow installs (26.x).

Check: `.venv-ft/bin/python -c "import sys, mssql_python.ddbc_bindings as d; print(sys._is_gil_enabled())"`
prints `False`.

Tests: `tests/port/test_free_threaded.py` (skips the GIL assertion on GIL builds).
`scripts/run_tests.py` needs `RUN_TESTS_PYTHON=<abs path to .venv-ft/bin/python>`
(a relative path fails because the runner changes directory). Run
`tests/upstream/*.py` files one per invocation: `from conftest import ...` resolves
to `tests/common_issues/conftest.py` when a common_issues file is collected first.

## Test results (3.15t, release, private DB)

| Suite | Result |
|---|---|
| tests/upstream/test_003_connection.py | 172 passed, 1 skipped |
| tests/upstream/test_004_cursor.py | 546 passed, 4 skipped |
| tests/upstream/test_004_cursor_arrow.py | 49 passed, 1 skipped, 1 deselected |
| tests/common_issues/test_rs_aio.py | 24 passed, 1 skipped |
| tests/port/test_free_threaded.py | 4 passed (5 repeated runs, all pass) |

`tests/common_issues/test_threading_gil.py` does not exist in the repo.

## Global-state audit

Every process-wide piece of mutable state, checked for data races without the GIL:

| Location | State | Verdict |
|---|---|---|
| src/logging.rs | `LEVEL: AtomicI32` | Relaxed atomic; a stale level only delays a log line. OK. |
| src/pooling.rs | `MANAGER: LazyLock<Mutex<Manager>>` (parking_lot) | All access under the lock. Fork hook uses `force_unlock` in the child only. OK. |
| src/runtime.rs | `RUNTIME: Mutex<Option<OwnedRuntime>>` | Lock held while (re)building the per-pid runtime. OK. |
| src/statement/mod.rs | `NEXT_STMT_ID: AtomicU64` | OK. |
| src/aio/hub.rs | `NEXT_REQUEST: AtomicU64`, `SQLSTATE_TO_EXCEPTION`, `DATABASE_ERROR` (`PyOnceLock`) | OK. |
| src/rowfactory.rs, src/aio/convert.rs | `ROW_TYPE: PyOnceLock<Py<PyType>>` | Init may run twice under a race; `PyOnceLock` serialises it on free-threaded builds. OK. |
| src/decode/{numeric,binary,temporal}.rs, src/params/py_types.rs | `PyOnceLock` caches for decimal/UUID/temporal types, `json.dumps` | Same as above. Temporal `Makers` are resolved once in `init` at import. OK. |
| src/arrow/sink.rs | `POW10` const table | Immutable. OK. |
| src/getinfo.rs, src/diag.rs, src/connection.rs | `&'static str` tables | Immutable. OK. |

Per-object state is held in `#[pyclass]` types. Non-frozen classes (`ParamInfo`,
`NumericData`, `ErrorInfo`) are only touched through PyO3's per-object borrow
checking. Connection and cursor-facing classes are `frozen` and keep their
mutable state behind locks.

No `static mut`, `Rc`, `RefCell` or `thread_local` is used in `src/`.

Unsafe blocks that touch CPython objects (src/rowfactory.rs, src/aio/convert.rs,
src/decode/*) only operate on objects they just allocated, or on immutable type
pointers, so they do not need the GIL for synchronisation.

Re-checked for this revision: the `unsafe` blocks in src/rowfactory.rs,
src/aio/convert.rs and src/decode/* only touch objects they allocate in the same
call; `frozen` pyclasses are compile-checked to be `Sync` (pyo3 requires it when
`gil_used = false`), so a shared non-`Sync` field would fail the build. No race
was found, so no code changed in this pass.

Possible follow-up, not a race: `runtime::runtime()` takes one global mutex on
every call to check the pid. With 8 to 16 threads this serialises a short critical
section; an atomic fast path would remove it. Not measured as a bottleneck.

## Thread scaling: 3.15t vs 3.13 GIL vs upstream 3.13

Script: `benchmarks/experiments/free_threaded_scaling.py`. Each (interpreter,
workload) runs in a fresh process under the machine-wide benchmark lock, one
warm-up repetition, then 7 to 9 timed repetitions. Connections are opened before
timing; threads start on a barrier. Taxi data on SQL Server (nyctaxi).

- `point_Nt`: N threads, each does 1000 `SELECT * FROM dbo.yellow_trips WHERE trip_id = ?`
  on random keys, own connection per thread.
- `fetchall_8t`: 8 threads, each `SELECT * FROM dbo.yellow_trips_100k` + `fetchall()`.

Medians from the most-repeated run of each workload (box load average 16 to 34
from other agents; across four runs, the 8- and 16-thread numbers were within
about 20%, the single-thread and fetchall numbers within about 15%):

| Workload | 3.15t (rust port) | 3.13 GIL (rust port) | 3.13 upstream (mssql-python) | 3.15t vs 3.13 GIL | 3.15t vs upstream |
|---|---|---|---|---|---|
| point_1t, s (queries/s) | 0.339 (2953) | 0.352 (2839) | 0.346 (2889) | 1.04x | 1.02x |
| point_8t, s (queries/s) | 0.499 (16023) | 1.472 (5434) | 1.832 (4366) | 2.9x | 3.7x |
| point_16t, s (queries/s) | 0.854 (18728) | 2.710 (5904) | 3.758 (4258) | 3.2x | 4.4x |
| fetchall_8t, s (rows/s) | 1.421 (562872) | 3.019 (265029) | 3.057 (261734) | 2.1x | 2.2x |

Scaling on 3.15t (throughput): 1 thread 2953 queries/s, 8 threads about 16000
(5.4x), 16 threads about 18700 (6.3x). Going from 8 to 16 threads adds about 15%.
The 3.13 GIL build does not scale at all: 2839, 5434 and 5904 queries/s.
Other agents kept the box loaded, and SQL Server runs on the same machine, so the
flattening above 8 threads is not isolated to the driver.

Summary: no GIL means 8-thread point lookups run about 3x faster than the GIL build
and about 3.7x faster than upstream, and 8-thread fetchall is about 2x faster than
either. Single-thread speed is unchanged.
