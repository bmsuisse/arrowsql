# Free-threaded CPython

The extension module `ddbc_bindings` is declared with `#[pymodule(gil_used = false)]`
(src/lib.rs). On a free-threaded interpreter (PEP 703, `3.13t`/`3.14t`/`3.15t`)
importing it does not re-enable the GIL. On a GIL build the attribute is a no-op.

## Build and test

    uv venv -p 3.15t .venv-ft
    VIRTUAL_ENV=$PWD/.venv-ft maturin develop --release

Tests: `tests/port/test_free_threaded.py` (skips the GIL assertion on GIL builds).

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

## Open items

- The Python layer (vendored `python/mssql_python`) is not audited here; it is
  owned by the upstream package.
- Benchmark results for 3.15t vs the 3.13 GIL build are in
  benchmarks/RESULTS.md when measured.
