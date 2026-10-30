# Packaging

The wheel has **no required runtime dependencies**. `pip install` of the core
brings only the compiled `ddbc_bindings` extension and the vendored
`mssql_python` Python layer. Optional stacks are extras.

| extra | pulls in | needed for |
|---|---|---|
| (none) | nothing | `connect()`, the DB-API cursor/row path, `aio`, `read_arrow` on the native Arrow stream |
| `arrow` | `pyarrow>=14` | `cursor.arrow()`, `arrow_batch()`, `arrow_reader()` (upstream API surface) |
| `azure` | `azure-identity` | `auth=` / `token_provider=` with azure-identity credentials |
| `async-preview` | `mssql-python-rs==0.3.0` (Microsoft `mssql_py_core`, ~30 MB) | `bulkcopy()`, `bulkcopy_arrow()`, upstream `async_query` preview |

## Import hygiene

`import mssql_python` and the basic DB-API path must not import any optional
stack. Every optional import is lazy:

* `cursor.py`: `pyarrow` is imported only inside `_ensure_pyarrow()` and the
  Arrow fetch/insert methods (`TYPE_CHECKING` guard at module top).
* `auth.py`, `db_connection.py`: `azure.identity` / `azure.core` are imported
  inside the functions that build those credentials.
* `cursor.py` bulkcopy: `mssql_py_core` is imported in
  `_bulkcopy_core_and_validate()`. A missing package raises an `ImportError`
  that names the `async-preview` extra.
* `odbc_provider.py`: the provider package (`mssql_python_odbc` / `mssql_py_core`)
  is imported only when the upstream ODBC provider is selected. The Rust
  port path (`ddbc_bindings.__rust_port__`) skips that import.

Check: `scripts/wheel_size.sh` installs the wheel alone into an empty venv and
lists which of `pyarrow`, `azure`, `mssql_py_core`, `polars`, `pandas`, `numpy`
were loaded by `import mssql_python`. The expected result is `none`.

## Release profile

`Cargo.toml` `[profile.release]`:

* `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`: speed.
* `panic = "unwind"`: required by PyO3. Panics are converted to Python
  exceptions at the FFI boundary, so the abort strategy cannot be used.
* `strip = true`: removes symbols from the `.so`. Runtime behavior does not
  change.

Dependencies removed because nothing in `src/` used them: `once_cell`,
`chrono`, `bigdecimal`, `thiserror`, and the `arrow-data` crate with its `ffi`
feature. PyO3's `chrono` feature was removed with them. `uuid`, `encoding_rs`,
`arrow-array`, `arrow-buffer` and `arrow-schema` are still used.

## Size targets

Target: wheel <= 3 MB, installed <= 6 MB, 0 required dependencies.
Upstream `mssql-python` 1.16.0 with its dependencies is 46 MB incl. deps (the baseline the user quoted).

Measure with:

```
scripts/wheel_size.sh                 # builds a release wheel, then measures
scripts/wheel_size.sh path/to.whl     # measures an existing wheel
```

The script reports the wheel size, `Requires-Dist` entries without an extra
marker, the installed `mssql_python/` size and the `.so` size, and the modules
pulled by `import mssql_python`.

## abi3

Decision rule: adopt `abi3-py310` only if `read.taxi_100k.fetchall`,
`read.taxi_100k.fetchone` and `latency.select1` are within noise (15%, median of
5 runs) of the version-specific build. Free-threaded (3.13t/3.14t) builds cannot
use abi3, so version-specific wheels stay in any case.
