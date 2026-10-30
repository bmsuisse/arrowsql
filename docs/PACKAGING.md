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
DB_CONNECTION_STRING=... scripts/wheel_size.sh   # also runs connect + SELECT 1 from the wheel alone
```

The script reports the wheel size, the largest entries, the bundled `.libs`
size, `Requires-Dist` entries without an extra marker, the installed footprint
(package plus bundled libs) and the `.so` size, and the modules pulled by
`import mssql_python` (and by connect + query when a connection string is set).
With no `DB_CONNECTION_STRING` in the environment it reads the main checkout's
`.env.local`.

### Where the size goes (release, stripped `.so` 5.21 MB)

Measured with an unstripped build (`CARGO_PROFILE_RELEASE_STRIP=none`) and
`nm -S -C`, summing symbol sizes by crate:

| crate | symbol bytes | share | owner |
|---|---|---|---|
| `mssql_tds` (vendored) | 1.92 MB | 37% | vendor: connection transport, token stream, SSRP, Always Encrypted, TVP, bigdecimal, x509 |
| `ddbc_bindings` (ours) | 0.61 MB | 12% | this repo |
| `core` + `alloc` + `std` | 0.65 MB | 13% | Rust std/core (incl. std's own backtrace code) |
| `encoding_rs` | 0.20 MB | 4% | mssql-tds needs the CJK encoder tables |
| `tokio` | 0.18 MB | 4% | mssql-tds uses `tokio` `full` |
| `x509_parser` + `der_parser` + `asn1_rs` + `nom` | ~0.25 MB | 5% | mssql-tds certificate expiry check |
| `num_bigint` + `bigdecimal` | 0.11 MB | 2% | mssql-tds (`bigdecimal`) |
| `arrow-*` (array, schema, buffer, data) | 0.12 MB | 2% | ours |
| `pyo3` | 0.08 MB | 2% | ours |

Dynamic libraries: `libssl.so.3`, `libcrypto.so.3` and `libzstd.so.1` are
linked, not static. The `openssl` crate that mssql-tds pulls in on Linux
(Always Encrypted primitives, see `vendor/mssql-tds/Cargo.toml`) makes auditwheel
bundle them into the wheel (`*.libs/`): about 3.2 MB compressed, 6.4 MB for
`libcrypto` alone.

Measured wheel (release, `scripts/wheel_size.sh`, cp313 manylinux_2_38):

| measure | value |
|---|---|
| wheel file | 5.70 MB (5572 KiB) |
| of which bundled `.libs` (libssl, libcrypto, libzstd) | 3.20 MB compressed |
| rest of the wheel (`ddbc_bindings*.so`, Python layer, metadata) | about 2.5 MB compressed (the `.so` is 5.21 MB stripped) |
| installed (package + `.libs`) | 14.3 MB (`mssql_python/` 5.8 MB) |
| required deps | none |
| modules loaded by `import mssql_python` | none of pyarrow, azure, mssql_py_core, polars, pandas, numpy |
| connect + `SELECT 1` from the wheel alone | works, same module list: none |

### Tried, not adopted

| change | release `.so` | verdict |
|---|---|---|
| baseline | 5,210,736 B | |
| `opt-level = "s"` for `mssql-tds` only | 5,858,968 B (+12%) | larger, rejected |
| `opt-level = "s"` for everything | 5,380,208 B (+3%) | larger, rejected |
| `encoding_rs/no-static-ideograph-encoder-tables` | not built | rejected by reading code: `mssql-tds` encodes GBK/Big5/EUC-KR for varchar parameters (`vendor/mssql-tds/src/datatypes/sql_string/encoding.rs`), so the feature would break those writes |
| abi3-py310 | not built, not benchmarked | open, see below |

Opt-level `s` makes the binary larger here, because the code is dominated by
generic monomorphisations that LTO already merges. Speed was not re-measured
for those builds, since neither is adopted.

### Why the 3 MB target is out of reach from this repo

Everything large in the `.so` that is not ours comes from the vendored
`mssql-tds`. The Cargo features we control cannot remove it. The remaining
levers are in `vendor/mssql-tds` (not owned by this lane):

1. Gate the `openssl` dependency (Always Encrypted, Linux). Without it, the wheel
   would not bundle `libcrypto` (about 3 MB compressed) and would need only the
   system `libssl`. Whether that is acceptable for a manylinux wheel is a
   packaging decision, not made here.
2. Gate `x509-parser` (certificate expiry check, about 0.25 MB).
3. Gate `bigdecimal` / `num-bigint` (about 0.11 MB).
4. Narrow the `tokio` `full` feature in `mssql-tds` (not measured).
5. Gate SSRP, multi-subnet failover, and the TVP / Always Encrypted code paths
   behind features (`ssrp::query_browser` alone is 18 KB).

Until (1) lands the wheel stays above 5 MB, so the 3 MB target is not met.

## abi3

Status: not adopted. No abi3 build was made or timed, so there are no numbers.
The version-specific wheel stays the default.

Decision rule: adopt `abi3-py310` only if `read.taxi_100k.fetchall`,
`read.taxi_100k.fetchone` and `latency.select1` are within noise (15%, median of
5 runs) of the version-specific build. Free-threaded (3.13t/3.14t) builds cannot
use abi3, so version-specific wheels stay in any case.
