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

`maturin` needs `patchelf` on `PATH` for the auditwheel step. Install it into
the worktree venv with `uv pip install -p .venv patchelf` if the build fails
with "Failed to execute 'patchelf'".

### Current measurement (release, `scripts/wheel_size.sh`, cp313 manylinux_2_38)

| measure | before tokio change | after (HEAD) |
|---|---|---|
| wheel file | 6,240,092 B (6096 KiB) | 6,227,827 B (6084 KiB) |
| `ddbc_bindings*.so` in the wheel (uncompressed) | 6,512,553 B | about 20 KiB smaller |
| bundled `.libs` (compressed) | 3.20 MB | 3.20 MB |
| installed (package + `.libs`) | 15,536 KiB (`mssql_python/` 7,052) | 15,516 KiB (`mssql_python/` 7,032) |
| required deps | none | none |
| modules loaded by `import mssql_python` | none of pyarrow, azure, mssql_py_core, polars, pandas, numpy | same |
| connect + `SELECT 1` from the wheel alone | not run in this lane | not run in this lane |

Bundled libs, compressed inside the wheel: `libcrypto.so.3` 2.37 MB,
`libssl.so.3` 0.48 MB, `libzstd.so.1` 0.35 MB. Nothing in `Cargo.lock` is a
zstd crate, so `libzstd` comes in through the system OpenSSL, not through a
Rust dependency.

The wheel is 6.2 MB, so it is about 3.2 MB over the 3 MB target. The `.so`
alone is about 2.8 MB compressed. Bundled libs are the other 3.2 MB.

### Where the size goes (release `.so`, 6.3 MB stripped)

The stripped `.so` grew from the 5.21 MB measured earlier (bulk copy, Decimal
cache and the native Arrow stream added code). Measured with an unstripped
build (`CARGO_PROFILE_RELEASE_STRIP=none`) and `nm -S -C`, summing symbol sizes
by crate:

| crate | symbol bytes | share | owner |
|---|---|---|---|
| `mssql_tds` (vendored) | 2.35 MB | 37% | vendor: connection transport, token stream, SSRP, Always Encrypted, TVP, bigdecimal, x509 |
| `ddbc_bindings` (ours) | 0.77 MB | 12% | this repo |
| `core` + `alloc` | 0.61 MB | 10% | Rust core/alloc |
| `std` + `gimli` + `addr2line` + `miniz_oxide` + `rustc_demangle` | 0.23 MB | 4% | std, incl. its backtrace code for panic output |
| `arrow_array` | 0.23 MB | 4% | ours (C Data / C Stream export) |
| `tokio` (before the narrowing below) | 0.23 MB | 4% | mssql-tds |
| `encoding_rs` | 0.20 MB | 3% | mssql-tds needs the CJK encoder tables (see below) |
| `pyo3` | 0.08 MB | 1% | ours |
| `x509_parser` + `der_parser` + `asn1_rs` + `nom` | 0.11 MB | 2% | mssql-tds certificate expiry check |
| `num_bigint` + `bigdecimal` | 0.10 MB | 2% | mssql-tds |
| `arrow_data` + `arrow_schema` + `arrow_buffer` | 0.14 MB | 2% | ours |

ELF sections of the stripped `.so`: `.text` 4.99 MB, `.rodata` 0.42 MB,
`.eh_frame` 0.37 MB (needed by `panic = "unwind"`), `.gcc_except_table` 0.24 MB
(same reason), `.rela.dyn` 0.17 MB (absolute pointers in PIC data).

Dynamic libraries: `libssl.so.3` and `libcrypto.so.3` are linked, not static.
`native-tls` (the TLS backend, `tokio-native-tls`) uses the `openssl` crate on
Linux, so OpenSSL is needed for TLS whatever Always Encrypted does. auditwheel
bundles it into `*.libs/`, which is the 3.2 MB above.

### Tried

| change | result | verdict |
|---|---|---|
| baseline (before this lane) | `.so` 6,512,553 B in the wheel | |
| `tokio` `full` and `tokio-util` `full` narrowed to the features the crate uses (`mssql-tds` `Cargo.toml`) | `.so` about 20 KiB smaller, `signal-hook-registry` and a duplicate `hashbrown` dropped from the lock | adopted |
| `opt-level = "s"` for `x509-parser`, `der-parser`, `asn1-rs`, `nom` (cold: connect-time cert parse) | a further 8 KiB smaller, below noise | rejected: not worth a per-package profile entry |
| `opt-level = "s"` for `mssql-tds` only | `.so` +12% | rejected (earlier run) |
| `opt-level = "s"` for everything | `.so` +3% | rejected (earlier run). Generic monomorphisations that LTO already merges dominate, so `s` only changes the code shape |
| `encoding_rs/no-static-ideograph-encoder-tables` | not built | rejected by reading code: `mssql-tds` encodes GBK/Big5/EUC-KR for varchar parameters (`vendor/mssql-tds/src/datatypes/sql_string/encoding.rs`), so the feature would break those writes |
| `abi3-py310` | not built, not benchmarked | open, see below |

Benchmark check for the adopted tokio change (`RUST_PORT_VENV=.venv benchmarks/run.py`,
one `run.py` pass with `--repeats 5`, load average about 11, so indicative only; values as printed): `read.taxi_100k.fetchall`
rust_port 0.128 s vs mssql_python 0.351 s; `latency.select1_x5000` rust_port
0.731 s vs mssql_python 0.779 s. There is no before/after run for the same
host state. The change touches no runtime code path (features only: signal
handling is not used by the crate), so the expectation is no change in the
hot paths.

### What would reach the 3 MB target

Cargo features and profiles in this repo are nearly exhausted: each option
above is either in `vendor/mssql-tds` source or in the packaging decision.
The levers, in order of size:

1. **Replace the TLS backend.** `native-tls` on Linux pulls in `openssl`, so
   `libcrypto` (2.37 MB compressed) is bundled. Dropping the Always Encrypted
   `openssl` use alone does not help, because TLS still needs it. A rustls
   backend would remove both. That is a transport change in
   `vendor/mssql-tds/src/connection/transport/` (not owned by this lane).
2. **Packaging decision: stop bundling OpenSSL.** Build with
   `--auditwheel skip` (or `repair` off), so the wheel depends on the system
   `libssl.so.3` and `libcrypto.so.3`. That saves the 3.2 MB of `.libs` and
   brings the wheel to about 3.0 MB. It is not manylinux-compliant (the tag
   would be `linux_x86_64`), and it needs OpenSSL 3 on the target. Not done
   here.
3. **Gate the vendored extras**, all in `vendor/mssql-tds/src`: `x509-parser`
   (about 0.07 MB symbols), `bigdecimal` / `num-bigint` (about 0.1 MB), SSRP and
   multi-subnet failover, TVP and Always Encrypted paths. Each is a small share.
4. **Tighter `.eh_frame` / `.gcc_except_table`** (about 0.6 MB together) needs
   `panic = "abort"`, which PyO3 does not allow. Not an option.

Until 1 or 2 is decided, the wheel stays above 6 MB and the 3 MB target is not met.

## abi3

Status: not adopted. No abi3 build was made or timed, so there are no numbers.
The version-specific wheel stays the default.

Decision rule: adopt `abi3-py310` only if `read.taxi_100k.fetchall`,
`read.taxi_100k.fetchone` and `latency.select1` are within noise (15%, median of
5 runs) of the version-specific build. Free-threaded (3.13t/3.14t) builds cannot
use abi3, so version-specific wheels stay in any case.

## TLS backend (lean_tls)

`vendor/mssql-tds` now has two TLS backends. Default: rustls (`tokio-rustls`
0.26 + `rustls` 0.23 with the `ring` provider, system roots through
`rustls-native-certs`). `ring` was chosen over `aws-lc-rs` (needs cmake/C++ and
is larger). Legacy: cargo feature `native-tls` (`maturin build --features native-tls`)
brings back native-tls plus OpenSSL for Always Encrypted.

- `libssl`, `libcrypto` and `libzstd` are no longer bundled. `libzstd` was never
  a Rust dependency: it was a dependency of the system `libcrypto.so.3` that
  auditwheel copied. Nothing links it now.
- Always Encrypted crypto (`security/crypto/openssl.rs`) is only compiled with
  `native-tls`; the default build uses `unsupported.rs`, which returns a clean
  `ColumnEncryptionError`. The port does not expose Always Encrypted.
- Semantics kept: TrustServerCertificate / LoginOnly / ServerCertificate pinning
  skip chain checks (handshake signatures are still verified; SQL Server's
  X.509 v1 self-signed certificates are accepted via an x509-parser fallback),
  HostNameInCertificate is the name checked, Encrypt=strict negotiates ALPN
  `tds/8.0`, TDS 7.4 prelogin-wrapped TLS uses the same one-packet-per-flight
  buffering as macOS. TLS 1.2/1.3 only.
- Pitfall: SQL Server drops the connection if a TDS packet is split across TLS
  records at odd offsets. tokio-rustls accepts partial writes when its queue is
  non-empty, so `RustlsStream` flushes queued output before every write/read.
  Without it, writes above about 2 MB failed.
- Cert verification failure with TrustServerCertificate=no fails cleanly:
  "invalid peer certificate: ... UnsupportedCertVersion" for the local
  X.509 v1 container certificate (webpki rejects v1), instead of OpenSSL's
  "self-signed certificate".
- Encrypt=strict (TDS 8) does not work against the local container
  (SQL Server 2022 without strict support); same as before (OpenSSL also failed).

### Measurement (release, cp313)

| measure | before | after |
|---|---|---|
| wheel | 6,227,827 B | 3,607,378 B |
| bundled .libs | 3.2 MB compressed | none |
| installed | 15,516 KiB | 7,996 KiB |
| `.so` | 6.5 MB | 7.3 MB (7,471,920 B uncompressed) |

Targets (wheel <= 3 MB, installed <= 6 MB) are not met: rustls + ring + webpki
add about 1 MB to the `.so`. `opt-level = "s"` for the cold TLS crates is set in
`Cargo.toml`. Remaining levers: drop `x509-parser` (used for the v1 certificate
fallback, expiry check) and gate TVP / SSRP in the vendored crate.
