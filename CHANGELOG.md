# Changelog

All notable changes to this project. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org/) once 1.0 is reached (0.x may break).

Version numbers in this project are deliberately three different things:

| what | value | meaning |
|---|---|---|
| distribution `mssql-python-rs-port` | 0.1.0 | this project's release number (`pyproject.toml`) |
| `mssql_python.__version__` | 1.16.0 | the upstream `mssql-python` release the Python layer is vendored from; kept so code that checks the upstream version keeps working |
| `mssql_python.ddbc_bindings.__version__` | 1.0.0 | version of the Rust binding contract (`docs/CONTRACT.md`) |

## [Unreleased]

### Added

- PyO3 extension `mssql_python.ddbc_bindings` over Microsoft's `mssql-tds`
  (vendored, with local patches, in `vendor/mssql-tds`): no ODBC driver, same
  `mssql_python` Python API as upstream 1.16.0.
- Native Arrow: `cursor.arrow()`, `arrow_batch()`, `arrow_reader()`,
  `fetchall_arrow()`, `fetchmany_arrow()`, `arrow_stream()`, `read_arrow()`
  (partitioned parallel reads).
- Native bulk copy (rows and Arrow), `write_arrow()` parallel loads.
- Native asyncio (`mssql_python.aio`) with a built-in pool (Unix only).
- `sqlstate` and `native_error` on every exception
  ([upstream #581](https://github.com/microsoft/mssql-python/issues/581));
  bulk copy on the cursor's own connection, so `#temp` tables work
  ([#492](https://github.com/microsoft/mssql-python/issues/492)).
- Type stubs for the port-specific API (`ddbc_bindings.pyi`, RUST-PORT section of
  `mssql_python.pyi`).
- CI (`.github/workflows/ci.yml`) and a wheel/sdist build workflow
  (`wheels.yml`, artifacts only, no publishing).
- Project metadata: SPDX license, classifiers, URLs; CONTRIBUTING.

### Changed

- `-z pack-relative-relocs` moved out of `.cargo/config.toml` into the local
  release scripts, so CI manylinux_2_28 wheels do not require glibc 2.36.
- The `async-preview` extra pins `mssql-python-rs==0.4.0` (was 0.3.0).
- The sdist now carries `vendor/mssql-tds`.

### Known gaps

- Pool checkout does not probe an idle connection with a timeout (upstream
  GH #854), and `set_attr(SQL_ATTR_CONNECTION_TIMEOUT)` is not carried across
  pool reuse. See `tests/upstream_excluded.txt`.
- Always Encrypted, `ActiveDirectoryPassword` and NTLM are not supported.
- Only Linux x86_64 has been exercised against a database.
