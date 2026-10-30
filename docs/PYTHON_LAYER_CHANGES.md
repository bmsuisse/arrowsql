# Python layer changes

`python/mssql_python/` is vendored from microsoft/mssql-python (commit in
`tests/upstream/UPSTREAM_COMMIT`). Keep it upstream-identical where possible;
every deliberate edit is marked `RUST-PORT` in the source and listed here.

## Core bindings

- `odbc_provider.py` `ProviderManager.ensure_available`: skip the
  "ODBC provider package must be importable" check when
  `ddbc_bindings.__rust_port__` is set. The Rust bindings talk TDS directly,
  so neither `mssql_python_odbc` nor msodbcsql is needed.
- `ddbc_bindings.py` (the platform-specific `.so` loader) is removed;
  maturin installs the extension as `mssql_python.ddbc_bindings` directly.
