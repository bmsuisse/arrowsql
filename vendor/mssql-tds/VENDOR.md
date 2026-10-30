# Vendored mssql-tds

Source: https://github.com/microsoft/mssql-rs, crate `mssql-tds`, commit
6b6b48ecbbbdad297039d1641e2be4775eed998b (the rev pinned in ../../Cargo.toml).
MIT, see LICENSE. Only `src/`, `README.md` and a trimmed `Cargo.toml` (no
benches, dev-dependencies or workspace lints) are kept.

Local patches (search for `VENDOR PATCH`):

1. `src/connection/transport/network_transport.rs`,
   `read_unicode_with_byte_length`: decode UTF-16 lossily. SQL Server error
   2628 quotes the truncated value, which can end in a lone high surrogate
   (e.g. inserting three emoji into NVARCHAR(5)); the strict decode failed the
   ERROR token mid-parse, desynchronised the TDS stream and retired the
   connection ("Connection is dead and session state does not allow
   recovery"). Repro: tests/upstream/test_004_cursor.py::
   test_executemany_utf16_length_validation on a cold plan cache.

To drop the vendored copy, delete this directory and the `[patch]` section
in ../../Cargo.toml once the fix is in the pinned upstream rev.
