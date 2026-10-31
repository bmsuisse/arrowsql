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
2. TLS backend (marked `RUST-PORT`): `src/connection/transport/tls/rustls_engine.rs`
   (new) is the default engine, tokio-rustls with the `ring` provider and the
   system trust store (rustls-native-certs, so `SSL_CERT_FILE` works). The
   native-tls/OpenSSL engine and the OpenSSL-backed Always Encrypted crypto
   are behind the `native-tls` feature; without it,
   `src/security/crypto/unsupported.rs` makes column encryption fail cleanly.
   Dropping OpenSSL removes libssl, libcrypto and libzstd from the wheel.
   Covered by tests/port/test_tls.py.
3. `src/connection/tds_client.rs` `execute_sp_executesql`: build the parameter
   declaration from a reference instead of cloning every parameter.
4. Client-side cost of many-parameter `sp_executesql` calls (multirow
   INSERT ... VALUES, 1000 parameters):
   - `src/connection/tds_client.rs`: `execute_sp_executesql_positional` (new)
     takes unnamed parameters and declares/sends parameter `n` as `@Pn`
     without allocating a name each; names are materialized when a parameter
     is output, data-at-exec or Always Encrypted applies.
   - `src/message/parameters/rpc_parameters.rs`: `write_plain_declaration`
     (declaration text written straight into the list, no per-parameter
     `String`) and `write_plain_value` (TYPE_INFO + value of non-NULL
     bit/tinyint/smallint/int/bigint/float/decimal/numeric/datetime2(7) and
     short UTF-16 nvarchar/ASCII varchar written straight into the packet
     buffer; byte-identical to the general encoder, which remains the fallback).
   - `src/io/packet_writer.rs`: `try_write_parts` for the above.
   - `src/datatypes/sql_string.rs` `from_utf8_string`: exact-capacity buffer,
     ASCII fast path. `src/datatypes/sqltypes.rs`: `get_nullable_type` is
     `pub(crate)`.

To drop the vendored copy, delete this directory and the `[patch]` section
in ../../Cargo.toml once the fix is in the pinned upstream rev.
