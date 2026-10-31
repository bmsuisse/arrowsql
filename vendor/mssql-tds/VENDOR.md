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
4. `src/connection/tds_client.rs`, trigger row counts: (a)
   `statement_count_before_trigger_count`: a counted DONEINPROC directly
   followed by a counted plain DONE (a trigger's count, then the batch
   statement's) swap, so the statement's count is the first update-count
   result and `SQLRowCount` after execute is not the trigger's. (b)
   `observe_prepared_batch_done`: per-RPC counts take the last DONEINPROC
   instead of summing, so executemany does not add trigger rows.
5. Client-side cost of many-parameter `sp_executesql` calls (multirow
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

6. Busy polling for the first response byte (marked `RUST-PORT`):
   `src/connection/transport/busy_poll.rs` (new), `network_transport.rs`
   (`Stream::raw_fd`, `NetworkTransport::{raw_fd, awaiting_response,
   first_byte_wait_ns}`, the first read in `get_new_tds_packet`). On threads
   that opt in (`busy_poll::ThreadGuard`, entered by the port's synchronous
   `block_on`), the first read after a request spins on `poll(fd, POLLIN, 0)`
   for at most twice the connection's recent first-byte wait (100-400 us,
   only while that wait is under 300 us), yielding to the current-thread
   runtime between checks (its zero-timeout `epoll_wait` makes the readiness
   visible, so the read that follows finds a ready socket and never parks), so a
   short query skips the sleep and the scheduler's wake-up latency. At most two
   threads spin at a time, none on machines with fewer than 4 CPUs;
   `MSSQL_RS_BUSY_POLL=0` turns it off. Costs CPU: a short round trip keeps
   one core busy while it waits.

7. Parsed COLMETADATA reuse (marked `RUST-PORT`): `receive_token_internal`
   (`src/io/token_stream.rs`) asks the reader for a cached token before parsing
   a COLMETADATA payload (no Always Encrypted); `NetworkTransport`
   (`take_cached_colmetadata`, `begin_colmetadata`, `end_colmetadata`) keeps the
   last token with the wire bytes it was parsed from and serves a
   byte-identical payload without parsing (only when the token was read from one
   buffer: `TdsReadBuffer::packets_read`). `Tokens::ColMetadata` now carries an
   `Arc<ColMetadataToken>` (every consumer wrapped it in one anyway), and
   `tds_client.rs` adds `MetadataHandle` / `TdsClient::metadata_handle` so the
   port can recognise an unchanged result shape by identity.

8. Client process id in LOGIN7 (marked `RUST-PORT`): `src/message/login.rs`
   sends `std::process::id()` instead of 0, as the ODBC driver does, so
   `sys.dm_exec_sessions.host_process_id` identifies the client process.

9. Round-trip-free liveness of an idle session (marked `RUST-PORT`):
   `TdsClient::idle_peer_gone`, `AnyTransport` / `NetworkTransport::idle_peer_gone`,
   `busy_poll::idle_peer_gone`. A zero-timeout `poll(fd, POLLIN)` on the TCP
   socket under the TLS layer: an idle TDS session never receives unsolicited
   bytes, so any readiness (data, FIN, error) means the server ended it. Unlike
   `TdsTransport::is_connection_dead` it reads nothing, so it cannot desync the
   TLS stream. The port's connection pool uses it instead of a probe request.

10. Bulk serializer allocation/per-byte cleanups (marked `RUST-PORT`):
   `src/datatypes/tds_value_serializer.rs` writes BINARY/VARBINARY payloads
   with one `write_async`, pads BINARY/CHAR/NCHAR from static zero/space
   chunks, streams UTF-16LE (NVARCHAR/NCHAR/JSON/NTEXT) through a stack buffer
   instead of collecting a `Vec<u16>`, and writes VECTOR f32 data in chunks;
   `src/datatypes/decoder.rs` `read_guid` reads into a `[u8; 16]` stack array.
   Wire output is unchanged.

To drop the vendored copy, delete this directory and the `[patch]` section
in ../../Cargo.toml once the fix is in the pinned upstream rev.
