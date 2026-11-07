// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Connection management types for TDS protocol communication with SQL Server.
//!
//! Key types:
//! - [`tds_client::TdsClient`] — primary client for executing queries and managing connections
//! - [`client_context::ClientContext`] — connection configuration (credentials, encryption, timeouts)
//! - [`bulk_copy::BulkCopy`] — bulk data loading

pub(crate) mod blocking;
pub mod bulk_copy;
pub(crate) mod bulk_copy_state;
/// Client connection context and authentication factories.
pub mod client_context;
pub mod connect_stage;
pub(crate) mod connection_actions;
/// Server cursor RPCs (`sp_cursor*`) via the
/// [`CursorClient`](crate::connection::cursor_ops::CursorClient) trait.
pub mod cursor_ops;
pub(crate) mod datasource_parser;
pub(crate) mod dns_cache;
// RUST-PORT: process-wide TTL DNS cache (VENDOR.md patch 15); std + tokio only
// so the main crate's tests can compile it too.
pub mod dns_ttl_cache;
pub(crate) mod execution_context;
pub(crate) mod instance_cache;
pub(crate) mod metadata_retriever;
pub mod multi_subnet_failover;
pub(crate) mod session_recovery;
/// Primary client type and result set traits.
pub mod tds_client;
/// Transport layer (TCP, Named Pipes, Shared Memory).
pub mod transport;

/// RUST-PORT: how many times the operating system's resolver was asked to
/// resolve a host for a connection (cache bypasses included), and the
/// process-wide DNS cache's counters. For tests and diagnostics.
pub fn dns_cache_stats() -> (u64, dns_cache::TtlCacheStats) {
    dns_cache::stats()
}

/// RUST-PORT: empties the process-wide DNS cache (counters are kept). For tests.
pub fn dns_cache_clear() {
    dns_cache::clear_ttl_cache();
}

/// RUST-PORT test hook: the next `failures` OS lookups fail with a temporary
/// resolver error and every lookup first sleeps `delay_ms`; `(0, 0)` resets.
pub fn dns_cache_test_fault(failures: u32, delay_ms: u64) {
    dns_cache::set_test_fault(failures, delay_ms);
}

/// RUST-PORT test hook: caches `addresses` for `host:port` in the DNS cache as
/// if resolved `age_secs` seconds ago.
pub fn dns_cache_test_seed(
    host: &str,
    port: u16,
    addresses: Vec<std::net::SocketAddr>,
    age_secs: u64,
) {
    dns_cache::seed_ttl_cache(
        host,
        port,
        addresses,
        std::time::Duration::from_secs(age_secs),
    );
}

/// RUST-PORT test hook (VENDOR.md patch 16): whether the rustls engine's
/// client configs resume TLS sessions (they must not).
#[cfg(feature = "rustls-tls")]
pub fn rustls_session_resumption_enabled() -> crate::core::TdsResult<bool> {
    transport::tls::rustls_engine::session_resumption_enabled()
}

/// This computer's name, as the client compares a data source's host with it
/// to decide whether the data source is local.
pub fn this_computer_name() -> Option<String> {
    datasource_parser::ParsedDataSource::get_computer_name()
}
