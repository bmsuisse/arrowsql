//! Connection pooling.
//!
//! Idle `TdsClient`s are kept per pool key (connection string + the Python
//! layer's `pool_key`, which encodes token identity). On checkout the client
//! is armed with RESETCONNECTION (`prepare_reset_connection`), which the
//! server applies with the next request — `sp_reset_connection` semantics
//! with no extra round trip. Dead or expired clients are dropped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use mssql_tds::connection::tds_client::TdsClient;
use parking_lot::Mutex;
use pyo3::prelude::*;

static ACCEPTING: AtomicBool = AtomicBool::new(false);
static MAX_SIZE: AtomicUsize = AtomicUsize::new(100);
static IDLE_TIMEOUT_SECS: AtomicU64 = AtomicU64::new(600);
static IDLE: Mutex<Option<HashMap<String, Vec<(TdsClient, Instant)>>>> = Mutex::new(None);

pub fn key(conn_str: &str, pool_key: &str) -> String {
    format!("{conn_str}\u{0}{pool_key}")
}

pub fn enabled() -> bool {
    ACCEPTING.load(Ordering::Acquire)
}

/// Take an idle client for `key`, armed to reset its session state.
pub fn checkout(key: &str) -> Option<TdsClient> {
    if !enabled() {
        return None;
    }
    let timeout = Duration::from_secs(IDLE_TIMEOUT_SECS.load(Ordering::Relaxed));
    let mut guard = IDLE.lock();
    let idle = guard.as_mut()?.get_mut(key)?;
    while let Some((mut client, since)) = idle.pop() {
        if since.elapsed() > timeout || client.is_connection_dead() {
            continue;
        }
        client.prepare_reset_connection(false);
        return Some(client);
    }
    None
}

/// Return a client to the pool. Returns it back if pooling is off, the
/// client is unusable, or the pool for this key is full (caller closes it).
pub fn checkin(key: String, client: TdsClient) -> Option<TdsClient> {
    if !enabled() || client.is_connection_dead() || client.has_open_batch() {
        return Some(client);
    }
    let max = MAX_SIZE.load(Ordering::Relaxed).max(1);
    let mut guard = IDLE.lock();
    let idle = guard.get_or_insert_with(HashMap::new).entry(key).or_default();
    if idle.len() >= max {
        return Some(client);
    }
    idle.push((client, Instant::now()));
    None
}

fn clear() {
    // Dropping idle clients closes their sockets.
    let drained = IDLE.lock().take();
    drop(drained);
}

#[pyfunction]
#[pyo3(signature = (max_size=100, idle_timeout=600))]
pub fn enable_pooling(max_size: i64, idle_timeout: i64) {
    MAX_SIZE.store(max_size.max(1) as usize, Ordering::Relaxed);
    IDLE_TIMEOUT_SECS.store(idle_timeout.max(0) as u64, Ordering::Relaxed);
    ACCEPTING.store(true, Ordering::Release);
}

#[pyfunction]
pub fn close_pooling(py: Python<'_>) {
    py.detach(clear);
}

#[pyfunction]
pub fn disable_pooling(py: Python<'_>) {
    ACCEPTING.store(false, Ordering::Release);
    py.detach(clear);
}
