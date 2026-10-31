//! Connection pooling.
//!
//! Idle `SyncClient`s are kept per pool key (connection string + the Python
//! layer's `pool_key`, which encodes token identity). A pool's `size` counts
//! idle *and* checked-out connections, capped at `max_size`; an acquire past
//! the cap fails instead of waiting. Capacity is held by a [`Lease`] that the
//! `Connection` keeps while it is open and that gives the slot back when
//! dropped, so every way of losing a connection (failed connect, failed
//! sanitation, destructor) releases it.
//!
//! On checkout the client is armed with RESETCONNECTION
//! (`prepare_reset_connection`), which the server applies with the next
//! request: `sp_reset_connection` semantics with no extra round trip.
//!
//! `disable_pooling` / `close_pooling` start a new *generation*: leases of
//! the old generation neither return connections to, nor free capacity in,
//! the new pools.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use crate::runtime::SyncClient;
use parking_lot::Mutex;
use pyo3::prelude::*;


const DEFAULT_MAX_SIZE: usize = 100;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// A pooled session plus what is needed to judge its access token on reuse.
pub struct IdleClient {
    pub client: SyncClient,
    /// Access token the session logged in with, if any.
    pub token: Option<String>,
    /// Token expiry as Unix seconds; 0 when the factory reported none.
    pub token_expiry: i64,
}

impl IdleClient {
    /// The token expires within `threshold_secs`. A token of unknown expiry
    /// counts as near expiry (fail closed); with no token there is nothing
    /// to expire.
    pub fn token_near_expiry(&self, threshold_secs: i64) -> bool {
        if self.token_expiry == 0 {
            return self.token.is_some();
        }
        unix_now() + threshold_secs >= self.token_expiry
    }
}

pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

struct Idle {
    entry: IdleClient,
    since: Instant,
}

#[derive(Default)]
struct KeyPool {
    idle: Vec<Idle>,
    /// Idle plus checked-out connections.
    size: usize,
}

struct Manager {
    accepting: bool,
    /// Size and timeout are fixed by the first `enable_pooling`, like the
    /// upstream binding; later calls only re-arm.
    configured: bool,
    generation: u64,
    max_size: usize,
    idle_timeout: Duration,
    pools: HashMap<String, KeyPool>,
}

static MANAGER: LazyLock<Mutex<Manager>> = LazyLock::new(|| {
    Mutex::new(Manager {
        accepting: false,
        configured: false,
        generation: 0,
        max_size: DEFAULT_MAX_SIZE,
        idle_timeout: DEFAULT_IDLE_TIMEOUT,
        pools: HashMap::new(),
    })
});

pub fn key(conn_str: &str, pool_key: &str) -> String {
    format!("{conn_str}\u{0}{pool_key}")
}

/// One unit of a pool's capacity. Dropping it frees the slot; handing it to
/// [`checkin`] keeps the slot occupied by the idle connection.
pub struct Lease {
    key: String,
    generation: u64,
    occupies_slot: bool,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.occupies_slot {
            release_slot(&self.key, self.generation);
        }
    }
}

fn release_slot(key: &str, generation: u64) {
    let mut manager = MANAGER.lock();
    if manager.generation != generation {
        return;
    }
    if let Some(pool) = manager.pools.get_mut(key) {
        pool.size = pool.size.saturating_sub(1);
        if pool.size == 0 && pool.idle.is_empty() {
            manager.pools.remove(key);
        }
    }
}

pub struct PoolFull;

pub enum Acquired {
    /// Pooling is off: connect without a lease.
    Declined,
    /// A reusable session; capacity is already accounted for.
    Idle(Lease, Box<IdleClient>),
    /// Capacity reserved for a new session.
    Fresh(Lease),
}

/// Check out a session for `key` or reserve capacity for a new one.
pub fn acquire(key: &str) -> Result<Acquired, PoolFull> {
    let mut to_close = Vec::new();
    let outcome = {
        let mut manager = MANAGER.lock();
        if !manager.accepting {
            return Ok(Acquired::Declined);
        }
        evict_expired(&mut manager, &mut to_close);
        let generation = manager.generation;
        let max_size = manager.max_size;
        let pool = manager.pools.entry(key.to_string()).or_default();
        let mut reusable = None;
        while let Some(idle) = pool.idle.pop() {
            if idle.entry.client.is_connection_dead() {
                pool.size = pool.size.saturating_sub(1);
                to_close.push(idle.entry.client);
            } else {
                reusable = Some(idle.entry);
                break;
            }
        }
        let lease = |pool: &mut KeyPool, new_slot: bool| {
            if new_slot {
                pool.size += 1;
            }
            Lease { key: key.to_string(), generation, occupies_slot: true }
        };
        match reusable {
            Some(entry) => Ok(Acquired::Idle(lease(pool, false), Box::new(entry))),
            None if pool.size < max_size => Ok(Acquired::Fresh(lease(pool, true))),
            None => Err(PoolFull),
        }
    };
    close_all(to_close);
    outcome
}

/// Drop idle sessions older than the idle timeout, in every pool.
fn evict_expired(manager: &mut Manager, to_close: &mut Vec<SyncClient>) {
    let timeout = manager.idle_timeout;
    manager.pools.retain(|_, pool| {
        let (expired, kept): (Vec<_>, Vec<_>) =
            std::mem::take(&mut pool.idle).into_iter().partition(|i| i.since.elapsed() > timeout);
        pool.size = pool.size.saturating_sub(expired.len());
        pool.idle = kept;
        to_close.extend(expired.into_iter().map(|i| i.entry.client));
        pool.size > 0 || !pool.idle.is_empty()
    });
}

/// Remove the idle sessions of `lease`'s pool that logged in with `token`.
pub fn evict_with_token(lease: &Lease, token: &str) {
    let mut to_close = Vec::new();
    {
        let mut manager = MANAGER.lock();
        if manager.generation == lease.generation
            && let Some(pool) = manager.pools.get_mut(&lease.key)
        {
            let (stale, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut pool.idle)
                .into_iter()
                .partition(|i| i.entry.token.as_deref() == Some(token));
            pool.size = pool.size.saturating_sub(stale.len());
            pool.idle = kept;
            to_close.extend(stale.into_iter().map(|i| i.entry.client));
        }
    }
    close_all(to_close);
}

/// Return a session to its pool. Gives it back when it cannot be pooled
/// (pooling disabled, new generation, dead session, open batch); the caller
/// then closes it, and `lease` frees the slot on drop.
pub fn checkin(mut lease: Lease, mut entry: IdleClient) -> Option<SyncClient> {
    verify_alive(&mut entry.client);
    let mut manager = MANAGER.lock();
    let poolable = manager.accepting
        && manager.generation == lease.generation
        && !entry.client.is_connection_dead()
        && !entry.client.has_open_batch();
    if !poolable {
        return Some(entry.client);
    }
    let Some(pool) = manager.pools.get_mut(&lease.key) else { return Some(entry.client) };
    pool.idle.push(Idle { entry, since: Instant::now() });
    lease.occupies_slot = false;
    None
}

/// Send one trivial request before a session is kept idle. A session the
/// server ended while it sat in the caller's hands (KILL, network loss) looks
/// healthy until something is sent on it; without this it would be pooled and
/// fail the next borrower. Failure marks the session dead, so `checkin`
/// refuses it. The request is a SET, which never opens an implicit
/// transaction (a SELECT would, under IMPLICIT_TRANSACTIONS). Runs outside
/// the pool lock.
fn verify_alive(client: &mut SyncClient) {
    if client.is_connection_dead() || client.has_open_batch() {
        return;
    }
    let _ = client.block_on(|c| async move {
        c.execute("SET NOEXEC OFF".to_string(), ()).await?;
        c.close_query().await
    });
}

/// Close sessions politely (outside any lock).
pub fn close_all(clients: Vec<SyncClient>) {
    for mut client in clients {
        let _ = client.block_on(|c| c.close_connection());
    }
}

/// Start a new generation and return every idle session for closing.
fn reset_pools(manager: &mut Manager) -> Vec<SyncClient> {
    manager.generation += 1;
    std::mem::take(&mut manager.pools)
        .into_values()
        .flat_map(|pool| pool.idle.into_iter().map(|i| i.entry.client))
        .collect()
}

/// Runs in a forked child. The idle sockets belong to the parent, so they are
/// leaked, not closed: a close would write to the parent's live connection.
pub fn forget_after_fork() {
    // Only the forking thread survives in the child; a lock another parent
    // thread held at fork time would never be released.
    if MANAGER.is_locked() {
        // SAFETY: its owner thread does not exist in this process.
        unsafe { MANAGER.force_unlock() };
    }
    let inherited = reset_pools(&mut MANAGER.lock());
    std::mem::forget(inherited);
}

#[pyfunction]
#[pyo3(signature = (max_size=100, idle_timeout=600))]
pub fn enable_pooling(max_size: i64, idle_timeout: i64) {
    let mut manager = MANAGER.lock();
    if !manager.configured {
        manager.max_size = usize::try_from(max_size).unwrap_or(1).max(1);
        manager.idle_timeout = Duration::from_secs(u64::try_from(idle_timeout).unwrap_or(0));
        manager.configured = true;
    }
    manager.accepting = true;
}

#[pyfunction]
pub fn close_pooling(py: Python<'_>) {
    let idle = reset_pools(&mut MANAGER.lock());
    py.detach(|| close_all(idle));
}

#[pyfunction]
pub fn disable_pooling(py: Python<'_>) {
    let idle = {
        let mut manager = MANAGER.lock();
        manager.accepting = false;
        reset_pools(&mut manager)
    };
    py.detach(|| close_all(idle));
}
