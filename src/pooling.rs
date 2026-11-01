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


/// Upper bound on the checkin liveness probe.
const VERIFY_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_SIZE: usize = 100;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// A pooled session plus what is needed to judge its access token on reuse.
pub struct IdleClient {
    pub client: SyncClient,
    /// Access token the session logged in with, if any.
    pub token: Option<String>,
    /// Token expiry as Unix seconds; 0 when the factory reported none.
    pub token_expiry: i64,
    /// `SQL_ATTR_CONNECTION_TIMEOUT` the session was returned with; it
    /// belongs to the physical connection, so the next borrower inherits it.
    pub connection_timeout: u32,
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

/// Check out a session for `key` or reserve capacity for a new one. Also
/// returns the expired or dead sessions it dropped, for the caller to close
/// (`close_all`, network I/O) once it no longer needs the result in a hurry:
/// the checkout itself does no I/O and takes the pool lock only briefly.
pub fn acquire(key: &str) -> (Result<Acquired, PoolFull>, Vec<SyncClient>) {
    let mut to_close = Vec::new();
    let outcome = {
        let mut manager = MANAGER.lock();
        if !manager.accepting {
            return (Ok(Acquired::Declined), to_close);
        }
        evict_expired(&mut manager, &mut to_close);
        let generation = manager.generation;
        let max_size = manager.max_size;
        if !manager.pools.contains_key(key) {
            manager.pools.insert(key.to_string(), KeyPool::default());
        }
        let pool = manager.pools.get_mut(key).expect("inserted above");
        let mut reusable = None;
        while let Some(idle) = pool.idle.pop() {
            let client = &idle.entry.client;
            if client.idle_peer_gone().unwrap_or_else(|| client.is_connection_dead()) {
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
    (outcome, to_close)
}

/// Drop idle sessions older than the idle timeout, in every pool.
fn evict_expired(manager: &mut Manager, to_close: &mut Vec<SyncClient>) {
    let timeout = manager.idle_timeout;
    manager.pools.retain(|_, pool| {
        // Sessions are checked in at the back, so the front one is the oldest.
        if pool.idle.first().is_none_or(|i| i.since.elapsed() <= timeout) {
            return pool.size > 0 || !pool.idle.is_empty();
        }
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
    if !verify_alive(&mut entry.client) {
        // Unanswered: the stream may sit mid-request, so even a polite close
        // could block on the same dead socket. Dropping closes the socket.
        drop(entry);
        return None;
    }
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

/// Make sure a session the server ended while it sat in the caller's hands
/// (KILL, network loss) is not kept idle: it looks healthy until something is
/// sent on it, and would fail the next borrower. Where the socket can be
/// looked at, `acquire` does that before reusing it, a zero-timeout poll
/// with no round trip: the server closes the socket of a session it ends, and
/// an idle session gets no other data. Otherwise one trivial request
/// is sent, a SET, which never opens an implicit transaction (a SELECT would,
/// under IMPLICIT_TRANSACTIONS), outside the pool lock and bounded by
/// [`VERIFY_TIMEOUT`]: a half-open socket (peer gone without a FIN) would
/// otherwise stall `close()` until TCP keepalive gives up. Failure marks the
/// session dead, so `checkin` refuses it. Returns false when the probe timed
/// out.
fn verify_alive(client: &mut SyncClient) -> bool {
    if client.is_connection_dead() || client.has_open_batch() {
        return true;
    }
    // On unix `acquire` can always look at the socket (network sessions only:
    // named pipes are Windows-only), and that check suffices.
    if cfg!(unix) {
        return true;
    }
    client
        .block_on(|c| async move {
            tokio::time::timeout(VERIFY_TIMEOUT, async move {
                c.execute("SET NOEXEC OFF".to_string(), ()).await?;
                c.close_query().await
            })
            .await
        })
        .is_ok()
}

/// Close sessions politely (outside any lock).
pub fn close_all(clients: Vec<SyncClient>) {
    for client in clients {
        client.close();
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
    // Only the forking thread survives in the child, so the lock may be held
    // by a thread that no longer exists and the map may have been caught
    // mid-mutation (a torn HashMap). Nothing may read or iterate it: forget
    // the whole map (leaking its sockets, which belong to the parent) and
    // start from an empty one.
    if MANAGER.is_locked() {
        // SAFETY: its owner thread does not exist in this process.
        unsafe { MANAGER.force_unlock() };
    }
    let mut manager = MANAGER.lock();
    manager.generation += 1;
    std::mem::forget(std::mem::take(&mut manager.pools));
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
