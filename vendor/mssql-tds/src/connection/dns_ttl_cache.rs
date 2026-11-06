// RUST-PORT: not in upstream mssql-tds (see vendor/mssql-tds/VENDOR.md, patch 15).
//
//! Process-wide positive DNS cache with a TTL, serve-stale and single-flight
//! lookups.
//!
//! The JVM JDBC driver caches successful lookups for the life of the process;
//! this driver used to resolve the host on every connection, so a slow or
//! flaky resolver (Databricks: seconds per lookup, "Temporary failure in name
//! resolution") was paid by every Spark task, read slice and pool member.
//!
//! Behaviour, keyed by (lowercased host, port):
//! - a fresh entry (younger than the TTL) is used without resolving;
//! - a stale entry younger than TTL + [`Policy::max_stale`] is served at once
//!   while one background thread refreshes it; an older one is refreshed in
//!   line, and its addresses are served if that refresh fails;
//! - concurrent lookups of one key share a single resolver call (the slices of
//!   a parallel read connect at the same moment); its error is shared with
//!   those waiters and never stored;
//! - each resolver call is bounded by [`Policy::attempt_timeout`] and a
//!   transient failure is retried ([`Policy::attempts`]);
//! - IP literals and a TTL of 0 bypass all of the above (one plain lookup);
//! - at most [`MAX_ENTRIES`] entries, the oldest is evicted;
//! - when connecting to cached addresses fails, the caller drops the entry
//!   with [`TtlDnsCache::invalidate`] and resolves once more.
//!
//! This file depends on `std` and `tokio` only, so the main crate's
//! `cargo test --lib` compiles it (`src/dns_ttl_cache_tests.rs`); the vendored
//! crate's own tests do not build.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::OnceCell;

/// Default positive TTL, in seconds.
pub const DEFAULT_TTL_SECS: u64 = 300;
/// Entries kept before the oldest is evicted.
pub const MAX_ENTRIES: usize = 256;
/// How long past its TTL an entry is still served without waiting.
pub const DEFAULT_MAX_STALE: Duration = Duration::from_secs(3600);
/// Bound on one resolver call.
pub const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
/// Resolver calls per lookup (the first plus retries of transient failures).
pub const DEFAULT_ATTEMPTS: u32 = 2;
/// Pause before retrying a transient failure.
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// How one lookup uses the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Age below which an entry is used without resolving; zero disables the cache.
    pub ttl: Duration,
    /// Age past `ttl` up to which an entry is served while it refreshes in the background.
    pub max_stale: Duration,
    /// Bound on one resolver call.
    pub attempt_timeout: Duration,
    /// Resolver calls per lookup; transient failures are retried up to this many.
    pub attempts: u32,
}

impl Policy {
    /// `ttl` with the default staleness, timeout and retry bounds.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            max_stale: DEFAULT_MAX_STALE,
            attempt_timeout: DEFAULT_ATTEMPT_TIMEOUT,
            attempts: DEFAULT_ATTEMPTS,
        }
    }
}

/// The TTL to apply: the connection's `DnsCacheTTL` if it set one, else the
/// `ARROWSQL_DNS_CACHE_TTL` environment value, else [`DEFAULT_TTL_SECS`].
/// A value that is not a non-negative integer is ignored.
pub fn effective_ttl(connection: Option<u64>, env: Option<&str>) -> Duration {
    let secs = connection
        .or_else(|| env.and_then(|v| v.trim().parse().ok()))
        .unwrap_or(DEFAULT_TTL_SECS);
    Duration::from_secs(secs)
}

/// Where the addresses of a lookup came from.
#[derive(Debug)]
pub enum Origin {
    /// Not cacheable (IP literal, or TTL 0): the resolver answered.
    Bypass,
    /// The resolver answered (possibly for a concurrent lookup of the same key)
    /// and the entry was stored.
    Resolved,
    /// A cached entry younger than the TTL; the resolver was not asked.
    Fresh,
    /// A stale entry. `None`: served without waiting while a background
    /// refresh runs. `Some`: refreshing it in line failed with this error.
    Stale(Option<io::Error>),
}

/// Counters since the process started, and the current sizes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Entries cached.
    pub entries: usize,
    /// Resolver calls in progress (background refreshes and waited-for lookups).
    pub in_flight: usize,
    /// Lookups answered by a fresh entry.
    pub hits: u64,
    /// Lookups answered by a stale entry (with or without a refresh behind it).
    pub stale_served: u64,
    /// Lookups that waited for a resolver call they started.
    pub misses: u64,
    /// Lookups that waited for a resolver call another lookup had started.
    pub coalesced: u64,
    /// Background refreshes started.
    pub refreshes: u64,
    /// Resolver calls made through the cache (retries included).
    pub resolver_calls: u64,
    /// Resolver calls that failed or timed out.
    pub resolver_failures: u64,
}

type Key = (String, u16);
/// A resolver answer as it is shared between waiters (`io::Error` is not `Clone`).
type Answer = Result<Vec<SocketAddr>, (io::ErrorKind, String)>;

struct Entry {
    addresses: Vec<SocketAddr>,
    resolved_at: Instant,
}

/// One resolver call that concurrent lookups of a key wait for together.
struct Flight {
    answer: OnceCell<Answer>,
    /// A flight started before a `fork` has no thread left to finish it in the child.
    pid: u32,
}

#[derive(Default)]
struct State {
    entries: HashMap<Key, Entry>,
    flights: HashMap<Key, Arc<Flight>>,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    stale_served: AtomicU64,
    misses: AtomicU64,
    coalesced: AtomicU64,
    refreshes: AtomicU64,
    resolver_calls: AtomicU64,
    resolver_failures: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// See the module documentation.
pub struct TtlDnsCache {
    state: Mutex<State>,
    counters: Counters,
    max_entries: usize,
}

impl Default for TtlDnsCache {
    fn default() -> Self {
        Self::with_capacity(MAX_ENTRIES)
    }
}

impl TtlDnsCache {
    /// A cache holding at most `max_entries` hosts.
    pub fn with_capacity(max_entries: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            counters: Counters::default(),
            max_entries: max_entries.max(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Every critical section leaves the maps consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn key(host: &str, port: u16) -> Key {
        (host.to_ascii_lowercase(), port)
    }

    /// Resolves `host:port` as of `now` (injectable for tests) under `policy`,
    /// asking `lookup` only when no fresh entry exists. The lock is never held
    /// across `lookup`.
    pub async fn resolve<L, Fut>(
        self: &Arc<Self>,
        host: &str,
        port: u16,
        policy: Policy,
        now: Instant,
        lookup: L,
    ) -> io::Result<(Vec<SocketAddr>, Origin)>
    where
        L: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static,
    {
        if policy.ttl.is_zero() || host.parse::<IpAddr>().is_ok() {
            return lookup().await.map(|addresses| (addresses, Origin::Bypass));
        }
        let key = Self::key(host, port);
        let (stale, flight, joined) = {
            let mut state = self.lock();
            let stale = match state.entries.get(&key) {
                Some(entry) => {
                    let age = now.saturating_duration_since(entry.resolved_at);
                    if age < policy.ttl {
                        bump(&self.counters.hits);
                        return Ok((entry.addresses.clone(), Origin::Fresh));
                    }
                    let servable = age < policy.ttl.saturating_add(policy.max_stale);
                    Some((entry.addresses.clone(), servable))
                }
                None => None,
            };
            let pid = std::process::id();
            let live = state.flights.get(&key).filter(|f| f.pid == pid).cloned();
            let joined = live.is_some();
            let flight = live.unwrap_or_else(|| {
                let flight = Arc::new(Flight {
                    answer: OnceCell::new(),
                    pid,
                });
                state.flights.insert(key.clone(), Arc::clone(&flight));
                flight
            });
            (stale, flight, joined)
        };
        let lookup = Arc::new(lookup);
        if let Some((addresses, true)) = &stale
            && (joined
                || self.spawn_refresh(
                    key.clone(),
                    Arc::clone(&flight),
                    policy,
                    now,
                    Arc::clone(&lookup),
                ))
        {
            bump(&self.counters.stale_served);
            return Ok((addresses.clone(), Origin::Stale(None)));
        }
        // No entry, a very old one, or no thread for a background refresh: resolve in line.
        bump(if joined {
            &self.counters.coalesced
        } else {
            &self.counters.misses
        });
        match self.run(&key, &flight, policy, now, &*lookup).await {
            Ok(addresses) => Ok((addresses, Origin::Resolved)),
            Err(error) => match stale {
                Some((addresses, _)) => {
                    bump(&self.counters.stale_served);
                    Ok((addresses, Origin::Stale(Some(error))))
                }
                None => Err(error),
            },
        }
    }

    /// Refreshes `key` on a thread of its own: a sync connection's runtime only
    /// runs while that connection is in a call, so it cannot host the refresh.
    /// Returns false when no thread could be started.
    fn spawn_refresh<L, Fut>(
        self: &Arc<Self>,
        key: Key,
        flight: Arc<Flight>,
        policy: Policy,
        now: Instant,
        lookup: Arc<L>,
    ) -> bool
    where
        L: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static,
    {
        let cache = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name("arrowsql-dns-refresh".into())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => {
                        let _ = runtime.block_on(cache.run(&key, &flight, policy, now, &*lookup));
                    }
                    Err(_) => cache.forget_flight(&key, &flight),
                }
            })
            .is_ok();
        if spawned {
            bump(&self.counters.refreshes);
        }
        spawned
    }

    /// Runs `flight` (or waits for whoever runs it) and returns its answer.
    /// The first poller does the resolver call; if it is cancelled (its
    /// connect timed out), the next waiter takes over.
    async fn run<L, Fut>(
        &self,
        key: &Key,
        flight: &Arc<Flight>,
        policy: Policy,
        now: Instant,
        lookup: &L,
    ) -> io::Result<Vec<SocketAddr>>
    where
        L: Fn() -> Fut,
        Fut: Future<Output = io::Result<Vec<SocketAddr>>>,
    {
        let answer = flight
            .answer
            .get_or_init(|| async {
                let answer = self.lookup_with_retry(policy, lookup).await;
                let mut state = self.lock();
                if let Ok(addresses) = &answer
                    && !addresses.is_empty()
                {
                    self.store(&mut state, key.clone(), addresses.clone(), now);
                }
                Self::remove_flight(&mut state, key, flight);
                answer.map_err(|error| (error.kind(), error.to_string()))
            })
            .await;
        answer
            .clone()
            .map_err(|(kind, message)| io::Error::new(kind, message))
    }

    async fn lookup_with_retry<L, Fut>(
        &self,
        policy: Policy,
        lookup: &L,
    ) -> io::Result<Vec<SocketAddr>>
    where
        L: Fn() -> Fut,
        Fut: Future<Output = io::Result<Vec<SocketAddr>>>,
    {
        let mut attempt = 1;
        loop {
            bump(&self.counters.resolver_calls);
            let result = match tokio::time::timeout(policy.attempt_timeout, lookup()).await {
                Ok(result) => result,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "DNS resolution timed out after {} ms",
                        policy.attempt_timeout.as_millis()
                    ),
                )),
            };
            match result {
                Ok(addresses) => return Ok(addresses),
                Err(error) => {
                    bump(&self.counters.resolver_failures);
                    if attempt >= policy.attempts || !is_transient(&error) {
                        return Err(error);
                    }
                    attempt += 1;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    fn store(&self, state: &mut State, key: Key, addresses: Vec<SocketAddr>, now: Instant) {
        let entries = &mut state.entries;
        if !entries.contains_key(&key) && entries.len() >= self.max_entries {
            let oldest = entries
                .iter()
                .min_by_key(|(_, entry)| entry.resolved_at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            Entry {
                addresses,
                resolved_at: now,
            },
        );
    }

    fn remove_flight(state: &mut State, key: &Key, flight: &Arc<Flight>) {
        if state
            .flights
            .get(key)
            .is_some_and(|f| Arc::ptr_eq(f, flight))
        {
            state.flights.remove(key);
        }
    }

    fn forget_flight(&self, key: &Key, flight: &Arc<Flight>) {
        Self::remove_flight(&mut self.lock(), key, flight);
    }

    /// Stores `addresses` for `host:port` as resolved `age` before `now` (tests).
    pub fn seed(
        &self,
        host: &str,
        port: u16,
        addresses: Vec<SocketAddr>,
        now: Instant,
        age: Duration,
    ) {
        let resolved_at = now.checked_sub(age).unwrap_or(now);
        self.store(
            &mut self.lock(),
            Self::key(host, port),
            addresses,
            resolved_at,
        );
    }

    /// Drops the entry for `host:port` (connecting to its addresses failed).
    pub fn invalidate(&self, host: &str, port: u16) {
        self.lock().entries.remove(&Self::key(host, port));
    }

    /// Drops every entry; counters are kept.
    pub fn clear(&self) {
        self.lock().entries.clear();
    }

    /// Entries cached.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether nothing is cached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Counters and sizes.
    pub fn stats(&self) -> Stats {
        let (entries, in_flight) = {
            let state = self.lock();
            (state.entries.len(), state.flights.len())
        };
        let c = &self.counters;
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Stats {
            entries,
            in_flight,
            hits: get(&c.hits),
            stale_served: get(&c.stale_served),
            misses: get(&c.misses),
            coalesced: get(&c.coalesced),
            refreshes: get(&c.refreshes),
            resolver_calls: get(&c.resolver_calls),
            resolver_failures: get(&c.resolver_failures),
        }
    }
}

/// Whether retrying `error` can help: a timeout or the resolver's "try again"
/// (`EAI_AGAIN`: "Temporary failure in name resolution"), not an unknown name.
fn is_transient(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
    ) {
        return true;
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("temporar") || message.contains("try again") || message.contains("timed out")
}
