//! Tests for the vendored process-wide DNS cache
//! (`vendor/mssql-tds/src/connection/dns_ttl_cache.rs`, VENDOR.md patch 15).
//! The vendored crate's own `cargo test` does not build, so the cache file
//! (std + tokio only) is compiled into this crate for tests (see `lib.rs`).

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::dns_ttl_cache::{Origin, Policy, TtlDnsCache, effective_ttl};

const TTL: Duration = Duration::from_secs(300);

fn addr(ip: &str) -> SocketAddr {
    format!("{ip}:1433").parse().unwrap()
}

fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}

fn policy(ttl: Duration) -> Policy {
    Policy {
        attempts: 1,
        ..Policy::with_ttl(ttl)
    }
}

/// One scripted resolver answer.
type Answer = Result<Vec<SocketAddr>, String>;

/// A scripted resolver: answers from a queue (the last answer repeats) and
/// counts its calls.
#[derive(Clone, Default)]
struct Fake {
    calls: Arc<AtomicU32>,
    answers: Arc<Mutex<Vec<Answer>>>,
    delay: Duration,
}

impl Fake {
    fn new(answers: Vec<Result<Vec<&str>, &str>>) -> Self {
        let answers = answers
            .into_iter()
            .map(|a| {
                a.map(|ips| ips.into_iter().map(addr).collect())
                    .map_err(str::to_owned)
            })
            .collect();
        Self {
            answers: Arc::new(Mutex::new(answers)),
            ..Self::default()
        }
    }

    fn ok(ips: &[&str]) -> Self {
        Self::new(vec![Ok(ips.to_vec())])
    }

    fn failing(message: &str) -> Self {
        Self::new(vec![Err(message)])
    }

    fn slow(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn lookup(
        &self,
    ) -> impl Fn() -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Vec<SocketAddr>>> + Send>,
    > + Send
    + Sync
    + 'static {
        let fake = self.clone();
        move || {
            fake.calls.fetch_add(1, Ordering::SeqCst);
            let answer = {
                let mut answers = fake.answers.lock().unwrap();
                if answers.len() > 1 {
                    answers.remove(0)
                } else {
                    answers[0].clone()
                }
            };
            let delay = fake.delay;
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                answer.map_err(io::Error::other)
            })
        }
    }
}

fn resolve(
    cache: &Arc<TtlDnsCache>,
    host: &str,
    policy: Policy,
    now: Instant,
    fake: &Fake,
) -> io::Result<(Vec<SocketAddr>, Origin)> {
    block_on(cache.resolve(host, 1433, policy, now, fake.lookup()))
}

/// Waits for background refreshes to finish.
fn settle(cache: &TtlDnsCache) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while cache.stats().in_flight > 0 {
        assert!(
            Instant::now() < deadline,
            "background refresh did not finish"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn fresh_entry_is_served_without_resolving() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.0.0.1", "10.0.0.2"]);
    let (a, origin) = resolve(&cache, "Db.Example", policy(TTL), t0, &fake).unwrap();
    assert_eq!(
        a,
        vec![addr("10.0.0.1"), addr("10.0.0.2")],
        "order and all IPs kept"
    );
    assert!(matches!(origin, Origin::Resolved));

    // Host case does not matter; the resolver would fail if it were asked.
    let down = Fake::failing("Temporary failure in name resolution");
    let later = t0 + Duration::from_secs(299);
    let (a, origin) = resolve(&cache, "db.example", policy(TTL), later, &down).unwrap();
    assert_eq!(a, vec![addr("10.0.0.1"), addr("10.0.0.2")]);
    assert!(matches!(origin, Origin::Fresh));
    assert_eq!((fake.calls(), down.calls()), (1, 0));
    let stats = cache.stats();
    assert_eq!((stats.hits, stats.misses, stats.entries), (1, 1, 1));
}

#[test]
fn stale_entry_is_served_at_once_and_refreshed_in_the_background() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    resolve(&cache, "h", policy(TTL), t0, &Fake::ok(&["10.0.0.1"])).unwrap();

    // The refresh takes a while; the caller does not wait for it.
    let slow = Fake::ok(&["10.0.0.2"]).slow(Duration::from_millis(300));
    let t1 = t0 + TTL;
    let started = Instant::now();
    let (a, origin) = resolve(&cache, "h", policy(TTL), t1, &slow).unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "served without waiting"
    );
    assert_eq!(a, vec![addr("10.0.0.1")]);
    assert!(matches!(origin, Origin::Stale(None)));
    // A second caller during the refresh is served stale too, without a second refresh.
    let (a, _) = resolve(&cache, "h", policy(TTL), t1, &slow).unwrap();
    assert_eq!(a, vec![addr("10.0.0.1")]);

    settle(&cache);
    assert_eq!(slow.calls(), 1, "one background refresh");
    let (a, origin) =
        resolve(&cache, "h", policy(TTL), t1 + Duration::from_secs(1), &slow).unwrap();
    assert_eq!(a, vec![addr("10.0.0.2")], "the refresh replaced the entry");
    assert!(matches!(origin, Origin::Fresh));
    let stats = cache.stats();
    assert_eq!((stats.stale_served, stats.refreshes), (2, 1));
}

#[test]
fn a_failed_background_refresh_keeps_the_stale_entry() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    resolve(&cache, "h", policy(TTL), t0, &Fake::ok(&["10.0.0.1"])).unwrap();
    let down = Fake::failing("Temporary failure in name resolution");
    let t1 = t0 + TTL * 3;
    for _ in 0..2 {
        let (a, origin) = resolve(&cache, "h", policy(TTL), t1, &down).unwrap();
        assert_eq!(a, vec![addr("10.0.0.1")]);
        assert!(matches!(origin, Origin::Stale(None)));
        settle(&cache);
    }
    // Each stale use retried the refresh; the entry is still there and still stale.
    assert_eq!(down.calls(), 2);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.stats().resolver_failures, 2);
}

#[test]
fn a_very_stale_entry_is_refreshed_in_line_and_served_if_that_fails() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let p = Policy {
        max_stale: Duration::from_secs(60),
        ..policy(TTL)
    };
    resolve(&cache, "h", p, t0, &Fake::ok(&["10.0.0.1"])).unwrap();
    let too_old = t0 + TTL + Duration::from_secs(61);

    let (a, origin) =
        resolve(&cache, "h", p, too_old, &Fake::failing("Temporary failure")).unwrap();
    assert_eq!(a, vec![addr("10.0.0.1")]);
    match origin {
        Origin::Stale(Some(error)) => assert!(error.to_string().contains("Temporary failure")),
        other => panic!("expected stale after an in-line failure, got {other:?}"),
    }
    let (a, origin) = resolve(&cache, "h", p, too_old, &Fake::ok(&["10.0.0.3"])).unwrap();
    assert_eq!(a, vec![addr("10.0.0.3")]);
    assert!(matches!(origin, Origin::Resolved));
}

#[test]
fn failures_and_empty_answers_are_never_cached() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::new(vec![
        Err("Name or service not known"),
        Ok(vec![]),
        Ok(vec!["10.0.0.1"]),
    ]);
    assert!(resolve(&cache, "h", policy(TTL), t0, &fake).is_err());
    assert!(cache.is_empty());
    assert!(
        resolve(&cache, "h", policy(TTL), t0, &fake)
            .unwrap()
            .0
            .is_empty()
    );
    assert!(cache.is_empty());
    let (a, _) = resolve(&cache, "h", policy(TTL), t0, &fake).unwrap();
    assert_eq!(
        (a, fake.calls()),
        (vec![addr("10.0.0.1")], 3),
        "a failure did not stick"
    );
}

#[test]
fn ttl_zero_disables_the_cache_entirely() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.0.0.1"]);
    for _ in 0..3 {
        let (_, origin) = resolve(&cache, "h", policy(Duration::ZERO), t0, &fake).unwrap();
        assert!(matches!(origin, Origin::Bypass));
    }
    assert_eq!(fake.calls(), 3);
    assert!(cache.is_empty());
    // A failure is not softened by anything cached earlier, nor retried.
    resolve(&cache, "h", policy(TTL), t0, &fake).unwrap();
    let down = Fake::failing("Temporary failure in name resolution");
    let p = Policy {
        attempts: 3,
        ..policy(Duration::ZERO)
    };
    assert!(resolve(&cache, "h", p, t0, &down).is_err());
    assert_eq!(down.calls(), 1, "no retry when disabled");
    assert_eq!(
        cache.stats().resolver_calls,
        1,
        "bypasses are not counted as cache lookups"
    );
}

#[test]
fn ip_literals_bypass_the_cache() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.1.2.3"]);
    for host in ["10.1.2.3", "::1", "2001:db8::1"] {
        for _ in 0..2 {
            let (_, origin) = resolve(&cache, host, policy(TTL), t0, &fake).unwrap();
            assert!(matches!(origin, Origin::Bypass), "{host}");
        }
    }
    assert_eq!(fake.calls(), 6);
    assert!(cache.is_empty());
}

#[test]
fn concurrent_lookups_share_one_resolver_call() {
    // The slices of a parallel read connect at the same moment from a cold cache.
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.0.0.1", "10.0.0.2"]).slow(Duration::from_millis(200));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let cache = Arc::clone(&cache);
            let fake = fake.clone();
            std::thread::spawn(move || resolve(&cache, "h", policy(TTL), t0, &fake).unwrap())
        })
        .collect();
    for handle in handles {
        let (a, origin) = handle.join().unwrap();
        assert_eq!(a, vec![addr("10.0.0.1"), addr("10.0.0.2")]);
        assert!(matches!(origin, Origin::Resolved), "{origin:?}");
    }
    assert_eq!(fake.calls(), 1);
    let stats = cache.stats();
    assert_eq!((stats.misses, stats.coalesced, stats.in_flight), (1, 7, 0));
}

#[test]
fn concurrent_waiters_share_a_failure_without_caching_it() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let down = Fake::failing("Name or service not known").slow(Duration::from_millis(200));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let cache = Arc::clone(&cache);
            let down = down.clone();
            std::thread::spawn(move || resolve(&cache, "h", policy(TTL), t0, &down).is_err())
        })
        .collect();
    assert!(handles.into_iter().all(|h| h.join().unwrap()));
    assert_eq!(down.calls(), 1);
    // The next lookup asks again.
    resolve(&cache, "h", policy(TTL), t0, &Fake::ok(&["10.0.0.1"])).unwrap();
}

#[test]
fn a_transient_failure_is_retried_and_a_permanent_one_is_not() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let p = Policy::with_ttl(TTL);
    assert_eq!(p.attempts, 2);
    let flaky = Fake::new(vec![
        Err("Temporary failure in name resolution"),
        Ok(vec!["10.0.0.1"]),
    ]);
    let (a, _) = resolve(&cache, "flaky", p, t0, &flaky).unwrap();
    assert_eq!((a, flaky.calls()), (vec![addr("10.0.0.1")], 2));

    let unknown = Fake::failing("Name or service not known");
    assert!(resolve(&cache, "unknown", p, t0, &unknown).is_err());
    assert_eq!(unknown.calls(), 1, "an unknown name is not retried");

    let down = Fake::failing("Temporary failure in name resolution");
    assert!(resolve(&cache, "down", p, t0, &down).is_err());
    assert_eq!(down.calls(), 2, "bounded retries");
}

#[test]
fn a_hung_resolver_call_times_out_and_is_retried() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let p = Policy {
        attempt_timeout: Duration::from_millis(50),
        ..Policy::with_ttl(TTL)
    };
    let hung = Fake::ok(&["10.0.0.1"]).slow(Duration::from_secs(5));
    let started = Instant::now();
    let error = resolve(&cache, "h", p, t0, &hung).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(hung.calls(), 2);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(cache.stats().resolver_failures, 2);
}

#[test]
fn the_oldest_entry_is_evicted_when_full() {
    let cache = Arc::new(TtlDnsCache::with_capacity(2));
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.0.0.1"]);
    for (i, host) in ["a", "b", "c"].into_iter().enumerate() {
        resolve(
            &cache,
            host,
            policy(TTL),
            t0 + Duration::from_secs(i as u64),
            &fake,
        )
        .unwrap();
    }
    assert_eq!(cache.len(), 2);
    let now = t0 + Duration::from_secs(3);
    let down = Fake::failing("Name or service not known");
    assert!(matches!(
        resolve(&cache, "b", policy(TTL), now, &down).unwrap().1,
        Origin::Fresh
    ));
    assert!(matches!(
        resolve(&cache, "c", policy(TTL), now, &down).unwrap().1,
        Origin::Fresh
    ));
    assert!(
        resolve(&cache, "a", policy(TTL), now, &down).is_err(),
        "a was evicted"
    );
}

#[test]
fn the_port_is_part_of_the_key() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.0.0.1"]);
    block_on(cache.resolve("h", 1433, policy(TTL), t0, fake.lookup())).unwrap();
    let other = block_on(cache.resolve("h", 1434, policy(TTL), t0, fake.lookup())).unwrap();
    assert!(matches!(other.1, Origin::Resolved));
    assert_eq!(cache.len(), 2);
}

#[test]
fn invalidate_after_a_failed_connect_forces_a_fresh_lookup() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    resolve(&cache, "h", policy(TTL), t0, &Fake::ok(&["10.0.0.1"])).unwrap();
    let down = Fake::failing("Name or service not known");
    assert!(matches!(
        resolve(&cache, "h", policy(TTL), t0, &down).unwrap().1,
        Origin::Fresh
    ));

    // The caller could not connect to the cached answer: drop it and look again.
    cache.invalidate("H", 1433);
    let (a, origin) = resolve(&cache, "h", policy(TTL), t0, &Fake::ok(&["10.0.0.7"])).unwrap();
    assert_eq!(a, vec![addr("10.0.0.7")]);
    assert!(matches!(origin, Origin::Resolved));
    // If that lookup fails there is nothing stale left to hide behind.
    cache.invalidate("h", 1433);
    assert!(resolve(&cache, "h", policy(TTL), t0, &down).is_err());
}

#[test]
fn ttl_comes_from_the_connection_then_the_environment_then_the_default() {
    assert_eq!(effective_ttl(Some(5), Some("60")), Duration::from_secs(5));
    assert_eq!(effective_ttl(Some(0), Some("60")), Duration::ZERO);
    assert_eq!(effective_ttl(None, Some(" 60 ")), Duration::from_secs(60));
    assert_eq!(effective_ttl(None, Some("0")), Duration::ZERO);
    assert_eq!(effective_ttl(None, Some("junk")), Duration::from_secs(300));
    assert_eq!(effective_ttl(None, None), Duration::from_secs(300));
}

#[test]
fn concurrent_fresh_lookups_from_many_threads() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    resolve(&cache, "h", policy(TTL), t0, &Fake::ok(&["10.0.0.1"])).unwrap();
    let down = Fake::failing("Name or service not known");
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let cache = Arc::clone(&cache);
            let down = down.clone();
            std::thread::spawn(move || resolve(&cache, "h", policy(TTL), t0, &down).unwrap().1)
        })
        .collect();
    for handle in handles {
        assert!(matches!(handle.join().unwrap(), Origin::Fresh));
    }
    assert_eq!(down.calls(), 0);
}

#[test]
fn clear_drops_the_entries_and_keeps_the_counters() {
    let cache = Arc::new(TtlDnsCache::default());
    let t0 = Instant::now();
    let fake = Fake::ok(&["10.0.0.1"]);
    resolve(&cache, "h", policy(TTL), t0, &fake).unwrap();
    cache.clear();
    assert!(cache.is_empty());
    resolve(&cache, "h", policy(TTL), t0, &fake).unwrap();
    assert_eq!((fake.calls(), cache.stats().misses), (2, 2));
}

#[test]
fn a_seeded_entry_ages_like_a_resolved_one() {
    let cache = Arc::new(TtlDnsCache::default());
    let now = Instant::now();
    let down = Fake::failing("Name or service not known");
    cache.seed(
        "h",
        1433,
        vec![addr("10.0.0.5")],
        now,
        Duration::from_secs(10),
    );
    assert!(matches!(
        resolve(&cache, "h", policy(TTL), now, &down).unwrap().1,
        Origin::Fresh
    ));
    cache.seed("h", 1433, vec![addr("10.0.0.5")], now, TTL);
    let (a, origin) = resolve(&cache, "h", policy(TTL), now, &down).unwrap();
    assert_eq!(a, vec![addr("10.0.0.5")]);
    assert!(matches!(origin, Origin::Stale(None)));
    settle(&cache);
}
