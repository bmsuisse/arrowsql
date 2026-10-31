//! RUST-PORT: bounded busy polling for the first byte of a response.
//!
//! A synchronous caller that sends a short query and waits for the answer
//! sleeps in `epoll_wait` for the ~100 us the server needs, and pays the
//! scheduler's wake-up latency (idle core, C-state exit, a second syscall) on
//! top. Checking the socket with a zero timeout for that short window, through
//! the runtime's own zero-timeout driver turns, skips the sleep. The spin is deliberately narrow:
//!
//! * only on threads that opted in ([`ThreadGuard`], used by the synchronous
//!   client; async tasks on the shared runtime never spin),
//! * only for the first read after a request was sent,
//! * only while this connection's recent waits were short (an adaptive
//!   estimate), and for at most twice that estimate,
//! * at most two threads at a time, none on machines with fewer than 4 CPUs,
//! * never when `MSSQL_RS_BUSY_POLL=0`.

use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Connections whose recent first-byte wait is above this do not spin.
pub(crate) const ENTRY_WAIT_NS: u64 = 300_000;
const MIN_SPIN_NS: u64 = 100_000;
const MAX_SPIN_NS: u64 = 400_000;
/// Estimate used before the first measurement.
pub(crate) const INITIAL_WAIT_NS: u64 = 150_000;

static SPINNERS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
}

/// Allows busy polling on the current thread until dropped.
pub struct ThreadGuard(bool);

impl ThreadGuard {
    pub fn enter() -> Self {
        Self(ENABLED.with(|e| e.replace(true)))
    }
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        ENABLED.with(|e| e.set(self.0));
    }
}

fn max_spinners() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        if std::env::var("MSSQL_RS_BUSY_POLL").is_ok_and(|v| matches!(v.trim(), "0" | "off" | "false")) {
            return 0;
        }
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        (cpus / 4).min(2)
    })
}

/// Whether a first-byte wait of `expected_ns` is worth spinning for.
#[inline]
pub(crate) fn should_spin(expected_ns: u64) -> bool {
    expected_ns < ENTRY_WAIT_NS && ENABLED.with(Cell::get)
}

/// Holds one of the spinner slots until dropped.
struct Slot;

impl Slot {
    fn acquire() -> Option<Self> {
        let max = max_spinners();
        if max == 0 || SPINNERS.fetch_add(1, Ordering::AcqRel) >= max {
            if max != 0 {
                SPINNERS.fetch_sub(1, Ordering::AcqRel);
            }
            return None;
        }
        Some(Self)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        SPINNERS.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(unix)]
fn readable(fd: i32) -> bool {
    let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
    // SAFETY: `pfd` is a valid pollfd for the duration of the call.
    unsafe { libc::poll(&mut pfd, 1, 0) != 0 }
}

/// Waits until `fd` has data, for at most twice `expected_ns`, without sleeping.
///
/// Each round yields to the runtime, whose zero-timeout driver turn
/// (`epoll_wait(0)`) is what makes the socket's readiness visible to the read
/// that follows; the last yield after the data showed up lets that read start
/// from a ready socket instead of parking once more.
#[cfg(unix)]
pub(crate) async fn spin(fd: i32, expected_ns: u64) {
    let Some(_slot) = Slot::acquire() else { return };
    let limit = Duration::from_nanos(expected_ns.saturating_mul(2).clamp(MIN_SPIN_NS, MAX_SPIN_NS));
    let started = Instant::now();
    loop {
        if readable(fd) {
            tokio::task::yield_now().await;
            return;
        }
        if started.elapsed() > limit {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(not(unix))]
pub(crate) async fn spin(_fd: i32, _expected_ns: u64) {}

/// Whether an idle session's socket shows that the server ended it. A TDS
/// server sends nothing unasked, so any readiness at all (data such as a TLS
/// alert, a FIN, an error) means the session is gone. Reads nothing. `None`
/// when it cannot be told (no socket descriptor, or not a unix platform).
#[cfg(unix)]
pub(crate) fn idle_peer_gone(fd: i32) -> Option<bool> {
    (fd >= 0).then(|| readable(fd))
}

#[cfg(not(unix))]
pub(crate) fn idle_peer_gone(_fd: i32) -> Option<bool> {
    None
}
