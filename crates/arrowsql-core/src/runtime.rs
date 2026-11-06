//! Tokio runtimes: one per sync connection, one shared for everything else.
//!
//! A sync connection ([`SyncClient`]) owns a current-thread runtime that has
//! no worker threads, only a reactor and a timer. Every network call is
//! `block_on`'d from the calling Python thread with the GIL released, and that
//! thread itself waits on the socket, so readiness wakes it directly (one
//! wake-up per network wait, like a blocking `recv`). Going through the shared
//! multi-thread runtime instead costs a second thread hop per wait.
//!
//! The statement mutex lets one thread at a time use a connection, and a
//! current-thread runtime may be driven from any thread in turn, so pooled
//! connections move freely between threads.
//!
//! The shared multi-thread runtime ([`try_runtime`]) serves `aio` and the
//! cancellation timers; one runtime keeps the thread count flat no matter how
//! many connections the application opens.
//!
//! A host with its own interruption model (Python's signal handlers) installs
//! an [`InterruptCheck`]; waits on a statement published with [`watching`] ask
//! it every [`SIGNAL_SLICE`] whether to cancel the statement.
//!
//! Fork safety: `fork(2)` copies only the calling thread into the child, so a
//! runtime created before the fork has no workers there and can never finish a
//! task. Each runtime is therefore tagged with the pid that built it, and a
//! process with another pid builds its own. The parent's runtime is leaked
//! rather than dropped: shutting it down would try to join worker threads that
//! only exist in the parent.

use std::future::Future;
use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::Duration;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use mssql_tds::connection::client_context::ClientContext;
use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::TdsResult;

use parking_lot::Mutex;
use tokio::runtime::{Builder, Runtime};
use tokio::time::{Instant, Sleep};

use crate::statement::StmtShared;

/// The runtime of this process and the pid that built it. Readers check the
/// pid first, without a lock: a forked child must never need a lock another
/// parent thread held at fork time just to notice it is in a new process.
static CURRENT: AtomicPtr<Runtime> = AtomicPtr::new(std::ptr::null_mut());
static OWNER_PID: AtomicU32 = AtomicU32::new(0);
/// Serializes building a runtime.
static BUILD: Mutex<()> = Mutex::new(());

/// The diagnostic for a shared runtime that could not start.
pub fn start_failure(e: &std::io::Error) -> crate::diag::DiagRecord {
    crate::diag::DiagRecord::driver(
        "HY001",
        format!("failed to start the mssql-rs I/O runtime (thread limit reached?): {e}"),
    )
}

/// The process-wide runtime, or the OS error that stopped it from starting
/// (thread limit reached).
pub fn try_runtime() -> std::io::Result<&'static Runtime> {
    let pid = std::process::id();
    if OWNER_PID.load(Ordering::Acquire) == pid {
        // SAFETY: the pointer is a leaked runtime, stored before the pid.
        return Ok(unsafe { &*CURRENT.load(Ordering::Acquire) });
    }
    let _build = BUILD.lock();
    if OWNER_PID.load(Ordering::Acquire) == pid {
        return Ok(unsafe { &*CURRENT.load(Ordering::Acquire) });
    }
    // First use in this process, or first use in a child forked after the
    // runtime was built. The previous runtime (if any) is intentionally not
    // dropped; see the module docs.
    let runtime: &'static Runtime = Box::leak(Box::new(build_runtime()?));
    CURRENT.store(std::ptr::from_ref(runtime).cast_mut(), Ordering::Release);
    OWNER_PID.store(pid, Ordering::Release);
    Ok(runtime)
}

/// Runs in a forked child, which has only the forking thread: a `BUILD` lock
/// held by another parent thread would never be released. The next
/// [`try_runtime`] call builds a runtime for the child.
pub fn forget_after_fork() {
    if BUILD.is_locked() {
        // SAFETY: its owner thread does not exist in this process.
        unsafe { BUILD.force_unlock() };
    }
    OWNER_PID.store(0, Ordering::Release);
}

fn build_runtime() -> std::io::Result<Runtime> {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4);
    Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("mssql-rs-io")
        .enable_all()
        .build()
}

/// A TDS session together with the current-thread runtime its socket is
/// registered with. Field order matters: the client must drop before the
/// runtime whose reactor holds its socket.
pub struct SyncClient {
    client: TdsClient,
    /// Wakes a watched wait every [`SIGNAL_SLICE`]; created on first use and
    /// kept registered between calls, so a short call only polls it.
    slice: Option<Pin<Box<Sleep>>>,
    io: Runtime,
    /// The process that opened the socket; a forked child shares it with the parent.
    pid: u32,
}

impl SyncClient {
    /// Open a session; its socket belongs to a fresh runtime of its own.
    pub fn connect(context: ClientContext, datasource: &str) -> TdsResult<Self> {
        let io = Builder::new_current_thread().enable_all().build().map_err(|e| {
            mssql_tds::error::Error::ConnectionError(format!("failed to start the connection I/O runtime: {e}"))
        })?;
        let client = io.block_on(TdsConnectionProvider {}.create_client(context, datasource, None))?;
        Ok(Self { client, slice: None, io, pid: std::process::id() })
    }

    /// Whether a forked child inherited this session from its parent. The
    /// socket (and its TLS state) is the parent's: the child must never write
    /// to it, so it forgets the client instead of closing or sanitising it.
    pub fn is_foreign(&self) -> bool {
        self.pid != std::process::id()
    }

    /// Close the session politely, unless it belongs to the parent process.
    pub fn close(mut self) {
        if self.is_foreign() {
            std::mem::forget(self);
            return;
        }
        let _ = self.block_on(|c| c.close_connection());
    }

    /// Drive the future built by `op` to completion on this connection's
    /// runtime, on the calling thread. Callers must not hold an interpreter
    /// lock (Python's GIL).
    pub fn block_on<'a, F, Fut>(&'a mut self, op: F) -> Fut::Output
    where
        F: FnOnce(&'a mut TdsClient) -> Fut,
        Fut: Future,
    {
        let Self { client, slice, io, .. } = self;
        let _spin = mssql_tds::connection::transport::busy_poll::ThreadGuard::enter();
        if WATCHED.with(Cell::get).is_null() {
            return io.block_on(op(client));
        }
        io.block_on(async move {
            let mut work = pin!(op(client));
            let slice = slice.get_or_insert_with(|| Box::pin(tokio::time::sleep(SIGNAL_SLICE)));
            let mut watching = true;
            let mut armed = false;
            std::future::poll_fn(|cx| {
                if let Poll::Ready(out) = work.as_mut().poll(cx) {
                    return Poll::Ready(out);
                }
                // The busy poll wakes this future many times per round trip;
                // the timer needs a poll only to register this call's waker
                // and once its deadline has passed.
                if armed && Instant::now() < slice.deadline() {
                    return Poll::Pending;
                }
                armed = true;
                while watching && slice.as_mut().poll(cx).is_ready() {
                    watching = !interrupted();
                    slice.as_mut().reset(Instant::now() + SIGNAL_SLICE);
                }
                Poll::Pending
            })
            .await
        })
    }
}

/// How often a watched wait asks the [`InterruptCheck`].
pub const SIGNAL_SLICE: Duration = Duration::from_millis(100);

/// Asked periodically while a statement published with [`watching`] waits
/// for the server. Returning true cancels that statement on the server and
/// stops asking for the rest of the wait. The check runs with the
/// statement's connection and state locks held by the calling thread: it
/// must not run driver calls other than a cancel ([`Lockable::lock_checked`]
/// turns such a call into an immediate panic instead of a self-deadlock).
pub trait InterruptCheck: Send + Sync {
    fn interrupted(&self) -> bool;
}

static INTERRUPT: OnceLock<Box<dyn InterruptCheck>> = OnceLock::new();

/// Install the process-wide interrupt check (once; later calls are ignored).
/// The Python bindings install one that runs pending signal handlers.
pub fn set_interrupt_check(check: Box<dyn InterruptCheck>) {
    let _ = INTERRUPT.set(check);
}

thread_local! {
    /// The statement whose execution or fetch this thread is waiting on
    /// inside [`watching`]; null otherwise.
    static WATCHED: Cell<*const StmtShared> = const { Cell::new(std::ptr::null()) };
    /// The interrupt check is running inside a watched wait, with the
    /// statement's locks held (see [`interrupted`]).
    static IN_HANDLER: Cell<bool> = const { Cell::new(false) };
}

/// Ask the installed [`InterruptCheck`]; when it says stop, cancel the
/// watched statement on the server. Returns whether to stop asking.
fn interrupted() -> bool {
    let stmt = WATCHED.with(Cell::get);
    let Some(check) = INTERRUPT.get() else { return true };
    IN_HANDLER.with(|h| h.set(true));
    let stop = check.interrupted();
    IN_HANDLER.with(|h| h.set(false));
    if stop {
        // SAFETY: `watching` keeps the statement alive for as long as the
        // pointer is published.
        unsafe { &*stmt }.cancel_execution();
    }
    stop
}

/// A lock that fails fast when taken by a signal handler that interrupted
/// this thread's own wait, instead of deadlocking on itself.
pub trait Lockable<T> {
    fn lock_checked(&self) -> parking_lot::MutexGuard<'_, T>;
}

impl<T> Lockable<T> for Mutex<T> {
    #[inline]
    fn lock_checked(&self) -> parking_lot::MutexGuard<'_, T> {
        if IN_HANDLER.with(Cell::get) {
            return self.try_lock().unwrap_or_else(|| {
                panic!(
                    "arrowsql: this call needs the connection a statement on this thread is \
                     waiting on, and was made from a signal handler that interrupted that wait. \
                     Only cursor.cancel() is allowed there; do other work after the handler returns."
                )
            });
        }
        self.lock()
    }
}

/// Run `f` (which waits on `stmt`) with `stmt` published to the installed
/// [`InterruptCheck`]: every [`SIGNAL_SLICE`] of waiting asks it, and a stop
/// cancels the statement on the server, leaving the connection usable.
pub fn watching<R>(stmt: &StmtShared, f: impl FnOnce() -> R) -> R {
    struct Restore(*const StmtShared);
    impl Drop for Restore {
        fn drop(&mut self) {
            WATCHED.with(|w| w.set(self.0));
        }
    }
    let _restore = Restore(WATCHED.with(|w| w.replace(std::ptr::from_ref(stmt))));
    f()
}

thread_local! {
    /// Executions started on this thread are watched (see [`watch_executions`]).
    static AUTO_WATCH: Cell<bool> = const { Cell::new(false) };
}

/// Run `f` with every statement execution it starts on this thread
/// published to the [`InterruptCheck`], as [`watching`] does for one.
pub fn watch_executions<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            AUTO_WATCH.with(|w| w.set(self.0));
        }
    }
    let _restore = Restore(AUTO_WATCH.with(|w| w.replace(true)));
    f()
}

/// [`watching`] when inside [`watch_executions`], else just `f`.
pub fn watching_if_enabled<R>(stmt: &StmtShared, f: impl FnOnce() -> R) -> R {
    if AUTO_WATCH.with(Cell::get) { watching(stmt, f) } else { f() }
}

impl Deref for SyncClient {
    type Target = TdsClient;
    fn deref(&self) -> &TdsClient {
        &self.client
    }
}

impl DerefMut for SyncClient {
    fn deref_mut(&mut self) -> &mut TdsClient {
        &mut self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_start_failure_is_a_driver_diagnostic() {
        let record = start_failure(&std::io::Error::other("Resource temporarily unavailable"));
        assert_eq!(record.sqlstate, "HY001");
        assert!(record.message.contains("failed to start the mssql-rs I/O runtime"));
        assert!(record.message.contains("Resource temporarily unavailable"));
    }
}
