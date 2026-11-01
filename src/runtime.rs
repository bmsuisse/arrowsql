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
//! The shared multi-thread runtime ([`runtime`]) serves `aio` and the
//! cancellation timers; one runtime keeps the thread count flat no matter how
//! many connections the application opens.
//!
//! Fork safety: `fork(2)` copies only the calling thread into the child, so a
//! runtime created before the fork has no workers there and can never finish a
//! task. Each runtime is therefore tagged with the pid that built it, and a
//! process with another pid builds its own. The parent's runtime is leaked
//! rather than dropped: shutting it down would try to join worker threads that
//! only exist in the parent.

use std::future::Future;
use std::cell::{Cell, RefCell};
use std::ops::{Deref, DerefMut};
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::Duration;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use mssql_tds::connection::client_context::ClientContext;
use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::TdsResult;

use parking_lot::Mutex;
use pyo3::prelude::*;
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

pub fn runtime() -> &'static Runtime {
    try_runtime().expect("failed to start the mssql-rs I/O runtime (thread limit reached?)")
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
/// [`runtime`] call builds a runtime for the child.
pub fn forget_after_fork() {
    if BUILD.is_locked() {
        // SAFETY: its owner thread does not exist in this process.
        unsafe { BUILD.force_unlock() };
    }
    OWNER_PID.store(0, Ordering::Release);
    // This thread is the child's only one, hence its main thread whatever it
    // was in the parent (`threading` makes the forking thread the main one).
    MAIN_THREAD.with(|m| m.set(0));
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
    /// runtime, on the calling thread. Callers must not hold the GIL.
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

/// How often a wait on the main thread looks at pending Python signals.
const SIGNAL_SLICE: Duration = Duration::from_millis(100);

thread_local! {
    /// The statement whose execution or fetch this thread is running with the
    /// GIL released (see [`detach_interruptible`]); null otherwise, and always
    /// null off the main thread (only it can receive signals).
    static WATCHED: Cell<*const StmtShared> = const { Cell::new(std::ptr::null()) };
    /// A Python signal handler is running inside a watched wait, with the
    /// statement's locks held (see [`interrupted`]).
    static IN_HANDLER: Cell<bool> = const { Cell::new(false) };
    /// The exception a signal handler raised while the statement was waited on.
    static RAISED: RefCell<Option<PyErr>> = const { RefCell::new(None) };
    /// 0 = not asked yet, 1 = main thread, 2 = another thread.
    static MAIN_THREAD: Cell<u8> = const { Cell::new(0) };
}

/// Whether the calling thread is Python's main thread. Asked of Python once
/// per thread, with the GIL held and no driver lock held (it runs Python
/// code, which can run finalizers).
fn on_main_thread(py: Python<'_>) -> bool {
    if MAIN_THREAD.with(Cell::get) == 0 {
        let is_main = || -> PyResult<bool> {
            let threading = py.import("threading")?;
            Ok(threading.call_method0("current_thread")?.is(&threading.call_method0("main_thread")?))
        };
        MAIN_THREAD.with(|m| m.set(if is_main().unwrap_or(false) { 1 } else { 2 }));
    }
    MAIN_THREAD.with(Cell::get) == 1
}

/// Look at Python's pending signals (only the main thread can). When a signal
/// handler raises (Ctrl-C), cancel the watched statement on the server and
/// keep the exception for [`detach_interruptible`] to re-raise. Returns whether
/// to stop looking.
///
/// The handler runs inside the wait, so the statement's connection and state
/// locks are held by this very thread. A handler may call `cursor.cancel()`
/// (lock free), but any driver call that needs those locks would wait for
/// itself: [`Lockable::lock_checked`] turns that into an immediate error.
/// Attaching to the GIL here cannot deadlock against another thread: nobody
/// blocks on a statement lock while holding the GIL.
fn interrupted() -> bool {
    let stmt = WATCHED.with(Cell::get);
    Python::attach(|py| {
        IN_HANDLER.with(|h| h.set(true));
        let result = py.check_signals();
        IN_HANDLER.with(|h| h.set(false));
        match result {
            Ok(()) => false,
            Err(err) => {
                RAISED.with(|r| *r.borrow_mut() = Some(err));
                // SAFETY: `detach_interruptible` keeps the statement alive
                // for as long as the pointer is published.
                unsafe { &*stmt }.cancel_execution();
                true
            }
        }
    })
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

/// [`Python::detach`] for running a statement: while `f` waits for the server
/// on the main thread, Python signals are looked at every [`SIGNAL_SLICE`].
/// Ctrl-C cancels the statement on the server and raises its
/// `KeyboardInterrupt` once `f` has returned, leaving the connection usable.
pub fn detach_interruptible<R: Send>(
    py: Python<'_>,
    stmt: &StmtShared,
    f: impl FnOnce() -> R + Send,
) -> PyResult<R> {
    struct Restore(*const StmtShared);
    impl Drop for Restore {
        fn drop(&mut self) {
            WATCHED.with(|w| w.set(self.0));
        }
    }
    let published = if on_main_thread(py) { std::ptr::from_ref(stmt) as usize } else { 0 };
    let out = py.detach(|| {
        let _restore = Restore(WATCHED.with(|w| w.replace(published as *const StmtShared)));
        f()
    });
    match RAISED.with(|r| r.borrow_mut().take()) {
        Some(err) => Err(err),
        None => Ok(out),
    }
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
