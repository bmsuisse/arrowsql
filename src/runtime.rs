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
    let pid = std::process::id();
    if OWNER_PID.load(Ordering::Acquire) == pid {
        // SAFETY: the pointer is a leaked runtime, stored before the pid.
        return unsafe { &*CURRENT.load(Ordering::Acquire) };
    }
    let _build = BUILD.lock();
    if OWNER_PID.load(Ordering::Acquire) == pid {
        return unsafe { &*CURRENT.load(Ordering::Acquire) };
    }
    // First use in this process, or first use in a child forked after the
    // runtime was built. The previous runtime (if any) is intentionally not
    // dropped; see the module docs.
    let runtime: &'static Runtime = Box::leak(Box::new(build_runtime()));
    CURRENT.store(std::ptr::from_ref(runtime).cast_mut(), Ordering::Release);
    OWNER_PID.store(pid, Ordering::Release);
    runtime
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
}

fn build_runtime() -> Runtime {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4);
    Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("mssql-rs-io")
        .enable_all()
        .build()
        .expect("failed to start the mssql-rs I/O runtime (thread limit reached?)")
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
}

impl SyncClient {
    /// Open a session; its socket belongs to a fresh runtime of its own.
    pub fn connect(context: ClientContext, datasource: &str) -> TdsResult<Self> {
        let io = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to start the connection I/O runtime (file descriptor limit reached?)");
        let client = io.block_on(TdsConnectionProvider {}.create_client(context, datasource, None))?;
        Ok(Self { client, slice: None, io })
    }

    /// Drive the future built by `op` to completion on this connection's
    /// runtime, on the calling thread. Callers must not hold the GIL.
    pub fn block_on<'a, F, Fut>(&'a mut self, op: F) -> Fut::Output
    where
        F: FnOnce(&'a mut TdsClient) -> Fut,
        Fut: Future,
    {
        let Self { client, slice, io } = self;
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
    /// GIL released (see [`detach_interruptible`]); null otherwise.
    static WATCHED: Cell<*const StmtShared> = const { Cell::new(std::ptr::null()) };
    /// The exception a signal handler raised while the statement was waited on.
    static RAISED: RefCell<Option<PyErr>> = const { RefCell::new(None) };
    /// 0 = not asked yet, 1 = main thread, 2 = another thread.
    static MAIN_THREAD: Cell<u8> = const { Cell::new(0) };
}

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
fn interrupted() -> bool {
    let stmt = WATCHED.with(Cell::get);
    Python::attach(|py| {
        if !on_main_thread(py) {
            return true;
        }
        match py.check_signals() {
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
    let out = py.detach(|| {
        let _restore = Restore(WATCHED.with(|w| w.replace(std::ptr::from_ref(stmt))));
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
