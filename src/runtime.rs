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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use mssql_tds::connection::client_context::ClientContext;
use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::TdsResult;

use parking_lot::Mutex;
use tokio::runtime::{Builder, Runtime};

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
        Ok(Self { client, io })
    }

    /// Drive the future built by `op` to completion on this connection's
    /// runtime, on the calling thread. Callers must not hold the GIL.
    pub fn block_on<'a, F, Fut>(&'a mut self, op: F) -> Fut::Output
    where
        F: FnOnce(&'a mut TdsClient) -> Fut,
        Fut: Future,
    {
        let Self { client, io } = self;
        let _spin = mssql_tds::connection::transport::busy_poll::ThreadGuard::enter();
        io.block_on(op(client))
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
