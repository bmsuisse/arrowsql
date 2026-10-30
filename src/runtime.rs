//! The process-wide tokio runtime.
//!
//! Every network call is `block_on`'d from the calling Python thread with the
//! GIL released (see `py.detach` call sites); the runtime's workers drive the
//! socket I/O. One runtime is shared by all connections so thread count stays
//! flat no matter how many connections the application opens.
//!
//! Fork safety: `fork(2)` copies only the calling thread into the child, so a
//! runtime created before the fork has no workers there and can never finish a
//! task. Each runtime is therefore tagged with the pid that built it, and a
//! process with another pid builds its own. The parent's runtime is leaked
//! rather than dropped: shutting it down would try to join worker threads that
//! only exist in the parent.

use std::future::Future;

use parking_lot::Mutex;
use tokio::runtime::{Builder, Runtime};

struct OwnedRuntime {
    pid: u32,
    runtime: &'static Runtime,
}

static RUNTIME: Mutex<Option<OwnedRuntime>> = Mutex::new(None);

pub fn runtime() -> &'static Runtime {
    let pid = std::process::id();
    let mut slot = RUNTIME.lock();
    if let Some(owned) = slot.as_ref() {
        if owned.pid == pid {
            return owned.runtime;
        }
    }
    // First use in this process, or first use in a child forked after the
    // runtime was built. The previous runtime (if any) is intentionally not
    // dropped; see the module docs.
    let runtime: &'static Runtime = Box::leak(Box::new(build_runtime()));
    *slot = Some(OwnedRuntime { pid, runtime });
    runtime
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

/// Drive `future` to completion on the shared runtime. Callers must not hold
/// the GIL (wrap in `py.detach`) unless the future never awaits I/O.
pub fn block_on<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}
