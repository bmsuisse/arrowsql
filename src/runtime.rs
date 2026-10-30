//! The single process-wide tokio runtime.
//!
//! Every network call is `block_on`'d from the calling Python thread with the
//! GIL released (see `py.detach` call sites); the runtime's workers drive the
//! socket I/O. One runtime is shared by all connections so thread count stays
//! flat no matter how many connections the application opens.

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get().clamp(2, 8))
            .unwrap_or(4);
        Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("mssql-rs-io")
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}

/// Drive `future` to completion on the shared runtime. Callers must not hold
/// the GIL (wrap in `py.detach`) unless the future never awaits I/O.
pub fn block_on<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}
