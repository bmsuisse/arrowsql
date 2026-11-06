//! Python's side of the runtimes in `arrowsql_core::runtime`: Ctrl-C while a
//! statement waits on the server.
//!
//! A statement run with [`detach_interruptible`] on Python's main thread is
//! published to the core's watched waits; the [`InterruptCheck`] installed
//! here runs the pending signal handlers every `SIGNAL_SLICE`. A handler that
//! raises (Ctrl-C) cancels the statement on the server, and its exception is
//! raised once the wait returned, leaving the connection usable.

use std::cell::{Cell, RefCell};

use pyo3::prelude::*;
use tokio::runtime::Runtime;

pub use arrowsql_core::runtime::*;

use crate::statement::StmtShared;

thread_local! {
    /// The exception a signal handler raised while the statement was waited on.
    static RAISED: RefCell<Option<PyErr>> = const { RefCell::new(None) };
    /// 0 = not asked yet, 1 = main thread, 2 = another thread.
    static MAIN_THREAD: Cell<u8> = const { Cell::new(0) };
}

/// [`try_runtime`] for entry points called from Python: a start failure is a
/// DB-API error, not a panic.
pub fn runtime_or_raise(py: Python<'_>) -> PyResult<&'static Runtime> {
    try_runtime().map_err(|e| crate::errors::raise_connection_records(py, &[start_failure(&e)]))
}

/// Runs in a forked child: the forking thread is the child's only one, hence
/// its main thread whatever it was in the parent (`threading` makes the
/// forking thread the main one).
pub fn forget_main_thread() {
    MAIN_THREAD.with(|m| m.set(0));
}

/// Whether the calling thread is Python's main thread. Asked of Python once
/// per thread, with the GIL held and no driver lock held (it runs Python
/// code, which can run finalizers).
fn on_main_thread(py: Python<'_>) -> bool {
    if MAIN_THREAD.with(Cell::get) == 0 {
        let is_main = || -> PyResult<bool> {
            let threading = py.import("threading")?;
            Ok(threading
                .call_method0("current_thread")?
                .is(&threading.call_method0("main_thread")?))
        };
        MAIN_THREAD.with(|m| m.set(if is_main().unwrap_or(false) { 1 } else { 2 }));
    }
    MAIN_THREAD.with(Cell::get) == 1
}

/// Runs Python's pending signal handlers (only the main thread can) from a
/// watched wait. A handler that raises stops the wait; its exception is kept
/// for [`detach_interruptible`] to re-raise.
///
/// The handler runs inside the wait, so the statement's connection and state
/// locks are held by this very thread. A handler may call `cursor.cancel()`
/// (lock free), but any driver call that needs those locks would wait for
/// itself: `Lockable::lock_checked` turns that into an immediate error.
/// Attaching to the GIL here cannot deadlock against another thread: nobody
/// blocks on a statement lock while holding the GIL.
struct PySignals;

impl InterruptCheck for PySignals {
    fn interrupted(&self) -> bool {
        Python::attach(|py| match py.check_signals() {
            Ok(()) => false,
            Err(err) => {
                RAISED.with(|r| *r.borrow_mut() = Some(err));
                true
            }
        })
    }
}

/// Install the signal check into the core (module init).
pub fn install_interrupt_check() {
    set_interrupt_check(Box::new(PySignals));
}

/// [`Python::detach`] for running a statement: while `f` waits for the server
/// on the main thread, Python signals are looked at every `SIGNAL_SLICE`.
/// Ctrl-C cancels the statement on the server and raises its
/// `KeyboardInterrupt` once `f` has returned, leaving the connection usable.
pub fn detach_interruptible<R: Send>(
    py: Python<'_>,
    stmt: &StmtShared,
    f: impl FnOnce() -> R + Send,
) -> PyResult<R> {
    let out = if on_main_thread(py) {
        py.detach(|| watching(stmt, f))
    } else {
        py.detach(f)
    };
    match RAISED.with(|r| r.borrow_mut().take()) {
        Some(err) => Err(err),
        None => Ok(out),
    }
}

/// [`detach_interruptible`] for core calls that run statements of their own
/// (the parallel read's probes, the UTF-8 describe): every execution `f`
/// starts on this thread is watched.
pub fn detach_watched<R: Send>(py: Python<'_>, f: impl FnOnce() -> R + Send) -> PyResult<R> {
    let out = if on_main_thread(py) {
        py.detach(|| watch_executions(f))
    } else {
        py.detach(f)
    };
    match RAISED.with(|r| r.borrow_mut().take()) {
        Some(err) => Err(err),
        None => Ok(out),
    }
}
