//! Statement handles on top of one shared `SyncClient`.
//!
//! * `wire`: which statement owns the connection's TDS stream and how it is
//!   handed over (MARS emulation).
//! * `cursor`: a statement's current and queued results; fetch and
//!   next-result.
//! * `exec`: running SQL (`execute`, `executemany`).
//! * `cancel`: cancellation and timeout of one execution.
//!
//! Lock order is always `ConnShared::slot` -> `StmtShared::state`. Callers
//! that hold an interpreter lock (Python's GIL) release it before calling in.

pub mod cancel;
pub mod cursor;
pub mod exec;
pub mod wire;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::connection::ConnShared;
use crate::runtime::Lockable;
pub use cancel::StmtCancel;
pub use cursor::{BatchSink, StmtState, diag_records, fetch_into, read_rows_into, result_columns};
pub use exec::{Command, execute_command, with_exclusive_wire};
pub use wire::{DRAIN_BUDGET_BYTES, Slot, abandon_result, discard_all, discard_if_owner, free_for_connection};

static NEXT_STMT_ID: AtomicU64 = AtomicU64::new(1);

pub struct StmtShared {
    pub id: u64,
    pub conn: Arc<ConnShared>,
    pub state: Mutex<StmtState>,
    /// `SQL_ATTR_QUERY_TIMEOUT` in seconds; atomic so setting it never waits
    /// behind a running fetch.
    pub query_timeout: AtomicU32,
    /// Separate lock so `_cancel` from another thread never waits on a fetch.
    pub cancel: Mutex<Option<Arc<StmtCancel>>>,
    /// An execution is between publishing its cancel handle and finishing;
    /// `_cancel` must reach it even while it still waits for the wire.
    pub executing: AtomicBool,
    /// A `fetchall` is running. Between its batches the statement lock is
    /// free while the rows are converted to Python objects; a `_cancel` in
    /// that gap must still arm the cancellation, or the next batch would
    /// never notice it.
    pub fetching_all: AtomicBool,
}

impl StmtShared {
    pub fn new(conn: Arc<ConnShared>) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
            conn,
            state: Mutex::new(StmtState::new()),
            query_timeout: AtomicU32::new(0),
            cancel: Mutex::new(None),
            executing: AtomicBool::new(false),
            fetching_all: AtomicBool::new(false),
        })
    }

    pub fn query_timeout(&self) -> u32 {
        self.query_timeout.load(Ordering::Relaxed)
    }

    pub fn set_query_timeout(&self, seconds: u32) {
        self.query_timeout.store(seconds, Ordering::Relaxed);
    }

    pub fn cancel_execution(&self) {
        // Like SQLCancel, only an operation in flight is cancelled: an
        // execution (flagged from before it waits for the wire) or a fetch
        // (which holds the state lock). Arming the handle of an idle
        // statement would instead fail a later, unrelated read on its batch.
        if !self.executing.load(Ordering::Acquire)
            && !self.fetching_all.load(Ordering::Acquire)
            && let Some(idle) = self.state.try_lock()
        {
            drop(idle);
            return;
        }
        if let Some(cancel) = self.cancel.lock().as_ref() {
            cancel.cancel();
        }
    }
}

/// Close the statement's cursor; with `free` also retire the statement.
/// Returns whether it had already been freed. Blocks on the wire when the
/// statement's results are still on it.
pub fn close(stmt: &StmtShared, free: bool) -> bool {
    // Closing from another thread while an execution runs cancels it first;
    // otherwise the slot lock below would wait for the statement to finish.
    stmt.cancel_execution();
    let mut slot = stmt.conn.slot.lock_checked();
    wire::discard_if_owner(&mut slot, stmt.id);
    let mut state = stmt.state.lock_checked();
    let was_freed = state.is_freed();
    state.reset_results();
    if free {
        state.mark_freed();
    }
    was_freed
}

/// Marks `stmt` as running a `fetchall` until dropped.
pub struct FetchingAll<'a>(&'a StmtShared);

impl<'a> FetchingAll<'a> {
    pub fn enter(stmt: &'a StmtShared) -> Self {
        stmt.fetching_all.store(true, Ordering::Release);
        Self(stmt)
    }
}

impl Drop for FetchingAll<'_> {
    fn drop(&mut self) {
        self.0.fetching_all.store(false, Ordering::Release);
    }
}
