//! Statement handles (`SqlHandle`) on top of one shared `SyncClient`.
//!
//! * `wire`: which statement owns the connection's TDS stream and how it is
//!   handed over (MARS emulation).
//! * `cursor`: a statement's current and queued results; fetch and
//!   next-result.
//! * `exec`: running SQL (`execute`, `executemany`).
//! * `odbc`: the `DDBCSQL*` Python functions, thin wrappers over the above.
//!
//! Concurrency model (see docs/ARCHITECTURE.md): lock order is always
//! `ConnShared::slot` -> `StmtShared::state`, and both are only taken with the
//! GIL released (`py.detach`). Python objects are built from a detached
//! `RowBuffer` after the locks are dropped.

pub(crate) mod batch_rpc;
mod cancel;
mod cursor;
mod exec;
mod odbc;
mod wire;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use mssql_tds::message::parameters::rpc_parameters::RpcParameter;
use mssql_tds::query::metadata::ColumnMetadata;
use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::connection::ConnShared;
use crate::consts::*;
use crate::diag::DiagRecord;
pub(crate) use cancel::StmtCancel;
use cursor::StmtState;
use exec::{Command, execute_command};
pub(crate) use exec::with_exclusive_wire;

#[allow(unused_imports)] // the decode seam for new row sinks
pub use cursor::read_rows_into;
pub use cursor::{BatchSink, SinkFetch, diag_records, fetch_into, result_columns};
pub use odbc::{
    check_error, describe_col, exec_direct, execute, execute_many, fetch_all, fetch_many,
    fetch_one, fetch_scroll, free_handle, get_all_diag_records, more_results, num_result_cols,
    reset_stmt, row_count, set_stmt_attr,
};
pub use odbc::append_messages;
pub use batch_rpc::execute_many_rows;
pub use wire::{Slot, abandon_result, discard_all, free_for_connection};

static NEXT_STMT_ID: AtomicU64 = AtomicU64::new(1);

pub struct StmtShared {
    pub id: u64,
    pub conn: Arc<ConnShared>,
    pub(crate) state: Mutex<StmtState>,
    /// `SQL_ATTR_QUERY_TIMEOUT` in seconds; atomic so setting it never waits
    /// behind a running fetch.
    query_timeout: AtomicU32,
    /// Separate lock so `_cancel` from another thread never waits on a fetch.
    cancel: Mutex<Option<Arc<StmtCancel>>>,
    /// An execution is between publishing its cancel handle and finishing;
    /// `_cancel` must reach it even while it still waits for the wire.
    executing: AtomicBool,
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
        })
    }

    pub fn query_timeout(&self) -> u32 {
        self.query_timeout.load(Ordering::Relaxed)
    }

    pub(crate) fn set_query_timeout(&self, seconds: u32) {
        self.query_timeout.store(seconds, Ordering::Relaxed);
    }

    pub(crate) fn cancel_execution(&self) {
        // Like SQLCancel, only an operation in flight is cancelled: an
        // execution (flagged from before it waits for the wire) or a fetch
        // (which holds the state lock). Arming the handle of an idle
        // statement would instead fail a later, unrelated read on its batch.
        if !self.executing.load(Ordering::Acquire)
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

pub fn stmt_of(handle: &Bound<'_, PyAny>) -> PyResult<Arc<StmtShared>> {
    Ok(handle.cast::<SqlHandle>()?.get().inner.clone())
}

/// Run `f` with the GIL released, from code that only has a `Python` token
/// implicitly (callers that hold no `py` argument).
fn without_gil<R: Send>(f: impl FnOnce() -> R + Send) -> R {
    Python::attach(|py| py.detach(f))
}

#[pyclass(name = "SqlHandle", module = "ddbc_bindings", frozen)]
pub struct SqlHandle {
    pub inner: Arc<StmtShared>,
}

/// Close the statement's cursor; with `free` also retire the handle. Returns
/// whether the handle had already been freed.
fn close_cursor(py: Python<'_>, stmt: &Arc<StmtShared>, free: bool) -> bool {
    // Closing from another thread while an execution runs cancels it first;
    // otherwise the slot lock below would wait for the statement to finish.
    stmt.cancel_execution();
    py.detach(|| {
        let mut slot = stmt.conn.slot.lock();
        wire::discard_if_owner(&mut slot, stmt.id);
        let mut state = stmt.state.lock();
        let was_freed = state.is_freed();
        state.reset_results();
        if free {
            state.mark_freed();
        }
        was_freed
    })
}

#[pymethods]
impl SqlHandle {
    fn free(&self, py: Python<'_>) {
        close_cursor(py, &self.inner, true);
    }

    fn _close_cursor(&self, py: Python<'_>) {
        close_cursor(py, &self.inner, false);
    }

    fn _cancel(&self) {
        self.inner.cancel_execution();
    }
}

// ---------------------------------------------------------------------------
// Internal API for other modules (catalog, lib)
// ---------------------------------------------------------------------------

/// Record a "not implemented" diagnostic and return SQL_ERROR.
pub fn not_implemented(handle: &Bound<'_, PyAny>, function: &str) -> i16 {
    if let Ok(stmt) = stmt_of(handle) {
        without_gil(|| {
            let mut state = stmt.state.lock();
            state.diag.clear();
            state.diag.push(DiagRecord::not_implemented(function));
        });
    }
    SQL_ERROR
}

/// Run `sql` as a metadata query on this statement (catalog functions).
pub fn exec_catalog_query(py: Python<'_>, handle: &Bound<'_, PyAny>, sql: String) -> PyResult<i16> {
    let stmt = stmt_of(handle)?;
    crate::runtime::detach_interruptible(py, &stmt, || execute_command(&stmt, Command::Batch(sql)))
}

/// Run an RPC-parameterized metadata query on this statement.
pub fn exec_catalog_rpc(
    py: Python<'_>,
    handle: &Bound<'_, PyAny>,
    sql: String,
    params: Vec<RpcParameter>,
) -> PyResult<i16> {
    let stmt = stmt_of(handle)?;
    crate::runtime::detach_interruptible(py, &stmt, || execute_command(&stmt, Command::Typed(sql, params)))
}

/// Replace the current result's column metadata (catalog column renames).
pub fn map_columns(
    stmt: &StmtShared,
    f: impl FnOnce(&Arc<Vec<ColumnMetadata>>) -> Arc<Vec<ColumnMetadata>> + Send,
) {
    without_gil(|| {
        let mut state = stmt.state.lock();
        state.map_columns(f);
    });
}
