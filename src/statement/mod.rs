//! Statement handles (`SqlHandle`) for Python. The statement machinery
//! (wire ownership, cursors, execution) is `arrowsql_core::statement`; this
//! module adds the pyclass and the `DDBCSQL*` functions:
//!
//! * `odbc`: the `DDBCSQL*` Python functions, thin wrappers over the core.
//! * `batch_rpc`: executemany from Python rows.
//!
//! Concurrency model (see docs/ARCHITECTURE.md): lock order is always
//! `ConnShared::slot` -> `StmtShared::state`, and both are only taken with the
//! GIL released (`py.detach`). Python objects are built from a detached
//! `RowBuffer` after the locks are dropped.

pub(crate) mod batch_rpc;
mod odbc;

use crate::runtime::Lockable;
use std::sync::Arc;

use mssql_tds::message::parameters::rpc_parameters::RpcParameter;
use mssql_tds::query::metadata::ColumnMetadata;
use pyo3::prelude::*;

use crate::consts::*;
use crate::diag::DiagRecord;
pub use arrowsql_core::statement::*;

pub use odbc::{
    check_error, raise_for, describe_col, exec_direct, execute, execute_many, fetch_all, fetch_many,
    fetch_one, fetch_scroll, free_handle, get_all_diag_records, more_results, num_result_cols,
    reset_stmt, row_count, set_stmt_attr,
};
pub use odbc::append_messages;
pub use batch_rpc::execute_many_rows;

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
    py.detach(|| close(stmt, free))
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
            let mut state = stmt.state.lock_checked();
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
        let mut state = stmt.state.lock_checked();
        state.map_columns(f);
    });
}
