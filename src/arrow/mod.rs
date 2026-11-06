//! Native Arrow fetch.
//!
//! * `plan`: SQL column -> Arrow type (identical to upstream's C++ binding).
//! * `sink`: `ArrowSink`, a `RowWriter` decoding TDS rows straight into Arrow
//!   buffers; plugged into `statement::fetch_into` like `RowBuffer` is into
//!   the row fetch. Batches are decoded with the GIL released.
//! * `export`: PyCapsule export (Arrow C Data / C Stream interfaces) and the
//!   zero-dependency `ArrowTable` / `ArrowStream` classes over
//!   `arrowsql_core::arrow::stream`.
//!
//! Entry points: `DDBCSQLFetchArrowBatch` (upstream's `cursor.arrow*()`
//! contract), `DDBCArrowFetchTable` (`cursor.fetchall_arrow()` /
//! `fetchmany_arrow()`), `DDBCArrowStream` (`cursor.arrow_stream()`).

pub mod export;

pub use arrowsql_core::arrow::{DEFAULT_BATCH_ROWS, plan, sink};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::consts::*;
use std::sync::Arc;

use crate::statement::{self, StmtCancel};
use export::{ArrowMergeStream, ArrowStream, ArrowTable};
use plan::Plan;
use sink::ArrowSink;

/// The statement lock is waited for with the GIL released: a thread running a
/// query on this statement holds it, and that thread may need the GIL (to look
/// at signals) before it can let go.
fn current_plan(py: Python<'_>, stmt: &statement::StmtShared) -> PyResult<(Plan, u64, Option<Arc<StmtCancel>>)> {
    py.detach(|| arrowsql_core::arrow::current_plan(stmt)).map_err(PyRuntimeError::new_err)
}

/// Upstream contract: fetch up to `arrowBatchSize` rows as one record batch,
/// appended to `capsules` as (`arrow_schema`, `arrow_array`) PyCapsules.
#[pyfunction]
#[pyo3(name = "DDBCSQLFetchArrowBatch", signature = (StatementHandle, capsules, arrowBatchSize, charCtype, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_arrow_batch(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    capsules: &Bound<'_, PyList>,
    arrowBatchSize: i64,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = statement::stmt_of(StatementHandle)?;
    let (plan, _, _) = current_plan(py, &stmt)?;
    let rows = arrowBatchSize.max(0) as usize;
    let (fetched, batch) = py.detach(|| {
        let mut sink = ArrowSink::new(&plan, rows);
        let fetched = statement::fetch_into(&stmt, &mut sink, rows, None);
        (fetched, sink.finish())
    });
    statement::append_messages(py, messages, &fetched.info)?;
    if fetched.ret == SQL_ERROR || fetched.ret == SQL_INVALID_HANDLE {
        return Ok(fetched.ret);
    }
    export::batch_capsules(py, batch.map_err(PyRuntimeError::new_err)?, capsules)?;
    Ok(if fetched.ret == SQL_SUCCESS_WITH_INFO { SQL_SUCCESS_WITH_INFO } else { SQL_SUCCESS })
}

/// Fetch up to `max_rows` (negative: all) remaining rows as an `ArrowTable`
/// of `batch_size`-row batches. Returns `(return code, table)`; on an error
/// the table holds the rows delivered before it.
#[pyfunction]
#[pyo3(name = "DDBCArrowFetchTable", signature = (StatementHandle, max_rows=-1, batch_size=DEFAULT_BATCH_ROWS, messages=None))]
#[allow(non_snake_case)]
pub fn fetch_arrow_table(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    max_rows: i64,
    batch_size: usize,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<(i16, ArrowTable)> {
    let stmt = statement::stmt_of(StatementHandle)?;
    let (plan, _, _) = current_plan(py, &stmt)?;
    let max_rows = if max_rows < 0 { usize::MAX } else { max_rows as usize };
    let fetched = py.detach(|| arrowsql_core::arrow::fetch_table(&stmt, &plan, max_rows, batch_size));
    statement::append_messages(py, messages, &fetched.info)?;
    let ret = fetched.ret.map_err(PyRuntimeError::new_err)?;
    Ok((ret, ArrowTable::new(plan.schema, fetched.batches)))
}

/// Stream the rest of the current result as Arrow batches decoded ahead on a
/// background thread (`prefetch` batches deep). `owner` (the Python cursor)
/// is kept alive for as long as the stream.
#[pyfunction]
#[pyo3(name = "DDBCArrowStream", signature = (StatementHandle, batch_size=DEFAULT_BATCH_ROWS, prefetch=2, owner=None))]
#[allow(non_snake_case)]
pub fn arrow_stream(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    batch_size: usize,
    prefetch: usize,
    owner: Option<Py<PyAny>>,
) -> PyResult<ArrowStream> {
    let stmt = statement::stmt_of(StatementHandle)?;
    let (plan, generation, execution) = current_plan(py, &stmt)?;
    Ok(ArrowStream::start(stmt, plan, generation, execution, batch_size.max(1), prefetch, owner))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrowTable>()?;
    m.add_class::<ArrowStream>()?;
    m.add_class::<ArrowMergeStream>()?;
    m.add_function(wrap_pyfunction!(fetch_arrow_batch, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_arrow_table, m)?)?;
    m.add_function(wrap_pyfunction!(arrow_stream, m)?)?;
    Ok(())
}
