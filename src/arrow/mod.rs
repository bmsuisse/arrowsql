//! Native Arrow fetch.
//!
//! * `plan`: SQL column -> Arrow type (identical to upstream's C++ binding).
//! * `sink`: `ArrowSink`, a `RowWriter` decoding TDS rows straight into Arrow
//!   buffers; plugged into `statement::fetch_into` like `RowBuffer` is into
//!   the row fetch. Batches are decoded with the GIL released.
//! * `export`: PyCapsule export (Arrow C Data / C Stream interfaces) and the
//!   zero-dependency `ArrowTable` / `ArrowStream` classes.
//!
//! Entry points: `DDBCSQLFetchArrowBatch` (upstream's `cursor.arrow*()`
//! contract), `DDBCArrowFetchTable` (`cursor.fetchall_arrow()` /
//! `fetchmany_arrow()`), `DDBCArrowStream` (`cursor.arrow_stream()`).

pub mod export;
pub mod plan;
pub mod sink;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::consts::*;
use crate::statement::{self, BatchSink};
use export::{ArrowStream, ArrowTable};
use plan::Plan;
use sink::ArrowSink;

/// Rows per batch when the caller doesn't care (native API default).
pub const DEFAULT_BATCH_ROWS: usize = 65_536;

fn current_plan(stmt: &statement::StmtShared) -> PyResult<(Plan, u64)> {
    let Some((columns, generation)) = statement::result_columns(stmt) else {
        return Err(PyRuntimeError::new_err("No active result set. Cannot fetch Arrow batch."));
    };
    let plan = Plan::new(&columns).map_err(PyRuntimeError::new_err)?;
    Ok((plan, generation))
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
    let (plan, _) = current_plan(&stmt)?;
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
    let (plan, _) = current_plan(&stmt)?;
    let batch_size = batch_size.max(1);
    let mut remaining = if max_rows < 0 { usize::MAX } else { max_rows as usize };
    let (ret, info, batches) = py.detach(|| {
        let mut batches = Vec::new();
        let mut info = Vec::new();
        let mut ret = SQL_SUCCESS;
        while remaining > 0 {
            let want = remaining.min(batch_size);
            let mut sink = ArrowSink::new(&plan, want);
            let fetched = statement::fetch_into(&stmt, &mut sink, want, None);
            info.extend(fetched.info);
            match fetched.ret {
                SQL_SUCCESS => {}
                SQL_SUCCESS_WITH_INFO => ret = SQL_SUCCESS_WITH_INFO,
                SQL_NO_DATA => break,
                other => {
                    ret = other;
                    break;
                }
            }
            let got = sink.rows();
            match sink.finish() {
                Ok(batch) => batches.push(batch),
                Err(err) => return (Err(err), info, batches),
            }
            remaining -= got;
            // A short batch is not the end: an error that cut it short is
            // reported by the next fetch, which is what ends the loop.
        }
        (Ok(ret), info, batches)
    });
    statement::append_messages(py, messages, &info)?;
    let ret = ret.map_err(PyRuntimeError::new_err)?;
    Ok((ret, ArrowTable::new(plan.schema, batches)))
}

/// Stream the rest of the current result as Arrow batches decoded ahead on a
/// background thread (`prefetch` batches deep). `owner` (the Python cursor)
/// is kept alive for as long as the stream.
#[pyfunction]
#[pyo3(name = "DDBCArrowStream", signature = (StatementHandle, batch_size=DEFAULT_BATCH_ROWS, prefetch=2, owner=None))]
#[allow(non_snake_case)]
pub fn arrow_stream(
    StatementHandle: &Bound<'_, PyAny>,
    batch_size: usize,
    prefetch: usize,
    owner: Option<Py<PyAny>>,
) -> PyResult<ArrowStream> {
    let stmt = statement::stmt_of(StatementHandle)?;
    let (plan, generation) = current_plan(&stmt)?;
    Ok(ArrowStream::start(stmt, plan, generation, batch_size.max(1), prefetch, owner))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrowTable>()?;
    m.add_class::<ArrowStream>()?;
    m.add_function(wrap_pyfunction!(fetch_arrow_batch, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_arrow_table, m)?)?;
    m.add_function(wrap_pyfunction!(arrow_stream, m)?)?;
    Ok(())
}
