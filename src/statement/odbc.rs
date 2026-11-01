//! The `DDBCSQL*` functions the Python layer calls. Each one unwraps the
//! handle, converts Python arguments, and hands the work to `exec` /
//! `cursor` with the GIL released.

use crate::runtime::Lockable;
use std::sync::Arc;

use mssql_tds::datatypes::sqltypes::SqlType;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use super::cursor::{FETCHALL_BATCH_ROWS, FETCHONE_READAHEAD, Fetched, FetchPlan, StmtState, fetch, fetch_buffered, next_result};
use super::exec::{Command, execute_batch, execute_command};
use super::{StmtShared, close_cursor, stmt_of};
use crate::consts::*;
use crate::decode::{describe, python as pyconv, text};
use crate::diag::{self, DiagRecord};
use crate::params::py_types::{
    ParameterHint, SQL_C_CHAR, narrow_codec, py_to_sql_type, py_to_sql_type_with_hint,
};
use crate::types::ParamInfo;

/// Malformed UTF-16 policy for a batch fetch. Upstream binds bounded columns
/// in bulk, which is lossy; a LOB column forces row-wise, strict decoding.
fn bulk_utf16_errors(has_lob: bool) -> text::Utf16Errors {
    if has_lob { text::Utf16Errors::Strict } else { text::Utf16Errors::BulkBounded }
}

pub fn append_messages(py: Python<'_>, messages: Option<&Bound<'_, PyAny>>, info: &[DiagRecord]) -> PyResult<()> {
    let Some(messages) = messages else { return Ok(()) };
    if messages.is_none() || info.is_empty() {
        return Ok(());
    }
    let list = messages.cast::<PyList>()?;
    for record in info {
        list.append(PyTuple::new(py, [diag::state_label(record), record.message.clone()])?)?;
    }
    Ok(())
}

/// Read or update the statement state. The lock is only ever held briefly, so
/// take it while keeping the GIL; releasing and re-acquiring the GIL for a
/// getter costs more than the getter and, with many threads, a thread switch.
/// Falls back to a GIL-free wait when an execution holds the state.
fn with_state<R: Send>(py: Python<'_>, stmt: &StmtShared, f: impl FnOnce(&mut StmtState) -> R + Send) -> R {
    if let Some(mut state) = stmt.state.try_lock() {
        return f(&mut state);
    }
    py.detach(|| f(&mut stmt.state.lock_checked()))
}

/// [`fetch`] that avoids releasing the GIL when no wire read is needed.
fn fetch_with_gil(py: Python<'_>, stmt: &StmtShared, n: usize, plan: FetchPlan) -> PyResult<Fetched> {
    match fetch_buffered(stmt, n, plan) {
        Some(fetched) => Ok(fetched),
        None => crate::runtime::detach_interruptible(py, stmt, || fetch(stmt, n, plan)),
    }
}

/// Replace the statement's diagnostics with one driver record.
fn reject(py: Python<'_>, stmt: &Arc<StmtShared>, record: DiagRecord) -> i16 {
    py.detach(|| {
        let mut state = stmt.state.lock_checked();
        state.diag.clear();
        state.diag.push(record);
    });
    SQL_ERROR
}

#[pyfunction]
#[pyo3(name = "DDBCSQLExecDirect")]
pub fn exec_direct(py: Python<'_>, statement_handle: &Bound<'_, PyAny>, query: String) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    crate::logging::debug(py, "SQLExecDirect: executing batch");
    let query = crate::params::placeholders::translate_call_escape(&query).unwrap_or(query);
    crate::runtime::detach_interruptible(py, &stmt, || execute_command(&stmt, Command::Batch(query)))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLExecute", signature = (statementHandle, query, params, inputSizes, isStmtPrepared, usePrepare, encodingSettings))]
#[allow(non_snake_case, unused_variables)]
pub fn execute(
    py: Python<'_>,
    statementHandle: &Bound<'_, PyAny>,
    query: String,
    params: &Bound<'_, PyAny>,
    inputSizes: &Bound<'_, PyAny>,
    isStmtPrepared: &Bound<'_, PyAny>,
    usePrepare: bool,
    encodingSettings: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    let stmt = stmt_of(statementHandle)?;
    let codec = narrow_codec(encodingSettings)?;
    let rpc_params = crate::params::build_rpc_params(params, Some(inputSizes), codec.as_deref())?;
    let (sql, markers) = crate::params::placeholders::rewrite_qmarks(&query);
    if markers != rpc_params.types.len() {
        let message = format!(
            "COUNT field incorrect or syntax error: the SQL contains {markers} parameter markers, but {} parameters were supplied",
            rpc_params.types.len()
        );
        return Ok(reject(py, &stmt, DiagRecord::driver("07002", message)));
    }
    if usePrepare
        && let Ok(list) = isStmtPrepared.cast::<PyList>()
        && !list.is_empty()
    {
        list.set_item(0, true)?;
    }
    crate::runtime::detach_interruptible(py, &stmt, || execute_command(&stmt, Command::Parameterized(sql, rpc_params)))
}

/// Type hints from executemany's `paramInfos`. DECIMAL/NUMERIC columns are
/// hinted: the Python layer sends their values as strings and relies on the
/// declared precision and scale to give them their numeric type. A column
/// holding NULLs stays unhinted: the batch is declared from its first row,
/// and a NULL decimal carries no precision or scale to declare it with.
/// With a codec, narrow text columns bound as `SQL_C_CHAR` are hinted as well,
/// so their values are encoded with it.
fn param_hints(
    param_infos: &Bound<'_, PyAny>,
    columns: &[Bound<'_, PyAny>],
    codec: Option<&str>,
) -> PyResult<Vec<Option<ParameterHint>>> {
    let mut hints = vec![None; columns.len()];
    let Ok(infos) = param_infos.cast::<PyList>() else { return Ok(hints) };
    for ((slot, info), column) in hints.iter_mut().zip(infos.iter()).zip(columns) {
        let Ok(info) = info.cast::<ParamInfo>() else { continue };
        let info = info.borrow();
        if codec.is_some()
            && i32::from(info.paramCType) == SQL_C_CHAR
            && matches!(info.paramSQLType, SQL_CHAR | SQL_VARCHAR | SQL_LONGVARCHAR)
        {
            let size = u32::try_from(info.columnSize).unwrap_or(u32::MAX);
            *slot = ParameterHint::new(i32::from(info.paramSQLType), size, 0)
                .ok()
                .map(|hint| hint.with_c_type(SQL_C_CHAR));
            continue;
        }
        if info.paramSQLType != SQL_DECIMAL && info.paramSQLType != SQL_NUMERIC {
            continue;
        }
        let size = u32::try_from(info.columnSize).unwrap_or(u32::MAX);
        let scale = u8::try_from(info.decimalDigits.max(0)).unwrap_or(u8::MAX);
        let has_null = column.try_iter()?.any(|v| v.is_ok_and(|v| v.is_none()));
        if !has_null {
            *slot = ParameterHint::new(i32::from(info.paramSQLType), size, scale).ok();
        }
    }
    Ok(hints)
}

/// A hinted value falls back to plain inference when the hint cannot hold it.
fn convert_cell(
    value: &Bound<'_, PyAny>,
    hint: Option<ParameterHint>,
    codec: Option<&str>,
) -> PyResult<SqlType> {
    let converted = match hint {
        Some(hint) => {
            py_to_sql_type_with_hint(value, hint, codec).or_else(|_| py_to_sql_type(value))
        }
        None => py_to_sql_type(value),
    };
    converted.map_err(|e| crate::params::conversion_error(&e))
}

#[pyfunction]
#[pyo3(name = "SQLExecuteMany", signature = (statementHandle, query, columnwise_params, paramInfos, paramSetSize, encodingSettings))]
#[allow(non_snake_case, unused_variables)]
pub fn execute_many(
    py: Python<'_>,
    statementHandle: &Bound<'_, PyAny>,
    query: String,
    columnwise_params: &Bound<'_, PyAny>,
    paramInfos: &Bound<'_, PyAny>,
    paramSetSize: usize,
    encodingSettings: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    let stmt = stmt_of(statementHandle)?;
    let columns: Vec<Bound<'_, PyAny>> = columnwise_params.cast::<PyList>()?.iter().collect();
    let (sql, markers) = crate::params::placeholders::rewrite_qmarks(&query);
    if markers != columns.len() {
        let message = format!(
            "COUNT field incorrect: the SQL contains {markers} parameter markers, but {} were supplied",
            columns.len()
        );
        return Ok(reject(py, &stmt, DiagRecord::driver("07002", message)));
    }
    let codec = narrow_codec(encodingSettings)?;
    let hints = param_hints(paramInfos, &columns, codec.as_deref())?;
    crate::params::check_text_sizes(paramInfos, &columns)?;
    let mut rows: Vec<Vec<SqlType>> = Vec::with_capacity(paramSetSize);
    let mut nulls: Vec<Vec<bool>> = Vec::with_capacity(paramSetSize);
    for row in 0..paramSetSize {
        let mut types = Vec::with_capacity(columns.len());
        let mut null_row = Vec::with_capacity(columns.len());
        for (column, hint) in columns.iter().zip(&hints) {
            let value = column.get_item(row)?;
            null_row.push(value.is_none());
            types.push(convert_cell(&value, *hint, codec.as_deref())?);
        }
        rows.push(types);
        nulls.push(null_row);
    }
    let all_null = crate::params::batch::normalize(&mut rows, &nulls)
        .map_err(|e| crate::params::conversion_error(&e))?;
    crate::runtime::detach_interruptible(py, &stmt, || execute_batch(&stmt, sql, rows, all_null))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLRowCount")]
pub fn row_count(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i64> {
    let stmt = stmt_of(statement_handle)?;
    Ok(with_state(py, &stmt, |state| state.rowcount()))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLNumResultCols", signature = (statementHandle, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn num_result_cols(
    py: Python<'_>,
    statementHandle: &Bound<'_, PyAny>,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i64> {
    let stmt = stmt_of(statementHandle)?;
    Ok(with_state(py, &stmt, |state| state.ncols() as i64))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLDescribeCol", signature = (StatementHandle, ColumnMetadata, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn describe_col(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    ColumnMetadata: &Bound<'_, PyList>,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    // The per-column dicts are shared with the previous call while the
    // columns stay the same, so the Python layer recognises an unchanged
    // layout by identity. They are the layer's own and never modified.
    // Python objects are built with the statement lock released: allocating
    // them can run a GC and finalizers, which must not wait on this lock.
    let quick = stmt.state.try_lock().map(|mut state| (state.columns.clone(), state.describe_cache.take()));
    if let Some((columns, mut cache)) = quick {
        let Some(columns) = columns else {
            return Ok(SQL_SUCCESS);
        };
        let shared = describe::shared_columns(py, &columns, &mut cache);
        if let Some(mut state) = stmt.state.try_lock() {
            state.describe_cache = cache;
        }
        for dict in shared?.iter() {
            ColumnMetadata.append(dict)?;
        }
        return Ok(SQL_SUCCESS);
    }
    let columns = with_state(py, &stmt, |state| state.columns());
    let Some(columns) = columns else { return Ok(SQL_SUCCESS) };
    describe::append_columns(py, &columns, ColumnMetadata)?;
    Ok(SQL_SUCCESS)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchOne", signature = (StatementHandle, row, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_one(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    row: &Bound<'_, PyList>,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    let fetched = fetch_with_gil(py, &stmt, 1, FetchPlan::Exact { readahead: FETCHONE_READAHEAD })?;
    append_messages(py, messages, &fetched.info)?;
    if fetched.rows.rows > 0 {
        pyconv::extend_with_row(py, &fetched.rows, &fetched.rows.cells[..fetched.ncols], row)?;
    }
    Ok(fetched.ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchMany", signature = (StatementHandle, rows, fetchSize, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_many(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    rows: &Bound<'_, PyList>,
    fetchSize: usize,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    let fetched = fetch_with_gil(py, &stmt, fetchSize, FetchPlan::Exact { readahead: fetchSize })?;
    append_messages(py, messages, &fetched.info)?;
    pyconv::append_rows(py, &fetched.rows, fetched.ncols, rows, bulk_utf16_errors(fetched.has_lob))?;
    // An empty fetchmany at end-of-data is a success with no rows upstream.
    Ok(if fetched.ret == SQL_NO_DATA { SQL_SUCCESS } else { fetched.ret })
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchAll", signature = (StatementHandle, rows, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_all(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    rows: &Bound<'_, PyList>,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> PyResult<i16> {
    let stmt = stmt_of(StatementHandle)?;
    crate::logging::debug(py, "FetchAll: fetching remaining rows");
    let mut ret = SQL_SUCCESS;
    let _fetching = super::FetchingAll::enter(&stmt);
    loop {
        let fetched = crate::runtime::detach_interruptible(py, &stmt, || fetch(&stmt, FETCHALL_BATCH_ROWS, FetchPlan::Batch))?;
        append_messages(py, messages, &fetched.info)?;
        pyconv::append_rows(py, &fetched.rows, fetched.ncols, rows, bulk_utf16_errors(fetched.has_lob))?;
        // Batches that finish within one signal slice never trip the wait's own
        // check; Ctrl-C between them discards the rest of the result.
        if let Err(interrupt) = py.check_signals() {
            close_cursor(py, &stmt, false);
            return Err(interrupt);
        }
        match fetched.ret {
            SQL_NO_DATA => break,
            SQL_SUCCESS_WITH_INFO => ret = SQL_SUCCESS_WITH_INFO,
            SQL_SUCCESS => {}
            other => return Ok(other),
        }
    }
    Ok(ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchScroll")]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_scroll(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    FetchOrientation: i16,
    FetchOffset: i64,
    row_data: &Bound<'_, PyList>,
) -> PyResult<i16> {
    const SQL_FETCH_NEXT: i16 = 1;
    let stmt = stmt_of(StatementHandle)?;
    if FetchOrientation != SQL_FETCH_NEXT {
        return Ok(reject(py, &stmt, DiagRecord::driver("HY106", "Fetch type out of range")));
    }
    let fetched = fetch_with_gil(py, &stmt, 1, FetchPlan::Exact { readahead: FETCHONE_READAHEAD })?;
    if fetched.rows.rows > 0 && row_data.is_empty() {
        pyconv::extend_with_row(py, &fetched.rows, &fetched.rows.cells[..fetched.ncols], row_data)?;
    }
    Ok(fetched.ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLMoreResults")]
pub fn more_results(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    crate::runtime::detach_interruptible(py, &stmt, || next_result(&stmt))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLResetStmt")]
pub fn reset_stmt(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    // Common case after a fully fetched result: no wire access, no GIL
    // release. `try_lock` because an execution on another thread may hold
    // the state while we hold the GIL.
    if let Some(mut state) = stmt.state.try_lock() {
        if state.is_off_wire() {
            state.reset_results();
            state.diag.clear();
            return Ok(SQL_SUCCESS);
        }
    }
    if with_state(py, &stmt, |state| state.is_freed()) {
        return Ok(SQL_INVALID_HANDLE);
    }
    close_cursor(py, &stmt, false);
    with_state(py, &stmt, |state| state.diag.clear());
    Ok(SQL_SUCCESS)
}

/// `SQLFreeHandle`. The first call (or the first after `SqlHandle.free()`
/// found the handle still live) succeeds; later calls report an invalid
/// handle, like ODBC.
#[pyfunction]
#[pyo3(name = "DDBCSQLFreeHandle")]
pub fn free_handle(py: Python<'_>, handle_type: i16, handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    if handle_type != SQL_HANDLE_STMT {
        return Ok(SQL_INVALID_HANDLE);
    }
    let Ok(stmt) = stmt_of(handle) else { return Ok(SQL_INVALID_HANDLE) };
    let was_freed = close_cursor(py, &stmt, true);
    Ok(if was_freed { SQL_INVALID_HANDLE } else { SQL_SUCCESS })
}

#[pyfunction]
#[pyo3(name = "DDBCSQLSetStmtAttr")]
pub fn set_stmt_attr(stmt: &Bound<'_, PyAny>, attr: i32, value: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(stmt)?;
    if attr == SQL_ATTR_QUERY_TIMEOUT {
        let timeout = value.extract::<i64>().unwrap_or(0).clamp(0, u32::MAX as i64) as u32;
        stmt.set_query_timeout(timeout);
    }
    Ok(SQL_SUCCESS)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLGetAllDiagRecords")]
pub fn get_all_diag_records<'py>(py: Python<'py>, handle: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    if let Ok(stmt) = stmt_of(handle) {
        let records = with_state(py, &stmt, |state| state.diag.clone());
        for record in records {
            out.append(PyTuple::new(py, [diag::state_label(&record), record.message])?)?;
        }
    }
    Ok(out)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLCheckError")]
pub fn check_error(
    py: Python<'_>,
    handle_type: i16,
    handle: &Bound<'_, PyAny>,
    retcode: i16,
) -> PyResult<crate::types::ErrorInfo> {
    let _ = handle_type;
    let mut info = crate::types::ErrorInfo::default();
    if retcode == SQL_INVALID_HANDLE || handle.is_none() {
        info.ddbcErrorMsg = "Invalid handle!".to_string();
        return Ok(info);
    }
    if retcode >= 0 {
        return Ok(info);
    }
    let Ok(stmt) = stmt_of(handle) else {
        info.ddbcErrorMsg = "Invalid handle!".to_string();
        return Ok(info);
    };
    if let Some(record) = with_state(py, &stmt, |state| state.diag.first().cloned()) {
        info.sqlState = record.sqlstate;
        info.ddbcErrorMsg = record.message;
        info.nativeError = record.native_error;
    }
    Ok(info)
}
