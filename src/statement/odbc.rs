//! The `DDBCSQL*` functions the Python layer calls. Each one unwraps the
//! handle, converts Python arguments, and hands the work to `exec` /
//! `cursor` with the GIL released.

use std::sync::Arc;

use mssql_tds::datatypes::sqltypes::SqlType;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use super::cursor::{FETCHALL_BATCH_ROWS, FETCHONE_READAHEAD, FetchPlan, fetch, next_result};
use super::exec::{Command, execute_batch, execute_command};
use super::{StmtShared, close_cursor, stmt_of};
use crate::consts::*;
use crate::decode::{describe, python as pyconv};
use crate::diag::{self, DiagRecord};
use crate::params::py_types::{ParameterHint, py_to_sql_type, py_to_sql_type_with_hint};
use crate::types::ParamInfo;

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

/// Replace the statement's diagnostics with one driver record.
fn reject(py: Python<'_>, stmt: &Arc<StmtShared>, record: DiagRecord) -> i16 {
    py.detach(|| {
        let mut state = stmt.state.lock();
        state.diag.clear();
        state.diag.push(record);
    });
    SQL_ERROR
}

#[pyfunction]
#[pyo3(name = "DDBCSQLExecDirect")]
pub fn exec_direct(py: Python<'_>, statement_handle: &Bound<'_, PyAny>, query: String) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    let query = crate::params::placeholders::translate_call_escape(&query).unwrap_or(query);
    Ok(py.detach(|| execute_command(&stmt, Command::Batch(query))))
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
    let rpc_params = crate::params::build_rpc_params(params, Some(inputSizes))?;
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
    Ok(py.detach(|| execute_command(&stmt, Command::Parameterized(sql, rpc_params))))
}

/// Type hints from executemany's `paramInfos`. Only DECIMAL/NUMERIC columns
/// are hinted: the Python layer sends their values as strings and relies on
/// the declared precision and scale to give them their numeric type. A column
/// holding NULLs stays unhinted: the batch is declared from its first row,
/// and a NULL decimal carries no precision or scale to declare it with.
fn decimal_hints(
    param_infos: &Bound<'_, PyAny>,
    columns: &[Bound<'_, PyAny>],
) -> PyResult<Vec<Option<ParameterHint>>> {
    let mut hints = vec![None; columns.len()];
    let Ok(infos) = param_infos.cast::<PyList>() else { return Ok(hints) };
    for ((slot, info), column) in hints.iter_mut().zip(infos.iter()).zip(columns) {
        let Ok(info) = info.cast::<ParamInfo>() else { continue };
        let info = info.borrow();
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
fn convert_cell(value: &Bound<'_, PyAny>, hint: Option<ParameterHint>) -> PyResult<SqlType> {
    let converted = match hint {
        Some(hint) => py_to_sql_type_with_hint(value, hint).or_else(|_| py_to_sql_type(value)),
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
    let hints = decimal_hints(paramInfos, &columns)?;
    let mut rows: Vec<Vec<SqlType>> = Vec::with_capacity(paramSetSize);
    let mut nulls: Vec<Vec<bool>> = Vec::with_capacity(paramSetSize);
    for row in 0..paramSetSize {
        let mut types = Vec::with_capacity(columns.len());
        let mut null_row = Vec::with_capacity(columns.len());
        for (column, hint) in columns.iter().zip(&hints) {
            let value = column.get_item(row)?;
            null_row.push(value.is_none());
            types.push(convert_cell(&value, *hint)?);
        }
        rows.push(types);
        nulls.push(null_row);
    }
    let all_null = crate::params::batch::normalize(&mut rows, &nulls);
    Ok(py.detach(|| execute_batch(&stmt, sql, rows, all_null)))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLRowCount")]
pub fn row_count(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i64> {
    let stmt = stmt_of(statement_handle)?;
    Ok(py.detach(|| stmt.state.lock().rowcount()))
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
    Ok(py.detach(|| stmt.state.lock().ncols() as i64))
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
    let columns = py.detach(|| stmt.state.lock().columns());
    let Some(columns) = columns else { return Ok(SQL_SUCCESS) };
    for meta in columns.iter() {
        let d = PyDict::new(py);
        d.set_item("ColumnName", &meta.column_name)?;
        d.set_item("DataType", describe::odbc_sql_type(meta))?;
        d.set_item("ColumnSize", describe::column_size(meta))?;
        d.set_item("DecimalDigits", describe::decimal_digits(meta))?;
        d.set_item("Nullable", describe::nullable(meta))?;
        ColumnMetadata.append(d)?;
    }
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
    let fetched = py.detach(|| fetch(&stmt, 1, FetchPlan::Exact { readahead: FETCHONE_READAHEAD }));
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
    let fetched = py.detach(|| fetch(&stmt, fetchSize, FetchPlan::Exact { readahead: fetchSize }));
    append_messages(py, messages, &fetched.info)?;
    pyconv::append_rows(py, &fetched.rows, fetched.ncols, rows)?;
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
    let mut ret = SQL_SUCCESS;
    loop {
        let fetched = py.detach(|| fetch(&stmt, FETCHALL_BATCH_ROWS, FetchPlan::Batch));
        append_messages(py, messages, &fetched.info)?;
        pyconv::append_rows(py, &fetched.rows, fetched.ncols, rows)?;
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
    let fetched = py.detach(|| fetch(&stmt, 1, FetchPlan::Exact { readahead: FETCHONE_READAHEAD }));
    if fetched.rows.rows > 0 && row_data.is_empty() {
        pyconv::extend_with_row(py, &fetched.rows, &fetched.rows.cells[..fetched.ncols], row_data)?;
    }
    Ok(fetched.ret)
}

#[pyfunction]
#[pyo3(name = "DDBCSQLMoreResults")]
pub fn more_results(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    Ok(py.detach(|| next_result(&stmt)))
}

#[pyfunction]
#[pyo3(name = "DDBCSQLResetStmt")]
pub fn reset_stmt(py: Python<'_>, statement_handle: &Bound<'_, PyAny>) -> PyResult<i16> {
    let stmt = stmt_of(statement_handle)?;
    if py.detach(|| stmt.state.lock().is_freed()) {
        return Ok(SQL_INVALID_HANDLE);
    }
    close_cursor(py, &stmt, false);
    py.detach(|| stmt.state.lock().diag.clear());
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
        let records = py.detach(|| stmt.state.lock().diag.clone());
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
    if let Some(record) = py.detach(|| stmt.state.lock().diag.first().cloned()) {
        info.sqlState = record.sqlstate;
        info.ddbcErrorMsg = record.message;
        info.nativeError = record.native_error;
    }
    Ok(info)
}
