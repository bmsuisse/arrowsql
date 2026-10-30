//! `mssql_python.ddbc_bindings`, re-implemented in Rust on mssql-tds.
//!
//! Drop-in for the C++/pybind11 ODBC binding: same symbol names, argument
//! names, defaults and return codes. See docs/ARCHITECTURE.md for the design
//! and docs/CONTRACT.md for the symbol table and ownership.

use std::sync::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

mod catalog;
mod connection;
mod connstr;
mod consts;
mod decode;
mod diag;
mod getinfo;
mod logging;
mod params;
mod pooling;
mod rowfactory;
mod runtime;
mod statement;
mod types;
mod arrow;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

static DECIMAL_SEPARATOR: Mutex<String> = Mutex::new(String::new());

#[pyfunction]
#[pyo3(name = "ThrowStdException")]
fn throw_std_exception(message: String) -> PyResult<()> {
    Err(PyRuntimeError::new_err(message))
}

/// There is no ODBC driver to locate; kept for API compatibility.
#[pyfunction]
#[pyo3(name = "GetDriverPathCpp", signature = (*_args))]
fn get_driver_path(_args: &Bound<'_, pyo3::types::PyTuple>) -> String {
    String::new()
}

#[pyfunction]
#[pyo3(name = "_get_odbc_driver_path", signature = (*_args))]
fn get_odbc_driver_path(_args: &Bound<'_, pyo3::types::PyTuple>) -> String {
    String::new()
}

/// Provider selection is meaningless without ODBC; accepted and ignored.
#[pyfunction]
#[pyo3(name = "_set_odbc_provider", signature = (*_args))]
fn set_odbc_provider(_args: &Bound<'_, pyo3::types::PyTuple>) {}

#[pyfunction]
#[pyo3(name = "DDBCSetDecimalSeparator")]
fn set_decimal_separator(separator: String) {
    // TODO(decode/numeric): upstream applies this when converting decimals
    // fetched as text; stored for parity, unused by the native decoder.
    if let Ok(mut s) = DECIMAL_SEPARATOR.lock() {
        *s = separator;
    }
}

/// Low-level SQLFetch/SQLGetData pair: the Python layer only uses the
/// composite fetch functions, so these report IM001.
#[pyfunction]
#[pyo3(name = "DDBCSQLFetch")]
fn sql_fetch(handle: &Bound<'_, PyAny>) -> i16 {
    statement::not_implemented(handle, "DDBCSQLFetch")
}

#[pyfunction]
#[pyo3(name = "DDBCSQLGetData", signature = (StatementHandle, colCount, row, charEncoding="utf-16le", wcharEncoding="utf-16le", charCtype=consts::SQL_C_WCHAR, messages=None))]
#[allow(non_snake_case, unused_variables)]
fn sql_get_data(
    StatementHandle: &Bound<'_, PyAny>,
    colCount: i64,
    row: &Bound<'_, PyAny>,
    charEncoding: &str,
    wcharEncoding: &str,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> i16 {
    statement::not_implemented(StatementHandle, "DDBCSQLGetData")
}

#[pymodule]
fn ddbc_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Resolve temporal classes before any fetch, so substitutions made before import apply.
    decode::temporal::init(m.py())?;
    m.add("__doc__", "mssql-tds (Rust) bindings for mssql-python")?;
    m.add("__version__", "1.0.0")?;
    m.add("__architecture__", std::env::consts::ARCH)?;
    m.add("ARCHITECTURE", std::env::consts::ARCH)?;
    m.add("SQL_NO_TOTAL", consts::SQL_NO_TOTAL)?;
    m.add("__rust_port__", true)?;
    // Upstream's loader exposed the loaded extension as `ddbc_bindings.module`.
    m.add("module", m)?;

    m.add_class::<types::ParamInfo>()?;
    m.add_class::<types::NumericData>()?;
    m.add_class::<types::ErrorInfo>()?;
    m.add_class::<statement::SqlHandle>()?;
    m.add_class::<connection::Connection>()?;

    m.add_function(wrap_pyfunction!(throw_std_exception, m)?)?;
    m.add_function(wrap_pyfunction!(get_driver_path, m)?)?;
    m.add_function(wrap_pyfunction!(get_odbc_driver_path, m)?)?;
    m.add_function(wrap_pyfunction!(set_odbc_provider, m)?)?;
    m.add_function(wrap_pyfunction!(set_decimal_separator, m)?)?;
    m.add_function(wrap_pyfunction!(sql_fetch, m)?)?;
    m.add_function(wrap_pyfunction!(sql_get_data, m)?)?;

    m.add_function(wrap_pyfunction!(pooling::enable_pooling, m)?)?;
    m.add_function(wrap_pyfunction!(pooling::close_pooling, m)?)?;
    m.add_function(wrap_pyfunction!(pooling::disable_pooling, m)?)?;
    m.add_function(wrap_pyfunction!(logging::update_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(rowfactory::construct_rows, m)?)?;

    m.add_function(wrap_pyfunction!(statement::exec_direct, m)?)?;
    m.add_function(wrap_pyfunction!(statement::execute, m)?)?;
    m.add_function(wrap_pyfunction!(statement::execute_many, m)?)?;
    m.add_function(wrap_pyfunction!(statement::row_count, m)?)?;
    m.add_function(wrap_pyfunction!(statement::num_result_cols, m)?)?;
    m.add_function(wrap_pyfunction!(statement::describe_col, m)?)?;
    m.add_function(wrap_pyfunction!(statement::fetch_one, m)?)?;
    m.add_function(wrap_pyfunction!(statement::fetch_many, m)?)?;
    m.add_function(wrap_pyfunction!(statement::fetch_all, m)?)?;
    m.add_function(wrap_pyfunction!(statement::fetch_scroll, m)?)?;
    m.add_function(wrap_pyfunction!(statement::more_results, m)?)?;
    m.add_function(wrap_pyfunction!(statement::reset_stmt, m)?)?;
    m.add_function(wrap_pyfunction!(statement::free_handle, m)?)?;
    m.add_function(wrap_pyfunction!(statement::set_stmt_attr, m)?)?;
    m.add_function(wrap_pyfunction!(statement::get_all_diag_records, m)?)?;
    m.add_function(wrap_pyfunction!(statement::check_error, m)?)?;
    m.add_function(wrap_pyfunction!(arrow::fetch_arrow_batch, m)?)?;

    m.add_function(wrap_pyfunction!(catalog::tables, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::columns, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::procedures, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::primary_keys, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::foreign_keys, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::special_columns, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::statistics, m)?)?;
    m.add_function(wrap_pyfunction!(catalog::get_type_info, m)?)?;
    Ok(())
}
