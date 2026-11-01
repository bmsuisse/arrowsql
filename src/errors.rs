//! DB-API exceptions built in Rust.
//!
//! The SQLSTATE -> class table, the 1222 / 2628 / 8114 class promotions and
//! the exception text mirror what `arrowsql.exceptions` used to do in Python;
//! the Python functions of the same names now delegate here. The classes are
//! imported once from `arrowsql.exceptions`.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;

use crate::diag::DiagRecord;
use crate::logging;

const LOCK_TIMEOUT_ERROR: i32 = 1222;
const VERBOSE_TRUNCATION_ERROR: i32 = 2628;
const CONVERSION_ERROR: i32 = 8114;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Warning,
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
}

struct Classes {
    by_kind: [Py<PyAny>; 6],
    lock_timeout: Py<PyAny>,
    string_truncation: Py<PyAny>,
    conversion: Py<PyAny>,
    database: Py<PyAny>,
    operational: Py<PyAny>,
}

static CLASSES: PyOnceLock<Classes> = PyOnceLock::new();

fn classes(py: Python<'_>) -> PyResult<&'static Classes> {
    CLASSES.get_or_try_init(py, || {
        let module = PyModule::import(py, "arrowsql.exceptions")?;
        let get = |name: &str| -> PyResult<Py<PyAny>> { Ok(module.getattr(name)?.unbind()) };
        Ok(Classes {
            by_kind: [
                get("Warning")?,
                get("DataError")?,
                get("IntegrityError")?,
                get("NotSupportedError")?,
                get("OperationalError")?,
                get("ProgrammingError")?,
            ],
            lock_timeout: get("LockTimeoutError")?,
            string_truncation: get("StringTruncationError")?,
            conversion: get("ConversionDataError")?,
            database: get("DatabaseError")?,
            operational: get("OperationalError")?,
        })
    })
}

// Generated from the former exceptions._SQLSTATE_TABLE: (SQLSTATE, class, driver_error).
const SQLSTATE_TABLE: &[(&str, Kind, &str)] = &[
    ("01000", Kind::Warning, "General warning"),
    ("01001", Kind::OperationalError, "Cursor operation conflict"),
    ("01002", Kind::OperationalError, "Disconnect error"),
    ("01003", Kind::DataError, "NULL value eliminated in set function"),
    ("01004", Kind::DataError, "String data, right-truncated"),
    ("01006", Kind::OperationalError, "Privilege not revoked"),
    ("01007", Kind::OperationalError, "Privilege not granted"),
    ("01S00", Kind::ProgrammingError, "Invalid connection string attribute"),
    ("01S01", Kind::DataError, "Error in row"),
    ("01S02", Kind::Warning, "Option value changed"),
    ("01S06", Kind::OperationalError, "Attempt to fetch before the result set returned the first rowset"),
    ("01S07", Kind::DataError, "Fractional truncation"),
    ("01S08", Kind::OperationalError, "Error saving File DSN"),
    ("01S09", Kind::ProgrammingError, "Invalid keyword"),
    ("07001", Kind::ProgrammingError, "Wrong number of parameters"),
    ("07002", Kind::ProgrammingError, "COUNT field incorrect"),
    ("07005", Kind::ProgrammingError, "Prepared statement not a cursor-specification"),
    ("07006", Kind::ProgrammingError, "Restricted data type attribute violation"),
    ("07009", Kind::ProgrammingError, "Invalid descriptor index"),
    ("07S01", Kind::ProgrammingError, "Invalid use of default parameter"),
    ("08001", Kind::OperationalError, "Client unable to establish connection"),
    ("08002", Kind::OperationalError, "Connection name in use"),
    ("08003", Kind::OperationalError, "Connection not open"),
    ("08004", Kind::OperationalError, "Server rejected the connection"),
    ("08007", Kind::OperationalError, "Connection failure during transaction"),
    ("08S01", Kind::OperationalError, "Communication link failure"),
    ("21S01", Kind::ProgrammingError, "Insert value list does not match column list"),
    ("21S02", Kind::ProgrammingError, "Degree of derived table does not match column list"),
    ("22001", Kind::DataError, "String data, right-truncated"),
    ("22002", Kind::DataError, "Indicator variable required but not supplied"),
    ("22003", Kind::DataError, "Numeric value out of range"),
    ("22007", Kind::DataError, "Invalid datetime format"),
    ("22008", Kind::DataError, "Datetime field overflow"),
    ("22012", Kind::DataError, "Division by zero"),
    ("22015", Kind::DataError, "Interval field overflow"),
    ("22018", Kind::DataError, "Invalid character value for cast specification"),
    ("22019", Kind::ProgrammingError, "Invalid escape character"),
    ("22025", Kind::ProgrammingError, "Invalid escape sequence"),
    ("22026", Kind::DataError, "String data, length mismatch"),
    ("23000", Kind::IntegrityError, "Integrity constraint violation"),
    ("24000", Kind::ProgrammingError, "Invalid cursor state"),
    ("25000", Kind::OperationalError, "Invalid transaction state"),
    ("25S01", Kind::OperationalError, "Transaction state"),
    ("25S02", Kind::OperationalError, "Transaction is still active"),
    ("25S03", Kind::OperationalError, "Transaction is rolled back"),
    ("28000", Kind::OperationalError, "Invalid authorization specification"),
    ("34000", Kind::ProgrammingError, "Invalid cursor name"),
    ("3C000", Kind::ProgrammingError, "Duplicate cursor name"),
    ("3D000", Kind::ProgrammingError, "Invalid catalog name"),
    ("3F000", Kind::ProgrammingError, "Invalid schema name"),
    ("40001", Kind::OperationalError, "Serialization failure"),
    ("40002", Kind::IntegrityError, "Integrity constraint violation"),
    ("40003", Kind::OperationalError, "Statement completion unknown"),
    ("42000", Kind::ProgrammingError, "Syntax error or access violation"),
    ("42S01", Kind::ProgrammingError, "Base table or view already exists"),
    ("42S02", Kind::ProgrammingError, "Base table or view not found"),
    ("42S11", Kind::ProgrammingError, "Index already exists"),
    ("42S12", Kind::ProgrammingError, "Index not found"),
    ("42S21", Kind::ProgrammingError, "Column already exists"),
    ("42S22", Kind::ProgrammingError, "Column not found"),
    ("44000", Kind::IntegrityError, "WITH CHECK OPTION violation"),
    ("HY000", Kind::OperationalError, "General error"),
    ("HY001", Kind::OperationalError, "Memory allocation error"),
    ("HY003", Kind::ProgrammingError, "Invalid application buffer type"),
    ("HY004", Kind::ProgrammingError, "Invalid SQL data type"),
    ("HY007", Kind::ProgrammingError, "Associated statement is not prepared"),
    ("HY008", Kind::OperationalError, "Operation canceled"),
    ("HY009", Kind::ProgrammingError, "Invalid use of null pointer"),
    ("HY010", Kind::ProgrammingError, "Function sequence error"),
    ("HY011", Kind::ProgrammingError, "Attribute cannot be set now"),
    ("HY012", Kind::ProgrammingError, "Invalid transaction operation code"),
    ("HY013", Kind::OperationalError, "Memory management error"),
    ("HY014", Kind::OperationalError, "Limit on the number of handles exceeded"),
    ("HY015", Kind::ProgrammingError, "No cursor name available"),
    ("HY016", Kind::ProgrammingError, "Cannot modify an implementation row descriptor"),
    ("HY017", Kind::ProgrammingError, "Invalid use of an automatically allocated descriptor handle"),
    ("HY018", Kind::OperationalError, "Server declined cancel request"),
    ("HY019", Kind::DataError, "Non-character and non-binary data sent in pieces"),
    ("HY020", Kind::DataError, "Attempt to concatenate a null value"),
    ("HY021", Kind::ProgrammingError, "Inconsistent descriptor information"),
    ("HY024", Kind::ProgrammingError, "Invalid attribute value"),
    ("HY090", Kind::ProgrammingError, "Invalid string or buffer length"),
    ("HY091", Kind::ProgrammingError, "Invalid descriptor field identifier"),
    ("HY092", Kind::ProgrammingError, "Invalid attribute/option identifier"),
    ("HY095", Kind::ProgrammingError, "Function type out of range"),
    ("HY096", Kind::ProgrammingError, "Invalid information type"),
    ("HY097", Kind::ProgrammingError, "Column type out of range"),
    ("HY098", Kind::ProgrammingError, "Scope type out of range"),
    ("HY099", Kind::ProgrammingError, "Nullable type out of range"),
    ("HY100", Kind::ProgrammingError, "Uniqueness option type out of range"),
    ("HY101", Kind::ProgrammingError, "Accuracy option type out of range"),
    ("HY103", Kind::ProgrammingError, "Invalid retrieval code"),
    ("HY104", Kind::ProgrammingError, "Invalid precision or scale value"),
    ("HY105", Kind::ProgrammingError, "Invalid parameter type"),
    ("HY106", Kind::ProgrammingError, "Fetch type out of range"),
    ("HY107", Kind::ProgrammingError, "Row value out of range"),
    ("HY109", Kind::ProgrammingError, "Invalid cursor position"),
    ("HY110", Kind::ProgrammingError, "Invalid driver completion"),
    ("HY111", Kind::ProgrammingError, "Invalid bookmark value"),
    ("HYC00", Kind::NotSupportedError, "Optional feature not implemented"),
    ("HYT00", Kind::OperationalError, "Timeout expired"),
    ("HYT01", Kind::OperationalError, "Connection timeout expired"),
    ("IM001", Kind::NotSupportedError, "Driver does not support this function"),
    ("IM002", Kind::OperationalError, "Data source name not found and no default driver specified"),
    ("IM003", Kind::OperationalError, "Specified driver could not be loaded"),
    ("IM004", Kind::OperationalError, "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"),
    ("IM005", Kind::OperationalError, "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"),
    ("IM006", Kind::OperationalError, "Driver's SQLSetConnectAttr failed"),
    ("IM007", Kind::OperationalError, "No data source or driver specified; dialog prohibited"),
    ("IM008", Kind::OperationalError, "Dialog failed"),
    ("IM009", Kind::OperationalError, "Unable to load translation DLL"),
    ("IM010", Kind::OperationalError, "Data source name too long"),
    ("IM011", Kind::OperationalError, "Driver name too long"),
    ("IM012", Kind::OperationalError, "DRIVER keyword syntax error"),
    ("IM013", Kind::OperationalError, "Trace file error"),
    ("IM014", Kind::OperationalError, "Invalid name of File DSN"),
    ("IM015", Kind::OperationalError, "Corrupt file data source"),
];

fn lookup(sqlstate: &str) -> Option<(Kind, &'static str)> {
    SQLSTATE_TABLE
        .binary_search_by_key(&sqlstate, |&(state, _, _)| state)
        .ok()
        .map(|i| (SQLSTATE_TABLE[i].1, SQLSTATE_TABLE[i].2))
}

fn instantiate<'py>(
    class: &Bound<'py, PyAny>,
    driver_error: &str,
    ddbc_error: &str,
    sqlstate: Option<&str>,
    native_error: Option<i32>,
) -> PyResult<Bound<'py, PyAny>> {
    let kwargs = PyDict::new(class.py());
    kwargs.set_item("driver_error", driver_error)?;
    kwargs.set_item("ddbc_error", ddbc_error)?;
    kwargs.set_item("sqlstate", sqlstate)?;
    kwargs.set_item("native_error", native_error)?;
    class.call((), Some(&kwargs))
}

/// The exception for `sqlstate`, or `None` when the SQLSTATE is unknown.
pub fn exception_for<'py>(
    py: Python<'py>,
    sqlstate: &str,
    ddbc_error: &str,
    native_error: Option<i32>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let Some((kind, driver_error)) = lookup(sqlstate) else { return Ok(None) };
    let classes = classes(py)?;
    let class = match (kind, native_error) {
        (Kind::ProgrammingError, Some(LOCK_TIMEOUT_ERROR)) => &classes.lock_timeout,
        (Kind::ProgrammingError, Some(CONVERSION_ERROR)) => &classes.conversion,
        (Kind::DataError, Some(VERBOSE_TRUNCATION_ERROR)) => &classes.string_truncation,
        _ => &classes.by_kind[kind as usize],
    };
    instantiate(class.bind(py), driver_error, ddbc_error, Some(sqlstate), native_error).map(Some)
}

/// `exception_for`, with a `DatabaseError` for an unknown SQLSTATE.
fn exception<'py>(
    py: Python<'py>,
    sqlstate: &str,
    ddbc_error: &str,
    native_error: Option<i32>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Some(exc) = exception_for(py, sqlstate, ddbc_error, native_error)? {
        logging::python_error(py, &format!("Raising exception: {}", exc.str()?));
        return Ok(exc);
    }
    unknown(py, sqlstate, ddbc_error, native_error)
}

fn unknown<'py>(
    py: Python<'py>,
    sqlstate: &str,
    ddbc_error: &str,
    native_error: Option<i32>,
) -> PyResult<Bound<'py, PyAny>> {
    logging::python_error(py, &format!("Unknown SQLSTATE {sqlstate}, raising DatabaseError"));
    instantiate(
        classes(py)?.database.bind(py),
        &format!("An error occurred with SQLSTATE code: {sqlstate}"),
        if ddbc_error.is_empty() { "Unknown DDBC error" } else { ddbc_error },
        (!sqlstate.is_empty()).then_some(sqlstate),
        native_error,
    )
}

/// 0 is a driver-generated diagnostic, not a server error.
fn native_of(native_error: i32) -> Option<i32> {
    (native_error != 0).then_some(native_error)
}

/// The error `check_error` raises: the mapped exception, logged.
pub fn raise_statement(py: Python<'_>, sqlstate: &str, ddbc_error: &str, native_error: Option<i32>) -> PyErr {
    match exception(py, sqlstate, ddbc_error, native_error) {
        Ok(exc) => PyErr::from_value(exc),
        Err(err) => err,
    }
}

/// The error a native connection call raises for a diagnostic record.
pub fn raise_connection(py: Python<'_>, record: &DiagRecord) -> PyErr {
    let build = || -> PyResult<PyErr> {
        if record.sqlstate.len() != 5 {
            logging::python_error(py, &format!("Connection error (malformed SQLSTATE): {}", record.message));
            let exc = instantiate(
                classes(py)?.operational.bind(py),
                "Connection operation failed",
                &record.message,
                None,
                None,
            )?;
            return Ok(PyErr::from_value(exc));
        }
        let native = native_of(record.native_error);
        let exc = match exception_for(py, &record.sqlstate, &record.message, native)? {
            Some(exc) => exc,
            None => unknown(py, &record.sqlstate, &record.message, native)?,
        };
        logging::python_error(py, &format!("Connection error (SQLSTATE {}): {}", record.sqlstate, record.message));
        Ok(PyErr::from_value(exc))
    };
    build().unwrap_or_else(|err| err)
}

/// The error for the first of `records` (HY000 when there are none).
pub fn raise_connection_records(py: Python<'_>, records: &[DiagRecord]) -> PyErr {
    match records.first() {
        Some(record) => raise_connection(py, record),
        None => raise_connection(py, &DiagRecord::new("HY000", 0, "Unknown error")),
    }
}

/// `sqlstate_to_exception(sqlstate, ddbc_error, native_error=None)`.
#[pyfunction]
#[pyo3(signature = (sqlstate, ddbc_error, native_error=None))]
pub fn sqlstate_to_exception<'py>(
    py: Python<'py>,
    sqlstate: &str,
    ddbc_error: &str,
    native_error: Option<i32>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    exception_for(py, sqlstate, ddbc_error, native_error)
}

/// `raise_exception(sqlstate, ddbc_error, native_error=None)`.
#[pyfunction]
#[pyo3(signature = (sqlstate, ddbc_error, native_error=None))]
pub fn raise_exception(py: Python<'_>, sqlstate: &str, ddbc_error: &str, native_error: Option<i32>) -> PyResult<()> {
    Err(raise_statement(py, sqlstate, ddbc_error, native_error))
}
