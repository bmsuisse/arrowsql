//! Small value classes the Python layer constructs or reads:
//! `ParamInfo`, `NumericData`, `ErrorInfo` (pybind-compatible attributes).

#![allow(non_snake_case)]

use pyo3::prelude::*;

#[pyclass(module = "ddbc_bindings", get_all, set_all)]
#[derive(Default)]
pub struct ParamInfo {
    pub inputOutputType: i16,
    pub paramCType: i16,
    pub paramSQLType: i16,
    pub columnSize: u64,
    pub decimalDigits: i16,
    pub strLenOrInd: i64,
    pub isDAE: bool,
    dataPtr_: Option<Py<PyAny>>,
}

#[pymethods]
impl ParamInfo {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    #[getter]
    fn get_dataPtr(&self, py: Python<'_>) -> Py<PyAny> {
        self.dataPtr_.as_ref().map_or_else(|| py.None(), |o| o.clone_ref(py))
    }

    #[setter]
    fn set_dataPtr(&mut self, value: Py<PyAny>) {
        self.dataPtr_ = Some(value);
    }
}

#[pyclass(module = "ddbc_bindings", get_all, set_all)]
#[derive(Default)]
pub struct NumericData {
    pub precision: u8,
    pub scale: i8,
    pub sign: u8,
    pub val: Vec<u8>,
}

#[pymethods]
impl NumericData {
    #[new]
    #[pyo3(signature = (precision=None, scale=None, sign=None, val=None))]
    fn new(precision: Option<u8>, scale: Option<i8>, sign: Option<u8>, val: Option<Vec<u8>>) -> Self {
        Self {
            precision: precision.unwrap_or(0),
            scale: scale.unwrap_or(0),
            sign: sign.unwrap_or(0),
            val: val.unwrap_or_default(),
        }
    }
}

/// First diagnostic record of a handle. `nativeError` is the SQL Server
/// error number (0 for driver-generated errors).
#[pyclass(module = "ddbc_bindings", get_all, set_all, skip_from_py_object)]
#[derive(Default, Clone)]
pub struct ErrorInfo {
    pub sqlState: String,
    pub ddbcErrorMsg: String,
    pub nativeError: i32,
}
