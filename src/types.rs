//! Small value classes the Python layer constructs or reads:
//! `ParamInfo`, `NumericData`, `ErrorInfo` (pybind-compatible attributes).

#![allow(non_snake_case)]

use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

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
        self.dataPtr_
            .as_ref()
            .map_or_else(|| py.None(), |o| o.clone_ref(py))
    }

    #[setter]
    fn set_dataPtr(&mut self, value: Py<PyAny>) {
        self.dataPtr_ = Some(value);
    }
}

/// Width of SQL_NUMERIC_STRUCT's `val` buffer (SQL_MAX_NUMERIC_LEN).
const SQL_MAX_NUMERIC_LEN: usize = 16;

#[pyclass(module = "ddbc_bindings")]
pub struct NumericData {
    #[pyo3(get, set)]
    pub precision: u8,
    #[pyo3(get, set)]
    pub scale: i8,
    #[pyo3(get, set)]
    pub sign: u8,
    pub val: Vec<u8>,
}

#[pymethods]
impl NumericData {
    /// `val` is little-endian magnitude bytes, given as `bytes` or as `str`
    /// (UTF-8 encoded, as pybind11's `std::string` caster does). The constructor
    /// zero-pads it to SQL_MAX_NUMERIC_LEN bytes and rejects anything longer.
    #[new]
    #[pyo3(signature = (precision=None, scale=None, sign=None, val=None))]
    fn new(
        precision: Option<u8>,
        scale: Option<i8>,
        sign: Option<u8>,
        val: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let mut data = Self {
            precision: precision.unwrap_or(0),
            scale: scale.unwrap_or(0),
            sign: sign.unwrap_or(0),
            val: vec![0; SQL_MAX_NUMERIC_LEN],
        };
        if let Some(value) = val {
            let bytes = value_bytes(value)?;
            if bytes.len() > SQL_MAX_NUMERIC_LEN {
                return Err(PyRuntimeError::new_err(
                    "NumericData valueBytes size exceeds SQL_MAX_NUMERIC_LEN (16)",
                ));
            }
            data.val[..bytes.len()].copy_from_slice(&bytes);
        }
        Ok(data)
    }

    #[getter]
    fn get_val<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.val)
    }

    /// Stores the bytes as given, without padding, as pybind's
    /// `def_readwrite` did.
    #[setter]
    fn set_val(&mut self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.val = value_bytes(value)?;
        Ok(())
    }
}

/// Bytes of a `str` (UTF-8) or `bytes` value. Any other type is a TypeError,
/// as in pybind11.
fn value_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(text.to_str()?.as_bytes().to_vec());
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    Err(PyTypeError::new_err(
        "NumericData(): incompatible constructor arguments; val must be str or bytes",
    ))
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
