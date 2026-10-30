//! bit / integers / float / real / decimal / numeric / money -> Python.

use mssql_tds::datatypes::decoder::{DECIMAL_STR_LEN, DecimalParts};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

fn check(py: Python<'_>, ptr: *mut ffi::PyObject) -> PyResult<*mut ffi::PyObject> {
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}

pub fn boolean(value: bool) -> *mut ffi::PyObject {
    unsafe { ffi::PyBool_FromLong(value.into()) }
}

pub fn int(py: Python<'_>, value: i64) -> PyResult<*mut ffi::PyObject> {
    check(py, unsafe { ffi::PyLong_FromLongLong(value) })
}

pub fn float(py: Python<'_>, value: f64) -> PyResult<*mut ffi::PyObject> {
    check(py, unsafe { ffi::PyFloat_FromDouble(value) })
}

/// `decimal.Decimal(text)`, called through the one-argument C-API call so no
/// argument tuple is built.
fn decimal_from_str<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let cls = DECIMAL_TYPE.import(py, "decimal", "Decimal")?;
    let arg = unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            ffi::PyUnicode_FromStringAndSize(text.as_ptr().cast(), text.len() as ffi::Py_ssize_t),
        )?
    };
    unsafe {
        Bound::from_owned_ptr_or_err(py, ffi::PyObject_CallOneArg(cls.as_ptr(), arg.as_ptr()))
    }
}

/// A Decimal for `value`, keeping the scale the server sent (trailing zeros
/// included).
pub fn decimal_object<'py>(py: Python<'py>, value: &DecimalParts) -> PyResult<Bound<'py, PyAny>> {
    let mut buf = [0u8; DECIMAL_STR_LEN];
    decimal_from_str(py, value.format_into(&mut buf))
}

pub fn decimal(py: Python<'_>, value: &DecimalParts) -> PyResult<*mut ffi::PyObject> {
    Ok(decimal_object(py, value)?.into_ptr())
}

/// money/smallmoney scaled by 10^4 -> Decimal with 4 fractional digits.
pub fn money(py: Python<'_>, scaled: i64) -> PyResult<*mut ffi::PyObject> {
    let sign = if scaled < 0 { "-" } else { "" };
    let magnitude = scaled.unsigned_abs();
    let text = format!("{sign}{}.{:04}", magnitude / 10_000, magnitude % 10_000);
    Ok(decimal_from_str(py, &text)?.into_ptr())
}
