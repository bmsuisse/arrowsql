//! char / varchar / nchar / nvarchar / xml / json -> `str`.
//!
//! TODO(decode/text): honour `setdecoding()` for SQL_CHAR columns when the
//! caller asked for a non-default codec with SQL_C_CHAR (upstream decodes the
//! raw code-page bytes with that Python codec). Today code-page text is
//! decoded via the column collation, which matches the SQL_C_WCHAR default.

use pyo3::ffi;
use pyo3::prelude::*;

/// UTF-8 bytes (already validated) -> new `str` reference.
pub fn utf8(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    let ptr = unsafe {
        ffi::PyUnicode_FromStringAndSize(bytes.as_ptr().cast(), bytes.len() as ffi::Py_ssize_t)
    };
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}

/// UTF-16LE bytes -> new `str` reference. Lone surrogates are replaced.
pub fn utf16le(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    let mut byteorder: std::os::raw::c_int = -1; // little endian
    let ptr = unsafe {
        ffi::PyUnicode_DecodeUTF16(
            bytes.as_ptr().cast(),
            bytes.len() as ffi::Py_ssize_t,
            c"replace".as_ptr(),
            &mut byteorder,
        )
    };
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}
