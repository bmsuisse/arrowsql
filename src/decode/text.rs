//! char / varchar / nchar / nvarchar / xml / json -> `str`.
//!
//! Text reaches this module already decoded to UTF-8 or UTF-16LE by the TDS
//! layer. Code-page (non-Unicode) text is decoded there with the column
//! collation, which is what upstream's driver-side conversion produces on
//! Linux/macOS. Upstream ignores `setdecoding()`'s codec for SQL_C_CHAR on
//! those platforms (`GetEffectiveCharDecoding` pins UTF-8), so no codec
//! parameter is threaded through here. The Windows branch, which applies the
//! codec to raw code-page bytes, is not implemented.

use pyo3::ffi;
use pyo3::prelude::*;

/// What to do with a UTF-16 payload that is not well formed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Utf16Errors {
    /// `fetchone` and scroll fetches: raise `UnicodeDecodeError`.
    Strict,
    /// Bulk-bound `fetchmany` / `fetchall` over bounded columns. Upstream
    /// returns `''` for the value on Linux/macOS and keeps the lone surrogates
    /// on Windows.
    BulkBounded,
}

/// UTF-8 bytes (already validated) -> new `str` reference.
pub fn utf8(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    let ptr = unsafe {
        ffi::PyUnicode_FromStringAndSize(bytes.as_ptr().cast(), bytes.len() as ffi::Py_ssize_t)
    };
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}

/// UTF-16LE bytes -> new `str` reference.
///
/// The strict decode is the hot path; the fallback only runs after it fails.
pub fn utf16le(py: Python<'_>, bytes: &[u8], errors: Utf16Errors) -> PyResult<*mut ffi::PyObject> {
    let mut byteorder: std::os::raw::c_int = -1; // little endian
    let ptr = unsafe {
        ffi::PyUnicode_DecodeUTF16(
            bytes.as_ptr().cast(),
            bytes.len() as ffi::Py_ssize_t,
            std::ptr::null(),
            &mut byteorder,
        )
    };
    if !ptr.is_null() {
        return Ok(ptr);
    }
    let err = PyErr::fetch(py);
    if errors == Utf16Errors::Strict {
        return Err(err);
    }
    drop(err);
    malformed_bulk(py, bytes)
}

/// Bulk-bound fallback for a malformed value (see [`Utf16Errors::BulkBounded`]).
fn malformed_bulk(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    if !cfg!(windows) {
        return utf8(py, b"");
    }
    let mut byteorder: std::os::raw::c_int = -1;
    let ptr = unsafe {
        ffi::PyUnicode_DecodeUTF16(
            bytes.as_ptr().cast(),
            bytes.len() as ffi::Py_ssize_t,
            c"surrogatepass".as_ptr(),
            &mut byteorder,
        )
    };
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}
