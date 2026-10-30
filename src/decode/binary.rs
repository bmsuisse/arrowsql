//! binary / varbinary / image -> `bytes`; uniqueidentifier -> `uuid.UUID`.

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyType};
use uuid::Uuid;

static UUID_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

pub fn bytes(py: Python<'_>, data: &[u8]) -> PyResult<*mut ffi::PyObject> {
    let ptr = unsafe {
        ffi::PyBytes_FromStringAndSize(data.as_ptr().cast(), data.len() as ffi::Py_ssize_t)
    };
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}

pub fn uuid(py: Python<'_>, value: &Uuid) -> PyResult<*mut ffi::PyObject> {
    // TODO(perf): bypass UUID.__init__ (object.__new__ + set `int`).
    let cls = UUID_TYPE.import(py, "uuid", "UUID")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("bytes", pyo3::types::PyBytes::new(py, value.as_bytes()))?;
    Ok(cls.call((), Some(&kwargs))?.into_ptr())
}
