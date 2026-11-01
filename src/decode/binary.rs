//! binary / varbinary / image -> `bytes`; uniqueidentifier -> `uuid.UUID`.

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;
use uuid::Uuid;

static SAFE_UNKNOWN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static UUID_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static STDLIB_UUID: PyOnceLock<bool> = PyOnceLock::new();

pub fn bytes(py: Python<'_>, data: &[u8]) -> PyResult<*mut ffi::PyObject> {
    let ptr = unsafe {
        ffi::PyBytes_FromStringAndSize(data.as_ptr().cast(), data.len() as ffi::Py_ssize_t)
    };
    if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
}

pub fn uuid(py: Python<'_>, value: &Uuid) -> PyResult<*mut ffi::PyObject> {
    let cls = UUID_TYPE.import(py, "uuid", "UUID")?;
    // A replaced `uuid.UUID` (a subclass, say) is called as the API promises.
    if !*STDLIB_UUID.get_or_try_init(py, || -> PyResult<bool> {
        // NAMESPACE_DNS was built when the uuid module loaded: its type is the real class.
        Ok(py.import("uuid")?.getattr("NAMESPACE_DNS")?.get_type().is(cls))
    })? {
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("bytes", pyo3::types::PyBytes::new(py, value.as_bytes()))?;
        return Ok(cls.call((), Some(&kwargs))?.into_ptr());
    }
    // Same construction as CPython's `UUID._from_int`: allocate without running
    // `UUID.__init__` and fill the slots. `UUID.__setattr__` raises, hence the
    // generic setattr.
    let unknown = SAFE_UNKNOWN.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("uuid")?.getattr("SafeUUID")?.getattr("unknown")?.unbind())
    })?;
    let int = value.as_u128().into_pyobject(py)?;
    unsafe {
        let obj = ffi::PyType_GenericAlloc(cls.as_type_ptr(), 0);
        if obj.is_null() {
            return Err(PyErr::fetch(py));
        }
        let owned = Bound::from_owned_ptr(py, obj);
        if ffi::PyObject_GenericSetAttr(obj, pyo3::intern!(py, "int").as_ptr(), int.as_ptr()) != 0
            || ffi::PyObject_GenericSetAttr(obj, pyo3::intern!(py, "is_safe").as_ptr(), unknown.as_ptr()) != 0
        {
            return Err(PyErr::fetch(py));
        }
        Ok(owned.into_ptr())
    }
}
