//! `construct_rows`: wrap fetched value lists in `Row` objects without
//! running `Row.__init__` (port of the C++ RowFactory fast path).

use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyString, PyType};

static ROW_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

#[pyfunction]
#[pyo3(signature = (rows_data, row_class, column_map, cursor, column_map_lower=None, column_names=None))]
pub fn construct_rows<'py>(
    py: Python<'py>,
    rows_data: &Bound<'py, PyList>,
    row_class: &Bound<'py, PyAny>,
    column_map: &Bound<'py, PyAny>,
    cursor: &Bound<'py, PyAny>,
    column_map_lower: Option<&Bound<'py, PyAny>>,
    column_names: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyList>> {
    let row_type = row_class
        .cast::<PyType>()
        .map_err(|_| PyTypeError::new_err("row_class must be a type"))?;
    let base = ROW_TYPE.import(py, "mssql_python.row", "Row")?;
    if !row_type.is_subclass(base)? {
        return Err(PyTypeError::new_err("row_class must be Row or a Row subclass"));
    }
    let none = py.None().into_bound(py);
    let column_map_lower = column_map_lower.unwrap_or(&none);
    let column_names = column_names.unwrap_or(&none);
    let names = [
        (pyo3::intern!(py, "_row_values"), None),
        (pyo3::intern!(py, "_column_map"), Some(column_map)),
        (pyo3::intern!(py, "_row_cursor"), Some(cursor)),
        (pyo3::intern!(py, "_column_map_lower"), Some(column_map_lower)),
        (pyo3::intern!(py, "_column_names"), Some(column_names)),
    ];
    let n = rows_data.len();
    let out = PyList::empty(py);
    let type_ptr = row_type.as_type_ptr();
    for i in 0..n {
        let values = rows_data.get_item(i)?;
        let row = unsafe {
            let alloc = (*type_ptr).tp_alloc.unwrap_or(ffi::PyType_GenericAlloc);
            let ptr = alloc(type_ptr, 0);
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            Bound::from_owned_ptr(py, ptr)
        };
        for (name, value) in &names {
            let value: &Bound<'_, PyAny> = value.unwrap_or(&values);
            set_attr(py, &row, name, value)?;
        }
        out.append(row)?;
    }
    Ok(out)
}

fn set_attr(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    name: &Bound<'_, PyString>,
    value: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let rc = unsafe { ffi::PyObject_GenericSetAttr(obj.as_ptr(), name.as_ptr(), value.as_ptr()) };
    if rc < 0 { Err(PyErr::fetch(py)) } else { Ok(()) }
}
