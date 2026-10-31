//! Decoded rows -> Python rows, on the event-loop thread.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyString, PyType};

use crate::decode::RowBuffer;
use crate::decode::python::cell_to_py;
use crate::decode::text::Utf16Errors;

static ROW_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Python-side description of a result shape, built once per distinct set of
/// columns by `aio._make_shape` and cached by the hub.
pub struct Shape {
    pub obj: Py<PyAny>,
    column_map: Py<PyAny>,
    column_map_lower: Py<PyAny>,
    column_names: Py<PyAny>,
}

impl Shape {
    pub fn from_factory(py: Python<'_>, obj: Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            column_map: obj.getattr(intern!(py, "column_map"))?.unbind(),
            column_map_lower: obj.getattr(intern!(py, "column_map_lower"))?.unbind(),
            column_names: obj.getattr(intern!(py, "column_names"))?.unbind(),
            obj: obj.unbind(),
        })
    }
}

/// Values of one row as a list (`Row._row_values`) or a tuple.
fn row_values(py: Python<'_>, buf: &RowBuffer, row: &[crate::decode::buffer::Cell], as_tuple: bool) -> PyResult<*mut ffi::PyObject> {
    unsafe {
        let n = row.len() as ffi::Py_ssize_t;
        let container = if as_tuple { ffi::PyTuple_New(n) } else { ffi::PyList_New(n) };
        if container.is_null() {
            return Err(PyErr::fetch(py));
        }
        // Owned from here: dropped (and decref'd) on any early return.
        let owned = Bound::from_owned_ptr(py, container);
        for (i, cell) in row.iter().enumerate() {
            let item = cell_to_py(py, buf, cell, Utf16Errors::Strict)?;
            if as_tuple {
                ffi::PyTuple_SET_ITEM(container, i as ffi::Py_ssize_t, item);
            } else {
                ffi::PyList_SET_ITEM(container, i as ffi::Py_ssize_t, item);
            }
        }
        Ok(owned.into_ptr())
    }
}

fn set_slot(py: Python<'_>, obj: *mut ffi::PyObject, name: &Bound<'_, PyString>, value: *mut ffi::PyObject) -> PyResult<()> {
    let rc = unsafe { ffi::PyObject_GenericSetAttr(obj, name.as_ptr(), value) };
    if rc < 0 { Err(PyErr::fetch(py)) } else { Ok(()) }
}

/// Wrap `values` (a new reference) in a `Row` without running `Row.__init__`.
fn make_row(py: Python<'_>, row_type: &Bound<'_, PyType>, values: *mut ffi::PyObject, shape: &Shape) -> PyResult<*mut ffi::PyObject> {
    let values = unsafe { Bound::from_owned_ptr(py, values) };
    let row = unsafe {
        let type_ptr = row_type.as_type_ptr();
        let alloc = (*type_ptr).tp_alloc.unwrap_or(ffi::PyType_GenericAlloc);
        let ptr = alloc(type_ptr, 0);
        if ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        Bound::from_owned_ptr(py, ptr)
    };
    let none = py.None();
    let ptr = row.as_ptr();
    set_slot(py, ptr, intern!(py, "_row_values"), values.as_ptr())?;
    set_slot(py, ptr, intern!(py, "_column_map"), shape.column_map.as_ptr())?;
    set_slot(py, ptr, intern!(py, "_row_cursor"), none.as_ptr())?;
    set_slot(py, ptr, intern!(py, "_column_map_lower"), shape.column_map_lower.as_ptr())?;
    set_slot(py, ptr, intern!(py, "_column_names"), shape.column_names.as_ptr())?;
    Ok(row.into_ptr())
}

/// Rows `from..from+count` of `buf` as a list of `Row` objects (or tuples).
fn rows_range<'py>(
    py: Python<'py>,
    buf: &RowBuffer,
    ncols: usize,
    from: usize,
    count: usize,
    shape: Option<&Shape>,
    as_tuple: bool,
) -> PyResult<Bound<'py, PyList>> {
    let row_type = match (shape, as_tuple) {
        (Some(_), false) => Some(ROW_TYPE.import(py, "arrowsql.row", "Row")?.clone()),
        _ => None,
    };
    unsafe {
        let list = ffi::PyList_New(count as ffi::Py_ssize_t);
        if list.is_null() {
            return Err(PyErr::fetch(py));
        }
        let list = Bound::from_owned_ptr(py, list);
        if ncols == 0 {
            return Ok(list.cast_into_unchecked());
        }
        let rows = buf.cells[from * ncols..(from + count) * ncols].chunks_exact(ncols);
        for (i, row) in rows.enumerate() {
            let values = row_values(py, buf, row, as_tuple)?;
            let item = match (&row_type, shape) {
                (Some(t), Some(s)) => make_row(py, t, values, s)?,
                _ => values,
            };
            ffi::PyList_SET_ITEM(list.as_ptr(), i as ffi::Py_ssize_t, item);
        }
        Ok(list.cast_into_unchecked())
    }
}

pub fn rows_to_list<'py>(
    py: Python<'py>,
    buf: &RowBuffer,
    ncols: usize,
    shape: Option<&Shape>,
    as_tuple: bool,
) -> PyResult<Bound<'py, PyList>> {
    let count = buf.rows;
    rows_range(py, buf, ncols, 0, count, shape, as_tuple)
}

/// Rows read ahead by `execute`, turned into Python rows only when fetched
/// (or moved into Arrow untouched by `fetchall_arrow`).
#[pyclass(name = "AioRowBatch", module = "ddbc_bindings", frozen)]
pub struct RowBatch {
    state: Mutex<(RowBuffer, usize)>,
    ncols: usize,
    shape: Arc<Shape>,
    as_tuple: bool,
}

impl RowBatch {
    pub fn new(rows: RowBuffer, ncols: usize, shape: Arc<Shape>, as_tuple: bool) -> Self {
        Self { state: Mutex::new((rows, 0)), ncols, shape, as_tuple }
    }

    /// The undelivered rows, leaving the batch empty.
    pub fn take_rest(&self) -> Option<(RowBuffer, usize)> {
        let mut state = self.state.lock();
        let (buf, pos) = &mut *state;
        let remaining = buf.rows - *pos;
        if remaining == 0 {
            return None;
        }
        let rest = if *pos == 0 { std::mem::take(buf) } else { buf.slice_rows(self.ncols, *pos, remaining) };
        buf.clear_and_trim();
        *pos = 0;
        Some((rest, self.ncols))
    }
}

#[pymethods]
impl RowBatch {
    /// Up to `n` rows (all remaining when `n` is None).
    #[pyo3(signature = (n=None))]
    fn take<'py>(&self, py: Python<'py>, n: Option<usize>) -> PyResult<Bound<'py, PyList>> {
        let mut state = self.state.lock();
        let (buf, pos) = &mut *state;
        let count = n.unwrap_or(usize::MAX).min(buf.rows - *pos);
        let list = rows_range(py, buf, self.ncols, *pos, count, Some(&self.shape), self.as_tuple)?;
        *pos += count;
        if *pos == buf.rows {
            buf.clear_and_trim();
            *pos = 0;
        }
        Ok(list)
    }

    /// The next row, or None.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let list = self.take(py, Some(1))?;
        if list.is_empty() { Ok(None) } else { Ok(Some(list.get_item(0)?)) }
    }

    fn remaining(&self) -> usize {
        let state = self.state.lock();
        state.0.rows - state.1
    }

    #[getter]
    fn shape(&self, py: Python<'_>) -> Py<PyAny> {
        self.shape.obj.clone_ref(py)
    }
}

pub fn row_batch_of<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, RowBatch>>> {
    if obj.is_none() {
        return Ok(None);
    }
    obj.cast::<RowBatch>()
        .map(|b| Some(b.clone()))
        .map_err(|_| PyTypeError::new_err("expected an AioRowBatch"))
}
