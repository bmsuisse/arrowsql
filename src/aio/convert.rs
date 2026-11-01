//! Decoded rows -> Python rows, on the event-loop thread.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyType};

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

/// Wrap `values` (a new reference) in a `Row` without running `Row.__init__`.
fn make_row(py: Python<'_>, row_type: &Bound<'_, PyType>, plain: bool, values: *mut ffi::PyObject, shape: &Shape) -> PyResult<*mut ffi::PyObject> {
    let values = unsafe { Bound::from_owned_ptr(py, values) };
    let none = py.None().into_bound(py);
    let row = crate::rowfactory::new_row(
        py,
        row_type.as_type_ptr(),
        plain,
        [&values, shape.column_map.bind(py), &none, shape.column_map_lower.bind(py), shape.column_names.bind(py)],
    )?;
    Ok(row.into_ptr())
}

/// Rows `from..from+count` of `buf` as a list of `Row` objects (or tuples).
pub fn rows_range<'py>(
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
        for i in 0..count {
            // A result without visible columns still has rows (empty ones).
            let row = if ncols == 0 { &[][..] } else { &buf.cells[(from + i) * ncols..(from + i + 1) * ncols] };
            let values = row_values(py, buf, row, as_tuple)?;
            let item = match (&row_type, shape) {
                (Some(t), Some(s)) => make_row(py, t, true, values, s)?,
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
///
/// The buffer sits behind an `Arc` so conversion - which runs Python code that
/// can reach back into this batch (allocation can trigger a GC finalizer) -
/// happens without holding the mutex.
#[pyclass(name = "AioRowBatch", module = "ddbc_bindings", frozen)]
pub struct RowBatch {
    state: Mutex<(Arc<RowBuffer>, usize)>,
    ncols: usize,
    shape: Arc<Shape>,
    as_tuple: bool,
}

impl RowBatch {
    pub fn new(rows: RowBuffer, ncols: usize, shape: Arc<Shape>, as_tuple: bool) -> Self {
        Self { state: Mutex::new((Arc::new(rows), 0)), ncols, shape, as_tuple }
    }

    /// Reserve up to `n` rows for the caller, to convert outside the lock.
    /// Concurrent or re-entrant callers get disjoint ranges.
    fn reserve(&self, n: usize) -> (Arc<RowBuffer>, usize, usize) {
        let mut state = self.state.lock();
        let buf = Arc::clone(&state.0);
        let pos = state.1;
        let count = n.min(buf.rows - pos);
        state.1 += count;
        if count > 0 && state.1 >= buf.rows {
            *state = (Arc::new(RowBuffer::default()), 0);
        }
        (buf, pos, count)
    }

    /// Give a reserved range back after a failed conversion, when nothing was
    /// reserved behind it. The rows stay undelivered.
    fn release(&self, buf: &Arc<RowBuffer>, pos: usize, count: usize) {
        let mut state = self.state.lock();
        if Arc::ptr_eq(&state.0, buf) && state.1 == pos + count {
            state.1 = pos;
        } else if state.0.rows == 0 && state.1 == 0 && pos + count == buf.rows {
            *state = (Arc::clone(buf), pos);
        }
    }

    /// The undelivered rows, leaving the batch empty.
    pub fn take_rest(&self) -> Option<(RowBuffer, usize)> {
        let mut state = self.state.lock();
        let (buf, pos) = std::mem::replace(&mut *state, (Arc::new(RowBuffer::default()), 0));
        let remaining = buf.rows - pos;
        if remaining == 0 {
            return None;
        }
        let rest = match Arc::try_unwrap(buf) {
            Ok(owned) if pos == 0 => owned,
            Ok(owned) => owned.slice_rows(self.ncols, pos, remaining),
            Err(shared) => shared.slice_rows(self.ncols, pos, remaining),
        };
        Some((rest, self.ncols))
    }
}

#[pymethods]
impl RowBatch {
    /// Up to `n` rows (all remaining when `n` is None).
    #[pyo3(signature = (n=None))]
    fn take<'py>(&self, py: Python<'py>, n: Option<usize>) -> PyResult<Bound<'py, PyList>> {
        let (buf, pos, count) = self.reserve(n.unwrap_or(usize::MAX));
        rows_range(py, &buf, self.ncols, pos, count, Some(&self.shape), self.as_tuple).inspect_err(|_| self.release(&buf, pos, count))
    }

    /// The next row, or None when the batch is exhausted.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let (buf, pos, count) = self.reserve(1);
        if count == 0 {
            return Ok(None);
        }
        rows_range(py, &buf, self.ncols, pos, 1, Some(&self.shape), self.as_tuple)
            .and_then(|list| list.get_item(0))
            .map(Some)
            .inspect_err(|_| self.release(&buf, pos, 1))
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
