//! `RowCore`, the native base of `arrowsql.row.Row`, and `construct_rows`
//! (port of the C++ RowFactory fast path). Rows are allocated through
//! `RowCore`'s tp_new and filled directly, with no `Row.__init__`.

use pyo3::exceptions::{PyAttributeError, PyKeyError, PyTypeError};
use pyo3::ffi;
use pyo3::PyTypeInfo;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyInt, PyList, PySlice, PyString, PyTuple, PyType};
use std::ptr;

static ROW_MAPPING: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DECIMAL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Values, shared column maps and cursor of one fetched row.
#[pyclass(subclass, dict, weakref, module = "arrowsql.ddbc_bindings")]
pub struct RowCore {
    #[pyo3(get, set, name = "_row_values")]
    values: Py<PyAny>,
    #[pyo3(get, set, name = "_column_map")]
    column_map: Py<PyAny>,
    #[pyo3(get, set, name = "_row_cursor")]
    cursor: Py<PyAny>,
    #[pyo3(get, set, name = "_column_map_lower")]
    column_map_lower: Py<PyAny>,
    #[pyo3(get, set, name = "_column_names")]
    column_names: Py<PyAny>,
}

/// `map.get(key)`, None when absent or when `map` is None.
fn lookup<'py>(map: &Bound<'py, PyAny>, key: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    if map.is_none() {
        return Ok(None);
    }
    if let Ok(d) = map.cast::<PyDict>() {
        return d.get_item(key);
    }
    let v = map.call_method1(pyo3::intern!(map.py(), "get"), (key,))?;
    Ok(if v.is_none() { None } else { Some(v) })
}

#[pymethods]
impl RowCore {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        py: Python<'_>,
        _args: &Bound<'_, PyTuple>,
        _kwargs: Option<&Bound<'_, PyDict>>,
    ) -> Self {
        let none = py.None();
        Self {
            values: none.clone_ref(py),
            column_map: none.clone_ref(py),
            cursor: none.clone_ref(py),
            column_map_lower: none.clone_ref(py),
            column_names: none,
        }
    }

    fn __getitem__<'py>(&self, index: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let py = index.py();
        let values = self.values.bind(py);
        if index.is_exact_instance_of::<PyInt>() {
            if let (Ok(list), Ok(i)) = (values.cast::<PyList>(), index.extract::<isize>()) {
                let n = list.len() as isize;
                let j = if i < 0 { i + n } else { i };
                if j >= 0 && j < n {
                    return list.get_item(j as usize);
                }
            }
            return values.get_item(index);
        }
        if let Ok(key) = index.cast::<PyString>() {
            if let Some(idx) = lookup(self.column_map.bind(py), key)? {
                return values.get_item(idx);
            }
            if !self.column_map_lower.bind(py).is_none() {
                let lower = key.call_method0(pyo3::intern!(py, "lower"))?;
                if let Some(idx) = lookup(self.column_map_lower.bind(py), &lower)? {
                    return values.get_item(idx);
                }
            }
            return Err(PyKeyError::new_err(format!("Row has no column '{}'", key.to_str()?)));
        }
        if index.is_instance_of::<PyInt>() || index.is_instance_of::<PySlice>() {
            return values.get_item(index);
        }
        Err(PyTypeError::new_err(format!(
            "Row indices must be integers, slices, or strings, not {}",
            index.get_type().name()?
        )))
    }

    fn __getattr__<'py>(&self, py: Python<'py>, name: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let values = self.values.bind(py);
        if let Some(idx) = lookup(self.column_map.bind(py), name)? {
            return values.get_item(idx);
        }
        if !self.column_map_lower.bind(py).is_none() {
            let lower = name.call_method0(pyo3::intern!(py, "lower"))?;
            if let Some(idx) = lookup(self.column_map_lower.bind(py), &lower)? {
                return values.get_item(idx);
            }
        }
        // Old names of the internal fields, kept readable for existing callers.
        if let Ok(s) = name.cast::<PyString>() {
            match s.to_str()? {
                "_values" => return Ok(values.clone()),
                "_cursor" => return Ok(self.cursor.bind(py).clone()),
                _ => {}
            }
        }
        Err(PyAttributeError::new_err(format!("Row has no attribute '{}'", name.str()?)))
    }

    #[getter(_mapping)]
    fn mapping<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        ROW_MAPPING.import(py, "arrowsql.row", "RowMapping")?.call1((slf,))
    }

    /// Canonical, order-preserving column names backing `_mapping`.
    fn _mapping_keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let names = self.column_names.bind(py);
        if !names.is_none() {
            return Ok(names.clone());
        }
        let map = self.column_map.bind(py);
        if !map.is_none() && map.is_truthy()? {
            let mut pairs: Vec<(isize, Bound<'py, PyAny>)> = Vec::new();
            for item in map.call_method0(pyo3::intern!(py, "items"))?.try_iter()? {
                let (name, idx): (Bound<'py, PyAny>, isize) = item?.extract()?;
                if !pairs.iter().any(|(i, _)| *i == idx) {
                    pairs.push((idx, name));
                }
            }
            pairs.sort_by_key(|(i, _)| *i);
            return Ok(PyTuple::new(py, pairs.into_iter().map(|(_, n)| n))?.into_any());
        }
        Ok(PyTuple::empty(py).into_any())
    }

    fn __richcmp__(&self, py: Python<'_>, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<Py<PyAny>> {
        if !matches!(op, CompareOp::Eq | CompareOp::Ne) {
            return Ok(py.NotImplemented());
        }
        let values = self.values.bind(py);
        let eq = if other.is_instance_of::<PyList>() {
            values.eq(other)?
        } else if let Ok(o) = other.cast::<RowCore>() {
            values.eq(o.borrow().values.bind(py))?
        } else {
            return Ok(py.NotImplemented());
        };
        Ok((eq == matches!(op, CompareOp::Eq)).into_pyobject(py)?.to_owned().into_any().unbind())
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        self.values.bind(py).len()
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.values.bind(py).try_iter()?.into_any())
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        let decimal = DECIMAL.import(py, "decimal", "Decimal")?;
        let mut parts: Vec<String> = Vec::new();
        for value in self.values.bind(py).try_iter()? {
            let value = value?;
            if value.is_instance(decimal)? {
                let s = value.str()?.to_string();
                let sep: String = py
                    .import("arrowsql")?
                    .getattr("getDecimalSeparator")?
                    .call0()?
                    .extract()?;
                if sep != "." && s.contains('.') {
                    parts.push(s.replace('.', &sep));
                } else {
                    parts.push(s);
                }
            } else {
                parts.push(value.repr()?.to_string());
            }
        }
        Ok(format!("({})", parts.join(", ")))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let t = PyTuple::type_object(py).call1((self.values.bind(py),))?;
        Ok(t.repr()?.to_string())
    }
}

/// Allocate a `row_type` instance through `RowCore.__new__` and fill its fields.
pub(crate) fn new_row<'py>(
    py: Python<'py>,
    row_type: *mut ffi::PyTypeObject,
    f: [&Bound<'py, PyAny>; 5],
) -> PyResult<Bound<'py, PyAny>> {
    let args = PyTuple::empty(py);
    let tp_new = unsafe { (*py.get_type::<RowCore>().as_type_ptr()).tp_new }
        .ok_or_else(|| PyTypeError::new_err("RowCore has no tp_new"))?;
    let ptr = unsafe { tp_new(row_type, args.as_ptr(), ptr::null_mut()) };
    if ptr.is_null() {
        return Err(PyErr::fetch(py));
    }
    let row = unsafe { Bound::from_owned_ptr(py, ptr) };
    {
        let core = unsafe { row.cast_unchecked::<RowCore>() };
        let mut c = core.borrow_mut();
        c.values = f[0].clone().unbind();
        c.column_map = f[1].clone().unbind();
        c.cursor = f[2].clone().unbind();
        c.column_map_lower = f[3].clone().unbind();
        c.column_names = f[4].clone().unbind();
    }
    Ok(row)
}

fn check_row_type<'py>(py: Python<'py>, row_class: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyType>> {
    let row_type = row_class
        .cast::<PyType>()
        .map_err(|_| PyTypeError::new_err("row_class must be a type"))?;
    if !row_type.is_subclass(&py.get_type::<RowCore>())? {
        return Err(PyTypeError::new_err("row_class must be Row or a Row subclass"));
    }
    Ok(row_type.clone())
}

/// One row, bypassing `Row.__init__`: `Row._fast_create` delegates here.
#[pyfunction]
#[pyo3(signature = (row_class, values, column_map, cursor, column_map_lower=None, column_names=None))]
pub fn make_row_fast<'py>(
    py: Python<'py>,
    row_class: &Bound<'py, PyAny>,
    values: &Bound<'py, PyAny>,
    column_map: &Bound<'py, PyAny>,
    cursor: &Bound<'py, PyAny>,
    column_map_lower: Option<&Bound<'py, PyAny>>,
    column_names: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let row_type = check_row_type(py, row_class)?;
    let none = py.None().into_bound(py);
    new_row(
        py,
        row_type.as_type_ptr(),
        [values, column_map, cursor, column_map_lower.unwrap_or(&none), column_names.unwrap_or(&none)],
    )
}

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
    let row_type = check_row_type(py, row_class)?;
    let none = py.None().into_bound(py);
    let (lower, names) = (column_map_lower.unwrap_or(&none), column_names.unwrap_or(&none));
    let type_ptr = row_type.as_type_ptr();
    let out = PyList::empty(py);
    for values in rows_data.iter() {
        out.append(new_row(py, type_ptr, [&values, column_map, cursor, lower, names])?)?;
    }
    Ok(out)
}
