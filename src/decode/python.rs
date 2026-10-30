//! `RowBuffer` -> Python objects. Runs with the GIL held and no Rust locks.

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyList;

use super::buffer::{Cell, RowBuffer};
use super::{binary, numeric, temporal, text};

fn cell_to_py(
    py: Python<'_>,
    buf: &RowBuffer,
    cell: &Cell,
    errors: text::Utf16Errors,
) -> PyResult<*mut ffi::PyObject> {
    Ok(match cell {
        Cell::Null => unsafe {
            let none = ffi::Py_None();
            ffi::Py_IncRef(none);
            none
        },
        Cell::Bool(b) => numeric::boolean(*b),
        Cell::Int(i) => numeric::int(py, *i)?,
        Cell::Float(f) => numeric::float(py, *f)?,
        Cell::Real(f) => numeric::float(py, f64::from(*f))?,
        Cell::Utf8(span) => text::utf8(py, buf.bytes(*span))?,
        Cell::Utf16(span) => text::utf16le(py, buf.bytes(*span), errors)?,
        Cell::Bytes(span) => binary::bytes(py, buf.bytes(*span))?,
        Cell::Decimal(parts) => numeric::decimal(py, parts)?,
        Cell::Money(scaled) => numeric::money(py, *scaled)?,
        Cell::Date(days) => temporal::date(py, *days)?.into_ptr(),
        Cell::Time { ticks } => temporal::time(py, *ticks)?.into_ptr(),
        Cell::DateTime { days, ticks } => temporal::datetime(py, *days, *ticks)?.into_ptr(),
        Cell::DateTimeOffset { days, ticks, offset_minutes } => {
            temporal::datetimeoffset(py, *days, *ticks, *offset_minutes)?.into_ptr()
        }
        Cell::Uuid(u) => binary::uuid(py, u)?,
    })
}

/// Build one `list` per row and append each to `out`.
///
/// `errors` applies to malformed UTF-16 text; see [`text::Utf16Errors`].
pub fn append_rows(
    py: Python<'_>,
    buf: &RowBuffer,
    ncols: usize,
    out: &Bound<'_, PyList>,
    errors: text::Utf16Errors,
) -> PyResult<()> {
    if ncols == 0 {
        return Ok(());
    }
    for row in buf.cells.chunks_exact(ncols) {
        let list = row_list(py, buf, row, errors)?;
        out.append(list)?;
    }
    Ok(())
}

/// Fill `out` (a list) in place with the values of a single row. Single-row
/// fetches always decode strictly.
pub fn extend_with_row(
    py: Python<'_>,
    buf: &RowBuffer,
    row: &[Cell],
    out: &Bound<'_, PyList>,
) -> PyResult<()> {
    for cell in row {
        let ptr = cell_to_py(py, buf, cell, text::Utf16Errors::Strict)?;
        let obj = unsafe { Bound::from_owned_ptr(py, ptr) };
        out.append(obj)?;
    }
    Ok(())
}

fn row_list<'py>(
    py: Python<'py>,
    buf: &RowBuffer,
    row: &[Cell],
    errors: text::Utf16Errors,
) -> PyResult<Bound<'py, PyAny>> {
    unsafe {
        let list = ffi::PyList_New(row.len() as ffi::Py_ssize_t);
        if list.is_null() {
            return Err(PyErr::fetch(py));
        }
        // Owned from here: dropped (and decref'd) on any early return.
        let list = Bound::from_owned_ptr(py, list);
        for (i, cell) in row.iter().enumerate() {
            let item = cell_to_py(py, buf, cell, errors)?;
            ffi::PyList_SET_ITEM(list.as_ptr(), i as ffi::Py_ssize_t, item);
        }
        Ok(list)
    }
}
