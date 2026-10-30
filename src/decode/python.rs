//! `RowBuffer` -> Python objects. Runs with the GIL held and no Rust locks.
//!
//! A batch is decoded without the GIL into a `RowBuffer` (see `buffer.rs`);
//! `Converter` then turns each cell into a Python object. Over a whole batch
//! the converter also caches Decimal objects per column, because taxi-style
//! columns repeat a few thousand values and building a Decimal is the most
//! expensive cell conversion.

use std::collections::HashMap;
use std::os::raw::c_int;

use mssql_tds::datatypes::decoder::DecimalParts;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyList;

use super::buffer::{Cell, RowBuffer};
use super::{binary, numeric, temporal, text};

/// Most distinct Decimal values cached per column in one batch.
const DECIMAL_CACHE_LIMIT: usize = 4096;

/// Decimal objects of one column, keyed by the exact value: magnitude, scale
/// and sign. Those three fields determine the Decimal, trailing zeros included.
type DecimalCache = HashMap<(u128, u8, bool), Py<PyAny>>;

/// Converts cells of one `RowBuffer` into Python objects.
struct Converter<'a> {
    buf: &'a RowBuffer,
    errors: text::Utf16Errors,
    /// One cache per column for a batch; empty when converting single values.
    decimals: Vec<DecimalCache>,
    /// Reused buffer that holds an ASCII UTF-16 value narrowed to one byte per
    /// character.
    ascii: Vec<u8>,
}

impl<'a> Converter<'a> {
    fn uncached(buf: &'a RowBuffer, errors: text::Utf16Errors) -> Self {
        Self { buf, errors, decimals: Vec::new(), ascii: Vec::new() }
    }

    fn for_batch(buf: &'a RowBuffer, ncols: usize, errors: text::Utf16Errors) -> Self {
        Self {
            buf,
            errors,
            decimals: (0..ncols).map(|_| DecimalCache::new()).collect(),
            ascii: Vec::new(),
        }
    }

    fn cell(&mut self, py: Python<'_>, col: usize, cell: &Cell) -> PyResult<*mut ffi::PyObject> {
        let buf = self.buf;
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
            Cell::Utf16(span) => self.utf16(py, buf.bytes(*span))?,
            Cell::Bytes(span) => binary::bytes(py, buf.bytes(*span))?,
            Cell::Decimal(parts) => self.decimal(py, col, parts)?,
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

    /// A new reference to the column's cached Decimal, or a fresh one.
    fn decimal(
        &mut self,
        py: Python<'_>,
        col: usize,
        parts: &DecimalParts,
    ) -> PyResult<*mut ffi::PyObject> {
        let Some(cache) = self.decimals.get_mut(col) else {
            return numeric::decimal(py, parts);
        };
        let key = (parts.magnitude(), parts.scale, parts.is_positive);
        if let Some(value) = cache.get(&key) {
            return Ok(value.clone_ref(py).into_ptr());
        }
        let value = numeric::decimal_object(py, parts)?;
        if cache.len() < DECIMAL_CACHE_LIMIT {
            cache.insert(key, value.clone().unbind());
        }
        Ok(value.into_ptr())
    }

    /// UTF-16LE text. ASCII-only values, the common case, are narrowed to one
    /// byte per character and built directly; anything else takes the codec.
    fn utf16(&mut self, py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
        if !is_ascii_utf16(bytes) {
            return text::utf16le(py, bytes, self.errors);
        }
        self.ascii.clear();
        self.ascii.extend(bytes.iter().step_by(2));
        let ptr = unsafe {
            ffi::PyUnicode_FromKindAndData(
                ffi::PyUnicode_1BYTE_KIND as c_int,
                self.ascii.as_ptr().cast(),
                self.ascii.len() as ffi::Py_ssize_t,
            )
        };
        if ptr.is_null() { Err(PyErr::fetch(py)) } else { Ok(ptr) }
    }

    /// Build one `list` for a row. Its items come from `row`, whose cells are
    /// columns `0..row.len()`.
    fn row_list<'py>(&mut self, py: Python<'py>, row: &[Cell]) -> PyResult<Bound<'py, PyAny>> {
        unsafe {
            let list = ffi::PyList_New(row.len() as ffi::Py_ssize_t);
            if list.is_null() {
                return Err(PyErr::fetch(py));
            }
            // Owned from here: dropped (and decref'd) on any early return.
            let list = Bound::from_owned_ptr(py, list);
            for (col, cell) in row.iter().enumerate() {
                let item = self.cell(py, col, cell)?;
                ffi::PyList_SET_ITEM(list.as_ptr(), col as ffi::Py_ssize_t, item);
            }
            Ok(list)
        }
    }
}

/// True when `bytes` is UTF-16LE in which every code unit is ASCII.
fn is_ascii_utf16(bytes: &[u8]) -> bool {
    bytes.len() % 2 == 0 && bytes.chunks_exact(2).all(|unit| unit[0] < 0x80 && unit[1] == 0)
}

/// Convert one cell without any batch cache.
pub(crate) fn cell_to_py(
    py: Python<'_>,
    buf: &RowBuffer,
    cell: &Cell,
    errors: text::Utf16Errors,
) -> PyResult<*mut ffi::PyObject> {
    Converter::uncached(buf, errors).cell(py, 0, cell)
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
    let mut converter = Converter::for_batch(buf, ncols, errors);
    for row in buf.cells.chunks_exact(ncols) {
        let list = converter.row_list(py, row)?;
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
    let mut converter = Converter::uncached(buf, text::Utf16Errors::Strict);
    for (col, cell) in row.iter().enumerate() {
        let ptr = converter.cell(py, col, cell)?;
        let obj = unsafe { Bound::from_owned_ptr(py, ptr) };
        out.append(obj)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_ascii_utf16;

    #[test]
    fn ascii_utf16_detection() {
        assert!(is_ascii_utf16(&[]));
        assert!(is_ascii_utf16(&[b'a', 0, b'Z', 0]));
        assert!(!is_ascii_utf16(&[0xE9, 0]), "U+00E9 is not ASCII");
        assert!(!is_ascii_utf16(&[b'a', 1]), "U+0161 is not ASCII");
        assert!(!is_ascii_utf16(&[b'a']), "odd length is malformed");
    }
}
