//! `RowBuffer` -> Python objects. Runs with the GIL held and no Rust locks.
//!
//! A batch is decoded without the GIL into a `RowBuffer` (see `buffer.rs`);
//! `Converter` then turns each cell into a Python object. Over a whole batch
//! the converter also caches Decimal objects per column, because taxi-style
//! columns repeat a few thousand values and building a Decimal is the most
//! expensive cell conversion.

use std::mem::MaybeUninit;
use std::sync::OnceLock;

use mssql_tds::datatypes::decoder::DecimalParts;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyList;

use super::buffer::{Cell, RowBuffer};
use super::{binary, numeric, temporal, text};

/// Most slots in one column's Decimal cache.
const DECIMAL_CACHE_SLOTS: usize = 4096;

/// Decimal objects of one column in a direct-mapped table, keyed by the exact
/// value: magnitude, scale and sign. Those three fields determine the Decimal,
/// trailing zeros included. A hit compares the full key; a collision simply
/// overwrites the slot, so the table never grows or rehashes.
#[derive(Default)]
struct DecimalCache {
    slots: Vec<Option<DecimalSlot>>,
    bits: u32,
}

struct DecimalSlot {
    magnitude: u128,
    scale: u8,
    positive: bool,
    value: Py<PyAny>,
}

impl DecimalCache {
    /// Allocated on first use, sized for `rows` values (a power of two).
    fn init(&mut self, rows: usize) {
        let slots = rows.next_power_of_two().max(16).min(slot_limit());
        self.slots = (0..slots).map(|_| None).collect();
        self.bits = slots.trailing_zeros();
    }

    /// A new reference to the cached Decimal for `parts`, building and
    /// caching it on a miss.
    fn get(&mut self, py: Python<'_>, parts: &DecimalParts, rows: usize) -> PyResult<*mut ffi::PyObject> {
        if let Some(hit) = self.lookup(py, parts, rows) {
            return Ok(hit);
        }
        let value = numeric::decimal_object(py, parts)?;
        self.store(parts, &value);
        Ok(value.into_ptr())
    }

    /// A new reference to the cached Decimal for `parts`, if present. Never
    /// calls into Python beyond a refcount increment.
    fn lookup(&mut self, py: Python<'_>, parts: &DecimalParts, rows: usize) -> Option<*mut ffi::PyObject> {
        if self.slots.is_empty() {
            self.init(rows);
        }
        let (magnitude, scale, positive) = (parts.magnitude(), parts.scale, parts.is_positive);
        let slot = self.slots[self.index(magnitude, scale, positive)].as_ref()?;
        (slot.magnitude == magnitude && slot.scale == scale && slot.positive == positive)
            .then(|| slot.value.clone_ref(py).into_ptr())
    }

    fn store(&mut self, parts: &DecimalParts, value: &Bound<'_, PyAny>) {
        if self.slots.is_empty() {
            return;
        }
        let (magnitude, scale, positive) = (parts.magnitude(), parts.scale, parts.is_positive);
        let index = self.index(magnitude, scale, positive);
        self.slots[index] = Some(DecimalSlot { magnitude, scale, positive, value: value.clone().unbind() });
    }

    fn index(&self, magnitude: u128, scale: u8, positive: bool) -> usize {
        let folded = (magnitude as u64) ^ ((magnitude >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        if self.bits == 0 {
            return 0;
        }
        let key = folded ^ (u64::from(scale) << 1) ^ u64::from(positive);
        let mixed = key.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        (mixed >> (64 - self.bits)) as usize
    }
}

/// Slots of the per-thread cache used by fetchone and other single-row paths.
const SINGLE_ROW_SLOTS: usize = 1024;

thread_local! {
    static SINGLE_ROW_DECIMALS: std::cell::RefCell<DecimalCache> = std::cell::RefCell::new(DecimalCache::default());
}

/// Slot count cap; `MSSQL_RS_DECIMAL_CACHE_SLOTS` shrinks it so tests can
/// force collisions.
fn slot_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("MSSQL_RS_DECIMAL_CACHE_SLOTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map_or(DECIMAL_CACHE_SLOTS, |n| n.clamp(1, DECIMAL_CACHE_SLOTS).next_power_of_two())
    })
}

/// Converts cells of one `RowBuffer` into Python objects.
struct Converter<'a> {
    buf: &'a RowBuffer,
    errors: text::Utf16Errors,
    /// One cache per column for a batch; empty when converting single values.
    decimals: Vec<DecimalCache>,
    /// Rows in the batch, which sizes the Decimal caches.
    rows: usize,
}

impl<'a> Converter<'a> {
    fn uncached(buf: &'a RowBuffer, errors: text::Utf16Errors) -> Self {
        Self { buf, errors, decimals: Vec::new(), rows: 0 }
    }

    fn for_batch(buf: &'a RowBuffer, ncols: usize, errors: text::Utf16Errors) -> Self {
        Self {
            buf,
            errors,
            decimals: (0..ncols).map(|_| DecimalCache::default()).collect(),
            rows: buf.rows,
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
            Cell::Money(scaled) => {
                let parts = DecimalParts::new(*scaled >= 0, 19, 4, u128::from(scaled.unsigned_abs()));
                self.decimal(py, col, &parts)?
            }
            Cell::Date(days) => temporal::date(py, *days)?.into_ptr(),
            Cell::Time { ticks } => temporal::time(py, *ticks)?.into_ptr(),
            Cell::DateTime { days, ticks } => temporal::datetime(py, *days, *ticks)?.into_ptr(),
            Cell::DateTimeOffset { days, ticks, offset_minutes } => {
                temporal::datetimeoffset(py, *days, *ticks, *offset_minutes)?.into_ptr()
            }
            Cell::Uuid(u) => binary::uuid(py, u)?,
        })
    }

    /// A new reference to the column's cached Decimal, or a fresh one. Single
    /// row fetches share one per-thread cache across columns and calls.
    fn decimal(
        &mut self,
        py: Python<'_>,
        col: usize,
        parts: &DecimalParts,
    ) -> PyResult<*mut ffi::PyObject> {
        match self.decimals.get_mut(col) {
            Some(cache) => cache.get(py, parts, self.rows),
            None => {
                // Building a Decimal runs Python code (a GC finalizer may re-enter
                // fetchone on this thread), so the cache is never borrowed across it.
                let hit = SINGLE_ROW_DECIMALS
                    .with(|cache| cache.try_borrow_mut().ok().and_then(|mut c| c.lookup(py, parts, SINGLE_ROW_SLOTS)));
                if let Some(hit) = hit {
                    return Ok(hit);
                }
                let value = numeric::decimal_object(py, parts)?;
                SINGLE_ROW_DECIMALS.with(|cache| {
                    if let Ok(mut cache) = cache.try_borrow_mut() {
                        cache.store(parts, &value);
                    }
                });
                Ok(value.into_ptr())
            }
        }
    }

    /// UTF-16LE text. ASCII-only values, the common case, are narrowed to one
    /// byte per character straight into a new `str`; anything else takes the
    /// codec.
    fn utf16(&mut self, py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
        if bytes.len() % 2 != 0 {
            return text::utf16le(py, bytes, self.errors);
        }
        unsafe {
            let ptr = ffi::PyUnicode_New((bytes.len() / 2) as ffi::Py_ssize_t, 127);
            if ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            let dst = std::slice::from_raw_parts_mut(
                ffi::PyUnicode_1BYTE_DATA(ptr).cast::<MaybeUninit<u8>>(),
                bytes.len() / 2,
            );
            if narrow_ascii_utf16(bytes, dst) {
                Ok(ptr)
            } else {
                ffi::Py_DecRef(ptr);
                text::utf16le(py, bytes, self.errors)
            }
        }
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

/// High bytes and bit 7 of the low byte of four UTF-16LE units.
const NON_ASCII_MASK: u64 = 0xFF80_FF80_FF80_FF80;

/// Narrow UTF-16LE `src` into `dst` (one byte per unit) while checking that
/// every unit is ASCII. Returns false at the first non-ASCII unit, leaving
/// `dst` partly written; on true all of `dst` is initialised. `src` must hold
/// exactly `2 * dst.len()` bytes.
pub(super) fn narrow_ascii_utf16(src: &[u8], dst: &mut [MaybeUninit<u8>]) -> bool {
    debug_assert_eq!(src.len(), dst.len() * 2);
    let mut chunks = src.chunks_exact(8);
    let mut out = dst.chunks_exact_mut(4);
    for (chunk, o) in (&mut chunks).zip(&mut out) {
        let word = u64::from_le_bytes(chunk.try_into().unwrap());
        if word & NON_ASCII_MASK != 0 {
            return false;
        }
        o[0].write(chunk[0]);
        o[1].write(chunk[2]);
        o[2].write(chunk[4]);
        o[3].write(chunk[6]);
    }
    for (unit, o) in chunks.remainder().chunks_exact(2).zip(out.into_remainder()) {
        if unit[0] >= 0x80 || unit[1] != 0 {
            return false;
        }
        o.write(unit[0]);
    }
    true
}

/// True when `bytes` is UTF-16LE in which every code unit is ASCII.
#[cfg(test)]
fn is_ascii_utf16(bytes: &[u8]) -> bool {
    bytes.len() % 2 == 0 && narrow_ascii_utf16(bytes, &mut vec![MaybeUninit::uninit(); bytes.len() / 2])
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
    use super::{is_ascii_utf16, narrow_ascii_utf16};

    #[test]
    fn ascii_utf16_detection() {
        assert!(is_ascii_utf16(&[]));
        assert!(is_ascii_utf16(&[b'a', 0, b'Z', 0]));
        assert!(!is_ascii_utf16(&[0xE9, 0]), "U+00E9 is not ASCII");
        assert!(!is_ascii_utf16(&[b'a', 1]), "U+0161 is not ASCII");
        assert!(!is_ascii_utf16(&[b'a']), "odd length is malformed");
    }

    fn utf16(s: &[u16]) -> Vec<u8> {
        s.iter().flat_map(|u| u.to_le_bytes()).collect()
    }

    #[test]
    fn ascii_utf16_word_loop_edges() {
        // Every length around the 4-unit word boundary, with the bad unit at
        // every position and with each kind of non-ASCII value.
        for len in 0..=13usize {
            let ok: Vec<u16> = (0..len).map(|i| 0x20 + i as u16).collect();
            let mut dst = vec![std::mem::MaybeUninit::<u8>::uninit(); len];
            assert!(narrow_ascii_utf16(&utf16(&ok), &mut dst));
            // SAFETY: a true result initialised all of `dst`.
            let dst: Vec<u8> = dst.iter().map(|b| unsafe { b.assume_init() }).collect();
            assert_eq!(dst, ok.iter().map(|u| *u as u8).collect::<Vec<_>>());
            for pos in 0..len {
                for bad in [0x80u16, 0xE9, 0x100, 0x161, 0xFF00, 0xD800, 0xDC00] {
                    let mut units = ok.clone();
                    units[pos] = bad;
                    assert!(!is_ascii_utf16(&utf16(&units)), "len {len} pos {pos} unit {bad:#x}");
                }
            }
        }
        assert!(is_ascii_utf16(&utf16(&[0, 0x7F, 0, 0x7F, 0x7F])), "NUL and DEL are ASCII");
        assert!(!is_ascii_utf16(&[b'a', 0, b'b', 0, b'c']), "odd length with a word");
    }
}
