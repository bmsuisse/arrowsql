//! `RowBuffer`: the GIL-free decode sink for the Python-object fetch path.
//!
//! mssql-tds decodes TDS rows by calling `RowWriter` methods (the seam every
//! sink implements). `RowBuffer` stores each value as a compact `Cell`; text
//! and binary payloads go into one shared byte arena, so a fetch of N rows
//! performs O(1) allocations instead of one per string. The buffer is filled
//! with the GIL released and the connection lock held, then turned into
//! Python objects by `decode::python` with the GIL held and no lock held.
//!
//! A different sink (e.g. the Arrow builder sink, see `arrow.rs`) implements
//! the same `RowWriter` trait and plugs into `statement::read_rows_into`.

use std::borrow::Cow;

use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::datatypes::sql_json::SqlJson;
use super::python::narrow_ascii_utf16;
use mssql_tds::datatypes::sql_string::{EncodingType, SqlString};
use mssql_tds::datatypes::sql_vector::SqlVector;
use uuid::Uuid;

/// Byte range inside `RowBuffer::arena`.
#[derive(Debug, Clone, Copy)]
pub struct Span {
    pub start: usize,
    pub len: usize,
}

/// One decoded value. Temporal values are normalised to
/// (days since 0001-01-01, 100ns ticks since midnight).
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Real(f32),
    /// Valid UTF-8 in the arena.
    Utf8(Span),
    /// UTF-16LE in the arena.
    Utf16(Span),
    Bytes(Span),
    Decimal(DecimalParts),
    /// money / smallmoney, scaled by 10^4.
    Money(i64),
    Date(u32),
    Time { ticks: u64 },
    DateTime { days: u32, ticks: u64 },
    DateTimeOffset { days: u32, ticks: u64, offset_minutes: i16 },
    Uuid(Uuid),
}

/// 1900-01-01 as days since 0001-01-01.
const DAYS_TO_1900: u32 = 693_595;
const TICKS_PER_SECOND: u64 = 10_000_000;

#[derive(Default)]
pub struct RowBuffer {
    pub cells: Vec<Cell>,
    pub arena: Vec<u8>,
    pub rows: usize,
    /// Rows the fetch is expected to read; sizes the arena once for large values.
    pub row_hint: usize,
    /// Byte budget of the fetch (0 = none); caps what is reserved for those rows.
    pub byte_budget: usize,
}

/// Small buffers kept per thread so single-row and short fetches (fetchone,
/// partial fetchmany) reuse allocations instead of making two per call.
/// Values from this size on are stored narrowed (ASCII UTF-16) and reserve
/// arena room for the whole fetch.
const NARROW_ON_PUSH_MIN: usize = 16 << 10;
const LOB_RESERVE_MIN: usize = 16 << 10;
const LOB_RESERVE_MAX: usize = 1 << 30;
const POOL_DEPTH: usize = 4;
const POOL_MAX_CELLS: usize = 1024;
const POOL_MAX_ARENA: usize = 16 << 10;

thread_local! {
    static POOL: std::cell::RefCell<Vec<(Vec<Cell>, Vec<u8>)>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl Drop for RowBuffer {
    fn drop(&mut self) {
        if self.cells.capacity() == 0
            || self.cells.capacity() > POOL_MAX_CELLS
            || self.arena.capacity() > POOL_MAX_ARENA
        {
            return;
        }
        let mut cells = std::mem::take(&mut self.cells);
        let mut arena = std::mem::take(&mut self.arena);
        cells.clear();
        arena.clear();
        let _ = POOL.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < POOL_DEPTH {
                pool.push((cells, arena));
            }
        });
    }
}

impl RowBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.arena.clear();
        self.rows = 0;
    }

    /// Like [`clear`](Self::clear) for a buffer that will sit idle: a large
    /// allocation is freed instead of kept at its high-water mark.
    pub fn clear_and_trim(&mut self) {
        const KEEP_CELLS: usize = 16 * 1024;
        const KEEP_ARENA: usize = 1 << 20;
        if self.cells.capacity() > KEEP_CELLS || self.arena.capacity() > KEEP_ARENA {
            *self = RowBuffer::new();
        } else {
            self.clear();
        }
    }

    /// Approximate heap use; fetch loops cap batches on it.
    pub fn byte_size(&self) -> usize {
        self.cells.len() * std::mem::size_of::<Cell>() + self.arena.len()
    }

    pub fn bytes(&self, span: Span) -> &[u8] {
        &self.arena[span.start..span.start + span.len]
    }

    /// Pad the text of fixed-width char/nchar columns (`(column, chars)`) with
    /// spaces up to their declared width, as msodbcsql does. SQL Server sends
    /// a shorter value when ANSI_PADDING was OFF at CREATE TABLE time.
    pub fn pad_fixed_text(&mut self, ncols: usize, widths: &[(usize, usize)]) {
        if ncols == 0 {
            return;
        }
        for row in 0..self.rows {
            for &(col, width) in widths {
                let index = row * ncols + col;
                let (span, wide) = match self.cells.get(index) {
                    Some(Cell::Utf8(span)) => (*span, false),
                    Some(Cell::Utf16(span)) => (*span, true),
                    _ => continue,
                };
                // char(n) counts bytes in the column's code page, which a decoded
                // non-ASCII value no longer shows (UTF-8 and DBCS collations), so only
                // ASCII values are padded; nchar(n) counts UTF-16 units, always exact.
                let bytes = &self.arena[span.start..span.start + span.len];
                let chars = if wide { bytes.len() / 2 } else { bytes.len() };
                if chars >= width || (!wide && !bytes.is_ascii()) {
                    continue;
                }
                let start = self.arena.len();
                self.arena.extend_from_within(span.start..span.start + span.len);
                if wide {
                    let end = self.arena.len() + 2 * (width - chars);
                    self.arena.resize(end, 0);
                    for unit in self.arena[start + span.len..].chunks_exact_mut(2) {
                        unit[0] = b' ';
                    }
                } else {
                    let end = self.arena.len() + (width - chars);
                    self.arena.resize(end, b' ');
                }
                let padded = Span { start, len: self.arena.len() - start };
                self.cells[index] = if wide { Cell::Utf16(padded) } else { Cell::Utf8(padded) };
            }
        }
    }

    /// Text value of a cell, for small internal queries (not the hot path).
    pub fn text(&self, cell: &Cell) -> Option<String> {
        match cell {
            Cell::Utf8(span) => Some(String::from_utf8_lossy(self.bytes(*span)).into_owned()),
            Cell::Utf16(span) => {
                let units: Vec<u16> = self
                    .bytes(*span)
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect();
                Some(String::from_utf16_lossy(&units))
            }
            _ => None,
        }
    }

    /// Room for `len` more arena bytes. A large value sizes the arena once for
    /// the rows still to come: doubling a huge arena copies it each time and
    /// faults its pages in twice.
    fn reserve_lob(&mut self, len: usize) {
        if len >= LOB_RESERVE_MIN && self.arena.capacity() - self.arena.len() < len {
            // Rows that fit the rest of the fetch budget, but always this value.
            let fits = self.byte_budget.saturating_sub(self.byte_size()) / len;
            let more = self.row_hint.saturating_sub(self.rows).min(fits).clamp(1, LOB_RESERVE_MAX / len + 1);
            self.arena.reserve_exact(len.saturating_mul(more));
        }
    }

    fn push_span(&mut self, bytes: &[u8]) -> Span {
        let start = self.arena.len();
        self.reserve_lob(bytes.len());
        self.arena.extend_from_slice(bytes);
        Span { start, len: bytes.len() }
    }

    fn push_text(&mut self, bytes: &[u8], encoding: EncodingType) -> Cell {
        match encoding {
            EncodingType::Utf16 if bytes.len() >= NARROW_ON_PUSH_MIN && bytes.len() % 2 == 0 => {
                // Large ASCII values are stored one byte per character: half the
                // arena to fill and to fault in.
                let start = self.arena.len();
                let len = bytes.len() / 2;
                self.reserve_lob(len);
                self.arena.reserve(len);
                let narrowed = narrow_ascii_utf16(bytes, &mut self.arena.spare_capacity_mut()[..len]);
                if narrowed {
                    // SAFETY: `narrow_ascii_utf16` initialised all `len` bytes after `start`.
                    unsafe { self.arena.set_len(start + len) };
                    Cell::Utf8(Span { start, len })
                } else {
                    Cell::Utf16(self.push_span(bytes))
                }
            }
            EncodingType::Utf16 => Cell::Utf16(self.push_span(bytes)),
            EncodingType::Utf8 => match std::str::from_utf8(bytes) {
                Ok(_) => Cell::Utf8(self.push_span(bytes)),
                Err(_) => {
                    let s = String::from_utf8_lossy(bytes).into_owned();
                    Cell::Utf8(self.push_span(s.as_bytes()))
                }
            },
            // Code-page text: ASCII is identical in every SQL Server code page.
            _ if bytes.is_ascii() => Cell::Utf8(self.push_span(bytes)),
            other => {
                let s = SqlString::decode(bytes, other);
                Cell::Utf8(self.push_span(s.as_bytes()))
            }
        }
    }

    /// Copy rows `[from, from+count)` of an `ncols`-wide buffer into a new one.
    ///
    /// Rows are written in order, so the spans of a row range normally sit in
    /// one contiguous arena stretch: it is copied once and the spans rebased,
    /// instead of one arena push per text cell. Padded copies of fixed-width
    /// text (`pad_fixed_text`) live at the arena tail, which stretches that
    /// range over most of the arena; then each span is copied on its own.
    pub fn slice_rows(&self, ncols: usize, from: usize, count: usize) -> RowBuffer {
        let src = &self.cells[from * ncols..(from + count) * ncols];
        let (mut lo, mut hi, mut used) = (usize::MAX, 0usize, 0usize);
        for cell in src {
            if let Cell::Utf8(s) | Cell::Utf16(s) | Cell::Bytes(s) = cell {
                lo = lo.min(s.start);
                hi = hi.max(s.start + s.len);
                used += s.len;
            }
        }
        let mut out = RowBuffer::new();
        if src.len() <= POOL_MAX_CELLS
            && let Some((cells, arena)) = POOL.try_with(|pool| pool.borrow_mut().pop()).ok().flatten()
        {
            (out.cells, out.arena) = (cells, arena);
        }
        out.cells.reserve(src.len());
        if lo < hi && hi - lo > used.saturating_mul(2) + 256 {
            out.arena.reserve(used);
            let mut copy = |s: &Span| {
                let start = out.arena.len();
                out.arena.extend_from_slice(&self.arena[s.start..s.start + s.len]);
                Span { start, len: s.len }
            };
            out.cells.extend(src.iter().map(|cell| match cell {
                Cell::Utf8(s) => Cell::Utf8(copy(s)),
                Cell::Utf16(s) => Cell::Utf16(copy(s)),
                Cell::Bytes(s) => Cell::Bytes(copy(s)),
                other => other.clone(),
            }));
        } else {
            if lo < hi {
                out.arena.extend_from_slice(&self.arena[lo..hi]);
            }
            let rebase = |s: &Span| Span { start: s.start - lo, len: s.len };
            out.cells.extend(src.iter().map(|cell| match cell {
                Cell::Utf8(s) => Cell::Utf8(rebase(s)),
                Cell::Utf16(s) => Cell::Utf16(rebase(s)),
                Cell::Bytes(s) => Cell::Bytes(rebase(s)),
                other => other.clone(),
            }));
        }
        out.rows = count;
        out
    }
}

impl RowWriter for RowBuffer {
    fn write_null(&mut self, _col: usize) {
        self.cells.push(Cell::Null);
    }
    fn write_bool(&mut self, _col: usize, val: bool) {
        self.cells.push(Cell::Bool(val));
    }
    fn write_u8(&mut self, _col: usize, val: u8) {
        self.cells.push(Cell::Int(val.into()));
    }
    fn write_i16(&mut self, _col: usize, val: i16) {
        self.cells.push(Cell::Int(val.into()));
    }
    fn write_i32(&mut self, _col: usize, val: i32) {
        self.cells.push(Cell::Int(val.into()));
    }
    fn write_i64(&mut self, _col: usize, val: i64) {
        self.cells.push(Cell::Int(val));
    }
    fn write_f32(&mut self, _col: usize, val: f32) {
        self.cells.push(Cell::Real(val));
    }
    fn write_f64(&mut self, _col: usize, val: f64) {
        self.cells.push(Cell::Float(val));
    }
    fn write_string(&mut self, _col: usize, bytes: Cow<'_, [u8]>, encoding_type: EncodingType) {
        let cell = self.push_text(&bytes, encoding_type);
        self.cells.push(cell);
    }
    fn write_bytes(&mut self, _col: usize, bytes: Cow<'_, [u8]>) {
        let span = self.push_span(&bytes);
        self.cells.push(Cell::Bytes(span));
    }
    fn write_decimal(&mut self, _col: usize, val: DecimalParts) {
        self.cells.push(Cell::Decimal(val));
    }
    fn write_numeric(&mut self, _col: usize, val: DecimalParts) {
        self.cells.push(Cell::Decimal(val));
    }
    fn write_date(&mut self, _col: usize, val: SqlDate) {
        self.cells.push(Cell::Date(val.get_days()));
    }
    fn write_time(&mut self, _col: usize, val: SqlTime) {
        self.cells.push(Cell::Time { ticks: val.time_nanoseconds });
    }
    fn write_datetime(&mut self, _col: usize, val: SqlDateTime) {
        // datetime counts 1/300 s; msodbcsql rounds to whole milliseconds.
        let millis = (u64::from(val.time) * 10 + 1) / 3;
        self.cells.push(Cell::DateTime {
            days: (DAYS_TO_1900 as i64 + i64::from(val.days)) as u32,
            ticks: millis * 10_000,
        });
    }
    fn write_smalldatetime(&mut self, _col: usize, val: SqlSmallDateTime) {
        self.cells.push(Cell::DateTime {
            days: DAYS_TO_1900 + u32::from(val.days),
            ticks: u64::from(val.time) * 60 * TICKS_PER_SECOND,
        });
    }
    fn write_datetime2(&mut self, _col: usize, val: SqlDateTime2) {
        self.cells.push(Cell::DateTime { days: val.days, ticks: val.time.time_nanoseconds });
    }
    fn write_datetimeoffset(&mut self, _col: usize, val: SqlDateTimeOffset) {
        self.cells.push(Cell::DateTimeOffset {
            days: val.datetime2.days,
            ticks: val.datetime2.time.time_nanoseconds,
            offset_minutes: val.offset,
        });
    }
    fn write_money(&mut self, _col: usize, val: SqlMoney) {
        let lsb = (val.lsb_part as i64) & 0xFFFF_FFFF;
        self.cells.push(Cell::Money(lsb | ((val.msb_part as i64) << 32)));
    }
    fn write_smallmoney(&mut self, _col: usize, val: SqlSmallMoney) {
        self.cells.push(Cell::Money(val.int_val.into()));
    }
    fn write_uuid(&mut self, _col: usize, val: Uuid) {
        self.cells.push(Cell::Uuid(val));
    }
    fn write_xml(&mut self, _col: usize, val: SqlXml) {
        let span = self.push_span(&val.bytes);
        self.cells.push(Cell::Utf16(span));
    }
    fn write_json(&mut self, _col: usize, val: SqlJson) {
        let cell = self.push_text(&val.bytes, EncodingType::Utf8);
        self.cells.push(cell);
    }
    fn write_vector(&mut self, _col: usize, val: SqlVector) {
        // TODO(decode/vector): upstream returns list[float]; text form for now.
        let text = match val.as_f32() {
            Some(values) => format!("{values:?}"),
            None => format!("{val:?}"),
        };
        let span = self.push_span(text.as_bytes());
        self.cells.push(Cell::Utf8(span));
    }
    fn end_row(&mut self) {
        self.rows += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lob_reservation_respects_fetch_budget() {
        let value = vec![b'a'; 128 << 10];
        let budget = 16 << 20;
        let mut buf = RowBuffer::new();
        buf.row_hint = 8192;
        buf.byte_budget = budget;
        buf.push_span(&value);
        assert!(buf.arena.capacity() <= budget, "reserved {} bytes", buf.arena.capacity());
        assert!(buf.arena.capacity() >= 64 * value.len(), "still reserves for the batch");
    }

    #[test]
    fn single_value_over_budget_is_still_stored() {
        let value = vec![7u8; 2 << 20];
        let mut buf = RowBuffer::new();
        buf.row_hint = 8192;
        buf.byte_budget = 1 << 20;
        let span = buf.push_span(&value);
        assert_eq!(buf.bytes(span), &value[..]);
        assert!(buf.arena.capacity() < 2 * value.len());
    }

    #[test]
    fn narrow_on_push_keeps_ascii_and_falls_back() {
        let ascii: Vec<u8> =
            (0..NARROW_ON_PUSH_MIN).map(|i| if i % 2 == 0 { b'a' + (i % 20) as u8 } else { 0 }).collect();
        let mut buf = RowBuffer::new();
        match buf.push_text(&ascii, EncodingType::Utf16) {
            Cell::Utf8(span) => assert_eq!(span.len, ascii.len() / 2),
            _ => panic!("ASCII not narrowed"),
        }
        let mut wide = ascii.clone();
        wide[1] = 1;
        assert!(matches!(buf.push_text(&wide, EncodingType::Utf16), Cell::Utf16(_)));
    }
}
