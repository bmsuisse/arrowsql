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

    /// Approximate heap use; fetch loops cap batches on it.
    pub fn byte_size(&self) -> usize {
        self.cells.len() * std::mem::size_of::<Cell>() + self.arena.len()
    }

    pub fn bytes(&self, span: Span) -> &[u8] {
        &self.arena[span.start..span.start + span.len]
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

    fn push_span(&mut self, bytes: &[u8]) -> Span {
        let start = self.arena.len();
        self.arena.extend_from_slice(bytes);
        Span { start, len: bytes.len() }
    }

    fn push_text(&mut self, bytes: &[u8], encoding: EncodingType) -> Cell {
        match encoding {
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

    /// Move rows `[from, from+count)` of an `ncols`-wide buffer into a new one.
    pub fn slice_rows(&self, ncols: usize, from: usize, count: usize) -> RowBuffer {
        let mut out = RowBuffer::new();
        let cells = &self.cells[from * ncols..(from + count) * ncols];
        out.cells.reserve(cells.len());
        for cell in cells {
            let moved = match cell {
                Cell::Utf8(s) => Cell::Utf8(out.push_span(self.bytes(*s))),
                Cell::Utf16(s) => Cell::Utf16(out.push_span(self.bytes(*s))),
                Cell::Bytes(s) => Cell::Bytes(out.push_span(self.bytes(*s))),
                other => other.clone(),
            };
            out.cells.push(moved);
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
