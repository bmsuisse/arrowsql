//! `ArrowSink`: decodes TDS rows straight into Arrow column buffers.
//!
//! mssql-tds calls the `RowWriter` methods while it parses a row; each value
//! lands in its column's buffer (plain `Vec`s: values, offsets, validity
//! bits) with no per-cell allocation and no Python objects. Text is converted
//! to UTF-8 in one pass straight into the column's data buffer. `finish`
//! wraps the buffers as arrow-rs arrays without copying.
//!
//! Rows already decoded as `RowBuffer` cells (read ahead by `fetchone`, or
//! drained for MARS emulation) are appended through `push_buffered`, so Arrow
//! and row fetches can be interleaved on one result.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Time64NanosecondType, UInt8Type,
};
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, LargeBinaryArray, LargeStringArray, PrimitiveArray,
    RecordBatch, RecordBatchOptions, TimestampMicrosecondArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::EncodingType;
use mssql_tds::datatypes::sql_vector::SqlVector;
use uuid::Uuid;

use super::plan::{Kind, Plan};
use crate::agent::fmt;
use crate::decode::RowBuffer;
use crate::decode::buffer::Cell;
use crate::statement::BatchSink;

/// Days from 0001-01-01 to 1970-01-01.
const UNIX_EPOCH_DAYS: i64 = 719_162;
/// 1900-01-01 as days since 0001-01-01 (datetime / smalldatetime epoch).
const DAYS_TO_1900: i64 = 693_595;
const MICROS_PER_DAY: i64 = 86_400_000_000;

const fn pow10_table() -> [i128; 39] {
    let mut table = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
}
static POW10: [i128; 39] = pow10_table();

/// Growable bitmap (Arrow LSB bit order).
#[derive(Default)]
struct Bits {
    bytes: Vec<u8>,
    len: usize,
}

impl Bits {
    fn with_capacity(bits: usize) -> Self {
        Bits { bytes: Vec::with_capacity(bits.div_ceil(8)), len: 0 }
    }

    #[inline]
    fn push(&mut self, bit: bool) {
        if self.len % 8 == 0 {
            self.bytes.push(0);
        }
        if bit {
            *self.bytes.last_mut().unwrap() |= 1 << (self.len % 8);
        }
        self.len += 1;
    }

    fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        self.len = len;
        self.bytes.truncate(len.div_ceil(8));
        if len % 8 != 0 {
            *self.bytes.last_mut().unwrap() &= (1u8 << (len % 8)) - 1;
        }
    }

    fn count_zeros(&self) -> usize {
        let ones: usize = self.bytes.iter().map(|b| b.count_ones() as usize).sum();
        self.len - ones
    }

    fn into_buffer(self) -> BooleanBuffer {
        BooleanBuffer::new(Buffer::from_vec(self.bytes), 0, self.len)
    }
}

enum Values {
    UInt8(Vec<u8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Bool(Bits),
    Decimal(Vec<i128>, i8),
    /// Timestamp (micros), Date32 and Time64 share i64/i32 storage below.
    Timestamp(Vec<i64>),
    Date32(Vec<i32>),
    Time64(Vec<i64>),
    Var { offsets: Vec<i64>, data: Vec<u8> },
}

struct Column {
    kind: Kind,
    values: Values,
    valid: Bits,
    /// A `sql_variant` column (text storage): binary values show as hex.
    variant: bool,
}

impl Column {
    fn new(kind: Kind, rows: usize, width: usize) -> Column {
        let values = match kind {
            Kind::UInt8 => Values::UInt8(Vec::with_capacity(rows)),
            Kind::Int16 => Values::Int16(Vec::with_capacity(rows)),
            Kind::Int32 => Values::Int32(Vec::with_capacity(rows)),
            Kind::Int64 => Values::Int64(Vec::with_capacity(rows)),
            Kind::Float32 => Values::Float32(Vec::with_capacity(rows)),
            Kind::Float64 => Values::Float64(Vec::with_capacity(rows)),
            Kind::Bool => Values::Bool(Bits::with_capacity(rows)),
            Kind::Decimal { scale, .. } => Values::Decimal(Vec::with_capacity(rows), scale),
            Kind::Timestamp { .. } => Values::Timestamp(Vec::with_capacity(rows)),
            Kind::Date32 => Values::Date32(Vec::with_capacity(rows)),
            Kind::Time64Ns => Values::Time64(Vec::with_capacity(rows)),
            Kind::Text | Kind::Binary => {
                let mut offsets = Vec::with_capacity(rows + 1);
                offsets.push(0);
                Values::Var { offsets, data: Vec::with_capacity(rows.saturating_mul(width)) }
            }
        };
        Column { kind, values, valid: Bits::with_capacity(rows), variant: false }
    }

    fn len(&self) -> usize {
        self.valid.len
    }

    #[inline]
    fn null(&mut self) {
        match &mut self.values {
            Values::UInt8(v) => v.push(0),
            Values::Int16(v) => v.push(0),
            Values::Int32(v) => v.push(0),
            Values::Int64(v) => v.push(0),
            Values::Float32(v) => v.push(0.0),
            Values::Float64(v) => v.push(0.0),
            Values::Bool(v) => v.push(false),
            Values::Decimal(v, _) => v.push(0),
            Values::Timestamp(v) | Values::Time64(v) => v.push(0),
            Values::Date32(v) => v.push(0),
            Values::Var { offsets, data } => offsets.push(data.len() as i64),
        }
        self.valid.push(false);
    }

    #[inline]
    fn int(&mut self, value: i64) {
        match &mut self.values {
            Values::UInt8(v) => v.push(value as u8),
            Values::Int16(v) => v.push(value as i16),
            Values::Int32(v) => v.push(value as i32),
            Values::Int64(v) => v.push(value),
            Values::Float32(v) => v.push(value as f32),
            Values::Float64(v) => v.push(value as f64),
            Values::Bool(v) => v.push(value != 0),
            Values::Decimal(v, scale) => v.push(i128::from(value) * POW10[*scale as usize]),
            Values::Var { .. } => return self.text(value.to_string().as_bytes()),
            _ => return self.null(),
        }
        self.valid.push(true);
    }

    #[inline]
    fn float(&mut self, value: f64) {
        match &mut self.values {
            Values::Float32(v) => v.push(value as f32),
            Values::Float64(v) => v.push(value),
            Values::Var { .. } => return self.text(value.to_string().as_bytes()),
            _ => return self.int(value as i64),
        }
        self.valid.push(true);
    }

    /// Decimal `magnitude` scaled by 10^`scale`, rescaled to the column's scale.
    #[inline]
    fn decimal(&mut self, negative: bool, magnitude: u128, scale: i8) {
        if let Values::Decimal(v, column_scale) = &mut self.values {
            let mut value = magnitude as i128;
            if *column_scale > scale {
                value *= POW10[(*column_scale - scale) as usize];
            } else if *column_scale < scale {
                value /= POW10[(scale - *column_scale) as usize];
            }
            v.push(if negative { -value } else { value });
            self.valid.push(true);
            return;
        }
        if matches!(self.values, Values::Var { .. }) {
            // sql_variant decimal: exact text, not just the whole part.
            let signed = if negative { -(magnitude as i128) } else { magnitude as i128 };
            let mut text = String::new();
            fmt::write_decimal(&mut text, signed, scale.max(0) as u32);
            return self.text(text.as_bytes());
        }
        let digits = POW10[scale.max(0) as usize] as u128;
        let whole = (magnitude / digits) as i64;
        if matches!(self.values, Values::Float32(_) | Values::Float64(_)) {
            let value = magnitude as f64 / digits as f64;
            self.float(if negative { -value } else { value });
        } else {
            self.int(if negative { -whole } else { whole });
        }
    }

    /// datetime family: days since 0001-01-01 + micros since midnight.
    #[inline]
    fn timestamp(&mut self, days: i64, micros: i64) {
        match &mut self.values {
            Values::Timestamp(v) => v.push((days - UNIX_EPOCH_DAYS) * MICROS_PER_DAY + micros),
            Values::Date32(v) => v.push((days - UNIX_EPOCH_DAYS) as i32),
            Values::Time64(v) => v.push(micros * 1000),
            Values::Var { .. } => {
                // sql_variant datetime: the shortest exact text.
                let mut text = String::new();
                fmt::write_date(&mut text, days - UNIX_EPOCH_DAYS);
                text.push(' ');
                fmt::write_time_trimmed(&mut text, micros * 1000);
                return self.text(text.as_bytes());
            }
            _ => return self.null(),
        }
        self.valid.push(true);
    }

    #[inline]
    fn time(&mut self, ticks: u64) {
        match &mut self.values {
            Values::Time64(v) => v.push(ticks as i64 * 100),
            Values::Var { .. } => {
                let mut text = String::new();
                fmt::write_time_trimmed(&mut text, ticks as i64 * 100);
                return self.text(text.as_bytes());
            }
            _ => return self.null(),
        }
        self.valid.push(true);
    }

    #[inline]
    fn date(&mut self, days: u32) {
        match &mut self.values {
            Values::Date32(v) => v.push((i64::from(days) - UNIX_EPOCH_DAYS) as i32),
            Values::Timestamp(v) => v.push((i64::from(days) - UNIX_EPOCH_DAYS) * MICROS_PER_DAY),
            Values::Var { .. } => {
                let mut text = String::new();
                fmt::write_date(&mut text, i64::from(days) - UNIX_EPOCH_DAYS);
                return self.text(text.as_bytes());
            }
            _ => return self.null(),
        }
        self.valid.push(true);
    }

    /// Already-valid UTF-8 (or raw bytes for a binary column).
    #[inline]
    fn text(&mut self, bytes: &[u8]) {
        match &mut self.values {
            Values::Var { offsets, data } => {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as i64);
                self.valid.push(true);
            }
            _ => self.null(),
        }
    }

    /// Raw bytes: kept as-is in a binary column, made valid UTF-8 in a text one.
    #[inline]
    fn bytes(&mut self, bytes: &[u8]) {
        if self.variant {
            let mut text = String::new();
            fmt::write_hex(&mut text, bytes);
            return self.text(text.as_bytes());
        }
        if self.kind == Kind::Text && std::str::from_utf8(bytes).is_err() {
            return self.text(String::from_utf8_lossy(bytes).as_bytes());
        }
        self.text(bytes)
    }

    /// UTF-16LE -> UTF-8 into the data buffer (binary columns keep the bytes).
    #[inline]
    fn utf16(&mut self, bytes: &[u8]) {
        if self.kind == Kind::Binary {
            return self.text(bytes);
        }
        let Values::Var { offsets, data } = &mut self.values else { return self.null() };
        utf16le_to_utf8(bytes, data);
        offsets.push(data.len() as i64);
        self.valid.push(true);
    }

    fn encoded(&mut self, bytes: &[u8], encoding: EncodingType) {
        if self.kind == Kind::Binary {
            return self.text(bytes);
        }
        let Values::Var { offsets, data } = &mut self.values else { return self.null() };
        match encoding {
            EncodingType::Utf16 => utf16le_to_utf8(bytes, data),
            _ if bytes.is_ascii() => data.extend_from_slice(bytes),
            EncodingType::Utf8 => match std::str::from_utf8(bytes) {
                Ok(_) => data.extend_from_slice(bytes),
                Err(_) => data.extend_from_slice(String::from_utf8_lossy(bytes).as_bytes()),
            },
            other => match other.encoding() {
                Some(encoding) => decode_into(encoding, bytes, data),
                None => data.extend_from_slice(String::from_utf8_lossy(bytes).as_bytes()),
            },
        }
        offsets.push(data.len() as i64);
        self.valid.push(true);
    }

    fn uuid(&mut self, value: Uuid) {
        if self.kind == Kind::Binary {
            return self.text(value.as_bytes());
        }
        let mut buf = [0u8; 36];
        let text = value.hyphenated().encode_upper(&mut buf);
        self.text(text.as_bytes());
    }

    fn truncate(&mut self, len: usize) {
        if self.len() <= len {
            return;
        }
        match &mut self.values {
            Values::UInt8(v) => v.truncate(len),
            Values::Int16(v) => v.truncate(len),
            Values::Int32(v) => v.truncate(len),
            Values::Int64(v) => v.truncate(len),
            Values::Float32(v) => v.truncate(len),
            Values::Float64(v) => v.truncate(len),
            Values::Bool(v) => v.truncate(len),
            Values::Decimal(v, _) => v.truncate(len),
            Values::Timestamp(v) | Values::Time64(v) => v.truncate(len),
            Values::Date32(v) => v.truncate(len),
            Values::Var { offsets, data } => {
                offsets.truncate(len + 1);
                data.truncate(*offsets.last().unwrap() as usize);
            }
        }
        self.valid.truncate(len);
    }

    fn byte_size(&self) -> usize {
        match &self.values {
            Values::Var { offsets, data } => offsets.len() * 8 + data.len(),
            Values::Decimal(v, _) => v.len() * 16,
            _ => self.len() * 8,
        }
    }

    fn finish(self) -> ArrayRef {
        let nulls = if self.valid.count_zeros() == 0 {
            None
        } else {
            Some(NullBuffer::new(self.valid.into_buffer()))
        };
        fn prim<T: arrow_array::types::ArrowPrimitiveType>(
            values: Vec<T::Native>,
            nulls: Option<NullBuffer>,
        ) -> PrimitiveArray<T> {
            PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls)
        }
        match (self.values, self.kind) {
            (Values::UInt8(v), _) => Arc::new(prim::<UInt8Type>(v, nulls)),
            (Values::Int16(v), _) => Arc::new(prim::<Int16Type>(v, nulls)),
            (Values::Int32(v), _) => Arc::new(prim::<Int32Type>(v, nulls)),
            (Values::Int64(v), _) => Arc::new(prim::<Int64Type>(v, nulls)),
            (Values::Float32(v), _) => Arc::new(prim::<Float32Type>(v, nulls)),
            (Values::Float64(v), _) => Arc::new(prim::<Float64Type>(v, nulls)),
            (Values::Bool(v), _) => Arc::new(BooleanArray::new(v.into_buffer(), nulls)),
            (Values::Decimal(v, _), Kind::Decimal { precision, scale }) => Arc::new(
                Decimal128Array::new(ScalarBuffer::from(v), nulls)
                    .with_precision_and_scale(precision, scale)
                    .expect("precision/scale validated by Plan"),
            ),
            (Values::Timestamp(v), Kind::Timestamp { utc }) => Arc::new(
                TimestampMicrosecondArray::new(ScalarBuffer::from(v), nulls)
                    .with_timezone_opt(utc.then_some("+00:00")),
            ),
            (Values::Date32(v), _) => Arc::new(prim::<Date32Type>(v, nulls)),
            (Values::Time64(v), _) => Arc::new(prim::<Time64NanosecondType>(v, nulls)),
            (Values::Var { offsets, data }, Kind::Binary) => Arc::new(LargeBinaryArray::new(
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                Buffer::from_vec(data),
                nulls,
            )),
            (Values::Var { offsets, data }, _) => {
                // SAFETY: offsets are monotonic (pushed after each append) and
                // every append writes valid UTF-8 (converted or validated).
                Arc::new(unsafe {
                    LargeStringArray::new_unchecked(
                        OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)),
                        Buffer::from_vec(data),
                        nulls,
                    )
                })
            }
            _ => unreachable!("column storage always matches its kind"),
        }
    }
}

/// UTF-16LE bytes -> UTF-8 appended to `out`, lone surrogates replaced.
#[inline]
fn utf16le_to_utf8(bytes: &[u8], out: &mut Vec<u8>) {
    // ASCII fast path: every unit < 0x80 means one byte per unit.
    let ascii = bytes.chunks_exact(2).fold(0u8, |acc, u| acc | (u[0] & 0x80) | u[1]) == 0;
    if ascii {
        out.extend(bytes.chunks_exact(2).map(|u| u[0]));
        return;
    }
    decode_into(encoding_rs::UTF_16LE, bytes, out);
}

fn decode_into(encoding: &'static encoding_rs::Encoding, bytes: &[u8], out: &mut Vec<u8>) {
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let max = decoder.max_utf8_buffer_length(bytes.len()).unwrap_or(bytes.len() * 3 + 16);
    let start = out.len();
    out.resize(start + max, 0);
    let (_, _, written, _) = decoder.decode_to_utf8(bytes, &mut out[start..], true);
    out.truncate(start + written);
}

/// One Arrow record batch being decoded.
pub struct ArrowSink {
    plan: Plan,
    columns: Vec<Column>,
    rows: usize,
}

impl ArrowSink {
    pub fn new(plan: &Plan, capacity: usize) -> ArrowSink {
        let rows = capacity.min(1 << 16);
        let columns = plan
            .kinds
            .iter()
            .zip(&plan.widths)
            .zip(&plan.variants)
            .map(|((kind, width), variant)| {
                let mut column = Column::new(*kind, rows, *width);
                column.variant = *variant;
                column
            })
            .collect();
        ArrowSink { plan: plan.clone(), columns, rows: 0 }
    }

    pub fn finish(self) -> Result<RecordBatch, String> {
        let rows = self.rows;
        let arrays: Vec<ArrayRef> = self.columns.into_iter().map(Column::finish).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(self.plan.schema.clone(), arrays, &options)
            .map_err(|e| e.to_string())
    }
}

impl BatchSink for ArrowSink {
    fn rows(&self) -> usize {
        self.rows
    }

    fn byte_size(&self) -> usize {
        self.columns.iter().map(Column::byte_size).sum()
    }

    fn push_buffered(&mut self, buffer: &RowBuffer, ncols: usize) {
        if ncols == 0 {
            return;
        }
        for row in buffer.cells.chunks_exact(ncols) {
            for (column, cell) in self.columns.iter_mut().zip(row) {
                match cell {
                    Cell::Null => column.null(),
                    Cell::Bool(v) => column.int(i64::from(*v)),
                    Cell::Int(v) => column.int(*v),
                    Cell::Float(v) => column.float(*v),
                    Cell::Real(v) => column.float(f64::from(*v)),
                    Cell::Utf8(span) => column.text(buffer.bytes(*span)),
                    Cell::Utf16(span) => column.utf16(buffer.bytes(*span)),
                    Cell::Bytes(span) => column.bytes(buffer.bytes(*span)),
                    Cell::Decimal(parts) => {
                        column.decimal(!parts.is_positive, parts.magnitude(), parts.scale as i8)
                    }
                    Cell::Money(v) => column.decimal(*v < 0, u128::from(v.unsigned_abs()), 4),
                    Cell::Date(days) => column.date(*days),
                    Cell::Time { ticks } => column.time(*ticks),
                    Cell::DateTime { days, ticks } | Cell::DateTimeOffset { days, ticks, .. } => {
                        column.timestamp(i64::from(*days), (*ticks / 10) as i64)
                    }
                    Cell::Uuid(v) => column.uuid(*v),
                }
            }
            self.rows += 1;
        }
    }

    fn truncate_to_rows(&mut self) {
        let rows = self.rows;
        for column in &mut self.columns {
            column.truncate(rows);
        }
    }
}

impl RowWriter for ArrowSink {
    fn write_null(&mut self, col: usize) {
        self.columns[col].null();
    }
    fn write_bool(&mut self, col: usize, val: bool) {
        self.columns[col].int(i64::from(val));
    }
    fn write_u8(&mut self, col: usize, val: u8) {
        self.columns[col].int(i64::from(val));
    }
    fn write_i16(&mut self, col: usize, val: i16) {
        self.columns[col].int(i64::from(val));
    }
    fn write_i32(&mut self, col: usize, val: i32) {
        self.columns[col].int(i64::from(val));
    }
    fn write_i64(&mut self, col: usize, val: i64) {
        self.columns[col].int(val);
    }
    fn write_f32(&mut self, col: usize, val: f32) {
        self.columns[col].float(f64::from(val));
    }
    fn write_f64(&mut self, col: usize, val: f64) {
        self.columns[col].float(val);
    }
    fn write_string(&mut self, col: usize, bytes: Cow<'_, [u8]>, encoding_type: EncodingType) {
        self.columns[col].encoded(&bytes, encoding_type);
    }
    fn write_bytes(&mut self, col: usize, bytes: Cow<'_, [u8]>) {
        self.columns[col].bytes(&bytes);
    }
    fn write_decimal(&mut self, col: usize, val: DecimalParts) {
        self.columns[col].decimal(!val.is_positive, val.magnitude(), val.scale as i8);
    }
    fn write_numeric(&mut self, col: usize, val: DecimalParts) {
        self.write_decimal(col, val);
    }
    fn write_date(&mut self, col: usize, val: SqlDate) {
        self.columns[col].date(val.get_days());
    }
    fn write_time(&mut self, col: usize, val: SqlTime) {
        self.columns[col].time(val.time_nanoseconds);
    }
    fn write_datetime(&mut self, col: usize, val: SqlDateTime) {
        // datetime counts 1/300 s; round to whole milliseconds like msodbcsql.
        let millis = (i64::from(val.time) * 10 + 1) / 3;
        self.columns[col].timestamp(DAYS_TO_1900 + i64::from(val.days), millis * 1000);
    }
    fn write_smalldatetime(&mut self, col: usize, val: SqlSmallDateTime) {
        let micros = i64::from(val.time) * 60_000_000;
        self.columns[col].timestamp(DAYS_TO_1900 + i64::from(val.days), micros);
    }
    fn write_datetime2(&mut self, col: usize, val: SqlDateTime2) {
        let micros = (val.time.time_nanoseconds / 10) as i64;
        self.columns[col].timestamp(i64::from(val.days), micros);
    }
    fn write_datetimeoffset(&mut self, col: usize, val: SqlDateTimeOffset) {
        // The wire value is UTC already; Arrow stores UTC with tz "+00:00".
        self.write_datetime2(col, val.datetime2);
    }
    fn write_money(&mut self, col: usize, val: SqlMoney) {
        let lsb = (val.lsb_part as i64) & 0xFFFF_FFFF;
        let scaled = lsb | ((val.msb_part as i64) << 32);
        self.columns[col].decimal(scaled < 0, u128::from(scaled.unsigned_abs()), 4);
    }
    fn write_smallmoney(&mut self, col: usize, val: SqlSmallMoney) {
        let scaled = i64::from(val.int_val);
        self.columns[col].decimal(scaled < 0, u128::from(scaled.unsigned_abs()), 4);
    }
    fn write_uuid(&mut self, col: usize, val: Uuid) {
        self.columns[col].uuid(val);
    }
    fn write_xml(&mut self, col: usize, val: SqlXml) {
        self.columns[col].utf16(&val.bytes);
    }
    fn write_json(&mut self, col: usize, val: SqlJson) {
        self.columns[col].encoded(&val.bytes, EncodingType::Utf8);
    }
    fn write_vector(&mut self, col: usize, val: SqlVector) {
        // Same text form as the row path (decode/buffer.rs).
        let text = match val.as_f32() {
            Some(values) => format!("{values:?}"),
            None => format!("{val:?}"),
        };
        self.columns[col].text(text.as_bytes());
    }
    fn end_row(&mut self) {
        self.rows += 1;
    }
}
