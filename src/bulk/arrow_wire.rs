//! Direct TDS encoding of Arrow cells for the common column types.
//!
//! mssql-tds serializes a bulk row cell by cell through async calls, which
//! costs far more than the bytes it writes. For the types below the cell is
//! written straight into a row buffer instead, byte for byte as
//! `TdsValueSerializer` would (the wire format per TDS type is spelled out in
//! `vendor/mssql-tds/src/datatypes/tds_value_serializer.rs`), and the buffer is
//! handed to the packet writer once per row. Every other (type, column) pair
//! stays on the `ColumnValues` path of `arrow_encode`.

use std::ops::Range;

use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::TimeUnit;
use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType};
use uuid::Uuid;

use super::arrow_encode::{
    EPOCH_DAYS_FROM_YEAR_ONE, Kind, MAX_SQL_DAYS, TICKS_PER_DAY, TICKS_PER_SECOND,
    decimal_magnitude, legacy_datetime_parts,
};

const PLP_UNKNOWN_LEN: u64 = 0xFFFF_FFFF_FFFF_FFFE;
const NVARCHAR: u8 = 0xE7;
const NCHAR: u8 = 0xEF;
const BIGVARCHAR: u8 = 0xA7;
const BIGCHAR: u8 = 0xAF;

/// Writes one cell (or fails with a message) for the given row of the batch.
pub type Cell<'a> = Box<dyn Fn(usize, &mut Vec<u8>) -> Result<(), String> + 'a>;

/// What the packet writer's type context says about a destination column.
#[derive(Clone, Copy)]
struct Layout {
    tds_type: u8,
    /// A fixed-size TDS type: values carry no length byte.
    fixed_type: bool,
    fixed_length: bool,
    plp: bool,
    /// Character capacity for wide strings, bytes otherwise.
    max_size: usize,
    precision: u8,
}

impl Layout {
    fn of(dest: &BulkCopyColumnMetadata) -> Layout {
        let plp = dest.length_type.is_plp();
        let max_size = if matches!(dest.tds_type, NVARCHAR | NCHAR) && !plp {
            (dest.length / 2) as usize
        } else {
            dest.length as usize
        };
        Layout {
            tds_type: dest.tds_type,
            fixed_type: matches!(dest.tds_type, 0x30..=0x3F | 0x7A | 0x7F),
            fixed_length: dest.length_type.is_fixed(),
            plp,
            max_size,
            precision: if dest.precision > 0 { dest.precision } else { 38 },
        }
    }

    /// Date and time types carry a length byte unless fixed in either sense.
    fn length_prefixed(&self) -> bool {
        !self.fixed_type && !self.fixed_length
    }

    /// The bytes of a NULL for this column (`serialize_null`), or None when
    /// the type cannot hold one.
    fn null_bytes(&self) -> Option<Vec<u8>> {
        match self.tds_type {
            0x23 | 0x63 | 0x22 => Some(vec![0]),
            0x26 | 0x6D | 0x68 | 0x6E | 0x6F | 0x6A | 0x6C | 0x24 | 0x28 | 0x29 | 0x2A | 0x2B => Some(vec![0]),
            0x32 => None,
            _ if self.plp => Some(vec![0xFF; 8]),
            _ if self.fixed_type => None,
            _ => Some(vec![0xFF, 0xFF]),
        }
    }
}

/// How many bytes `time` / `datetime2` store for a fractional-second scale.
fn time_length(scale: u8) -> usize {
    match scale {
        0..=2 => 3,
        3 | 4 => 4,
        _ => 5,
    }
}

/// 100-ns ticks in the unit a scale stores.
fn time_units(ticks: u64, scale: u8) -> u64 {
    ticks / 10u64.pow(7 - u32::from(scale.min(7)))
}

fn put_time(out: &mut Vec<u8>, ticks: u64, scale: u8) {
    let length = time_length(scale);
    out.extend_from_slice(&time_units(ticks, scale).to_le_bytes()[..length]);
}

fn put_days(out: &mut Vec<u8>, days: u32) {
    out.extend_from_slice(&days.to_le_bytes()[..3]);
}

/// Ticks per unit: (multiplier, divisor) to reach 100-ns ticks.
fn unit_scale(unit: TimeUnit) -> (i64, i64) {
    match unit {
        TimeUnit::Second => (TICKS_PER_SECOND as i64, 1),
        TimeUnit::Millisecond => (TICKS_PER_SECOND as i64 / 1_000, 1),
        TimeUnit::Microsecond => (TICKS_PER_SECOND as i64 / 1_000_000, 1),
        TimeUnit::Nanosecond => (1, 100),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, String> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!("Arrow array downcast failed: expected {}, got {:?}", std::any::type_name::<T>(), array.data_type())
    })
}

fn timestamp_values(array: &dyn Array, unit: TimeUnit) -> Result<&[i64], String> {
    Ok(match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(array)?.values(),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?.values(),
        TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?.values(),
        TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array)?.values(),
    })
}

/// A timestamp as 100-ns ticks since the epoch.
fn to_ticks(raw: i64, (mul, div): (i64, i64)) -> Result<i64, String> {
    if div > 1 {
        return Ok(raw.div_euclid(div));
    }
    raw.checked_mul(mul).ok_or_else(|| "Timestamp overflow when scaling to 100-ns ticks".to_string())
}

/// (SQL days since year one, ticks of the day) of a tick count since the epoch.
fn split_days(ticks: i64) -> Result<(u32, u64), String> {
    let sql_days = ticks.div_euclid(TICKS_PER_DAY) + i64::from(EPOCH_DAYS_FROM_YEAR_ONE);
    if !(0..=MAX_SQL_DAYS).contains(&sql_days) {
        return Err(format!("Timestamp out of SQL DATETIME2 range: days_since_year_one={sql_days}"));
    }
    Ok((sql_days as u32, ticks.rem_euclid(TICKS_PER_DAY) as u64))
}

/// A string column of any Arrow width.
enum Text<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> Text<'a> {
    fn of(kind: Kind, array: &'a dyn Array) -> Result<Text<'a>, String> {
        Ok(match kind {
            Kind::Utf8 => Text::Utf8(downcast(array)?),
            Kind::LargeUtf8 => Text::Large(downcast(array)?),
            _ => Text::View(downcast(array)?),
        })
    }

    #[inline]
    fn get(&self, row: usize) -> &'a str {
        match self {
            Text::Utf8(a) => a.value(row),
            Text::Large(a) => a.value(row),
            Text::View(a) => a.value(row),
        }
    }

    /// Whether every value in `rows` is ASCII (null slots hold empty values).
    fn is_ascii(&self, rows: Range<usize>) -> bool {
        match self {
            Text::Utf8(a) => {
                let offsets = a.value_offsets();
                a.value_data()[offsets[rows.start] as usize..offsets[rows.end] as usize].is_ascii()
            }
            Text::Large(a) => {
                let offsets = a.value_offsets();
                a.value_data()[offsets[rows.start] as usize..offsets[rows.end] as usize].is_ascii()
            }
            Text::View(a) => rows.into_iter().all(|row| a.value(row).is_ascii()),
        }
    }
}

/// Build the cell writer of `array` for `dest`, or None when this pairing
/// stays on the `ColumnValues` path. `rows` is the window about to be written.
pub fn cell<'a>(
    kind: Kind,
    array: &'a dyn Array,
    dest: &'a BulkCopyColumnMetadata,
    rows: Range<usize>,
) -> Result<Option<Cell<'a>>, String> {
    let layout = Layout::of(dest);
    let Some(cell) = typed_cell(kind, array, dest, layout, rows)? else {
        return Ok(None);
    };
    Ok(with_nulls(array, dest, layout, cell))
}

/// Wrap `cell` with the column's NULL handling.
fn with_nulls<'a>(
    array: &'a dyn Array,
    dest: &'a BulkCopyColumnMetadata,
    layout: Layout,
    cell: Cell<'a>,
) -> Option<Cell<'a>> {
    let Some(nulls) = array.nulls().filter(|_| array.null_count() > 0) else {
        return Some(cell);
    };
    if !dest.is_nullable {
        let name = dest.column_name.as_str();
        return Some(Box::new(move |row, out| {
            if nulls.is_null(row) {
                return Err(format!("Cannot insert NULL value into non-nullable column '{name}'"));
            }
            cell(row, out)
        }));
    }
    let null = layout.null_bytes()?;
    Some(Box::new(move |row, out| null_or(nulls, &null, &cell, row, out)))
}

#[inline]
fn null_or(
    nulls: &NullBuffer,
    null: &[u8],
    cell: &Cell<'_>,
    row: usize,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    if nulls.is_null(row) {
        out.extend_from_slice(null);
        return Ok(());
    }
    cell(row, out)
}

fn typed_cell<'a>(
    kind: Kind,
    array: &'a dyn Array,
    dest: &'a BulkCopyColumnMetadata,
    layout: Layout,
    rows: Range<usize>,
) -> Result<Option<Cell<'a>>, String> {
    let fixed = layout.fixed_type;
    Ok(match kind {
        Kind::Bool => {
            let values = downcast::<BooleanArray>(array)?;
            Some(Box::new(move |row, out| {
                if !fixed {
                    out.push(1);
                }
                out.push(u8::from(values.value(row)));
                Ok(())
            }))
        }
        Kind::Int8 => ints(&downcast::<Int8Array>(array)?.values()[..], dest, fixed),
        Kind::Int16 => ints(&downcast::<Int16Array>(array)?.values()[..], dest, fixed),
        Kind::Int32 => ints(&downcast::<Int32Array>(array)?.values()[..], dest, fixed),
        Kind::Int64 => ints(&downcast::<Int64Array>(array)?.values()[..], dest, fixed),
        Kind::UInt8 => ints(&downcast::<UInt8Array>(array)?.values()[..], dest, fixed),
        Kind::UInt16 => ints(&downcast::<UInt16Array>(array)?.values()[..], dest, fixed),
        Kind::UInt32 => ints(&downcast::<UInt32Array>(array)?.values()[..], dest, fixed),
        Kind::UInt64 => {
            let values = downcast::<UInt64Array>(array)?.values();
            let name = dest.column_name.as_str();
            Some(Box::new(move |row, out| {
                let value = i64::try_from(values[row]).map_err(|_| {
                    format!("Value {} out of range for BIGINT column '{name}' (exceeds i64::MAX)", values[row])
                })?;
                put_int(out, fixed, &value.to_le_bytes());
                Ok(())
            }))
        }
        Kind::Float32 => {
            let values = downcast::<Float32Array>(array)?.values();
            let name = dest.column_name.as_str();
            let real = dest.sql_type == SqlDbType::Real;
            Some(Box::new(move |row, out| {
                let value = values[row];
                finite(f64::from(value), name)?;
                if real {
                    put_float(out, fixed, &value.to_le_bytes());
                } else {
                    put_float(out, fixed, &f64::from(value).to_le_bytes());
                }
                Ok(())
            }))
        }
        Kind::Float64 => {
            let values = downcast::<Float64Array>(array)?.values();
            let name = dest.column_name.as_str();
            Some(Box::new(move |row, out| {
                finite(values[row], name)?;
                put_float(out, fixed, &values[row].to_le_bytes());
                Ok(())
            }))
        }
        Kind::Decimal128 { scale } => {
            let values = downcast::<Decimal128Array>(array)?.values();
            let value_bytes = match layout.precision {
                1..=9 => 4,
                10..=19 => 8,
                20..=28 => 12,
                _ => 16,
            };
            let prefixed = !layout.fixed_length;
            Some(Box::new(move |row, out| {
                let (positive, magnitude) = decimal_magnitude(values[row], scale, dest)?;
                if prefixed {
                    out.push(1 + value_bytes as u8);
                }
                out.push(u8::from(positive));
                out.extend_from_slice(&magnitude.to_le_bytes()[..value_bytes]);
                Ok(())
            }))
        }
        Kind::Date32 => {
            let values = downcast::<Date32Array>(array)?.values();
            let prefixed = layout.length_prefixed();
            Some(Box::new(move |row, out| {
                let days = values[row];
                let sql_days = days
                    .checked_add(EPOCH_DAYS_FROM_YEAR_ONE)
                    .filter(|d| *d >= 0)
                    .ok_or_else(|| format!("Date {days} is before 0001-01-01"))?;
                if prefixed {
                    out.push(3);
                }
                put_days(out, sql_days as u32);
                Ok(())
            }))
        }
        Kind::Date64 => {
            let values = downcast::<Date64Array>(array)?.values();
            let prefixed = layout.length_prefixed();
            Some(Box::new(move |row, out| {
                let sql_days = values[row].div_euclid(86_400_000) + i64::from(EPOCH_DAYS_FROM_YEAR_ONE);
                if !(0..=MAX_SQL_DAYS).contains(&sql_days) {
                    return Err(format!("Date64 value {} ms out of SQL DATE range", values[row]));
                }
                if prefixed {
                    out.push(3);
                }
                put_days(out, sql_days as u32);
                Ok(())
            }))
        }
        Kind::TimestampDateTime2 { unit, scale } => {
            let values = timestamp_values(array, unit)?;
            let prefixed = layout.length_prefixed();
            let length = time_length(scale);
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, ticks) = split_days(to_ticks(values[row], scaling)?)?;
                if prefixed {
                    out.push((length + 3) as u8);
                }
                put_time(out, ticks, scale);
                put_days(out, days);
                Ok(())
            }))
        }
        Kind::TimestampDateTimeOffset { unit, scale } => {
            let values = timestamp_values(array, unit)?;
            let prefixed = layout.length_prefixed();
            let length = time_length(scale);
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, ticks) = split_days(to_ticks(values[row], scaling)?)?;
                if prefixed {
                    out.push((length + 5) as u8);
                }
                put_time(out, ticks, scale);
                put_days(out, days);
                out.extend_from_slice(&0i16.to_le_bytes());
                Ok(())
            }))
        }
        Kind::Time { unit, scale } => time_cell(array, unit, scale, layout)?,
        Kind::DateTime { unit } => {
            let values = timestamp_values(array, unit)?;
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, time) = legacy_datetime_parts(to_ticks(values[row], scaling)?)?;
                if !fixed {
                    out.push(8);
                }
                out.extend_from_slice(&days.to_le_bytes());
                out.extend_from_slice(&time.to_le_bytes());
                Ok(())
            }))
        }
        Kind::FixedUuid => {
            let values = downcast::<FixedSizeBinaryArray>(array)?;
            let prefixed = !layout.fixed_length;
            Some(Box::new(move |row, out| {
                let bytes: [u8; 16] =
                    values.value(row).try_into().map_err(|_| "Invalid fixed_size_binary(16) value".to_string())?;
                if prefixed {
                    out.push(16);
                }
                out.extend_from_slice(&Uuid::from_bytes(bytes).to_bytes_le());
                Ok(())
            }))
        }
        Kind::Utf8 | Kind::LargeUtf8 | Kind::Utf8View => text_cell(kind, array, dest, layout, rows)?,
        _ => None,
    })
}

fn finite(value: f64, name: &str) -> Result<(), String> {
    if value.is_finite() {
        return Ok(());
    }
    Err(format!("Non-finite float value ({value}) is not allowed in column '{name}'"))
}

/// `[length][value]` for nullable ints, the bare value for fixed ones.
fn put_int(out: &mut Vec<u8>, fixed: bool, value: &[u8]) {
    if !fixed {
        out.push(value.len() as u8);
    }
    out.extend_from_slice(value);
}

fn put_float(out: &mut Vec<u8>, fixed: bool, value: &[u8]) {
    put_int(out, fixed, value);
}

/// Any Arrow integer width into the destination's integer width.
fn ints<'a, T>(values: &'a [T], dest: &'a BulkCopyColumnMetadata, fixed: bool) -> Option<Cell<'a>>
where
    T: Copy + Into<i64> + 'a,
{
    let name = dest.column_name.as_str();
    let out_of_range = move |value: i64, type_name: &str| format!("Value {value} out of range for {type_name} column '{name}'");
    Some(match dest.sql_type {
        SqlDbType::BigInt => Box::new(move |row, out| {
            put_int(out, fixed, &values[row].into().to_le_bytes());
            Ok(())
        }),
        SqlDbType::Int => Box::new(move |row, out| {
            let value: i64 = values[row].into();
            let narrow = i32::try_from(value).map_err(|_| out_of_range(value, "INT"))?;
            put_int(out, fixed, &narrow.to_le_bytes());
            Ok(())
        }),
        SqlDbType::SmallInt => Box::new(move |row, out| {
            let value: i64 = values[row].into();
            let narrow = i16::try_from(value).map_err(|_| out_of_range(value, "SMALLINT"))?;
            put_int(out, fixed, &narrow.to_le_bytes());
            Ok(())
        }),
        SqlDbType::TinyInt => Box::new(move |row, out| {
            let value: i64 = values[row].into();
            let narrow = u8::try_from(value).map_err(|_| out_of_range(value, "TINYINT"))?;
            put_int(out, fixed, &[narrow]);
            Ok(())
        }),
        _ => return None,
    })
}

fn time_cell<'a>(
    array: &'a dyn Array,
    unit: TimeUnit,
    scale: u8,
    layout: Layout,
) -> Result<Option<Cell<'a>>, String> {
    let ticks_of: Box<dyn Fn(usize) -> i64 + 'a> = match unit {
        TimeUnit::Second => {
            let values = downcast::<Time32SecondArray>(array)?.values();
            Box::new(move |row| i64::from(values[row]) * TICKS_PER_SECOND as i64)
        }
        TimeUnit::Millisecond => {
            let values = downcast::<Time32MillisecondArray>(array)?.values();
            Box::new(move |row| i64::from(values[row]) * (TICKS_PER_SECOND / 1_000) as i64)
        }
        TimeUnit::Microsecond => {
            let values = downcast::<Time64MicrosecondArray>(array)?.values();
            Box::new(move |row| values[row].saturating_mul((TICKS_PER_SECOND / 1_000_000) as i64))
        }
        TimeUnit::Nanosecond => {
            let values = downcast::<Time64NanosecondArray>(array)?.values();
            Box::new(move |row| values[row].div_euclid(100))
        }
    };
    let prefixed = layout.length_prefixed();
    let length = time_length(scale);
    Ok(Some(Box::new(move |row, out| {
        let ticks = u64::try_from(ticks_of(row)).map_err(|_| "Time value is negative".to_string())?;
        if prefixed {
            out.push(length as u8);
        }
        put_time(out, ticks, scale);
        Ok(())
    })))
}

/// UTF-8 into NVARCHAR / NCHAR (UTF-16LE) and, for ASCII data, VARCHAR / CHAR.
fn text_cell<'a>(
    kind: Kind,
    array: &'a dyn Array,
    dest: &'a BulkCopyColumnMetadata,
    layout: Layout,
    rows: Range<usize>,
) -> Result<Option<Cell<'a>>, String> {
    let text = Text::of(kind, array)?;
    match layout.tds_type {
        NVARCHAR | NCHAR => Ok(Some(wide_text(text, layout))),
        // Narrow columns only take the shortcut while the bytes are the same
        // in every code page, i.e. ASCII.
        BIGVARCHAR | BIGCHAR if text.is_ascii(rows.clone()) => Ok(Some(narrow_ascii_text(text, dest, layout))),
        _ => Ok(None),
    }
}

fn wide_text<'a>(text: Text<'a>, layout: Layout) -> Cell<'a> {
    Box::new(move |row, out| {
        let value = text.get(row);
        let wide = |out: &mut Vec<u8>| {
            if value.is_ascii() {
                let start = out.len();
                out.resize(start + 2 * value.len(), 0);
                for (i, byte) in value.bytes().enumerate() {
                    out[start + 2 * i] = byte;
                }
            } else {
                for unit in value.encode_utf16() {
                    out.extend_from_slice(&unit.to_le_bytes());
                }
            }
        };
        if layout.plp {
            out.extend_from_slice(&PLP_UNKNOWN_LEN.to_le_bytes());
            let start = out.len();
            out.extend_from_slice(&[0; 4]);
            wide(out);
            let length = (out.len() - start - 4) as u32;
            if length == 0 {
                // An empty chunk header is the terminator; a second one would desync the stream.
                out.truncate(start);
            } else {
                out[start..start + 4].copy_from_slice(&length.to_le_bytes());
            }
            out.extend_from_slice(&0u32.to_le_bytes());
            return Ok(());
        }
        let start = out.len();
        if !layout.fixed_length {
            out.extend_from_slice(&[0; 2]);
        }
        wide(out);
        let chars = (out.len() - start - if layout.fixed_length { 0 } else { 2 }) / 2;
        if chars > layout.max_size {
            return Err(format!("String length ({chars} characters) exceeds schema size ({} characters)", layout.max_size));
        }
        if layout.fixed_length {
            for _ in chars..layout.max_size {
                out.extend_from_slice(&0x0020u16.to_le_bytes());
            }
        } else {
            out[start..start + 2].copy_from_slice(&((chars * 2) as u16).to_le_bytes());
        }
        Ok(())
    })
}

fn narrow_ascii_text<'a>(text: Text<'a>, dest: &'a BulkCopyColumnMetadata, layout: Layout) -> Cell<'a> {
    let name = dest.column_name.as_str();
    Box::new(move |row, out| {
        let bytes = text.get(row).as_bytes();
        if layout.plp {
            out.extend_from_slice(&PLP_UNKNOWN_LEN.to_le_bytes());
            if !bytes.is_empty() {
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
            out.extend_from_slice(&0u32.to_le_bytes());
            return Ok(());
        }
        if bytes.len() > layout.max_size {
            return Err(format!(
                "Encoded value of {} bytes exceeds the {} bytes of column '{name}'",
                bytes.len(),
                layout.max_size
            ));
        }
        if layout.fixed_length {
            out.extend_from_slice(bytes);
            out.resize(out.len() + layout.max_size - bytes.len(), b' ');
        } else {
            out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        Ok(())
    })
}
