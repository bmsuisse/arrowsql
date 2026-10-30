//! TDS wire encoding of bulk row cells, shared by the Arrow and Python row
//! sources.
//!
//! mssql-tds serializes a bulk row cell by cell through async calls, which
//! costs far more than the bytes it writes. For the types below a cell is
//! written straight into a row buffer instead, byte for byte as
//! `TdsValueSerializer` does (see
//! `vendor/mssql-tds/src/datatypes/tds_value_serializer.rs`), and the buffer is
//! handed to the packet writer once per row.

use mssql_tds::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
use mssql_tds::datatypes::sql_string::encode_narrow;

pub const PLP_UNKNOWN_LEN: u64 = 0xFFFF_FFFF_FFFF_FFFE;
pub const NVARCHAR: u8 = 0xE7;
pub const NCHAR: u8 = 0xEF;
pub const BIGVARCHAR: u8 = 0xA7;
pub const BIGCHAR: u8 = 0xAF;
pub const BIGVARBINARY: u8 = 0xA5;
pub const BIGBINARY: u8 = 0xAD;

/// What the packet writer's type context says about a destination column.
#[derive(Clone, Copy)]
pub struct Layout {
    pub tds_type: u8,
    /// A fixed-size TDS type: values carry no length byte.
    pub fixed_type: bool,
    pub fixed_length: bool,
    pub plp: bool,
    /// Character capacity for wide strings, bytes otherwise.
    pub max_size: usize,
    /// Decimal precision, 38 when the column does not say.
    pub precision: u8,
}

impl Layout {
    pub fn of(dest: &BulkCopyColumnMetadata) -> Layout {
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
    pub fn length_prefixed(&self) -> bool {
        !self.fixed_type && !self.fixed_length
    }

    /// The bytes of a NULL for this column (`serialize_null`), or None when
    /// the type cannot hold one.
    pub fn null_bytes(&self) -> Option<Vec<u8>> {
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
pub fn time_length(scale: u8) -> usize {
    match scale {
        0..=2 => 3,
        3 | 4 => 4,
        _ => 5,
    }
}

/// `ticks` (100 ns) in the unit a fractional-second scale stores.
fn time_units(ticks: u64, scale: u8) -> u64 {
    ticks / 10u64.pow(7 - u32::from(scale.min(7)))
}

pub fn put_time(out: &mut Vec<u8>, ticks: u64, scale: u8) {
    out.extend_from_slice(&time_units(ticks, scale).to_le_bytes()[..time_length(scale)]);
}

pub fn put_days(out: &mut Vec<u8>, days: u32) {
    out.extend_from_slice(&days.to_le_bytes()[..3]);
}

/// `[length][value]` for nullable ints and floats, the bare value for fixed ones.
pub fn put_number(out: &mut Vec<u8>, fixed: bool, value: &[u8]) {
    if !fixed {
        out.push(value.len() as u8);
    }
    out.extend_from_slice(value);
}

pub fn put_date(out: &mut Vec<u8>, layout: Layout, sql_days: u32) {
    if layout.length_prefixed() {
        out.push(3);
    }
    put_days(out, sql_days);
}

pub fn put_time_of_day(out: &mut Vec<u8>, layout: Layout, ticks: u64, scale: u8) {
    if layout.length_prefixed() {
        out.push(time_length(scale) as u8);
    }
    put_time(out, ticks, scale);
}

pub fn put_datetime2(out: &mut Vec<u8>, layout: Layout, days: u32, ticks: u64, scale: u8) {
    if layout.length_prefixed() {
        out.push((time_length(scale) + 3) as u8);
    }
    put_time(out, ticks, scale);
    put_days(out, days);
}

pub fn put_datetimeoffset(out: &mut Vec<u8>, layout: Layout, days: u32, ticks: u64, scale: u8, offset_minutes: i16) {
    if layout.length_prefixed() {
        out.push((time_length(scale) + 5) as u8);
    }
    put_time(out, ticks, scale);
    put_days(out, days);
    out.extend_from_slice(&offset_minutes.to_le_bytes());
}

pub fn put_legacy_datetime(out: &mut Vec<u8>, layout: Layout, days: i32, ticks: u32) {
    if !layout.fixed_type {
        out.push(8);
    }
    out.extend_from_slice(&days.to_le_bytes());
    out.extend_from_slice(&ticks.to_le_bytes());
}

pub fn put_small_datetime(out: &mut Vec<u8>, layout: Layout, days: u16, minutes: u16) {
    if !layout.fixed_type {
        out.push(4);
    }
    out.extend_from_slice(&days.to_le_bytes());
    out.extend_from_slice(&minutes.to_le_bytes());
}

/// A decimal given as sign and magnitude already scaled to the column.
pub fn put_decimal(out: &mut Vec<u8>, layout: Layout, positive: bool, magnitude: u128) {
    let value_bytes = decimal_value_bytes(layout.precision);
    if !layout.fixed_length {
        out.push(1 + value_bytes as u8);
    }
    out.push(u8::from(positive));
    out.extend_from_slice(&magnitude.to_le_bytes()[..value_bytes]);
}

pub fn decimal_value_bytes(precision: u8) -> usize {
    match precision {
        1..=9 => 4,
        10..=19 => 8,
        20..=28 => 12,
        _ => 16,
    }
}

/// `uuid` is the 16 bytes in SQL Server's mixed-endian layout.
pub fn put_uuid(out: &mut Vec<u8>, layout: Layout, uuid: [u8; 16]) {
    if !layout.fixed_length {
        out.push(16);
    }
    out.extend_from_slice(&uuid);
}

/// UTF-16LE text for NVARCHAR / NCHAR, PLP or not.
pub fn put_wide_text(out: &mut Vec<u8>, layout: Layout, text: &str) -> Result<(), String> {
    let put_units = |out: &mut Vec<u8>| {
        if text.is_ascii() {
            let start = out.len();
            out.resize(start + 2 * text.len(), 0);
            for (i, byte) in text.bytes().enumerate() {
                out[start + 2 * i] = byte;
            }
        } else {
            for unit in text.encode_utf16() {
                out.extend_from_slice(&unit.to_le_bytes());
            }
        }
    };
    if layout.plp {
        out.extend_from_slice(&PLP_UNKNOWN_LEN.to_le_bytes());
        let start = out.len();
        out.extend_from_slice(&[0; 4]);
        put_units(out);
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
    let header = if layout.fixed_length { 0 } else { 2 };
    out.resize(start + header, 0);
    put_units(out);
    let chars = (out.len() - start - header) / 2;
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
}

/// Text for VARCHAR / CHAR: ASCII goes out as is, anything else through the
/// column's collation (a Latin-1 mapping without one).
pub fn put_narrow_text(out: &mut Vec<u8>, dest: &BulkCopyColumnMetadata, layout: Layout, text: &str) -> Result<(), String> {
    if text.is_ascii() {
        return put_narrow_bytes(out, dest, layout, text.as_bytes());
    }
    let encoded = match dest.collation {
        Some(collation) => encode_narrow(text, collation).bytes,
        None => text.chars().flat_map(|c| std::iter::repeat_n(u8::try_from(u32::from(c)).unwrap_or(b'?'), c.len_utf16().max(1))).collect(),
    };
    put_narrow_bytes(out, dest, layout, &encoded)
}

pub fn put_narrow_bytes(out: &mut Vec<u8>, dest: &BulkCopyColumnMetadata, layout: Layout, bytes: &[u8]) -> Result<(), String> {
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
            "Encoded value of {} bytes exceeds the {} bytes of column '{}'",
            bytes.len(),
            layout.max_size,
            dest.column_name
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
}

/// VARBINARY / BINARY.
pub fn put_binary(out: &mut Vec<u8>, layout: Layout, bytes: &[u8]) -> Result<(), String> {
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
        return Err(format!("Binary data length ({}) exceeds schema size ({})", bytes.len(), layout.max_size));
    }
    if layout.fixed_length {
        out.extend_from_slice(bytes);
        out.resize(out.len() + layout.max_size - bytes.len(), 0);
    } else {
        out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    Ok(())
}

pub fn finite(value: f64, name: &str) -> Result<(), String> {
    if value.is_finite() {
        return Ok(());
    }
    Err(format!("Non-finite float value ({value}) is not allowed in column '{name}'"))
}
