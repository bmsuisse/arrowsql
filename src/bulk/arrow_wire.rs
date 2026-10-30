//! Direct TDS encoding of Arrow cells for the common column types (see
//! `wire` for why). Every other (type, column) pair stays on the
//! `ColumnValues` path of `arrow_encode`.

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, LargeBinaryArray, Date32Array, Date64Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
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
    EPOCH_DAYS_FROM_YEAR_ONE, Kind, MAX_SQL_DAYS, TICKS_PER_DAY, TICKS_PER_SECOND, decimal_magnitude,
    legacy_datetime_parts, small_datetime_parts,
};
use super::wire::{
    self, BIGBINARY, BIGCHAR, BIGVARBINARY, BIGVARCHAR, Layout, NCHAR, NVARCHAR, finite, put_number,
};

/// Writes one cell (or fails with a message) for the given row of the batch.
pub type Cell<'a> = Box<dyn Fn(usize, &mut Vec<u8>) -> Result<(), String> + 'a>;

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
pub(super) fn split_days(ticks: i64) -> Result<(u32, u64), String> {
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
}

/// A binary column of any Arrow width (fixed-size binary included).
enum Bytes<'a> {
    Binary(&'a BinaryArray),
    Fixed(&'a FixedSizeBinaryArray),
    Large(&'a LargeBinaryArray),
    View(&'a BinaryViewArray),
}

impl<'a> Bytes<'a> {
    fn of(kind: Kind, array: &'a dyn Array) -> Result<Bytes<'a>, String> {
        Ok(match kind {
            Kind::Binary => match array.as_any().downcast_ref::<BinaryArray>() {
                Some(binary) => Bytes::Binary(binary),
                None => Bytes::Fixed(downcast(array)?),
            },
            Kind::LargeBinary => Bytes::Large(downcast(array)?),
            _ => Bytes::View(downcast(array)?),
        })
    }

    #[inline]
    fn get(&self, row: usize) -> &'a [u8] {
        match self {
            Bytes::Binary(a) => a.value(row),
            Bytes::Fixed(a) => a.value(row),
            Bytes::Large(a) => a.value(row),
            Bytes::View(a) => a.value(row),
        }
    }
}

/// Build the cell writer of `array` for `dest`, or None when this pairing
/// stays on the `ColumnValues` path.
pub fn cell<'a>(
    kind: Kind,
    array: &'a dyn Array,
    dest: &'a BulkCopyColumnMetadata,
) -> Result<Option<Cell<'a>>, String> {
    let layout = Layout::of(dest);
    let Some(cell) = typed_cell(kind, array, dest, layout)? else {
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
) -> Result<Option<Cell<'a>>, String> {
    let fixed = layout.fixed_type;
    Ok(match kind {
        Kind::Bool => {
            let values = downcast::<BooleanArray>(array)?;
            Some(Box::new(move |row, out| {
                put_number(out, fixed, &[u8::from(values.value(row))]);
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
                put_number(out, fixed, &value.to_le_bytes());
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
                    put_number(out, fixed, &value.to_le_bytes());
                } else {
                    put_number(out, fixed, &f64::from(value).to_le_bytes());
                }
                Ok(())
            }))
        }
        Kind::Float64 => {
            let values = downcast::<Float64Array>(array)?.values();
            let name = dest.column_name.as_str();
            Some(Box::new(move |row, out| {
                finite(values[row], name)?;
                put_number(out, fixed, &values[row].to_le_bytes());
                Ok(())
            }))
        }
        Kind::Decimal128 { scale } => {
            let values = downcast::<Decimal128Array>(array)?.values();
            Some(Box::new(move |row, out| {
                let (positive, magnitude) = decimal_magnitude(values[row], scale, dest)?;
                wire::put_decimal(out, layout, positive, magnitude);
                Ok(())
            }))
        }
        Kind::Date32 => {
            let values = downcast::<Date32Array>(array)?.values();
            Some(Box::new(move |row, out| {
                let days = values[row];
                let sql_days = days
                    .checked_add(EPOCH_DAYS_FROM_YEAR_ONE)
                    .filter(|d| *d >= 0)
                    .ok_or_else(|| format!("Date {days} is before 0001-01-01"))?;
                wire::put_date(out, layout, sql_days as u32);
                Ok(())
            }))
        }
        Kind::Date64 => {
            let values = downcast::<Date64Array>(array)?.values();
            Some(Box::new(move |row, out| {
                let sql_days = values[row].div_euclid(86_400_000) + i64::from(EPOCH_DAYS_FROM_YEAR_ONE);
                if !(0..=MAX_SQL_DAYS).contains(&sql_days) {
                    return Err(format!("Date64 value {} ms out of SQL DATE range", values[row]));
                }
                wire::put_date(out, layout, sql_days as u32);
                Ok(())
            }))
        }
        Kind::TimestampDateTime2 { unit, scale } => {
            let values = timestamp_values(array, unit)?;
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, ticks) = split_days(to_ticks(values[row], scaling)?)?;
                wire::put_datetime2(out, layout, days, ticks, scale);
                Ok(())
            }))
        }
        Kind::TimestampDateTimeOffset { unit, scale } => {
            let values = timestamp_values(array, unit)?;
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, ticks) = split_days(to_ticks(values[row], scaling)?)?;
                wire::put_datetimeoffset(out, layout, days, ticks, scale, 0);
                Ok(())
            }))
        }
        Kind::Time { unit, scale } => time_cell(array, unit, scale, layout)?,
        Kind::DateTime { unit } => {
            let values = timestamp_values(array, unit)?;
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, ticks) = legacy_datetime_parts(to_ticks(values[row], scaling)?)?;
                wire::put_legacy_datetime(out, layout, days, ticks);
                Ok(())
            }))
        }
        Kind::SmallDateTime { unit } => {
            let values = timestamp_values(array, unit)?;
            let scaling = unit_scale(unit);
            Some(Box::new(move |row, out| {
                let (days, minutes) = small_datetime_parts(to_ticks(values[row], scaling)?)?;
                wire::put_small_datetime(out, layout, days, minutes);
                Ok(())
            }))
        }
        Kind::FixedUuid => {
            let values = downcast::<FixedSizeBinaryArray>(array)?;
            Some(Box::new(move |row, out| {
                let bytes: [u8; 16] =
                    values.value(row).try_into().map_err(|_| "Invalid fixed_size_binary(16) value".to_string())?;
                wire::put_uuid(out, layout, Uuid::from_bytes(bytes).to_bytes_le());
                Ok(())
            }))
        }
        Kind::Utf8 | Kind::LargeUtf8 | Kind::Utf8View => {
            let text = Text::of(kind, array)?;
            match layout.tds_type {
                NVARCHAR | NCHAR => Some(Box::new(move |row, out| wire::put_wide_text(out, layout, text.get(row)))),
                BIGVARCHAR | BIGCHAR => {
                    Some(Box::new(move |row, out| wire::put_narrow_text(out, dest, layout, text.get(row))))
                }
                _ => None,
            }
        }
        Kind::Binary | Kind::LargeBinary | Kind::BinaryView => {
            if !matches!(layout.tds_type, BIGVARBINARY | BIGBINARY) {
                return Ok(None);
            }
            let bytes = Bytes::of(kind, array)?;
            Some(Box::new(move |row, out| wire::put_binary(out, layout, bytes.get(row))))
        }
        Kind::Null
        | Kind::Money { .. }
        | Kind::SmallMoney { .. }
        | Kind::Utf8Uuid
        | Kind::Xml
        | Kind::Json => None,
    })
}

/// Any Arrow integer width into the destination's integer width.
fn ints<'a, T>(values: &'a [T], dest: &'a BulkCopyColumnMetadata, fixed: bool) -> Option<Cell<'a>>
where
    T: Copy + Into<i64> + 'a,
{
    let name = dest.column_name.as_str();
    let out_of_range =
        move |value: i64, type_name: &str| format!("Value {value} out of range for {type_name} column '{name}'");
    Some(match dest.sql_type {
        SqlDbType::BigInt => Box::new(move |row, out| {
            put_number(out, fixed, &values[row].into().to_le_bytes());
            Ok(())
        }),
        SqlDbType::Int => Box::new(move |row, out| {
            let value: i64 = values[row].into();
            let narrow = i32::try_from(value).map_err(|_| out_of_range(value, "INT"))?;
            put_number(out, fixed, &narrow.to_le_bytes());
            Ok(())
        }),
        SqlDbType::SmallInt => Box::new(move |row, out| {
            let value: i64 = values[row].into();
            let narrow = i16::try_from(value).map_err(|_| out_of_range(value, "SMALLINT"))?;
            put_number(out, fixed, &narrow.to_le_bytes());
            Ok(())
        }),
        SqlDbType::TinyInt => Box::new(move |row, out| {
            let value: i64 = values[row].into();
            let narrow = u8::try_from(value).map_err(|_| out_of_range(value, "TINYINT"))?;
            put_number(out, fixed, &[narrow]);
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
    Ok(Some(Box::new(move |row, out| {
        let ticks = u64::try_from(ticks_of(row)).map_err(|_| "Time value is negative".to_string())?;
        wire::put_time_of_day(out, layout, ticks, scale);
        Ok(())
    })))
}
