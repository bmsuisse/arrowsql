//! Arrow record batches -> TDS bulk load rows.
//!
//! Each (Arrow column, destination column) pair gets a [`ColumnPlan`] once,
//! before anything is sent; every cell then costs one typed buffer read and one
//! `StreamingBulkLoadWriter::write_column_value`. The (Arrow type, SQL type)
//! matrix and the range checks follow Microsoft's `mssql-py-core`
//! (`arrow_bulkcopy.rs`, MIT) so both engines accept and reject the same data.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StringViewArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use async_trait::async_trait;
use mssql_tds::connection::bulk_copy::BulkLoadRow;
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType};
use mssql_tds::datatypes::column_values::{
    ColumnValues, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::{EncodingType, SqlString};
use mssql_tds::error::Error;
use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;
use uuid::Uuid;

use super::arrow_source::ArrowBatches;
use super::settings::{BulkSettings, Resolved, SourceShape};
use super::{BulkFailure, Outcome, Rows, SourceFailure, run};
use crate::statement::StmtShared;

/// Days from `0001-01-01` to `1970-01-01`: Arrow dates count from the epoch,
/// SQL `date` from year one.
const EPOCH_DAYS_FROM_YEAR_ONE: i32 = 719_162;
/// Days from `1900-01-01` to `1970-01-01`, the origin of legacy `datetime`.
const DAYS_1900_TO_EPOCH: i64 = 25_567;
/// 100-ns ticks per second, the unit of SQL `time` / `datetime2` payloads.
const TICKS_PER_SECOND: u64 = 10_000_000;
const TICKS_PER_DAY: i64 = TICKS_PER_SECOND as i64 * 86_400;
/// Last day `datetime2` / `date` can hold, counted from year one.
const MAX_SQL_DAYS: i64 = 3_652_058;
/// `datetime` counts 1/300 s ticks.
const DATETIME_TICKS_PER_DAY: u64 = 86_400 * 300;

/// How one Arrow column is read and what SQL value it becomes.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
    BinaryView,
    Date32,
    Date64,
    TimestampDateTime2 { unit: TimeUnit, scale: u8 },
    TimestampDateTimeOffset { unit: TimeUnit, scale: u8 },
    Time { unit: TimeUnit, scale: u8 },
    Decimal128 { scale: u8 },
    Money { scale: u8 },
    SmallMoney { scale: u8 },
    DateTime { unit: TimeUnit },
    SmallDateTime { unit: TimeUnit },
    FixedUuid,
    Utf8Uuid,
    Xml,
    Json,
}

struct ColumnPlan {
    source: usize,
    kind: Kind,
    destination: BulkCopyColumnMetadata,
}

fn build_plans(
    schema: &Schema,
    destination: &[BulkCopyColumnMetadata],
    resolved: &[Resolved],
) -> Result<Vec<ColumnPlan>, String> {
    resolved
        .iter()
        .map(|mapping| {
            let field = schema.fields().get(mapping.source).ok_or_else(|| {
                format!(
                    "Arrow source column index {} out of bounds (schema has {} fields)",
                    mapping.source,
                    schema.fields().len()
                )
            })?;
            let dest = &destination[mapping.destination];
            let kind = resolve_kind(field.data_type(), dest).map_err(|reason| {
                format!(
                    "Cannot map Arrow column '{}' ({:?}) to SQL column '{}' ({:?}): {reason}",
                    field.name(),
                    field.data_type(),
                    dest.column_name,
                    dest.sql_type
                )
            })?;
            Ok(ColumnPlan { source: mapping.source, kind, destination: dest.clone() })
        })
        .collect()
}

fn resolve_kind(arrow: &DataType, dest: &BulkCopyColumnMetadata) -> Result<Kind, String> {
    use SqlDbType as S;
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};

    let ints = |kind| match dest.sql_type {
        S::TinyInt | S::SmallInt | S::Int | S::BigInt => Ok(kind),
        _ => Err("integer columns load into tinyint, smallint, int or bigint".to_string()),
    };
    let unsupported = || "type combination is not supported by the native Arrow bulk writer".to_string();
    let tz_dropped = |target: &str| {
        format!(
            "cannot write a timezone-aware Arrow timestamp to a {target} column without dropping the \
             timezone; use a datetimeoffset destination column instead"
        )
    };
    Ok(match (arrow, dest.sql_type) {
        (DataType::Null, _) => Kind::Null,
        (DataType::Boolean, S::Bit) => Kind::Bool,
        (DataType::Int8, _) => ints(Kind::Int8)?,
        (DataType::Int16, _) => ints(Kind::Int16)?,
        (DataType::Int32, _) => ints(Kind::Int32)?,
        (DataType::Int64, _) => ints(Kind::Int64)?,
        (DataType::UInt8, _) => ints(Kind::UInt8)?,
        (DataType::UInt16, _) => ints(Kind::UInt16)?,
        (DataType::UInt32, _) => ints(Kind::UInt32)?,
        (DataType::UInt64, S::BigInt) => Kind::UInt64,
        (DataType::Float32, S::Real | S::Float) => Kind::Float32,
        (DataType::Float64, S::Float) => Kind::Float64,
        (DataType::Utf8, S::NVarChar | S::NChar | S::NText | S::VarChar | S::Char | S::Text) => Kind::Utf8,
        (DataType::LargeUtf8, S::NVarChar | S::NChar | S::NText | S::VarChar | S::Char | S::Text) => Kind::LargeUtf8,
        (DataType::Utf8View, S::NVarChar | S::NChar | S::NText | S::VarChar | S::Char | S::Text) => Kind::Utf8View,
        (DataType::Binary, S::VarBinary | S::Binary | S::Image) => Kind::Binary,
        (DataType::LargeBinary, S::VarBinary | S::Binary | S::Image) => Kind::LargeBinary,
        (DataType::BinaryView, S::VarBinary | S::Binary | S::Image) => Kind::BinaryView,
        (DataType::FixedSizeBinary(_), S::VarBinary | S::Binary | S::Image) => Kind::Binary,
        (DataType::Date32, S::Date) => Kind::Date32,
        (DataType::Date64, S::Date) => Kind::Date64,
        (DataType::Timestamp(unit, None), S::DateTime2) => {
            Kind::TimestampDateTime2 { unit: *unit, scale: dest.scale.min(7) }
        }
        (DataType::Timestamp(unit, Some(_)), S::DateTimeOffset) => {
            Kind::TimestampDateTimeOffset { unit: *unit, scale: dest.scale.min(7) }
        }
        (DataType::Timestamp(_, Some(_)), S::DateTime2) => return Err(tz_dropped("datetime2")),
        (DataType::Timestamp(unit, None), S::DateTime) => Kind::DateTime { unit: *unit },
        (DataType::Timestamp(unit, None), S::SmallDateTime) => Kind::SmallDateTime { unit: *unit },
        (DataType::Timestamp(_, Some(_)), S::DateTime | S::SmallDateTime) => {
            return Err(tz_dropped("datetime/smalldatetime"));
        }
        (DataType::Time32(unit @ (Second | Millisecond)), S::Time)
        | (DataType::Time64(unit @ (Microsecond | Nanosecond)), S::Time) => {
            Kind::Time { unit: *unit, scale: dest.scale.min(7) }
        }
        (DataType::Decimal128(_, scale), S::Decimal | S::Numeric) => Kind::Decimal128 { scale: checked_scale(*scale)? },
        (DataType::Decimal128(_, scale), S::Money) => Kind::Money { scale: checked_scale(*scale)? },
        (DataType::Decimal128(_, scale), S::SmallMoney) => Kind::SmallMoney { scale: checked_scale(*scale)? },
        (DataType::FixedSizeBinary(16), S::UniqueIdentifier) => Kind::FixedUuid,
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, S::UniqueIdentifier) => Kind::Utf8Uuid,
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, S::Xml) => Kind::Xml,
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, S::Json) => Kind::Json,
        _ => return Err(unsupported()),
    })
}

/// Arrow decimal scales are signed; SQL Server has no negative scale.
fn checked_scale(scale: i8) -> Result<u8, String> {
    u8::try_from(scale).map_err(|_| format!("negative Arrow decimal scale ({scale}) is not supported"))
}

/// The columns of one record batch in plan order, shared by its rows.
struct BatchColumns {
    plans: Arc<[ColumnPlan]>,
    columns: Vec<ArrayRef>,
}

/// One row of a batch.
struct ArrowRow {
    batch: Arc<BatchColumns>,
    index: usize,
}

#[async_trait]
impl BulkLoadRow for ArrowRow {
    async fn write_to_packet(
        &self,
        writer: &mut StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        for (plan, column) in self.batch.plans.iter().zip(&self.batch.columns) {
            let value = plan.value(column.as_ref(), self.index)?;
            writer.write_column_value(*column_index, &value).await?;
            *column_index += 1;
        }
        Ok(())
    }
}

/// Rows of every batch of the stream, in order. A read failure ends the
/// iteration and is recorded for the caller (an iterator cannot return one).
struct ArrowRows {
    batches: ArrowBatches,
    plans: Arc<[ColumnPlan]>,
    current: Option<Arc<BatchColumns>>,
    rows_in_current: usize,
    next_row: usize,
    failure: SourceFailure,
}

impl ArrowRows {
    fn enter(&mut self, batch: &RecordBatch) {
        let columns = self.plans.iter().map(|plan| Arc::clone(batch.column(plan.source))).collect();
        self.current = Some(Arc::new(BatchColumns { plans: Arc::clone(&self.plans), columns }));
        self.rows_in_current = batch.num_rows();
        self.next_row = 0;
    }
}

impl Iterator for ArrowRows {
    type Item = ArrowRow;

    fn next(&mut self) -> Option<ArrowRow> {
        loop {
            if let Some(batch) = self.current.as_ref().filter(|_| self.next_row < self.rows_in_current) {
                let row = ArrowRow { batch: Arc::clone(batch), index: self.next_row };
                self.next_row += 1;
                return Some(row);
            }
            match self.batches.next()? {
                Ok(batch) => self.enter(&batch),
                Err(err) => {
                    self.failure.record(format!("Failed to read the next Arrow batch: {err}"));
                    return None;
                }
            }
        }
    }
}

/// Load `batches` into the destination of `settings`.
pub fn load(
    stmt: &Arc<StmtShared>,
    settings: &BulkSettings,
    batches: ArrowBatches,
) -> Result<Outcome, BulkFailure> {
    let schema = batches.schema();
    let shape = SourceShape {
        column_count: schema.fields().len(),
        names: Some(schema.fields().iter().map(|f| f.name().clone()).collect()),
    };
    run(stmt, settings, &shape, |destination, resolved| {
        let plans = build_plans(&schema, destination, resolved).map_err(BulkFailure::Unsupported)?;
        let failure = SourceFailure::default();
        let iter = ArrowRows {
            batches,
            plans: plans.into(),
            current: None,
            rows_in_current: 0,
            next_row: 0,
            failure: failure.clone(),
        };
        Ok(Rows { iter, failure })
    })
}

fn usage(message: String) -> Error {
    Error::UsageError(message)
}

impl ColumnPlan {
    /// The SQL value of row `row` of `array`, honouring the validity bitmap.
    fn value(&self, array: &dyn Array, row: usize) -> TdsResult<ColumnValues> {
        let dest = &self.destination;
        if matches!(self.kind, Kind::Null) || array.is_null(row) {
            if !dest.is_nullable {
                return Err(usage(format!("Cannot insert NULL value into non-nullable column '{}'", dest.column_name)));
            }
            return Ok(ColumnValues::Null);
        }
        match self.kind {
            Kind::Null => Ok(ColumnValues::Null),
            Kind::Bool => Ok(ColumnValues::Bit(downcast::<BooleanArray>(array)?.value(row))),
            Kind::Int8 => narrow_int(downcast::<Int8Array>(array)?.value(row).into(), dest),
            Kind::Int16 => narrow_int(downcast::<Int16Array>(array)?.value(row).into(), dest),
            Kind::Int32 => narrow_int(downcast::<Int32Array>(array)?.value(row).into(), dest),
            Kind::Int64 => narrow_int(downcast::<Int64Array>(array)?.value(row), dest),
            Kind::UInt8 => narrow_int(downcast::<UInt8Array>(array)?.value(row).into(), dest),
            Kind::UInt16 => narrow_int(downcast::<UInt16Array>(array)?.value(row).into(), dest),
            Kind::UInt32 => narrow_int(downcast::<UInt32Array>(array)?.value(row).into(), dest),
            Kind::UInt64 => {
                let value = downcast::<UInt64Array>(array)?.value(row);
                let value = i64::try_from(value).map_err(|_| {
                    usage(format!(
                        "Value {value} out of range for BIGINT column '{}' (exceeds i64::MAX)",
                        dest.column_name
                    ))
                })?;
                narrow_int(value, dest)
            }
            Kind::Float32 => {
                let value = downcast::<Float32Array>(array)?.value(row);
                check_finite(value.into(), dest)?;
                Ok(if dest.sql_type == SqlDbType::Real {
                    ColumnValues::Real(value)
                } else {
                    ColumnValues::Float(value.into())
                })
            }
            Kind::Float64 => {
                let value = downcast::<Float64Array>(array)?.value(row);
                check_finite(value, dest)?;
                Ok(ColumnValues::Float(value))
            }
            Kind::Utf8 => Ok(text(downcast::<StringArray>(array)?.value(row))),
            Kind::LargeUtf8 => Ok(text(downcast::<LargeStringArray>(array)?.value(row))),
            Kind::Utf8View => Ok(text(downcast::<StringViewArray>(array)?.value(row))),
            Kind::Binary => Ok(ColumnValues::Bytes(match array.as_any().downcast_ref::<BinaryArray>() {
                Some(binary) => binary.value(row).to_vec(),
                None => downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec(),
            })),
            Kind::LargeBinary => Ok(ColumnValues::Bytes(downcast::<LargeBinaryArray>(array)?.value(row).to_vec())),
            Kind::BinaryView => Ok(ColumnValues::Bytes(downcast::<BinaryViewArray>(array)?.value(row).to_vec())),
            Kind::Date32 => {
                let days = downcast::<Date32Array>(array)?.value(row);
                let sql_days = days
                    .checked_add(EPOCH_DAYS_FROM_YEAR_ONE)
                    .filter(|d| *d >= 0)
                    .ok_or_else(|| usage(format!("Date {days} is before 0001-01-01")))?;
                Ok(ColumnValues::Date(SqlDate::create(sql_days as u32)?))
            }
            Kind::Date64 => {
                let millis = downcast::<Date64Array>(array)?.value(row);
                let sql_days = millis.div_euclid(86_400_000) + i64::from(EPOCH_DAYS_FROM_YEAR_ONE);
                if !(0..=MAX_SQL_DAYS).contains(&sql_days) {
                    return Err(usage(format!("Date64 value {millis} ms out of SQL DATE range")));
                }
                Ok(ColumnValues::Date(SqlDate::create(sql_days as u32)?))
            }
            Kind::TimestampDateTime2 { unit, scale } => {
                let ticks = timestamp_ticks(array, row, unit)?;
                Ok(ColumnValues::DateTime2(datetime2(ticks, scale)?))
            }
            Kind::TimestampDateTimeOffset { unit, scale } => {
                let ticks = timestamp_ticks(array, row, unit)?;
                Ok(ColumnValues::DateTimeOffset(SqlDateTimeOffset { datetime2: datetime2(ticks, scale)?, offset: 0 }))
            }
            Kind::Time { unit, scale } => {
                Ok(ColumnValues::Time(SqlTime { time_nanoseconds: time_ticks(array, row, unit)?, scale }))
            }
            Kind::Decimal128 { scale } => {
                let raw = downcast::<Decimal128Array>(array)?.value(row);
                let parts = decimal_parts(raw, scale, dest)?;
                Ok(if dest.sql_type == SqlDbType::Numeric {
                    ColumnValues::Numeric(parts)
                } else {
                    ColumnValues::Decimal(parts)
                })
            }
            Kind::Money { scale } => {
                let raw = downcast::<Decimal128Array>(array)?.value(row);
                let cents = i64::try_from(rescale_rounding(raw, scale, 4)?)
                    .map_err(|_| usage(format!("Value out of range for MONEY column '{}'", dest.column_name)))?;
                Ok(ColumnValues::Money(SqlMoney { lsb_part: (cents & 0xFFFF_FFFF) as i32, msb_part: (cents >> 32) as i32 }))
            }
            Kind::SmallMoney { scale } => {
                let raw = downcast::<Decimal128Array>(array)?.value(row);
                let int_val = i32::try_from(rescale_rounding(raw, scale, 4)?)
                    .map_err(|_| usage(format!("Value out of range for SMALLMONEY column '{}'", dest.column_name)))?;
                Ok(ColumnValues::SmallMoney(SqlSmallMoney { int_val }))
            }
            Kind::DateTime { unit } => {
                let ticks = timestamp_ticks(array, row, unit)?;
                let (days, time) = legacy_datetime(ticks)?;
                Ok(ColumnValues::DateTime(SqlDateTime { days, time }))
            }
            Kind::SmallDateTime { unit } => {
                let ticks = timestamp_ticks(array, row, unit)?;
                Ok(ColumnValues::SmallDateTime(small_datetime(ticks)?))
            }
            Kind::FixedUuid => {
                let bytes: [u8; 16] = downcast::<FixedSizeBinaryArray>(array)?
                    .value(row)
                    .try_into()
                    .map_err(|_| usage("Invalid fixed_size_binary(16) value".to_string()))?;
                Ok(ColumnValues::Uuid(Uuid::from_bytes(bytes)))
            }
            Kind::Utf8Uuid => {
                let text = utf8_cell(array, row)?;
                Uuid::parse_str(text.trim())
                    .map(ColumnValues::Uuid)
                    .map_err(|err| usage(format!("Invalid GUID string '{text}' for column '{}': {err}", dest.column_name)))
            }
            Kind::Xml => Ok(ColumnValues::Xml(SqlXml::from(utf8_cell(array, row)?.to_owned()))),
            Kind::Json => Ok(ColumnValues::Json(SqlJson { bytes: utf8_cell(array, row)?.as_bytes().to_vec() })),
        }
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> TdsResult<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        usage(format!(
            "Arrow array downcast failed: expected {}, got {:?}",
            std::any::type_name::<T>(),
            array.data_type()
        ))
    })
}

fn utf8_cell(array: &dyn Array, row: usize) -> TdsResult<&str> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        Ok(strings.value(row))
    } else if let Some(strings) = array.as_any().downcast_ref::<LargeStringArray>() {
        Ok(strings.value(row))
    } else {
        Ok(downcast::<StringViewArray>(array)?.value(row))
    }
}

/// UTF-16LE, the form `SqlString` carries for both wide and narrow columns
/// (mssql-tds transcodes to the column's code page when it serializes).
fn text(value: &str) -> ColumnValues {
    let mut bytes = Vec::with_capacity(value.len() * 2);
    for unit in value.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    ColumnValues::String(SqlString::new(bytes, EncodingType::Utf16))
}

fn check_finite(value: f64, dest: &BulkCopyColumnMetadata) -> TdsResult<()> {
    if value.is_finite() {
        return Ok(());
    }
    Err(usage(format!("Non-finite float value ({value}) is not allowed in column '{}'", dest.column_name)))
}

/// Any Arrow integer into the destination's integer width, range checked.
fn narrow_int(value: i64, dest: &BulkCopyColumnMetadata) -> TdsResult<ColumnValues> {
    let out_of_range =
        |type_name: &str| usage(format!("Value {value} out of range for {type_name} column '{}'", dest.column_name));
    match dest.sql_type {
        SqlDbType::TinyInt => u8::try_from(value).map(ColumnValues::TinyInt).map_err(|_| out_of_range("TINYINT")),
        SqlDbType::SmallInt => i16::try_from(value).map(ColumnValues::SmallInt).map_err(|_| out_of_range("SMALLINT")),
        SqlDbType::Int => i32::try_from(value).map(ColumnValues::Int).map_err(|_| out_of_range("INT")),
        _ => Ok(ColumnValues::BigInt(value)),
    }
}

fn pow10(exponent: u32) -> TdsResult<u128> {
    10u128.checked_pow(exponent).ok_or_else(|| usage("decimal scale overflow".to_string()))
}

/// A `decimal128` raw value (scaled by `from_scale`) as the destination's
/// decimal. A value that does not fit the destination's scale or precision is
/// rejected, never rounded.
fn decimal_parts(raw: i128, from_scale: u8, dest: &BulkCopyColumnMetadata) -> TdsResult<DecimalParts> {
    let magnitude = raw.unsigned_abs();
    let scaled = if dest.scale >= from_scale {
        magnitude
            .checked_mul(pow10(u32::from(dest.scale - from_scale))?)
            .ok_or_else(|| usage(format!("Decimal value out of range for column '{}'", dest.column_name)))?
    } else {
        let divisor = pow10(u32::from(from_scale - dest.scale))?;
        if magnitude % divisor != 0 {
            return Err(usage(format!(
                "Input decimal scale {from_scale} exceeds target scale {} of column '{}'",
                dest.scale, dest.column_name
            )));
        }
        magnitude / divisor
    };
    if scaled >= pow10(u32::from(dest.precision))? {
        return Err(usage(format!(
            "Decimal value exceeds target precision {} of column '{}'",
            dest.precision, dest.column_name
        )));
    }
    Ok(DecimalParts::new(raw >= 0, dest.precision, dest.scale, scaled))
}

/// `raw` at `from_scale` as an integer at `to_scale`, half away from zero when
/// the scale shrinks.
fn rescale_rounding(raw: i128, from_scale: u8, to_scale: u8) -> TdsResult<i128> {
    use std::cmp::Ordering;
    match from_scale.cmp(&to_scale) {
        Ordering::Equal => Ok(raw),
        Ordering::Less => {
            let factor = i128::try_from(pow10(u32::from(to_scale - from_scale))?)
                .map_err(|_| usage("money value overflow".to_string()))?;
            raw.checked_mul(factor).ok_or_else(|| usage("money value overflow".to_string()))
        }
        Ordering::Greater => {
            let divisor = i128::try_from(pow10(u32::from(from_scale - to_scale))?)
                .map_err(|_| usage("decimal scale overflow".to_string()))?;
            let half = divisor / 2;
            Ok(if raw >= 0 { (raw + half) / divisor } else { (raw - half) / divisor })
        }
    }
}

/// A timestamp cell as 100-ns ticks since the Unix epoch.
fn timestamp_ticks(array: &dyn Array, row: usize, unit: TimeUnit) -> TdsResult<i64> {
    let overflow = || usage("Timestamp overflow when scaling to 100-ns ticks".to_string());
    match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(array)?
            .value(row)
            .checked_mul(TICKS_PER_SECOND as i64)
            .ok_or_else(overflow),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?
            .value(row)
            .checked_mul((TICKS_PER_SECOND / 1_000) as i64)
            .ok_or_else(overflow),
        TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?
            .value(row)
            .checked_mul((TICKS_PER_SECOND / 1_000_000) as i64)
            .ok_or_else(overflow),
        // Floor, so pre-epoch values agree with the div_euclid day split.
        TimeUnit::Nanosecond => Ok(downcast::<TimestampNanosecondArray>(array)?.value(row).div_euclid(100)),
    }
}

/// A time-of-day cell as 100-ns ticks.
fn time_ticks(array: &dyn Array, row: usize, unit: TimeUnit) -> TdsResult<u64> {
    let ticks = match unit {
        TimeUnit::Second => i64::from(downcast::<Time32SecondArray>(array)?.value(row)) * TICKS_PER_SECOND as i64,
        TimeUnit::Millisecond => {
            i64::from(downcast::<Time32MillisecondArray>(array)?.value(row)) * (TICKS_PER_SECOND / 1_000) as i64
        }
        TimeUnit::Microsecond => {
            downcast::<Time64MicrosecondArray>(array)?.value(row).saturating_mul((TICKS_PER_SECOND / 1_000_000) as i64)
        }
        TimeUnit::Nanosecond => downcast::<Time64NanosecondArray>(array)?.value(row).div_euclid(100),
    };
    u64::try_from(ticks).map_err(|_| usage("Time value is negative".to_string()))
}

fn datetime2(ticks_since_epoch: i64, scale: u8) -> TdsResult<SqlDateTime2> {
    let sql_days = ticks_since_epoch.div_euclid(TICKS_PER_DAY) + i64::from(EPOCH_DAYS_FROM_YEAR_ONE);
    if !(0..=MAX_SQL_DAYS).contains(&sql_days) {
        return Err(usage(format!("Timestamp out of SQL DATETIME2 range: days_since_year_one={sql_days}")));
    }
    Ok(SqlDateTime2 {
        days: sql_days as u32,
        time: SqlTime { time_nanoseconds: ticks_since_epoch.rem_euclid(TICKS_PER_DAY) as u64, scale },
    })
}

/// Legacy `datetime`: (days since 1900-01-01, 1/300 s ticks), rounded to the
/// nearest tick like SqlClient.
fn legacy_datetime(ticks_since_epoch: i64) -> TdsResult<(i32, u32)> {
    let days = ticks_since_epoch.div_euclid(TICKS_PER_DAY) + DAYS_1900_TO_EPOCH;
    let micros = ticks_since_epoch.rem_euclid(TICKS_PER_DAY) as u64 / 10;
    let mut ticks = (micros * 300 + 500_000) / 1_000_000;
    let mut days = days;
    if ticks >= DATETIME_TICKS_PER_DAY {
        ticks -= DATETIME_TICKS_PER_DAY;
        days += 1;
    }
    if !(-53_690..=2_958_463).contains(&days) {
        return Err(usage(format!(
            "Timestamp out of DATETIME range (days since 1900 = {days}); valid 1753-01-01 to 9999-12-31"
        )));
    }
    Ok((days as i32, ticks as u32))
}

/// Legacy `smalldatetime`: minute precision, seconds >= 30 round up.
fn small_datetime(ticks_since_epoch: i64) -> TdsResult<SqlSmallDateTime> {
    let days = ticks_since_epoch.div_euclid(TICKS_PER_DAY) + DAYS_1900_TO_EPOCH;
    let seconds = ticks_since_epoch.rem_euclid(TICKS_PER_DAY) / TICKS_PER_SECOND as i64;
    let mut minutes = seconds / 60 + i64::from(seconds % 60 >= 30);
    let mut days = days;
    if minutes >= 24 * 60 {
        minutes -= 24 * 60;
        days += 1;
    }
    if !(0..=65_535).contains(&days) {
        return Err(usage(format!(
            "Timestamp out of SMALLDATETIME range (days since 1900 = {days}); valid 1900-01-01 to 2079-06-06"
        )));
    }
    Ok(SqlSmallDateTime { days: days as u16, time: minutes as u16 })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal_dest(precision: u8, scale: u8) -> BulkCopyColumnMetadata {
        let mut meta = BulkCopyColumnMetadata::new("d", SqlDbType::Decimal, SqlDbType::Decimal.to_tds_type());
        meta.precision = precision;
        meta.scale = scale;
        meta
    }

    #[test]
    fn decimal_rescales_up_to_the_destination() {
        let parts = decimal_parts(12_345, 2, &decimal_dest(10, 4)).unwrap();
        assert_eq!(parts.to_string(), "123.4500");
    }

    #[test]
    fn decimal_scale_down_needs_zero_remainder() {
        assert!(decimal_parts(12_300, 4, &decimal_dest(10, 2)).is_ok());
        assert!(decimal_parts(12_345, 4, &decimal_dest(10, 2)).is_err());
    }

    #[test]
    fn decimal_precision_overflow_is_rejected() {
        assert!(decimal_parts(100_000, 2, &decimal_dest(5, 2)).is_err());
        assert!(decimal_parts(99_999, 2, &decimal_dest(5, 2)).is_ok());
    }

    #[test]
    fn negative_decimal_keeps_its_sign() {
        let parts = decimal_parts(-150, 2, &decimal_dest(10, 2)).unwrap();
        assert_eq!(parts.to_string(), "-1.50");
    }

    #[test]
    fn datetime_rounds_to_third_of_a_second_ticks() {
        // 1900-01-01 00:00:00.123 = 37 ticks (36.9 rounded).
        let ticks = -DAYS_1900_TO_EPOCH * TICKS_PER_DAY + 1_230_000;
        assert_eq!(legacy_datetime(ticks).unwrap(), (0, 37));
    }

    #[test]
    fn datetime_rounding_carries_into_the_next_day() {
        let ticks = -DAYS_1900_TO_EPOCH * TICKS_PER_DAY + TICKS_PER_DAY - 1;
        assert_eq!(legacy_datetime(ticks).unwrap(), (1, 0));
    }

    #[test]
    fn smalldatetime_rounds_seconds_up_from_thirty() {
        let base = -DAYS_1900_TO_EPOCH * TICKS_PER_DAY;
        assert_eq!(small_datetime(base + 29 * TICKS_PER_SECOND as i64).unwrap().time, 0);
        assert_eq!(small_datetime(base + 30 * TICKS_PER_SECOND as i64).unwrap().time, 1);
    }

    #[test]
    fn money_rescale_rounds_half_away_from_zero() {
        assert_eq!(rescale_rounding(12_345, 5, 4).unwrap(), 1_235);
        assert_eq!(rescale_rounding(-12_345, 5, 4).unwrap(), -1_235);
        assert_eq!(rescale_rounding(12, 2, 4).unwrap(), 1_200);
    }
}
