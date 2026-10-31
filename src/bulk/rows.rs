//! Python row tuples -> TDS bulk load rows.
//!
//! The Python iterator is read a window of rows at a time with the GIL held;
//! every cell of a window is converted straight to wire bytes (see `wire`), so
//! the network phase needs no Python objects. Column types without a direct
//! encoder (money, xml, json, vector, ...) convert through
//! `params::py_types::py_to_column_value` and are serialized by mssql-tds.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mssql_tds::connection::bulk_copy::BulkLoadRow;
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType};
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::error::Error;
use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateAccess, PyDateTime, PyFloat, PyInt, PyIterator, PyString, PyTime,
    PyTimeAccess, PyTuple, PyType,
};

use super::arrow_encode::{
    EPOCH_DAYS_FROM_YEAR_ONE, MAX_SQL_DAYS, TICKS_PER_DAY, TICKS_PER_SECOND, legacy_datetime_parts, small_datetime_parts,
};
use super::arrow_wire::split_days;
use super::settings::{BulkSettings, Resolved, SourceShape};
use super::wire::{self, BIGBINARY, BIGCHAR, BIGVARBINARY, BIGVARCHAR, Layout, NCHAR, NVARCHAR, finite, put_number};
use super::{Outcome, run};
use crate::params::py_types::py_to_column_value;
use crate::statement::StmtShared;

/// Most rows, bytes and wall time one window may take from the iterator.
const WINDOW_ROWS: usize = 4096;
const WINDOW_BYTES: usize = 4 << 20;
const WINDOW_TIME: Duration = Duration::from_millis(20);

static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static UUID_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// How a destination column is converted from Python values.
#[derive(Clone, Copy)]
enum Conv {
    Int,
    Bit,
    Real,
    Float,
    Decimal,
    Money,
    SmallMoney,
    Date,
    Time { scale: u8 },
    DateTime2 { scale: u8 },
    DateTimeOffset { scale: u8 },
    DateTime,
    SmallDateTime,
    Wide,
    Narrow,
    Binary,
    Uuid,
    /// Converted to a `ColumnValues` and serialized by mssql-tds.
    Generic,
}

struct Column {
    source: usize,
    conv: Conv,
    dest: BulkCopyColumnMetadata,
    layout: Layout,
    null: Option<Vec<u8>>,
}

fn conversion_for(dest: &BulkCopyColumnMetadata, layout: Layout) -> Conv {
    use SqlDbType as S;
    match dest.sql_type {
        S::TinyInt | S::SmallInt | S::Int | S::BigInt => Conv::Int,
        S::Bit => Conv::Bit,
        S::Real => Conv::Real,
        S::Float => Conv::Float,
        S::Decimal | S::Numeric => Conv::Decimal,
        S::Money => Conv::Money,
        S::SmallMoney => Conv::SmallMoney,
        S::Date => Conv::Date,
        S::Time => Conv::Time { scale: dest.scale.min(7) },
        S::DateTime2 => Conv::DateTime2 { scale: dest.scale.min(7) },
        S::DateTimeOffset => Conv::DateTimeOffset { scale: dest.scale.min(7) },
        S::DateTime => Conv::DateTime,
        S::SmallDateTime => Conv::SmallDateTime,
        S::NVarChar | S::NChar if matches!(layout.tds_type, NVARCHAR | NCHAR) => Conv::Wide,
        S::VarChar | S::Char if matches!(layout.tds_type, BIGVARCHAR | BIGCHAR) => Conv::Narrow,
        S::VarBinary | S::Binary if matches!(layout.tds_type, BIGVARBINARY | BIGBINARY) => Conv::Binary,
        S::UniqueIdentifier => Conv::Uuid,
        _ => Conv::Generic,
    }
}

fn build_columns(destination: &[BulkCopyColumnMetadata], resolved: &[Resolved]) -> Vec<Column> {
    resolved
        .iter()
        .map(|mapping| {
            let dest = destination[mapping.destination].clone();
            let layout = Layout::of(&dest);
            let conv = conversion_for(&dest, layout);
            Column { source: mapping.source, conv, null: layout.null_bytes(), dest, layout }
        })
        .collect()
}

/// How a window writes a run of columns.
enum Step {
    /// Columns `first..end`, encoded into the window's row buffer.
    Encoded { first: usize, end: usize },
    /// One column converted to a `ColumnValues`.
    Value(usize),
}

fn steps_for(columns: &[Column]) -> Vec<Step> {
    let mut steps: Vec<Step> = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        match (column.conv, steps.last_mut()) {
            (Conv::Generic, _) => steps.push(Step::Value(index)),
            (_, Some(Step::Encoded { end, .. })) => *end = index + 1,
            _ => steps.push(Step::Encoded { first: index, end: index + 1 }),
        }
    }
    steps
}

/// What every window of a load shares.
struct Shared {
    columns: Vec<Column>,
    steps: Vec<Step>,
    encoded_steps: usize,
    generic_columns: usize,
    /// Number of values per source row; later rows must match the first.
    source_columns: usize,
}

/// Converted rows of one read from the iterator.
struct Window {
    shared: Arc<Shared>,
    rows: usize,
    bytes: Vec<u8>,
    /// Where each (row, `Encoded` step) pair starts in `bytes`, plus the end.
    starts: Vec<usize>,
    /// Generic-column values, row by row.
    values: Vec<ColumnValues>,
    error: Option<String>,
}

impl Window {
    fn new(shared: &Arc<Shared>) -> Window {
        Window {
            shared: Arc::clone(shared),
            rows: 0,
            bytes: Vec::new(),
            starts: Vec::new(),
            values: Vec::new(),
            error: None,
        }
    }

    fn failed(shared: &Arc<Shared>, message: String) -> Window {
        Window { rows: 1, error: Some(message), ..Window::new(shared) }
    }
}

struct PyRow {
    window: Arc<Window>,
    row: usize,
}

#[async_trait]
impl BulkLoadRow for PyRow {
    async fn write_to_packet(
        &self,
        writer: &mut StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        let window = &*self.window;
        if let Some(message) = &window.error {
            return Err(Error::UsageError(message.clone()));
        }
        let shared = &*window.shared;
        let mut encoded = self.row * shared.encoded_steps;
        let mut value = self.row * shared.generic_columns;
        for step in &shared.steps {
            match *step {
                Step::Encoded { first, end } => {
                    let bytes = &window.bytes[window.starts[encoded]..window.starts[encoded + 1]];
                    writer.write_raw_bytes(bytes).await?;
                    encoded += 1;
                    *column_index += end - first;
                }
                Step::Value(_) => {
                    writer.write_column_value(*column_index, &window.values[value]).await?;
                    value += 1;
                    *column_index += 1;
                }
            }
        }
        Ok(())
    }
}

/// Rows read from a Python iterator. A Python exception raised by the
/// iterator is kept in `raised` for the caller to re-raise and fails the load.
struct PyRows {
    iter: Py<PyIterator>,
    first: Option<Py<PyAny>>,
    shared: Arc<Shared>,
    window: Option<Arc<Window>>,
    window_row: usize,
    rows_read: u64,
    done: bool,
    raised: Arc<Mutex<Option<PyErr>>>,
}

impl PyRows {
    /// Read and convert the next window; false when the iterator is exhausted.
    fn fill(&mut self) -> bool {
        let window = Python::attach(|py| self.read_window(py));
        if window.rows == 0 {
            return false;
        }
        self.done |= window.error.is_some();
        self.window = Some(Arc::new(window));
        self.window_row = 0;
        true
    }

    /// Fail the load with `err`, which the caller re-raises as is.
    fn fail_with(&self, shared: &Arc<Shared>, what: &str, err: PyErr) -> Window {
        let message = format!("{what}: {err}");
        if let Ok(mut slot) = self.raised.lock() {
            slot.get_or_insert(err);
        }
        Window::failed(shared, message)
    }

    fn read_window(&mut self, py: Python<'_>) -> Window {
        let shared = Arc::clone(&self.shared);
        // A Ctrl-C only reaches Python code while the GIL is held, i.e. here.
        if let Err(err) = py.check_signals() {
            return self.fail_with(&shared, "Interrupted", err);
        }
        let mut window = Window::new(&shared);
        let started = Instant::now();
        let mut iterator = self.iter.bind(py).clone();
        while window.rows < WINDOW_ROWS && window.bytes.len() < WINDOW_BYTES {
            let item = match self.first.take() {
                Some(first) => Ok(first.into_bound(py)),
                None => match iterator.next() {
                    Some(item) => item,
                    None => {
                        self.done = true;
                        break;
                    }
                },
            };
            let row = match item {
                Ok(row) => row,
                Err(err) => return self.fail_with(&shared, "Reading the next row failed", err),
            };
            self.rows_read += 1;
            if let Err(message) = convert_row(&shared, &row, self.rows_read, &mut window) {
                return Window::failed(&shared, message);
            }
            window.rows += 1;
            if window.rows % 64 == 0 && started.elapsed() > WINDOW_TIME {
                break;
            }
        }
        window.starts.push(window.bytes.len());
        window
    }
}

impl Iterator for PyRows {
    type Item = PyRow;

    fn next(&mut self) -> Option<PyRow> {
        loop {
            if let Some(window) = self.window.as_ref().filter(|window| self.window_row < window.rows) {
                let row = PyRow { window: Arc::clone(window), row: self.window_row };
                self.window_row += 1;
                return Some(row);
            }
            if self.done && self.window.as_ref().is_none_or(|w| self.window_row >= w.rows) {
                return None;
            }
            if !self.fill() {
                return None;
            }
        }
    }
}

/// Append the cells of `row` to `window`.
fn convert_row(shared: &Shared, row: &Bound<'_, PyAny>, number: u64, window: &mut Window) -> Result<(), String> {
    let tuple = row.cast::<PyTuple>().map_err(|_| {
        format!(
            "bulkcopy data rows must be tuples or Row objects, got {}",
            row.get_type().name().map(|n| n.to_string()).unwrap_or_default()
        )
    })?;
    if tuple.len() != shared.source_columns {
        return Err(format!(
            "Row {number} has {} columns, but first row had {}. All rows must have the same number of columns as the first row.",
            tuple.len(),
            shared.source_columns
        ));
    }
    let cells = tuple.as_slice();
    for step in &shared.steps {
        match *step {
            Step::Encoded { first, end } => {
                window.starts.push(window.bytes.len());
                for column in &shared.columns[first..end] {
                    encode_cell(column, &cells[column.source], &mut window.bytes)
                        .map_err(|message| format!("Row {number}: {message}"))?;
                }
            }
            Step::Value(index) => {
                let column = &shared.columns[index];
                let value = generic_value(column, &cells[column.source]).map_err(|message| format!("Row {number}: {message}"))?;
                window.values.push(value);
            }
        }
    }
    Ok(())
}

fn generic_value(column: &Column, cell: &Bound<'_, PyAny>) -> Result<ColumnValues, String> {
    if cell.is_none() {
        return null_value(column);
    }
    py_to_column_value(cell, Some(&column.dest)).map_err(|err| err.to_string())
}

fn null_value(column: &Column) -> Result<ColumnValues, String> {
    if !column.dest.is_nullable {
        return Err(format!("Cannot insert NULL value into non-nullable column '{}'", column.dest.column_name));
    }
    Ok(ColumnValues::Null)
}

fn encode_cell(column: &Column, cell: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Result<(), String> {
    if cell.is_none() {
        null_value(column)?;
        let null = column
            .null
            .as_deref()
            .ok_or_else(|| format!("Column '{}' cannot hold NULL", column.dest.column_name))?;
        out.extend_from_slice(null);
        return Ok(());
    }
    let layout = column.layout;
    let dest = &column.dest;
    let fixed = layout.fixed_type;
    let name = dest.column_name.as_str();
    match column.conv {
        Conv::Int => {
            let value = to_i64(cell)?;
            let range_error = |type_name: &str| format!("Value {value} out of range for {type_name} column '{name}'");
            match dest.sql_type {
                SqlDbType::TinyInt => put_number(out, fixed, &[u8::try_from(value).map_err(|_| range_error("TINYINT"))?]),
                SqlDbType::SmallInt => {
                    put_number(out, fixed, &i16::try_from(value).map_err(|_| range_error("SMALLINT"))?.to_le_bytes())
                }
                SqlDbType::Int => {
                    put_number(out, fixed, &i32::try_from(value).map_err(|_| range_error("INT"))?.to_le_bytes())
                }
                _ => put_number(out, fixed, &value.to_le_bytes()),
            }
        }
        Conv::Bit => put_number(out, fixed, &[u8::from(to_bool(cell)?)]),
        Conv::Real => {
            let value = to_f64(cell)?;
            finite(value, name)?;
            put_number(out, fixed, &(value as f32).to_le_bytes());
        }
        Conv::Float => {
            let value = to_f64(cell)?;
            finite(value, name)?;
            put_number(out, fixed, &value.to_le_bytes());
        }
        Conv::Decimal => {
            let (positive, magnitude) = to_decimal(cell, dest)?;
            wire::put_decimal(out, layout, positive, magnitude);
        }
        Conv::Money => {
            let scaled = i64::try_from(to_money(cell, dest)?)
                .map_err(|_| format!("Value out of range for MONEY column '{name}'"))?;
            wire::put_money(out, layout, scaled);
        }
        Conv::SmallMoney => {
            let scaled = i32::try_from(to_money(cell, dest)?)
                .map_err(|_| format!("Value out of range for SMALLMONEY column '{name}'"))?;
            wire::put_small_money(out, layout, scaled);
        }
        Conv::Date => {
            let date = temporal(cell, "date")?;
            let date = date.cast::<PyDate>().map_err(|_| unsupported(cell, "DATE"))?;
            wire::put_date(out, layout, sql_days(date)?);
        }
        Conv::Time { scale } => {
            let time = temporal(cell, "time")?;
            let time = time.cast::<PyTime>().map_err(|_| unsupported(cell, "TIME"))?;
            wire::put_time_of_day(out, layout, time_of_day_ticks(time), scale);
        }
        Conv::DateTime2 { scale } => {
            let (days, ticks) = date_and_ticks(&temporal(cell, "datetime")?, "DATETIME2")?;
            wire::put_datetime2(out, layout, days, ticks, scale);
        }
        Conv::DateTimeOffset { scale } => {
            let value = temporal(cell, "datetime")?;
            let (days, ticks) = date_and_ticks(&value, "DATETIMEOFFSET")?;
            let offset = utc_offset_minutes(&value)?;
            let utc = i64::from(days as i32 - EPOCH_DAYS_FROM_YEAR_ONE) * TICKS_PER_DAY + ticks as i64
                - i64::from(offset) * 60 * TICKS_PER_SECOND as i64;
            let (days, ticks) = split_days(utc)?;
            wire::put_datetimeoffset(out, layout, days, ticks, scale, offset);
        }
        Conv::DateTime => {
            let ticks = epoch_ticks(&temporal(cell, "datetime")?, "DATETIME")?;
            let (days, time) = legacy_datetime_parts(ticks)?;
            wire::put_legacy_datetime(out, layout, days, time);
        }
        Conv::SmallDateTime => {
            let ticks = epoch_ticks(&temporal(cell, "datetime")?, "SMALLDATETIME")?;
            let (days, minutes) = small_datetime_parts(ticks)?;
            wire::put_small_datetime(out, layout, days, minutes);
        }
        Conv::Wide => with_text(cell, |text| wire::put_wide_text(out, layout, text))?,
        Conv::Narrow => with_text(cell, |text| wire::put_narrow_text(out, dest, layout, text))?,
        Conv::Binary => with_bytes(cell, |bytes| wire::put_binary(out, layout, bytes))?,
        Conv::Uuid => wire::put_uuid(out, layout, to_uuid(cell)?),
        Conv::Generic => return Err("internal error: generic column in the encoded path".to_string()),
    }
    Ok(())
}

fn type_name(cell: &Bound<'_, PyAny>) -> String {
    cell.get_type().name().map(|n| n.to_string()).unwrap_or_default()
}

fn unsupported(cell: &Bound<'_, PyAny>, sql: &str) -> String {
    format!("Cannot convert Python {} to {sql}", type_name(cell))
}

fn is_decimal(cell: &Bound<'_, PyAny>) -> bool {
    DECIMAL_TYPE
        .import(cell.py(), "decimal", "Decimal")
        .ok()
        .is_some_and(|class| cell.is_instance(class).unwrap_or(false))
}

fn to_i64(cell: &Bound<'_, PyAny>) -> Result<i64, String> {
    if cell.is_instance_of::<PyInt>() {
        return cell.extract::<i64>().map_err(|_| format!("Integer {cell} is out of range for BIGINT"));
    }
    if let Ok(text) = cell.cast::<PyString>() {
        let text = text.to_str().map_err(|err| err.to_string())?;
        return text.trim().parse::<i64>().map_err(|_| format!("Cannot convert string '{text}' to an integer"));
    }
    cell.extract::<i64>().map_err(|_| format!("Cannot convert Python {} to an integer", type_name(cell)))
}

fn to_f64(cell: &Bound<'_, PyAny>) -> Result<f64, String> {
    if let Ok(float) = cell.cast::<PyFloat>() {
        return Ok(float.value());
    }
    if let Ok(text) = cell.cast::<PyString>() {
        let text = text.to_str().map_err(|err| err.to_string())?;
        return text.trim().parse::<f64>().map_err(|_| format!("Cannot convert string '{text}' to a float"));
    }
    cell.extract::<f64>().map_err(|_| format!("Cannot convert Python {} to a float", type_name(cell)))
}

fn to_bool(cell: &Bound<'_, PyAny>) -> Result<bool, String> {
    if cell.is_instance_of::<PyBool>() {
        return cell.extract::<bool>().map_err(|err| err.to_string());
    }
    if cell.is_instance_of::<PyInt>() {
        return Ok(to_i64(cell)? != 0);
    }
    if let Ok(text) = cell.cast::<PyString>() {
        let text = text.to_str().map_err(|err| err.to_string())?;
        return match text.to_lowercase().as_str() {
            "0" | "false" => Ok(false),
            "1" | "true" => Ok(true),
            _ => Err(format!("Cannot convert string '{text}' to BIT. Valid values: '0', '1', 'true', 'false'")),
        };
    }
    Err(unsupported(cell, "BIT"))
}

/// Run `write` on the text of `cell`: a str, or the str() of a number or Decimal.
fn with_text(cell: &Bound<'_, PyAny>, write: impl FnOnce(&str) -> Result<(), String>) -> Result<(), String> {
    if let Ok(text) = cell.cast::<PyString>() {
        return write(text.to_str().map_err(|err| err.to_string())?);
    }
    if cell.is_instance_of::<PyInt>() || cell.is_instance_of::<PyFloat>() || is_decimal(cell) {
        let text = cell.str().map_err(|err| err.to_string())?;
        return write(text.to_str().map_err(|err| err.to_string())?);
    }
    Err(unsupported(cell, "a character column"))
}

fn with_bytes(cell: &Bound<'_, PyAny>, write: impl FnOnce(&[u8]) -> Result<(), String>) -> Result<(), String> {
    if let Ok(bytes) = cell.cast::<PyBytes>() {
        return write(bytes.as_bytes());
    }
    if let Ok(bytes) = cell.cast::<PyByteArray>() {
        return write(&bytes.to_vec());
    }
    Err(unsupported(cell, "a binary column"))
}

fn to_uuid(cell: &Bound<'_, PyAny>) -> Result<[u8; 16], String> {
    let py = cell.py();
    let class = UUID_TYPE.import(py, "uuid", "UUID").map_err(|err| err.to_string())?;
    let uuid = if cell.is_instance(class).unwrap_or(false) {
        cell.clone()
    } else if cell.is_instance_of::<PyString>() {
        class.call1((cell,)).map_err(|err| format!("Invalid GUID string: {err}"))?
    } else {
        return Err(unsupported(cell, "UNIQUEIDENTIFIER"));
    };
    let bytes = uuid.getattr("bytes_le").map_err(|err| err.to_string())?;
    bytes.cast::<PyBytes>().map_err(|err| err.to_string())?.as_bytes().try_into().map_err(|_| "Invalid UUID".to_string())
}

/// `cell` as a datetime.date / time / datetime object: strings are parsed as ISO 8601.
fn temporal<'py>(cell: &Bound<'py, PyAny>, class: &str) -> Result<Bound<'py, PyAny>, String> {
    if cell.is_instance_of::<PyString>() {
        let module = cell.py().import("datetime").map_err(|err| err.to_string())?;
        let class = module.getattr(class).map_err(|err| err.to_string())?;
        return class.call_method1("fromisoformat", (cell,)).map_err(|err| err.to_string());
    }
    Ok(cell.clone())
}

/// SQL days since 0001-01-01 of a date or datetime.
fn sql_days<T: PyDateAccess>(date: &T) -> Result<u32, String> {
    let (year, month, day) = (i64::from(date.get_year()), i64::from(date.get_month()), i64::from(date.get_day()));
    // Howard Hinnant's days_from_civil, relative to 1970-01-01.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468 + i64::from(EPOCH_DAYS_FROM_YEAR_ONE);
    if !(0..=MAX_SQL_DAYS).contains(&days) {
        return Err(format!("Date out of SQL DATE range: {year}-{month:02}-{day:02}"));
    }
    Ok(days as u32)
}

fn time_of_day_ticks<T: PyTimeAccess>(time: &T) -> u64 {
    let seconds = u64::from(time.get_hour()) * 3600 + u64::from(time.get_minute()) * 60 + u64::from(time.get_second());
    seconds * TICKS_PER_SECOND + u64::from(time.get_microsecond()) * 10
}

/// (SQL days, ticks of the day) of a datetime; a plain date is midnight.
fn date_and_ticks(value: &Bound<'_, PyAny>, sql: &str) -> Result<(u32, u64), String> {
    if let Ok(datetime) = value.cast::<PyDateTime>() {
        return Ok((sql_days(datetime.as_any().cast::<PyDate>().map_err(|e| e.to_string())?)?, time_of_day_ticks(datetime)));
    }
    let date = value.cast::<PyDate>().map_err(|_| unsupported(value, sql))?;
    Ok((sql_days(date)?, 0))
}

/// 100-ns ticks since the Unix epoch of a datetime's wall clock.
fn epoch_ticks(value: &Bound<'_, PyAny>, sql: &str) -> Result<i64, String> {
    let (days, ticks) = date_and_ticks(value, sql)?;
    Ok((i64::from(days) - i64::from(EPOCH_DAYS_FROM_YEAR_ONE)) * TICKS_PER_DAY + ticks as i64)
}

/// Whole-minute UTC offset of an aware datetime; 0 for a naive one.
fn utc_offset_minutes(value: &Bound<'_, PyAny>) -> Result<i16, String> {
    let offset = value.call_method0("utcoffset").map_err(|err| err.to_string())?;
    if offset.is_none() {
        return Ok(0);
    }
    let seconds: f64 = offset
        .call_method0("total_seconds")
        .and_then(|seconds| seconds.extract())
        .map_err(|err| err.to_string())?;
    if !seconds.is_finite() || seconds % 60.0 != 0.0 {
        return Err("DATETIMEOFFSET requires a whole-minute UTC offset".to_string());
    }
    let minutes = seconds / 60.0;
    if !(-840.0..=840.0).contains(&minutes) {
        return Err(format!("Timezone offset {minutes} minutes is outside the DATETIMEOFFSET range"));
    }
    Ok(minutes as i16)
}

/// Sign and magnitude of a Python Decimal, int, float or numeric string at the
/// destination's scale. Extra scale is rejected, except for floats: they carry
/// no exact scale, so they are rounded half to even like mssql_py_core does.
fn to_decimal(cell: &Bound<'_, PyAny>, dest: &BulkCopyColumnMetadata) -> Result<(bool, u128), String> {
    parse_decimal(&number_text(cell, "DECIMAL")?, dest, cell.is_instance_of::<PyFloat>())
}

/// A decimal string as `(negative, digits, shift)`: the value is
/// `digits * 10^shift`.
fn decimal_digits(text: &str, column: &str) -> Result<(bool, u128, i64), String> {
    let bad = || format!("Invalid decimal value '{text}' for column '{column}'");
    let trimmed = text.trim();
    let (negative, unsigned) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (&unsigned[..at], unsigned[at + 1..].parse::<i32>().map_err(|_| bad())?),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err(bad());
    }
    let mut digits: u128 = 0;
    for byte in whole.bytes().chain(fraction.bytes()) {
        if !byte.is_ascii_digit() {
            return Err(bad());
        }
        digits = digits
            .checked_mul(10)
            .and_then(|d| d.checked_add(u128::from(byte - b'0')))
            .ok_or_else(|| format!("Decimal value '{text}' is out of range for column '{column}'"))?;
    }
    Ok((negative, digits, i64::from(exponent) - fraction.len() as i64))
}

fn parse_decimal(text: &str, dest: &BulkCopyColumnMetadata, round: bool) -> Result<(bool, u128), String> {
    let (negative, digits, shift) = decimal_digits(text, &dest.column_name)?;
    let too_wide = || format!("Decimal value exceeds target precision {} of column '{}'", dest.precision, dest.column_name);
    let to_scale = shift + i64::from(dest.scale);
    let scaled = if to_scale >= 0 {
        let factor = 10u128.checked_pow(u32::try_from(to_scale).map_err(|_| too_wide())?).ok_or_else(too_wide)?;
        digits.checked_mul(factor).ok_or_else(too_wide)?
    } else {
        match 10u128.checked_pow(u32::try_from(-to_scale).unwrap_or(u32::MAX)) {
            Some(divisor) if digits % divisor == 0 => digits / divisor,
            _ if digits == 0 => 0,
            Some(divisor) if round => {
                let (quotient, remainder) = (digits / divisor, digits % divisor);
                let round_up = remainder * 2 > divisor || (remainder * 2 == divisor && quotient % 2 == 1);
                quotient + u128::from(round_up)
            }
            None if round => 0,
            _ => {
                return Err(format!(
                    "Input decimal scale exceeds target scale {} of column '{}'",
                    dest.scale, dest.column_name
                ));
            }
        }
    };
    let limit = 10u128.checked_pow(u32::from(dest.precision)).ok_or_else(too_wide)?;
    if scaled >= limit {
        return Err(too_wide());
    }
    Ok((!negative, scaled))
}

/// A Python number as MONEY: scaled by 10^4, rounded half to even.
fn to_money(cell: &Bound<'_, PyAny>, dest: &BulkCopyColumnMetadata) -> Result<i128, String> {
    let text = number_text(cell, "MONEY")?;
    let (negative, digits, shift) = decimal_digits(&text, &dest.column_name)?;
    let overflow = || format!("Value {text} out of range for column '{}'", dest.column_name);
    let to_scale = shift + 4;
    let scaled = if to_scale >= 0 {
        let factor = 10u128.checked_pow(u32::try_from(to_scale).map_err(|_| overflow())?).ok_or_else(overflow)?;
        digits.checked_mul(factor).ok_or_else(overflow)?
    } else {
        match 10u128.checked_pow(u32::try_from(-to_scale).unwrap_or(u32::MAX)) {
            Some(divisor) => {
                let (quotient, remainder) = (digits / divisor, digits % divisor);
                let round_up = remainder * 2 > divisor || (remainder * 2 == divisor && quotient % 2 == 1);
                quotient + u128::from(round_up)
            }
            None => 0,
        }
    };
    let scaled = i128::try_from(scaled).map_err(|_| overflow())?;
    Ok(if negative { -scaled } else { scaled })
}

/// The text of a number (or numeric string) cell.
fn number_text(cell: &Bound<'_, PyAny>, sql: &str) -> Result<String, String> {
    let numeric = cell.is_instance_of::<PyString>()
        || cell.is_instance_of::<PyInt>()
        || cell.is_instance_of::<PyFloat>()
        || is_decimal(cell);
    if !numeric {
        return Err(unsupported(cell, sql));
    }
    Ok(cell.str().map_err(|err| err.to_string())?.to_string())
}

/// Load the rows of the Python iterator `rows` into the destination of `settings`.
/// The first row is read here to learn the column count.
pub fn load(
    py: Python<'_>,
    stmt: &Arc<StmtShared>,
    settings: &BulkSettings,
    rows: &Bound<'_, PyAny>,
) -> PyResult<Option<Outcome>> {
    let mut iterator = rows.try_iter()?;
    let Some(first) = iterator.next().transpose()? else {
        return Ok(None);
    };
    let first_row = first
        .cast::<PyTuple>()
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err(format!(
            "bulkcopy data rows must be tuples or Row objects, got {}",
            type_name(&first)
        )))?;
    let shape = SourceShape { column_count: first_row.len(), names: None };
    let raised = Arc::new(Mutex::new(None));
    let source = iterator.unbind();
    let first = first.unbind();
    let result = py.detach(|| {
        run(stmt, settings, &shape, |destination, resolved| {
            let columns = build_columns(destination, resolved);
            let steps = steps_for(&columns);
            let shared = Arc::new(Shared {
                encoded_steps: steps.iter().filter(|step| matches!(step, Step::Encoded { .. })).count(),
                generic_columns: columns.iter().filter(|c| matches!(c.conv, Conv::Generic)).count(),
                source_columns: shape.column_count,
                columns,
                steps,
            });
            Ok(PyRows {
                iter: source,
                first: Some(first),
                shared,
                window: None,
                window_row: 0,
                rows_read: 0,
                done: false,
                raised: Arc::clone(&raised),
            })
        })
    });
    // Whatever the iterator raised is the real cause of a failed load.
    if let Some(err) = raised.lock().ok().and_then(|mut slot| slot.take()) {
        return Err(err);
    }
    result.map(Some).map_err(PyErr::from)
}
