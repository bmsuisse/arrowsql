//! `SQLExecuteManyRows`: executemany straight from the caller's rows.
//!
//! The Python layer's `executemany` inspects every cell several times (type
//! detection, Decimal formatting, transposing to columns) before the binding
//! converts each cell again. For the common case this function takes the rows
//! as given: one pass converts every cell, the batch is unified per column
//! (`params::batch`) and sent as `sp_execute` RPCs.
//!
//! It never guesses. A batch it cannot run exactly as the Python layer would
//! (ragged rows, a column mixing kinds, long strings that stream as
//! data-at-execution, Decimals next to NULLs, anything that fails to convert)
//! returns `None` before a byte is sent, and the caller runs its general path,
//! which also reports the error.

use std::mem::{Discriminant, discriminant};

use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::sqltypes::SqlType;
use pyo3::prelude::*;
use mssql_tds::datatypes::column_values::{SqlDateTime2, SqlTime};
use pyo3::types::{PyDateAccess, PyDateTime, PyList, PyTimeAccess, PyTuple, PyType, PyTzInfoAccess};

use super::exec::execute_batch;
use super::stmt_of;
use crate::params::batch;
use crate::params::placeholders::rewrite_qmarks;
use crate::params::py_types::py_to_sql_type;

/// Longest text (in characters) that still binds inline; longer values stream.
const MAX_INLINE_CHARS: usize = 4000;
/// Longest binary value that still binds inline.
const MAX_INLINE_BYTES: usize = 8000;

/// SQL Server's maximum DECIMAL precision.
const MAX_DECIMAL_DIGITS: usize = 38;

/// A `decimal.Decimal` as a NUMERIC cell, read from its plain `str()` form.
/// `None` for anything else (subclasses, exponent notation, NaN, more than 38
/// digits), which the general converter then handles or rejects.
fn decimal_cell(value: &Bound<'_, PyAny>, decimal_type: &Bound<'_, PyType>) -> Option<SqlType> {
    if !value.get_type().is(decimal_type) {
        return None;
    }
    let text = value.str().ok()?;
    let text = text.to_str().ok()?;
    let unsigned = text.strip_prefix('-');
    let (mut magnitude, mut int_digits, mut scale) = (0u128, 0usize, 0usize);
    let mut seen_point = false;
    let mut digit_count = 0usize;
    for byte in unsigned.unwrap_or(text).bytes() {
        match byte {
            b'0'..=b'9' => {
                digit_count += 1;
                magnitude = magnitude * 10 + u128::from(byte - b'0');
                if seen_point {
                    scale += 1;
                } else if magnitude != 0 {
                    int_digits += 1;
                }
            }
            b'.' if !seen_point => seen_point = true,
            _ => return None,
        }
    }
    let int_digits = int_digits.max(1);
    let precision = int_digits + scale;
    if digit_count == 0 || digit_count > MAX_DECIMAL_DIGITS || precision > MAX_DECIMAL_DIGITS {
        return None;
    }
    let positive = unsigned.is_none() || magnitude == 0;
    // Both fit in u8: they are at most 38.
    Some(SqlType::Numeric(Some(DecimalParts::new(positive, precision as u8, scale as u8, magnitude))))
}

/// A naive `datetime.datetime` as DATETIME2(7), read through the C-level
/// accessors. Same result as `py_to_sql_type`, without its attribute lookups.
/// `None` for subclasses and timezone-aware values, which the general
/// converter handles.
fn datetime_cell(value: &Bound<'_, PyAny>, datetime_type: &Bound<'_, PyType>) -> Option<SqlType> {
    if !value.get_type().is(datetime_type) {
        return None;
    }
    let dt = value.cast::<PyDateTime>().ok()?;
    if dt.get_tzinfo().is_some() {
        return None;
    }
    let (year, month, day) = (i64::from(dt.get_year()), i64::from(dt.get_month()), i64::from(dt.get_day()));
    // Howard Hinnant's days_from_civil, shifted from 1970-01-01 to 0001-01-01.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468 + 719_162;
    let days = u32::try_from(days).ok()?;
    let seconds = u64::from(dt.get_hour()) * 3600 + u64::from(dt.get_minute()) * 60 + u64::from(dt.get_second());
    Some(SqlType::DateTime2(Some(SqlDateTime2 {
        days,
        time: SqlTime { time_nanoseconds: seconds * 10_000_000 + u64::from(dt.get_microsecond()) * 10, scale: 7 },
    })))
}

/// What a column holds. Cells of one column must share a kind.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Booleans, integers and floats mix freely; `batch::normalize` unifies them.
    Number,
    Decimal,
    Text,
    Binary,
    /// Any other type: dates, times, UUIDs. One SQL type per column.
    Other(Discriminant<SqlType>),
}

/// The kind of a non-NULL cell, or `None` when it cannot bind inline.
fn kind_of(cell: &SqlType) -> Option<Kind> {
    Some(match cell {
        SqlType::Bit(_) | SqlType::TinyInt(_) | SqlType::SmallInt(_) | SqlType::Int(_) | SqlType::BigInt(_) | SqlType::Float(_) => Kind::Number,
        SqlType::Decimal(_) | SqlType::Numeric(_) => Kind::Decimal,
        SqlType::Varchar(_, n) | SqlType::NVarchar(_, n) if usize::from(*n) <= MAX_INLINE_CHARS => Kind::Text,
        SqlType::VarBinary(_, n) if usize::from(*n) <= MAX_INLINE_BYTES => Kind::Binary,
        SqlType::Varchar(..) | SqlType::NVarchar(..) | SqlType::VarcharMax(_) | SqlType::NVarcharMax(_) => return None,
        SqlType::VarBinary(..) | SqlType::VarBinaryMax(_) => return None,
        other => Kind::Other(discriminant(other)),
    })
}

/// Per-column bookkeeping while the rows are converted.
#[derive(Default)]
struct ColumnScan {
    kind: Option<Kind>,
    has_null: bool,
}

impl ColumnScan {
    /// Records one cell; `false` when the column can no longer take the fast path.
    fn accept(&mut self, cell: &SqlType, is_null: bool) -> bool {
        if is_null {
            self.has_null = true;
        } else {
            let Some(kind) = kind_of(cell) else { return false };
            if *self.kind.get_or_insert(kind) != kind {
                return false;
            }
        }
        // A NULL Decimal has no precision to declare the column with.
        !(self.has_null && self.kind == Some(Kind::Decimal))
    }
}

/// Converts every row, or `None` when the batch needs the general path.
fn convert_rows(rows: &Bound<'_, PyAny>, ncols: usize) -> Option<(Vec<Vec<SqlType>>, Vec<Vec<bool>>)> {
    let decimal_type = rows.py().import("decimal").and_then(|module| module.getattr("Decimal")).ok()?;
    let decimal_type = decimal_type.cast_into::<PyType>().ok()?;
    let datetime_type = rows.py().import("datetime").and_then(|module| module.getattr("datetime")).ok()?;
    let datetime_type = datetime_type.cast_into::<PyType>().ok()?;
    let mut scans: Vec<ColumnScan> = (0..ncols).map(|_| ColumnScan::default()).collect();
    let (mut cells, mut nulls) = (Vec::new(), Vec::new());
    let mut push_row = |row: &Bound<'_, PyAny>| -> Option<()> {
        let mut types = Vec::with_capacity(ncols);
        let mut null_row = Vec::with_capacity(ncols);
        let mut convert = |index: usize, value: Bound<'_, PyAny>| -> Option<()> {
            let is_null = value.is_none();
            let cell = match datetime_cell(&value, &datetime_type).or_else(|| decimal_cell(&value, &decimal_type)) {
                Some(cell) => cell,
                None => py_to_sql_type(&value).ok()?,
            };
            if !scans[index].accept(&cell, is_null) {
                return None;
            }
            types.push(cell);
            null_row.push(is_null);
            Some(())
        };
        if let Ok(tuple) = row.cast::<PyTuple>() {
            if tuple.len() != ncols {
                return None;
            }
            for (index, value) in tuple.iter().enumerate() {
                convert(index, value)?;
            }
        } else if let Ok(list) = row.cast::<PyList>() {
            if list.len() != ncols {
                return None;
            }
            for (index, value) in list.iter().enumerate() {
                convert(index, value)?;
            }
        } else {
            return None;
        }
        cells.push(types);
        nulls.push(null_row);
        Some(())
    };
    if let Ok(list) = rows.cast::<PyList>() {
        list.iter().try_for_each(|row| push_row(&row))?;
    } else if let Ok(tuple) = rows.cast::<PyTuple>() {
        tuple.iter().try_for_each(|row| push_row(&row))?;
    } else {
        return None;
    }
    Some((cells, nulls))
}

/// Runs `query` once per row of `rows`. Returns the ODBC return code, or
/// `None` (nothing sent) when the caller must use its general path.
#[pyfunction]
#[pyo3(name = "SQLExecuteManyRows", signature = (statementHandle, query, rows))]
#[allow(non_snake_case)]
pub fn execute_many_rows(
    py: Python<'_>,
    statementHandle: &Bound<'_, PyAny>,
    query: String,
    rows: &Bound<'_, PyAny>,
) -> PyResult<Option<i16>> {
    let stmt = stmt_of(statementHandle)?;
    let Some(ncols) = first_row_len(rows) else { return Ok(None) };
    let (sql, markers) = rewrite_qmarks(&query);
    if ncols == 0 || markers != ncols {
        return Ok(None);
    }
    let Some((mut cells, nulls)) = convert_rows(rows, ncols) else { return Ok(None) };
    let Ok(all_null) = batch::normalize(&mut cells, &nulls) else { return Ok(None) };
    let ret = py.detach(|| execute_batch(&stmt, sql, cells, all_null));
    Ok(Some(ret))
}

fn first_row_len(rows: &Bound<'_, PyAny>) -> Option<usize> {
    let first = if let Ok(list) = rows.cast::<PyList>() {
        list.iter().next()?
    } else {
        rows.cast::<PyTuple>().ok()?.iter().next()?
    };
    first.cast::<PyTuple>().map(|t| t.len()).or_else(|_| first.cast::<PyList>().map(|l| l.len())).ok()
}
