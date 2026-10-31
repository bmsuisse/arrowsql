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

use mssql_tds::datatypes::sqltypes::SqlType;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use super::exec::execute_batch;
use super::stmt_of;
use crate::params::batch;
use crate::params::placeholders::rewrite_qmarks;
use crate::params::py_types::py_to_sql_type;

/// Longest text (in characters) that still binds inline; longer values stream.
const MAX_INLINE_CHARS: usize = 4000;
/// Longest binary value that still binds inline.
const MAX_INLINE_BYTES: usize = 8000;

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
    let mut scans: Vec<ColumnScan> = (0..ncols).map(|_| ColumnScan::default()).collect();
    let (mut cells, mut nulls) = (Vec::new(), Vec::new());
    let mut push_row = |row: &Bound<'_, PyAny>| -> Option<()> {
        let mut types = Vec::with_capacity(ncols);
        let mut null_row = Vec::with_capacity(ncols);
        let mut convert = |index: usize, value: Bound<'_, PyAny>| -> Option<()> {
            let is_null = value.is_none();
            let cell = py_to_sql_type(&value).ok()?;
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
