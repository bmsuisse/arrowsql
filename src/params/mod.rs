//! Parameter binding: Python values -> `RpcParameter`s for `sp_executesql`.
//!
//! - `placeholders`: `?` -> `@Pn` rewriting.
//! - `py_types`: per-value type inference (vendored from mssql-py-core).
//!
//! TODO(params): `inputSizes` (setinputsizes) is accepted but only the SQL
//! type of each hint is applied; NULL parameters default to NVARCHAR(1)
//! unless a hint gives the type (upstream used SQLDescribeParam).

pub mod batch;
pub mod null_types;
pub mod placeholders;
pub mod py_types;

use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use pyo3::prelude::*;
use pyo3::types::{PyList, PySequence};

use py_types::{ParameterHint, py_to_sql_type, py_to_sql_type_with_hint};

/// Parse `setinputsizes()` entries as handed to DDBCSQLExecute. Each entry is
/// either an int SQL type or a (sql_type, size, scale) tuple; anything else
/// is ignored.
fn parse_hint(item: &Bound<'_, PyAny>) -> Option<ParameterHint> {
    if item.is_none() {
        return None;
    }
    if let Ok(sql_type) = item.extract::<i32>() {
        return ParameterHint::new(sql_type, 0, 0).ok();
    }
    if let Ok(seq) = item.cast::<PySequence>() {
        let sql_type = seq.get_item(0).ok()?.extract::<i32>().ok()?;
        let size = seq.get_item(1).ok().and_then(|v| v.extract::<u32>().ok()).unwrap_or(0);
        let scale = seq.get_item(2).ok().and_then(|v| v.extract::<u8>().ok()).unwrap_or(0);
        return ParameterHint::new(sql_type, size, scale).ok();
    }
    None
}

/// Converted parameters, before naming. `untyped_nulls` lists positions of
/// `None` values with no type hint: their SQL type is resolved against the
/// server (`resolve_null_types`) because no single NULL type converts
/// implicitly to every column type (e.g. nvarchar -> varbinary fails).
pub struct ParamSet {
    pub types: Vec<SqlType>,
    pub untyped_nulls: Vec<usize>,
}

impl ParamSet {
    pub fn into_rpc(self) -> Vec<RpcParameter> {
        self.types
            .into_iter()
            .enumerate()
            .map(|(i, t)| RpcParameter::new(Some(format!("@P{}", i + 1)), StatusFlags::NONE, t))
            .collect()
    }
}

pub fn build_rpc_params(
    params: &Bound<'_, PyAny>,
    input_sizes: Option<&Bound<'_, PyAny>>,
) -> PyResult<ParamSet> {
    let seq = params.cast::<PySequence>()?;
    let hints: Vec<Option<ParameterHint>> = match input_sizes {
        Some(sizes) if !sizes.is_none() => match sizes.cast::<PyList>() {
            Ok(list) => list.iter().map(|i| parse_hint(&i)).collect(),
            Err(_) => Vec::new(),
        },
        _ => Vec::new(),
    };
    let len = seq.len()?;
    let mut types = Vec::with_capacity(len);
    let mut untyped_nulls = Vec::new();
    for i in 0..len {
        let value = seq.get_item(i)?;
        let hint = hints.get(i).copied().flatten();
        if hint.is_none() && value.is_none() {
            untyped_nulls.push(i);
        }
        let sql_type = match hint {
            Some(hint) => py_to_sql_type_with_hint(&value, hint),
            None => py_to_sql_type(&value),
        }
        .map_err(|e| conversion_error(&e))?;
        types.push(sql_type);
    }
    Ok(ParamSet { types, untyped_nulls })
}

/// Out-of-range values raise ValueError (like the C++ binding); values of an
/// unsupported type raise TypeError.
pub fn conversion_error(err: &mssql_tds::error::Error) -> PyErr {
    let msg = err.to_string();
    let lower = msg.to_ascii_lowercase();
    if lower.contains("precision") {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Precision of the numeric value is too high - the maximum precision supported by SQL Server is 38 ({msg})"
        ))
    } else if lower.contains("range") || lower.contains("overflow") {
        pyo3::exceptions::PyValueError::new_err(msg)
    } else {
        pyo3::exceptions::PyTypeError::new_err(msg)
    }
}
