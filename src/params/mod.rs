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
pub mod udt;

use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PySequence, PyTuple};

use py_types::{ParameterHint, py_to_sql_type, py_to_sql_type_with_hint};

/// Parses one `setinputsizes()` entry. cursor.py always builds a
/// `(sql_type, c_type, column_size, decimal_digits)` tuple; `None` leaves the
/// parameter to inference. Malformed entries raise, as upstream's tuple cast does.
fn parse_hint(item: &Bound<'_, PyAny>) -> PyResult<Option<ParameterHint>> {
    if item.is_none() {
        return Ok(None);
    }
    let fields = item.cast::<PyTuple>().map_err(|_| {
        PyTypeError::new_err("setinputsizes entries must be (sql_type, c_type, size, scale) tuples")
    })?;
    if fields.len() != 4 {
        return Err(PyValueError::new_err(format!(
            "setinputsizes entries must have 4 fields, got {}",
            fields.len()
        )));
    }
    let sql_type = fields.get_item(0)?.extract::<i32>()?;
    let c_type = fields.get_item(1)?.extract::<i32>()?;
    let size = u32::try_from(saturating_size(&fields.get_item(2)?)?).unwrap_or(u32::MAX);
    let scale = u8::try_from(saturating_size(&fields.get_item(3)?)?).unwrap_or(u8::MAX);
    ParameterHint::new(sql_type, size, scale)
        .map(|hint| Some(hint.with_c_type(c_type)))
        .map_err(|error| conversion_error(&error))
}

/// Reads a non-negative `setinputsizes()` integer. Values beyond `u64` saturate
/// rather than fail, so the caller clamps oversized precision and scale to the
/// SQL Server limits as upstream does.
fn saturating_size(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    if !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(
            "setinputsizes column size and decimal digits must be integers",
        ));
    }
    if let Ok(number) = value.extract::<u64>() {
        return Ok(number);
    }
    if value.lt(0)? {
        return Err(PyValueError::new_err(
            "setinputsizes column size and decimal digits must be non-negative",
        ));
    }
    Ok(u64::MAX)
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

/// Binds `params` as RPC parameters. `codec` encodes narrow text bound as
/// `SQL_C_CHAR` (see [`py_types::narrow_codec`]); `None` leaves text to inference.
pub fn build_rpc_params(
    params: &Bound<'_, PyAny>,
    input_sizes: Option<&Bound<'_, PyAny>>,
    codec: Option<&str>,
) -> PyResult<ParamSet> {
    let seq = params.cast::<PySequence>()?;
    let hints: Vec<Option<ParameterHint>> = match input_sizes {
        Some(sizes) if !sizes.is_none() => sizes
            .cast::<PyList>()
            .map_err(|_| PyTypeError::new_err("setinputsizes must be a list"))?
            .iter()
            .map(|entry| parse_hint(&entry))
            .collect::<PyResult<_>>()?,
        _ => Vec::new(),
    };
    let len = seq.len()?;
    let mut types = Vec::with_capacity(len);
    let mut untyped_nulls = Vec::new();
    for i in 0..len {
        let value = seq.get_item(i)?;
        // A NULL whose hint carries metadata a SQL NULL cannot hold is typed by
        // the server, the same as an untyped NULL.
        let hint = hints
            .get(i)
            .copied()
            .flatten()
            .filter(|hint| !(value.is_none() && hint.null_needs_server_type()));
        if hint.is_none() && value.is_none() {
            untyped_nulls.push(i);
        }
        let sql_type = match hint {
            Some(hint) => py_to_sql_type_with_hint(&value, hint, codec),
            None => py_to_sql_type(&value),
        }
        .map_err(|e| conversion_error(&e))?;
        types.push(sql_type);
    }
    Ok(ParamSet { types, untyped_nulls })
}

/// Maps a conversion failure to the Python exception upstream raises for it.
///
/// `TypeConversionError` marks a value the target type cannot represent
/// (overflow, out of range, precision, non-finite) and raises `ValueError`.
/// `UsageError` marks a value of a type the driver cannot bind and raises
/// `TypeError`. The message is passed through unprefixed so it matches upstream.
pub fn conversion_error(err: &mssql_tds::error::Error) -> PyErr {
    match err {
        mssql_tds::error::Error::TypeConversionError(msg) => PyValueError::new_err(msg.clone()),
        mssql_tds::error::Error::UsageError(msg) => PyTypeError::new_err(msg.clone()),
        other => PyTypeError::new_err(other.to_string()),
    }
}
