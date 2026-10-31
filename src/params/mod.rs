//! Parameter binding: Python values -> `RpcParameter`s for `sp_executesql`.
//!
//! - `placeholders`: `?` -> `@Pn` rewriting.
//! - `py_types`: per-value type inference (vendored from mssql-py-core).
//!
//! `inputSizes` (setinputsizes) hints supply the SQL type, size, scale and
//! C type of each parameter; an untyped NULL is typed from the server
//! (`sp_describe_undeclared_parameters`, see `null_types`), as upstream did
//! with SQLDescribeParam.

pub mod batch;
pub mod null_types;
pub mod placeholders;
pub mod py_types;
pub mod udt;

use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyUnicodeEncodeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyInt, PyList, PyModule, PySequence, PyTuple, PyType};

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

/// `@Pn` without going through the `format!` machinery: it runs once per
/// parameter per row in `executemany`.
pub(crate) fn param_name(n: usize) -> String {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    let mut rest = n;
    loop {
        at -= 1;
        digits[at] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let mut name = String::with_capacity(2 + digits.len() - at);
    name.push_str("@P");
    for &d in &digits[at..] {
        name.push(d as char);
    }
    name
}

impl ParamSet {
    pub fn into_rpc(self) -> Vec<RpcParameter> {
        self.types
            .into_iter()
            .enumerate()
            .map(|(i, t)| RpcParameter::new(Some(param_name(i + 1)), StatusFlags::NONE, t))
            .collect()
    }

    /// The parameters without names, for `execute_sp_executesql_positional`,
    /// which names parameter `n` `@Pn` itself.
    pub fn into_unnamed_rpc(self) -> Vec<RpcParameter> {
        self.types
            .into_iter()
            .map(|t| RpcParameter::new(None, StatusFlags::NONE, t))
            .collect()
    }
}

/// Statements with more parameters than this are bulk inserts or long IN lists.
/// Their uninformed small ints bind as INT instead of the narrowest type: the
/// declaration is part of the plan cache key, so one that follows the value
/// (tinyint, then smallint, then int across a run of multi-row INSERTs) compiles
/// extra plans, and a plan over a thousand parameters takes ~10 ms to compile.
const WIDE_INT_PARAM_COUNT: usize = 100;

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
            None => match crate::statement::batch_rpc::fast_param_cell(&value) {
                Some(cell) => Ok(cell),
                None => py_to_sql_type(&value),
            },
        }
        .map_err(|e| conversion_error(&e))?;
        let sql_type = match sql_type {
            SqlType::TinyInt(Some(v)) if hint.is_none() && len > WIDE_INT_PARAM_COUNT => {
                SqlType::Int(Some(i32::from(v)))
            }
            SqlType::SmallInt(Some(v)) if hint.is_none() && len > WIDE_INT_PARAM_COUNT => {
                SqlType::Int(Some(i32::from(v)))
            }
            other => other,
        };
        types.push(sql_type);
    }
    Ok(ParamSet { types, untyped_nulls })
}

/// Prefix of a conversion message for a value SQL Server would reject with a
/// data exception (SQLSTATE 22xxx). Raised as PEP 249 `DataError`.
pub(crate) const DATA_ERROR_PREFIX: &str = "data exception: ";

/// Prefix of a conversion message that is both a `DataError` and, for backward
/// compatibility with earlier releases, a `ValueError`.
pub(crate) const DATA_VALUE_ERROR_PREFIX: &str = "data value exception: ";

/// Prefix of a conversion message that upstream raises as a bare `RuntimeError`
/// (a tz offset outside SQL Server's range).
pub(crate) const RUNTIME_ERROR_PREFIX: &str = "runtime error: ";

/// Maps a conversion failure to the Python exception upstream raises for it.
///
/// `TypeConversionError` marks a value the target type cannot represent
/// (overflow, out of range, precision, non-finite) and raises `ValueError`,
/// unless its message carries a data-exception prefix. `UsageError` marks a
/// value of a type the driver cannot bind and raises `TypeError`. Messages are
/// passed through unprefixed so they match upstream.
pub fn conversion_error(err: &mssql_tds::error::Error) -> PyErr {
    match err {
        mssql_tds::error::Error::TypeConversionError(msg) => {
            if let Some(text) = msg.strip_prefix(DATA_VALUE_ERROR_PREFIX) {
                data_error(text, true)
            } else if let Some(text) = msg.strip_prefix(DATA_ERROR_PREFIX) {
                data_error(text, false)
            } else if let Some(text) = msg.strip_prefix(RUNTIME_ERROR_PREFIX) {
                PyRuntimeError::new_err(text.to_string())
            } else {
                PyValueError::new_err(msg.clone())
            }
        }
        mssql_tds::error::Error::UsageError(msg) => match msg.strip_prefix(UNICODE_ENCODE_PREFIX) {
            Some(reason) => PyErr::new::<PyUnicodeEncodeError, _>((
                "utf-8",
                "",
                0usize,
                1usize,
                reason.to_string(),
            )),
            None => PyTypeError::new_err(msg.clone()),
        },
        other => PyTypeError::new_err(other.to_string()),
    }
}

/// Marks a `UsageError` for a str that cannot be encoded (a lone surrogate); the
/// rest of the message is the encoder's reason. Raised as `UnicodeEncodeError`.
pub(crate) const UNICODE_ENCODE_PREFIX: &str = "unicode encode error: ";

static DATA_VALUE_ERROR_CLASS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Builds a `DataError` (or a `DataError` that is also a `ValueError`). Falls back
/// to a plain `ValueError` when the exception module cannot be loaded.
fn data_error(message: &str, also_value_error: bool) -> PyErr {
    Python::attach(|py| {
        let class = if also_value_error {
            data_value_error_class(py)
        } else {
            PyModule::import(py, "arrowsql.exceptions")
                .and_then(|module| module.getattr("DataError"))
        };
        // `Error` takes (driver_error, ddbc_error) like the errors raised for server diagnostics.
        match class.and_then(|class| class.call1(("Data exception", message))) {
            Ok(instance) => PyErr::from_value(instance),
            Err(_) => PyValueError::new_err(message.to_string()),
        }
    })
}

/// The class `DataError` + `ValueError`, created once per interpreter.
fn data_value_error_class(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let class = DATA_VALUE_ERROR_CLASS.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let data_error = PyModule::import(py, "arrowsql.exceptions")?.getattr("DataError")?;
        let bases = PyTuple::new(py, [data_error, py.get_type::<PyValueError>().into_any()])?;
        // str() is the bare message, as upstream's ValueError reads: its parity
        // tests compare it byte for byte (test_023 test_integer_overflow_detected).
        let namespace = PyDict::new(py);
        namespace.set_item("__str__", py.eval(c"lambda self: self.ddbc_error", None, None)?)?;
        let class = py.get_type::<PyType>().call1(("DataValueError", bases, namespace))?;
        Ok(class.unbind())
    })?;
    Ok(class.bind(py).clone())
}

/// executemany: a string longer than its column's declared size (from
/// `setinputsizes`) is a data error, as it is for `execute()`. Declared sizes
/// the Python layer derives from the data itself are never exceeded.
pub fn check_text_sizes(param_infos: &Bound<'_, PyAny>, columns: &[Bound<'_, PyAny>]) -> PyResult<()> {
    use crate::consts::{SQL_CHAR, SQL_VARCHAR, SQL_WCHAR, SQL_WVARCHAR};
    let Ok(infos) = param_infos.cast::<PyList>() else { return Ok(()) };
    for (info, column) in infos.iter().zip(columns) {
        let Ok(info) = info.cast::<crate::types::ParamInfo>() else { continue };
        let (sql_type, declared) = {
            let info = info.borrow();
            (info.paramSQLType, info.columnSize)
        };
        if declared == 0 || !matches!(sql_type, SQL_CHAR | SQL_VARCHAR | SQL_WCHAR | SQL_WVARCHAR) {
            continue;
        }
        for value in column.try_iter()? {
            let value = value?;
            if value.is_instance_of::<pyo3::types::PyString>() {
                let actual = value.len()? as u64;
                if actual > declared {
                    return Err(conversion_error(&mssql_tds::error::Error::TypeConversionError(format!(
                        "{DATA_ERROR_PREFIX}String or binary data would be truncated: a value of \
                         {actual} units exceeds the declared size of {declared}"
                    ))));
                }
            }
        }
    }
    Ok(())
}
