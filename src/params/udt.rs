//! SQL_SS_UDT parameters bound from Python bytes; the name resolution is
//! `arrowsql_core::params::udt`.

use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::sql_udt::UdtTypeName;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::error::Error;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes};

pub use arrowsql_core::params::udt::*;

/// A non-NULL value bound through a SQL_SS_UDT hint. The server stores the
/// serialized form as-is, so the payload is sent unchanged.
pub(crate) fn hinted_udt(py_obj: &Bound<'_, PyAny>) -> TdsResult<SqlType> {
    let payload = if let Ok(bytes) = py_obj.cast::<PyBytes>() {
        bytes.as_bytes().to_vec()
    } else if let Ok(bytes) = py_obj.cast::<PyByteArray>() {
        bytes.to_vec()
    } else {
        return Err(Error::UsageError(
            "SQL_SS_UDT parameters require bytes or bytearray values".to_string(),
        ));
    };
    Ok(SqlType::Udt(UdtTypeName::default(), Some(payload)))
}
