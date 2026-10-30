//! `Connection.get_info` (SQLGetInfo).
//!
//! PARTIAL: returns plain `str` / `int` values (the Python layer passes
//! these through unchanged) for the common info types below. Unknown types
//! raise, which `Connection.getinfo` turns into `None`.
//! TODO(getinfo): fill the remaining SQLGetInfo types from msodbcsql's
//! values (mssql-rs `mssql-odbc/src/api/get_info.rs` has the full table).

use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::connection::ConnShared;

enum Info {
    Str(String),
    Int(i64),
}

pub fn get_info(py: Python<'_>, conn: &Arc<ConnShared>, info_type: u16) -> PyResult<Py<PyAny>> {
    // The slot can be held by a running query; waiting for it with the GIL
    // held would stop every other Python thread (including one that could
    // cancel that query).
    let (server_version, database, server_name) = py.detach(|| {
        let slot = conn.slot.lock();
        match slot.client.as_ref() {
            Some(c) => (
                c.server_version().map(|v| format!("{:02}.{:02}.{:04}", v.major, v.minor, v.build)),
                c.database().to_string(),
                c.server_reported_name().map(str::to_string),
            ),
            None => (None, String::new(), None),
        }
    });
    let value = match info_type {
        0 => Info::Int(0),                                   // SQL_MAX_DRIVER_CONNECTIONS
        1 => Info::Int(1),                                   // SQL_MAX_CONCURRENT_ACTIVITIES
        2 => Info::Str(String::new()),                       // SQL_DATA_SOURCE_NAME
        6 => Info::Str("libmsodbcsql-18.so".into()),         // SQL_DRIVER_NAME
        7 => Info::Str("18.06.0002".into()),                 // SQL_DRIVER_VER
        10 => Info::Str("03.80.0000".into()),                // SQL_ODBC_VER
        13 => Info::Str(server_name.unwrap_or_default()),    // SQL_SERVER_NAME
        14 => Info::Str("\\".into()),                        // SQL_SEARCH_PATTERN_ESCAPE
        16 => Info::Str(database),                           // SQL_DATABASE_NAME
        17 => Info::Str("Microsoft SQL Server".into()),      // SQL_DBMS_NAME
        18 => Info::Str(server_version.unwrap_or_default()), // SQL_DBMS_VER
        19 | 20 => Info::Str("Y".into()),                    // SQL_ACCESSIBLE_TABLES/PROCEDURES
        25 => Info::Str("N".into()),                         // SQL_DATA_SOURCE_READ_ONLY
        26 => Info::Int(2),                                  // SQL_DEFAULT_TXN_ISOLATION
        29 => Info::Str("\"".into()),                        // SQL_IDENTIFIER_QUOTE_CHAR
        30 | 35 | 32 | 34 => Info::Int(128),                 // SQL_MAX_*_NAME_LEN
        39 => Info::Str("owner".into()),                     // SQL_SCHEMA_TERM
        40 => Info::Str("stored procedure".into()),          // SQL_PROCEDURE_TERM
        41 => Info::Str(".".into()),                         // SQL_CATALOG_NAME_SEPARATOR
        42 => Info::Str("database".into()),                  // SQL_CATALOG_TERM
        45 => Info::Str("table".into()),                     // SQL_TABLE_TERM
        46 => Info::Int(2),                                  // SQL_TXN_CAPABLE = SQL_TC_ALL
        77 => Info::Str("03.80".into()),                     // SQL_DRIVER_ODBC_VER
        _ => {
            return Err(PyRuntimeError::new_err(format!(
                "SQLSTATE:HY096:NATIVE:0:Information type {info_type} not supported by Rust port"
            )));
        }
    };
    match value {
        Info::Str(s) => s.into_py_any(py),
        Info::Int(i) => i.into_py_any(py),
    }
}
