//! Diagnostic records (ODBC's SQLGetDiagRec model) and SQLSTATE mapping.
//!
//! Each handle owns a `Vec<DiagRecord>` that is cleared at the start of every
//! operation and filled on error / info. `DDBCSQLCheckError` reads the first
//! record; `DDBCSQLGetAllDiagRecords` returns all of them as
//! `("[SQLSTATE] (native)", message)` tuples, the exact shape the C++ binding
//! produced.
//!
//! To map a new SQL Server error number to a SQLSTATE, add it (sorted) to
//! `SERVER_ERROR_TO_SQLSTATE`. Table and severity tiers are taken from
//! microsoft/mssql-rs `mssql-odbc/src/api/sqlstate.rs` (MIT), which mirrors
//! msodbcsql.

use pyo3::prelude::*;
use mssql_tds::error::{Error as TdsError, SqlInfoMessage};

/// Prefix msodbcsql puts on every message; kept so message text matches
/// what upstream tests and user code grep for.
pub const DRIVER_PREFIX: &str = "[Microsoft][ODBC Driver 18 for SQL Server]";
const SERVER_SUBSOURCE: &str = "[SQL Server]";

#[derive(Debug, Clone)]
pub struct DiagRecord {
    pub sqlstate: String,
    pub native_error: i32,
    pub message: String,
}

impl DiagRecord {
    pub fn new(sqlstate: &str, native_error: i32, message: impl Into<String>) -> Self {
        Self {
            sqlstate: sqlstate.to_string(),
            native_error,
            message: message.into(),
        }
    }

    /// A driver-originated record (no `[SQL Server]` sub-source).
    pub fn driver(sqlstate: &str, message: impl AsRef<str>) -> Self {
        Self::new(sqlstate, 0, format!("{DRIVER_PREFIX}{}", message.as_ref()))
    }

    pub fn not_implemented(function: &str) -> Self {
        Self::driver("IM001", format!("{function} not implemented in Rust port"))
    }
}

const SERVER_ERROR_TO_SQLSTATE: &[(u32, &str)] = &[
    (109, "21S01"), (110, "21S01"), (120, "07008"), (121, "07008"), (168, "22003"),
    (206, "22018"), (207, "42S22"), (208, "42S02"), (210, "22007"), (211, "22007"),
    (213, "21S01"), (220, "22003"), (229, "42000"), (230, "42000"), (232, "22003"),
    (233, "23000"), (234, "22003"), (235, "22018"), (236, "22003"), (237, "22003"),
    (238, "22003"), (241, "22007"), (242, "22007"), (244, "22003"), (245, "22018"),
    (246, "22003"), (248, "22003"), (256, "22018"), (266, "25000"), (267, "42S02"),
    (272, "23000"), (273, "23000"), (295, "22007"), (296, "22007"), (305, "42000"),
    (310, "22025"), (409, "22018"), (512, "21000"), (515, "23000"), (517, "22007"),
    (518, "22018"), (529, "22018"), (535, "22003"), (544, "23000"), (547, "23000"),
    (550, "44000"), (628, "25000"), (911, "42000"), (916, "08004"), (919, "01000"),
    (1007, "22003"), (1010, "22019"), (1205, "40001"), (1211, "40001"), (1505, "23000"),
    (1508, "23000"), (1906, "42S02"), (1911, "42S22"), (1913, "42S11"), (2501, "42S02"),
    (2601, "23000"), (2615, "23000"), (2627, "23000"), (2628, "22001"), (2705, "42S21"), (2706, "42S02"),
    (2714, "42S01"), (2727, "42S21"), (2740, "08004"), (3605, "23000"), (3606, "01000"),
    (3607, "01000"), (3622, "01000"), (3701, "42S02"), (3718, "42S12"), (3902, "25000"),
    (3903, "25000"), (3906, "25000"), (3908, "25000"), (4002, "28000"), (4017, "08004"),
    (4019, "08004"), (4401, "42S02"), (4409, "21S02"), (4501, "21S02"), (4502, "21S02"),
    (4506, "42S21"), (4701, "42S02"), (4902, "42S02"), (4924, "42S22"), (5701, "01000"),
    (5703, "01000"), (6401, "25000"), (7112, "40001"), (8101, "23000"), (8115, "22003"),
    (8134, "22012"), (8152, "22001"), (8153, "01003"), (16902, "HY109"), (16916, "34000"),
    (16930, "24000"), (16931, "24000"), (16934, "01001"), (16947, "01001"), (17809, "08004"),
    (18450, "08004"), (18452, "28000"), (18456, "28000"), (18458, "08004"), (18459, "28000"),
    (18463, "28000"), (18464, "28000"), (18465, "28000"), (18466, "28000"), (18467, "28000"),
    (18468, "28000"), (18487, "28000"), (18488, "28000"),
];

pub fn sqlstate_for_server_error(number: u32, class: i32) -> &'static str {
    match SERVER_ERROR_TO_SQLSTATE.binary_search_by_key(&number, |&(n, _)| n) {
        Ok(i) => SERVER_ERROR_TO_SQLSTATE[i].1,
        // msodbcsql severity tiers: >18 HY000, >10 42000, else 01000.
        Err(_) if class > 18 => "HY000",
        Err(_) if class > 10 => "42000",
        Err(_) => "01000",
    }
}

/// Python-visible `sqlstate_for_server_error(number, severity=None)`; no
/// severity means the most severe tier (HY000) for unmapped numbers.
#[pyfunction]
#[pyo3(name = "sqlstate_for_server_error", signature = (number, severity=None))]
pub fn sqlstate_for_server_error_py(number: u32, severity: Option<i32>) -> &'static str {
    sqlstate_for_server_error(number, severity.unwrap_or(i32::MAX))
}

/// Server error raised to a request that was running when its session was
/// KILLed. The server then closes the connection, so the driver reports it
/// as a driver error, the way msodbcsql does.
const SESSION_KILLED_ERROR: u32 = 596;
const SESSION_KILLED_MESSAGE: &str =
    "Unspecified error occurred on SQL Server. Connection may have been terminated by the server.";

fn server_record(number: u32, class: i32, message: &str) -> DiagRecord {
    if number == SESSION_KILLED_ERROR {
        return DiagRecord::new(
            "HY000",
            i32::try_from(number).unwrap_or(i32::MAX),
            format!("{DRIVER_PREFIX}{SESSION_KILLED_MESSAGE}"),
        );
    }
    DiagRecord::new(
        sqlstate_for_server_error(number, class),
        i32::try_from(number).unwrap_or(i32::MAX),
        format!("{DRIVER_PREFIX}{SERVER_SUBSOURCE}{message}"),
    )
}

pub fn info_records(messages: &[SqlInfoMessage]) -> impl Iterator<Item = DiagRecord> + '_ {
    messages
        .iter()
        .map(|m| server_record(m.number, m.class, &m.message))
}

/// Convert a TDS error into diagnostic records. `default_state` is used for
/// failures the server did not raise (`08001` at connect, `HY000` otherwise).
pub fn records_for_error(err: &TdsError, default_state: &str) -> Vec<DiagRecord> {
    match err {
        TdsError::SqlServerError { diagnostics } => {
            let mut out: Vec<DiagRecord> = diagnostics
                .errors
                .iter()
                .map(|e| server_record(e.number, e.class, &e.message))
                .collect();
            if out.is_empty() {
                out.push(DiagRecord::driver(default_state, err.to_string()));
            }
            out.extend(info_records(&diagnostics.info_messages));
            out
        }
        TdsError::TimeoutError(_) => vec![DiagRecord::driver("HYT00", "Query timeout expired")],
        TdsError::OperationCancelledError(_) => {
            vec![DiagRecord::driver("HY008", "Operation canceled")]
        }
        TdsError::ConnectionResetNotAcknowledged => {
            vec![DiagRecord::driver("08S01", "Communication link failure")]
        }
        TdsError::EncodedValueTooLong { .. } => {
            vec![DiagRecord::driver("22001", format!("String data, right truncation: {err}"))]
        }
        TdsError::Io(_) | TdsError::ConnectionClosed(_) => {
            let state = if default_state == "08001" { "08001" } else { "08S01" };
            vec![DiagRecord::driver(state, format!("Communication link failure: {err}"))]
        }
        _ => vec![DiagRecord::driver(default_state, err.to_string())],
    }
}

/// Records for a failure while logging in. Server errors raised by the login
/// carry SQLSTATE 28000 whatever their number (msodbcsql reports a database
/// that cannot be opened as "Invalid authorization specification"); other
/// failures keep the connect default `08001`.
pub fn login_records_for_error(err: &TdsError) -> Vec<DiagRecord> {
    match err {
        TdsError::SqlServerError { diagnostics } if !diagnostics.errors.is_empty() => {
            let mut out: Vec<DiagRecord> = diagnostics
                .errors
                .iter()
                .map(|e| {
                    let message = format!("{DRIVER_PREFIX}{SERVER_SUBSOURCE}{}", e.message);
                    DiagRecord::new("28000", i32::try_from(e.number).unwrap_or(i32::MAX), message)
                })
                .collect();
            out.extend(info_records(&diagnostics.info_messages));
            out
        }
        _ => records_for_error(err, "08001"),
    }
}

/// Python-visible formatting used by GetAllDiagRecords: `"[STATE] (native)"`.
pub fn state_label(record: &DiagRecord) -> String {
    format!("[{}] ({})", record.sqlstate, record.native_error)
}
