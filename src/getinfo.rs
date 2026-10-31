//! `Connection.get_info` (SQLGetInfoW).
//!
//! Returns upstream's raw result, `{"data": bytes, "length": int, "info_type": int}`:
//! character values as UTF-16LE bytes (`length` counts bytes, no terminator) and
//! numeric values as native-endian integers of their ODBC width. The Python layer
//! (`Connection.getinfo`) decodes them per information type.
//!
//! Values mirror msodbcsql 18 as reported by upstream `ddbc_bindings`. Server
//! metadata (server name, database, version, user) is read from the live
//! connection; the rest are driver constants. Information types the driver does
//! not answer are rejected with HY096, which `Connection.getinfo` turns into None.

use crate::runtime::Lockable;
use std::borrow::Cow;

use mssql_tds::connection::tds_client::StatementResult;
use mssql_tds::core::Version;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::connection::ConnShared;
use crate::decode::RowBuffer;
use crate::diag;
use crate::statement::free_for_connection;

/// One SQLGetInfo answer, before it is serialised to bytes.
enum Value {
    /// Character data (SQLGetInfoW wide string).
    Text(Cow<'static, str>),
    U16(u16),
    U32(u32),
}

fn text(value: &'static str) -> Value {
    Value::Text(Cow::Borrowed(value))
}

/// Driver constants, mirrored from the upstream capture (`SQLGetInfoW` on msodbcsql 18).
/// Information types missing here are unsupported.
fn static_value(info_type: u16) -> Option<Value> {
    Some(match info_type {
        0 => Value::U16(0),                    // SQL_MAX_DRIVER_CONNECTIONS
        1 => Value::U16(1),                    // SQL_MAX_CONCURRENT_ACTIVITIES
        2 => text(""),                         // SQL_DATA_SOURCE_NAME
        6 => text("libmsodbcsql-18.6.so.2.1"), // SQL_DRIVER_NAME
        7 => text("18.06.0002"),               // SQL_DRIVER_VER
        8 => Value::U32(191),                  // SQL_FETCH_DIRECTION
        14 => text("\\"),                      // SQL_SEARCH_PATTERN_ESCAPE
        17 => text("Microsoft SQL Server"),    // SQL_DBMS_NAME
        19 => text("Y"),                       // SQL_ACCESSIBLE_TABLES
        20 => text("Y"),                       // SQL_ACCESSIBLE_PROCEDURES
        21 => text("Y"),                       // SQL_PROCEDURES
        22 => Value::U16(0),                   // SQL_CONCAT_NULL_BEHAVIOR
        23 => Value::U16(1),                   // SQL_CURSOR_COMMIT_BEHAVIOR
        24 => Value::U16(1),                   // SQL_CURSOR_ROLLBACK_BEHAVIOR
        25 => text("N"),                       // SQL_DATA_SOURCE_READ_ONLY
        26 => Value::U32(2),                   // SQL_DEFAULT_TXN_ISOLATION
        27 => text("Y"),                       // SQL_EXPRESSIONS_IN_ORDERBY
        28 => Value::U16(4),                   // SQL_IDENTIFIER_CASE
        29 => text("\""),                      // SQL_IDENTIFIER_QUOTE_CHAR
        30 => Value::U16(128),                 // SQL_MAX_COLUMN_NAME_LEN
        32 => Value::U16(128),                 // SQL_MAX_SCHEMA_NAME_LEN
        34 => Value::U16(128),                 // SQL_MAX_CATALOG_NAME_LEN
        35 => Value::U16(128),                 // SQL_MAX_TABLE_NAME_LEN
        36 => text("Y"),                       // SQL_MULT_RESULT_SETS
        37 => text("Y"),                       // SQL_MULTIPLE_ACTIVE_TXN
        38 => text("F"),                       // SQL_OUTER_JOINS
        39 => text("owner"),                   // SQL_SCHEMA_TERM
        40 => text("stored procedure"),        // SQL_PROCEDURE_TERM
        41 => text("."),                       // SQL_CATALOG_NAME_SEPARATOR
        42 => text("database"),                // SQL_CATALOG_TERM
        43 => Value::U32(15),                  // SQL_SCROLL_CONCURRENCY
        44 => Value::U32(23),                  // SQL_SCROLL_OPTIONS
        45 => text("table"),                   // SQL_TABLE_TERM
        46 => Value::U16(2),                   // SQL_TXN_CAPABLE
        48 => Value::U32(3),                   // SQL_CONVERT_FUNCTIONS
        49 => Value::U32(16777215),            // SQL_NUMERIC_FUNCTIONS
        50 => Value::U32(5242879),             // SQL_STRING_FUNCTIONS
        51 => Value::U32(7),                   // SQL_SYSTEM_FUNCTIONS
        52 => Value::U32(2097151),             // SQL_TIMEDATE_FUNCTIONS
        72 => Value::U32(47),                  // SQL_TXN_ISOLATION_OPTION
        74 => Value::U16(2),                   // SQL_CORRELATION_NAME
        77 => text("03.52"),                   // SQL_DRIVER_ODBC_VER
        80 => Value::U32(7),                   // SQL_POSITIONED_STATEMENTS
        81 => Value::U32(20),                  // SQL_GETDATA_EXTENSIONS
        82 => Value::U32(82),                  // SQL_BOOKMARK_PERSISTENCE
        83 => Value::U32(5),                   // SQL_STATIC_SENSITIVITY
        85 => Value::U16(1),                   // SQL_NULL_COLLATION
        86 => Value::U32(39017),               // SQL_ALTER_TABLE
        87 => text("Y"),                       // SQL_COLUMN_ALIAS
        88 => Value::U16(2),                   // SQL_GROUP_BY
        89 => text(
            "BACKUP,BREAK,BROWSE,BULK,CHECKPOINT,CLUSTERED,COMMITTED,COMPUTE,CONFIRM,CONTROLROW,DATABASE,DBCC,DISK,DISTRIBUTED,DUMMY,ERRLVL,ERROREXIT,EXIT,FILE,FILLFACTOR,FLOPPY,HOLDLOCK,IDENTITY_INSERT,IDENTITYCOL,IF,KILL,LINENO,MERGE,MIRROREXIT,NONCLUSTERED,OFF,OFFSETS,ONCE,OVER,PERCENT,PERM,PERMANENT,PLAN,PRINT,PROC,PROCESSEXIT,RAISERROR,READ,READTEXT,RECONFIGURE,REPEATABLE,RESTORE,RETURN,ROWCOUNT,RULE,SAVE,SERIALIZABLE,SETUSER,SHUTDOWN,STATISTICS,TAPE,TEMP,TEXTSIZE,TOP,TRAN,TRIGGER,TRUNCATE,TSEQUEL,UNCOMMITTED,UPDATETEXT,USE,WAITFOR,WHILE,WRITETEXT",
        ), // SQL_KEYWORDS
        90 => text("N"),                       // SQL_ORDER_BY_COLUMNS_IN_SELECT
        91 => Value::U32(31),                  // SQL_SCHEMA_USAGE
        92 => Value::U32(7),                   // SQL_CATALOG_USAGE
        93 => Value::U16(4),                   // SQL_QUOTED_IDENTIFIER_CASE
        94 => text(
            "#$\u{c0}\u{c1}\u{c2}\u{c3}\u{c4}\u{c5}\u{c6}\u{c7}\u{c8}\u{c9}\u{ca}\u{cb}\u{cc}\u{cd}\u{ce}\u{cf}\u{d0}\u{d1}\u{d2}\u{d3}\u{d4}\u{d5}\u{d6}\u{d8}\u{d9}\u{da}\u{db}\u{dc}\u{dd}\u{de}\u{df}\u{e0}\u{e1}\u{e2}\u{e3}\u{e4}\u{e5}\u{e6}\u{e7}\u{e8}\u{e9}\u{ea}\u{eb}\u{ec}\u{ed}\u{ee}\u{ef}\u{f0}\u{f1}\u{f2}\u{f3}\u{f4}\u{f5}\u{f6}\u{f8}\u{f9}\u{fa}\u{fb}\u{fc}\u{fd}\u{fe}\u{ff}",
        ), // SQL_SPECIAL_CHARACTERS
        95 => Value::U32(31),                  // SQL_SUBQUERIES
        96 => Value::U32(3),                   // SQL_UNION
        97 => Value::U16(0),                   // SQL_MAX_COLUMNS_IN_GROUP_BY
        98 => Value::U16(16),                  // SQL_MAX_COLUMNS_IN_INDEX
        99 => Value::U16(0),                   // SQL_MAX_COLUMNS_IN_ORDER_BY
        100 => Value::U16(4096),               // SQL_MAX_COLUMNS_IN_SELECT
        101 => Value::U16(1024),               // SQL_MAX_COLUMNS_IN_TABLE
        104 => Value::U32(8060),               // SQL_MAX_ROW_SIZE
        105 => Value::U32(524288),             // SQL_MAX_STATEMENT_LEN
        106 => Value::U16(32),                 // SQL_MAX_TABLES_IN_SELECT
        107 => Value::U16(128),                // SQL_MAX_USER_NAME_LEN
        108 => Value::U32(524288),             // SQL_MAX_CHAR_LITERAL_LEN
        109 => Value::U32(511),                // SQL_TIMEDATE_ADD_INTERVALS
        110 => Value::U32(511),                // SQL_TIMEDATE_DIFF_INTERVALS
        111 => text("Y"),                      // SQL_NEED_LONG_DATA_LEN
        112 => Value::U32(524288),             // SQL_MAX_BINARY_LITERAL_LEN
        113 => text("Y"),                      // SQL_LIKE_ESCAPE_CLAUSE
        115 => Value::U32(127),                // SQL_OJ_CAPABILITIES
        118 => Value::U32(1),                  // SQL_SQL_CONFORMANCE
        120 => Value::U32(2),                  // SQL_BATCH_ROW_COUNT
        121 => Value::U32(15),                 // SQL_BATCH_SUPPORT
        127 => Value::U32(0),                  // SQL_CREATE_ASSERTION
        144 => Value::U32(130631),             // SQL_DYNAMIC_CURSOR_ATTRIBUTES1
        145 => Value::U32(69599),              // SQL_DYNAMIC_CURSOR_ATTRIBUTES2
        146 => Value::U32(57345),              // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
        147 => Value::U32(3983),               // SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
        150 => Value::U32(1048143),            // SQL_KEYSET_CURSOR_ATTRIBUTES1
        151 => Value::U32(73695),              // SQL_KEYSET_CURSOR_ATTRIBUTES2
        153 => Value::U32(1),                  // SQL_PARAM_ARRAY_ROW_COUNTS
        154 => Value::U32(1),                  // SQL_PARAM_ARRAY_SELECTS
        167 => Value::U32(528975),             // SQL_STATIC_CURSOR_ATTRIBUTES1
        168 => Value::U32(8065),               // SQL_STATIC_CURSOR_ATTRIBUTES2
        170 => Value::U32(3),                  // SQL_DDL_INDEX
        10000 => text("1995"),                 // SQL_XOPEN_CLI_YEAR
        10001 => Value::U32(2),                // SQL_CURSOR_SENSITIVITY
        10002 => text("Y"),                    // SQL_DESCRIBE_PARAMETER
        10003 => text("Y"),                    // SQL_CATALOG_NAME
        10005 => Value::U16(128),              // SQL_MAX_IDENTIFIER_LEN
        10021 => Value::U32(2),                // SQL_ASYNC_MODE
        _ => return None,
    })
}

/// SQLGetInfo answers that depend on the live connection.
fn server_text(py: Python<'_>, conn: &ConnShared, info_type: u16) -> PyResult<Value> {
    // Metadata is read under the connection lock, with the GIL released.
    py.detach(|| {
        let slot = conn.slot.lock_checked();
        let client = slot.client.as_ref().ok_or_else(connection_closed)?;
        let value = match info_type {
            // @@SERVERNAME, reported by the server in the login response.
            13 => client.server_reported_name().unwrap_or_default().to_string(),
            16 => client.database().to_string(),
            _ => client.server_version().map(format_version).unwrap_or_default(),
        };
        Ok(Value::Text(Cow::Owned(value)))
    })
}

/// SQL_USER_NAME: the database principal (`dbo` for `sa`), which the login response
/// does not carry. One query on the wire, run after any statement that holds it has
/// released it.
fn user_name(py: Python<'_>, conn: &ConnShared) -> PyResult<Value> {
    py.detach(|| {
        let mut slot = conn.slot.lock_checked();
        free_for_connection(&mut slot);
        let client = slot.client.as_mut().ok_or_else(connection_closed)?;
        let mut rows = RowBuffer::new();
        let sink = &mut rows;
        let outcome = client.block_on(|c| async move {
            if c.execute("SELECT USER_NAME()".to_string(), ()).await? == StatementResult::Rows {
                while c.next_row_into(sink).await? {}
            }
            c.close_query().await
        });
        let _ = client.take_info_messages();
        outcome.map_err(|err| native_error(&diag::records_for_error(&err, "HY000")))?;
        let name = rows.cells.first().and_then(|cell| rows.text(cell)).unwrap_or_default();
        Ok(Value::Text(Cow::Owned(name)))
    })
}

/// `major.minor.build` as ODBC reports SQL_DBMS_VER (for example `16.00.4305`).
fn format_version(version: Version) -> String {
    format!("{:02}.{:02}.{:04}", version.major, version.minor, version.build)
}

fn connection_closed() -> PyErr {
    PyRuntimeError::new_err("SQLSTATE:08003:NATIVE:0:Connection is closed")
}

fn native_error(records: &[diag::DiagRecord]) -> PyErr {
    match records.first() {
        Some(record) => PyRuntimeError::new_err(format!(
            "SQLSTATE:{}:NATIVE:{}:{}",
            record.sqlstate, record.native_error, record.message
        )),
        None => PyRuntimeError::new_err("SQLSTATE:HY000:NATIVE:0:Unknown error"),
    }
}

/// Serialise one answer as upstream's raw dict.
fn raw_result(py: Python<'_>, info_type: u16, value: Value) -> PyResult<Py<PyAny>> {
    let data: Vec<u8> = match value {
        Value::Text(s) => s.encode_utf16().flat_map(u16::to_le_bytes).collect(),
        Value::U16(n) => n.to_ne_bytes().to_vec(),
        Value::U32(n) => n.to_ne_bytes().to_vec(),
    };
    let result = PyDict::new(py);
    result.set_item("length", data.len())?;
    result.set_item("data", PyBytes::new(py, &data))?;
    result.set_item("info_type", info_type)?;
    Ok(result.into_any().unbind())
}

pub fn get_info(py: Python<'_>, conn: &ConnShared, info_type: u16) -> PyResult<Py<PyAny>> {
    let value = match info_type {
        13 | 16 | 18 => server_text(py, conn, info_type)?,
        47 => user_name(py, conn)?,
        _ => static_value(info_type).ok_or_else(|| {
            PyRuntimeError::new_err(format!(
                "SQLSTATE:HY096:NATIVE:0:Information type {info_type} not supported by Rust port"
            ))
        })?,
    };
    raw_result(py, info_type, value)
}
