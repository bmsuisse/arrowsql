//! The error type of the public API.

use std::fmt;

use crate::diag::DiagRecord;

/// What kind of failure an [`Error`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// SQL Server or the driver reported a diagnostic (`sqlstate` is set).
    Database,
    /// An argument has the wrong type (e.g. a partition column that is not
    /// an integer column).
    Type,
    /// An argument has an invalid value (e.g. bounds that do not ascend).
    Value,
    /// The call does not fit the object's state (e.g. a statement without a
    /// result set read as Arrow).
    State,
    /// A host callback (token provider, connection factory) failed; the
    /// original error is the [`source`](std::error::Error::source).
    External,
}

/// A failed call. Database errors carry the ODBC-style SQLSTATE, the server's
/// error number (`native`, 0 for driver errors) and the message of the first
/// diagnostic record; [`records`](Error::records) has all of them.
#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub sqlstate: String,
    pub native: i32,
    pub message: String,
    records: Vec<DiagRecord>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    /// The error for the first of `records` (HY000 when there are none).
    pub fn from_records(records: Vec<DiagRecord>) -> Error {
        let first = records
            .first()
            .cloned()
            .unwrap_or_else(|| DiagRecord::new("HY000", 0, "Unknown error"));
        Error {
            kind: ErrorKind::Database,
            sqlstate: first.sqlstate,
            native: first.native_error,
            message: first.message,
            records,
            source: None,
        }
    }

    /// A driver-originated diagnostic.
    pub fn driver(sqlstate: &str, message: impl AsRef<str>) -> Error {
        Error::from_records(vec![DiagRecord::driver(sqlstate, message)])
    }

    pub fn type_error(message: impl Into<String>) -> Error {
        Error::plain(ErrorKind::Type, message.into())
    }

    pub fn value_error(message: impl Into<String>) -> Error {
        Error::plain(ErrorKind::Value, message.into())
    }

    pub fn state_error(message: impl Into<String>) -> Error {
        Error::plain(ErrorKind::State, message.into())
    }

    /// Wrap a host callback's error; [`into_source`](Error::into_source) gives it back.
    pub fn external(source: Box<dyn std::error::Error + Send + Sync>) -> Error {
        let message = source.to_string();
        Error {
            source: Some(source),
            ..Error::plain(ErrorKind::External, message)
        }
    }

    fn plain(kind: ErrorKind, message: String) -> Error {
        Error {
            kind,
            sqlstate: String::new(),
            native: 0,
            message,
            records: Vec::new(),
            source: None,
        }
    }

    /// Every diagnostic record of a database error, in server order.
    pub fn records(&self) -> &[DiagRecord] {
        &self.records
    }

    /// The wrapped host error of an [`ErrorKind::External`] error.
    pub fn into_source(self) -> Option<Box<dyn std::error::Error + Send + Sync>> {
        self.source
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sqlstate.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "[{}] {}", self.sqlstate, self.message)
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl From<mssql_tds::error::Error> for Error {
    fn from(err: mssql_tds::error::Error) -> Error {
        Error::from_records(crate::diag::records_for_error(&err, "HY000"))
    }
}
