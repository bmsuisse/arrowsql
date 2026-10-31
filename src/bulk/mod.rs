//! Native bulk copy (`Cursor.bulkcopy` / `Cursor.bulkcopy_arrow`) on the
//! cursor's own connection, replacing the separate `mssql_py_core` connection
//! upstream opens per call.
//!
//! Running on the cursor's connection means a bulk load into a `#temp` table
//! sees that table (upstream issue #492) and, in manual-commit mode, takes
//! part in the open transaction like any other statement.
//!
//! * `arrow_source`: Arrow PyCapsules -> a record batch stream.
//! * `arrow_encode`: per-column plans from (Arrow type, SQL type) and the
//!   cell encoders that feed mssql-tds's `BulkLoadRow`.
//! * `settings`: option and column-mapping parsing, destination mapping.
//!
//! Anything the native encoders cannot represent raises [`BulkUnsupported`]
//! before a byte is sent, so the Python layer can hand the call to
//! `mssql_py_core` when it is installed.

mod arrow_encode;
mod arrow_source;
mod arrow_wire;
mod rows;
mod wire;
mod settings;

use std::sync::Arc;
use std::time::Duration;

use mssql_tds::connection::bulk_copy::{BulkCopy, BulkCopyResult, BulkLoadRow, ColumnMapping};
use mssql_tds::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
use mssql_tds::error::Error as TdsError;
use mssql_tds::error::bulk_copy_errors::BulkCopyError;
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::connection::error_text;
use crate::consts::*;
use crate::diag;
use crate::runtime::SyncClient;
use crate::statement::{self, StmtShared, with_exclusive_wire};
use settings::{BulkSettings, Resolved, SourceShape};

pyo3::create_exception!(
    ddbc_bindings,
    BulkUnsupported,
    PyValueError,
    "The native bulk copy cannot load this source into this destination."
);

/// Seconds to wait for the server to acknowledge an abandoned bulk request.
const RECOVER_SETTLE_SECS: u32 = 5;

/// Why a bulk copy did not complete.
enum BulkFailure {
    /// Rejected before anything was sent; the caller may use another engine.
    Unsupported(String),
    Tds(TdsError),
    /// The connection could not be taken (closed, freed handle).
    Connection(String),
}

impl From<BulkFailure> for PyErr {
    fn from(failure: BulkFailure) -> PyErr {
        match failure {
            BulkFailure::Unsupported(message) => BulkUnsupported::new_err(message),
            BulkFailure::Connection(message) => PyRuntimeError::new_err(message),
            BulkFailure::Tds(err) => tds_error_to_py(err),
        }
    }
}

/// A SQL Server error in the `SQLSTATE:<state>:NATIVE:<n>:<message>` form, so the
/// Python layer raises the same DB-API class `execute` does for it.
fn server_error_to_py(err: &TdsError) -> PyErr {
    match diag::records_for_error(err, "HY000").first() {
        Some(record) => PyRuntimeError::new_err(error_text(record)),
        None => PyRuntimeError::new_err(err.to_string()),
    }
}

/// The exception classes `mssql_py_core` raised for the same failures.
fn tds_error_to_py(err: TdsError) -> PyErr {
    if matches!(err, TdsError::SqlServerError { .. }) {
        return server_error_to_py(&err);
    }
    match err {
        TdsError::UsageError(message) | TdsError::TypeConversionError(message) => PyValueError::new_err(message),
        TdsError::EncodedValueTooLong { .. } => PyValueError::new_err(err.to_string()),
        TdsError::ProtocolError(message) => PyRuntimeError::new_err(format!("Protocol Error: {message}")),
        TdsError::BulkCopyError(BulkCopyError::Timeout(timeout)) => PyTimeoutError::new_err(format!(
            "Bulk copy timeout: the operation timed out after {} seconds ({} rows copied){}",
            timeout.timeout_seconds,
            timeout.rows_copied,
            timeout.context.as_ref().map(|c| format!(". {c}")).unwrap_or_default()
        )),
        TdsError::BulkCopyError(BulkCopyError::AttentionTimeout(attention)) => {
            PyTimeoutError::new_err(if attention.connection_broken {
                "Attention acknowledgment not received within 5 seconds. Connection has been broken."
            } else {
                "Attention acknowledgment not received within 5 seconds."
            })
        }
        TdsError::BulkCopyError(BulkCopyError::ConnectionBroken(message)) => {
            PyRuntimeError::new_err(format!("Connection is broken: {message}"))
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// What a finished load reports to Python.
pub struct Outcome {
    result: BulkCopyResult,
    batch_size: usize,
}

impl Outcome {
    /// The result of a source without any rows.
    fn empty() -> Outcome {
        Outcome { result: BulkCopyResult::new(0, Duration::ZERO), batch_size: 0 }
    }

    fn into_dict(self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let rows = self.result.rows_affected;
        let seconds = self.result.elapsed.as_secs_f64();
        let batches = if self.batch_size > 0 { rows.div_ceil(self.batch_size as u64) } else { 1 };
        let dict = PyDict::new(py);
        dict.set_item("rows_copied", rows)?;
        dict.set_item("batch_count", batches)?;
        dict.set_item("elapsed_time", seconds)?;
        dict.set_item("rows_per_second", if seconds > 0.0 { rows as f64 / seconds } else { 0.0 })?;
        Ok(dict.unbind())
    }
}

/// Restore the client to a usable state after a failed bulk request.
fn recover(client: &mut SyncClient) {
    if client.has_open_batch() {
        let _ = client.block_on(|c| c.cancel_query(RECOVER_SETTLE_SECS));
    }
    let _ = client.take_info_messages();
}

/// Run `load` while holding the connection's wire. The GIL must not be held.
fn with_wire(
    stmt: &Arc<StmtShared>,
    internal_transaction: bool,
    load: impl FnOnce(&mut SyncClient) -> Result<Outcome, BulkFailure>,
) -> Result<Outcome, BulkFailure> {
    let mut outcome = None;
    let code = with_exclusive_wire(stmt, |ex| {
        // In manual-commit mode the driver has begun an (empty) transaction
        // for this call, which would make use_internal_transaction fail.
        // Upstream loads over a separate connection and never sees this.
        // Drop the empty one for the load (the next statement begins a new
        // one); a transaction holding the caller's work stays, and mssql-tds
        // reports the conflict, as SqlBulkCopy does.
        if internal_transaction && ex.fresh_txn {
            let _ = ex.client.block_on(|c| c.rollback_transaction(None, None));
        }
        let result = load(ex.client);
        if matches!(result, Err(BulkFailure::Tds(_))) {
            recover(ex.client);
        }
        let ok = result.is_ok();
        outcome = Some(result);
        if ok { SQL_SUCCESS } else { SQL_ERROR }
    });
    outcome.unwrap_or_else(|| {
        let detail = statement::diag_records(stmt)
            .into_iter()
            .map(|record| record.message)
            .collect::<Vec<_>>()
            .join("; ");
        let detail = if detail.is_empty() { format!("return code {code}") } else { detail };
        Err(BulkFailure::Connection(format!("Bulk copy could not start: {detail}")))
    })
}

/// Resolve the destination, plan the source's encoders with `plan`, and load
/// the rows it yields. Nothing is sent to the server before planning succeeds.
fn run<I, R>(
    stmt: &Arc<StmtShared>,
    settings: &BulkSettings,
    shape: &SourceShape,
    plan: impl FnOnce(&[BulkCopyColumnMetadata], &[Resolved]) -> Result<I, BulkFailure>,
) -> Result<Outcome, BulkFailure>
where
    I: Iterator<Item = R>,
    R: BulkLoadRow,
{
    with_wire(stmt, settings.use_internal_transaction, |client| {
        client.block_on(|client| async move {
            let mut bulk = BulkCopy::new(client, settings.table_name.clone())
                .batch_size(settings.batch_size)
                .timeout(Duration::from_secs(settings.timeout_secs))
                .check_constraints(settings.check_constraints)
                .fire_triggers(settings.fire_triggers)
                .keep_identity(settings.keep_identity)
                .keep_nulls(settings.keep_nulls)
                .table_lock(settings.table_lock)
                .use_internal_transaction(settings.use_internal_transaction);
            let destination = bulk.retrieve_destination_metadata().await.map_err(BulkFailure::Tds)?;
            let resolved = settings::resolve(settings, shape, &destination).map_err(BulkFailure::Unsupported)?;
            let rows = plan(&destination, &resolved)?;
            for mapping in &resolved {
                bulk = bulk.add_column_mapping(ColumnMapping::by_ordinal(
                    mapping.source,
                    mapping.destination_name.clone(),
                ));
            }
            let result = bulk.write_to_server_zerocopy(rows).await.map_err(BulkFailure::Tds)?;
            Ok(Outcome { result, batch_size: settings.batch_size })
        })
    })
}

/// `DDBCBulkCopyArrow`: load an Arrow source (anything with
/// `__arrow_c_stream__` / `__arrow_c_array__`). Returns the result dictionary
/// of `Cursor.bulkcopy_arrow`; raises [`BulkUnsupported`] when the schema
/// cannot be encoded natively.
#[pyfunction]
#[pyo3(name = "DDBCBulkCopyArrow", signature = (
    handle, table_name, source, batch_size=0, timeout=30, column_mappings=None, keep_identity=false,
    check_constraints=false, table_lock=false, keep_nulls=false, fire_triggers=false,
    use_internal_transaction=false,
))]
#[allow(clippy::too_many_arguments)]
fn bulk_copy_arrow(
    py: Python<'_>,
    handle: &Bound<'_, PyAny>,
    table_name: String,
    source: &Bound<'_, PyAny>,
    batch_size: usize,
    timeout: u64,
    column_mappings: Option<&Bound<'_, PyAny>>,
    keep_identity: bool,
    check_constraints: bool,
    table_lock: bool,
    keep_nulls: bool,
    fire_triggers: bool,
    use_internal_transaction: bool,
) -> PyResult<Py<PyDict>> {
    let stmt = statement::stmt_of(handle)?;
    let settings = BulkSettings {
        table_name,
        batch_size,
        timeout_secs: timeout,
        mappings: settings::parse_mappings(column_mappings)?,
        keep_identity,
        check_constraints,
        table_lock,
        keep_nulls,
        fire_triggers,
        use_internal_transaction,
    };
    let stream = arrow_source::open(source)?;
    let outcome = py.detach(|| arrow_encode::load(&stmt, &settings, stream))?;
    outcome.into_dict(py)
}

/// `DDBCBulkCopyRows`: load an iterator of row tuples. Returns the result
/// dictionary of `Cursor.bulkcopy`.
#[pyfunction]
#[pyo3(name = "DDBCBulkCopyRows", signature = (
    handle, table_name, rows, batch_size=0, timeout=30, column_mappings=None, keep_identity=false,
    check_constraints=false, table_lock=false, keep_nulls=false, fire_triggers=false,
    use_internal_transaction=false,
))]
#[allow(clippy::too_many_arguments)]
fn bulk_copy_rows(
    py: Python<'_>,
    handle: &Bound<'_, PyAny>,
    table_name: String,
    rows: &Bound<'_, PyAny>,
    batch_size: usize,
    timeout: u64,
    column_mappings: Option<&Bound<'_, PyAny>>,
    keep_identity: bool,
    check_constraints: bool,
    table_lock: bool,
    keep_nulls: bool,
    fire_triggers: bool,
    use_internal_transaction: bool,
) -> PyResult<Py<PyDict>> {
    let stmt = statement::stmt_of(handle)?;
    let settings = BulkSettings {
        table_name,
        batch_size,
        timeout_secs: timeout,
        mappings: settings::parse_mappings(column_mappings)?,
        keep_identity,
        check_constraints,
        table_lock,
        keep_nulls,
        fire_triggers,
        use_internal_transaction,
    };
    match rows::load(py, &stmt, &settings, rows)? {
        Some(outcome) => outcome.into_dict(py),
        None => Outcome::empty().into_dict(py),
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("BulkUnsupported", m.py().get_type::<BulkUnsupported>())?;
    m.add_function(wrap_pyfunction!(bulk_copy_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(bulk_copy_rows, m)?)?;
    Ok(())
}
