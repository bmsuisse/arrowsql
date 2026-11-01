//! Parallel Arrow bulk load (`write_arrow` with several workers): one Arrow
//! source, N connections, one TDS bulk-load stream each.
//!
//! The source is imported once through the Arrow C stream. Workers pull
//! zero-copy slices of its record batches from a shared cursor under a mutex,
//! so a streaming source is read on demand (never materialized) and a faster
//! connection simply takes more slices. The first failure stops the others.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::arrow_encode;
use super::arrow_source::{self, ArrowBatches};
use super::settings::{self, BulkSettings};
use super::{BulkFailure, Outcome};
use crate::statement::{self, StmtShared};

/// Cap on rows per slice. Smaller slices spread work across workers; larger
/// ones amortize per-batch overhead.
const MAX_SLICE_ROWS: usize = 65_536;
const SLICES_PER_WORKER: usize = 4;

struct SourceCursor {
    reader: ArrowBatches,
    /// The batch being cut up and the next row to hand out.
    current: Option<(RecordBatch, usize)>,
}

/// The source, shared by all workers.
struct Shared {
    cursor: Mutex<SourceCursor>,
    workers: usize,
    stop: AtomicBool,
}

impl Shared {
    /// The next non-empty slice, `None` at the end or once the load was stopped.
    fn next_slice(&self) -> Option<Result<RecordBatch, ArrowError>> {
        if self.stop.load(Ordering::Relaxed) {
            return None;
        }
        let mut cursor = self.cursor.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some((batch, offset)) = cursor.current.as_mut() {
                let rows = batch.num_rows();
                if *offset < rows {
                    let size = rows.div_ceil(self.workers * SLICES_PER_WORKER).clamp(1, MAX_SLICE_ROWS);
                    let length = size.min(rows - *offset);
                    let slice = batch.slice(*offset, length);
                    *offset += length;
                    return Some(Ok(slice));
                }
            }
            match cursor.reader.next() {
                Some(Ok(batch)) => cursor.current = Some((batch, 0)),
                Some(Err(err)) => {
                    cursor.current = None;
                    self.stop.store(true, Ordering::Relaxed);
                    return Some(Err(err));
                }
                None => return None,
            }
        }
    }
}

/// What one worker feeds its bulk load with.
struct SliceReader {
    shared: Arc<Shared>,
    schema: SchemaRef,
    first: Option<RecordBatch>,
}

impl Iterator for SliceReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.first.take() {
            Some(batch) => Some(Ok(batch)),
            None => self.shared.next_slice(),
        }
    }
}

impl RecordBatchReader for SliceReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn run_worker(
    shared: &Arc<Shared>,
    schema: &SchemaRef,
    stmt: &Arc<StmtShared>,
    settings: &BulkSettings,
) -> Result<Outcome, BulkFailure> {
    // A worker that finds no slice left opens no bulk request at all.
    let first = match shared.next_slice() {
        None => return Ok(Outcome::empty()),
        Some(Ok(batch)) => batch,
        Some(Err(err)) => {
            return Err(BulkFailure::Connection(format!("Invalid Arrow source: {err}")));
        }
    };
    let reader = SliceReader { shared: Arc::clone(shared), schema: Arc::clone(schema), first: Some(first) };
    arrow_encode::load(stmt, settings, Box::new(reader))
}

/// `DDBCBulkCopyArrowParallel`: load `source` over the connections of
/// `handles` (one statement handle each, all distinct connections). Returns
/// one result dictionary per worker; raises the first worker's error.
#[pyfunction]
#[pyo3(name = "DDBCBulkCopyArrowParallel", signature = (
    handles, table_name, source, batch_size=0, timeout=30, column_mappings=None, keep_identity=false,
    check_constraints=false, table_lock=false, keep_nulls=false, fire_triggers=false,
))]
#[allow(clippy::too_many_arguments)]
pub(super) fn bulk_copy_arrow_parallel(
    py: Python<'_>,
    handles: &Bound<'_, PyList>,
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
) -> PyResult<Py<PyList>> {
    let stmts = handles.iter().map(|h| statement::stmt_of(&h)).collect::<PyResult<Vec<_>>>()?;
    if stmts.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err("handles must not be empty"));
    }
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
        use_internal_transaction: false,
    };
    let reader = arrow_source::open(source)?;
    let schema = reader.schema();
    let shared = Arc::new(Shared {
        cursor: Mutex::new(SourceCursor { reader, current: None }),
        workers: stmts.len(),
        stop: AtomicBool::new(false),
    });

    let (outcomes, failure) = py.detach(|| {
        let failure: Mutex<Option<BulkFailure>> = Mutex::new(None);
        let outcomes: Vec<Option<Outcome>> = std::thread::scope(|scope| {
            let threads: Vec<_> = stmts
                .iter()
                .map(|stmt| {
                    let (shared, schema, settings, failure) = (&shared, &schema, &settings, &failure);
                    scope.spawn(move || match run_worker(shared, schema, stmt, settings) {
                        Ok(outcome) => Some(outcome),
                        Err(err) => {
                            shared.stop.store(true, Ordering::Relaxed);
                            let mut first = failure.lock().unwrap_or_else(|p| p.into_inner());
                            first.get_or_insert(err);
                            None
                        }
                    })
                })
                .collect();
            threads.into_iter().map(|t| t.join().unwrap_or(None)).collect()
        });
        (outcomes, failure.into_inner().unwrap_or_else(|p| p.into_inner()))
    });
    drop(shared);
    if let Some(failure) = failure {
        return Err(failure.into());
    }
    let results = PyList::empty(py);
    for outcome in outcomes.into_iter().flatten() {
        let dict: Py<PyDict> = outcome.into_dict(py)?;
        results.append(dict)?;
    }
    Ok(results.unbind())
}
