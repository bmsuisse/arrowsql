//! Handing Arrow data to Python through the Arrow C Data / C Stream
//! interfaces (PyCapsules), so no Arrow library is needed on the Python side.
//!
//! * `batch_capsules`: the upstream `DDBCSQLFetchArrowBatch` contract
//!   (`arrow_schema` + `arrow_array` capsules of a struct array).
//! * `ArrowTable`: a materialized result; `__arrow_c_stream__` can be called
//!   any number of times (DuckDB peeks the schema, then scans).
//! * `ArrowStream`: rows still arriving. A producer thread decodes batch N+1
//!   while the consumer works on batch N (bounded queue). Exporting it without
//!   reading a batch (a schema peek) leaves it available for the next export.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, SchemaRef};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple};

use super::plan::Plan;
use super::sink::ArrowSink;
use crate::consts::*;
use crate::statement::{self, BatchSink, StmtShared};

fn arrow_err(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Append the schema and array capsules of `batch` (as a struct array).
pub fn batch_capsules(py: Python<'_>, batch: RecordBatch, capsules: &Bound<'_, PyList>) -> PyResult<()> {
    let data = StructArray::from(batch).into_data();
    let schema = FFI_ArrowSchema::try_from(data.data_type()).map_err(arrow_err)?;
    let array = FFI_ArrowArray::new(&data);
    capsules.append(PyCapsule::new_with_value(py, schema, c"arrow_schema")?)?;
    capsules.append(PyCapsule::new_with_value(py, array, c"arrow_array")?)?;
    Ok(())
}

fn stream_capsule(py: Python<'_>, reader: Box<dyn RecordBatchReader + Send>) -> PyResult<Bound<'_, PyCapsule>> {
    PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(reader), c"arrow_array_stream")
}

fn schema_capsule<'py>(py: Python<'py>, schema: &SchemaRef) -> PyResult<Bound<'py, PyCapsule>> {
    let ffi = FFI_ArrowSchema::try_from(schema.as_ref()).map_err(arrow_err)?;
    PyCapsule::new_with_value(py, ffi, c"arrow_schema")
}

/// A fully fetched result. Zero-copy for any Arrow consumer.
#[pyclass(name = "ArrowTable", module = "mssql_python.ddbc_bindings", frozen)]
pub struct ArrowTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl ArrowTable {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> ArrowTable {
        ArrowTable { schema, batches }
    }
}

#[pymethods]
impl ArrowTable {
    #[getter]
    fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    #[getter]
    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    fn __len__(&self) -> usize {
        self.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "ArrowTable(num_rows={}, num_columns={}, columns={:?})",
            self.num_rows(),
            self.num_columns(),
            self.column_names()
        )
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema; // optional per the protocol; we export as-is
        let batches = self.batches.clone().into_iter().map(Ok);
        stream_capsule(py, Box::new(RecordBatchIterator::new(batches, self.schema.clone())))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, &self.schema)
    }
}

type BatchResult = Result<RecordBatch, String>;

/// Shared by the `ArrowStream` object and any C stream exported from it.
struct StreamCore {
    schema: SchemaRef,
    stmt: Arc<StmtShared>,
    generation: u64,
    rx: Mutex<Option<Receiver<BatchResult>>>,
    producer: Mutex<Option<JoinHandle<()>>>,
    /// An exported C stream exists that hasn't been released yet.
    exporting: AtomicBool,
    /// A batch was handed out: the stream can't be exported again.
    consumed: AtomicBool,
    /// The producer reached the end of the result.
    exhausted: Arc<AtomicBool>,
}

impl StreamCore {
    /// Stop the producer and give the connection back: unread rows of the
    /// result are discarded unless it was read to the end.
    fn shutdown(&self) {
        drop(self.rx.lock().unwrap().take()); // a blocked producer's send fails
        if let Some(handle) = self.producer.lock().unwrap().take() {
            let _ = handle.join();
        }
        if !self.exhausted.load(Ordering::Acquire) {
            statement::abandon_result(&self.stmt, self.generation);
        }
    }
}

impl Drop for StreamCore {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn produce(
    stmt: Arc<StmtShared>,
    plan: Plan,
    generation: u64,
    batch_rows: usize,
    tx: SyncSender<BatchResult>,
    exhausted: Arc<AtomicBool>,
) {
    loop {
        let mut sink = ArrowSink::new(&plan, batch_rows);
        let fetched = statement::fetch_into(&stmt, &mut sink, batch_rows, Some(generation));
        if fetched.stale {
            return;
        }
        if fetched.ret == SQL_ERROR {
            let message = statement::diag_records(&stmt)
                .iter()
                .map(|r| format!("[{}] {}", r.sqlstate, r.message))
                .collect::<Vec<_>>()
                .join("; ");
            exhausted.store(true, Ordering::Release);
            let _ = tx.send(Err(message));
            return;
        }
        if sink.rows() == 0 {
            exhausted.store(true, Ordering::Release);
            return;
        }
        if tx.send(sink.finish()).is_err() {
            return;
        }
    }
}

/// One exported C stream over an `ArrowStream`.
struct ExportReader {
    core: Arc<StreamCore>,
    pulled: bool,
}

impl Iterator for ExportReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pulled = true;
        self.core.consumed.store(true, Ordering::Release);
        let rx = self.core.rx.lock().unwrap();
        match rx.as_ref()?.recv() {
            Ok(Ok(batch)) => Some(Ok(batch)),
            Ok(Err(message)) => Some(Err(ArrowError::ExternalError(message.into()))),
            Err(_) => None, // producer finished
        }
    }
}

impl RecordBatchReader for ExportReader {
    fn schema(&self) -> SchemaRef {
        self.core.schema.clone()
    }
}

impl Drop for ExportReader {
    fn drop(&mut self) {
        self.core.exporting.store(false, Ordering::Release);
        if self.pulled {
            self.core.shutdown();
        }
    }
}

/// A result being streamed as Arrow record batches (one-shot).
#[pyclass(name = "ArrowStream", module = "mssql_python.ddbc_bindings", frozen)]
pub struct ArrowStream {
    core: Arc<StreamCore>,
}

impl ArrowStream {
    pub fn start(stmt: Arc<StmtShared>, plan: Plan, generation: u64, batch_rows: usize, depth: usize) -> ArrowStream {
        let (tx, rx) = sync_channel(depth.max(1));
        let exhausted = Arc::new(AtomicBool::new(false));
        let producer = {
            let (stmt, plan, exhausted) = (stmt.clone(), plan.clone(), exhausted.clone());
            std::thread::Builder::new()
                .name("mssql-rs-arrow".into())
                .spawn(move || produce(stmt, plan, generation, batch_rows, tx, exhausted))
                .expect("failed to spawn Arrow producer thread")
        };
        ArrowStream {
            core: Arc::new(StreamCore {
                schema: plan.schema,
                stmt,
                generation,
                rx: Mutex::new(Some(rx)),
                producer: Mutex::new(Some(producer)),
                exporting: AtomicBool::new(false),
                consumed: AtomicBool::new(false),
                exhausted,
            }),
        }
    }
}

#[pymethods]
impl ArrowStream {
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.core.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.core.schema.fields().len()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.rx.lock().unwrap().is_none()
    }

    /// Stop streaming and discard unread rows; the cursor stays usable.
    fn close(&self, py: Python<'_>) {
        py.detach(|| self.core.shutdown());
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) {
        self.close(py);
    }

    /// Read all remaining batches into an `ArrowTable`.
    fn read_all(&self, py: Python<'_>) -> PyResult<ArrowTable> {
        let core = self.core.clone();
        let mut reader = take_reader(&core)?;
        let batches = py.detach(|| reader.by_ref().collect::<Result<Vec<_>, _>>());
        drop(reader);
        Ok(ArrowTable::new(core.schema.clone(), batches.map_err(arrow_err)?))
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = take_reader(&self.core)?;
        stream_capsule(py, Box::new(reader))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, &self.core.schema)
    }

    fn __repr__(&self) -> String {
        format!("ArrowStream(columns={:?}, closed={})", self.column_names(), self.closed())
    }
}

fn take_reader(core: &Arc<StreamCore>) -> PyResult<ExportReader> {
    if core.rx.lock().unwrap().is_none() {
        return Err(PyValueError::new_err("ArrowStream is closed"));
    }
    if core.consumed.load(Ordering::Acquire) {
        return Err(PyValueError::new_err("ArrowStream was already consumed"));
    }
    if core.exporting.swap(true, Ordering::AcqRel) {
        return Err(PyValueError::new_err("ArrowStream is already being read"));
    }
    Ok(ExportReader { core: core.clone(), pulled: false })
}
