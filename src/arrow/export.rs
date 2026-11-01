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
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

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
use crate::statement::{self, BatchSink, StmtCancel, StmtShared};

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
#[pyclass(name = "ArrowTable", module = "arrowsql.ddbc_bindings", frozen)]
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
    /// One table holding the batches of `tables`, which must share a schema
    /// (nullability may differ; the result's fields are nullable if any is).
    #[staticmethod]
    fn concat(tables: Vec<PyRef<'_, ArrowTable>>) -> PyResult<ArrowTable> {
        let Some(first) = tables.first() else {
            return Err(PyValueError::new_err("concat() needs at least one table"));
        };
        let mut fields: Vec<arrow_schema::Field> =
            first.schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        for table in &tables[1..] {
            let other = table.schema.fields();
            let same = other.len() == fields.len()
                && fields.iter().zip(other.iter()).all(|(a, b)| {
                    a.name() == b.name() && a.data_type() == b.data_type()
                });
            if !same {
                return Err(PyValueError::new_err("concat() needs tables with the same columns"));
            }
            for (field, theirs) in fields.iter_mut().zip(other.iter()) {
                if theirs.is_nullable() && !field.is_nullable() {
                    *field = field.clone().with_nullable(true);
                }
            }
        }
        let schema: SchemaRef = Arc::new(arrow_schema::Schema::new(fields));
        let mut batches = Vec::new();
        for table in &tables {
            for batch in &table.batches {
                batches.push(batch.clone().with_schema(schema.clone()).map_err(arrow_err)?);
            }
        }
        Ok(ArrowTable::new(schema, batches))
    }

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
    /// Cancellation of the execution this stream reads: closing the stream
    /// interrupts that execution only. The statement's current execution may
    /// be a newer query on the same cursor.
    execution: Option<Arc<StmtCancel>>,
    rx: Mutex<Option<Receiver<BatchResult>>>,
    /// `close()` was called: readers end, `rx` may still be held by one.
    closing: AtomicBool,
    producer: Mutex<Option<JoinHandle<()>>>,
    /// An exported C stream exists that hasn't been released yet.
    exporting: AtomicBool,
    /// A batch was handed out: the stream can't be exported again.
    consumed: AtomicBool,
    /// The producer reached the end of the result.
    exhausted: Arc<AtomicBool>,
    /// The Python cursor, kept alive by the stream and by every C stream
    /// exported from it: `cur.execute(q).arrow_stream()` leaves no other
    /// reference, and freeing the cursor would free the statement mid-stream.
    /// (Dropped without the GIL on a consumer thread, pyo3 defers the decref.)
    _owner: Option<Py<PyAny>>,
}

impl StreamCore {
    /// Stop the producer and give the connection back: unread rows of the
    /// result are discarded unless it was read to the end.
    ///
    /// Never waits for a reader: one parked in `recv` holds the `rx` lock, so
    /// the producer is interrupted first and the reader wakes once it is gone.
    fn shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        if let Some(handle) = self.producer.lock().unwrap().take() {
            let mut backoff = Duration::from_micros(50);
            while !handle.is_finished() {
                // A producer blocked in `send` wakes when the receiver goes;
                // one blocked inside a read (e.g. on a row lock) never sees the
                // closed channel: interrupt its read until it is gone.
                if let Ok(mut rx) = self.rx.try_lock() {
                    drop(rx.take());
                }
                if let Some(execution) = &self.execution {
                    execution.cancel();
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(5));
            }
            let _ = handle.join();
            if !self.exhausted.load(Ordering::Acquire) {
                statement::abandon_result(&self.stmt, self.generation);
            }
        }
        // The producer is gone, so a reader in `recv` has returned.
        drop(self.rx.lock().unwrap().take());
    }
}

impl Drop for StreamCore {
    fn drop(&mut self) {
        Python::attach(|py| py.detach(|| self.shutdown()));
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
        if self.core.closing.load(Ordering::Acquire) {
            return None;
        }
        let rx = self.core.rx.lock().unwrap();
        match rx.as_ref()?.recv() {
            _ if self.core.closing.load(Ordering::Acquire) => None,
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
            Python::attach(|py| py.detach(|| self.core.shutdown()));
        }
    }
}

/// A result being streamed as Arrow record batches (one-shot).
#[pyclass(name = "ArrowStream", module = "arrowsql.ddbc_bindings", frozen)]
pub struct ArrowStream {
    core: Arc<StreamCore>,
}

impl ArrowStream {
    pub fn start(
        stmt: Arc<StmtShared>,
        plan: Plan,
        generation: u64,
        execution: Option<Arc<StmtCancel>>,
        batch_rows: usize,
        depth: usize,
        owner: Option<Py<PyAny>>,
    ) -> ArrowStream {
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
                execution,
                rx: Mutex::new(Some(rx)),
                closing: AtomicBool::new(false),
                producer: Mutex::new(Some(producer)),
                exporting: AtomicBool::new(false),
                consumed: AtomicBool::new(false),
                exhausted,
                _owner: owner,
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
        self.core.closing.load(Ordering::Acquire)
    }

    /// Stop streaming and discard unread rows; the cursor stays usable.
    fn close(&self, py: Python<'_>) {
        py.detach(|| self.core.shutdown());
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
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
    if core.closing.load(Ordering::Acquire) {
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

/// Shared by the `ArrowMergeStream` object, the worker threads that feed it
/// and any C stream exported from it.
struct MergeCore {
    schema: SchemaRef,
    /// Master sender, cloned per `pump`; dropped when the last worker is done,
    /// which ends the merged stream.
    tx: Mutex<Option<SyncSender<BatchResult>>>,
    workers: AtomicUsize,
    rx: Mutex<Option<Receiver<BatchResult>>>,
    /// The consumer side is finished or closed.
    closing: AtomicBool,
    /// Workers must stop (closed, or a worker failed).
    stop: AtomicBool,
    /// Streams being pumped right now, so a stop can interrupt their reads.
    active: Mutex<Vec<Arc<StreamCore>>>,
    exporting: AtomicBool,
    consumed: AtomicBool,
}

impl MergeCore {
    /// Interrupt every stream being pumped (workers then see `stop`).
    fn cancel_active(&self) {
        let active: Vec<Arc<StreamCore>> = self.active.lock().unwrap().clone();
        for core in active {
            core.shutdown();
        }
    }

    /// End the merged stream: discard buffered batches and stop the workers.
    /// Never waits for a reader parked in `recv` (it wakes when the workers are
    /// done and the last sender is gone).
    fn shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        self.stop.store(true, Ordering::Release);
        self.cancel_active();
        if let Ok(mut rx) = self.rx.try_lock() {
            drop(rx.take());
        }
    }

    fn send_error(&self, message: String) {
        self.stop.store(true, Ordering::Release);
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            let _ = tx.send(Err(message));
        }
        self.cancel_active();
    }
}

struct MergeReader {
    core: Arc<MergeCore>,
    pulled: bool,
}

impl Iterator for MergeReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pulled = true;
        self.core.consumed.store(true, Ordering::Release);
        if self.core.closing.load(Ordering::Acquire) {
            return None;
        }
        let item = {
            let rx = self.core.rx.lock().unwrap();
            rx.as_ref()?.recv()
        };
        match item {
            _ if self.core.closing.load(Ordering::Acquire) => None,
            Ok(Ok(batch)) => Some(Ok(batch)),
            Ok(Err(message)) => {
                self.core.shutdown();
                Some(Err(ArrowError::ExternalError(message.into())))
            }
            Err(_) => None, // every worker is done
        }
    }
}

impl RecordBatchReader for MergeReader {
    fn schema(&self) -> SchemaRef {
        self.core.schema.clone()
    }
}

impl Drop for MergeReader {
    fn drop(&mut self) {
        self.core.exporting.store(false, Ordering::Release);
        if self.pulled {
            Python::attach(|py| py.detach(|| self.core.shutdown()));
        }
    }
}

/// Batches of several `ArrowStream`s (one per worker connection) as one
/// unordered stream with a bounded buffer. Worker threads call `pump` with the
/// stream of each slice they read; the consumer exports the merge as an Arrow
/// C stream (one-shot). `close()` / dropping the exported stream early stops
/// the workers and cancels their statements.
#[pyclass(name = "ArrowMergeStream", module = "arrowsql.ddbc_bindings", frozen)]
pub struct ArrowMergeStream {
    core: Arc<MergeCore>,
}

#[pymethods]
impl ArrowMergeStream {
    /// `schema_from`: a stream with the result's columns (it is not consumed).
    /// `workers`: how many threads will call `worker_done()`. `depth`: batches
    /// buffered between the workers and the consumer.
    #[new]
    #[pyo3(signature = (schema_from, workers, depth=8))]
    fn new(schema_from: &ArrowStream, workers: usize, depth: usize) -> ArrowMergeStream {
        let (tx, rx) = sync_channel(depth.max(1));
        ArrowMergeStream {
            core: Arc::new(MergeCore {
                schema: schema_from.core.schema.clone(),
                tx: Mutex::new(if workers == 0 { None } else { Some(tx) }),
                workers: AtomicUsize::new(workers),
                rx: Mutex::new(Some(rx)),
                closing: AtomicBool::new(false),
                stop: AtomicBool::new(false),
                active: Mutex::new(Vec::new()),
                exporting: AtomicBool::new(false),
                consumed: AtomicBool::new(false),
            }),
        }
    }

    /// Move every batch of `stream` into the merge (blocks, without the GIL,
    /// until the stream ends, the merge stops or a read fails; a failure is
    /// delivered to the consumer). The stream is closed on return.
    fn pump(&self, py: Python<'_>, stream: &ArrowStream) -> PyResult<()> {
        let core = self.core.clone();
        let source = stream.core.clone();
        py.detach(move || {
            let mut reader = take_reader(&source)?;
            core.active.lock().unwrap().push(source.clone());
            let tx = core.tx.lock().unwrap().clone();
            if let Some(tx) = tx {
                while !core.stop.load(Ordering::Acquire) {
                    match reader.next() {
                        None => break,
                        Some(Ok(batch)) => {
                            let batch = if batch.schema() == core.schema {
                                batch
                            } else {
                                batch.clone().with_schema(core.schema.clone()).unwrap_or(batch)
                            };
                            if tx.send(Ok(batch)).is_err() {
                                break;
                            }
                        }
                        Some(Err(err)) => {
                            core.send_error(err.to_string());
                            break;
                        }
                    }
                }
            }
            drop(reader);
            core.active.lock().unwrap().retain(|c| !Arc::ptr_eq(c, &source));
            Ok(())
        })
    }

    /// A worker is finished; the last one ends the stream.
    fn worker_done(&self) {
        if self.core.workers.fetch_sub(1, Ordering::AcqRel) == 1 {
            drop(self.core.tx.lock().unwrap().take());
        }
    }

    /// Report a worker failure to the consumer and stop the other workers.
    fn fail(&self, py: Python<'_>, message: String) {
        let core = self.core.clone();
        py.detach(move || core.send_error(message));
    }

    /// Workers should stop picking up work (closed or failed).
    #[getter]
    fn stopped(&self) -> bool {
        self.core.stop.load(Ordering::Acquire)
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.closing.load(Ordering::Acquire)
    }

    /// The stream was handed to a consumer (export in progress or done).
    #[getter]
    fn in_use(&self) -> bool {
        self.core.exporting.load(Ordering::Acquire) || self.core.consumed.load(Ordering::Acquire)
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.core.schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.core.schema.fields().len()
    }

    /// Stop the workers and discard unread batches.
    fn close(&self, py: Python<'_>) {
        let core = self.core.clone();
        py.detach(move || core.shutdown());
    }

    /// Read all remaining batches into an `ArrowTable` (Ctrl-C interrupts).
    fn read_all(&self, py: Python<'_>) -> PyResult<ArrowTable> {
        let core = self.core.clone();
        let _reader = take_merge_reader(&core)?;
        let mut batches = Vec::new();
        loop {
            if core.closing.load(Ordering::Acquire) {
                break;
            }
            let item = py.detach(|| {
                let rx = core.rx.lock().unwrap();
                match rx.as_ref() {
                    Some(rx) => rx.recv_timeout(Duration::from_millis(100)),
                    None => Err(RecvTimeoutError::Disconnected),
                }
            });
            match item {
                Ok(Ok(batch)) => batches.push(batch),
                Ok(Err(message)) => {
                    py.detach(|| core.shutdown());
                    return Err(PyRuntimeError::new_err(message));
                }
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(err) = py.check_signals() {
                        py.detach(|| core.shutdown());
                        return Err(err);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        core.consumed.store(true, Ordering::Release);
        py.detach(|| core.shutdown());
        Ok(ArrowTable::new(core.schema.clone(), batches))
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = take_merge_reader(&self.core)?;
        stream_capsule(py, Box::new(reader))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, &self.core.schema)
    }
}

fn take_merge_reader(core: &Arc<MergeCore>) -> PyResult<MergeReader> {
    if core.closing.load(Ordering::Acquire) {
        return Err(PyValueError::new_err("stream is closed"));
    }
    if core.consumed.load(Ordering::Acquire) {
        return Err(PyValueError::new_err("stream was already consumed"));
    }
    if core.exporting.swap(true, Ordering::AcqRel) {
        return Err(PyValueError::new_err("stream is already being read"));
    }
    Ok(MergeReader { core: core.clone(), pulled: false })
}
