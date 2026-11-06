//! Handing Arrow data to Python through the Arrow C Data / C Stream
//! interfaces (PyCapsules), so no Arrow library is needed on the Python side.
//!
//! * `batch_capsules`: the upstream `DDBCSQLFetchArrowBatch` contract
//!   (`arrow_schema` + `arrow_array` capsules of a struct array).
//! * `ArrowTable`: a materialized result; `__arrow_c_stream__` can be called
//!   any number of times (DuckDB peeks the schema, then scans).
//! * `ArrowStream`: rows still arriving (`arrowsql_core::arrow::stream`). A
//!   producer thread decodes batch N+1 while the consumer works on batch N
//!   (bounded queue). Exporting it without reading a batch (a schema peek)
//!   leaves it available for the next export.

use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, SchemaRef};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple};

use super::plan::Plan;
use crate::statement::{StmtCancel, StmtShared};
use arrowsql_core::arrow::stream::{MergeCore, ReaderError, StreamCore};

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

pub(crate) fn stream_capsule(py: Python<'_>, reader: Box<dyn RecordBatchReader + Send>) -> PyResult<Bound<'_, PyCapsule>> {
    PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(reader), c"arrow_array_stream")
}

pub(crate) fn schema_capsule<'py>(py: Python<'py>, schema: &SchemaRef) -> PyResult<Bound<'py, PyCapsule>> {
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

/// Drop `value` with the GIL released: the last handle of a stream stops its
/// producer thread, which may wait on the server.
pub(crate) fn drop_detached<T: Send>(value: T) {
    Python::attach(|py| py.detach(move || drop(value)));
}

/// A reader handed to an Arrow C stream consumer. The consumer may release
/// it with the GIL held; dropping it shuts the stream down without the GIL.
pub(crate) struct Detached<R: Send>(pub(crate) Option<R>);

impl<R: Iterator<Item = Result<RecordBatch, ArrowError>> + Send> Iterator for Detached<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut()?.next()
    }
}

impl<R: RecordBatchReader + Send> RecordBatchReader for Detached<R> {
    fn schema(&self) -> SchemaRef {
        self.0.as_ref().map(|r| r.schema()).expect("reader used after drop")
    }
}

impl<R: Send> Drop for Detached<R> {
    fn drop(&mut self) {
        if let Some(reader) = self.0.take() {
            drop_detached(reader);
        }
    }
}

pub(crate) fn reader_error(what: &str, err: ReaderError) -> PyErr {
    PyValueError::new_err(match err {
        ReaderError::Closed => format!("{what} is closed"),
        ReaderError::Consumed => format!("{what} was already consumed"),
        ReaderError::Busy => format!("{what} is already being read"),
    })
}

/// A result being streamed as Arrow record batches (one-shot).
#[pyclass(name = "ArrowStream", module = "arrowsql.ddbc_bindings", frozen)]
pub struct ArrowStream {
    core: ManuallyDrop<Arc<StreamCore>>,
}

impl Drop for ArrowStream {
    fn drop(&mut self) {
        // SAFETY: taken once, here.
        drop_detached(unsafe { ManuallyDrop::take(&mut self.core) });
    }
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
        // The Python cursor, kept alive by the stream and by every C stream
        // exported from it: `cur.execute(q).arrow_stream()` leaves no other
        // reference. (Dropped without the GIL on a consumer thread, pyo3
        // defers the decref.)
        let keep_alive = owner.map(|o| Box::new(o) as Box<dyn std::any::Any + Send + Sync>);
        let core = StreamCore::start(stmt, plan, generation, execution, batch_rows, depth, keep_alive);
        ArrowStream { core: ManuallyDrop::new(core) }
    }
}

#[pymethods]
impl ArrowStream {
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.core.schema().fields().iter().map(|f| f.name().clone()).collect()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.core.schema().fields().len()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.is_closed()
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
        let mut reader = self.core.reader().map_err(|e| reader_error("ArrowStream", e))?;
        let batches = py.detach(move || {
            let batches = reader.by_ref().collect::<Result<Vec<_>, _>>();
            drop(reader);
            batches
        });
        Ok(ArrowTable::new(self.core.schema().clone(), batches.map_err(arrow_err)?))
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = self.core.reader().map_err(|e| reader_error("ArrowStream", e))?;
        stream_capsule(py, Box::new(Detached(Some(reader))))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, self.core.schema())
    }

    fn __repr__(&self) -> String {
        format!("ArrowStream(columns={:?}, closed={})", self.column_names(), self.closed())
    }
}

/// Batches of several `ArrowStream`s (one per worker connection) as one
/// unordered stream with a bounded buffer. Worker threads call `pump` with the
/// stream of each slice they read; the consumer exports the merge as an Arrow
/// C stream (one-shot). `close()` / dropping the exported stream early stops
/// the workers and cancels their statements.
#[pyclass(name = "ArrowMergeStream", module = "arrowsql.ddbc_bindings", frozen)]
pub struct ArrowMergeStream {
    core: ManuallyDrop<Arc<MergeCore>>,
}

impl Drop for ArrowMergeStream {
    fn drop(&mut self) {
        // SAFETY: taken once, here.
        drop_detached(unsafe { ManuallyDrop::take(&mut self.core) });
    }
}

impl ArrowMergeStream {
    pub fn from_core(core: Arc<MergeCore>) -> ArrowMergeStream {
        ArrowMergeStream { core: ManuallyDrop::new(core) }
    }
}

#[pymethods]
impl ArrowMergeStream {
    /// `schema_from`: a stream with the result's columns (it is not consumed).
    /// `workers`: how many threads will call `worker_done()`. `depth`: batches
    /// buffered between the workers and the consumer.
    #[new]
    #[pyo3(signature = (schema_from, workers, depth=8))]
    fn new(schema_from: &ArrowStream, workers: usize, depth: usize) -> ArrowMergeStream {
        ArrowMergeStream::from_core(MergeCore::new(schema_from.core.schema().clone(), workers, depth))
    }

    /// Move every batch of `stream` into the merge (blocks, without the GIL,
    /// until the stream ends, the merge stops or a read fails; a failure is
    /// delivered to the consumer). The stream is closed on return.
    fn pump(&self, py: Python<'_>, stream: &ArrowStream) -> PyResult<()> {
        let core: &MergeCore = &self.core;
        let source: &Arc<StreamCore> = &stream.core;
        py.detach(|| core.pump(source)).map_err(|e| reader_error("ArrowStream", e))
    }

    /// A worker is finished; the last one ends the stream.
    fn worker_done(&self) {
        self.core.worker_done();
    }

    /// Report a worker failure to the consumer and stop the other workers.
    fn fail(&self, py: Python<'_>, message: String) {
        py.detach(|| self.core.fail(message));
    }

    /// Workers should stop picking up work (closed or failed).
    #[getter]
    fn stopped(&self) -> bool {
        self.core.stopped()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.closed()
    }

    /// The stream was handed to a consumer (export in progress or done).
    #[getter]
    fn in_use(&self) -> bool {
        self.core.in_use()
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.core.schema().fields().iter().map(|f| f.name().clone()).collect()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.core.schema().fields().len()
    }

    /// Stop the workers and discard unread batches.
    fn close(&self, py: Python<'_>) {
        py.detach(|| self.core.shutdown());
    }

    /// Read all remaining batches into an `ArrowTable` (Ctrl-C interrupts).
    fn read_all(&self, py: Python<'_>) -> PyResult<ArrowTable> {
        read_merged(py, &self.core)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = self.core.reader().map_err(|e| reader_error("stream", e))?;
        stream_capsule(py, Box::new(Detached(Some(reader))))
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, self.core.schema())
    }
}

/// Read all remaining batches of a merged stream into an `ArrowTable`,
/// looking at Python signals between batches (Ctrl-C interrupts). A failure
/// is raised as `RuntimeError` with the stream's error text.
pub fn read_merged(py: Python<'_>, core: &Arc<MergeCore>) -> PyResult<ArrowTable> {
    let reader = core.reader().map_err(|e| reader_error("stream", e))?;
    let mut batches = Vec::new();
    let result = loop {
        if core.closed() {
            break Ok(());
        }
        match py.detach(|| core.recv_timeout(Duration::from_millis(100))) {
            Ok(Ok(batch)) => batches.push(batch),
            Ok(Err(message)) => break Err(PyRuntimeError::new_err(message)),
            Err(RecvTimeoutError::Timeout) => {
                if let Err(err) = py.check_signals() {
                    break Err(err);
                }
            }
            Err(RecvTimeoutError::Disconnected) => break Ok(()),
        }
    };
    core.mark_consumed();
    py.detach(move || {
        core.shutdown();
        drop(reader);
    });
    result.map(|()| ArrowTable::new(core.schema().clone(), batches))
}
