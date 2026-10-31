//! The completion hub: hands finished async operations back to one asyncio loop.
//!
//! Tokio tasks never take the GIL. They push `(request id, Outcome)` onto the
//! hub queue and write one byte to a socket pair only when the hub is not
//! already signalled. The loop watches the read end with `add_reader` and
//! calls `drain`, which turns every queued outcome into Python objects and
//! resolves its future: one wake-up completes all operations finished since
//! the last one.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use mssql_tds::query::metadata::ColumnMetadata;
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyTuple};
use pyo3::intern;

use crate::arrow::export::ArrowTable;
use crate::decode::describe;
use crate::diag::{self, DiagRecord};

use super::convert::{RowBatch, Shape, rows_to_list};
use super::session::{ExecOut, RowsOut};

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

/// What a finished operation hands back. Pure Rust: built off the GIL.
pub enum Outcome {
    None,
    Int(i64),
    Exec(ExecOut),
    /// `nextset` past the last result.
    NoMoreResults,
    Rows(RowsOut),
    /// All rows of a shortcut query: `fetch` (list), `fetchrow` (row or
    /// None), `fetchval` (first value or None).
    Query { out: ExecOut, ncols: usize, mode: QueryMode },
    Arrow(SchemaRef, Vec<RecordBatch>),
    Connection(Arc<super::connection::ConnCore>),
    Pool(Arc<super::pool::PoolInner>),
    Error(Vec<DiagRecord>),
}

#[derive(Clone, Copy)]
pub enum QueryMode {
    All,
    One,
    Value,
}

pub struct HubInner {
    queue: Mutex<Vec<(u64, Outcome)>>,
    signalled: AtomicBool,
    writer: UnixStream,
    reader: UnixStream,
}

impl HubInner {
    pub fn complete(&self, id: u64, outcome: Outcome) {
        self.queue.lock().push((id, outcome));
        if !self.signalled.swap(true, Ordering::AcqRel) {
            // WouldBlock means the socket is already full of wake-ups: fine.
            let _ = (&self.writer).write(&[1]);
        }
    }
}

/// Per event loop. `futures` and `shapes` are only touched on the loop thread.
#[pyclass(name = "AioHub", module = "ddbc_bindings", frozen)]
pub struct Hub {
    pub inner: Arc<HubInner>,
    /// A `weakref.ref` to the loop: the loop owns the hub (via add_reader), so a
    /// strong reference here would keep every closed loop alive.
    event_loop: Py<PyAny>,
    shape_factory: Py<PyAny>,
    /// Pending futures and whether their rows are plain tuples.
    futures: Mutex<HashMap<u64, (Py<PyAny>, bool)>>,
    shapes: Mutex<HashMap<u64, Arc<Shape>>>,
}

impl Hub {
    /// Create a loop future and register it under a new request id.
    pub fn register<'py>(&self, py: Python<'py>, as_tuple: bool) -> PyResult<(u64, Bound<'py, PyAny>)> {
        let event_loop = self.event_loop.bind(py).call0()?;
        if event_loop.is_none() {
            return Err(PyRuntimeError::new_err("The event loop is gone"));
        }
        let future = event_loop.call_method0(intern!(py, "create_future"))?;
        let id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        self.futures.lock().insert(id, (future.clone().unbind(), as_tuple));
        Ok((id, future))
    }

    /// The cached Python-side shape (description, column maps) of `columns`.
    pub fn shape(&self, py: Python<'_>, columns: &[ColumnMetadata]) -> PyResult<Arc<Shape>> {
        let key = shape_key(columns);
        if let Some(shape) = self.shapes.lock().get(&key) {
            return Ok(shape.clone());
        }
        let specs = PyList::empty(py);
        for meta in columns {
            specs.append((
                meta.column_name.as_str(),
                describe::odbc_sql_type(meta),
                describe::column_size(meta),
                describe::decimal_digits(meta),
                describe::nullable(meta),
            ))?;
        }
        let shape = Arc::new(Shape::from_factory(py, self.shape_factory.bind(py).call1((specs,))?)?);
        let mut shapes = self.shapes.lock();
        if shapes.len() > 4096 {
            shapes.clear();
        }
        shapes.insert(key, shape.clone());
        Ok(shape)
    }

    fn to_python<'py>(&self, py: Python<'py>, outcome: Outcome, as_tuple: bool) -> PyResult<Bound<'py, PyAny>> {
        Ok(match outcome {
            Outcome::None => py.None().into_bound(py),
            Outcome::Int(n) => n.into_pyobject(py)?.into_any(),
            Outcome::NoMoreResults => py.None().into_bound(py),
            Outcome::Exec(out) => self.exec_tuple(py, out, as_tuple)?.into_any(),
            Outcome::Rows(rows) => {
                let shape = match &rows.columns {
                    Some(columns) if !as_tuple => Some(self.shape(py, columns)?),
                    _ => None,
                };
                let list = rows_to_list(py, &rows.rows, rows.ncols, shape.as_deref(), as_tuple)?;
                (list, rows.done).into_pyobject(py)?.into_any()
            }
            Outcome::Query { out, ncols, mode } => {
                let shape = match &out.columns {
                    Some(columns) if !as_tuple => Some(self.shape(py, columns)?),
                    _ => None,
                };
                let list = rows_to_list(py, &out.rows, ncols, shape.as_deref(), as_tuple)?;
                match mode {
                    QueryMode::All => list.into_any(),
                    QueryMode::One => match list.len() {
                        0 => py.None().into_bound(py),
                        _ => list.get_item(0)?,
                    },
                    QueryMode::Value => match list.len() {
                        0 => py.None().into_bound(py),
                        _ => list.get_item(0)?.get_item(0)?,
                    },
                }
            }
            Outcome::Arrow(schema, batches) => {
                Bound::new(py, ArrowTable::new(schema, batches))?.into_any()
            }
            Outcome::Connection(core) => {
                Bound::new(py, super::connection::AioConnection::from_core(core))?.into_any()
            }
            Outcome::Pool(inner) => Bound::new(py, super::pool::AioPool::from_inner(inner))?.into_any(),
            Outcome::Error(records) => return Err(to_exception(py, &records)),
        })
    }

    /// `(shape | None, RowBatch | None, done, rowcount, messages | None)`.
    fn exec_tuple<'py>(&self, py: Python<'py>, out: ExecOut, as_tuple: bool) -> PyResult<Bound<'py, PyTuple>> {
        let shape = match &out.columns {
            Some(columns) => Some(self.shape(py, columns)?),
            None => None,
        };
        let shape_obj = shape.as_ref().map_or_else(|| py.None(), |s| s.obj.clone_ref(py));
        let batch = match &shape {
            Some(shape) => {
                let ncols = out.columns.as_ref().map_or(0, |c| c.len());
                Bound::new(py, RowBatch::new(out.rows, ncols, shape.clone(), as_tuple))?.into_any()
            }
            None => py.None().into_bound(py),
        };
        let messages = if out.info.is_empty() {
            py.None().into_bound(py)
        } else {
            let list = PyList::empty(py);
            for record in &out.info {
                list.append((diag::state_label(record), record.message.as_str()))?;
            }
            list.into_any()
        };
        (shape_obj, batch, out.done, out.rowcount, messages).into_pyobject(py)
    }
}

#[pymethods]
impl Hub {
    #[new]
    fn new(event_loop: Py<PyAny>, shape_factory: Py<PyAny>) -> PyResult<Self> {
        let (reader, writer) = UnixStream::pair().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        reader.set_nonblocking(true).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writer.set_nonblocking(true).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(HubInner { queue: Mutex::new(Vec::new()), signalled: AtomicBool::new(false), writer, reader }),
            event_loop,
            shape_factory,
            futures: Mutex::new(HashMap::new()),
            shapes: Mutex::new(HashMap::new()),
        })
    }

    fn fileno(&self) -> i32 {
        self.inner.reader.as_raw_fd()
    }

    /// Resolve every finished operation. Called by the loop when the socket
    /// is readable.
    fn drain(&self, py: Python<'_>) {
        let mut scratch = [0u8; 256];
        while let Ok(n) = (&self.inner.reader).read(&mut scratch) {
            if n < scratch.len() {
                break;
            }
        }
        self.inner.signalled.store(false, Ordering::Release);
        let items = std::mem::take(&mut *self.inner.queue.lock());
        for (id, outcome) in items {
            let Some((future, as_tuple)) = self.futures.lock().remove(&id) else { continue };
            let future = future.into_bound(py);
            // A cancelled future (task.cancel(), wait_for timeout) is done.
            let done = future.call_method0(intern!(py, "done")).and_then(|d| d.is_truthy());
            if !matches!(done, Ok(false)) {
                continue;
            }
            let settled = match self.to_python(py, outcome, as_tuple) {
                Ok(value) => future.call_method1(intern!(py, "set_result"), (value,)),
                Err(err) => future.call_method1(intern!(py, "set_exception"), (err.into_value(py),)),
            };
            if let Err(err) = settled {
                err.write_unraisable(py, Some(&future));
            }
        }
    }

    /// Pending (unresolved) operations; for tests and diagnostics.
    fn pending(&self) -> usize {
        self.futures.lock().len()
    }
}

fn shape_key(columns: &[ColumnMetadata]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for meta in columns {
        meta.column_name.hash(&mut hasher);
        describe::odbc_sql_type(meta).hash(&mut hasher);
        describe::column_size(meta).hash(&mut hasher);
        describe::decimal_digits(meta).hash(&mut hasher);
        describe::nullable(meta).hash(&mut hasher);
    }
    hasher.finish()
}

static SQLSTATE_TO_EXCEPTION: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DATABASE_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// The same exception the sync driver raises for these diagnostics.
pub fn to_exception(py: Python<'_>, records: &[DiagRecord]) -> PyErr {
    let record = records.first().cloned().unwrap_or_else(|| DiagRecord::driver("HY000", "Unknown error"));
    let build = || -> PyResult<PyErr> {
        let mapper = SQLSTATE_TO_EXCEPTION.import(py, "mssql_python.exceptions", "sqlstate_to_exception")?;
        let native = (record.native_error != 0).then_some(record.native_error);
        let exc = mapper.call1((record.sqlstate.as_str(), record.message.as_str(), native))?;
        if !exc.is_none() {
            return Ok(PyErr::from_value(exc));
        }
        let class = DATABASE_ERROR.import(py, "mssql_python.exceptions", "DatabaseError")?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item(intern!(py, "sqlstate"), record.sqlstate.as_str())?;
        kwargs.set_item(intern!(py, "native_error"), native)?;
        let exc = class.call(
            (format!("An error occurred with SQLSTATE code: {}", record.sqlstate), record.message.as_str()),
            Some(&kwargs),
        )?;
        Ok(PyErr::from_value(exc))
    };
    build().unwrap_or_else(|err| err)
}

