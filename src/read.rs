//! Python entry points of the partitioned parallel read and the UTF-8
//! transfer rewrite (`arrowsql_core::read`, `arrowsql_core::utf8`).
//!
//! `arrow_parallel.py` validates the arguments and passes a zero-argument
//! callable that opens an `arrowsql.Connection` (so `connect()` keyword
//! arguments, pooling and authentication behave as for any connection); the
//! planning, the probes, the worker threads and the merge run here without
//! the GIL.

use std::mem::ManuallyDrop;
use std::sync::Arc;

use arrowsql_core::read::{self, Connector, ParallelReader, Partitioning, Planned};
use arrowsql_core::{Error, ErrorKind, Param};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple};

use crate::arrow::export::{
    ArrowTable, Detached, drop_detached, read_merged, reader_error, schema_capsule, stream_capsule,
};
use crate::runtime::detach_watched;

/// The Python exception for a core error: the DB-API class for its SQLSTATE,
/// the host's own exception for a failed callback.
pub fn core_error(py: Python<'_>, err: Error) -> PyErr {
    match err.kind {
        ErrorKind::Database => {
            let native = (err.native != 0).then_some(err.native);
            crate::errors::raise_statement(py, &err.sqlstate, &err.message, native)
        }
        ErrorKind::Type => PyTypeError::new_err(err.message),
        ErrorKind::Value => PyValueError::new_err(err.message),
        ErrorKind::External => match err.into_source().map(|source| source.downcast::<PyErr>()) {
            Some(Ok(pyerr)) => *pyerr,
            Some(Err(other)) => PyRuntimeError::new_err(other.to_string()),
            None => PyRuntimeError::new_err("callback failed"),
        },
        _ => PyRuntimeError::new_err(err.message),
    }
}

/// A Python `arrowsql.Connection` (or the native `ddbc_bindings.Connection`
/// behind it) as a core connection. With `close`, dropping the last core
/// handle closes the Python connection (on a worker thread too); otherwise
/// the caller keeps owning it.
fn core_connection(obj: &Bound<'_, PyAny>, close: bool) -> PyResult<arrowsql_core::Connection> {
    let native = match obj.cast::<crate::connection::Connection>() {
        Ok(native) => native.clone(),
        Err(_) => obj.getattr("_conn")?.cast_into::<crate::connection::Connection>()?,
    };
    let shared = native.get().shared()?;
    let owner = obj.clone().unbind();
    let on_close: Box<dyn FnOnce() + Send> = if close {
        Box::new(move || {
            Python::attach(|py| {
                let _ = owner.bind(py).call_method0("close");
            })
        })
    } else {
        Box::new(move || drop(owner))
    };
    Ok(arrowsql_core::Connection::from_shared(shared, Some(on_close)))
}

/// Opens each connection of a read by calling a Python `connect()` closure.
struct PyConnector(Py<PyAny>);

impl Connector for PyConnector {
    fn connect(&self) -> Result<arrowsql_core::Connection, Error> {
        Python::attach(|py| {
            let conn = self.0.bind(py).call0()?;
            core_connection(&conn, true)
        })
        .map_err(|e| Error::external(Box::new(e)))
    }
}

/// A Python value bound as a parameter, typed as `cursor.execute` types it.
fn param_of(value: &Bound<'_, PyAny>) -> PyResult<Param> {
    let sql_type = match crate::statement::batch_rpc::fast_param_cell(value) {
        Some(cell) => cell,
        None => crate::params::py_types::py_to_sql_type(value)
            .map_err(|e| crate::params::conversion_error(&e))?,
    };
    Ok(Param::Sql(sql_type))
}

/// The partitioning `arrow_parallel.py` asks for (`partition_on` is `None`
/// for a read that is not split).
fn partitioning(
    partition_on: Option<String>,
    partitions: i64,
    partition_bounds: Option<&Bound<'_, PyAny>>,
    partition_nullable: Option<bool>,
) -> PyResult<Partitioning> {
    let partitions = usize::try_from(partitions).unwrap_or(0);
    Ok(match (partition_on, partition_bounds) {
        (None, _) => Partitioning::None,
        (Some(column), Some(bounds)) => Partitioning::Bounds {
            column,
            bounds: bounds.try_iter()?.map(|b| param_of(&b?)).collect::<PyResult<_>>()?,
            nullable: partition_nullable,
            partitions,
        },
        (Some(column), None) => Partitioning::Range { column, partitions },
    })
}

fn rows(batch_size: i64) -> usize {
    usize::try_from(batch_size).unwrap_or(0).max(1)
}

fn plan(
    py: Python<'_>,
    connector: &Arc<dyn Connector>,
    query: &str,
    partitioning: &Partitioning,
    utf8_strings: bool,
) -> PyResult<Planned> {
    detach_watched(py, || read::plan_read(connector.as_ref(), query, partitioning, utf8_strings))?
        .map_err(|e| core_error(py, e))
}

/// `read_arrow(...)` after argument checks: the whole result as an
/// `ArrowTable`. A read planned as one slice is fetched on this thread.
#[pyfunction]
#[pyo3(signature = (connect, query, partition_on=None, partitions=1, batch_size=65536, utf8_strings=true, partition_bounds=None, partition_nullable=None))]
#[allow(clippy::too_many_arguments)]
pub fn read_arrow(
    py: Python<'_>,
    connect: Py<PyAny>,
    query: String,
    partition_on: Option<String>,
    partitions: i64,
    batch_size: i64,
    utf8_strings: bool,
    partition_bounds: Option<Bound<'_, PyAny>>,
    partition_nullable: Option<bool>,
) -> PyResult<ArrowTable> {
    let partitioning = partitioning(partition_on, partitions, partition_bounds.as_ref(), partition_nullable)?;
    let connector: Arc<dyn Connector> = Arc::new(PyConnector(connect));
    let planned = plan(py, &connector, &query, &partitioning, utf8_strings)?;
    let batch_rows = rows(batch_size);
    if planned.work.len() == 1 {
        let (schema, batches) =
            detach_watched(py, || read::fetch_planned(planned, batch_rows))?.map_err(|e| core_error(py, e))?;
        return Ok(ArrowTable::new(schema, batches));
    }
    let reader = detach_watched(py, || read::start(planned, connector, batch_rows, None))?
        .map_err(|e| core_error(py, e))?;
    let stream = ParallelArrowStream::new(reader);
    let table = stream.read_all(py);
    stream.close(py);
    table
}

/// `read_arrow_stream(...)` after argument checks.
#[pyfunction]
#[pyo3(signature = (connect, query, partition_on=None, partitions=1, batch_size=65536, partition_bounds=None, max_buffered_batches=None, partition_nullable=None, utf8_strings=true))]
#[allow(clippy::too_many_arguments)]
pub fn read_arrow_stream(
    py: Python<'_>,
    connect: Py<PyAny>,
    query: String,
    partition_on: Option<String>,
    partitions: i64,
    batch_size: i64,
    partition_bounds: Option<Bound<'_, PyAny>>,
    max_buffered_batches: Option<i64>,
    partition_nullable: Option<bool>,
    utf8_strings: bool,
) -> PyResult<ParallelArrowStream> {
    let partitioning = partitioning(partition_on, partitions, partition_bounds.as_ref(), partition_nullable)?;
    let connector: Arc<dyn Connector> = Arc::new(PyConnector(connect));
    let planned = plan(py, &connector, &query, &partitioning, utf8_strings)?;
    let depth = max_buffered_batches.and_then(|d| usize::try_from(d).ok());
    let batch_rows = rows(batch_size);
    let reader = detach_watched(py, || read::start(planned, connector, batch_rows, depth))?
        .map_err(|e| core_error(py, e))?;
    Ok(ParallelArrowStream::new(reader))
}

/// Python `(sql, params)` tuples of a plan.
fn work_list<'py>(py: Python<'py>, work: Vec<(String, Vec<Py<PyAny>>)>) -> PyResult<Bound<'py, PyList>> {
    let items = work
        .into_iter()
        .map(|(sql, params)| PyTuple::new(py, [sql.into_pyobject(py)?.into_any(), PyTuple::new(py, params)?.into_any()]))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, items)
}

/// The Python value of a planned parameter (the planner only makes ints).
fn param_value(py: Python<'_>, param: Param) -> PyResult<Py<PyAny>> {
    match param {
        Param::I64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        Param::Str(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        other => Err(PyRuntimeError::new_err(format!("unexpected planned parameter {other:?}"))),
    }
}

/// `_plan_read(connect, ...)`: the statements a read would run, `[(sql,
/// params), ...]`, without running them (the probe connection is closed).
#[pyfunction]
#[pyo3(name = "_plan_read", signature = (connect, query, partition_on=None, partitions=1, partition_bounds=None, partition_nullable=None, utf8_strings=true))]
#[allow(clippy::too_many_arguments)]
pub fn plan_read(
    py: Python<'_>,
    connect: Py<PyAny>,
    query: String,
    partition_on: Option<String>,
    partitions: i64,
    partition_bounds: Option<Bound<'_, PyAny>>,
    partition_nullable: Option<bool>,
    utf8_strings: bool,
) -> PyResult<Py<PyList>> {
    let bounds: Option<Vec<Py<PyAny>>> = match &partition_bounds {
        Some(b) => Some(b.try_iter()?.map(|v| v.map(Bound::unbind)).collect::<PyResult<_>>()?),
        None => None,
    };
    let partitioning = partitioning(partition_on, partitions, partition_bounds.as_ref(), partition_nullable)?;
    let connector: Arc<dyn Connector> = Arc::new(PyConnector(connect));
    let planned = plan(py, &connector, &query, &partitioning, utf8_strings)?;
    let Planned { conn, work, .. } = planned;
    py.detach(move || drop(conn));
    let out = match bounds.filter(|b| !b.is_empty() && matches!(partitioning, Partitioning::Bounds { .. })) {
        // The bounds come back as the caller's own objects: the slices take
        // them in the order `range_slices` lays them out.
        Some(bounds) => {
            let nullable = work.len() == bounds.len() + 2;
            let positions: Vec<usize> = (0..bounds.len()).collect();
            let layout = read::range_slices("", "", &positions, nullable);
            work.into_iter()
                .zip(layout)
                .map(|(slice, (_, at))| (slice.sql, at.into_iter().map(|i| bounds[i].clone_ref(py)).collect()))
                .collect()
        }
        None => work
            .into_iter()
            .map(|s| Ok((s.sql, s.params.into_iter().map(|p| param_value(py, p)).collect::<PyResult<_>>()?)))
            .collect::<PyResult<Vec<_>>>()?,
    };
    Ok(work_list(py, out)?.unbind())
}

/// `_range_plan(conn, query, column, name, partitions, plain=None)`: probe
/// the integer `column` of `query` on `conn` and return the slices.
#[pyfunction]
#[pyo3(name = "_range_plan", signature = (conn, query, column, name, partitions, plain=None))]
pub fn range_plan(
    py: Python<'_>,
    conn: &Bound<'_, PyAny>,
    query: String,
    column: String,
    name: String,
    partitions: i64,
    plain: Option<String>,
) -> PyResult<Py<PyList>> {
    let conn = core_connection(conn, false)?;
    let partitions = usize::try_from(partitions).unwrap_or(0);
    let work = detach_watched(py, || {
        read::range_plan(&conn, &query, &column, &name, partitions, plain.as_deref())
    })?
    .map_err(|e| core_error(py, e))?;
    let work = work
        .into_iter()
        .map(|s| Ok((s.sql, s.params.into_iter().map(|p| param_value(py, p)).collect::<PyResult<_>>()?)))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(work_list(py, work)?.unbind())
}

/// `_bounds_plan(conn, query, column, bounds, nullable)`: the slices for
/// validated, ascending `bounds` (asking the server whether the column is
/// nullable when `nullable` is None). The parameters are the bound objects.
#[pyfunction]
#[pyo3(name = "_bounds_plan")]
pub fn bounds_plan(
    py: Python<'_>,
    conn: &Bound<'_, PyAny>,
    query: String,
    column: String,
    bounds: Vec<Py<PyAny>>,
    nullable: Option<bool>,
) -> PyResult<Py<PyList>> {
    if bounds.is_empty() {
        return Ok(PyList::empty(py).unbind());
    }
    let nullable = match nullable {
        Some(nullable) => nullable,
        None => {
            let conn = core_connection(conn, false)?;
            detach_watched(py, || read::probe_meta(&conn, &query, &column, None, false).1)?
        }
    };
    let bounds: Vec<Bound<'_, PyAny>> = bounds.iter().map(|b| b.bind(py).clone()).collect();
    let work = read::range_slices(&query, &column, &bounds, nullable)
        .into_iter()
        .map(|(sql, params)| (sql, params.into_iter().map(Bound::unbind).collect()))
        .collect();
    Ok(work_list(py, work)?.unbind())
}

/// `_probe_meta(conn, query, column, plain=None, with_rows=True)`:
/// `(estimated row count or None, column may hold NULL)`.
#[pyfunction]
#[pyo3(name = "_probe_meta", signature = (conn, query, column, plain=None, with_rows=true))]
pub fn probe_meta(
    py: Python<'_>,
    conn: &Bound<'_, PyAny>,
    query: String,
    column: String,
    plain: Option<String>,
    with_rows: bool,
) -> PyResult<(Option<i64>, bool)> {
    let conn = core_connection(conn, false)?;
    detach_watched(py, || read::probe_meta(&conn, &query, &column, plain.as_deref(), with_rows))
}

/// `utf8_query(conn, query)`: `query` rewritten to transfer national
/// character columns as UTF-8, or unchanged (see `arrowsql_core::utf8`).
#[pyfunction]
pub fn utf8_query(py: Python<'_>, conn: &Bound<'_, PyAny>, query: String) -> PyResult<String> {
    let Ok(conn) = core_connection(conn, false) else { return Ok(query) };
    detach_watched(py, || arrowsql_core::utf8::utf8_query(&conn, &query))
}

/// Batches of a partitioned read, arriving while the slices are read.
///
/// Created by `read_arrow_stream`. One-shot, unordered, bounded buffer.
#[pyclass(name = "ParallelArrowStream", module = "arrowsql.ddbc_bindings", frozen)]
pub struct ParallelArrowStream {
    reader: ManuallyDrop<ParallelReader>,
}

impl Drop for ParallelArrowStream {
    /// A stream nobody read stops its workers; one handed to a consumer
    /// stops itself when the consumer is done.
    fn drop(&mut self) {
        // SAFETY: taken once, here.
        drop_detached(unsafe { ManuallyDrop::take(&mut self.reader) });
    }
}

impl ParallelArrowStream {
    fn new(reader: ParallelReader) -> ParallelArrowStream {
        ParallelArrowStream { reader: ManuallyDrop::new(reader) }
    }
}

#[pymethods]
impl ParallelArrowStream {
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.reader.schema().fields().iter().map(|f| f.name().clone()).collect()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.reader.schema().fields().len()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.reader.is_closed()
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, &self.reader.schema())
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let merged = self.reader.merge().reader().map_err(|e| reader_error("stream", e))?;
        stream_capsule(py, Box::new(Detached(Some(merged))))
    }

    /// Read every remaining batch into an `ArrowTable` (Ctrl-C interrupts).
    fn read_all(&self, py: Python<'_>) -> PyResult<ArrowTable> {
        match read_merged(py, self.reader.merge()) {
            Ok(table) => Ok(table),
            Err(err) => {
                self.close(py);
                if !err.is_instance_of::<PyRuntimeError>(py) {
                    return Err(err);
                }
                if let Some(worker) = self.reader.take_error() {
                    return Err(core_error(py, worker));
                }
                // A read that failed mid-statement arrives as "[SQLSTATE] message".
                let text = err.value(py).str()?.to_string();
                match read::parse_stream_error(&text) {
                    Some((sqlstate, message)) => Err(crate::errors::raise_statement(py, sqlstate, message, None)),
                    None => Err(err),
                }
            }
        }
    }

    /// Cancel running statements, stop the workers, close their connections.
    fn close(&self, py: Python<'_>) {
        py.detach(|| self.reader.close());
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) {
        self.close(py);
    }

    fn __repr__(&self) -> String {
        format!("ParallelArrowStream(columns={:?}, closed={})", self.column_names(), self.closed())
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ParallelArrowStream>()?;
    m.add_function(wrap_pyfunction!(read_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(read_arrow_stream, m)?)?;
    m.add_function(wrap_pyfunction!(plan_read, m)?)?;
    m.add_function(wrap_pyfunction!(range_plan, m)?)?;
    m.add_function(wrap_pyfunction!(bounds_plan, m)?)?;
    m.add_function(wrap_pyfunction!(probe_meta, m)?)?;
    m.add_function(wrap_pyfunction!(utf8_query, m)?)?;
    Ok(())
}
