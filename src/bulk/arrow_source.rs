//! Reading an Arrow source handed over as PyCapsules (the Arrow C stream and
//! C data interfaces), so no Arrow library is needed on the Python side.

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::{RecordBatchStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, SchemaRef};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyIterator, PyString};

/// The batches of one Arrow source.
pub type ArrowBatches = Box<dyn RecordBatchReader + Send>;

fn arrow_error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("Invalid Arrow source: {err}"))
}

/// Open `source` through `__arrow_c_stream__`, `__arrow_c_array__` for a single
/// batch, or an iterable of objects exposing `__arrow_c_array__` (record
/// batches).
///
/// `drain_native_streams`: read a stream of this driver to its end first. That
/// is needed when the load runs on a connection a native stream may still be
/// reading from (the same-connection deadlock); a parallel load always opens
/// its own connections, so it streams instead.
pub fn open(source: &Bound<'_, PyAny>, drain_native_streams: bool) -> PyResult<ArrowBatches> {
    if source.hasattr("__arrow_c_stream__")? {
        let capsule = source.call_method1("__arrow_c_stream__", (source.py().None(),))?;
        let batches = stream_from_capsule(&capsule)?;
        // A stream of this driver reads the wire of a connection that may be
        // the one the load is about to occupy, so it is read to the end first.
        return if drain_native_streams && is_native_result_stream(source) { source.py().detach(|| read_to_end(batches)) } else { Ok(batches) };
    }
    if source.hasattr("__arrow_c_array__")? {
        let capsules = source.call_method1("__arrow_c_array__", (source.py().None(),))?;
        return batch_from_capsules(&capsules.get_item(0)?, &capsules.get_item(1)?);
    }
    if !source.is_instance_of::<PyString>() && !source.is_instance_of::<PyBytes>() {
        if let Ok(iterator) = source.try_iter() {
            return batches_from_iterable(iterator);
        }
    }
    Err(PyTypeError::new_err(
        "source must expose __arrow_c_stream__ or __arrow_c_array__ (pyarrow Table / RecordBatch / RecordBatchReader, an iterable of RecordBatch, polars, pandas >= 2.2, duckdb)",
    ))
}

fn is_native_result_stream(source: &Bound<'_, PyAny>) -> bool {
    source
        .get_type()
        .fully_qualified_name()
        .is_ok_and(|name| name == "arrowsql.ddbc_bindings.ArrowStream")
}

fn read_to_end(batches: ArrowBatches) -> PyResult<ArrowBatches> {
    let schema = batches.schema();
    let all = batches.collect::<Result<Vec<_>, _>>().map_err(arrow_error)?;
    Ok(Box::new(RecordBatchIterator::new(all.into_iter().map(Ok), schema)))
}

fn stream_from_capsule(capsule: &Bound<'_, PyAny>) -> PyResult<ArrowBatches> {
    let capsule = capsule.cast::<PyCapsule>()?;
    let pointer = capsule.pointer_checked(Some(c"arrow_array_stream"))?.as_ptr() as *mut FFI_ArrowArrayStream;
    // SAFETY: the capsule owns a valid FFI_ArrowArrayStream (checked by name).
    // Moving it out and leaving an empty stream behind transfers ownership of
    // the release callback, as the Arrow C stream specification requires of a
    // consumer.
    let stream = unsafe { std::ptr::replace(pointer, FFI_ArrowArrayStream::empty()) };
    let reader = RecordBatchStreamReader::try_new(stream).map_err(arrow_error)?;
    Ok(Box::new(reader))
}

/// An iterable of record batches (anything with `__arrow_c_array__`), read
/// lazily; the first one gives the schema.
fn batches_from_iterable(iterator: Bound<'_, PyIterator>) -> PyResult<ArrowBatches> {
    let first = match iterator.clone().next() {
        Some(item) => batch_from_object(&item?)?,
        None => return Err(PyValueError::new_err("the iterable of RecordBatch is empty")),
    };
    let schema = first.schema();
    Ok(Box::new(IterableBatches { iterator: iterator.unbind(), schema, first: Some(first) }))
}

fn batch_from_object(item: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    if !item.hasattr("__arrow_c_array__")? {
        return Err(PyTypeError::new_err(format!(
            "iterable elements must implement __arrow_c_array__ (RecordBatch), got {}",
            item.get_type().name()?
        )));
    }
    let capsules = item.call_method1("__arrow_c_array__", (item.py().None(),))?;
    batch_from_capsule_pair(&capsules.get_item(0)?, &capsules.get_item(1)?)
}

struct IterableBatches {
    iterator: Py<PyIterator>,
    schema: SchemaRef,
    first: Option<RecordBatch>,
}

impl Iterator for IterableBatches {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.first.take() {
            return Some(Ok(batch));
        }
        Python::attach(|py| {
            let item = self.iterator.bind(py).clone().next()?;
            Some(item.and_then(|item| batch_from_object(&item)).map_err(|err| ArrowError::ExternalError(Box::new(err))))
        })
    }
}

impl RecordBatchReader for IterableBatches {
    fn schema(&self) -> SchemaRef {
        std::sync::Arc::clone(&self.schema)
    }
}

fn batch_from_capsules(schema: &Bound<'_, PyAny>, array: &Bound<'_, PyAny>) -> PyResult<ArrowBatches> {
    let batch = batch_from_capsule_pair(schema, array)?;
    let schema = batch.schema();
    Ok(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
}

fn batch_from_capsule_pair(schema: &Bound<'_, PyAny>, array: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let schema_ptr = schema.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_schema"))?.as_ptr() as *const FFI_ArrowSchema;
    let array_ptr = array.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_array"))?.as_ptr() as *mut FFI_ArrowArray;
    // SAFETY: both capsules hold valid C data interface structs (checked by
    // name); the array is moved out so its release callback runs exactly once.
    let data = unsafe {
        let array = std::ptr::replace(array_ptr, FFI_ArrowArray::empty());
        from_ffi(array, &*schema_ptr).map_err(arrow_error)?
    };
    Ok(RecordBatch::from(StructArray::from(data)))
}
