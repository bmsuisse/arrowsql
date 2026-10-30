//! Reading an Arrow source handed over as PyCapsules (the Arrow C stream and
//! C data interfaces), so no Arrow library is needed on the Python side.

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::{RecordBatchStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// The batches of one Arrow source.
pub type ArrowBatches = Box<dyn RecordBatchReader + Send>;

fn arrow_error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("Invalid Arrow source: {err}"))
}

/// Open `source` through `__arrow_c_stream__`, or `__arrow_c_array__` for a
/// single batch.
pub fn open(source: &Bound<'_, PyAny>) -> PyResult<ArrowBatches> {
    if source.hasattr("__arrow_c_stream__")? {
        let capsule = source.call_method1("__arrow_c_stream__", (source.py().None(),))?;
        let batches = stream_from_capsule(&capsule)?;
        // A stream of this driver reads the wire of a connection that may be
        // the one the load is about to occupy, so it is read to the end first.
        return if is_native_result_stream(source) { source.py().detach(|| read_to_end(batches)) } else { Ok(batches) };
    }
    if source.hasattr("__arrow_c_array__")? {
        let capsules = source.call_method1("__arrow_c_array__", (source.py().None(),))?;
        return batch_from_capsules(&capsules.get_item(0)?, &capsules.get_item(1)?);
    }
    Err(PyTypeError::new_err(
        "source must expose __arrow_c_stream__ or __arrow_c_array__ (pyarrow Table / RecordBatch / RecordBatchReader, polars, pandas >= 2.2, duckdb)",
    ))
}

fn is_native_result_stream(source: &Bound<'_, PyAny>) -> bool {
    source
        .get_type()
        .fully_qualified_name()
        .is_ok_and(|name| name == "mssql_python.ddbc_bindings.ArrowStream")
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

fn batch_from_capsules(schema: &Bound<'_, PyAny>, array: &Bound<'_, PyAny>) -> PyResult<ArrowBatches> {
    let schema_ptr = schema.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_schema"))?.as_ptr() as *const FFI_ArrowSchema;
    let array_ptr = array.cast::<PyCapsule>()?.pointer_checked(Some(c"arrow_array"))?.as_ptr() as *mut FFI_ArrowArray;
    // SAFETY: both capsules hold valid C data interface structs (checked by
    // name); the array is moved out so its release callback runs exactly once.
    let data = unsafe {
        let array = std::ptr::replace(array_ptr, FFI_ArrowArray::empty());
        from_ffi(array, &*schema_ptr).map_err(arrow_error)?
    };
    let batch = RecordBatch::from(StructArray::from(data));
    let schema = batch.schema();
    Ok(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
}
