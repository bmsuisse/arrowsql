//! Arrow fetch (`DDBCSQLFetchArrowBatch`).
//!
//! STUB — returns IM001. Design seam for the native Arrow path:
//! implement an `ArrowSink` that implements `mssql_tds::datatypes::row_writer::RowWriter`
//! (one Arrow builder per column, chosen from the result's `ColumnMetadata`),
//! fill it with `statement::read_rows_into(client, &mut sink, rows_fn,
//! bytes_fn, batch_size, max_bytes)` while the GIL is released and the
//! connection lock is held, then finish the builders into a RecordBatch and
//! export it through the Arrow C Data Interface as PyCapsules
//! (`arrow_schema` / `arrow_array`) appended to `capsules`. Large PLP values
//! can be written straight into builder buffers via
//! `RowWriter::value_destination`.

use pyo3::prelude::*;

#[pyfunction]
#[pyo3(name = "DDBCSQLFetchArrowBatch", signature = (StatementHandle, capsules, arrowBatchSize, charCtype, messages=None))]
#[allow(non_snake_case, unused_variables)]
pub fn fetch_arrow_batch(
    StatementHandle: &Bound<'_, PyAny>,
    capsules: &Bound<'_, PyAny>,
    arrowBatchSize: i64,
    charCtype: i32,
    messages: Option<&Bound<'_, PyAny>>,
) -> i16 {
    crate::statement::not_implemented(StatementHandle, "DDBCSQLFetchArrowBatch")
}
