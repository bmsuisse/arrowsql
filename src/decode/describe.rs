//! Column descriptions handed to Python (`cursor.description` source). The
//! ODBC mapping itself is `arrowsql_core::decode::describe`.

use mssql_tds::query::metadata::ColumnMetadata;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub use arrowsql_core::decode::describe::*;

/// Append one ODBC metadata dict per column to `out`. Runs on every execute,
/// so the keys are interned rather than allocated per column.
pub fn append_columns(py: Python<'_>, columns: &[ColumnMetadata], out: &Bound<'_, PyList>) -> PyResult<()> {
    for meta in columns {
        let d = PyDict::new(py);
        d.set_item(intern!(py, "ColumnName"), &meta.column_name)?;
        d.set_item(intern!(py, "DataType"), odbc_sql_type(meta))?;
        d.set_item(intern!(py, "ColumnSize"), column_size(meta))?;
        d.set_item(intern!(py, "DecimalDigits"), decimal_digits(meta))?;
        d.set_item(intern!(py, "Nullable"), nullable(meta))?;
        out.append(d)?;
    }
    Ok(())
}

/// The ODBC description of one column, as handed to Python.
type ColumnSig = (String, i16, u64, i16, i16);

fn same_column(sig: &ColumnSig, meta: &ColumnMetadata) -> bool {
    sig.0 == meta.column_name
        && sig.1 == odbc_sql_type(meta)
        && sig.2 == column_size(meta)
        && sig.3 == decimal_digits(meta)
        && sig.4 == nullable(meta)
}

/// The last description a statement handed to Python. A statement that runs
/// the same query again gets the same list back, so the Python layer can
/// recognise an unchanged layout by identity instead of rebuilding it.
pub struct DescribeCache {
    sigs: Vec<ColumnSig>,
    list: Py<PyList>,
}

/// The metadata list for `columns`, shared with the previous call when the
/// columns did not change. Callers must not modify the list.
pub fn shared_columns<'py>(
    py: Python<'py>,
    columns: &[ColumnMetadata],
    cache: &mut Option<Box<dyn std::any::Any + Send>>,
) -> PyResult<Bound<'py, PyList>> {
    if let Some(hit) = cache.as_ref().and_then(|c| c.downcast_ref::<DescribeCache>())
        && hit.sigs.len() == columns.len()
        && hit.sigs.iter().zip(columns).all(|(sig, meta)| same_column(sig, meta))
    {
        return Ok(hit.list.bind(py).clone());
    }
    let list = PyList::empty(py);
    append_columns(py, columns, &list)?;
    let sigs = columns
        .iter()
        .map(|m| (m.column_name.clone(), odbc_sql_type(m), column_size(m), decimal_digits(m), nullable(m)))
        .collect();
    *cache = Some(Box::new(DescribeCache { sigs, list: list.clone().unbind() }));
    Ok(list)
}

