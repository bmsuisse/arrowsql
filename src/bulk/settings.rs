//! Bulk copy options and source-to-destination column mapping.

use mssql_tds::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

/// The keyword arguments of `Cursor.bulkcopy` / `bulkcopy_arrow`.
pub struct BulkSettings {
    pub table_name: String,
    pub batch_size: usize,
    pub timeout_secs: u64,
    pub mappings: Vec<MappingSpec>,
    pub keep_identity: bool,
    pub check_constraints: bool,
    pub table_lock: bool,
    pub keep_nulls: bool,
    pub fire_triggers: bool,
    pub use_internal_transaction: bool,
}

/// Where a mapped source column is found.
pub enum SourceRef {
    Ordinal(usize),
    /// Only meaningful for sources with named columns (Arrow).
    Name(String),
}

/// One user-supplied `column_mappings` entry.
pub struct MappingSpec {
    pub source: SourceRef,
    pub destination: String,
}

/// A mapping with both ends located.
pub struct Resolved {
    pub source: usize,
    /// Index into the destination's full column list.
    pub destination: usize,
    pub destination_name: String,
}

/// The shape of the data being loaded, as far as it is known up front.
pub struct SourceShape {
    pub column_count: usize,
    /// Column names, for sources that have them.
    pub names: Option<Vec<String>>,
    /// Without column mappings, a source wider than the destination is an
    /// error. Arrow sources keep mssql-rs' behaviour: the extra columns are ignored.
    pub reject_extra_columns: bool,
}

/// `None`, `['colA', 'colB']` or `[(0, 'colA'), (1, 'colB')]`.
pub fn parse_mappings(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<MappingSpec>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(Vec::new());
    };
    let list = obj
        .cast::<PyList>()
        .map_err(|_| PyTypeError::new_err("column_mappings must be a list of column names or (index, name) tuples"))?;
    let Some(first) = list.try_iter()?.next() else {
        return Ok(Vec::new());
    };
    if first?.is_instance_of::<PyTuple>() {
        list.iter().enumerate().map(|(i, item)| parse_pair(i, &item)).collect()
    } else {
        list.iter().enumerate().map(|(i, item)| parse_name(i, &item)).collect()
    }
}

fn parse_name(index: usize, item: &Bound<'_, PyAny>) -> PyResult<MappingSpec> {
    let destination: String = item.extract().map_err(|_| {
        PyTypeError::new_err(format!(
            "Invalid column_mappings[{index}]: expected str, got {}",
            type_name(item)
        ))
    })?;
    Ok(MappingSpec { source: SourceRef::Ordinal(index), destination })
}

fn parse_pair(index: usize, item: &Bound<'_, PyAny>) -> PyResult<MappingSpec> {
    let pair = item.cast::<PyTuple>().map_err(|_| {
        PyTypeError::new_err(format!(
            "Invalid column_mappings[{index}]: expected tuple, got {}",
            type_name(item)
        ))
    })?;
    if pair.len() != 2 {
        return Err(PyTypeError::new_err(format!(
            "Invalid column_mappings[{index}]: tuple must have exactly 2 elements (source, destination)"
        )));
    }
    let source = pair.get_item(0)?;
    let source = if let Ok(ordinal) = source.extract::<usize>() {
        SourceRef::Ordinal(ordinal)
    } else if let Ok(name) = source.extract::<String>() {
        SourceRef::Name(name)
    } else {
        return Err(PyTypeError::new_err(format!(
            "Invalid column_mappings[{index}]: source must be an int or str, got {}",
            type_name(&source)
        )));
    };
    let destination: String = pair.get_item(1)?.extract().map_err(|_| {
        PyTypeError::new_err(format!("Invalid column_mappings[{index}]: destination must be a str"))
    })?;
    Ok(MappingSpec { source, destination })
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type().name().map(|n| n.to_string()).unwrap_or_default()
}

/// Locate every mapping in the destination, in destination column order (the
/// order columns go on the wire). Without explicit mappings the source columns
/// map by position onto the destination columns, skipping identity columns
/// unless `keep_identity`.
pub fn resolve(
    settings: &BulkSettings,
    shape: &SourceShape,
    destination: &[BulkCopyColumnMetadata],
) -> Result<Vec<Resolved>, String> {
    let eligible = |column: &BulkCopyColumnMetadata| settings.keep_identity || !column.is_identity;
    let mut resolved: Vec<Resolved> = if settings.mappings.is_empty() {
        let eligible_count = destination.iter().filter(|column| eligible(column)).count();
        if shape.reject_extra_columns && shape.column_count > eligible_count {
            return Err(format!(
                "source has {} columns, destination has {eligible_count}",
                shape.column_count
            ));
        }
        destination
            .iter()
            .enumerate()
            .filter(|(_, column)| eligible(column))
            .take(shape.column_count)
            .enumerate()
            .map(|(source, (index, column))| Resolved {
                source,
                destination: index,
                destination_name: column.column_name.clone(),
            })
            .collect()
    } else {
        settings
            .mappings
            .iter()
            .map(|spec| resolve_one(spec, shape, destination, &eligible))
            .collect::<Result<_, _>>()?
    };
    resolved.sort_by_key(|mapping| mapping.destination);
    Ok(resolved)
}

fn resolve_one(
    spec: &MappingSpec,
    shape: &SourceShape,
    destination: &[BulkCopyColumnMetadata],
    eligible: &dyn Fn(&BulkCopyColumnMetadata) -> bool,
) -> Result<Resolved, String> {
    let source = match &spec.source {
        SourceRef::Ordinal(ordinal) => *ordinal,
        SourceRef::Name(name) => {
            let names = shape
                .names
                .as_ref()
                .ok_or("Source column name mappings are not supported for row tuples; use column indexes")?;
            names.iter().position(|n| n == name).ok_or_else(|| {
                format!("Source column '{name}' not found in the source (available: {})", names.join(", "))
            })?
        }
    };
    if source >= shape.column_count {
        return Err(format!(
            "Source column index {source} is out of bounds (the source has {} columns)",
            shape.column_count
        ));
    }
    let index = destination
        .iter()
        .position(|column| column.column_name == spec.destination && eligible(column))
        .or_else(|| {
            destination
                .iter()
                .position(|column| column.column_name.eq_ignore_ascii_case(&spec.destination) && eligible(column))
        })
        .ok_or_else(|| format!("Destination column '{}' not found in table", spec.destination))?;
    Ok(Resolved { source, destination: index, destination_name: destination[index].column_name.clone() })
}
