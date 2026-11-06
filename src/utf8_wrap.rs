//! Python entry points of the UTF-8 transfer rewrite (`arrowsql_core::utf8_wrap`).

use arrowsql_core::utf8_wrap::{Column, build, server_supports_utf8};
use pyo3::prelude::*;

/// ddbc_bindings.utf8_wrap_query(query, columns) -> (wrapped_sql, sample_sql) | None
///
/// `columns` holds (name, system_type_id, max_length, is_nullable) per described column.
#[pyfunction]
pub fn utf8_wrap_query(
    query: &str,
    columns: Vec<(Option<String>, i32, i64, Option<bool>)>,
) -> Option<(String, String)> {
    let cols: Option<Vec<Column>> = columns
        .into_iter()
        .map(|(name, system_type_id, max_length, nullable)| {
            name.map(|name| Column {
                name,
                system_type_id,
                max_length,
                nullable: nullable != Some(false),
            })
        })
        .collect();
    build(query, &cols?).map(|r| (r.wrapped, r.sample))
}

/// ddbc_bindings.utf8_collations_supported(product_major_version, engine_edition) -> bool
#[pyfunction]
pub fn utf8_collations_supported(
    product_major_version: Option<i32>,
    engine_edition: Option<i32>,
) -> bool {
    server_supports_utf8(product_major_version, engine_edition)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(utf8_wrap_query, m)?)?;
    m.add_function(wrap_pyfunction!(utf8_collations_supported, m)?)?;
    Ok(())
}
