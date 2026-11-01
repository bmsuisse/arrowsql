//! UTF-8 transfer for Arrow reads.
//!
//! SQL Server sends nvarchar/nchar/ntext as UTF-16, two bytes per character
//! even for ASCII, and TDS has no compression. Casting such a column to a
//! varchar with a UTF-8 collation makes the server send UTF-8 instead, about
//! half the bytes for mostly-ASCII text. The Arrow type stays a UTF-8 string.
//!
//! This module builds the wrapping query from the described result columns;
//! the describe round trip itself is run by the caller.

use pyo3::prelude::*;

const SYS_NTEXT: i32 = 99;
const SYS_NVARCHAR: i32 = 231;
const SYS_NCHAR: i32 = 239;
const UTF8_COLLATION: &str = "Latin1_General_100_BIN2_UTF8";
/// Largest finite varchar length.
const VARCHAR_MAX_LEN: i64 = 8000;

/// One described result column (`sys.dm_exec_describe_first_result_set`).
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub system_type_id: i32,
    /// Bytes; -1 for (max) and ntext.
    pub max_length: i64,
    /// False only when the server says the column never holds NULL.
    pub nullable: bool,
}

fn quote(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// The varchar length that holds a column of `max_length_bytes` UTF-16 bytes
/// as UTF-8 (4 bytes per character at most), or `max` when that exceeds a
/// finite varchar. Never shorter than the worst case, because an explicit
/// CAST truncates silently.
fn varchar_len(max_length_bytes: i64) -> String {
    if max_length_bytes < 0 {
        return "max".into();
    }
    let need = max_length_bytes / 2 * 4;
    if need > VARCHAR_MAX_LEN { "max".into() } else { need.max(1).to_string() }
}

/// `SELECT <list> FROM (<query>) AS q` with national character columns cast to
/// UTF-8 varchar, or `None` when nothing would change or the shape is unsafe
/// (empty or duplicate column names).
pub fn build(query: &str, columns: &[Column]) -> Option<String> {
    if columns.is_empty() {
        return None;
    }
    let mut seen = std::collections::HashSet::new();
    let mut converted = false;
    let mut items = Vec::with_capacity(columns.len());
    for col in columns {
        if col.name.is_empty() || !seen.insert(col.name.to_lowercase()) {
            return None;
        }
        let quoted = quote(&col.name);
        match col.system_type_id {
            SYS_NVARCHAR | SYS_NCHAR | SYS_NTEXT => {
                converted = true;
                // COLLATE must sit on the national operand: a COLLATE after the
                // CAST would only relabel text already squeezed through the
                // database code page ('?' for anything it lacks). ntext cannot
                // take COLLATE, so it goes through nvarchar(max) first.
                let (operand, len) = if col.system_type_id == SYS_NTEXT {
                    (format!("CAST({quoted} AS nvarchar(max))"), "max".to_string())
                } else {
                    (quoted.clone(), varchar_len(col.max_length))
                };
                let cast = format!("CAST({operand} COLLATE {UTF8_COLLATION} AS varchar({len}))");
                // A CAST is always nullable; ISNULL keeps NOT NULL columns NOT NULL.
                items.push(if col.nullable {
                    format!("{cast} AS {quoted}")
                } else {
                    format!("ISNULL({cast}, '') AS {quoted}")
                });
            }
            _ => items.push(quoted),
        }
    }
    if !converted {
        return None;
    }
    Some(format!("SELECT {} FROM ({}\n) AS q", items.join(", "), query))
}

/// Whether the server accepts UTF-8 collations: SQL Server 2019 (major 15)
/// or later, Azure SQL Database (5) and Managed Instance (8).
pub fn server_supports_utf8(product_major_version: Option<i32>, engine_edition: Option<i32>) -> bool {
    matches!(engine_edition, Some(5 | 8)) || product_major_version.is_some_and(|v| v >= 15)
}

/// ddbc_bindings.utf8_wrap_query(query, columns) -> str | None
///
/// `columns` holds (name, system_type_id, max_length, is_nullable) per described column.
#[pyfunction]
pub fn utf8_wrap_query(query: &str, columns: Vec<(Option<String>, i32, i64, Option<bool>)>) -> Option<String> {
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
    build(query, &cols?)
}

/// ddbc_bindings.utf8_collations_supported(product_major_version, engine_edition) -> bool
#[pyfunction]
pub fn utf8_collations_supported(product_major_version: Option<i32>, engine_edition: Option<i32>) -> bool {
    server_supports_utf8(product_major_version, engine_edition)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(utf8_wrap_query, m)?)?;
    m.add_function(wrap_pyfunction!(utf8_collations_supported, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, t: i32, len: i64) -> Column {
        Column { name: name.into(), system_type_id: t, max_length: len, nullable: true }
    }

    #[test]
    fn wraps_national_columns_only() {
        let sql =
            build("SELECT 1", &[col("id", 56, 4), col("a]b", SYS_NVARCHAR, 40), col("v", 167, 10)]).unwrap();
        assert_eq!(
            sql,
            "SELECT [id], CAST([a]]b] COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(80)) AS [a]]b], [v] FROM (SELECT 1\n) AS q"
        );
    }

    #[test]
    fn not_null_stays_not_null() {
        let mut c = col("a", SYS_NCHAR, 6);
        c.nullable = false;
        let sql = build("q", &[c]).unwrap();
        assert!(sql.contains("ISNULL(CAST([a] COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(12)), '') AS [a]"));
    }

    #[test]
    fn lengths() {
        assert_eq!(varchar_len(-1), "max");
        assert_eq!(varchar_len(4000), "8000");
        assert_eq!(varchar_len(4002), "max");
        assert_eq!(varchar_len(8000), "max");
        assert_eq!(varchar_len(2), "4");
    }

    #[test]
    fn falls_back() {
        assert!(build("q", &[col("a", 56, 4)]).is_none());
        assert!(build("q", &[col("a", SYS_NCHAR, 4), col("A", SYS_NCHAR, 4)]).is_none());
        assert!(build("q", &[col("", SYS_NCHAR, 4)]).is_none());
        assert!(build("q", &[]).is_none());
    }

    #[test]
    fn server_gate() {
        assert!(!server_supports_utf8(Some(14), Some(3)));
        assert!(server_supports_utf8(Some(15), Some(3)));
        assert!(server_supports_utf8(Some(12), Some(5)));
        assert!(!server_supports_utf8(None, None));
    }
}
