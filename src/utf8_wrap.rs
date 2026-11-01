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
/// Minimum share (percent) of the declared row width that national character
/// columns must have for the rewrite to pay. Below it the saved bytes are
/// negligible (the taxi table: one nvarchar(1) among 19 numeric columns).
const MIN_SHARE_PERCENT: i64 = 25;

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

/// The rewritten query and a probe that sizes a sample of the data.
#[derive(Debug)]
pub struct Rewrite {
    /// `SELECT <list> FROM (<query>) AS q`, national columns cast to UTF-8 varchar.
    pub wrapped: String,
    /// One row: (UTF-8 bytes, UTF-16 bytes) of the national columns in the first
    /// [`SAMPLE_ROWS`] rows. UTF-8 is larger than UTF-16 for CJK and similar
    /// scripts (3 bytes against 2), so the caller compares them before using `wrapped`.
    pub sample: String,
}

/// Rows the size probe looks at.
const SAMPLE_ROWS: usize = 256;

/// The rewrite of `query`, or `None` when nothing would change or the shape is
/// unsafe (empty or duplicate column names, small national share).
pub fn build(query: &str, columns: &[Column]) -> Option<Rewrite> {
    if columns.is_empty() {
        return None;
    }
    let mut seen = std::collections::HashSet::new();
    let mut converted = false;
    let (mut national_bytes, mut total_bytes) = (0i64, 0i64);
    let mut items = Vec::with_capacity(columns.len());
    let (mut utf8_terms, mut utf16_terms) = (Vec::new(), Vec::new());
    for col in columns {
        if col.name.is_empty() || !seen.insert(col.name.to_lowercase()) {
            return None;
        }
        let quoted = quote(&col.name);
        // (max) and legacy LOB types have no declared width: assume 8000 bytes.
        let width = if col.max_length < 0 { VARCHAR_MAX_LEN } else { col.max_length };
        total_bytes += width;
        if matches!(col.system_type_id, SYS_NVARCHAR | SYS_NCHAR | SYS_NTEXT) {
            national_bytes += width;
        }
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
                utf8_terms.push(format!("ISNULL(DATALENGTH({cast}), 0)"));
                utf16_terms.push(format!("ISNULL(DATALENGTH({quoted}), 0)"));
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
    if !converted || national_bytes * 100 < total_bytes * MIN_SHARE_PERCENT {
        return None;
    }
    let sample = format!(
        "SELECT SUM(CAST({} AS bigint)), SUM(CAST({} AS bigint)) FROM (SELECT TOP ({SAMPLE_ROWS}) * FROM ({}\n) AS s) AS q",
        utf8_terms.join(" + "),
        utf16_terms.join(" + "),
        query
    );
    Some(Rewrite { wrapped: format!("SELECT {} FROM ({}\n) AS q", items.join(", "), query), sample })
}

/// Whether the server accepts UTF-8 collations: SQL Server 2019 (major 15)
/// or later, Azure SQL Database (5) and Managed Instance (8).
pub fn server_supports_utf8(product_major_version: Option<i32>, engine_edition: Option<i32>) -> bool {
    matches!(engine_edition, Some(5 | 8)) || product_major_version.is_some_and(|v| v >= 15)
}

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
            build("SELECT 1", &[col("id", 56, 4), col("a]b", SYS_NVARCHAR, 40), col("v", 167, 10)]).unwrap()
            .wrapped;
        assert_eq!(
            sql,
            "SELECT [id], CAST([a]]b] COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(80)) AS [a]]b], [v] FROM (SELECT 1\n) AS q"
        );
    }

    #[test]
    fn not_null_stays_not_null() {
        let mut c = col("a", SYS_NCHAR, 6);
        c.nullable = false;
        let sql = build("q", &[c]).unwrap().wrapped;
        assert!(sql.contains("ISNULL(CAST([a] COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(12)), '') AS [a]"));
    }

    #[test]
    fn small_string_share_is_left_alone() {
        let mut cols: Vec<Column> = (0..19).map(|i| col(&format!("n{i}"), 127, 8)).collect();
        cols.push(col("flag", SYS_NVARCHAR, 2));
        assert!(build("q", &cols).is_none());
        cols.push(col("name", SYS_NVARCHAR, 200));
        assert!(build("q", &cols).is_some());
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
