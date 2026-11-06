//! UTF-8 transfer of national character columns for Arrow reads.
//!
//! SQL Server sends nvarchar/nchar/ntext as UTF-16 (two bytes per character,
//! even ASCII) and TDS has no compression. [`utf8_query`] rewrites a query so
//! those columns travel as `varchar` in a UTF-8 collation, about half the
//! bytes for mostly-ASCII text; the decoded Arrow type is the same string
//! type. The SQL text is built by [`crate::utf8_wrap`]; this module runs the
//! describe round trip and decides when to skip.
//!
//! Every failure path returns the original query: the rewrite is an
//! optimization, never a reason for a read to fail.

use std::sync::atomic::Ordering;

use crate::api::{Connection, Param, Value};
use crate::utf8_wrap::{Column, build, server_supports_utf8};

/// One round trip: server capability plus the described, visible columns.
const DESCRIBE_SQL: &str = "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int), \
     CAST(SERVERPROPERTY('EngineEdition') AS int), \
     d.name, d.system_type_id, d.max_length, d.is_nullable \
     FROM sys.dm_exec_describe_first_result_set(?, NULL, 0) AS d \
     WHERE d.is_hidden = 0 ORDER BY d.column_ordinal";
/// Rewrite only when UTF-8 is at most this share of the UTF-16 bytes of the
/// sampled rows. CJK is 3 bytes per character in UTF-8 against 2 in UTF-16.
const MAX_SIZE_RATIO: f64 = 0.9;
/// Most described columns read (a result has at most 4096 columns).
const MAX_COLUMNS: usize = 4096;

/// `ConnShared::utf8_support`: not asked yet / UTF-8 collations / none.
pub const SUPPORT_UNKNOWN: u8 = 0;
pub const SUPPORT_YES: u8 = 1;
pub const SUPPORT_NO: u8 = 2;

/// `query` rewritten to transfer nvarchar/nchar/ntext columns as UTF-8.
///
/// Returns `query` itself when the server predates UTF-8 collations (SQL
/// Server 2019 / Azure SQL), the query has an ORDER BY (the rewrite puts it in
/// a derived table, which drops it), no column would change, the column names
/// are empty or duplicated, the describe or the sample probe fails, or the
/// first 256 rows are not at least 10% smaller in UTF-8.
pub fn utf8_query(conn: &Connection, query: &str) -> String {
    let support = &conn.shared().utf8_support;
    if has_order_by(query) || support.load(Ordering::Relaxed) == SUPPORT_NO {
        return query.to_string();
    }
    match rewrite(conn, query) {
        Ok(Some(wrapped)) => wrapped,
        Ok(None) => query.to_string(),
        Err(err) => {
            log::debug!("utf8_strings: keeping the original query ({err})");
            query.to_string()
        }
    }
}

fn rewrite(conn: &Connection, query: &str) -> Result<Option<String>, crate::Error> {
    let rows = conn
        .execute(DESCRIBE_SQL, &[Param::Str(query.to_string())])?
        .scalar_rows(MAX_COLUMNS)?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let int = |v: Option<&Value>| {
        v.and_then(Value::as_int)
            .and_then(|v| i32::try_from(v).ok())
    };
    let supported = server_supports_utf8(int(first.first()), int(first.get(1)));
    conn.shared().utf8_support.store(
        if supported { SUPPORT_YES } else { SUPPORT_NO },
        Ordering::Relaxed,
    );
    if !supported {
        return Ok(None);
    }
    let columns: Option<Vec<Column>> = rows
        .iter()
        .map(|row| {
            let name = match row.get(2) {
                Some(Value::Text(name)) => name.clone(),
                _ => return None,
            };
            Some(Column {
                name,
                system_type_id: int(row.get(3)).unwrap_or(0),
                max_length: row.get(4).and_then(Value::as_int).unwrap_or(0),
                nullable: row.get(5).is_none_or(|v| *v != Value::Bool(false)),
            })
        })
        .collect();
    let Some(plan) = columns.and_then(|columns| build(query, &columns)) else {
        return Ok(None);
    };
    // Runs the query's first rows: also finds out that the rewrite is invalid
    // (batches, EXEC, ...) and how big the strings are in UTF-8.
    let sample = conn.execute(&plan.sample, &[])?.scalar_rows(1)?;
    let sizes = sample.first().map(|row| {
        (
            row.first().and_then(Value::as_int),
            row.get(1).and_then(Value::as_int),
        )
    });
    let (utf8_bytes, utf16_bytes) = sizes.unwrap_or((None, None));
    match (utf8_bytes, utf16_bytes) {
        (Some(utf8), Some(utf16))
            if utf16 != 0 && (utf8 as f64) <= MAX_SIZE_RATIO * utf16 as f64 =>
        {
            Ok(Some(plan.wrapped))
        }
        (utf8, utf16) => {
            log::debug!("utf8_strings: no saving in sample ({utf8:?} vs {utf16:?} bytes)");
            Ok(None)
        }
    }
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `\bORDER\s+BY\b`, ignoring case.
fn has_order_by(query: &str) -> bool {
    let chars: Vec<char> = query.chars().collect();
    let word_at = |i: usize, word: &str| {
        word.chars()
            .enumerate()
            .all(|(k, w)| chars.get(i + k).is_some_and(|c| c.eq_ignore_ascii_case(&w)))
    };
    (0..chars.len()).any(|i| {
        if (i > 0 && is_word(chars[i - 1])) || !word_at(i, "ORDER") {
            return false;
        }
        let mut j = i + 5;
        let spaces = j;
        while chars.get(j).is_some_and(|c| c.is_whitespace()) {
            j += 1;
        }
        j > spaces && word_at(j, "BY") && !chars.get(j + 2).is_some_and(|c| is_word(*c))
    })
}

#[cfg(test)]
mod tests {
    use super::has_order_by;

    #[test]
    fn order_by_detection() {
        assert!(has_order_by("SELECT a FROM t ORDER BY a"));
        assert!(has_order_by("select a from t order\n  by a"));
        assert!(!has_order_by("SELECT reorder BY FROM t"));
        assert!(!has_order_by("SELECT a FROM t ORDER_BY"));
        assert!(!has_order_by("SELECT ORDERBY FROM t"));
        assert!(!has_order_by("SELECT a FROM t ORDER BYTES"));
        assert!(has_order_by("ORDER BY"));
    }
}
