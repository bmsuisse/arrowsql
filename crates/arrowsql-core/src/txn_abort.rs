//! Policy for statements issued after the server rolled the caller's
//! transaction back (error under `SET XACT_ABORT ON`), shared by the sync and
//! async paths.
//!
//! Contract: once a failed statement took the transaction with it and the
//! session has XACT_ABORT on, every statement that could change data is
//! rejected with SQLSTATE 25000 until `commit()` or `rollback()` ends the
//! state. Such a statement would otherwise run in a fresh transaction and a
//! later `commit()` would persist it while the earlier work was lost
//! (go-mssqldb: `checkServerAbortedTransaction`). Read-only statements still
//! run.

use crate::diag::DiagRecord;

/// Answers "yes" with a server error when `@@OPTIONS` has XACT_ABORT (bit 16384) on.
pub const XACT_ABORT_PROBE: &str = "IF (@@OPTIONS & 16384) = 16384 RAISERROR('xact_abort', 16, 1)";

/// The diagnostic of a rejected statement.
pub fn rejection() -> DiagRecord {
    DiagRecord::driver(
        "25000",
        "The server rolled back the transaction after an error; the statement was not run. Call rollback() to continue.",
    )
}

/// Words that start a statement which may still run in an aborted state.
const READ_STARTS: [&str; 5] = ["SELECT", "WITH", "SET", "DECLARE", "PRINT"];
/// Words that change data or schema or run code that can.
const WRITE_WORDS: [&str; 12] = [
    "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "CREATE", "ALTER", "DROP", "EXEC", "EXECUTE", "BULK", "INTO",
];

/// Whether `sql` is certainly read-only: it starts like a query and none of
/// its words (outside comments, string literals and quoted identifiers)
/// writes. Anything else, including `BEGIN TRY ... END TRY` and `IF` blocks,
/// counts as a write.
pub fn is_read_only(sql: &str) -> bool {
    let mut words = words(sql);
    let Some(first) = words.next() else { return true };
    READ_STARTS.iter().any(|w| first.eq_ignore_ascii_case(w))
        && !std::iter::once(first).chain(words).any(|w| WRITE_WORDS.iter().any(|x| w.eq_ignore_ascii_case(x)))
}

/// The identifier-like words of `sql`, skipping comments, `'...'` literals
/// and `"..."` / `[...]` quoted identifiers.
fn words(sql: &str) -> impl Iterator<Item = &str> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    std::iter::from_fn(move || {
        while i < bytes.len() {
            match bytes[i] {
                b'-' if bytes.get(i + 1) == Some(&b'-') => {
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                }
                b'/' if bytes.get(i + 1) == Some(&b'*') => {
                    // T-SQL block comments nest.
                    let mut depth = 0usize;
                    while i < bytes.len() {
                        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                            depth += 1;
                            i += 2;
                        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                            depth -= 1;
                            i += 2;
                            if depth == 0 {
                                break;
                            }
                        } else {
                            i += 1;
                        }
                    }
                }
                quote @ (b'\'' | b'"' | b'[') => {
                    let close = if quote == b'[' { b']' } else { quote };
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == close {
                            // A doubled closing character is an escaped one.
                            if bytes.get(i + 1) == Some(&close) {
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i += 1;
                    }
                    i += 1;
                }
                b if b.is_ascii_alphabetic() || b == b'_' => {
                    let start = i;
                    while i < bytes.len()
                        && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'@' | b'#' | b'$'))
                    {
                        i += 1;
                    }
                    return Some(&sql[start..i]);
                }
                _ => i += 1,
            }
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::is_read_only;

    #[test]
    fn writes_are_found_behind_comments_ctes_and_blocks() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "/* c */ INSERT INTO t VALUES (1)",
            "-- c\n  update t set a = 1",
            "WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT a FROM x",
            "with x as (select 1 as a) delete from t",
            "SELECT 1 AS a INTO #copy",
            "BEGIN TRY INSERT INTO t VALUES (1) END TRY BEGIN CATCH END CATCH",
            "IF 1 = 1 INSERT INTO t VALUES (1)",
            "SELECT 1; EXEC p",
            "/* /* nested */ */ DROP TABLE t",
            "{call p(?)}",
        ] {
            assert!(!is_read_only(sql), "{sql}");
        }
    }

    #[test]
    fn plain_reads_stay_allowed() {
        for sql in [
            "SELECT @@TRANCOUNT",
            "/* insert */ SELECT 1 -- delete",
            "SELECT 'drop table t', [update] FROM t WHERE a = 'it''s insert'",
            "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
            "(SELECT 1)",
            "SET NOCOUNT ON",
            "SELECT create_date FROM sys.objects",
            "   ",
        ] {
            assert!(is_read_only(sql), "{sql}");
        }
    }
}
