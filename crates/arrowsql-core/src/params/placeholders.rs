//! DB-API qmark (`?`) -> SQL Server RPC names (`@P1`, `@P2`, ...).
//!
//! Markers inside string literals, quoted / bracketed identifiers and
//! comments are left alone. The Python layer already converts pyformat to
//! qmark before calling the bindings.

use std::cell::RefCell;
use std::fmt::Write;
use std::sync::Arc;

/// Translate the ODBC procedure-call escape `{call proc(args)}` /
/// `{? = call proc(args)}` into T-SQL `EXEC`. Other text is unchanged.
/// The `{d ...}`, `{ts ...}` and `{fn ...}` escapes need no translation: SQL Server parses them itself.
pub fn translate_call_escape(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let inner = trimmed.strip_prefix('{')?.strip_suffix('}')?.trim();
    let (ret, rest) = match inner.strip_prefix('?') {
        Some(after) => {
            let after = after.trim_start().strip_prefix('=')?.trim_start();
            (true, after)
        }
        None => (false, inner),
    };
    // `get` instead of slicing: byte 4 may fall inside a multi-byte character.
    let body = match rest.get(..4) {
        Some(keyword) if keyword.eq_ignore_ascii_case("call") => rest[4..].trim(),
        _ => return None,
    };
    let (name, args) = match body.find('(') {
        Some(open) => {
            let close = body.rfind(')')?;
            if close <= open {
                return None;
            }
            (body[..open].trim(), body[open + 1..close].trim())
        }
        None => (body, ""),
    };
    let target = if ret { format!("EXEC ? = {name}") } else { format!("EXEC {name}") };
    Some(if args.is_empty() { target } else { format!("{target} {args}") })
}

pub fn rewrite_qmarks(sql: &str) -> (String, usize) {
    // A repeated statement (multirow inserts, loops) skips the scan.
    // Per thread, so free-threaded callers do not serialise on one lock.
    type Remembered = Option<(Arc<str>, Arc<str>, usize)>;
    thread_local! {
        static LAST: RefCell<Remembered> = const { RefCell::new(None) };
    }
    let hit = LAST.with(|last| {
        let last = last.borrow();
        match last.as_ref() {
            Some((source, rewritten, count)) if &**source == sql => Some((rewritten.to_string(), *count)),
            _ => None,
        }
    });
    if let Some(hit) = hit {
        return hit;
    }
    let (rewritten, count) = match translate_call_escape(sql) {
        Some(translated) => rewrite_qmarks_plain(&translated),
        None => rewrite_qmarks_plain(sql),
    };
    // Only worth remembering when large enough for the scan to matter.
    if sql.len() >= 256 {
        LAST.with(|last| {
            *last.borrow_mut() = Some((Arc::from(sql), Arc::from(rewritten.as_str()), count));
        });
    }
    (rewritten, count)
}

fn rewrite_qmarks_plain(sql: &str) -> (String, usize) {
    let bytes = sql.as_bytes();
    let markers = bytes.iter().filter(|&&b| b == b'?').count();
    let mut out = String::with_capacity(sql.len() + 4 * markers);
    let mut count = 0usize;
    let mut i = 0;
    let mut last = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'[' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b']' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b']' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                // T-SQL block comments nest.
                let mut depth = 1;
                i += 2;
                while i < bytes.len() && depth > 0 {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            b'?' => {
                count += 1;
                out.push_str(&sql[last..i]);
                out.push_str("@P");
                let _ = write!(out, "{count}");
                i += 1;
                last = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[last.min(sql.len())..]);
    (out, count)
}

#[cfg(test)]
mod tests {
    use super::{rewrite_qmarks, translate_call_escape};

    #[test]
    fn call_escape_translates_procedure_calls() {
        assert_eq!(
            translate_call_escape("{call proc(?, 1)}").as_deref(),
            Some("EXEC proc ?, 1")
        );
        assert_eq!(
            translate_call_escape("{? = call proc(?)}").as_deref(),
            Some("EXEC ? = proc ?")
        );
    }

    #[test]
    fn call_escape_ignores_non_ascii_and_malformed_input() {
        // Byte 4 falls inside a multi-byte character: must not panic.
        assert_eq!(translate_call_escape("{日本語}"), None);
        assert_eq!(translate_call_escape("{call a) x (}"), None);
        assert_eq!(translate_call_escape("{call é}").as_deref(), Some("EXEC é"));
    }

    #[test]
    fn rewrites_only_live_markers() {
        let (sql, n) = rewrite_qmarks("SELECT ?, '?', [a?], \"b?\" -- ?\n, /* ? /* ? */ */ ?");
        assert_eq!(n, 2);
        assert_eq!(sql, "SELECT @P1, '?', [a?], \"b?\" -- ?\n, /* ? /* ? */ */ @P2");
    }

    #[test]
    fn many_markers_and_repeated_large_statements() {
        let sql = format!("INSERT INTO t VALUES ({})", vec!["?"; 2000].join(","));
        let expected = format!(
            "INSERT INTO t VALUES ({})",
            (1..=2000).map(|i| format!("@P{i}")).collect::<Vec<_>>().join(",")
        );
        // The second call is served from the per-thread memo.
        for _ in 0..2 {
            assert_eq!(rewrite_qmarks(&sql), (expected.clone(), 2000));
        }
    }

    #[test]
    fn escaped_quotes() {
        let (sql, n) = rewrite_qmarks("SELECT 'it''s ?', ?");
        assert_eq!(n, 1);
        assert_eq!(sql, "SELECT 'it''s ?', @P1");
    }
}
