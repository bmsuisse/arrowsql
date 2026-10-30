//! DB-API qmark (`?`) -> SQL Server RPC names (`@P1`, `@P2`, ...).
//!
//! Markers inside string literals, quoted / bracketed identifiers and
//! comments are left alone. The Python layer already converts pyformat to
//! qmark before calling the bindings.

/// Translate the ODBC procedure-call escape `{call proc(args)}` /
/// `{? = call proc(args)}` into T-SQL `EXEC`. Other text is unchanged.
/// TODO(params): `{d ...}`, `{ts ...}`, `{fn ...}` escapes.
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
    if let Some(translated) = translate_call_escape(sql) {
        return rewrite_qmarks_plain(&translated);
    }
    rewrite_qmarks_plain(sql)
}

fn rewrite_qmarks_plain(sql: &str) -> (String, usize) {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
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
                out.push_str(&count.to_string());
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
    fn escaped_quotes() {
        let (sql, n) = rewrite_qmarks("SELECT 'it''s ?', ?");
        assert_eq!(n, 1);
        assert_eq!(sql, "SELECT 'it''s ?', @P1");
    }
}
