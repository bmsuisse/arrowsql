//! Native port of parameter_helper.py's pyformat scanner.
//!
//! Semantics intentionally mirror the Python original (not `placeholders.rs`):
//! a `--` comment ends only at `\n`, `/* */` nests, `''` `""` `]]` are escapes,
//! an unterminated context runs to the end, and the `)` search of `%(name)s`
//! does not honour quotes. Offsets are code-point indices, as Python uses.

use pyo3::prelude::*;

/// Position after the quoted context starting at `i`, or `None` if none starts there.
fn skip_quoted_context(c: &[char], i: usize) -> Option<usize> {
    let n = c.len();
    let ch = c[i];
    if ch == '-' && i + 1 < n && c[i + 1] == '-' {
        let mut i = i + 2;
        while i < n && c[i] != '\n' {
            i += 1;
        }
        return Some(i);
    }
    if ch == '/' && i + 1 < n && c[i + 1] == '*' {
        let mut i = i + 2;
        let mut depth = 1;
        while i < n && depth > 0 {
            if i + 1 < n && c[i] == '/' && c[i + 1] == '*' {
                depth += 1;
                i += 2;
            } else if i + 1 < n && c[i] == '*' && c[i + 1] == '/' {
                depth -= 1;
                i += 2;
            } else {
                i += 1;
            }
        }
        return Some(i.min(n));
    }
    let close = match ch {
        '\'' => '\'',
        '"' => '"',
        '[' => ']',
        _ => return None,
    };
    let mut i = i + 1;
    loop {
        while i < n && c[i] != close {
            i += 1;
        }
        if i >= n {
            return Some(n);
        }
        if i + 1 < n && c[i + 1] == close {
            i += 2;
            continue;
        }
        return Some(i + 1);
    }
}

fn scan(sql: &str) -> Vec<(String, usize, usize)> {
    let c: Vec<char> = sql.chars().collect();
    let n = c.len();
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        if let Some(next) = skip_quoted_context(&c, i) {
            i = next;
            continue;
        }
        if i + 2 < n && c[i] == '%' && c[i + 1] == '(' {
            let mut j = i + 2;
            while j < n && c[j] != ')' {
                j += 1;
            }
            if j + 1 < n && c[j + 1] == 's' {
                out.push((c[i + 2..j].iter().collect(), i, j + 2));
                i = j + 2;
                continue;
            }
        }
        i += 1;
    }
    out
}

fn has_unquoted_qmark_impl(sql: &str) -> bool {
    let c: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < c.len() {
        if let Some(next) = skip_quoted_context(&c, i) {
            i = next;
            continue;
        }
        if c[i] == '?' {
            return true;
        }
        i += 1;
    }
    false
}

/// `(name, start, end)` of every executable `%(name)s`, as code-point offsets.
#[pyfunction]
pub fn scan_pyformat(sql: &str) -> Vec<(String, usize, usize)> {
    scan(sql)
}

/// True if `sql` has a `?` outside quotes, brackets and comments.
#[pyfunction]
pub fn has_unquoted_qmark(sql: &str) -> bool {
    has_unquoted_qmark_impl(sql)
}
