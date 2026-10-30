//! Column-uniform parameter types for executemany.
//!
//! All rows of an executemany run as `sp_execute` calls against one prepared
//! declaration (taken from the first row), so every row must carry the same
//! SQL type per column: integers widen to the largest width seen, strings
//! and binaries to the longest value, decimals to a common precision/scale,
//! and NULL cells take the column's type. All-NULL columns are left as
//! untyped NULLs for server-side resolution (`null_types`).

use mssql_tds::datatypes::decoder::{DECIMAL_STR_LEN, DecimalParts};
use mssql_tds::datatypes::sqltypes::SqlType;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IntWidth {
    Tiny,
    Small,
    Int,
    Big,
}

fn int_width(t: &SqlType) -> Option<IntWidth> {
    Some(match t {
        SqlType::TinyInt(_) => IntWidth::Tiny,
        SqlType::SmallInt(_) => IntWidth::Small,
        SqlType::Int(_) => IntWidth::Int,
        SqlType::BigInt(_) => IntWidth::Big,
        _ => return None,
    })
}

fn int_value(t: &SqlType) -> Option<i64> {
    match t {
        SqlType::TinyInt(v) => v.map(i64::from),
        SqlType::SmallInt(v) => v.map(i64::from),
        SqlType::Int(v) => v.map(i64::from),
        SqlType::BigInt(v) => *v,
        _ => None,
    }
}

fn make_int(width: IntWidth, v: Option<i64>) -> SqlType {
    match width {
        IntWidth::Tiny => SqlType::TinyInt(v.map(|v| v as u8)),
        IntWidth::Small => SqlType::SmallInt(v.map(|v| v as i16)),
        IntWidth::Int => SqlType::Int(v.map(|v| v as i32)),
        IntWidth::Big => SqlType::BigInt(v),
    }
}

/// Character length declared for a string type; `None` = MAX.
fn text_len(t: &SqlType) -> Option<Option<u16>> {
    match t {
        SqlType::Varchar(_, n) | SqlType::NVarchar(_, n) => Some(Some(*n)),
        SqlType::VarcharMax(_) | SqlType::NVarcharMax(_) => Some(None),
        _ => None,
    }
}

fn binary_len(t: &SqlType) -> Option<Option<u16>> {
    match t {
        SqlType::VarBinary(_, n) => Some(Some(*n)),
        SqlType::VarBinaryMax(_) => Some(None),
        _ => None,
    }
}

fn is_unicode(t: &SqlType) -> bool {
    matches!(t, SqlType::NVarchar(..) | SqlType::NVarcharMax(_))
}

/// A NULL of the same SQL type as `t`.
pub fn null_like(t: &SqlType) -> SqlType {
    match t {
        SqlType::Bit(_) => SqlType::Bit(None),
        SqlType::TinyInt(_) => SqlType::TinyInt(None),
        SqlType::SmallInt(_) => SqlType::SmallInt(None),
        SqlType::Int(_) => SqlType::Int(None),
        SqlType::BigInt(_) => SqlType::BigInt(None),
        SqlType::Real(_) => SqlType::Real(None),
        SqlType::Float(_) => SqlType::Float(None),
        SqlType::Decimal(_) => SqlType::Decimal(None),
        SqlType::Numeric(_) => SqlType::Numeric(None),
        SqlType::Money(_) => SqlType::Money(None),
        SqlType::SmallMoney(_) => SqlType::SmallMoney(None),
        SqlType::Time(_) => SqlType::Time(None),
        SqlType::DateTime2(_) => SqlType::DateTime2(None),
        SqlType::DateTimeOffset(_) => SqlType::DateTimeOffset(None),
        SqlType::SmallDateTime(_) => SqlType::SmallDateTime(None),
        SqlType::DateTime(_) => SqlType::DateTime(None),
        SqlType::Date(_) => SqlType::Date(None),
        SqlType::NVarchar(_, n) => SqlType::NVarchar(None, *n),
        SqlType::NVarcharMax(_) => SqlType::NVarcharMax(None),
        SqlType::Varchar(_, n) => SqlType::Varchar(None, *n),
        SqlType::VarcharMax(_) => SqlType::VarcharMax(None),
        SqlType::VarBinary(_, n) => SqlType::VarBinary(None, *n),
        SqlType::VarBinaryMax(_) => SqlType::VarBinaryMax(None),
        SqlType::Binary(_, n) => SqlType::Binary(None, *n),
        SqlType::Char(_, n) => SqlType::Char(None, *n),
        SqlType::NChar(_, n) => SqlType::NChar(None, *n),
        SqlType::Text(_) => SqlType::Text(None),
        SqlType::NText(_) => SqlType::NText(None),
        SqlType::Image(_) => SqlType::Image(None),
        SqlType::Json(_) => SqlType::Json(None),
        SqlType::Xml(_) => SqlType::Xml(None),
        SqlType::Uuid(_) => SqlType::Uuid(None),
        SqlType::Vector(_, n, base) => SqlType::Vector(None, *n, *base),
        // No NULL form to derive; keep the untyped default.
        _ => SqlType::NVarchar(None, 1),
    }
}

fn decimal_parts(t: &SqlType) -> Option<&DecimalParts> {
    match t {
        SqlType::Decimal(Some(p)) | SqlType::Numeric(Some(p)) => Some(p),
        _ => None,
    }
}

/// Normalise column `col` of `rows` in place. `is_null[r]` marks cells that
/// came from Python `None`. Returns `false` when the column is all NULL.
fn normalize_column(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> bool {
    let values: Vec<usize> = (0..rows.len()).filter(|&r| !is_null[r]).collect();
    let Some(&first) = values.first() else { return false };

    // Integers (bools count as 0/1 when mixed with integers).
    let any_int = values.iter().any(|&r| int_width(&rows[r][col]).is_some());
    let is_int_like = |t: &SqlType| int_width(t).is_some() || matches!(t, SqlType::Bit(Some(_)));
    if any_int && values.iter().all(|&r| is_int_like(&rows[r][col])) {
        let width = values.iter().filter_map(|&r| int_width(&rows[r][col])).max().unwrap();
        for r in 0..rows.len() {
            let v = match &rows[r][col] {
                _ if is_null[r] => None,
                SqlType::Bit(b) => b.map(i64::from),
                other => int_value(other),
            };
            rows[r][col] = make_int(width, v);
        }
        return true;
    }
    if values
        .iter()
        .all(|&r| matches!(rows[r][col], SqlType::Float(_)) || int_width(&rows[r][col]).is_some())
    {
        for r in 0..rows.len() {
            let v = match &rows[r][col] {
                _ if is_null[r] => None,
                SqlType::Float(v) => *v,
                other => int_value(other).map(|i| i as f64),
            };
            rows[r][col] = SqlType::Float(v);
        }
        return true;
    }
    // Strings: widest declared length; unicode if any value needs it.
    if values.iter().all(|&r| text_len(&rows[r][col]).is_some()) {
        let unicode = values.iter().any(|&r| is_unicode(&rows[r][col]));
        let limit = if unicode { 4000 } else { 8000 };
        let mut max_len: Option<u16> = Some(1);
        for &r in &values {
            max_len = match (max_len, text_len(&rows[r][col]).unwrap()) {
                (Some(a), Some(b)) if a.max(b) <= limit => Some(a.max(b)),
                _ => None,
            };
        }
        for r in 0..rows.len() {
            let value = match std::mem::replace(&mut rows[r][col], SqlType::Bit(None)) {
                SqlType::Varchar(v, _) | SqlType::NVarchar(v, _) => v,
                SqlType::VarcharMax(v) | SqlType::NVarcharMax(v) => v,
                _ => None,
            };
            rows[r][col] = match (unicode, max_len) {
                (true, Some(n)) => SqlType::NVarchar(value, n),
                (true, None) => SqlType::NVarcharMax(value),
                (false, Some(n)) => SqlType::Varchar(value, n),
                (false, None) => SqlType::VarcharMax(value),
            };
        }
        return true;
    }
    if values.iter().all(|&r| binary_len(&rows[r][col]).is_some()) {
        let mut max_len: Option<u16> = Some(1);
        for &r in &values {
            max_len = match (max_len, binary_len(&rows[r][col]).unwrap()) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None,
            };
        }
        for r in 0..rows.len() {
            let value = match std::mem::replace(&mut rows[r][col], SqlType::Bit(None)) {
                SqlType::VarBinary(v, _) | SqlType::VarBinaryMax(v) => v,
                _ => None,
            };
            rows[r][col] = match max_len {
                Some(n) => SqlType::VarBinary(value, n),
                None => SqlType::VarBinaryMax(value),
            };
        }
        return true;
    }
    // Decimals: common scale, enough integer digits for every value.
    if values.iter().all(|&r| decimal_parts(&rows[r][col]).is_some()) {
        let scale = values.iter().map(|&r| decimal_parts(&rows[r][col]).unwrap().scale).max().unwrap();
        let int_digits = values
            .iter()
            .map(|&r| {
                let p = decimal_parts(&rows[r][col]).unwrap();
                p.precision.saturating_sub(p.scale)
            })
            .max()
            .unwrap();
        let precision = (int_digits + scale).clamp(1, 38);
        let scale = scale.min(precision);
        for r in 0..rows.len() {
            rows[r][col] = if is_null[r] {
                SqlType::Decimal(None)
            } else {
                let parts = decimal_parts(&rows[r][col]).unwrap();
                let mut buf = [0u8; DECIMAL_STR_LEN];
                let text = parts.format_into(&mut buf).to_string();
                match DecimalParts::from_string(&text, precision, scale) {
                    Ok(p) => SqlType::Decimal(Some(p)),
                    Err(_) => continue,
                }
            };
        }
        return true;
    }
    // Anything else: NULLs take the first value's type.
    let template = null_like(&rows[first][col]);
    for r in 0..rows.len() {
        if is_null[r] {
            rows[r][col] = template.clone();
        }
    }
    true
}

/// Normalise every column; returns the indexes of all-NULL columns.
pub fn normalize(rows: &mut [Vec<SqlType>], nulls: &[Vec<bool>]) -> Vec<usize> {
    let ncols = rows.first().map_or(0, Vec::len);
    let mut all_null = Vec::new();
    for col in 0..ncols {
        let is_null: Vec<bool> = nulls.iter().map(|row| row[col]).collect();
        if !normalize_column(rows, col, &is_null) {
            all_null.push(col);
        }
    }
    all_null
}
