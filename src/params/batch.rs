//! Column-uniform parameter types for executemany.
//!
//! All rows of an executemany run as `sp_execute` calls against one prepared
//! declaration (taken from the first row), so every row must carry the same
//! SQL type per column: integers widen to the largest width seen, strings
//! and binaries to the longest value, and NULL cells take the column's type.
//! Numeric cells must not lose precision on the way: integers and floats share
//! a float column only while every integer is exact as an `f64`; otherwise,
//! and whenever a Decimal is present, the column becomes one DECIMAL that holds
//! every value. All-NULL columns are left as untyped NULLs for server-side
//! resolution (`null_types`).

use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::decoder::{DECIMAL_STR_LEN, DecimalParts};
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::error::Error;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, RpcTypeMetadata, StatusFlags};

/// Largest integer magnitude an `f64` holds exactly (2^53).
const MAX_EXACT_F64_INT: u64 = 1 << 53;

/// SQL Server's maximum DECIMAL precision.
const MAX_DECIMAL_PRECISION: usize = 38;

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

/// Integer value of an integer or bit cell; `None` for NULL or other types.
fn int_value(t: &SqlType) -> Option<i64> {
    match t {
        SqlType::TinyInt(v) => v.map(i64::from),
        SqlType::SmallInt(v) => v.map(i64::from),
        SqlType::Int(v) => v.map(i64::from),
        SqlType::BigInt(v) => *v,
        SqlType::Bit(v) => v.map(i64::from),
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

/// Whether a cell is a numeric type this module can unify.
fn is_numeric(t: &SqlType) -> bool {
    int_width(t).is_some()
        || matches!(
            t,
            SqlType::Bit(_) | SqlType::Float(_) | SqlType::Decimal(_) | SqlType::Numeric(_)
        )
}

/// Which numeric kinds a column mixes; decides the unified type.
#[derive(Default)]
struct NumericMix {
    float: bool,
    decimal: bool,
    /// An integer an `f64` cannot hold exactly.
    wide_int: bool,
}

impl NumericMix {
    fn of(rows: &[Vec<SqlType>], col: usize, values: &[usize]) -> Self {
        let mut mix = Self::default();
        for &r in values {
            match &rows[r][col] {
                SqlType::Float(_) => mix.float = true,
                SqlType::Decimal(_) | SqlType::Numeric(_) => mix.decimal = true,
                t => {
                    mix.wide_int |= int_value(t).is_some_and(|v| v.unsigned_abs() > MAX_EXACT_F64_INT);
                }
            }
        }
        mix
    }

    /// True when a plain float column would round a value.
    fn needs_decimal(&self) -> bool {
        self.decimal || (self.float && self.wide_int)
    }
}

/// Decimal text of a DECIMAL value, as `DecimalParts::format_into` prints it.
fn decimal_text(p: &DecimalParts) -> String {
    let mut buf = [0u8; DECIMAL_STR_LEN];
    p.format_into(&mut buf).to_string()
}

/// Exact decimal text of a numeric cell; `None` for NULL.
fn exact_decimal_text(t: &SqlType) -> TdsResult<Option<String>> {
    match t {
        SqlType::Float(Some(v)) if !v.is_finite() => Err(Error::UsageError(
            "NaN and infinite floats cannot share a column with integers or decimals".into(),
        )),
        // `Display` for f64 prints the shortest round-trip digits, never an exponent.
        SqlType::Float(v) => Ok(v.map(|v| v.to_string())),
        SqlType::Decimal(p) | SqlType::Numeric(p) => Ok(p.as_ref().map(decimal_text)),
        _ => Ok(int_value(t).map(|v| v.to_string())),
    }
}

fn fraction_digits(text: &str) -> usize {
    text.split_once('.').map_or(0, |(_, frac)| frac.len())
}

fn integer_digits(text: &str) -> usize {
    let digits = text.trim_start_matches('-');
    let int = digits.split_once('.').map_or(digits, |(int, _)| int);
    int.trim_start_matches('0').len()
}

/// Gives every cell of a numeric column one DECIMAL(precision, scale) type
/// that holds all of them exactly. Fails when the common precision exceeds 38.
fn unify_decimal(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<()> {
    if rows.iter().zip(is_null).all(|(row, &null)| null || decimal_parts(&row[col]).is_some()) {
        return unify_decimal_parts(rows, col, is_null);
    }
    if unify_int_decimal_parts(rows, col, is_null)? {
        return Ok(());
    }
    unify_decimal_text(rows, col, is_null)
}

/// [`unify_decimal`] through exact decimal text; handles float cells.
fn unify_decimal_text(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<()> {
    let texts = rows
        .iter()
        .zip(is_null)
        .map(|(row, &null)| if null { Ok(None) } else { exact_decimal_text(&row[col]) })
        .collect::<TdsResult<Vec<_>>>()?;
    let scale = texts.iter().flatten().map(|t| fraction_digits(t)).max().unwrap_or(0);
    let int_digits = texts.iter().flatten().map(|t| integer_digits(t)).max().unwrap_or(0);
    // Zero integer digits are fine ("0.5"); a bare 0 still needs precision 1.
    let precision = (int_digits + scale).max(1);
    if precision > MAX_DECIMAL_PRECISION {
        return Err(Error::TypeConversionError(format!(
            "values in one column need a numeric precision of {precision}, above the maximum of {MAX_DECIMAL_PRECISION}"
        )));
    }
    // Both fit in u8: `precision` is at most 38 and `scale` is at most `precision`.
    let (precision, scale) = (precision as u8, scale as u8);
    for (row, text) in rows.iter_mut().zip(texts) {
        row[col] = match text {
            Some(text) => SqlType::Decimal(Some(DecimalParts::from_string(&text, precision, scale)?)),
            None => SqlType::Decimal(None),
        };
    }
    Ok(())
}

use crate::numeric::POW10;

/// Number of decimal digits of `v`, 0 for 0 (leading zeros are not digits).
fn digit_count(v: u128) -> usize {
    POW10.partition_point(|&p| p <= v)
}

/// [`unify_decimal`] for a column of ints and Decimals without floats, which
/// is the same text-based unification done on the integer magnitudes. Returns
/// false, leaving `rows` untouched, when a float cell needs the text path.
fn unify_int_decimal_parts(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<bool> {
    let (mut scale, mut int_digits) = (0usize, 0usize);
    for (row, &null) in rows.iter().zip(is_null) {
        if null {
            continue;
        }
        if let Some(p) = decimal_parts(&row[col]) {
            let s = usize::from(p.scale);
            scale = scale.max(s);
            // A declared scale of at most 38 indexes the table.
            int_digits = int_digits.max(digit_count(p.magnitude() / POW10[s.min(38)]));
        } else if let Some(v) = int_value(&row[col]) {
            int_digits = int_digits.max(digit_count(u128::from(v.unsigned_abs())));
        } else if matches!(row[col], SqlType::Float(Some(_))) {
            return Ok(false);
        }
    }
    let precision = (int_digits + scale).max(1);
    if precision > MAX_DECIMAL_PRECISION {
        return Err(Error::TypeConversionError(format!(
            "values in one column need a numeric precision of {precision}, above the maximum of {MAX_DECIMAL_PRECISION}"
        )));
    }
    // Both fit in u8: `precision` is at most 38 and `scale` is at most `precision`.
    let (precision, scale) = (precision as u8, scale);
    for (row, &null) in rows.iter_mut().zip(is_null) {
        let cell = &row[col];
        row[col] = if null {
            SqlType::Decimal(None)
        } else if let Some(p) = decimal_parts(cell) {
            let magnitude = p.magnitude() * POW10[scale - usize::from(p.scale)];
            SqlType::Decimal(Some(DecimalParts::new(p.is_positive, precision, scale as u8, magnitude)))
        } else if let Some(v) = int_value(cell) {
            let magnitude = u128::from(v.unsigned_abs()) * POW10[scale];
            SqlType::Decimal(Some(DecimalParts::new(v >= 0, precision, scale as u8, magnitude)))
        } else {
            SqlType::Decimal(None)
        };
    }
    Ok(true)
}

fn decimal_parts(t: &SqlType) -> Option<&DecimalParts> {
    match t {
        SqlType::Decimal(Some(p)) | SqlType::Numeric(Some(p)) => Some(p),
        _ => None,
    }
}

/// [`unify_decimal`] for a column of Decimal cells only: rescales the
/// magnitudes without formatting them as text.
fn unify_decimal_parts(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<()> {
    let (mut scale, mut int_digits) = (0u8, 0u8);
    for part in rows.iter().filter_map(|row| decimal_parts(&row[col])) {
        scale = scale.max(part.scale);
        int_digits = int_digits.max(part.precision.saturating_sub(part.scale));
    }
    let precision = (usize::from(int_digits) + usize::from(scale)).max(1);
    if precision > MAX_DECIMAL_PRECISION {
        return Err(Error::TypeConversionError(format!(
            "values in one column need a numeric precision of {precision}, above the maximum of {MAX_DECIMAL_PRECISION}"
        )));
    }
    // At most 38, so it fits in u8.
    let precision = precision as u8;
    for (row, &null) in rows.iter_mut().zip(is_null) {
        row[col] = match decimal_parts(&row[col]) {
            Some(p) if !null => {
                // Below 10^precision < 2^127, so the rescaled magnitude cannot overflow.
                let magnitude = p.magnitude() * POW10[usize::from(scale - p.scale)];
                SqlType::Decimal(Some(DecimalParts::new(p.is_positive, precision, scale, magnitude)))
            }
            _ => SqlType::Decimal(None),
        };
    }
    Ok(())
}

/// Gives every cell of an integer column the widest integer type in the column.
fn widen_to_int(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool], width: IntWidth) {
    for (row, &null) in rows.iter_mut().zip(is_null) {
        let v = if null { None } else { int_value(&row[col]) };
        row[col] = make_int(width, v);
    }
}

/// Gives every cell of an int/float column a FLOAT type. Only called when every
/// integer is exact as an `f64`.
fn widen_to_float(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) {
    for (row, &null) in rows.iter_mut().zip(is_null) {
        let v = match &row[col] {
            _ if null => None,
            SqlType::Float(v) => *v,
            other => int_value(other).map(|i| i as f64),
        };
        row[col] = SqlType::Float(v);
    }
}

/// Gives every string cell the same string type. Unicode if any value needs
/// it; the length is the longest value, or MAX past the 4000/8000 limit.
fn widen_text(rows: &mut [Vec<SqlType>], col: usize, values: &[usize]) {
    let unicode = values.iter().any(|&r| is_unicode(&rows[r][col]));
    let limit: u16 = if unicode { 4000 } else { 8000 };
    // `None` once any value is MAX or the widest value passes the limit.
    let max_len = values.iter().try_fold(1u16, |acc, &r| {
        let n = text_len(&rows[r][col])??;
        let widest = acc.max(n);
        (widest <= limit).then_some(widest)
    });
    for row in rows.iter_mut() {
        let value = match std::mem::replace(&mut row[col], SqlType::Bit(None)) {
            SqlType::Varchar(v, _) | SqlType::NVarchar(v, _) => v,
            SqlType::VarcharMax(v) | SqlType::NVarcharMax(v) => v,
            _ => None,
        };
        row[col] = match (unicode, max_len) {
            (true, Some(n)) => SqlType::NVarchar(value, n),
            (true, None) => SqlType::NVarcharMax(value),
            (false, Some(n)) => SqlType::Varchar(value, n),
            (false, None) => SqlType::VarcharMax(value),
        };
    }
}

/// Gives every binary cell the same binary type, sized to the longest value
/// or MAX.
fn widen_binary(rows: &mut [Vec<SqlType>], col: usize, values: &[usize]) {
    let max_len = values.iter().try_fold(1u16, |acc, &r| {
        let n = binary_len(&rows[r][col])??;
        Some(acc.max(n))
    });
    for row in rows.iter_mut() {
        let value = match std::mem::replace(&mut row[col], SqlType::Bit(None)) {
            SqlType::VarBinary(v, _) | SqlType::VarBinaryMax(v) => v,
            _ => None,
        };
        row[col] = match max_len {
            Some(n) => SqlType::VarBinary(value, n),
            None => SqlType::VarBinaryMax(value),
        };
    }
}

/// Normalise column `col` of `rows` in place. `is_null[r]` marks cells that
/// came from Python `None`. Returns `Ok(false)` when the column is all NULL.
fn normalize_column(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<bool> {
    let values: Vec<usize> = (0..rows.len()).filter(|&r| !is_null[r]).collect();
    let Some(&first) = values.first() else { return Ok(false) };

    if values.iter().all(|&r| is_numeric(&rows[r][col])) {
        let mix = NumericMix::of(rows, col, &values);
        if mix.needs_decimal() {
            unify_decimal(rows, col, is_null)?;
            return Ok(true);
        }
        if mix.float {
            widen_to_float(rows, col, is_null);
            return Ok(true);
        }
        let widest = values.iter().filter_map(|&r| int_width(&rows[r][col])).max();
        if let Some(width) = widest {
            widen_to_int(rows, col, is_null, width);
            return Ok(true);
        }
    }
    if values.iter().all(|&r| text_len(&rows[r][col]).is_some()) {
        widen_text(rows, col, &values);
        return Ok(true);
    }
    if values.iter().all(|&r| binary_len(&rows[r][col]).is_some()) {
        widen_binary(rows, col, &values);
        return Ok(true);
    }
    // Anything else: NULLs take the first value's type.
    let template = null_like(&rows[first][col]);
    for r in 0..rows.len() {
        if is_null[r] {
            rows[r][col] = template.clone();
        }
    }
    Ok(true)
}

/// Normalise every column; returns the indexes of all-NULL columns.
///
/// Fails when a column's values cannot share one exact numeric type.
pub fn normalize(rows: &mut [Vec<SqlType>], nulls: &[Vec<bool>]) -> TdsResult<Vec<usize>> {
    let ncols = rows.first().map_or(0, Vec::len);
    if let Some((index, row)) = rows.iter().enumerate().find(|(_, row)| row.len() != ncols) {
        return Err(mssql_tds::error::Error::TypeConversionError(format!(
            "{}executemany row {index} has {} parameters, but the first row has {ncols}",
            crate::params::INDEX_ERROR_PREFIX,
            row.len()
        )));
    }
    let mut all_null = Vec::new();
    for col in 0..ncols {
        let is_null: Vec<bool> = nulls.iter().map(|row| row[col]).collect();
        if !normalize_column(rows, col, &is_null)? {
            all_null.push(col);
        }
    }
    Ok(all_null)
}

/// Precision/scale a NULL decimal cell cannot carry, per column, taken from the first
/// non-NULL cell of the (normalised) column. `None` for other columns.
///
/// The prepared declaration comes from row 0, so a NULL there would otherwise
/// declare the TDS default (`decimal(18,10)`) for the whole
/// batch and round every later value to it.
pub fn column_metadata(rows: &[Vec<SqlType>]) -> Vec<Option<RpcTypeMetadata>> {
    let ncols = rows.first().map_or(0, Vec::len);
    (0..ncols)
        .map(|col| match &rows[0][col] {
            SqlType::Decimal(None) | SqlType::Numeric(None) => rows.iter().find_map(|row| {
                decimal_parts(&row[col])
                    .map(|p| RpcTypeMetadata { precision: Some(p.precision), scale: Some(p.scale) })
            }),
            _ => None,
        })
        .collect()
}

/// The RPC parameters of one executemany row. `metadata` (from
/// [`column_metadata`]) types the NULL cells of decimal columns;
/// `named` adds `@P<n>` names (sp_execute takes values positionally, so the
/// sync path omits them).
pub fn rpc_parameters(types: Vec<SqlType>, metadata: &[Option<RpcTypeMetadata>], named: bool) -> Vec<RpcParameter> {
    types
        .into_iter()
        .enumerate()
        .map(|(j, t)| {
            let name = named.then(|| crate::params::param_name(j + 1));
            let parameter = RpcParameter::new(name, StatusFlags::NONE, t);
            match metadata.get(j).copied().flatten() {
                Some(m) => parameter.with_type_metadata(m),
                None => parameter,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIG: i64 = 9007199254740993; // 2^53 + 1

    fn dec(text: &str, precision: u8, scale: u8) -> SqlType {
        SqlType::Decimal(Some(DecimalParts::from_string(text, precision, scale).unwrap()))
    }

    /// Normalises one column of `cells`, where `nulls[i]` marks Python `None`.
    fn normalized(cells: Vec<SqlType>, nulls: &[bool]) -> TdsResult<(Vec<SqlType>, Vec<usize>)> {
        let mut rows: Vec<Vec<SqlType>> = cells.into_iter().map(|c| vec![c]).collect();
        let nulls: Vec<Vec<bool>> = nulls.iter().map(|&n| vec![n]).collect();
        let all_null = normalize(&mut rows, &nulls)?;
        Ok((rows.into_iter().map(|mut r| r.remove(0)).collect(), all_null))
    }

    fn parts_of(t: &SqlType) -> Option<(bool, u8, u8, u128)> {
        match t {
            SqlType::Decimal(Some(p)) => Some((p.is_positive, p.precision, p.scale, p.magnitude())),
            SqlType::Decimal(None) => None,
            _ => panic!("expected a decimal cell"),
        }
    }

    #[test]
    fn ragged_rows_are_an_error_not_a_panic() {
        let mut rows = vec![vec![SqlType::Int(Some(1)), SqlType::Int(Some(2))], vec![SqlType::Int(Some(3))]];
        let nulls = vec![vec![false, false], vec![false]];
        assert!(normalize(&mut rows, &nulls).is_err());
        let mut rows = vec![vec![SqlType::Int(Some(1))], vec![SqlType::Int(Some(3)), SqlType::Int(Some(4))]];
        let nulls = vec![vec![false], vec![false, false]];
        assert!(normalize(&mut rows, &nulls).is_err());
    }

    /// The int/Decimal fast path must agree with the text path, errors included.
    #[test]
    fn int_decimal_fast_path_matches_the_text_path() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..400 {
            let n = 1 + (next() % 6) as usize;
            let mut cells = Vec::new();
            let mut nulls = Vec::new();
            for _ in 0..n {
                let null = next() % 7 == 0;
                nulls.push(null);
                cells.push(match next() % 6 {
                    0 => SqlType::BigInt(Some(i64::MIN)),
                    1 => SqlType::Int(Some((next() as i32) >> (next() % 31))),
                    2 => SqlType::BigInt(Some((next() as i64) >> (next() % 63))),
                    3 => dec("-0.00", 3, 2),
                    4 => {
                        let scale = (next() % 8) as u8;
                        let digits = 1 + (next() % 30) as usize;
                        let mut text = String::from(if next() % 2 == 0 { "-" } else { "" });
                        for i in 0..digits {
                            text.push(char::from(b'0' + (next() % 10) as u8));
                            if i + 1 == digits.saturating_sub(scale as usize) && scale > 0 {
                                text.push('.');
                            }
                        }
                        if scale as usize >= digits {
                            text = format!("-0.{}", "7".repeat(scale as usize));
                        }
                        dec(&text, 38, scale)
                    }
                    _ => SqlType::TinyInt(Some((next() % 256) as u8)),
                });
            }
            let nulls: Vec<bool> = nulls;
            let mut fast: Vec<Vec<SqlType>> = cells.iter().cloned().map(|c| vec![c]).collect();
            let mut text = fast.clone();
            let fast_result = unify_int_decimal_parts(&mut fast, 0, &nulls).map(|handled| assert!(handled));
            let text_result = unify_decimal_text(&mut text, 0, &nulls);
            assert_eq!(fast_result.is_ok(), text_result.is_ok(), "round {round}: {cells:?}");
            if fast_result.is_ok() {
                for (a, b) in fast.iter().zip(&text) {
                    assert_eq!(parts_of(&a[0]), parts_of(&b[0]), "round {round}: {cells:?}");
                }
            }
        }
    }

    #[test]
    fn digit_count_matches_text_length() {
        assert_eq!(digit_count(0), 0);
        assert_eq!(digit_count(9), 1);
        assert_eq!(digit_count(10), 2);
        assert_eq!(digit_count(10u128.pow(38) - 1), 38);
        assert_eq!(digit_count(10u128.pow(38)), 39);
    }

    fn decimal_text_of(t: &SqlType) -> String {
        match t {
            SqlType::Decimal(Some(p)) => decimal_text(p),
            _ => panic!("expected a non-NULL decimal cell"),
        }
    }

    #[test]
    fn small_ints_mixed_with_floats_share_a_float_column() {
        let (out, all_null) = normalized(
            vec![
                SqlType::BigInt(Some(1)),
                SqlType::Float(Some(0.5)),
                SqlType::Float(None),
            ],
            &[false, false, true],
        )
        .unwrap();
        assert!(all_null.is_empty());
        assert!(matches!(out[0], SqlType::Float(Some(v)) if v == 1.0));
        assert!(matches!(out[1], SqlType::Float(Some(v)) if v == 0.5));
        assert!(matches!(out[2], SqlType::Float(None)));
    }

    #[test]
    fn integer_beyond_2_53_with_float_becomes_exact_decimal() {
        let (out, _) = normalized(
            vec![SqlType::BigInt(Some(BIG)), SqlType::Float(Some(0.0))],
            &[false, false],
        )
        .unwrap();
        // Rust prints 0.0_f64 as "0", so the column needs no fractional digits.
        assert_eq!(decimal_text_of(&out[0]), "9007199254740993");
        assert_eq!(decimal_text_of(&out[1]), "0");
    }

    #[test]
    fn negative_wide_integer_with_float_stays_exact() {
        let (out, _) = normalized(
            vec![SqlType::Float(Some(2.5)), SqlType::BigInt(Some(-BIG))],
            &[false, false],
        )
        .unwrap();
        assert_eq!(decimal_text_of(&out[1]), "-9007199254740993.0");
    }

    #[test]
    fn decimal_mixed_with_int_takes_the_common_scale() {
        let (out, _) = normalized(
            vec![dec("1.5", 10, 1), SqlType::Int(Some(7)), SqlType::Decimal(None)],
            &[false, false, true],
        )
        .unwrap();
        assert_eq!(decimal_text_of(&out[0]), "1.5");
        assert_eq!(decimal_text_of(&out[1]), "7.0");
        assert!(matches!(out[2], SqlType::Decimal(None)));
    }

    #[test]
    fn decimal_only_column_is_rescaled_to_common_precision_and_scale() {
        let (out, _) = normalized(
            vec![dec("5.5", 2, 1), dec("-123.25", 5, 2), SqlType::Decimal(None)],
            &[false, false, true],
        )
        .unwrap();
        assert_eq!(decimal_text_of(&out[0]), "5.50");
        assert_eq!(decimal_text_of(&out[1]), "-123.25");
        assert!(matches!(&out[0], SqlType::Decimal(Some(p)) if p.precision == 5 && p.scale == 2));
        assert!(matches!(out[2], SqlType::Decimal(None)));
    }

    #[test]
    fn decimal_only_column_above_38_digits_is_rejected() {
        let huge = dec(&"9".repeat(37), 37, 0);
        let tiny = dec("0.0000000001", 10, 10);
        assert!(normalized(vec![huge, tiny], &[false, false]).is_err());
    }

    #[test]
    fn decimal_mixed_with_float_is_exact_from_shortest_float_text() {
        let (out, _) = normalized(vec![dec("0.1", 10, 1), SqlType::Float(Some(0.25))], &[false, false]).unwrap();
        assert_eq!(decimal_text_of(&out[0]), "0.10");
        assert_eq!(decimal_text_of(&out[1]), "0.25");
    }

    #[test]
    fn common_precision_above_38_digits_is_rejected() {
        let huge = dec(&"9".repeat(37), 38, 0);
        let tiny = dec("0.0000000001", 38, 10);
        let err = normalized(vec![huge, tiny], &[false, false]).unwrap_err();
        assert!(err.to_string().contains("precision"));
    }

    #[test]
    fn non_finite_float_next_to_decimal_is_rejected() {
        let result = normalized(vec![dec("1.5", 10, 1), SqlType::Float(Some(f64::NAN))], &[false, false]);
        assert!(result.is_err());
    }

    #[test]
    fn all_null_column_is_reported_and_left_untyped() {
        let (out, all_null) = normalized(vec![SqlType::Float(None), SqlType::BigInt(None)], &[true, true]).unwrap();
        assert_eq!(all_null, vec![0]);
        assert!(matches!(out[0], SqlType::Float(None)));
    }

    #[test]
    fn mixed_int_widths_widen_to_the_largest() {
        let (out, _) = normalized(
            vec![SqlType::TinyInt(Some(1)), SqlType::BigInt(Some(5))],
            &[false, false],
        )
        .unwrap();
        assert!(matches!(out[0], SqlType::BigInt(Some(1))));
        assert!(matches!(out[1], SqlType::BigInt(Some(5))));
    }

    #[test]
    fn bit_only_column_keeps_bit_type() {
        let (out, _) = normalized(
            vec![SqlType::Bit(Some(true)), SqlType::Bit(Some(false))],
            &[false, false],
        )
        .unwrap();
        assert!(matches!(out[0], SqlType::Bit(Some(true))));
    }
}
