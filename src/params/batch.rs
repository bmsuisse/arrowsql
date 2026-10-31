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
    digits.split_once('.').map_or(digits, |(int, _)| int).len().max(1)
}

/// Gives every cell of a numeric column one DECIMAL(precision, scale) type
/// that holds all of them exactly. Fails when the common precision exceeds 38.
fn unify_decimal(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<()> {
    if rows.iter().zip(is_null).all(|(row, &null)| null || decimal_parts(&row[col]).is_some()) {
        return unify_decimal_parts(rows, col, is_null);
    }
    let texts = rows
        .iter()
        .zip(is_null)
        .map(|(row, &null)| if null { Ok(None) } else { exact_decimal_text(&row[col]) })
        .collect::<TdsResult<Vec<_>>>()?;
    let scale = texts.iter().flatten().map(|t| fraction_digits(t)).max().unwrap_or(0);
    let int_digits = texts.iter().flatten().map(|t| integer_digits(t)).max().unwrap_or(1);
    let precision = int_digits + scale;
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

fn decimal_parts(t: &SqlType) -> Option<&DecimalParts> {
    match t {
        SqlType::Decimal(Some(p)) | SqlType::Numeric(Some(p)) => Some(p),
        _ => None,
    }
}

/// [`unify_decimal`] for a column of Decimal cells only: rescales the
/// magnitudes without formatting them as text.
fn unify_decimal_parts(rows: &mut [Vec<SqlType>], col: usize, is_null: &[bool]) -> TdsResult<()> {
    let (mut scale, mut int_digits) = (0u8, 1u8);
    for part in rows.iter().filter_map(|row| decimal_parts(&row[col])) {
        scale = scale.max(part.scale);
        int_digits = int_digits.max(part.precision.saturating_sub(part.scale));
    }
    let precision = usize::from(int_digits) + usize::from(scale);
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
                let magnitude = p.magnitude() * 10u128.pow(u32::from(scale - p.scale));
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
    let mut all_null = Vec::new();
    for col in 0..ncols {
        let is_null: Vec<bool> = nulls.iter().map(|row| row[col]).collect();
        if !normalize_column(rows, col, &is_null)? {
            all_null.push(col);
        }
    }
    Ok(all_null)
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
