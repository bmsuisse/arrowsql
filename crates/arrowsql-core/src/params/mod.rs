//! Typed parameters for `sp_executesql` and `sp_execute` (executemany).
//!
//! - `placeholders`: `?` -> `@Pn` rewriting.
//! - `batch`: executemany rows, column by column.
//! - `null_types`: the type of an untyped NULL, from the server's describe.
//! - `udt`: CLR UDT (geography, geometry, hierarchyid) names.
//!
//! The Python bindings convert Python values into the `SqlType`s held here.

pub mod batch;
pub mod null_types;
pub mod placeholders;
pub mod udt;

use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};

/// Converted parameters, before naming. `untyped_nulls` lists positions of
/// `None` values with no type hint: their SQL type is resolved against the
/// server (`resolve_null_types`) because no single NULL type converts
/// implicitly to every column type (e.g. nvarchar -> varbinary fails).
pub struct ParamSet {
    pub types: Vec<SqlType>,
    pub untyped_nulls: Vec<usize>,
}

/// `@Pn` without going through the `format!` machinery: it runs once per
/// parameter per row in `executemany`.
pub fn param_name(n: usize) -> String {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    let mut rest = n;
    loop {
        at -= 1;
        digits[at] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let mut name = String::with_capacity(2 + digits.len() - at);
    name.push_str("@P");
    for &d in &digits[at..] {
        name.push(d as char);
    }
    name
}

impl ParamSet {
    pub fn into_rpc(self) -> Vec<RpcParameter> {
        self.types
            .into_iter()
            .enumerate()
            .map(|(i, t)| RpcParameter::new(Some(param_name(i + 1)), StatusFlags::NONE, t))
            .collect()
    }

    /// The parameters without names, for `execute_sp_executesql_positional`,
    /// which names parameter `n` `@Pn` itself.
    pub fn into_unnamed_rpc(self) -> Vec<RpcParameter> {
        self.types
            .into_iter()
            .map(|t| RpcParameter::new(None, StatusFlags::NONE, t))
            .collect()
    }
}

/// WKT prefixes that keep an ASCII string NVARCHAR, as upstream binds geometry text.
const GEOMETRY_WKT_PREFIXES: [&str; 3] = ["POINT", "LINESTRING", "POLYGON"];

/// The SQL type a string binds as (as for a Python `str` parameter): ASCII
/// text as varchar, anything else (and geometry WKT) as nvarchar, MAX past
/// 4000 units.
pub fn str_sql_type(value: String) -> SqlType {
    // Geometry WKT binds as NVARCHAR even when it is ASCII, as upstream does.
    let is_geometry = GEOMETRY_WKT_PREFIXES.iter().any(|prefix| value.starts_with(prefix));
    let narrow = value.is_ascii() && !is_geometry;
    let length = if narrow { value.len() } else { value.encode_utf16().count() }.max(1);
    let value = SqlString::from_utf8_string(value);
    // Inferred strings use the 4000-unit boundary for both types, so
    // anything longer is declared MAX rather than an 8000-byte varchar.
    if narrow {
        match u16::try_from(length) {
            Ok(length) if length <= 4_000 => SqlType::Varchar(Some(value), length),
            _ => SqlType::VarcharMax(Some(value)),
        }
    } else {
        match u16::try_from(length) {
            Ok(length) if length <= 4_000 => SqlType::NVarchar(Some(value), length),
            _ => SqlType::NVarcharMax(Some(value)),
        }
    }
}

/// The SQL type an integer binds as: the narrowest of tinyint, smallint, int
/// and bigint that holds it (as for a Python `int` parameter).
pub fn int_sql_type(value: i64) -> SqlType {
    match value {
        0..=255 => SqlType::TinyInt(Some(value as u8)),
        -32_768..=32_767 => SqlType::SmallInt(Some(value as i16)),
        -2_147_483_648..=2_147_483_647 => SqlType::Int(Some(value as i32)),
        _ => SqlType::BigInt(Some(value)),
    }
}

/// Prefix of a conversion message for a value SQL Server would reject with a
/// data exception (SQLSTATE 22xxx). Raised as PEP 249 `DataError`.
pub const DATA_ERROR_PREFIX: &str = "data exception: ";

/// Prefix of a conversion message that is both a `DataError` and, for backward
/// compatibility with earlier releases, a `ValueError`.
pub const DATA_VALUE_ERROR_PREFIX: &str = "data value exception: ";

/// Prefix of a conversion message that upstream raises as a bare `RuntimeError`
/// (a tz offset outside SQL Server's range).
pub const RUNTIME_ERROR_PREFIX: &str = "runtime error: ";

/// Prefix of a conversion message raised as `IndexError`: an `executemany` row
/// narrower or wider than the first (upstream indexes `row[col]` and fails there).
pub const INDEX_ERROR_PREFIX: &str = "index error: ";

/// Marks a `UsageError` for a str that cannot be encoded (a lone surrogate); the
/// rest of the message is the encoder's reason. Raised as `UnicodeEncodeError`.
pub const UNICODE_ENCODE_PREFIX: &str = "unicode encode error: ";
