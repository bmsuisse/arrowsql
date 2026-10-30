//! Result-set metadata in ODBC terms (SQLDescribeCol): SQL type code, column
//! size, decimal digits, nullability. Mapping follows msodbcsql, via
//! microsoft/mssql-rs `mssql-odbc/src/api/describe_col.rs` (MIT).

use mssql_tds::datatypes::sqldatatypes::TdsDataType;
use mssql_tds::query::metadata::ColumnMetadata;

use crate::consts::*;

pub fn odbc_sql_type(meta: &ColumnMetadata) -> i16 {
    match meta.data_type {
        TdsDataType::Int1 => SQL_TINYINT,
        TdsDataType::Int2 => SQL_SMALLINT,
        TdsDataType::Int4 => SQL_INTEGER,
        TdsDataType::Int8 => SQL_BIGINT,
        TdsDataType::IntN => match meta.type_info.length {
            1 => SQL_TINYINT,
            2 => SQL_SMALLINT,
            4 => SQL_INTEGER,
            8 => SQL_BIGINT,
            _ => SQL_UNKNOWN_TYPE,
        },
        TdsDataType::Bit | TdsDataType::BitN => SQL_BIT,
        TdsDataType::Flt4 => SQL_REAL,
        TdsDataType::Flt8 => SQL_DOUBLE,
        TdsDataType::FltN => match meta.type_info.length {
            4 => SQL_REAL,
            _ => SQL_DOUBLE,
        },
        TdsDataType::DecimalN => SQL_DECIMAL,
        TdsDataType::Decimal | TdsDataType::Numeric | TdsDataType::NumericN => SQL_NUMERIC,
        TdsDataType::Money | TdsDataType::Money4 | TdsDataType::MoneyN => SQL_DECIMAL,
        TdsDataType::DateN => SQL_TYPE_DATE,
        TdsDataType::TimeN => SQL_SS_TIME2,
        TdsDataType::DateTime
        | TdsDataType::DateTim4
        | TdsDataType::DateTimeN
        | TdsDataType::DateTime2N => SQL_TYPE_TIMESTAMP,
        TdsDataType::DateTimeOffsetN => SQL_SS_TIMESTAMPOFFSET,
        TdsDataType::Char | TdsDataType::BigChar => SQL_CHAR,
        TdsDataType::VarChar | TdsDataType::BigVarChar => SQL_VARCHAR,
        TdsDataType::Text => SQL_LONGVARCHAR,
        TdsDataType::NChar => SQL_WCHAR,
        TdsDataType::NVarChar => SQL_WVARCHAR,
        TdsDataType::NText => SQL_WLONGVARCHAR,
        TdsDataType::Binary | TdsDataType::BigBinary => SQL_BINARY,
        TdsDataType::VarBinary | TdsDataType::BigVarBinary => SQL_VARBINARY,
        TdsDataType::Image => SQL_LONGVARBINARY,
        TdsDataType::Guid => SQL_GUID,
        TdsDataType::Xml => SQL_SS_XML,
        TdsDataType::Json => SQL_WLONGVARCHAR,
        TdsDataType::SsVariant => SQL_SS_VARIANT,
        TdsDataType::Udt => SQL_SS_UDT,
        TdsDataType::Vector => SQL_VARCHAR,
        _ => SQL_UNKNOWN_TYPE,
    }
}

pub fn column_size(meta: &ColumnMetadata) -> u64 {
    if meta.data_type == TdsDataType::Udt && meta.type_info.length != usize::from(u16::MAX) {
        return meta.type_info.length as u64;
    }
    if meta.is_plp() {
        return 0;
    }
    match meta.data_type {
        TdsDataType::Int1 => 3,
        TdsDataType::Int2 => 5,
        TdsDataType::Int4 => 10,
        TdsDataType::Int8 => 19,
        TdsDataType::IntN => match meta.type_info.length {
            1 => 3,
            2 => 5,
            4 => 10,
            8 => 19,
            _ => 0,
        },
        TdsDataType::Bit | TdsDataType::BitN => 1,
        TdsDataType::Flt4 => 7,
        TdsDataType::Flt8 => 15,
        TdsDataType::FltN => match meta.type_info.length {
            4 => 7,
            _ => 15,
        },
        TdsDataType::DateN => 10,
        TdsDataType::TimeN => {
            let scale = meta.get_scale().unwrap_or(0) as u64;
            if scale > 0 { 9 + scale } else { 8 }
        }
        TdsDataType::DateTime => 23,
        TdsDataType::DateTim4 => 16,
        TdsDataType::DateTimeN => match meta.type_info.length {
            4 => 16,
            _ => 23,
        },
        TdsDataType::DateTime2N => {
            let scale = meta.get_scale().unwrap_or(0) as u64;
            if scale > 0 { 20 + scale } else { 19 }
        }
        TdsDataType::DateTimeOffsetN => {
            let scale = meta.get_scale().unwrap_or(0) as u64;
            if scale > 0 { 27 + scale } else { 26 }
        }
        TdsDataType::Decimal
        | TdsDataType::DecimalN
        | TdsDataType::Numeric
        | TdsDataType::NumericN
        | TdsDataType::Money
        | TdsDataType::Money4
        | TdsDataType::MoneyN => meta.get_precision().unwrap_or(0) as u64,
        TdsDataType::NChar | TdsDataType::NVarChar | TdsDataType::NText => {
            (meta.type_info.length / 2) as u64
        }
        TdsDataType::Guid => 36,
        _ => meta.type_info.length as u64,
    }
}

pub fn decimal_digits(meta: &ColumnMetadata) -> i16 {
    match meta.data_type {
        TdsDataType::Money | TdsDataType::Money4 | TdsDataType::MoneyN => 4,
        TdsDataType::DateTime => 3,
        TdsDataType::DateTim4 => 0,
        TdsDataType::DateTimeN => match meta.type_info.length {
            4 => 0,
            _ => 3,
        },
        _ => meta.get_scale().unwrap_or(0) as i16,
    }
}

pub fn nullable(meta: &ColumnMetadata) -> i16 {
    if meta.is_nullable() { SQL_NULLABLE } else { SQL_NO_NULLS }
}
