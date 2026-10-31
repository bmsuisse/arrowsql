//! SQL Server type names for the header line, derived from the result-set
//! metadata (`name(length)`, `decimal(p,s)`, `datetime2(n)`, ...).

use mssql_tds::datatypes::sqldatatypes::TdsDataType;
use mssql_tds::query::metadata::ColumnMetadata;

/// The declared type of a result column, e.g. `nvarchar(50)` or `decimal(18,2)`.
pub fn sql_type_name(meta: &ColumnMetadata) -> String {
    let length = meta.type_info.length;
    let max = meta.is_plp();
    let sized = |name: &str, chars: usize| {
        if max { format!("{name}(max)") } else { format!("{name}({chars})") }
    };
    let scale = meta.get_scale().unwrap_or(0);
    match meta.data_type {
        TdsDataType::Int1 => "tinyint".into(),
        TdsDataType::Int2 => "smallint".into(),
        TdsDataType::Int4 => "int".into(),
        TdsDataType::Int8 => "bigint".into(),
        TdsDataType::IntN => match length {
            1 => "tinyint".into(),
            2 => "smallint".into(),
            4 => "int".into(),
            _ => "bigint".into(),
        },
        TdsDataType::Bit | TdsDataType::BitN => "bit".into(),
        TdsDataType::Flt4 => "real".into(),
        TdsDataType::Flt8 => "float".into(),
        TdsDataType::FltN => if length == 4 { "real" } else { "float" }.into(),
        TdsDataType::Decimal | TdsDataType::DecimalN => {
            format!("decimal({},{})", meta.get_precision().unwrap_or(38), scale)
        }
        TdsDataType::Numeric | TdsDataType::NumericN => {
            format!("numeric({},{})", meta.get_precision().unwrap_or(38), scale)
        }
        TdsDataType::Money => "money".into(),
        TdsDataType::Money4 => "smallmoney".into(),
        TdsDataType::MoneyN => if length == 4 { "smallmoney" } else { "money" }.into(),
        TdsDataType::DateN => "date".into(),
        TdsDataType::TimeN => format!("time({scale})"),
        TdsDataType::DateTime => "datetime".into(),
        TdsDataType::DateTim4 => "smalldatetime".into(),
        TdsDataType::DateTimeN => if length == 4 { "smalldatetime" } else { "datetime" }.into(),
        TdsDataType::DateTime2N => format!("datetime2({scale})"),
        TdsDataType::DateTimeOffsetN => format!("datetimeoffset({scale})"),
        TdsDataType::Char | TdsDataType::BigChar => format!("char({length})"),
        TdsDataType::VarChar | TdsDataType::BigVarChar => sized("varchar", length),
        TdsDataType::Text => "text".into(),
        TdsDataType::NChar => format!("nchar({})", length / 2),
        TdsDataType::NVarChar => sized("nvarchar", length / 2),
        TdsDataType::NText => "ntext".into(),
        TdsDataType::Binary | TdsDataType::BigBinary => format!("binary({length})"),
        TdsDataType::VarBinary | TdsDataType::BigVarBinary => sized("varbinary", length),
        TdsDataType::Image => "image".into(),
        TdsDataType::Guid => "uniqueidentifier".into(),
        TdsDataType::Xml => "xml".into(),
        TdsDataType::Json => "json".into(),
        TdsDataType::SsVariant => "sql_variant".into(),
        TdsDataType::Udt => "udt".into(),
        TdsDataType::Vector => "vector".into(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

/// Fractional-second digits a temporal column shows.
pub fn fraction_digits(meta: &ColumnMetadata) -> u32 {
    match meta.data_type {
        TdsDataType::DateTime => 3,
        TdsDataType::DateTimeN if meta.type_info.length == 8 => 3,
        TdsDataType::DateTimeN | TdsDataType::DateTim4 => 0,
        // time keeps 100 ns ticks; datetime2 / datetimeoffset are decoded to microseconds.
        TdsDataType::TimeN => u32::from(meta.get_scale().unwrap_or(0)).min(7),
        _ => u32::from(meta.get_scale().unwrap_or(0)).min(6),
    }
}
