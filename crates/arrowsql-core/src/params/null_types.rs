//! Typing of untyped NULL parameters (ODBC's SQLDescribeParam).
//!
//! `sp_describe_undeclared_parameters` reports the type SQL Server infers
//! for each `@Pn`; results are cached per SQL text on the connection.

use mssql_tds::datatypes::sqltypes::SqlType;

/// NULL of the SQL type named by `sp_describe_undeclared_parameters`
/// (`suggested_system_type_name`, e.g. `varbinary(50)`, `decimal(10,2)`).
pub fn null_of(type_name: &str) -> Option<SqlType> {
    let lower = type_name.trim().to_ascii_lowercase();
    let (base, args) = match lower.find('(') {
        Some(i) => (&lower[..i], lower[i + 1..].trim_end_matches(')')),
        None => (lower.as_str(), ""),
    };
    let len = || -> Option<u16> {
        if args == "max" { None } else { args.split(',').next()?.trim().parse().ok() }
    };
    Some(match base {
        "bit" => SqlType::Bit(None),
        "tinyint" => SqlType::TinyInt(None),
        "smallint" => SqlType::SmallInt(None),
        "int" => SqlType::Int(None),
        "bigint" => SqlType::BigInt(None),
        "real" => SqlType::Real(None),
        "float" => SqlType::Float(None),
        "decimal" => SqlType::Decimal(None),
        "numeric" => SqlType::Numeric(None),
        "money" => SqlType::Money(None),
        "smallmoney" => SqlType::SmallMoney(None),
        "date" => SqlType::Date(None),
        "time" => SqlType::Time(None),
        "datetime" => SqlType::DateTime(None),
        "smalldatetime" => SqlType::SmallDateTime(None),
        "datetime2" => SqlType::DateTime2(None),
        "datetimeoffset" => SqlType::DateTimeOffset(None),
        "uniqueidentifier" => SqlType::Uuid(None),
        "xml" => SqlType::Xml(None),
        "varbinary" => match len() {
            Some(n) => SqlType::VarBinary(None, n.max(1)),
            None => SqlType::VarBinaryMax(None),
        },
        "binary" => SqlType::Binary(None, len().unwrap_or(1).max(1)),
        "image" => SqlType::VarBinaryMax(None),
        "varchar" | "char" | "text" => match len() {
            Some(n) => SqlType::Varchar(None, n.max(1)),
            None => SqlType::VarcharMax(None),
        },
        "nvarchar" | "nchar" | "ntext" | "sysname" => match len() {
            Some(n) => SqlType::NVarchar(None, n.max(1)),
            None => SqlType::NVarcharMax(None),
        },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::null_of;
    use mssql_tds::datatypes::sqltypes::SqlType;

    #[test]
    fn parses_sizes() {
        assert!(matches!(null_of("varbinary(50)"), Some(SqlType::VarBinary(None, 50))));
        assert!(matches!(null_of("varbinary(max)"), Some(SqlType::VarBinaryMax(None))));
        assert!(matches!(null_of("decimal(10,2)"), Some(SqlType::Decimal(None))));
        assert!(null_of("geography").is_none());
    }
}
