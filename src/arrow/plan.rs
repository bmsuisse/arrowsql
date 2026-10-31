//! SQL column -> Arrow type, identical to upstream's C++ `FetchArrowBatch_wrap`
//! (it switches on the ODBC SQL type from SQLDescribeCol; so do we, via
//! `decode::describe`, so both fetch paths agree on what a column is).

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mssql_tds::query::metadata::ColumnMetadata;

use crate::consts::*;
use crate::decode::describe;

/// Arrow storage of one result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    UInt8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Decimal { precision: u8, scale: i8 },
    /// Microseconds since the Unix epoch; `utc` for datetimeoffset.
    Timestamp { utc: bool },
    Date32,
    Time64Ns,
    /// UTF-8 text (`large_string`): char/varchar/nchar/nvarchar/xml/json/GUID.
    Text,
    /// `large_binary`: binary/varbinary/image/UDT.
    Binary,
}

impl Kind {
    pub fn data_type(self) -> DataType {
        match self {
            Kind::UInt8 => DataType::UInt8,
            Kind::Int16 => DataType::Int16,
            Kind::Int32 => DataType::Int32,
            Kind::Int64 => DataType::Int64,
            Kind::Float32 => DataType::Float32,
            Kind::Float64 => DataType::Float64,
            Kind::Bool => DataType::Boolean,
            Kind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
            Kind::Timestamp { utc } => {
                DataType::Timestamp(TimeUnit::Microsecond, utc.then(|| "+00:00".into()))
            }
            Kind::Date32 => DataType::Date32,
            Kind::Time64Ns => DataType::Time64(TimeUnit::Nanosecond),
            Kind::Text => DataType::LargeUtf8,
            Kind::Binary => DataType::LargeBinary,
        }
    }
}

/// The Arrow layout of a result set.
#[derive(Debug, Clone)]
pub struct Plan {
    pub kinds: Vec<Kind>,
    /// Typical value width in bytes, for pre-sizing variable-length buffers.
    pub widths: Vec<usize>,
    pub schema: SchemaRef,
}

impl Plan {
    /// Errors (upstream's wording) on SQL types upstream can't fetch as Arrow,
    /// e.g. sql_variant.
    pub fn new(columns: &[ColumnMetadata]) -> Result<Plan, String> {
        Self::build(columns, false)
    }

    /// Like `new`, but `sql_variant` columns become text (the agent renderer
    /// shows them in their value's own text form).
    pub fn for_display(columns: &[ColumnMetadata]) -> Result<Plan, String> {
        Self::build(columns, true)
    }

    fn build(columns: &[ColumnMetadata], variant_as_text: bool) -> Result<Plan, String> {
        let mut kinds = Vec::with_capacity(columns.len());
        let mut widths = Vec::with_capacity(columns.len());
        let mut fields = Vec::with_capacity(columns.len());
        for (i, meta) in columns.iter().enumerate() {
            let sql_type = describe::odbc_sql_type(meta);
            let kind = kind_of(sql_type, meta)
                .or((variant_as_text && sql_type == SQL_SS_VARIANT).then_some(Kind::Text))
                .ok_or_else(|| {
                format!(
                    "Unsupported data type for Arrow batch fetch for column - {}, Type - {}, column ID - {}",
                    meta.column_name,
                    sql_type,
                    i + 1
                )
            })?;
            let size = describe::column_size(meta) as usize;
            widths.push(match kind {
                // Estimate, not a limit: buffers grow on demand.
                Kind::Text | Kind::Binary if size > 0 && size <= 64 => size,
                Kind::Text | Kind::Binary => 32,
                _ => 0,
            });
            let nullable = describe::nullable(meta) != SQL_NO_NULLS;
            fields.push(Field::new(meta.column_name.clone(), kind.data_type(), nullable));
            kinds.push(kind);
        }
        Ok(Plan { kinds, widths, schema: Arc::new(Schema::new(fields)) })
    }
}

fn kind_of(sql_type: i16, meta: &ColumnMetadata) -> Option<Kind> {
    Some(match sql_type {
        SQL_CHAR | SQL_VARCHAR | SQL_LONGVARCHAR | SQL_SS_XML | SQL_WCHAR | SQL_WVARCHAR
        | SQL_WLONGVARCHAR | SQL_GUID => Kind::Text,
        SQL_SS_UDT | SQL_BINARY | SQL_VARBINARY | SQL_LONGVARBINARY => Kind::Binary,
        SQL_TINYINT => Kind::UInt8,
        SQL_SMALLINT => Kind::Int16,
        SQL_INTEGER => Kind::Int32,
        SQL_BIGINT => Kind::Int64,
        SQL_REAL => Kind::Float32,
        SQL_DOUBLE => Kind::Float64,
        SQL_DECIMAL | SQL_NUMERIC => Kind::Decimal {
            precision: describe::column_size(meta).clamp(1, 38) as u8,
            scale: describe::decimal_digits(meta) as i8,
        },
        SQL_TYPE_TIMESTAMP => Kind::Timestamp { utc: false },
        SQL_SS_TIMESTAMPOFFSET => Kind::Timestamp { utc: true },
        SQL_TYPE_DATE => Kind::Date32,
        SQL_SS_TIME2 => Kind::Time64Ns,
        SQL_BIT => Kind::Bool,
        _ => return None,
    })
}
