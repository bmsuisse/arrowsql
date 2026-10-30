//! ODBC catalog functions (SQLTables, SQLColumns, ...).
//!
//! Implemented the way msodbcsql does it: each call runs the matching
//! SQL Server catalog stored procedure on the statement, then renames the
//! ODBC 2.x result columns to their ODBC 3.x names.
//!
//! TODO(catalog): verify every result shape against msodbcsql (column order,
//! types, the `SQL_ALL_*` enumeration special cases of SQLTables, and
//! SQLGetTypeInfo's ODBC 3 type codes for date/time types).

use std::sync::Arc;

use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use pyo3::prelude::*;

use crate::consts::*;
use crate::statement::{SqlHandle, exec_catalog_rpc};

const ODBC3_RENAMES: &[(&str, &str)] = &[
    ("TABLE_QUALIFIER", "TABLE_CAT"),
    ("TABLE_OWNER", "TABLE_SCHEM"),
    ("PROCEDURE_QUALIFIER", "PROCEDURE_CAT"),
    ("PROCEDURE_OWNER", "PROCEDURE_SCHEM"),
    ("PKTABLE_QUALIFIER", "PKTABLE_CAT"),
    ("PKTABLE_OWNER", "PKTABLE_SCHEM"),
    ("FKTABLE_QUALIFIER", "FKTABLE_CAT"),
    ("FKTABLE_OWNER", "FKTABLE_SCHEM"),
    ("PRECISION", "COLUMN_SIZE"),
    ("LENGTH", "BUFFER_LENGTH"),
    ("SCALE", "DECIMAL_DIGITS"),
    ("RADIX", "NUM_PREC_RADIX"),
    ("SEQ_IN_INDEX", "ORDINAL_POSITION"),
    ("COLLATION", "ASC_OR_DESC"),
    ("MONEY", "FIXED_PREC_SCALE"),
    ("AUTO_INCREMENT", "AUTO_UNIQUE_VALUE"),
];

fn text_param(value: Option<String>) -> SqlType {
    let sql_type = match value {
        Some(s) if !s.is_empty() => {
            let len = (s.encode_utf16().count().max(1)).min(4000) as u16;
            SqlType::NVarchar(Some(SqlString::from_utf8_string(s)), len)
        }
        _ => SqlType::NVarchar(None, 128),
    };
    sql_type
}

fn opt_str(obj: &Bound<'_, PyAny>) -> Option<String> {
    if obj.is_none() { None } else { obj.extract::<String>().ok() }
}

fn named(params: Vec<SqlType>) -> Vec<RpcParameter> {
    params
        .into_iter()
        .enumerate()
        .map(|(i, t)| RpcParameter::new(Some(format!("@P{}", i + 1)), StatusFlags::NONE, t))
        .collect()
}

/// Run catalog procedure `proc` with named arguments `args` (value `None`
/// = NULL) plus `literal` extra arguments. A catalog (`*_qualifier`
/// argument) other than the current database runs `[db].sys.proc` there;
/// an unknown database yields an empty result of the same shape, as
/// msodbcsql does.
fn run(
    py: Python<'_>,
    handle: &Bound<'_, PyAny>,
    proc: &str,
    args: Vec<(&str, Option<String>)>,
    literal: &str,
) -> PyResult<i16> {
    let assign = |empty: bool| -> String {
        let mut parts: Vec<String> = args
            .iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let value = if !empty {
                    format!("@P{}", i + 1)
                } else if name.ends_with("_qualifier") {
                    "NULL".to_string()
                } else if name.ends_with("_name") {
                    "N''".to_string()
                } else {
                    format!("@P{}", i + 1)
                };
                format!("{name}={value}")
            })
            .collect();
        if !literal.is_empty() {
            parts.push(literal.to_string());
        }
        parts.join(", ")
    };
    let catalog = args
        .iter()
        .enumerate()
        .find(|(_, (name, value))| name.ends_with("_qualifier") && value.as_deref().is_some_and(|v| !v.is_empty()))
        .map(|(i, (_, value))| (i + 1, value.clone().unwrap_or_default()));
    let sql = match &catalog {
        Some((index, db)) => format!(
            "IF DB_ID(@P{index}) IS NULL EXEC sys.{proc} {empty} ELSE EXEC [{db}].sys.{proc} {full}",
            empty = assign(true),
            db = db.replace(']', "]]"),
            full = assign(false),
        ),
        None => format!("EXEC sys.{proc} {}", assign(false)),
    };
    let params = args.into_iter().map(|(_, v)| text_param(v)).collect();
    let ret = exec_catalog_rpc(py, handle, sql, named(params))?;
    if ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO {
        rename_columns(handle)?;
    }
    Ok(ret)
}

fn rename_columns(handle: &Bound<'_, PyAny>) -> PyResult<()> {
    let stmt = handle.cast::<SqlHandle>()?.get().inner.clone();
    crate::statement::map_columns(&stmt, |cols| {
        let mut cols = (**cols).clone();
        for col in cols.iter_mut() {
            if let Some((_, to)) = ODBC3_RENAMES.iter().find(|(from, _)| *from == col.column_name) {
                col.column_name = (*to).to_string();
            }
        }
        Arc::new(cols)
    });
    Ok(())
}

#[pyfunction]
#[pyo3(name = "DDBCSQLTables", signature = (StatementHandle, catalog=String::new(), schema=String::new(), table=String::new(), tableType=String::new()))]
#[allow(non_snake_case)]
pub fn tables(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    catalog: String,
    schema: String,
    table: String,
    tableType: String,
) -> PyResult<i16> {
    let quoted_types = if tableType.is_empty() {
        None
    } else {
        Some(
            tableType
                .split(',')
                .map(|t| {
                    let t = t.trim().trim_matches('\'');
                    format!("'{}'", t.replace('\'', "''"))
                })
                .collect::<Vec<_>>()
                .join(","),
        )
    };
    run(
        py,
        StatementHandle,
        "sp_tables",
        vec![
            ("@table_name", Some(table)),
            ("@table_owner", Some(schema)),
            ("@table_qualifier", Some(catalog)),
            ("@table_type", quoted_types),
        ],
        "@fUsePattern=1",
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLColumns")]
#[allow(non_snake_case)]
pub fn columns(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    catalog: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyAny>,
    table: &Bound<'_, PyAny>,
    column: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    run(
        py,
        StatementHandle,
        "sp_columns_100",
        vec![
            ("@table_name", opt_str(table)),
            ("@table_owner", opt_str(schema)),
            ("@table_qualifier", opt_str(catalog)),
            ("@column_name", opt_str(column)),
        ],
        "@ODBCVer=3, @fUsePattern=1",
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLProcedures")]
#[allow(non_snake_case)]
pub fn procedures(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    catalog: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyAny>,
    procedure: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    run(
        py,
        StatementHandle,
        "sp_stored_procedures",
        vec![("@sp_name", opt_str(procedure)), ("@sp_owner", opt_str(schema)), ("@sp_qualifier", opt_str(catalog))],
        "@fUsePattern=1",
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLPrimaryKeys")]
#[allow(non_snake_case)]
pub fn primary_keys(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    catalog: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyAny>,
    table: String,
) -> PyResult<i16> {
    run(
        py,
        StatementHandle,
        "sp_pkeys",
        vec![("@table_name", Some(table)), ("@table_owner", opt_str(schema)), ("@table_qualifier", opt_str(catalog))],
        "",
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLForeignKeys")]
#[allow(non_snake_case)]
pub fn foreign_keys(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    pkCatalog: &Bound<'_, PyAny>,
    pkSchema: &Bound<'_, PyAny>,
    pkTable: &Bound<'_, PyAny>,
    fkCatalog: &Bound<'_, PyAny>,
    fkSchema: &Bound<'_, PyAny>,
    fkTable: &Bound<'_, PyAny>,
) -> PyResult<i16> {
    run(
        py,
        StatementHandle,
        "sp_fkeys",
        vec![
            ("@pktable_name", opt_str(pkTable)),
            ("@pktable_owner", opt_str(pkSchema)),
            ("@pktable_qualifier", opt_str(pkCatalog)),
            ("@fktable_name", opt_str(fkTable)),
            ("@fktable_owner", opt_str(fkSchema)),
            ("@fktable_qualifier", opt_str(fkCatalog)),
        ],
        "",
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLSpecialColumns")]
#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub fn special_columns(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    identifierType: i16,
    catalog: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyAny>,
    table: String,
    scope: i16,
    nullable: i16,
) -> PyResult<i16> {
    // SQL_BEST_ROWID = 1 -> 'R', SQL_ROWVER = 2 -> 'V'.
    let col_type = if identifierType == 2 { "V" } else { "R" };
    let scope = if scope == 0 { "C" } else { "T" };
    let nullable = if nullable == 0 { "O" } else { "U" };
    run(
        py,
        StatementHandle,
        "sp_special_columns_100",
        vec![("@table_name", Some(table)), ("@table_owner", opt_str(schema)), ("@table_qualifier", opt_str(catalog))],
        &format!("@col_type='{col_type}', @scope='{scope}', @nullable='{nullable}', @ODBCVer=3"),
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLStatistics")]
#[allow(non_snake_case)]
pub fn statistics(
    py: Python<'_>,
    StatementHandle: &Bound<'_, PyAny>,
    catalog: &Bound<'_, PyAny>,
    schema: &Bound<'_, PyAny>,
    table: String,
    unique: u16,
    reserved: u16,
) -> PyResult<i16> {
    // SQL_INDEX_UNIQUE = 0 -> 'Y'; SQL_QUICK = 0 -> 'Q', SQL_ENSURE = 1 -> 'E'.
    let is_unique = if unique == 0 { "Y" } else { "N" };
    let accuracy = if reserved == 1 { "E" } else { "Q" };
    run(
        py,
        StatementHandle,
        "sp_statistics_100",
        vec![("@table_name", Some(table)), ("@table_owner", opt_str(schema)), ("@table_qualifier", opt_str(catalog))],
        &format!("@is_unique='{is_unique}', @accuracy='{accuracy}'"),
    )
}

#[pyfunction]
#[pyo3(name = "DDBCSQLGetTypeInfo")]
#[allow(non_snake_case)]
pub fn get_type_info(py: Python<'_>, StatementHandle: &Bound<'_, PyAny>, DataType: i16) -> PyResult<i16> {
    let ret = crate::statement::exec_catalog_query(
        py,
        StatementHandle,
        format!("EXEC sp_datatype_info_100 @data_type={DataType}, @ODBCVer=3"),
    )?;
    if ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO {
        rename_columns(StatementHandle)?;
    }
    Ok(ret)
}
