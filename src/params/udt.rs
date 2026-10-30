//! SQL_SS_UDT (CLR user-defined type) parameters: geography, geometry and
//! hierarchyid bound from their serialized bytes.
//!
//! `setinputsizes` only says "UDT"; it does not name the type. The name comes
//! from `sp_describe_undeclared_parameters`, the same metadata that types
//! untyped NULLs, read when the statement executes. Until then a UDT value is
//! a `SqlType::Udt` with an empty type name, a placeholder that
//! [`resolve_udt_names`] fills in before anything reaches the wire.

use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::sql_udt::UdtTypeName;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::error::Error;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes};

use super::ParamSet;

/// `suggested_system_type_id` that SQL Server reports for CLR UDTs.
const CLR_UDT_SYSTEM_TYPE_ID: i64 = 240;

/// The parts of one `sp_describe_undeclared_parameters` row that the driver uses.
#[derive(Clone, Debug)]
pub struct DescribedParam {
    /// Parameter marker as the server names it, e.g. `@P1`.
    pub name: String,
    /// `suggested_system_type_name`, e.g. `varbinary(50)`.
    pub system_type: String,
    /// Server-side identity of the CLR UDT behind this parameter, if any.
    pub udt: Option<UdtTypeName>,
}

/// Identity of a CLR UDT from its describe row, or `None` for any other type.
///
/// Alias types (`CREATE TYPE ... FROM int`) also carry a user type name, so
/// the system type id is what marks a CLR UDT.
pub fn udt_name(
    system_type_id: Option<i64>,
    database: Option<String>,
    schema: Option<String>,
    type_name: Option<String>,
) -> Option<UdtTypeName> {
    if system_type_id != Some(CLR_UDT_SYSTEM_TYPE_ID) {
        return None;
    }
    let type_name = type_name.filter(|name| !name.is_empty())?;
    Some(UdtTypeName::new(database, schema, type_name))
}

/// Whether any parameter still waits for its UDT name.
pub(crate) fn has_unresolved_udt(params: &ParamSet) -> bool {
    params.types.iter().any(is_placeholder)
}

fn is_placeholder(sql_type: &SqlType) -> bool {
    matches!(sql_type, SqlType::Udt(name, _) if name.type_name.is_empty())
}

/// Fills the name of every UDT placeholder from the statement's parameter
/// metadata. A marker the server does not describe as a UDT is an error, not
/// a silent fallback to varbinary.
pub(crate) fn resolve_udt_names(
    params: &mut ParamSet,
    described: &[DescribedParam],
) -> Result<(), String> {
    for (i, sql_type) in params.types.iter_mut().enumerate() {
        let SqlType::Udt(name, _) = sql_type else {
            continue;
        };
        if !name.type_name.is_empty() {
            continue;
        }
        let marker = format!("@P{}", i + 1);
        let resolved = described
            .iter()
            .find(|param| param.name.eq_ignore_ascii_case(&marker))
            .and_then(|param| param.udt.clone());
        match resolved {
            Some(udt) => *name = udt,
            None => {
                return Err(format!(
                    "SQL_SS_UDT parameter {marker} is not a CLR UDT in the statement's parameter metadata"
                ));
            }
        }
    }
    Ok(())
}

/// A non-NULL value bound through a SQL_SS_UDT hint. The server stores the
/// serialized form as-is, so the payload is sent unchanged.
pub(crate) fn hinted_udt(py_obj: &Bound<'_, PyAny>) -> TdsResult<SqlType> {
    let payload = if let Ok(bytes) = py_obj.cast::<PyBytes>() {
        bytes.as_bytes().to_vec()
    } else if let Ok(bytes) = py_obj.cast::<PyByteArray>() {
        bytes.to_vec()
    } else {
        return Err(Error::UsageError(
            "SQL_SS_UDT parameters require bytes or bytearray values".to_string(),
        ));
    };
    Ok(SqlType::Udt(UdtTypeName::default(), Some(payload)))
}

/// A NULL bound through a SQL_SS_UDT hint; its type name is resolved later.
pub(crate) fn null_udt() -> SqlType {
    SqlType::Udt(UdtTypeName::default(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn described(name: &str, udt: Option<UdtTypeName>) -> DescribedParam {
        DescribedParam {
            name: name.to_string(),
            system_type: "geography".to_string(),
            udt,
        }
    }

    fn placeholder(value: Option<Vec<u8>>) -> SqlType {
        SqlType::Udt(UdtTypeName::default(), value)
    }

    #[test]
    fn clr_udt_identity_needs_the_clr_system_type() {
        let sys = |id, name: &str| {
            udt_name(
                id,
                Some("db".to_string()),
                Some("sys".to_string()),
                Some(name.to_string()),
            )
        };
        assert_eq!(
            sys(Some(240), "hierarchyid"),
            Some(UdtTypeName::new(
                Some("db".to_string()),
                Some("sys".to_string()),
                "hierarchyid".to_string(),
            ))
        );
        assert!(sys(Some(165), "myalias").is_none());
        assert!(sys(None, "hierarchyid").is_none());
        assert!(udt_name(Some(240), None, None, Some(String::new())).is_none());
    }

    #[test]
    fn resolves_each_placeholder_from_its_marker() {
        let mut params = ParamSet {
            types: vec![
                SqlType::Int(Some(1)),
                placeholder(Some(vec![1, 2])),
                placeholder(None),
            ],
            untyped_nulls: vec![],
        };
        let geo = UdtTypeName::new(None, Some("sys".to_string()), "geography".to_string());
        let hid = UdtTypeName::new(None, Some("sys".to_string()), "hierarchyid".to_string());
        let metadata = [
            described("@p2", Some(hid.clone())),
            described("@P3", Some(geo.clone())),
        ];

        assert!(has_unresolved_udt(&params));
        resolve_udt_names(&mut params, &metadata).unwrap();

        assert!(!has_unresolved_udt(&params));
        assert!(
            matches!(&params.types[1], SqlType::Udt(name, Some(v)) if *name == hid && v == &[1, 2])
        );
        assert!(matches!(&params.types[2], SqlType::Udt(name, None) if *name == geo));
    }

    #[test]
    fn rejects_a_marker_that_is_not_a_udt() {
        let mut params = ParamSet {
            types: vec![placeholder(Some(vec![0]))],
            untyped_nulls: vec![],
        };
        let err = resolve_udt_names(&mut params, &[described("@P1", None)]).unwrap_err();
        assert!(err.contains("@P1"), "{err}");
        assert!(has_unresolved_udt(&params));
    }

    #[test]
    fn rejects_a_missing_marker() {
        let mut params = ParamSet {
            types: vec![placeholder(None)],
            untyped_nulls: vec![],
        };
        assert!(resolve_udt_names(&mut params, &[]).is_err());
    }
}
