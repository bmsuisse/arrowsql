//! ODBC constants the Python layer exchanges with the bindings.
//! Values match sql.h / sqlext.h / msodbcsql.h so the vendored Python code
//! (constants.py) keeps working unchanged.

pub type SqlReturn = i16;

pub const SQL_SUCCESS: SqlReturn = 0;
pub const SQL_SUCCESS_WITH_INFO: SqlReturn = 1;
pub const SQL_NO_DATA: SqlReturn = 100;
pub const SQL_ERROR: SqlReturn = -1;
pub const SQL_INVALID_HANDLE: SqlReturn = -2;

pub const SQL_HANDLE_DBC: i16 = 2;
pub const SQL_HANDLE_STMT: i16 = 3;

pub const SQL_NO_NULLS: i16 = 0;
pub const SQL_NULLABLE: i16 = 1;

pub const SQL_NO_TOTAL: i32 = -4;

// SQL data types (SQLDescribeCol DataType).
pub const SQL_UNKNOWN_TYPE: i16 = 0;
pub const SQL_CHAR: i16 = 1;
pub const SQL_NUMERIC: i16 = 2;
pub const SQL_DECIMAL: i16 = 3;
pub const SQL_INTEGER: i16 = 4;
pub const SQL_SMALLINT: i16 = 5;
pub const SQL_REAL: i16 = 7;
pub const SQL_DOUBLE: i16 = 8;
pub const SQL_VARCHAR: i16 = 12;
pub const SQL_TYPE_DATE: i16 = 91;
pub const SQL_TYPE_TIMESTAMP: i16 = 93;
pub const SQL_LONGVARCHAR: i16 = -1;
pub const SQL_BINARY: i16 = -2;
pub const SQL_VARBINARY: i16 = -3;
pub const SQL_LONGVARBINARY: i16 = -4;
pub const SQL_BIGINT: i16 = -5;
pub const SQL_TINYINT: i16 = -6;
pub const SQL_BIT: i16 = -7;
pub const SQL_WCHAR: i16 = -8;
pub const SQL_WVARCHAR: i16 = -9;
pub const SQL_WLONGVARCHAR: i16 = -10;
pub const SQL_GUID: i16 = -11;
pub const SQL_SS_VARIANT: i16 = -150;
pub const SQL_SS_UDT: i16 = -151;
pub const SQL_SS_XML: i16 = -152;
pub const SQL_SS_TIME2: i16 = -154;
pub const SQL_SS_TIMESTAMPOFFSET: i16 = -155;

// Connection attributes.
pub const SQL_ATTR_AUTOCOMMIT: i32 = 102;
pub const SQL_ATTR_LOGIN_TIMEOUT: i32 = 103;
pub const SQL_ATTR_TXN_ISOLATION: i32 = 108;
pub const SQL_ATTR_CURRENT_CATALOG: i32 = 109;
pub const SQL_ATTR_PACKET_SIZE: i32 = 112;
pub const SQL_ATTR_CONNECTION_TIMEOUT: i32 = 113;
pub const SQL_COPT_SS_ACCESS_TOKEN: i32 = 1256;

// Statement attributes.
pub const SQL_ATTR_QUERY_TIMEOUT: i32 = 0;

// SQL_TXN_* isolation bits.
pub const SQL_TXN_READ_UNCOMMITTED: i64 = 1;
pub const SQL_TXN_READ_COMMITTED: i64 = 2;
pub const SQL_TXN_REPEATABLE_READ: i64 = 4;
pub const SQL_TXN_SERIALIZABLE: i64 = 8;
pub const SQL_TXN_SS_SNAPSHOT: i64 = 32;

// C types (only SQL_C_WCHAR is consulted).
pub const SQL_C_WCHAR: i32 = -8;
