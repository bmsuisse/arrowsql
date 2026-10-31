"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains custom exception classes for the mssql_python package.
These classes are used to raise exceptions when an error occurs while executing a query.
"""

from typing import Optional
from arrowsql.logging import logger
import builtins


class ConnectionStringParseError(builtins.Exception):
    """
    Exception raised when connection string parsing fails.

    This exception is raised when the connection string parser encounters
    syntax errors, unknown keywords, duplicate keywords, or other validation
    failures. It collects all errors and reports them together.
    """

    def __init__(self, errors: list) -> None:
        """
        Initialize the error with a list of validation errors.

        Args:
            errors: List of error messages describing what went wrong
        """
        self.errors = errors
        message = "Connection string parsing failed:\n  " + "\n  ".join(errors)
        super().__init__(message)

    def __reduce__(self):
        return (self.__class__, (self.errors,))


class Exception(builtins.Exception):
    """
    Base class for all DB API 2.0 exceptions.
    """

    def __init__(
        self,
        driver_error: str,
        ddbc_error: str,
        sqlstate: Optional[str] = None,
        native_error: Optional[int] = None,
    ) -> None:
        self.driver_error = driver_error
        self.ddbc_error = truncate_error_message(ddbc_error)
        # Stable, locale-independent identifiers for programmatic handling (#581):
        # the ODBC SQLSTATE and the SQL Server engine error number. None when the
        # error was raised by the driver itself rather than reported by the server.
        self.sqlstate = sqlstate
        self.native_error = native_error
        if self.ddbc_error:
            # Both driver and DDBC errors are present
            self.message = f"Driver Error: {self.driver_error}; DDBC Error: {self.ddbc_error}"
        else:
            # Errors raised by the driver itself should not have a DDBC error message
            self.message = f"Driver Error: {self.driver_error}"
        super().__init__(self.message)

    def __reduce__(self):
        # Reconstruct without re-running __init__/truncate_error_message() to avoid
        # emitting warnings for already-truncated "[Microsoft]..." messages.
        return (
            Exception._unpickle,
            (
                self.__class__,
                self.driver_error,
                self.ddbc_error,
                self.message,
                getattr(self, "sqlstate", None),
                getattr(self, "native_error", None),
            ),
        )

    @staticmethod
    def _unpickle(
        cls,
        driver_error: str,
        ddbc_error: str,
        message: str,
        sqlstate: Optional[str] = None,
        native_error: Optional[int] = None,
    ):
        obj = cls.__new__(cls)
        obj.driver_error = driver_error
        obj.ddbc_error = ddbc_error
        obj.message = message
        obj.sqlstate = sqlstate
        obj.native_error = native_error
        builtins.Exception.__init__(obj, message)
        return obj


class Warning(Exception):
    """
    Exception raised for important warnings like data truncations while inserting, etc.
    """


class Error(Exception):
    """
    Base class for errors.
    """


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database
    interface rather than the database itself.
    """


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database.
    """


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range, etc.
    """


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer.
    """


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected,
    e.g., a foreign key check fails.
    """


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g., the cursor is not valid anymore, the transaction is out of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors,
    e.g., table not found or already exists, syntax error in the SQL statement,
    wrong number of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which
    is not supported by the database, e.g., requesting a .rollback()
    on a connection that does not support transaction or has transactions turned off.
    """


# Mapping SQLSTATE codes to custom exception classes
_LOCK_TIMEOUT_ERROR = 1222


class LockTimeoutError(OperationalError, ProgrammingError):  # RUST-PORT
    """SQL Server error 1222 (LOCK_TIMEOUT expired); SQLSTATE stays 42000."""


_VERBOSE_TRUNCATION_ERROR = 2628


class StringTruncationError(DataError, ProgrammingError):  # RUST-PORT
    """SQL Server error 2628 (string or binary data would be truncated, with
    table and column named). A data error (22001) that upstream raises as a
    ProgrammingError (42000); both classes match."""


_CONVERSION_ERROR = 8114


class ConversionDataError(DataError, ProgrammingError):  # RUST-PORT
    """SQL Server error 8114 (error converting data type). SQLSTATE stays 42000
    and it stays the ProgrammingError upstream raises, but it is a data
    conversion failure like error 245, so it is a DataError too."""


_SQLSTATE_TABLE: dict[str, tuple[type[Exception], str]] = {
    "01000": (Warning, "General warning"),
    "01001": (OperationalError, "Cursor operation conflict"),
    "01002": (OperationalError, "Disconnect error"),
    "01003": (DataError, "NULL value eliminated in set function"),
    "01004": (DataError, "String data, right-truncated"),
    "01006": (OperationalError, "Privilege not revoked"),
    "01007": (OperationalError, "Privilege not granted"),
    "01S00": (ProgrammingError, "Invalid connection string attribute"),
    "01S01": (DataError, "Error in row"),
    "01S02": (Warning, "Option value changed"),
    "01S06": (OperationalError, "Attempt to fetch before the result set returned the first rowset"),
    "01S07": (DataError, "Fractional truncation"),
    "01S08": (OperationalError, "Error saving File DSN"),
    "01S09": (ProgrammingError, "Invalid keyword"),
    "07001": (ProgrammingError, "Wrong number of parameters"),
    "07002": (ProgrammingError, "COUNT field incorrect"),
    "07005": (ProgrammingError, "Prepared statement not a cursor-specification"),
    "07006": (ProgrammingError, "Restricted data type attribute violation"),
    "07009": (ProgrammingError, "Invalid descriptor index"),
    "07S01": (ProgrammingError, "Invalid use of default parameter"),
    "08001": (OperationalError, "Client unable to establish connection"),
    "08002": (OperationalError, "Connection name in use"),
    "08003": (OperationalError, "Connection not open"),
    "08004": (OperationalError, "Server rejected the connection"),
    "08007": (OperationalError, "Connection failure during transaction"),
    "08S01": (OperationalError, "Communication link failure"),
    "21S01": (ProgrammingError, "Insert value list does not match column list"),
    "21S02": (ProgrammingError, "Degree of derived table does not match column list"),
    "22001": (DataError, "String data, right-truncated"),
    "22002": (DataError, "Indicator variable required but not supplied"),
    "22003": (DataError, "Numeric value out of range"),
    "22007": (DataError, "Invalid datetime format"),
    "22008": (DataError, "Datetime field overflow"),
    "22012": (DataError, "Division by zero"),
    "22015": (DataError, "Interval field overflow"),
    "22018": (DataError, "Invalid character value for cast specification"),
    "22019": (ProgrammingError, "Invalid escape character"),
    "22025": (ProgrammingError, "Invalid escape sequence"),
    "22026": (DataError, "String data, length mismatch"),
    "23000": (IntegrityError, "Integrity constraint violation"),
    "24000": (ProgrammingError, "Invalid cursor state"),
    "25000": (OperationalError, "Invalid transaction state"),
    "25S01": (OperationalError, "Transaction state"),
    "25S02": (OperationalError, "Transaction is still active"),
    "25S03": (OperationalError, "Transaction is rolled back"),
    "28000": (OperationalError, "Invalid authorization specification"),
    "34000": (ProgrammingError, "Invalid cursor name"),
    "3C000": (ProgrammingError, "Duplicate cursor name"),
    "3D000": (ProgrammingError, "Invalid catalog name"),
    "3F000": (ProgrammingError, "Invalid schema name"),
    "40001": (OperationalError, "Serialization failure"),
    "40002": (IntegrityError, "Integrity constraint violation"),
    "40003": (OperationalError, "Statement completion unknown"),
    "42000": (ProgrammingError, "Syntax error or access violation"),
    "42S01": (ProgrammingError, "Base table or view already exists"),
    "42S02": (ProgrammingError, "Base table or view not found"),
    "42S11": (ProgrammingError, "Index already exists"),
    "42S12": (ProgrammingError, "Index not found"),
    "42S21": (ProgrammingError, "Column already exists"),
    "42S22": (ProgrammingError, "Column not found"),
    "44000": (IntegrityError, "WITH CHECK OPTION violation"),
    "HY000": (OperationalError, "General error"),
    "HY001": (OperationalError, "Memory allocation error"),
    "HY003": (ProgrammingError, "Invalid application buffer type"),
    "HY004": (ProgrammingError, "Invalid SQL data type"),
    "HY007": (ProgrammingError, "Associated statement is not prepared"),
    "HY008": (OperationalError, "Operation canceled"),
    "HY009": (ProgrammingError, "Invalid use of null pointer"),
    "HY010": (ProgrammingError, "Function sequence error"),
    "HY011": (ProgrammingError, "Attribute cannot be set now"),
    "HY012": (ProgrammingError, "Invalid transaction operation code"),
    "HY013": (OperationalError, "Memory management error"),
    "HY014": (OperationalError, "Limit on the number of handles exceeded"),
    "HY015": (ProgrammingError, "No cursor name available"),
    "HY016": (ProgrammingError, "Cannot modify an implementation row descriptor"),
    "HY017": (ProgrammingError, "Invalid use of an automatically allocated descriptor handle"),
    "HY018": (OperationalError, "Server declined cancel request"),
    "HY019": (DataError, "Non-character and non-binary data sent in pieces"),
    "HY020": (DataError, "Attempt to concatenate a null value"),
    "HY021": (ProgrammingError, "Inconsistent descriptor information"),
    "HY024": (ProgrammingError, "Invalid attribute value"),
    "HY090": (ProgrammingError, "Invalid string or buffer length"),
    "HY091": (ProgrammingError, "Invalid descriptor field identifier"),
    "HY092": (ProgrammingError, "Invalid attribute/option identifier"),
    "HY095": (ProgrammingError, "Function type out of range"),
    "HY096": (ProgrammingError, "Invalid information type"),
    "HY097": (ProgrammingError, "Column type out of range"),
    "HY098": (ProgrammingError, "Scope type out of range"),
    "HY099": (ProgrammingError, "Nullable type out of range"),
    "HY100": (ProgrammingError, "Uniqueness option type out of range"),
    "HY101": (ProgrammingError, "Accuracy option type out of range"),
    "HY103": (ProgrammingError, "Invalid retrieval code"),
    "HY104": (ProgrammingError, "Invalid precision or scale value"),
    "HY105": (ProgrammingError, "Invalid parameter type"),
    "HY106": (ProgrammingError, "Fetch type out of range"),
    "HY107": (ProgrammingError, "Row value out of range"),
    "HY109": (ProgrammingError, "Invalid cursor position"),
    "HY110": (ProgrammingError, "Invalid driver completion"),
    "HY111": (ProgrammingError, "Invalid bookmark value"),
    "HYC00": (NotSupportedError, "Optional feature not implemented"),
    "HYT00": (OperationalError, "Timeout expired"),
    "HYT01": (OperationalError, "Connection timeout expired"),
    "IM001": (NotSupportedError, "Driver does not support this function"),
    "IM002": (OperationalError, "Data source name not found and no default driver specified"),
    "IM003": (OperationalError, "Specified driver could not be loaded"),
    'IM004': (OperationalError, "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"),
    'IM005': (OperationalError, "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"),
    'IM006': (OperationalError, "Driver's SQLSetConnectAttr failed"),
    "IM007": (OperationalError, "No data source or driver specified; dialog prohibited"),
    "IM008": (OperationalError, "Dialog failed"),
    "IM009": (OperationalError, "Unable to load translation DLL"),
    "IM010": (OperationalError, "Data source name too long"),
    "IM011": (OperationalError, "Driver name too long"),
    "IM012": (OperationalError, "DRIVER keyword syntax error"),
    "IM013": (OperationalError, "Trace file error"),
    "IM014": (OperationalError, "Invalid name of File DSN"),
    "IM015": (OperationalError, "Corrupt file data source"),
}


def sqlstate_to_exception(
    sqlstate: str, ddbc_error: str, native_error: Optional[int] = None
) -> Optional[Exception]:
    """Map an SQLSTATE code to a custom exception instance (None if unknown)."""
    entry = _SQLSTATE_TABLE.get(sqlstate)
    if entry is None:
        return None
    cls, driver_error = entry
    # RUST-PORT: a lock timeout (1222) is operational (PEP 249) but upstream
    # reports it as ProgrammingError (42000); keep both for compatibility.
    if native_error == _LOCK_TIMEOUT_ERROR and issubclass(cls, ProgrammingError):
        cls = LockTimeoutError
    if native_error == _CONVERSION_ERROR and issubclass(cls, ProgrammingError):
        cls = ConversionDataError
    if native_error == _VERBOSE_TRUNCATION_ERROR and issubclass(cls, DataError):
        cls = StringTruncationError
    exc = cls(driver_error=driver_error, ddbc_error=ddbc_error)
    exc.sqlstate = sqlstate
    exc.native_error = native_error
    return exc


def truncate_error_message(error_message: str) -> str:
    """
    - The Driver Error message is the message that is returned by the Internal driver.
    - This section will always be at the start of the message.
    """
    try:
        if not error_message.startswith("[Microsoft]"):
            # The message is not from the driver, so no need to truncate
            return error_message
        string_first = error_message[: error_message.index("]") + 1]
        string_second = error_message[error_message.index("]") + 1 :]
        string_third = string_second[string_second.index("]") + 1 :]
        return string_first + string_third
    except ValueError as e:
        logger.warning("Error while truncating error message: %s", e)
        return error_message


def raise_exception(sqlstate: str, ddbc_error: str, native_error: Optional[int] = None) -> None:
    """
    Raise a custom exception based on the given SQLSTATE code.
    This function raises a custom exception based on the provided SQLSTATE code.
    If the code is not found in the mapping, a generic DatabaseError is raised.

    Args:
        sqlstate (str): The SQLSTATE code to map to a custom exception.
        ddbc_error (str): The DDBC error message.
        native_error (int, optional): The SQL Server engine error number.

    Raises:
        DatabaseError: If the SQLSTATE code is not found in the mapping.
    """
    exception_class = sqlstate_to_exception(sqlstate, ddbc_error, native_error)
    if exception_class:
        logger.error(f"Raising exception: {exception_class}")
        raise exception_class
    logger.error(f"Unknown SQLSTATE {sqlstate}, raising DatabaseError")
    raise DatabaseError(
        driver_error=f"An error occurred with SQLSTATE code: {sqlstate}",
        ddbc_error=f"{ddbc_error}" if ddbc_error else "Unknown DDBC error",
        sqlstate=sqlstate or None,
        native_error=native_error,
    )
