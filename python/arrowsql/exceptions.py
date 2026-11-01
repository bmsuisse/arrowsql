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


class LockTimeoutError(OperationalError, ProgrammingError):  # RUST-PORT
    """SQL Server error 1222 (LOCK_TIMEOUT expired); SQLSTATE stays 42000."""


class StringTruncationError(DataError, ProgrammingError):  # RUST-PORT
    """SQL Server error 2628 (string or binary data would be truncated, with
    table and column named). A data error (22001) that upstream raises as a
    ProgrammingError (42000); both classes match."""


class ConversionDataError(DataError, ProgrammingError):  # RUST-PORT
    """SQL Server error 8114 (error converting data type). SQLSTATE stays 42000
    and it stays the ProgrammingError upstream raises, but it is a data
    conversion failure like error 245, so it is a DataError too."""


def sqlstate_to_exception(
    sqlstate: str, ddbc_error: str, native_error: Optional[int] = None
) -> Optional[Exception]:
    """Map an SQLSTATE code to a custom exception instance (None if unknown)."""
    from arrowsql import ddbc_bindings

    return ddbc_bindings.sqlstate_to_exception(sqlstate, ddbc_error, native_error)


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
    """Raise the exception for an SQLSTATE (DatabaseError if it is unknown)."""
    from arrowsql import ddbc_bindings

    ddbc_bindings.raise_exception(sqlstate, ddbc_error, native_error)
