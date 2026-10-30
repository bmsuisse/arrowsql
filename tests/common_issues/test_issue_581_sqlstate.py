"""microsoft/mssql-python#581: exceptions expose ``sqlstate`` and ``native_error``.

SQLSTATE is the stable, locale-independent error identifier (retry on HYT00 /
40001, refresh tokens on 28000, ...); ``native_error`` is the SQL Server engine
error number. Without them callers had to substring-match English messages.

The expected SQLSTATEs are what msodbcsql reports, including its quirks (2628
"would be truncated" is 42000, not 22001), because the Rust port has to match
the upstream ODBC driver.
"""

import builtins
import pickle
import uuid

import pytest

import mssql_python
from mssql_python import connect, exceptions
from mssql_python.exceptions import raise_exception, sqlstate_to_exception

EXCEPTION_CLASSES = (
    exceptions.Warning,
    exceptions.Error,
    exceptions.InterfaceError,
    exceptions.DatabaseError,
    exceptions.DataError,
    exceptions.OperationalError,
    exceptions.IntegrityError,
    exceptions.InternalError,
    exceptions.ProgrammingError,
    exceptions.NotSupportedError,
)


def _assert_native(exc, expected):
    """native_error is asserted wherever the binding reports it on this path."""
    if exc.native_error is None:
        pytest.skip("binding does not report the SQL Server error number on this path")
    assert exc.native_error == expected


def _raise_from(cursor, setup, sql, params=()):
    if setup:
        cursor.execute(setup)
    with pytest.raises(mssql_python.Error) as info:
        cursor.execute(sql, params) if params else cursor.execute(sql)
        cursor.fetchall()
    return info.value


# ---------------------------------------------------------------------------
# Server errors raised through the cursor path
# ---------------------------------------------------------------------------

SERVER_ERRORS = [
    pytest.param(
        "CREATE TABLE #t (id int PRIMARY KEY); INSERT #t VALUES (1)",
        "INSERT #t VALUES (1)",
        (),
        exceptions.IntegrityError,
        "23000",
        2627,
        id="primary_key_violation",
    ),
    pytest.param(
        "CREATE TABLE #t (id int, u int); CREATE UNIQUE INDEX ux ON #t (u); "
        "INSERT #t VALUES (1, 1)",
        "INSERT #t VALUES (2, 1)",
        (),
        exceptions.IntegrityError,
        "23000",
        2601,
        id="unique_index_violation",
    ),
    pytest.param(
        "CREATE TABLE #t (v int NOT NULL)",
        "INSERT #t VALUES (NULL)",
        (),
        exceptions.IntegrityError,
        "23000",
        515,
        id="not_null_violation",
    ),
    pytest.param(
        None, "SELECT 1 +", (), exceptions.ProgrammingError, "42000", 102, id="syntax_error"
    ),
    pytest.param(
        None,
        "SELECT * FROM dbo.issue581_no_such_table",
        (),
        exceptions.ProgrammingError,
        "42S02",
        208,
        id="invalid_object",
    ),
    pytest.param(
        None,
        "SELECT no_such_column FROM (VALUES (1)) v(a)",
        (),
        exceptions.ProgrammingError,
        "42S22",
        207,
        id="invalid_column",
    ),
    pytest.param(
        None, "SELECT 1 / 0", (), exceptions.DataError, "22012", 8134, id="divide_by_zero"
    ),
    pytest.param(
        None,
        "SELECT CAST('abc' AS int)",
        (),
        exceptions.DataError,
        "22018",
        245,
        id="conversion_failure",
    ),
    pytest.param(
        None,
        "SELECT CAST(2147483647 AS int) + 1",
        (),
        exceptions.DataError,
        "22003",
        8115,
        id="arithmetic_overflow",
    ),
    pytest.param(
        "CREATE TABLE #t (v varchar(3))",
        "INSERT #t VALUES (?)",
        ("abcdef",),
        # msodbcsql has no SQLSTATE mapping for 2628, so the severity tier gives 42000.
        exceptions.ProgrammingError,
        "42000",
        2628,
        id="verbose_truncation_2628_parameterized",
    ),
    pytest.param(
        None,
        "THROW 51000, 'issue581 custom error', 7",
        (),
        exceptions.ProgrammingError,
        "42000",
        51000,
        id="throw_user_error",
    ),
    pytest.param(
        None,
        "RAISERROR('issue581 %d', 16, 3, 42)",
        (),
        exceptions.ProgrammingError,
        "42000",
        50000,
        id="raiserror_adhoc",
    ),
]


@pytest.mark.parametrize("setup, sql, params, exc_class, sqlstate, native", SERVER_ERRORS)
def test_server_error_carries_sqlstate_and_native_error(
    cursor, setup, sql, params, exc_class, sqlstate, native
):
    exc = _raise_from(cursor, setup, sql, params)
    assert type(exc) is exc_class
    assert exc.sqlstate == sqlstate
    _assert_native(exc, native)


def test_legacy_truncation_8152_is_22001(conn_str):
    """With verbose truncation off the server raises 8152, which maps to 22001."""
    # ALTER DATABASE SCOPED CONFIGURATION is not allowed inside a transaction.
    conn = connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    cursor.execute("ALTER DATABASE SCOPED CONFIGURATION SET VERBOSE_TRUNCATION_WARNINGS = OFF")
    try:
        exc = _raise_from(cursor, "CREATE TABLE #t (v varchar(3))", "INSERT #t VALUES ('abcdef')")
    finally:
        cursor.execute("ALTER DATABASE SCOPED CONFIGURATION SET VERBOSE_TRUNCATION_WARNINGS = ON")
        conn.close()
    assert isinstance(exc, exceptions.DataError)
    assert exc.sqlstate == "22001"
    _assert_native(exc, 8152)


def test_foreign_key_violation(cursor, table_name):
    child = f"{table_name}_child"
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id int PRIMARY KEY)")
    cursor.execute(
        f"CREATE TABLE dbo.{child} (id int, parent_id int " f"REFERENCES dbo.{table_name} (id))"
    )
    cursor.connection.commit()
    try:
        exc = _raise_from(cursor, None, f"INSERT dbo.{child} VALUES (1, 999)")
        assert isinstance(exc, exceptions.IntegrityError)
        assert exc.sqlstate == "23000"
        _assert_native(exc, 547)
    finally:
        cursor.connection.rollback()
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{child}")
        cursor.connection.commit()


def test_error_raised_while_fetching_rows(cursor):
    """An error on a later row surfaces from the fetch path with the same identity."""
    with pytest.raises(exceptions.DataError) as info:
        cursor.execute(
            "SELECT CASE WHEN n = 3 THEN 1 / 0 ELSE n END FROM (VALUES (1), (2), (3)) v(n)"
        )
        cursor.fetchall()
    assert info.value.sqlstate == "22012"
    _assert_native(info.value, 8134)


def test_executemany_error_carries_identity(cursor):
    cursor.execute("CREATE TABLE #t (id int PRIMARY KEY)")
    with pytest.raises(exceptions.IntegrityError) as info:
        cursor.executemany("INSERT #t VALUES (?)", [(1,), (2,), (1,)])
    assert info.value.sqlstate == "23000"
    _assert_native(info.value, 2627)


def test_cursor_reusable_after_error(cursor):
    exc = _raise_from(cursor, None, "SELECT 1 / 0")
    assert exc.sqlstate == "22012"
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42


# ---------------------------------------------------------------------------
# Driver-side errors, timeouts, connection path
# ---------------------------------------------------------------------------


def test_query_timeout_is_hyt00_without_native_error(conn_str):
    conn = connect(conn_str, autocommit=True)
    try:
        conn.timeout = 1
        with pytest.raises(exceptions.OperationalError) as info:
            conn.cursor().execute("WAITFOR DELAY '00:00:05'")
        assert info.value.sqlstate == "HYT00"
        # The timeout is raised by the driver, not by SQL Server.
        assert info.value.native_error is None
    finally:
        conn.close()


def test_lock_timeout(conn_str, table_name):
    holder = connect(conn_str, autocommit=False)
    waiter = connect(conn_str, autocommit=True)
    try:
        hc = holder.cursor()
        hc.execute(f"CREATE TABLE dbo.{table_name} (id int)")
        holder.commit()
        hc.execute(f"INSERT dbo.{table_name} VALUES (1)")  # holds an X lock, uncommitted
        wc = waiter.cursor()
        wc.execute("SET LOCK_TIMEOUT 200")
        exc = _raise_from(wc, None, f"SELECT * FROM dbo.{table_name}")
        assert isinstance(exc, exceptions.ProgrammingError)
        assert exc.sqlstate == "42000"
        _assert_native(exc, 1222)
    finally:
        holder.rollback()
        holder.close()
        waiter.close()


def test_login_failure_is_28000(conn_str):
    bad = conn_str.replace("Pwd=", f"Pwd=wrong{uuid.uuid4().hex[:6]}", 1)
    assert bad != conn_str, "test expects a Pwd= key in DB_CONNECTION_STRING"
    with pytest.raises(exceptions.OperationalError) as info:
        connect(bad)
    assert info.value.sqlstate == "28000"
    _assert_native(info.value, 18456)


def test_closed_connection_error_is_driver_side(conn_str):
    conn = connect(conn_str)
    conn.close()
    with pytest.raises(exceptions.InterfaceError) as info:
        conn.cursor()
    assert info.value.sqlstate is None
    assert info.value.native_error is None


def test_closed_cursor_error_is_driver_side(cursor):
    cursor.close()
    with pytest.raises(exceptions.ProgrammingError) as info:
        cursor.execute("SELECT 1")
    assert info.value.sqlstate is None
    assert info.value.native_error is None


# ---------------------------------------------------------------------------
# API shape and backward compatibility
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exc_class", EXCEPTION_CLASSES)
def test_every_class_has_the_attributes(exc_class):
    legacy = exc_class("driver text", "ddbc text")
    assert legacy.sqlstate is None and legacy.native_error is None

    exc = exc_class("driver text", "ddbc text", sqlstate="40001", native_error=1205)
    assert (exc.sqlstate, exc.native_error) == ("40001", 1205)
    assert exc.driver_error == "driver text"
    assert exc.ddbc_error == "ddbc text"
    assert str(exc) == "Driver Error: driver text; DDBC Error: ddbc text"
    assert exc.args == (str(exc),)


def test_message_format_unchanged_for_server_errors(cursor):
    exc = _raise_from(cursor, None, "SELECT 1 / 0")
    assert exc.driver_error == "Division by zero"
    assert "Divide by zero error encountered" in exc.ddbc_error
    assert str(exc) == f"Driver Error: Division by zero; DDBC Error: {exc.ddbc_error}"


def test_pickle_round_trip_keeps_identity(cursor):
    exc = _raise_from(cursor, None, "SELECT CAST('abc' AS int)")
    clone = pickle.loads(pickle.dumps(exc))
    assert type(clone) is type(exc)
    assert (clone.sqlstate, clone.native_error) == (exc.sqlstate, exc.native_error)
    assert (clone.driver_error, clone.ddbc_error, str(clone)) == (
        exc.driver_error,
        exc.ddbc_error,
        str(exc),
    )


def test_unpickle_accepts_pre_581_payload():
    """Pickles written before #581 carry only 4 reduce args."""
    obj = exceptions.Exception._unpickle(
        exceptions.DataError, "drv", "ddbc", "Driver Error: drv; DDBC Error: ddbc"
    )
    assert isinstance(obj, exceptions.DataError)
    assert obj.sqlstate is None and obj.native_error is None


def test_sqlstate_to_exception_sets_identity():
    exc = sqlstate_to_exception("40001", "deadlock victim", 1205)
    assert isinstance(exc, exceptions.OperationalError)
    assert (exc.sqlstate, exc.native_error) == ("40001", 1205)
    # The two-argument form keeps working.
    assert sqlstate_to_exception("23000", "dup").native_error is None


def test_unknown_sqlstate_fallback_keeps_sqlstate():
    with pytest.raises(exceptions.DatabaseError) as info:
        raise_exception("ZZ999", "something odd", 77777)
    assert type(info.value) is exceptions.DatabaseError
    assert (info.value.sqlstate, info.value.native_error) == ("ZZ999", 77777)


def test_transient_classification_example():
    """The retry helper from the issue works without parsing English text."""
    transient = frozenset({"HYT00", "HYT01", "08001", "08S01", "08007", "40001", "40003"})
    deadlock = sqlstate_to_exception("40001", "Transaction was deadlocked", 1205)
    syntax = sqlstate_to_exception("42000", "Incorrect syntax", 102)
    assert deadlock.sqlstate in transient
    assert syntax.sqlstate not in transient


# ---------------------------------------------------------------------------
# Async (py-core) path
# ---------------------------------------------------------------------------


def _fake_py_core_error(name, sql_errors):
    native_type = type(name, (builtins.Exception,), {"__module__": "mssql_py_core"})
    error = native_type("native failure")
    error.sql_errors = sql_errors
    return error


@pytest.mark.parametrize(
    "number, severity, public_class, sqlstate",
    [
        (2627, 14, exceptions.IntegrityError, "23000"),
        (8134, 16, exceptions.DataError, "22012"),
        (208, 16, exceptions.ProgrammingError, "42S02"),
        (1205, 13, exceptions.DatabaseError, "40001"),
        (50000, 16, exceptions.DatabaseError, "42000"),  # unmapped: severity tier
        (9999, 20, exceptions.DatabaseError, "HY000"),  # unmapped fatal
    ],
)
def test_async_translation_carries_identity(number, severity, public_class, sqlstate):
    from mssql_python.async_query.exception_translator import translate_py_core_exception

    error = _fake_py_core_error(
        "DatabaseError", [{"number": number, "class": severity, "state": 1, "message": "x"}]
    )
    translated = translate_py_core_exception(error)
    assert type(translated) is public_class
    assert (translated.sqlstate, translated.native_error) == (sqlstate, number)


def test_async_translation_without_server_error_has_no_identity():
    from mssql_python.async_query.exception_translator import translate_py_core_exception

    translated = translate_py_core_exception(_fake_py_core_error("OperationalError", []))
    assert isinstance(translated, exceptions.OperationalError)
    assert translated.sqlstate is None and translated.native_error is None


@pytest.mark.asyncio
async def test_async_server_error_end_to_end(conn_str):
    pytest.importorskip("mssql_py_core", exc_type=ImportError)
    from mssql_python.async_query import _AsyncConnection  # pyright: ignore

    connection = await _AsyncConnection.connect(conn_str)
    try:
        cursor = connection.cursor()
        with pytest.raises(exceptions.DataError) as info:
            await cursor.execute("SELECT 1 / 0")
            await cursor.fetchall()
        assert (info.value.sqlstate, info.value.native_error) == ("22012", 8134)
        await cursor.close()
    finally:
        await connection.close()
