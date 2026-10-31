"""Errors and messages: RAISERROR severities, PRINT output, batch errors,
DB-API error class mapping and the SQLSTATE-derived exception text.

Ground truth is the upstream ODBC driver (.venv-upstream).
"""

import pytest
from mssql_python.exceptions import DataError, IntegrityError, ProgrammingError


# --- RAISERROR severity and cursor.messages ---------------------------------


@pytest.mark.parametrize("severity", [0, 10])
def test_raiserror_low_severity_does_not_raise(cursor, severity):
    cursor.execute(f"RAISERROR('low sev {severity}', {severity}, 1)")
    assert len(cursor.messages) == 1
    sqlstate_and_native, text = cursor.messages[0]
    assert sqlstate_and_native == "[01000] (50000)"
    assert text.endswith(f"[SQL Server]low sev {severity}")


@pytest.mark.parametrize("severity", [11, 16])
def test_raiserror_error_severity_raises_programming_error(cursor, severity):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute(f"RAISERROR('high sev {severity}', {severity}, 1)")
    assert excinfo.value.driver_error == "Syntax error or access violation"
    assert excinfo.value.ddbc_error == f"[Microsoft][SQL Server]high sev {severity}"


def test_raiserror_severity_10_formats_arguments(cursor):
    cursor.execute("RAISERROR('value %d of %s', 10, 1, 42, 'x')")
    assert len(cursor.messages) == 1
    assert cursor.messages[0][1].endswith("[SQL Server]value 42 of x")


def test_print_message_lands_in_cursor_messages(cursor):
    cursor.execute("PRINT 'hello print'")
    assert len(cursor.messages) == 1
    sqlstate_and_native, text = cursor.messages[0]
    assert sqlstate_and_native == "[01000] (0)"
    assert text.endswith("[SQL Server]hello print")


def test_messages_cleared_by_next_execute(cursor):
    cursor.execute("PRINT 'first statement output'")
    assert len(cursor.messages) == 1

    cursor.execute("SELECT 1")
    assert cursor.messages == []


def test_print_between_result_sets_visible_after_nextset(cursor):
    cursor.execute("SELECT 1 AS a; PRINT 'between result sets'; SELECT 2 AS b")
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]
    assert cursor.nextset() is True
    texts = [text for _, text in cursor.messages]
    assert any(t.endswith("[SQL Server]between result sets") for t in texts)


@pytest.mark.upstream_bug(
    reason=(
        "nextset() stops on a statement with no result set (PRINT, RAISERROR 10 or a "
        "DML count) and returns True, so the following fetch raises 'Invalid cursor "
        "state' instead of advancing to the next real result set"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_nextset_skips_print_to_reach_next_result_set(cursor):
    cursor.execute("SELECT 1 AS a; PRINT 'between result sets'; SELECT 2 AS b")
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]
    assert cursor.nextset() is True
    assert [tuple(r) for r in cursor.fetchall()] == [(2,)]


def test_print_before_error_in_second_statement_is_kept(cursor):
    # The PRINT runs before the failing SELECT, so its message is visible
    # once the batch has been executed.
    cursor.execute("PRINT 'before failure'; SELECT * FROM no_such_table_messages_zz")
    assert len(cursor.messages) == 1
    assert cursor.messages[0][1].endswith("[SQL Server]before failure")
    with pytest.raises(ProgrammingError):
        cursor.nextset()


# --- Errors in later statements of a batch ----------------------------------


def test_error_in_second_statement_is_deferred_to_nextset(cursor):
    # execute() succeeds; the error of the second SELECT surfaces on nextset().
    cursor.execute("SELECT 1 AS a; SELECT 1/0 AS b")
    assert [tuple(r) for r in cursor.fetchall()] == [(1,)]
    with pytest.raises(DataError) as excinfo:
        cursor.nextset()
    assert excinfo.value.driver_error == "Division by zero"


def test_cursor_usable_after_second_statement_error(cursor):
    cursor.execute("SELECT 1 AS a; SELECT 1/0 AS b")
    cursor.fetchall()
    with pytest.raises(DataError):
        cursor.nextset()

    cursor.execute("SELECT 42")
    assert tuple(cursor.fetchone()) == (42,)


def test_dml_batch_second_statement_pk_violation_surfaces_on_nextset(cursor):
    cursor.execute("CREATE TABLE #batch_pk (id INT PRIMARY KEY)")
    cursor.execute("INSERT INTO #batch_pk VALUES (1); INSERT INTO #batch_pk VALUES (1)")
    assert cursor.rowcount == 1
    with pytest.raises(IntegrityError) as excinfo:
        cursor.nextset()
    assert excinfo.value.driver_error == "Integrity constraint violation"
    assert "PRIMARY KEY" in excinfo.value.ddbc_error


# --- Error class mapping -----------------------------------------------------


def test_primary_key_violation_is_integrity_error(cursor):
    cursor.execute("CREATE TABLE #pk_map (id INT PRIMARY KEY)")
    cursor.execute("INSERT INTO #pk_map VALUES (1)")
    with pytest.raises(IntegrityError) as excinfo:
        cursor.execute("INSERT INTO #pk_map VALUES (1)")
    assert excinfo.value.driver_error == "Integrity constraint violation"
    assert "Violation of PRIMARY KEY constraint" in excinfo.value.ddbc_error


def test_not_null_violation_is_integrity_error(cursor):
    cursor.execute("CREATE TABLE #nn_map (id INT NOT NULL)")
    with pytest.raises(IntegrityError) as excinfo:
        cursor.execute("INSERT INTO #nn_map VALUES (NULL)")
    assert excinfo.value.driver_error == "Integrity constraint violation"
    assert "does not allow nulls" in excinfo.value.ddbc_error


def test_unique_violation_is_integrity_error(cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, code NVARCHAR(10) UNIQUE)")
    cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (1, N'A')")
    cursor.connection.commit()
    with pytest.raises(IntegrityError) as excinfo:
        cursor.execute(f"INSERT INTO dbo.{table_name} VALUES (2, N'A')")
    assert excinfo.value.driver_error == "Integrity constraint violation"
    assert "UNIQUE" in excinfo.value.ddbc_error


def test_foreign_key_violation_is_integrity_error(cursor, table_name):
    child = f"{table_name}_child"
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY)")
    cursor.execute(f"CREATE TABLE dbo.{child} (pid INT REFERENCES dbo.{table_name}(id))")
    cursor.connection.commit()
    try:
        with pytest.raises(IntegrityError) as excinfo:
            cursor.execute(f"INSERT INTO dbo.{child} VALUES (99)")
        assert excinfo.value.driver_error == "Integrity constraint violation"
        assert "FOREIGN KEY" in excinfo.value.ddbc_error
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS dbo.{child}")
        cursor.connection.commit()


def test_invalid_object_name_is_programming_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELECT * FROM no_such_table_map_zz")
    assert excinfo.value.driver_error == "Base table or view not found"
    assert excinfo.value.ddbc_error == (
        "[Microsoft][SQL Server]Invalid object name 'no_such_table_map_zz'."
    )


def test_invalid_column_name_is_programming_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELECT no_col_map_zz FROM sys.objects")
    assert excinfo.value.driver_error == "Column not found"
    assert "Invalid column name 'no_col_map_zz'" in excinfo.value.ddbc_error


def test_bad_sql_syntax_is_programming_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELEC 1")
    assert excinfo.value.driver_error == "Syntax error or access violation"


def test_arithmetic_overflow_is_data_error(cursor):
    with pytest.raises(DataError) as excinfo:
        cursor.execute("SELECT CAST(300 AS TINYINT)")
    assert excinfo.value.driver_error == "Numeric value out of range"
    assert excinfo.value.ddbc_error == (
        "[Microsoft][SQL Server]Arithmetic overflow error for data type tinyint, value = 300."
    )


def test_divide_by_zero_is_data_error(cursor):
    with pytest.raises(DataError) as excinfo:
        cursor.execute("SELECT 1/0")
    assert excinfo.value.driver_error == "Division by zero"


def test_failed_conversion_is_data_error(cursor):
    with pytest.raises(DataError) as excinfo:
        cursor.execute("SELECT CAST('abc' AS INT)")
    assert excinfo.value.driver_error == "Invalid character value for cast specification"


@pytest.mark.upstream_bug(
    reason=(
        "String truncation (SQLSTATE 22001) is raised as ProgrammingError with the "
        "42000 'Syntax error or access violation' text instead of DataError; the "
        "module maps 22001 to DataError"
    )
)
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_string_truncation_is_data_error(cursor):
    cursor.execute("CREATE TABLE #trunc_map (n NVARCHAR(3))")
    with pytest.raises(DataError) as excinfo:
        cursor.execute("INSERT INTO #trunc_map VALUES (N'toolong')")
    # Truncation is SQLSTATE 22001; "Numeric value out of range" is 22003's text.
    assert excinfo.value.driver_error == "String data, right-truncated"
    assert "would be truncated" in excinfo.value.ddbc_error


# --- Exception text derived from SQLSTATE ------------------------------------


def test_exception_args_carry_driver_and_ddbc_text(cursor):
    cursor.execute("CREATE TABLE #args_pk (id INT PRIMARY KEY)")
    cursor.execute("INSERT INTO #args_pk VALUES (1)")
    with pytest.raises(IntegrityError) as excinfo:
        cursor.execute("INSERT INTO #args_pk VALUES (1)")
    exc = excinfo.value
    assert len(exc.args) == 1
    assert exc.args[0] == f"Driver Error: {exc.driver_error}; DDBC Error: {exc.ddbc_error}"
    assert exc.args[0].startswith("Driver Error: Integrity constraint violation; DDBC Error: ")


def test_sqlstate_class_text_differs_between_integrity_and_programming(cursor):
    # 23000 (integrity) and 42S02 (missing object) map to different classes and
    # different driver text; the class is what callers should branch on.
    cursor.execute("CREATE TABLE #cls_pk (id INT PRIMARY KEY)")
    cursor.execute("INSERT INTO #cls_pk VALUES (1)")
    with pytest.raises(IntegrityError) as dup:
        cursor.execute("INSERT INTO #cls_pk VALUES (1)")
    with pytest.raises(ProgrammingError) as missing:
        cursor.execute("SELECT * FROM no_such_table_cls_zz")
    assert dup.value.driver_error != missing.value.driver_error
    assert not isinstance(missing.value, IntegrityError)
    assert not isinstance(dup.value, ProgrammingError)


def test_truncate_error_message_with_one_bracket_returns_message():
    """A driver message with a single "[Microsoft]" prefix has nothing to cut;
    it comes back unchanged instead of raising out of the error handler."""
    from arrowsql.exceptions import truncate_error_message

    assert truncate_error_message("[Microsoft]Operation canceled") == "[Microsoft]Operation canceled"
    assert truncate_error_message("[Microsoft][ODBC Driver]boom") == "[Microsoft]boom"
