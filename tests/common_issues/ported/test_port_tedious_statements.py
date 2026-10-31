"""Parameterised statements and error recovery across repeated executions.

Adapted from tediousjs/tedious (MIT), Copyright (c) 2010-2026 Mike D Pilsbury
and tedious contributors, from test/integration/parameterised-statements-test.ts
and test/integration/prepare-execute-statements-test.ts. The cases below are
written from those behaviours for DB-API cursors: qmark parameters, repeated
execute() on one cursor standing in for a prepared statement, and a failing
execution that must not leave state behind for the next one.
Upstream ODBC driver is ground truth.
"""

import pytest
from mssql_python import DataError, Error, ProgrammingError, connect


def test_parameter_type_error_does_not_affect_next_execute(cursor):
    with pytest.raises(TypeError):
        cursor.execute("SELECT ?", [object()])
    cursor.execute("SELECT ?", [5])
    assert tuple(cursor.fetchone()) == (5,)


def test_parameter_count_mismatch_then_correct_execute_works(cursor):
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT ?, ?", [1])
    cursor.execute("SELECT ?, ?", [1, 2])
    assert tuple(cursor.fetchone()) == (1, 2)


def test_runtime_error_in_one_execution_does_not_poison_reuse(cursor):
    results = []
    for divisor in [2, 0, 5]:
        try:
            cursor.execute("SELECT 10 / ?", [divisor])
            results.append(cursor.fetchone()[0])
        except Error:
            results.append("error")
    assert results == [5, "error", 2]


def test_reexecution_with_alternating_nulls_and_types_returns_each_value(cursor):
    values = [1, None, "x", None, 2.5, "", None, 7]
    observed = []
    for value in values:
        cursor.execute("SELECT ? AS v", [value])
        observed.append(cursor.fetchone()[0])
    assert observed == values
    assert isinstance(observed[0], int)
    assert isinstance(observed[2], str)


def test_repeated_execution_with_fresh_values_stays_correct(cursor):
    for i in range(500):
        cursor.execute("SELECT CAST(? AS int) + CAST(? AS int)", [i, i])
        assert cursor.fetchone()[0] == 2 * i


def test_small_int_parameters_bind_as_tinyint_in_arithmetic(cursor):
    # Ints 0..255 bind as tinyint by design (upstream test_023 test_map_sql_type_detection pins
    # SQL_TINYINT), so ? + ? overflows tinyint exactly as it does on upstream.
    with pytest.raises(DataError, match="tinyint"):
        cursor.execute("SELECT ? + ?", [200, 200])


@pytest.mark.parametrize("sql_type", ["varchar(max)", "nvarchar(max)"])
def test_empty_string_parameter_is_empty_not_null(cursor, sql_type):
    cursor.execute(f"SELECT CAST(? AS {sql_type}), CAST(? AS {sql_type})", ["", None])
    empty, nothing = cursor.fetchone()
    assert empty == ""
    assert nothing is None


def test_close_without_commit_discards_uncommitted_insert(conn_str, table_name):
    with connect(conn_str, autocommit=True) as admin:
        admin.cursor().execute(f"CREATE TABLE dbo.{table_name} (id int NOT NULL)")

    writer = connect(conn_str, autocommit=False)
    writer.cursor().execute(f"INSERT INTO dbo.{table_name} (id) VALUES (?)", [1])
    writer.close()

    with connect(conn_str, autocommit=True) as reader:
        assert reader.cursor().execute(f"SELECT COUNT(*) FROM dbo.{table_name}").fetchone()[0] == 0
