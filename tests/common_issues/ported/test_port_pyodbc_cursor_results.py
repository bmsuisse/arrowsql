"""Cursor execution, stored-procedure result shapes, Row semantics and parameter reuse.

Adapted from mkleehammer/pyodbc tests/sqlserver_test.py (MIT-0), Copyright the pyodbc
contributors (LICENSE.txt carries no copyright line). MIT-0 requires no notice to be
reproduced, so none is copied here. Cases were re-expressed for mssql_python:
qmark parameters, DB-API cursors, and Row objects returned by fetch*. Procedures use
unique names and are dropped in finally blocks so the shared database stays clean.
Upstream ODBC driver is ground truth.
"""

import uuid
from datetime import datetime

import pytest
from mssql_python import Error, ProgrammingError


def _drop_proc(cursor, name):
    cursor.execute(f"IF OBJECT_ID(N'dbo.{name}', N'P') IS NOT NULL DROP PROCEDURE dbo.{name}")
    cursor.connection.commit()


# --- stored procedures and result shapes ------------------------------------------------


def test_proc_select_returns_rows_as_list(cursor):
    name = "pp_" + uuid.uuid4().hex[:10]
    try:
        cursor.execute(f"create procedure dbo.{name} as select top 10 n from (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) v(n) order by n")
        rows = cursor.execute(f"exec dbo.{name}").fetchall()
        assert isinstance(rows, list)
        assert [r.n for r in rows] == list(range(1, 11))
    finally:
        _drop_proc(cursor, name)


def test_proc_with_temp_table_result_has_description(cursor):
    name = "pp_" + uuid.uuid4().hex[:10]
    try:
        # SET NOCOUNT keeps the INSERT row count out of the result sequence.
        cursor.execute(f"""
            create procedure dbo.{name} as
            set nocount on
            select top 3 name, object_id, type, create_date
            into #tmptable
            from sys.objects
            order by object_id
            select * from #tmptable
        """)
        cursor.execute(f"exec dbo.{name}")
        assert cursor.description is not None
        assert len(cursor.description) == 4
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert isinstance(rows[0].create_date, datetime)
    finally:
        _drop_proc(cursor, name)


def test_proc_with_table_variable_result(cursor):
    name = "pp_" + uuid.uuid4().hex[:10]
    try:
        cursor.execute(f"""
            create procedure dbo.{name} as
            set nocount on
            declare @t table(name varchar(100), id int, xtype varchar(4), refdate datetime)
            insert into @t
            select top 10 name, object_id, type, create_date from sys.objects
            select * from @t
        """)
        cursor.execute(f"exec dbo.{name}")
        rows = cursor.fetchall()
        assert len(rows) == 10
        assert isinstance(rows[0].refdate, datetime)
    finally:
        _drop_proc(cursor, name)


def test_proc_accepts_two_datetime_params(cursor):
    name = "pp_" + uuid.uuid4().hex[:10]
    try:
        # Forum reports say two datetime arguments to a proc misbehave; they must bind.
        cursor.execute(f"""
            create procedure dbo.{name}(@d1 datetime, @d2 datetime) as
            begin
              declare @d int = datediff(year, @d1, @d2)
              select @d
            end
        """)
        cursor.execute(f"exec dbo.{name} ?, ?", datetime.now(), datetime.now())
        rows = cursor.fetchall()
        assert rows[0][0] == 0
    finally:
        _drop_proc(cursor, name)


# --- Row object behaviour -----------------------------------------------------------


def test_row_slicing_returns_expected_tuples(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(a int, b int, c int, d int)")
    cursor.execute(f"insert into dbo.{table_name} values (1, 2, 3, 4)")
    row = cursor.execute(f"select * from dbo.{table_name}").fetchone()
    assert tuple(row[:]) == (1, 2, 3, 4)
    assert tuple(row[:-1]) == (1, 2, 3)
    assert tuple(row[0:4]) == (1, 2, 3, 4)
    assert tuple(row[:1]) == (1,)


def test_row_str_matches_tuple_text(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(a int, b int, c int, d varchar(50))")
    cursor.execute(f"insert into dbo.{table_name} values (1, 2, 3, 'four')")
    row = cursor.execute(f"select * from dbo.{table_name}").fetchone()
    assert str(tuple(row)) == "(1, 2, 3, 'four')"
    assert str(tuple(row[:-1])) == "(1, 2, 3)"
    assert str(tuple(row[:1])) == "(1,)"


def test_row_equality_is_by_value_and_inequality_on_different_rows(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(n int, s varchar(20))")
    cursor.execute(f"insert into dbo.{table_name} values (1, 'test1')")
    cursor.execute(f"insert into dbo.{table_name} values (1, 'test2')")
    rows = cursor.execute(f"select n, s from dbo.{table_name} order by s").fetchall()
    assert rows[0] != rows[1]
    assert tuple(rows[0]) < tuple(rows[1])
    assert sorted(reversed([tuple(r) for r in rows])) == [tuple(r) for r in rows]


def test_row_equal_for_identical_selects(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(n int, s varchar(20))")
    cursor.execute(f"insert into dbo.{table_name} values (1, 'test')")
    row1 = cursor.execute(f"select n, s from dbo.{table_name}").fetchone()
    row2 = cursor.execute(f"select n, s from dbo.{table_name}").fetchone()
    assert row1 == row2


def test_row_is_accepted_as_execute_and_executemany_parameter(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(n int, s varchar(10))")
    for i in range(3):
        cursor.execute(f"insert into dbo.{table_name} values (?, ?)", i, chr(ord("a") + i))
    rows = cursor.execute(f"select n, s from dbo.{table_name} order by n").fetchall()
    assert len(rows) == 3

    cursor.execute(f"create table dbo.{table_name}_copy(n int, s varchar(10))")
    try:
        cursor.execute(f"insert into dbo.{table_name}_copy values (?, ?)", rows[0])
        cursor.executemany(f"insert into dbo.{table_name}_copy values (?, ?)", rows)
        assert cursor.execute(f"select count(*) from dbo.{table_name}_copy").fetchval() == 4
    finally:
        cursor.execute(f"drop table dbo.{table_name}_copy")
        cursor.connection.commit()


# --- parameter reuse and subqueries ---------------------------------------------------


def test_repeated_parameterised_executes_on_one_cursor(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(n int)")
    for n in (1, 2, 3):
        cursor.execute(f"insert into dbo.{table_name} values (?)", n)
    for _ in range(3):
        assert [tuple(r) for r in cursor.execute(f"select n from dbo.{table_name} where n < ? order by n", 10).fetchall()] == [(1,), (2,), (3,)]
        assert [tuple(r) for r in cursor.execute(f"select n from dbo.{table_name} where n < 3 order by n").fetchall()] == [(1,), (2,)]


def test_parameters_inside_derived_table_and_between(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(id int, s varchar(20))")
    cursor.execute(f"insert into dbo.{table_name} values (?, ?)", 1, "test")
    row = cursor.execute(
        f"""
        select x.id
        from (select id from dbo.{table_name} where s = ? and id between ? and ?) x
        """,
        "test", 1, 10,
    ).fetchone()
    assert row is not None and row[0] == 1


def test_consecutive_selects_without_fetch_are_allowed(cursor):
    cursor.execute("select 1")
    cursor.execute("select 1")
    cursor.execute("select 1")
    assert cursor.fetchone()[0] == 1


def test_concatenation_of_wide_nvarchar_columns(cursor, table_name):
    v2 = "0123456789" * 30
    v3 = "9876543210" * 30
    cursor.execute(f"create table dbo.{table_name}(c1 int identity(1,1), c2 varchar(300), c3 varchar(300))")
    cursor.execute(f"insert into dbo.{table_name}(c2, c3) values (?, ?)", v2, v3)
    row = cursor.execute(f"select c2 + c3 as both from dbo.{table_name}").fetchone()
    assert row.both == v2 + v3


# --- large parameters and empty values --------------------------------------------------


def test_large_varbinary_update_with_false_predicate_is_a_noop(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(a varbinary(max))")
    hundred_kb = b"x" * 100 * 1024
    cursor.execute(f"update dbo.{table_name} set a = ? where 1 = 0", (hundred_kb,))
    assert cursor.rowcount == 0
    assert cursor.execute(f"select count(*) from dbo.{table_name}").fetchval() == 0


def test_executemany_empty_string_into_nvarchar_max(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(a nvarchar(max))")
    cursor.executemany(f"insert into dbo.{table_name}(a) values (?)", [[""]])
    value = cursor.execute(f"select a from dbo.{table_name}").fetchone()[0]
    assert value == ""


def test_too_large_string_insert_raises_error(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(s varchar(800))")
    with pytest.raises(Error):
        cursor.execute(f"insert into dbo.{table_name} values (?)", "x" * 1000)
