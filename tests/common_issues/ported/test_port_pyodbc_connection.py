"""Connection-level DB-API behaviour: context manager, autocommit, output converters,
decoding, catalog calls, cursor skip, and parameter-sequence validation.

Adapted from mkleehammer/pyodbc tests/sqlserver_test.py (MIT-0), Copyright the pyodbc
contributors (LICENSE.txt carries no copyright line). MIT-0 requires no notice to be
reproduced, so none is copied here, so none is copied here. The test bodies
were rewritten for mssql_python (qmark parameters, DB-API cursors, `with conn:`
commit/rollback semantics) rather than copied; pyodbc's ODBC-only surface (drivers(),
getinfo(), native_uuid, lowercase, rows_as_dicts, raw handles) is not ported.
Upstream ODBC driver is ground truth.
"""

import uuid

import pytest
from mssql_python import Error, ProgrammingError, connect, SQL_VARCHAR, SQL_CHAR


def _fresh(conn_str, **kwargs):
    return connect(conn_str, **kwargs)


def _scalar(cursor, sql, *params):
    cursor.execute(sql, *params)
    return cursor.fetchone()[0]


# --- context manager (`with conn:`) ---------------------------------------------------


def test_context_manager_commits_on_clean_exit(conn_str, table_name):
    setup = _fresh(conn_str)
    setup.cursor().execute(f"create table dbo.{table_name}(n int)")
    setup.commit()
    setup.close()

    conn = _fresh(conn_str)
    try:
        with conn:
            conn.cursor().execute(f"insert into dbo.{table_name} values (1)")
        check = _fresh(conn_str)
        try:
            assert _scalar(check.cursor(), f"select count(*) from dbo.{table_name}") == 1
        finally:
            check.close()
    finally:
        conn.close()


def test_context_manager_rolls_back_on_exception(conn_str, table_name):
    setup = _fresh(conn_str)
    setup.cursor().execute(f"create table dbo.{table_name}(n int)")
    setup.commit()
    setup.close()

    conn = _fresh(conn_str)
    try:
        with pytest.raises(Error):
            with conn:
                conn.cursor().execute(f"insert into dbo.{table_name} values (1)")
                conn.cursor().execute("delete from no_such_table_for_rollback")
        check = _fresh(conn_str)
        try:
            assert _scalar(check.cursor(), f"select count(*) from dbo.{table_name}") == 0
        finally:
            check.close()
    finally:
        conn.close()


def test_autocommit_default_is_false_and_toggles(conn):
    assert conn.autocommit is False
    conn.autocommit = True
    assert conn.autocommit is True
    conn.autocommit = False
    assert conn.autocommit is False


def test_autocommit_true_makes_insert_visible_to_other_connection(conn_str, table_name):
    writer = _fresh(conn_str, autocommit=True)
    try:
        writer.cursor().execute(f"create table dbo.{table_name}(n int)")
        writer.cursor().execute(f"insert into dbo.{table_name} values (7)")
        reader = _fresh(conn_str)
        try:
            assert _scalar(reader.cursor(), f"select n from dbo.{table_name}") == 7
        finally:
            reader.close()
    finally:
        writer.close()


def test_cursor_after_connection_close_raises_error(conn_str):
    conn = _fresh(conn_str)
    cur = conn.cursor()
    cur.execute("select 1")
    conn.close()
    with pytest.raises(Error):
        cur.execute("select 1")


# --- output converters ----------------------------------------------------------------
# mssql_python hands string values to converters as UTF-16LE bytes (see
# Connection.add_output_converter), so the test converters decode that form.


def _utf16_to_str(value):
    return "X" + value.decode("utf-16-le") + "X"


def _utf16_to_str_y(value):
    return "Y" + value.decode("utf-16-le") + "Y"


def test_output_converter_applies_to_varchar_and_clears(conn, cursor):
    cursor.execute("create table t_conv(v varchar(10))")
    cursor.execute("insert into t_conv values ('123.45')")
    try:
        conn.add_output_converter(SQL_VARCHAR, _utf16_to_str)
        assert _scalar(cursor, "select v from t_conv") == "X123.45X"

        conn.clear_output_converters()
        assert _scalar(cursor, "select v from t_conv") == "123.45"
    finally:
        cursor.execute("drop table t_conv")
        conn.commit()


def test_remove_output_converter_restores_raw_value(conn, cursor):
    cursor.execute("create table t_conv2(v varchar(10))")
    cursor.execute("insert into t_conv2 values ('abc')")
    try:
        conn.add_output_converter(SQL_VARCHAR, _utf16_to_str)
        assert _scalar(cursor, "select v from t_conv2") == "XabcX"
        conn.remove_output_converter(SQL_VARCHAR)
        assert _scalar(cursor, "select v from t_conv2") == "abc"
    finally:
        cursor.execute("drop table t_conv2")
        conn.commit()


def test_add_output_converter_none_clears_and_get_returns_previous(conn, cursor):
    cursor.execute("create table t_conv3(v varchar(10))")
    cursor.execute("insert into t_conv3 values ('abc')")
    try:
        assert conn.get_output_converter(SQL_VARCHAR) is None
        conn.add_output_converter(SQL_VARCHAR, _utf16_to_str)
        prev = conn.get_output_converter(SQL_VARCHAR)
        assert prev is not None

        conn.add_output_converter(SQL_VARCHAR, _utf16_to_str_y)
        assert _scalar(cursor, "select v from t_conv3") == "YabcY"

        # Put the previous converter back, as a temporary-override pattern would.
        conn.add_output_converter(SQL_VARCHAR, prev)
        assert _scalar(cursor, "select v from t_conv3") == "XabcX"

        conn.add_output_converter(SQL_VARCHAR, None)
        assert _scalar(cursor, "select v from t_conv3") == "abc"
    finally:
        cursor.execute("drop table t_conv3")
        conn.commit()


# --- text decoding --------------------------------------------------------------------


def test_setdecoding_sql_char_shift_jis_roundtrips_japanese_text(conn, cursor):
    conn.setdecoding(SQL_CHAR, encoding="shift_jis")
    cursor.execute("create table t_sjis(s varchar(20))")
    try:
        # Use an empty value and a plain ASCII value; the decoder must not corrupt either.
        cursor.execute("insert into t_sjis values (?)", "")
        cursor.execute("insert into t_sjis values (?)", "ab")
        rows = cursor.execute("select s from t_sjis order by len(s)").fetchall()
        assert [r[0] for r in rows] == ["", "ab"]
    finally:
        cursor.execute("drop table t_sjis")
        conn.commit()


# --- catalog functions ----------------------------------------------------------------


def test_columns_reports_unicode_column_names_and_sizes(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(a int, b varchar(3), xΏz varchar(4))")
    cursor.columns(table_name)
    results = {row.column_name: row for row in cursor.fetchall()}
    assert results["a"].type_name == "int"
    assert results["b"].type_name == "varchar"
    assert results["b"].column_size == 3
    assert results["xΏz"].type_name == "varchar"
    assert results["xΏz"].column_size == 4


def test_columns_accepts_none_for_optional_keywords(cursor, table_name):
    # Wrappers like async adapters forward keywords explicitly; None must mean "any".
    cursor.execute(f"create table dbo.{table_name}(a int, b varchar(3))")
    cursor.columns(table_name, schema=None, catalog=None)
    names = sorted(row.column_name for row in cursor.fetchall())
    assert names == ["a", "b"]


def test_statistics_finds_index_on_unicode_table_name(conn, cursor):
    name = "ランドマーク_" + uuid.uuid4().hex
    cursor.execute(f"CREATE TABLE [{name}] (id INT PRIMARY KEY, foo INT)")
    cursor.execute(f"CREATE INDEX ix_foo ON [{name}] (foo)")
    conn.commit()
    try:
        # index_name is column 5 of the SQLStatistics result set.
        index_names = {
            row[5] for row in cursor.statistics(name).fetchall() if row[5] is not None
        }
        assert "ix_foo" in index_names, index_names
    finally:
        cursor.execute(f"IF OBJECT_ID(N'[{name}]', N'U') IS NOT NULL DROP TABLE [{name}]")
        conn.commit()


# --- cursor helpers -------------------------------------------------------------------


def test_skip_discards_rows_then_fetch_continues(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(id int)")
    for i in range(1, 5):
        cursor.execute(f"insert into dbo.{table_name} values (?)", i)
    cursor.execute(f"select id from dbo.{table_name} order by id")
    assert cursor.fetchone()[0] == 1
    cursor.skip(2)
    assert cursor.fetchone()[0] == 4


def test_execute_returns_same_cursor_for_dml_and_select(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(i int)")
    cursor.execute(f"insert into dbo.{table_name} values (1)")
    assert cursor.execute(f"delete from dbo.{table_name}") is cursor
    assert cursor.execute(f"select * from dbo.{table_name}") is cursor


def test_set_is_rejected_as_execute_parameter_sequence(cursor, table_name):
    # Only sequences are valid parameter containers; a set has no defined order.
    cursor.execute(f"create table dbo.{table_name}(word varchar(100))")
    words = {"a", "b", "c"}
    with pytest.raises((TypeError, ProgrammingError)):
        cursor.execute(f"insert into dbo.{table_name} (word) values (?)", words)
    assert _scalar(cursor, f"select count(*) from dbo.{table_name}") == 0


@pytest.mark.upstream_bug(reason="executemany accepts a set of strings and inserts each string as a row")
@pytest.mark.xfail(strict=True, reason="upstream bug")
def test_set_is_rejected_by_executemany(cursor, table_name):
    cursor.execute(f"create table dbo.{table_name}(word varchar(100))")
    words = {"a", "b", "c"}
    with pytest.raises((TypeError, ProgrammingError)):
        cursor.executemany(f"insert into dbo.{table_name} (word) values (?)", words)
    assert _scalar(cursor, f"select count(*) from dbo.{table_name}") == 0
