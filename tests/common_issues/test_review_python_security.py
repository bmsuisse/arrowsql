"""Review r4 fixes in cursor / parameter logging (DB-backed)."""

import pytest

import mssql_python
from mssql_python import connect, setup_logging


def test_debug_log_has_no_sql_literals_or_param_values(conn_str, tmp_path):
    log = str(tmp_path / "dbg.log")
    setup_logging(log_file_path=log)
    conn = connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT 'LITERAL_SECRET_42' AS a, %(p)s AS b", {"p": "PARAM_SECRET_77"})
    cur.fetchall()
    cur.execute("SELECT ? AS b", ("QMARK_SECRET_99",))
    cur.fetchall()
    conn.close()
    with open(log) as f:
        content = f.read()
    assert "execute: Starting" in content
    for secret in ("LITERAL_SECRET_42", "PARAM_SECRET_77", "QMARK_SECRET_99"):
        assert secret not in content


def test_metadata_resultset_resets_uuid_indices(conn_str):
    old = mssql_python.native_uuid
    mssql_python.native_uuid = False
    try:
        conn = connect(conn_str)
        cur = conn.cursor()
        cols = ", ".join(f"{i} AS c{i}" for i in range(6)) + ", NEWID() AS g"
        cur.execute(f"SELECT {cols}")
        assert cur.fetchall()
        assert cur._uuid_str_indices is not None
        cur.tables(table="no_such_table_xyz")
        assert cur._uuid_str_indices is None
        cur.tables()
        rows = cur.fetchall()
        assert rows
        conn.close()
    finally:
        mssql_python.native_uuid = old


def test_bulkcopy_none_first_row_is_an_error_not_a_noop(conn_str):
    conn = connect(conn_str)
    cur = conn.cursor()
    with pytest.raises(TypeError, match="NoneType"):
        cur.bulkcopy("dbo.does_not_matter", [None, (1,)])
    assert list(cur._bulk_row_iterator([])) == []
    conn.close()


def test_bulkcopy_arrow_iterable_of_batches_uses_callers_connection(conn_str):
    pa = pytest.importorskip("pyarrow")
    conn = connect(conn_str)
    cur = conn.cursor()
    cur.execute("CREATE TABLE #rb (id INT, name NVARCHAR(20))")
    batches = (pa.record_batch({"id": [i], "name": [f"n{i}"]}) for i in range(3))  # one-shot
    result = cur.bulkcopy_arrow("#rb", batches, timeout=0)
    assert result["rows_copied"] == 3
    cur.execute("SELECT COUNT(*) FROM #rb")
    assert cur.fetchone()[0] == 3
    conn.close()


def test_bulkcopy_arrow_iterable_rejects_non_batches(conn_str):
    pytest.importorskip("pyarrow")
    cur = connect(conn_str).cursor()
    with pytest.raises(TypeError, match="RecordBatch"):
        cur.bulkcopy_arrow("#x", iter([(1, 2)]), timeout=0)
