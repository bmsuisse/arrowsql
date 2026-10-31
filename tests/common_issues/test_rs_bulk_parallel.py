"""Parallel Arrow bulk load (mssql_python.write_arrow) on the Rust port.

Each worker runs its own bulk copy with TABLOCK into a heap. The tests check
row counts and contents across worker counts, the non-heap fallback, error
propagation and the argument checks.
"""

import sys
import warnings

import pytest

pa = pytest.importorskip("pyarrow")

from mssql_python import ddbc_bindings, write_arrow  # noqa: E402

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="write_arrow is a Rust-port feature",
)

N_ROWS = 20_000


def _source(n=N_ROWS):
    ids = pa.array(range(n), type=pa.int64())
    names = pa.array([f"row-{i}" for i in range(n)], type=pa.string())
    return pa.table({"id": ids, "name": names})


def _create_heap(cursor, table):
    cursor.execute(f"CREATE TABLE {table} (id BIGINT NOT NULL, name NVARCHAR(40) NULL)")
    cursor.connection.commit()


def _fetch_stats(cursor, table):
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(id), MIN(id), MAX(id) FROM {table}")
    return tuple(cursor.fetchone())


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_loads_every_row_into_heap(conn_str, cursor, table_name, workers):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)

    result = write_arrow(conn_str, table, _source(), workers=workers)

    assert result["rows_copied"] == N_ROWS
    assert 1 <= result["workers"] <= workers
    count, total, low, high = _fetch_stats(cursor, table)
    assert count == N_ROWS
    assert total == sum(range(N_ROWS))
    assert (low, high) == (0, N_ROWS - 1)


def test_parallel_load_needs_no_mssql_py_core(conn_str, cursor, table_name, monkeypatch):
    # The base install has no mssql_py_core: every worker must use the native bulk copy.
    monkeypatch.setitem(sys.modules, "mssql_py_core", None)
    monkeypatch.setitem(sys.modules, "mssql_py_core.mssql_py_core", None)
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)

    result = write_arrow(conn_str, table, _source(), workers=4)

    assert result["rows_copied"] == N_ROWS


def test_parallel_load_keeps_row_contents(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)

    write_arrow(conn_str, table, _source(5_000), workers=4)

    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE name <> CONCAT('row-', id)")
    assert cursor.fetchone()[0] == 0


def test_accepts_record_batch_reader(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    source = _source()
    reader = pa.RecordBatchReader.from_batches(
        source.schema, source.to_batches(max_chunksize=1_000)
    )

    result = write_arrow(conn_str, table, reader, workers=3)

    assert result["rows_copied"] == N_ROWS
    assert _fetch_stats(cursor, table)[0] == N_ROWS


def test_empty_source_loads_nothing(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)

    result = write_arrow(conn_str, table, _source(0), workers=4)

    assert result["rows_copied"] == 0
    assert _fetch_stats(cursor, table)[0] == 0


def test_non_heap_target_falls_back_to_one_worker(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(
        f"CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY CLUSTERED, "
        "name NVARCHAR(40) NULL)"
    )
    cursor.connection.commit()

    with pytest.warns(RuntimeWarning, match="clustered index"):
        result = write_arrow(conn_str, table, _source(), workers=4)

    assert result["workers"] == 1
    assert result["rows_copied"] == N_ROWS
    assert _fetch_stats(cursor, table)[0] == N_ROWS


def test_heap_target_does_not_warn(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        warnings.filterwarnings("ignore", message=".*recovery model.*")
        write_arrow(conn_str, table, _source(1_000), workers=2)


def test_worker_error_is_raised(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    # Non-numeric ids cannot convert to BIGINT, so every worker's stream fails.
    bad = pa.table({"id": pa.array([f"x{i}" for i in range(N_ROWS)]), "name": pa.array(["n"] * N_ROWS)})

    with pytest.raises(Exception):
        write_arrow(conn_str, table, bad, workers=4)


@pytest.mark.parametrize("workers", [0, -1, 2.0, True])
def test_rejects_bad_workers(conn_str, workers):
    with pytest.raises(ValueError, match="workers"):
        write_arrow(conn_str, "dbo.nope", _source(10), workers=workers)


def test_rejects_negative_batch_size(conn_str):
    with pytest.raises(ValueError, match="batch_size"):
        write_arrow(conn_str, "dbo.nope", _source(10), batch_size=-1)
