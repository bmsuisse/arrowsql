"""Parallel row-tuple bulk load (mssql_python.write_rows) and target detection.

Each worker runs its own cursor.bulkcopy with TABLOCK into a heap. The tests
check row counts and checksums against the single-stream result for list and
generator inputs, error propagation, the non-heap fallbacks and NCI detection.
"""

import warnings

import pytest

from mssql_python import bulk_parallel, ddbc_bindings, write_arrow, write_rows

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="write_rows is a Rust-port feature",
)

N_ROWS = 30_000


def _rows(n=N_ROWS):
    return [(i, f"row-{i}", i * 0.5) for i in range(n)]


def _create(cursor, table, index=None):
    cursor.execute(f"CREATE TABLE {table} (id BIGINT NOT NULL, name NVARCHAR(40) NULL, v FLOAT NULL)")
    if index:
        cursor.execute(index.format(table=table))
    cursor.connection.commit()


def _stats(cursor, table):
    cursor.execute(
        f"SELECT COUNT_BIG(*), SUM(id), MIN(id), MAX(id), "
        f"CHECKSUM_AGG(CHECKSUM(id, name, v)) FROM {table}"
    )
    return tuple(cursor.fetchone())


def _single_stream_stats(conn_str, cursor, table_name):
    table = f"dbo.{table_name}_ref"
    _create(cursor, table)
    try:
        cursor.connection.cursor().bulkcopy(table, _rows(), table_lock=True)
        return _stats(cursor, table)
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.connection.commit()


@pytest.mark.parametrize("workers", [1, 2, 4])
@pytest.mark.parametrize("kind", ["list", "generator"])
def test_matches_single_stream(conn_str, cursor, table_name, workers, kind):
    expected = _single_stream_stats(conn_str, cursor, table_name)
    table = f"dbo.{table_name}"
    _create(cursor, table)
    source = _rows() if kind == "list" else (row for row in _rows())

    result = write_rows(conn_str, table, source, workers=workers)

    assert result["rows_copied"] == N_ROWS
    assert 1 <= result["workers"] <= workers
    assert _stats(cursor, table) == expected


def test_empty_input(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create(cursor, table)

    result = write_rows(conn_str, table, [], workers=4)

    assert result["rows_copied"] == 0
    assert _stats(cursor, table)[0] == 0


def test_error_in_one_worker_raises_and_stops(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create(cursor, table)
    rows = _rows(200_000)
    rows[150_000] = ("not a number", "x", 1.0)

    with pytest.raises(Exception):
        write_rows(conn_str, table, rows, workers=4)

    cursor.execute(f"SELECT COUNT_BIG(*) FROM {table}")
    assert cursor.fetchone()[0] < len(rows)


def test_generator_error_raises(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create(cursor, table)

    def broken():
        yield from _rows(1_000)
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        write_rows(conn_str, table, broken(), workers=2)


@pytest.mark.parametrize(
    "index, reason",
    [
        ("CREATE CLUSTERED INDEX ix ON {table} (id)", "clustered index"),
        ("CREATE NONCLUSTERED INDEX ix ON {table} (id, name)", "nonclustered"),
    ],
)
@pytest.mark.parametrize("loader", ["rows", "arrow"])
def test_non_heap_targets_use_one_worker(conn_str, cursor, table_name, index, reason, loader):
    table = f"dbo.{table_name}"
    _create(cursor, table, index)

    with pytest.warns(RuntimeWarning, match=reason):
        if loader == "rows":
            result = write_rows(conn_str, table, _rows(5_000), workers=4)
        else:
            pa = pytest.importorskip("pyarrow")
            source = pa.table(
                {
                    "id": pa.array(range(5_000), pa.int64()),
                    "name": pa.array([f"row-{i}" for i in range(5_000)]),
                    "v": pa.array([i * 0.5 for i in range(5_000)]),
                }
            )
            result = write_arrow(conn_str, table, source, workers=4)

    assert result["workers"] == 1
    assert result["rows_copied"] == 5_000
    assert _stats(cursor, table)[0] == 5_000


def test_pure_heap_does_not_warn(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create(cursor, table)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = write_rows(conn_str, table, _rows(5_000), workers=2)

    assert result["workers"] == 2
    assert not [w for w in caught if "1 worker" in str(w.message)]


@pytest.mark.parametrize(
    "index, kind",
    [
        (None, "heap"),
        ("CREATE NONCLUSTERED INDEX ix ON {table} (id)", "heap_nci"),
        ("CREATE CLUSTERED INDEX ix ON {table} (id)", "clustered"),
        ("CREATE CLUSTERED COLUMNSTORE INDEX ix ON {table}", "columnstore"),
        ("CREATE NONCLUSTERED COLUMNSTORE INDEX ix ON {table} (id)", "heap_nci"),
    ],
)
def test_target_query_kinds(cursor, table_name, index, kind):
    table = f"dbo.{table_name}"
    _create(cursor, table, index)

    cursor.execute(bulk_parallel._TARGET_QUERY, (table, table, table, table))
    found, recovery_model = cursor.fetchone()

    assert found == kind
    assert recovery_model in ("SIMPLE", "FULL", "BULK_LOGGED")


def test_target_query_missing_table(cursor):
    cursor.execute(bulk_parallel._TARGET_QUERY, ("dbo.no_such_table_xyz",) * 4)
    assert cursor.fetchone()[0] is None


@pytest.mark.parametrize("workers", [0, -1, True, 2.5])
def test_rejects_bad_workers(conn_str, workers):
    with pytest.raises(ValueError):
        write_rows(conn_str, "dbo.whatever", [], workers=workers)


def test_row_slices_cover_input_once():
    rows = _rows(100_000)
    pieces = list(bulk_parallel._row_slices(rows, 4))
    assert sum(map(len, pieces)) == len(rows)
    assert [r for p in pieces for r in p] == rows
    assert all(len(p) <= bulk_parallel._MAX_SLICE_ROWS for p in pieces)
    assert len(pieces) >= 4 * 4

    gen_pieces = list(bulk_parallel._row_slices((r for r in rows), 4))
    assert [r for p in gen_pieces for r in p] == rows


def _record_bulk_calls(monkeypatch):
    """Record (table_lock, batch_size) of every bulkcopy / bulkcopy_arrow call."""
    from mssql_python.cursor import Cursor

    calls = []
    for name in ("bulkcopy", "bulkcopy_arrow"):
        real = getattr(Cursor, name)

        def wrapper(self, *args, _real=real, **kwargs):
            calls.append((kwargs.get("table_lock"), kwargs.get("batch_size")))
            return _real(self, *args, **kwargs)

        monkeypatch.setattr(Cursor, name, wrapper)

    # The parallel Arrow load is one native call that takes every worker's handle.
    real_parallel = ddbc_bindings.DDBCBulkCopyArrowParallel

    def parallel(handles, *args, **kwargs):
        calls.extend((kwargs.get("table_lock"), kwargs.get("batch_size")) for _ in handles)
        return real_parallel(handles, *args, **kwargs)

    monkeypatch.setattr(ddbc_bindings, "DDBCBulkCopyArrowParallel", parallel)
    return calls


@pytest.mark.parametrize("loader", ["rows", "arrow"])
@pytest.mark.parametrize("batch_size", [0, 5_000, 200_000])
def test_columnstore_loads_in_parallel_without_tablock(
    conn_str, cursor, table_name, monkeypatch, loader, batch_size
):
    table = f"dbo.{table_name}"
    n = 500_000
    _create(cursor, table, "CREATE CLUSTERED COLUMNSTORE INDEX ix ON {table}")
    calls = _record_bulk_calls(monkeypatch)
    rows = _rows(n)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        warnings.filterwarnings("ignore", message=".*recovery model.*")
        if loader == "rows":
            result = write_rows(conn_str, table, rows, workers=2, batch_size=batch_size)
        else:
            pa = pytest.importorskip("pyarrow")
            ids, names, values = zip(*rows)
            source = pa.table({"id": pa.array(ids, pa.int64()), "name": list(names), "v": list(values)})
            result = write_arrow(conn_str, table, source, workers=2, batch_size=batch_size)

    assert result["workers"] == 2
    assert result["rows_copied"] == n
    assert len(calls) == 2
    assert all(lock is False for lock, _ in calls)
    assert all(batch >= 102_400 for _, batch in calls)
    assert max(batch for _, batch in calls) == max(batch_size, 102_400)
    stats = _stats(cursor, table)
    assert stats[0] == n and stats[2:4] == (0, n - 1)
    cursor.execute(
        "SELECT COUNT(*) FROM sys.dm_db_column_store_row_group_physical_stats "
        f"WHERE object_id = OBJECT_ID('{table}') AND state_desc = 'COMPRESSED'"
    )
    assert cursor.fetchone()[0] >= 1  # full batches compressed directly


def test_heap_keeps_tablock_and_batch_size(conn_str, cursor, table_name, monkeypatch):
    table = f"dbo.{table_name}"
    _create(cursor, table)
    calls = _record_bulk_calls(monkeypatch)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*recovery model.*")
        write_rows(conn_str, table, _rows(10_000), workers=2, batch_size=1_000)
    assert calls and all(call == (True, 1_000) for call in calls)
