"""write_arrow(workers > 1) without pyarrow: the source is imported once through
the Arrow C stream and cut into slices in Rust.

Covers a pyarrow-free load (subprocess with ``import pyarrow`` blocked),
streaming sources that are pulled on demand, an error in one worker, and the
heap / nonclustered / clustered / clustered columnstore target policies.
"""

import os
import subprocess
import sys
import textwrap
import warnings

import pytest

from mssql_python import ddbc_bindings, write_arrow

pytestmark = pytest.mark.skipif(
    not getattr(ddbc_bindings, "__rust_port__", False),
    reason="write_arrow is a Rust-port feature",
)

N_ROWS = 30_000

_NO_PYARROW_SCRIPT = textwrap.dedent(
    """
    import json, os, sys
    sys.modules["pyarrow"] = None  # any `import pyarrow` now raises ImportError
    import mssql_python
    from mssql_python import write_arrow

    conn_str, table, n = os.environ["CONN"], os.environ["TABLE"], int(os.environ["N"])
    with mssql_python.connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        source = cur.execute(
            f"SELECT TOP ({n}) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS BIGINT) AS id, "
            "CONCAT('row-', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1) AS name "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b"
        ).fetchall_arrow(batch_size=4_000)
        assert type(source).__module__.split(".")[0] != "pyarrow"
        result = write_arrow(conn_str, table, source, workers=4)
    assert "pyarrow" not in {k for k, v in sys.modules.items() if v is not None}
    print(json.dumps(result))
    """
)


def _create_heap(cursor, table):
    cursor.execute(f"CREATE TABLE {table} (id BIGINT NOT NULL, name NVARCHAR(40) NULL)")
    cursor.connection.commit()


def _stats(cursor, table):
    cursor.execute(f"SELECT COUNT_BIG(*), SUM(id), MIN(id), MAX(id) FROM {table}")
    return tuple(cursor.fetchone())


def _table(n=N_ROWS):
    pa = pytest.importorskip("pyarrow")
    return pa.table(
        {"id": pa.array(range(n), type=pa.int64()), "name": pa.array([f"row-{i}" for i in range(n)])}
    )


def test_parallel_write_arrow_without_pyarrow(conn_str, cursor, table_name):
    import json

    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    env = {**os.environ, "CONN": conn_str, "TABLE": table, "N": str(N_ROWS)}
    done = subprocess.run(
        [sys.executable, "-c", _NO_PYARROW_SCRIPT], env=env, capture_output=True, text=True, timeout=300
    )
    assert done.returncode == 0, done.stderr
    result = json.loads(done.stdout.strip().splitlines()[-1])
    assert result["rows_copied"] == N_ROWS
    assert result["workers"] == 4
    assert _stats(cursor, table) == (N_ROWS, sum(range(N_ROWS)), 0, N_ROWS - 1)
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE name <> CONCAT('row-', id)")
    assert cursor.fetchone()[0] == 0


def test_polars_source(conn_str, cursor, table_name):
    pl = pytest.importorskip("polars")
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    frame = pl.DataFrame({"id": list(range(N_ROWS)), "name": [f"row-{i}" for i in range(N_ROWS)]})

    result = write_arrow(conn_str, table, frame, workers=4)

    assert result["rows_copied"] == N_ROWS
    assert _stats(cursor, table)[0] == N_ROWS


def test_streaming_source_is_pulled_on_demand(conn_str, cursor, table_name):
    pa = pytest.importorskip("pyarrow")
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    full = _table()
    produced = []

    def batches():
        for batch in full.to_batches(max_chunksize=500):
            produced.append(batch.num_rows)
            yield batch

    reader = pa.RecordBatchReader.from_batches(full.schema, batches())
    result = write_arrow(conn_str, table, reader, workers=4)

    assert result["rows_copied"] == N_ROWS
    assert sum(produced) == N_ROWS
    assert _stats(cursor, table)[0] == N_ROWS


def test_error_in_one_worker_stops_the_others(conn_str, cursor, table_name):
    pa = pytest.importorskip("pyarrow")
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    produced = []
    total = 2_000

    def batches():
        for i in range(total):
            ids = pa.array([None if i == 3 else i * 100 + k for k in range(100)], type=pa.int64())
            produced.append(i)
            yield pa.record_batch([ids, pa.array(["n"] * 100)], schema=schema)

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    with pytest.raises(Exception, match="(?i)null"):
        write_arrow(conn_str, table, reader, workers=4)

    assert len(produced) < total  # the source was not read to the end


def test_source_read_error_is_raised(conn_str, cursor, table_name):
    pa = pytest.importorskip("pyarrow")
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

    def batches():
        yield pa.record_batch([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
        raise ValueError("source exploded")

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    with pytest.raises(Exception, match="source exploded"):
        write_arrow(conn_str, table, reader, workers=3)


def test_heap_with_nonclustered_index_uses_one_worker(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    _create_heap(cursor, table)
    cursor.execute(f"CREATE NONCLUSTERED INDEX ix_{table_name} ON {table} (id)")
    cursor.connection.commit()

    with pytest.warns(RuntimeWarning, match="nonclustered"):
        result = write_arrow(conn_str, table, _table(5_000), workers=4)

    assert result["workers"] == 1
    assert _stats(cursor, table)[0] == 5_000


def test_clustered_index_uses_one_worker(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id BIGINT NOT NULL PRIMARY KEY CLUSTERED, name NVARCHAR(40) NULL)")
    cursor.connection.commit()

    with pytest.warns(RuntimeWarning, match="clustered index"):
        result = write_arrow(conn_str, table, _table(5_000), workers=4)

    assert result["workers"] == 1
    assert _stats(cursor, table)[0] == 5_000


def test_columnstore_loads_in_parallel_without_table_lock(conn_str, cursor, table_name):
    table = f"dbo.{table_name}"
    cursor.execute(f"CREATE TABLE {table} (id BIGINT NOT NULL, name NVARCHAR(40) NULL)")
    cursor.execute(f"CREATE CLUSTERED COLUMNSTORE INDEX cci_{table_name} ON {table}")
    cursor.connection.commit()

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        warnings.filterwarnings("ignore", message=".*recovery model.*")
        result = write_arrow(conn_str, table, _table(), workers=4)

    assert result["workers"] == 4
    assert result["rows_copied"] == N_ROWS
    assert set(result) >= {"rows_copied", "batch_count", "elapsed_time", "rows_per_second", "workers"}
    assert _stats(cursor, table) == (N_ROWS, sum(range(N_ROWS)), 0, N_ROWS - 1)
