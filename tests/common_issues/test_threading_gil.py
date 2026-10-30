"""Threading, GIL release and thread-safety contract for the driver.

Covers the patterns users hit most with threaded workloads:

* one connection per thread, many threads at once (correct results, distinct
  server sessions, no cross-talk, concurrent connect works);
* the GIL is released while a statement blocks on the server - two WAITFORs
  on separate threads overlap, so wall time is well below the serial sum, and
  the main thread keeps making progress while a worker is blocked;
* one connection shared across threads behind a ``threading.Lock`` produces
  exact results and no lost or misrouted rows;
* exceptions, rollbacks and per-connection state (autocommit, #temp tables)
  stay isolated to the thread that caused them;
* the DB-API module attributes (``threadsafety`` level 1, ``apilevel`` 2.0).

Connections are opened inside each worker from ``conn_str`` (never the
``conn`` fixture, which belongs to the main thread). Tests that need real
overlap use a ``threading.Barrier`` so every worker holds its connection open
before any of them starts working.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import mssql_python
from mssql_python import DataError, ProgrammingError, connect

N_WORKERS = 8
WAIT_SECONDS = 1
BARRIER_TIMEOUT = 30


def _barrier(parties):
    return threading.Barrier(parties, timeout=BARRIER_TIMEOUT)


# ---------------------------------------------------------------------------
# DB-API module contract
# ---------------------------------------------------------------------------


def test_threadsafety_is_level_one():
    """DB-API level 1: threads may share the module, not connections."""
    assert mssql_python.threadsafety == 1


def test_apilevel_is_2_0():
    assert mssql_python.apilevel == "2.0"


# ---------------------------------------------------------------------------
# One connection per thread
# ---------------------------------------------------------------------------


def test_eight_threads_each_own_connection_return_exact_results(conn_str):
    """Each thread connects, runs a parameterized query and gets its own answer."""

    def worker(i):
        c = connect(conn_str)
        try:
            cur = c.cursor()
            cur.execute("SELECT ? AS tid, ? * ? AS square", i, i, i)
            row = cur.fetchone()
            cur.close()
            return (row[0], row[1], type(row[1]))
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        results = list(pool.map(worker, range(N_WORKERS)))

    assert results == [(i, i * i, int) for i in range(N_WORKERS)]


def test_concurrent_connections_have_distinct_server_sessions(conn_str):
    """Connections held open at the same time get distinct SQL Server SPIDs."""
    barrier = _barrier(N_WORKERS)

    def worker(_):
        c = connect(conn_str)
        try:
            spid = c.cursor().execute("SELECT @@SPID").fetchone()[0]
            barrier.wait()  # every worker holds its connection before any closes
            return spid
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        spids = list(pool.map(worker, range(N_WORKERS)))

    assert len(spids) == N_WORKERS
    assert len(set(spids)) == N_WORKERS
    assert all(isinstance(s, int) and s > 0 for s in spids)


def test_simultaneous_connect_from_barrier_all_succeed(conn_str):
    """Eight threads calling connect() at the same instant all get a usable connection."""
    barrier = _barrier(N_WORKERS)

    def worker(i):
        barrier.wait()
        c = connect(conn_str)
        try:
            return c.cursor().execute("SELECT ?", i).fetchone()[0]
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        values = list(pool.map(worker, range(N_WORKERS)))

    assert values == list(range(N_WORKERS))


def test_temp_tables_are_private_to_each_thread_connection(conn_str):
    """A #temp table is per session: each thread only sees the row it inserted."""
    barrier = _barrier(N_WORKERS)

    def worker(i):
        c = connect(conn_str)
        try:
            cur = c.cursor()
            cur.execute("DROP TABLE IF EXISTS #per_thread")
            cur.execute("CREATE TABLE #per_thread (tid INT NOT NULL)")
            cur.execute("INSERT INTO #per_thread (tid) VALUES (?)", i)
            barrier.wait()  # all threads have inserted before anyone reads
            cur.execute("SELECT tid FROM #per_thread")
            rows = [tuple(r) for r in cur.fetchall()]
            cur.close()
            return rows
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        per_thread_rows = list(pool.map(worker, range(N_WORKERS)))

    assert per_thread_rows == [[(i,)] for i in range(N_WORKERS)]


def test_threadpool_map_preserves_input_order_with_per_task_connections(conn_str):
    """The common pool.map(query, items) pattern returns results in input order."""
    items = list(range(40))

    def task(n):
        c = connect(conn_str)
        try:
            return c.cursor().execute("SELECT ? * 3", n).fetchone()[0]
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        results = list(pool.map(task, items))

    assert results == [n * 3 for n in items]


def test_large_fetchall_in_parallel_threads_is_exact(conn_str):
    """Eight threads each fetch 2000 rows; counts and sums are exact, no rows lost."""
    n_rows = 2000
    sql = (
        "SELECT TOP (?) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n "
        "FROM sys.all_objects a CROSS JOIN sys.all_objects b"
    )

    def worker(_):
        c = connect(conn_str)
        try:
            cur = c.cursor()
            cur.execute(sql, n_rows)
            rows = cur.fetchall()
            cur.close()
            return len(rows), sum(r[0] for r in rows)
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        results = list(pool.map(worker, range(N_WORKERS)))

    expected = (n_rows, n_rows * (n_rows + 1) // 2)
    assert results == [expected] * N_WORKERS


def test_concurrent_inserts_per_thread_connections_commit_all_rows(conn, conn_str, table_name):
    """Eight writers, one connection each, 25 rows each: exactly 200 rows, 25 per writer."""
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table_name} (tid INT NOT NULL, seq INT NOT NULL)")
    conn.commit()
    barrier = _barrier(N_WORKERS)

    def writer(i):
        c = connect(conn_str)
        try:
            wcur = c.cursor()
            barrier.wait()
            for seq in range(25):
                wcur.execute(f"INSERT INTO dbo.{table_name} (tid, seq) VALUES (?, ?)", i, seq)
            c.commit()
            wcur.close()
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        for f in [pool.submit(writer, i) for i in range(N_WORKERS)]:
            f.result()

    cur.execute(f"SELECT tid, COUNT(*) FROM dbo.{table_name} GROUP BY tid ORDER BY tid")
    assert [tuple(r) for r in cur.fetchall()] == [(i, 25) for i in range(N_WORKERS)]
    cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cur.fetchone()[0] == N_WORKERS * 25


def test_rollback_in_one_thread_does_not_undo_another_threads_commit(conn, conn_str, table_name):
    """Per-connection transactions are independent across threads."""
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table_name} (who NVARCHAR(10) NOT NULL)")
    conn.commit()
    barrier = _barrier(2)

    def rollback_writer():
        c = connect(conn_str)
        try:
            wc = c.cursor()
            wc.execute(f"INSERT INTO dbo.{table_name} (who) VALUES (?)", "rolled")
            barrier.wait()
            c.rollback()
        finally:
            c.close()

    def commit_writer():
        c = connect(conn_str)
        try:
            wc = c.cursor()
            wc.execute(f"INSERT INTO dbo.{table_name} (who) VALUES (?)", "kept")
            barrier.wait()
            c.commit()
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(rollback_writer)
        fb = pool.submit(commit_writer)
        fa.result()
        fb.result()

    cur.execute(f"SELECT who FROM dbo.{table_name}")
    assert [tuple(r) for r in cur.fetchall()] == [("kept",)]


# ---------------------------------------------------------------------------
# GIL release during server-side blocking
# ---------------------------------------------------------------------------


def _waitfor_worker(conn_str, barrier):
    c = connect(conn_str)
    try:
        cur = c.cursor()
        barrier.wait()
        t0 = time.perf_counter()
        cur.execute(f"WAITFOR DELAY '00:00:0{WAIT_SECONDS}'")
        elapsed = time.perf_counter() - t0
        cur.close()
        return elapsed
    finally:
        c.close()


def test_parallel_waitfor_wall_time_is_well_below_serial_sum(conn_str):
    """
    Eight 1-second WAITFORs on eight connections overlap. If the GIL (or any
    driver lock) were held across SQLExecDirect, the wall time would approach
    the serial sum of ~8 seconds.
    """
    barrier = _barrier(N_WORKERS + 1)  # + main thread, so timing starts at release

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = [pool.submit(_waitfor_worker, conn_str, barrier) for _ in range(N_WORKERS)]
        barrier.wait()
        t0 = time.perf_counter()
        elapsed = [f.result() for f in futures]
        wall = time.perf_counter() - t0

    serial_sum = sum(elapsed)
    assert min(elapsed) >= WAIT_SECONDS * 0.9, "WAITFOR returned before its delay"
    assert serial_sum >= N_WORKERS * WAIT_SECONDS * 0.9
    assert wall < serial_sum / 2, (
        f"wall={wall:.2f}s serial_sum={serial_sum:.2f}s: WAITFORs did not overlap"
    )


def test_main_thread_query_runs_while_worker_is_blocked_in_waitfor(conn_str):
    """
    While a worker sits in a 3-second WAITFOR, the main thread completes an
    independent query on its own connection and the worker is still running.
    """
    started = threading.Event()

    def blocked_worker():
        c = connect(conn_str)
        try:
            cur = c.cursor()
            started.set()
            cur.execute("WAITFOR DELAY '00:00:03'")
            cur.close()
            return "done"
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(blocked_worker)
        assert started.wait(timeout=BARRIER_TIMEOUT)

        main_conn = connect(conn_str)
        try:
            t0 = time.perf_counter()
            value = main_conn.cursor().execute("SELECT 42").fetchone()[0]
            main_elapsed = time.perf_counter() - t0
            still_blocked = not fut.done()
        finally:
            main_conn.close()

        assert value == 42
        assert still_blocked, "worker finished before the main query completed"
        assert main_elapsed < 2.0, f"main query took {main_elapsed:.2f}s while worker waited"
        assert fut.result(timeout=BARRIER_TIMEOUT) == "done"


# ---------------------------------------------------------------------------
# One connection shared across threads, guarded by a lock
# ---------------------------------------------------------------------------


def test_shared_connection_with_lock_returns_no_cross_talk(conn_str):
    """Threads sharing one connection under a lock each get exactly their own rows."""
    conn = connect(conn_str)
    lock = threading.Lock()
    per_iter = 5

    def worker(i):
        out = []
        for seq in range(per_iter):
            with lock:
                cur = conn.cursor()
                cur.execute("SELECT ? AS tid, ? AS seq", i, seq)
                out.append(tuple(cur.fetchone()))
                cur.close()
        return out

    try:
        with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
            chunks = list(pool.map(worker, range(N_WORKERS)))
    finally:
        conn.close()

    results = sorted(row for chunk in chunks for row in chunk)
    expected = sorted((i, seq) for i in range(N_WORKERS) for seq in range(per_iter))
    assert results == expected


def test_shared_connection_with_lock_concurrent_inserts_are_all_kept(conn, table_name, conn_str):
    """A single shared connection, serialized by a lock, loses no inserted rows."""
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table_name} (tid INT NOT NULL, seq INT NOT NULL)")
    conn.commit()

    shared = connect(conn_str)
    lock = threading.Lock()
    per_thread = 10

    def writer(i):
        for seq in range(per_thread):
            with lock:
                wc = shared.cursor()
                wc.execute(f"INSERT INTO dbo.{table_name} (tid, seq) VALUES (?, ?)", i, seq)
                wc.close()

    try:
        with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
            for f in [pool.submit(writer, i) for i in range(N_WORKERS)]:
                f.result()
        with lock:
            shared.commit()
    finally:
        shared.close()

    cur.execute(f"SELECT tid, COUNT(*), MIN(seq), MAX(seq) FROM dbo.{table_name} GROUP BY tid ORDER BY tid")
    assert [tuple(r) for r in cur.fetchall()] == [(i, per_thread, 0, per_thread - 1) for i in range(N_WORKERS)]


# ---------------------------------------------------------------------------
# Failure isolation and per-connection state
# ---------------------------------------------------------------------------


def test_error_in_one_thread_does_not_affect_others(conn_str):
    """One worker's DataError is raised to that worker only; siblings still succeed."""

    def worker(i):
        c = connect(conn_str)
        try:
            cur = c.cursor()
            if i == 3:
                cur.execute("SELECT 1/0")
            return cur.execute("SELECT ?", i).fetchone()[0]
        finally:
            c.close()

    outcomes = []
    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = [pool.submit(worker, i) for i in range(N_WORKERS)]
        for f in futures:
            try:
                outcomes.append(("ok", f.result()))
            except Exception as exc:  # noqa: BLE001 - class is asserted below
                outcomes.append(("err", type(exc)))

    expected = [("ok", i) if i != 3 else ("err", DataError) for i in range(N_WORKERS)]
    assert outcomes == expected


def test_syntax_error_in_worker_raises_programming_error(conn_str):
    def worker():
        c = connect(conn_str)
        try:
            c.cursor().execute("SELEC T bogus")
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(ProgrammingError):
            pool.submit(worker).result()


def test_autocommit_is_independent_per_connection_across_threads(conn_str):
    """Setting autocommit on one thread's connection does not change another's."""
    barrier = _barrier(2)

    def worker(set_autocommit):
        c = connect(conn_str)
        try:
            if set_autocommit:
                c.autocommit = True
            barrier.wait()
            return c.autocommit
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        fa = pool.submit(worker, True)
        fb = pool.submit(worker, False)
        assert fa.result() is True
        assert fb.result() is False


def test_cursor_used_after_connection_closed_from_another_thread_raises(conn_str):
    conn = connect(conn_str)
    cur = conn.cursor()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(conn.close).result()

    with pytest.raises(ProgrammingError, match="closed"):
        cur.execute("SELECT 1")
