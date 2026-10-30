"""Async-usage behaviour ported from FastMssql's async test suite, on the DB-API driver.

Adapted from Rivendael/FastMssql tests/test_async_advanced.py and tests/test_async_edge_cases.py
(MIT), Copyright (c) 2025 Rivendael. Permission notice reproduced from the upstream LICENSE:

    Permission is hereby granted, free of charge, to any person obtaining a copy of this
    software and associated documentation files (the "Software"), to deal in the Software
    without restriction, including without limitation the rights to use, copy, modify, merge,
    publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
    to whom the Software is furnished to do so, subject to the following conditions:
    The above copyright notice and this permission notice shall be included in all copies or
    substantial portions of the Software.
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED.

FastMssql's async API is its own; these tests keep the behaviours (concurrent connections,
per-task error isolation, non-blocking waits, rollback on failure, cancellation) and drive
them through mssql_python's synchronous DB-API, with each blocking call pushed onto a worker
thread via asyncio.to_thread. Every connection is used by one thread at a time.
"""

import asyncio
import datetime
import decimal
from concurrent.futures import ThreadPoolExecutor

import pytest
from mssql_python import connect
from mssql_python.exceptions import ProgrammingError


async def _open(conn_str, **kwargs):
    return await asyncio.to_thread(connect, conn_str, **kwargs)


async def _query(conn, sql, *params):
    def work():
        cur = conn.cursor()
        try:
            cur.execute(sql, *params)
            return [tuple(row) for row in cur.fetchall()]
        finally:
            cur.close()

    return await asyncio.to_thread(work)


async def _close(conn):
    await asyncio.to_thread(conn.close)


async def _worker_scalar(conn_str, n):
    conn = await _open(conn_str, autocommit=True)
    try:
        rows = await _query(conn, "SELECT ? AS worker, ? * 10 AS scaled", n, n)
        return rows[0]
    finally:
        await _close(conn)


@pytest.mark.asyncio
async def test_concurrent_connections_return_their_own_results(conn_str):
    results = await asyncio.gather(*(_worker_scalar(conn_str, n) for n in range(8)))

    assert results == [(n, n * 10) for n in range(8)]


@pytest.mark.asyncio
async def test_failure_in_one_task_does_not_affect_other_tasks(conn_str):
    async def bad(conn_str):
        conn = await _open(conn_str, autocommit=True)
        try:
            await _query(conn, "SELECT * FROM dbo.no_such_table_for_fastmssql_port")
        finally:
            await _close(conn)

    outcomes = await asyncio.gather(
        _worker_scalar(conn_str, 1), bad(conn_str), _worker_scalar(conn_str, 2), bad(conn_str),
        _worker_scalar(conn_str, 3), return_exceptions=True,
    )

    assert outcomes[0] == (1, 10)
    assert outcomes[2] == (2, 20)
    assert outcomes[4] == (3, 30)
    assert all(isinstance(o, ProgrammingError) for o in (outcomes[1], outcomes[3]))


@pytest.mark.asyncio
async def test_same_connection_recovers_after_async_syntax_error(conn_str):
    conn = await _open(conn_str, autocommit=True)
    try:
        with pytest.raises(ProgrammingError):
            await _query(conn, "INVALID ASYNC SQL STATEMENT")

        assert await _query(conn, "SELECT N'async recovery'") == [("async recovery",)]
    finally:
        await _close(conn)


@pytest.mark.asyncio
async def test_event_loop_keeps_running_during_blocking_query(conn_str):
    conn = await _open(conn_str, autocommit=True)
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.05)
            ticks += 1

    task = asyncio.create_task(ticker())
    try:
        rows = await _query(conn, "WAITFOR DELAY '00:00:01'; SELECT 1 AS done")
    finally:
        task.cancel()
        await _close(conn)

    assert rows == [(1,)]
    # About 20 ticks are expected over the one-second delay; a blocked loop would give 0 or 1.
    assert ticks >= 5


@pytest.mark.asyncio
async def test_failed_async_block_rolls_back_its_uncommitted_insert(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (id INT PRIMARY KEY, v NVARCHAR(20))")
    cursor.connection.commit()

    async def write_then_fail():
        conn = await _open(conn_str)
        try:
            def insert_then_raise():
                cur = conn.cursor()
                cur.execute(f"INSERT INTO dbo.{table_name} VALUES (?, ?)", 1, "uncommitted")
                raise RuntimeError("business rule failed")

            with pytest.raises(RuntimeError):
                await asyncio.to_thread(insert_then_raise)
            await asyncio.to_thread(conn.rollback)
        finally:
            await _close(conn)

    await write_then_fail()

    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    assert cursor.fetchone()[0] == 0


@pytest.mark.asyncio
async def test_concurrent_committed_inserts_are_all_visible(conn_str, cursor, table_name):
    cursor.execute(f"CREATE TABLE dbo.{table_name} (writer INT NOT NULL, seq INT NOT NULL)")
    cursor.connection.commit()

    async def writer(w):
        conn = await _open(conn_str)
        try:
            def work():
                cur = conn.cursor()
                cur.executemany(
                    f"INSERT INTO dbo.{table_name} (writer, seq) VALUES (?, ?)",
                    [(w, s) for s in range(50)],
                )
                conn.commit()

            await asyncio.to_thread(work)
        finally:
            await _close(conn)

    await asyncio.gather(*(writer(w) for w in range(6)))

    cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT CONCAT(writer, ':', seq)) FROM dbo.{table_name}")
    assert tuple(cursor.fetchone()) == (300, 300)


@pytest.mark.asyncio
async def test_row_by_row_fetch_in_worker_threads_keeps_order(conn_str):
    async def reader(_):
        conn = await _open(conn_str, autocommit=True)
        try:
            def fetch_all_one_by_one():
                cur = conn.cursor()
                cur.execute(
                    "SELECT TOP (?) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n "
                    "FROM sys.all_objects ORDER BY n",
                    300,
                )
                values = []
                while (row := cur.fetchone()) is not None:
                    values.append(row[0])
                return values

            return await asyncio.to_thread(fetch_all_one_by_one)
        finally:
            await _close(conn)

    results = await asyncio.gather(*(reader(i) for i in range(4)))

    assert all(values == list(range(1, 301)) for values in results)


@pytest.mark.asyncio
async def test_parameter_types_survive_concurrent_round_trips(conn_str):
    params = (decimal.Decimal("12.34"), None, b"\x00\xff", datetime.date(2024, 2, 29), "naïve 北京")

    async def roundtrip(_):
        conn = await _open(conn_str, autocommit=True)
        try:
            return (await _query(conn, "SELECT ?, ?, ?, ?, ?", *params))[0]
        finally:
            await _close(conn)

    results = await asyncio.gather(*(roundtrip(i) for i in range(6)))

    for row in results:
        assert [type(v) for v in row] == [decimal.Decimal, type(None), bytes, datetime.date, str]
        assert row[0] == decimal.Decimal("12.34")
        assert row[1] is None
        assert row[2] == b"\x00\xff"
        assert row[3] == datetime.date(2024, 2, 29)
        assert row[4] == "naïve 北京"


@pytest.mark.asyncio
async def test_cancelled_waiter_leaves_connection_usable_once_worker_finishes(conn_str):
    conn = await _open(conn_str, autocommit=True)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)

    def slow_query():
        cur = conn.cursor()
        cur.execute("WAITFOR DELAY '00:00:01'; SELECT 1")
        cur.close()

    try:
        waiter = loop.run_in_executor(executor, slow_query)
        await asyncio.sleep(0.05)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        # The worker thread cannot be interrupted; wait for it before reusing the connection.
        await asyncio.to_thread(executor.shutdown, True)

        assert await _query(conn, "SELECT 42") == [(42,)]
    finally:
        executor.shutdown(wait=True)
        await _close(conn)


@pytest.mark.asyncio
async def test_many_short_lived_connections_in_parallel(conn_str):
    async def one(i):
        conn = await _open(conn_str, autocommit=True)
        try:
            return (await _query(conn, "SELECT ?", i))[0][0]
        finally:
            await _close(conn)

    results = await asyncio.gather(*(one(i) for i in range(20)))

    assert results == list(range(20))


@pytest.mark.asyncio
async def test_async_wrapper_surfaces_driver_error_type(conn_str):
    conn = await _open(conn_str, autocommit=True)
    try:
        with pytest.raises(ProgrammingError):
            await _query(conn, "SELECT 1/ FROM")
    finally:
        await _close(conn)
