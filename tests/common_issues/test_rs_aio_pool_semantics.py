"""aio pool semantics (Rust port): checkin commit/rollback, cancellable connect,
idle / lifetime / acquire timeouts, checkout validation, non-Unix error.

Port-only: upstream mssql-python has no ``aio`` module.
"""

import asyncio
import time
import uuid

import pytest

aio = pytest.importorskip("mssql_python.aio")
pytest_asyncio = pytest.importorskip("pytest_asyncio")

import mssql_python  # noqa: E402
from mssql_python import ddbc_bindings  # noqa: E402

CONNECTION_ID = "SELECT CONVERT(varchar(64), connection_id) FROM sys.dm_exec_connections WHERE session_id = @@SPID"


@pytest_asyncio.fixture
async def table(conn_str):
    name = f"ci_pool_{uuid.uuid4().hex[:10]}"
    admin = await aio.connect(conn_str, autocommit=True)
    await admin.execute(f"CREATE TABLE dbo.{name} (id INT)")
    yield name
    await admin.execute(f"DROP TABLE IF EXISTS dbo.{name}")
    await admin.close()


async def _count(conn_str, table):
    reader = await aio.connect(conn_str, autocommit=True)
    try:
        return await reader.fetchval(f"SELECT COUNT(*) FROM dbo.{table}")
    finally:
        await reader.close()


# ---- (a) checkin semantics follow AsyncConnection


@pytest.mark.asyncio
async def test_acquire_commits_on_clean_exit(conn_str, table):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=2, autocommit=False)
    try:
        async with pool.acquire() as conn:
            assert not conn.autocommit
            await conn.execute(f"INSERT INTO dbo.{table} VALUES (1)")
        assert await _count(conn_str, table) == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_acquire_rolls_back_on_exception(conn_str, table):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=2, autocommit=False)
    try:
        with pytest.raises(RuntimeError):
            async with pool.acquire() as conn:
                await conn.execute(f"INSERT INTO dbo.{table} VALUES (1)")
                raise RuntimeError("boom")
        assert await _count(conn_str, table) == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_acquire_autocommit_pool_is_unchanged(conn_str, table):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"INSERT INTO dbo.{table} VALUES (1)")
        assert await _count(conn_str, table) == 1
        assert pool.in_use == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_plain_close_of_acquired_connection_rolls_back(conn_str, table):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=2, autocommit=False)
    try:
        conn = await pool.acquire()
        await conn.execute(f"INSERT INTO dbo.{table} VALUES (1)")
        await conn.close()
        assert await _count(conn_str, table) == 0
    finally:
        await pool.close()


# ---- (b) connect is cancellable


async def _silent_server():
    """Accepts TCP connections and never answers; records when clients hang up."""
    state = {"accepted": 0, "closed": asyncio.Event()}

    async def handle(reader, writer):
        state["accepted"] += 1
        try:
            while await reader.read(65536):
                pass
        except ConnectionError:
            pass
        state["closed"].set()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1], state


def _silent_conn_str(port):
    return f"Server=tcp:127.0.0.1,{port};UID=sa;PWD=x;Encrypt=no;TrustServerCertificate=yes;Connection Timeout=60"


@pytest.mark.asyncio
async def test_wait_for_connect_abandons_the_login():
    server, port, state = await _silent_server()
    try:
        start = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(aio.connect(_silent_conn_str(port)), 0.5)
        assert time.monotonic() - start < 3
        assert state["accepted"] == 1
        # the abandoned open closes its socket instead of waiting for the login timeout
        await asyncio.wait_for(state["closed"].wait(), 5)
    finally:
        server.close()


@pytest.mark.asyncio
async def test_wait_for_create_pool_abandons_the_logins():
    server, port, state = await _silent_server()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(aio.create_pool(_silent_conn_str(port), min_size=2, max_size=2), 0.5)
        await asyncio.wait_for(state["closed"].wait(), 5)
    finally:
        server.close()


@pytest.mark.asyncio
async def test_cancelled_connect_task_leaves_the_loop_usable(conn_str):
    server, port, _ = await _silent_server()
    try:
        task = asyncio.ensure_future(aio.connect(_silent_conn_str(port)))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        server.close()
    conn = await aio.connect(conn_str, autocommit=True)
    assert await conn.fetchval("SELECT 1") == 1
    await conn.close()


# ---- (c) pool lifetimes, acquire timeout, validation


async def _connection_id(pool):
    return await pool.fetchval(CONNECTION_ID)


@pytest.mark.asyncio
async def test_idle_sessions_expire(conn_str):
    pool = await aio.create_pool(
        conn_str, min_size=1, max_size=2, max_inactive_connection_lifetime=0.3
    )
    try:
        first = await _connection_id(pool)
        assert await _connection_id(pool) == first  # reused while fresh
        await asyncio.sleep(0.6)
        assert await _connection_id(pool) != first
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_idle_sessions_are_reaped_on_return(conn_str):
    pool = await aio.create_pool(
        conn_str, min_size=3, max_size=3, max_inactive_connection_lifetime=0.3
    )
    try:
        assert pool.idle == 3
        await asyncio.sleep(0.6)
        await pool.fetchval("SELECT 1")  # stale sessions are closed, one is opened and returned
        assert pool.idle == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_max_lifetime_retires_sessions(conn_str):
    pool = await aio.create_pool(
        conn_str, min_size=1, max_size=1, max_inactive_connection_lifetime=0, max_lifetime=0.4
    )
    try:
        first = await _connection_id(pool)
        assert await _connection_id(pool) == first
        await asyncio.sleep(0.6)
        assert await _connection_id(pool) != first  # too old: closed on return or at checkout
        assert pool.idle == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_acquire_timeout_per_call(conn_str):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=1)
    try:
        async with pool.acquire():
            start = time.monotonic()
            with pytest.raises(asyncio.TimeoutError):
                async with pool.acquire(timeout=0.3):
                    pass
            assert 0.2 < time.monotonic() - start < 3
        # the slot is free again
        async with pool.acquire(timeout=5) as conn:
            assert await conn.fetchval("SELECT 1") == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_acquire_timeout_pool_default_covers_shortcuts(conn_str):
    pool = await aio.create_pool(conn_str, min_size=1, max_size=1, acquire_timeout=0.3)
    try:
        assert pool.acquire_timeout == pytest.approx(0.3)
        async with pool.acquire():
            with pytest.raises(asyncio.TimeoutError):
                await pool.fetchval("SELECT 1")
        assert await pool.fetchval("SELECT 1") == 1
        pool.acquire_timeout = 0
        assert pool.acquire_timeout == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_invalid_pool_options_are_rejected(conn_str):
    with pytest.raises(ValueError):
        await aio.create_pool(conn_str, acquire_timeout=-1)
    with pytest.raises(ValueError):
        await aio.create_pool(conn_str, max_lifetime=float("nan"))


@pytest.mark.asyncio
async def test_dead_idle_session_is_replaced_at_checkout(conn_str):
    pool = await aio.create_pool(
        conn_str, min_size=1, max_size=1, max_inactive_connection_lifetime=0, validate_after=0.1
    )
    admin = await aio.connect(conn_str, autocommit=True)
    try:
        spid, first = (await pool.fetchrow(f"SELECT @@SPID, ({CONNECTION_ID})"))
        await admin.execute(f"KILL {int(spid)}")
        await asyncio.sleep(0.3)
        assert await pool.fetchval("SELECT 7") == 7
        assert await _connection_id(pool) != first
    finally:
        await admin.close()
        await pool.close()


# ---- (d) non-Unix platforms


@pytest.mark.asyncio
async def test_connect_and_create_pool_raise_not_supported_without_native_aio(monkeypatch, conn_str):
    monkeypatch.delattr(ddbc_bindings, "AioHub")
    with pytest.raises(mssql_python.NotSupportedError, match="Unix"):
        await aio.connect(conn_str)
    with pytest.raises(mssql_python.NotSupportedError, match="Unix"):
        await aio.create_pool(conn_str)
