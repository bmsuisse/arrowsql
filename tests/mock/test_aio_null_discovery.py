"""aio: the NULL-parameter type discovery (sp_describe_undeclared_parameters)
obeys the query timeout and cancellation instead of swallowing them."""

import asyncio
import sys
import time

import pytest

aio = pytest.importorskip("mssql_python.aio")
pytest.importorskip("pytest_asyncio")

from mssql_python import ddbc_bindings as _ddbc  # noqa: E402

pytestmark = pytest.mark.skipif(not hasattr(_ddbc, "AioHub"), reason="aio is Unix-only")

import mssql_python  # noqa: E402

DESCRIBE = "EXEC sp_describe_undeclared_parameters @tsql = N'SELECT @P1'"


@pytest.mark.asyncio
async def test_describe_timeout_fails_the_statement(mock):
    mock.register(DESCRIBE, [("i", "int")], [[1]], delay_ms=4000)
    mock.register("SELECT @P1", [("i", "int")], [[7]])
    conn = await aio.connect(mock.conn_str(), autocommit=True, timeout=1)
    try:
        start = time.monotonic()
        with pytest.raises(mssql_python.Error):
            await conn.fetch("SELECT ?", None)
        assert 0.8 < time.monotonic() - start < 3.5  # the describe timed out, not an unmatched query
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_describe_is_cancellable(mock):
    mock.register(DESCRIBE, [("i", "int")], [[1]], delay_ms=10_000)
    mock.register("SELECT @P1", [("i", "int")], [[7]])
    conn = await aio.connect(mock.conn_str(), autocommit=True)
    try:
        start = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(conn.fetch("SELECT ?", None), 0.5)
        assert time.monotonic() - start < 3
    finally:
        await conn.close()
