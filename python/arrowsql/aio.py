"""Native asyncio API on the Rust core.

    from arrowsql import aio

    async with await aio.connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT id, name FROM t WHERE id > ?", 10)
        rows = await cur.fetchall()

    pool = await aio.create_pool(conn_str, min_size=2, max_size=16)
    rows = await pool.fetch("SELECT * FROM t WHERE id = ?", 42)

Every operation runs on the Rust core's tokio runtime with the GIL released;
results come back to the event loop through one completion hub per loop.
Cancelling the awaiting task (``task.cancel()``, ``asyncio.wait_for``
timeouts) cancels the query on the server too. See docs/ASYNC.md.

Unix only (the hub needs ``loop.add_reader``): on other platforms the module
imports, but ``connect`` and ``create_pool`` raise ``NotSupportedError``.
"""

import asyncio
import weakref
from typing import Any, Optional, Sequence

from arrowsql import ddbc_bindings as _core
from arrowsql.constants import ConstantsDDBC as _const
from arrowsql.cursor import Cursor as _SyncCursor
from arrowsql.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from arrowsql.helpers import get_settings

__all__ = ["connect", "create_pool", "AsyncConnection", "AsyncCursor", "Pool"]

_hubs: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any]" = weakref.WeakKeyDictionary()
_SQL_NULLABLE = _const.SQL_NULLABLE.value
_map_type = _SyncCursor._map_data_type


class _Shape:
    """Description and column maps of one result shape (cached by the hub)."""

    __slots__ = ("description", "column_map", "column_map_lower", "column_names")

    def __init__(self, columns):
        lowercase = get_settings().lowercase
        description = []
        names = []
        for name, sql_type, size, digits, nullable in columns:
            if lowercase:
                name = name.lower()
            names.append(name)
            description.append(
                (name, _map_type(None, sql_type), None, size, size, digits, nullable == _SQL_NULLABLE)
            )
        self.description = description
        self.column_names = tuple(names)
        self.column_map = {name: i for i, name in enumerate(names)}
        self.column_map_lower = (
            None if lowercase else {name.lower(): i for i, name in enumerate(names)}
        )


def _require_native() -> None:
    # RUST-PORT: the native hub is compiled for Unix only (src/lib.rs).
    if not hasattr(_core, "AioHub"):
        message = "arrowsql.aio is only available on Unix platforms (Linux, macOS)"
        raise NotSupportedError(driver_error=message, ddbc_error=message)


def _hub():
    _require_native()
    loop = asyncio.get_running_loop()
    hub = _hubs.get(loop)
    if hub is None:
        # The hub holds the loop weakly: the loop owns the hub through add_reader.
        hub = _core.AioHub(weakref.ref(loop), _Shape)
        loop.add_reader(hub.fileno(), hub.drain)
        _hubs[loop] = hub
    return hub


async def _wait(conn, op):
    """Await `op` = (future, request id); cancel the request on cancellation."""
    future, request = op
    try:
        return await future
    except asyncio.CancelledError:
        conn.cancel(request)
        raise


def _params(params: Sequence[Any]):
    """execute(sql, a, b) and execute(sql, (a, b)) both work."""
    if len(params) == 1 and isinstance(params[0], (list, tuple)):
        return params[0]
    return params


class AsyncCursor:
    """DB-API style cursor; every method that talks to the server is a coroutine."""

    __slots__ = (
        "_conn", "_c", "_id", "_batch", "_rows", "_pos", "_done",
        "description", "rowcount", "messages", "arraysize", "_closed",
    )

    def __init__(self, connection: "AsyncConnection"):
        self._conn = connection
        self._c = connection._c
        self._id = self._c.cursor()
        self._batch = None  # rows read ahead by execute (converted lazily)
        self._rows = []  # rows fetched by fetchone in chunks
        self._pos = 0
        self._done = True
        self.description = None
        self.rowcount = -1
        self.messages = []
        self.arraysize = 1
        self._closed = False

    @property
    def connection(self) -> "AsyncConnection":
        return self._conn

    def _install(self, result) -> None:
        shape, batch, done, rowcount, messages = result
        self._batch = batch
        self._rows = []
        self._pos = 0
        self._done = done
        self.description = shape.description if shape is not None else None
        self.rowcount = rowcount
        self.messages = messages or []

    def _check(self) -> None:
        if self._closed:
            raise _interface_error("Cursor is closed")

    async def execute(self, operation: str, *parameters: Any) -> "AsyncCursor":
        self._check()
        params = _params(parameters) if parameters else None
        result = await _wait(self._c, self._c.execute(_hub(), self._id, operation, params))
        self._install(result)
        return self

    async def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]) -> None:
        self._check()
        rows = await _wait(self._c, self._c.executemany(_hub(), self._id, operation, seq_of_parameters))
        self._batch = None
        self._done = True
        self.description = None
        self.rowcount = rows

    async def _more(self, n: int) -> list:
        rows, done = await _wait(self._c, self._c.fetch(_hub(), self._id, n))
        self._done = done
        return rows

    def _local(self, n: int) -> list:
        """Up to `n` rows already on the Python side (no await)."""
        out = []
        batch = self._batch
        if batch is not None and batch.remaining():
            out = batch.take(n)
        if len(out) < n and self._pos < len(self._rows):
            end = self._pos + (n - len(out))
            if out:
                out.extend(self._rows[self._pos:end])
            else:
                out = self._rows[self._pos:end]
            self._pos = min(end, len(self._rows))
        return out

    async def fetchone(self) -> Optional[Any]:
        batch = self._batch
        if batch is not None:
            row = batch.next()
            if row is not None:
                return row
            self._batch = None
        if self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            return row
        if self._done:
            return None
        self._rows = await self._more(max(self.arraysize, 512))
        self._pos = 0
        return await self.fetchone()

    async def fetchmany(self, size: Optional[int] = None) -> list:
        size = self.arraysize if size is None else size
        out = self._local(size)
        while len(out) < size and not self._done:
            more = await self._more(size - len(out))
            if out:
                out.extend(more)
            else:
                out = more
        return out

    async def fetchall(self) -> list:
        out = self._local(2**62)
        while not self._done:  # a result that fails after some rows ends with the error
            more = await self._more(2**62)
            if out:
                out.extend(more)
            else:
                out = more
        return out

    async def fetchall_arrow(self, batch_size: int = 65_536):
        """All remaining rows as a native ArrowTable (DuckDB/polars/pyarrow read it directly)."""
        self._check()
        if self.description is None:
            raise _programming_error("No active result set. Cannot fetch Arrow batch.")
        if self._pos < len(self._rows):
            raise _programming_error("fetchall_arrow() after a partial fetchone() is not supported")
        head, self._batch = self._batch, None
        table = await _wait(
            self._c, self._c.fetch_arrow(_hub(), self._id, head, batch_size)
        )
        self._done = True
        return table

    async def nextset(self) -> Optional[bool]:
        self._check()
        result = await _wait(self._c, self._c.nextset(_hub(), self._id))
        if result is None:
            self._batch = None
            self._done = True
            self.description = None
            return None
        self._install(result)
        return True

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._batch = None
            self._c.close_cursor(self._id)

    def __del__(self):
        # Rust keeps a cursor's results until it is closed: retire it with the object.
        try:
            if not self._closed:
                self._closed = True
                self._c.close_cursor(self._id)
        except Exception:
            pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        batch = self._batch
        if batch is not None:
            row = batch.next()
            if row is not None:
                return row
        elif self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            return row
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


class _FetchMixin:
    """fetch/fetchrow/fetchval shared by connections and pools; needs ``_query``."""

    __slots__ = ()

    async def fetch(self, operation: str, *parameters: Any) -> list:
        """All rows of the first result, in one round trip."""
        return await self._query(operation, parameters, 0)

    async def fetchrow(self, operation: str, *parameters: Any) -> Optional[Any]:
        return await self._query(operation, parameters, 1)

    async def fetchval(self, operation: str, *parameters: Any) -> Any:
        return await self._query(operation, parameters, 2)


class AsyncConnection(_FetchMixin):
    """One TDS session. Operations on one connection run one at a time."""

    def __init__(self, native):
        self._c = native
        self._closed = False

    @property
    def autocommit(self) -> bool:
        return self._c.autocommit

    async def set_autocommit(self, value: bool) -> None:
        await _wait(self._c, self._c.set_autocommit(_hub(), bool(value)))

    @property
    def timeout(self) -> int:
        return self._c.timeout

    @timeout.setter
    def timeout(self, value: int) -> None:
        self._c.timeout = value

    @property
    def closed(self) -> bool:
        return self._closed

    def cursor(self) -> AsyncCursor:
        if self._closed:
            raise _interface_error("Connection is closed")
        return AsyncCursor(self)

    async def execute(self, operation: str, *parameters: Any) -> AsyncCursor:
        return await self.cursor().execute(operation, *parameters)

    async def _query(self, operation, parameters, mode):
        if self._closed:
            raise _interface_error("Connection is closed")
        params = _params(parameters) if parameters else None
        return await _wait(self._c, self._c.query(_hub(), operation, params, mode))

    async def commit(self) -> None:
        await _wait(self._c, self._c.commit(_hub()))

    async def rollback(self) -> None:
        await _wait(self._c, self._c.rollback(_hub()))

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await _wait(self._c, self._c.close(_hub()))

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if not self._closed and not self.autocommit:
                if exc_type is None:
                    await self.commit()
                else:
                    await self.rollback()
        finally:
            await self.close()


_POOL_TIMEOUT_TEXT = "Timed out waiting for a connection from the pool"


async def _pool_wait(pool, op):
    """`_wait` that raises asyncio.TimeoutError when the pool's acquire timeout expires."""
    try:
        return await _wait(pool, op)
    except Exception as exc:
        if _POOL_TIMEOUT_TEXT in str(exc):
            raise asyncio.TimeoutError(_POOL_TIMEOUT_TEXT) from exc
        raise


class _Acquire:
    def __init__(self, pool: "Pool", timeout: Optional[float] = None):
        self._pool = pool
        self._timeout = timeout
        self._conn: Optional[AsyncConnection] = None

    def __await__(self):
        return self._acquire().__await__()

    async def _acquire(self) -> AsyncConnection:
        native = await _pool_wait(
            self._pool._p, self._pool._p.acquire(_hub(), self._timeout)
        )
        return AsyncConnection(native)

    async def __aenter__(self) -> AsyncConnection:
        self._conn = await self._acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # Same as leaving ``async with connect(...)``: a pool created with
        # autocommit=False commits on a clean exit and rolls back on an
        # exception, then the session goes back to the pool.
        conn, self._conn = self._conn, None
        if conn is not None:
            await conn.__aexit__(exc_type, exc, tb)


class Pool(_FetchMixin):
    """A pool of sessions; the fetch/execute shortcuts use one session per call.

    With ``autocommit=False``, leaving ``async with pool.acquire()`` commits on
    a clean exit and rolls back on an exception (like ``AsyncConnection``);
    closing a connection obtained with ``await pool.acquire()`` rolls back.
    """

    def __init__(self, native):
        self._p = native

    def acquire(self, *, timeout: Optional[float] = None) -> _Acquire:
        """``async with pool.acquire() as conn`` (or ``conn = await pool.acquire()``).

        ``timeout`` (seconds, default: the pool's ``acquire_timeout``) bounds the
        wait for a free session, including opening a new one; on expiry
        ``asyncio.TimeoutError`` is raised."""
        return _Acquire(self, timeout)

    @property
    def acquire_timeout(self) -> float:
        """Default seconds ``acquire`` and the fetch/execute shortcuts wait (0 = unbounded)."""
        return self._p.acquire_timeout

    @acquire_timeout.setter
    def acquire_timeout(self, value: float) -> None:
        self._p.acquire_timeout = value

    async def execute(self, operation: str, *parameters: Any) -> int:
        """Run a statement; returns its rowcount (or rows, if it returns any)."""
        return await self._query(operation, parameters, 0)

    async def _query(self, operation, parameters, mode):
        params = _params(parameters) if parameters else None
        return await _pool_wait(self._p, self._p.query(_hub(), operation, params, mode))

    async def close(self) -> None:
        await _wait(self._p, self._p.close(_hub()))

    @property
    def max_size(self) -> int:
        return self._p.max_size

    @property
    def idle(self) -> int:
        return self._p.idle

    @property
    def in_use(self) -> int:
        return self._p.in_use

    async def __aenter__(self) -> "Pool":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


async def connect(
    connection_str: str,
    autocommit: bool = False,
    timeout: int = 0,
    *,
    tuple_rows: bool = False,
) -> AsyncConnection:
    """Open a connection. ``timeout`` is the per-query timeout in seconds
    (0 = none); ``tuple_rows=True`` returns plain tuples instead of Row objects.

    Cancelling the awaiting task (``asyncio.wait_for``) abandons the login."""
    hub = _hub()  # first: it reports an unsupported platform before the native symbol is looked up
    future, request, opening = _core.aio_connect(
        hub, connection_str, autocommit, timeout, tuple_rows
    )
    native = await _wait(opening, (future, request))
    return AsyncConnection(native)


async def create_pool(
    connection_str: str,
    min_size: int = 1,
    max_size: int = 10,
    autocommit: bool = True,
    timeout: int = 0,
    *,
    tuple_rows: bool = False,
    reset: bool = True,
    max_inactive_connection_lifetime: float = 300.0,
    max_lifetime: float = 0.0,
    acquire_timeout: float = 0.0,
    validate_after: float = 30.0,
) -> Pool:
    """Open ``min_size`` sessions now; up to ``max_size`` on demand.

    ``reset=True`` resets each session's state (temp tables, SET options,
    open transactions are rolled back regardless) before it is reused, via the
    TDS reset flag on the next request; ``reset=False`` skips the server-side
    reset for pools whose queries leave no session state behind.

    Lifetimes are in seconds, 0 turns a limit off (asyncpg-style):

    * ``max_inactive_connection_lifetime``: idle sessions older than this are
      closed (checked when sessions are returned or handed out).
    * ``max_lifetime``: sessions older than this are closed when returned and
      never handed out again.
    * ``acquire_timeout``: default wait for a free session in ``pool.acquire()``
      and the shortcuts; ``asyncio.TimeoutError`` on expiry.
    * ``validate_after``: a session idle at least this long is checked with a
      ``SELECT 1`` before it is handed out; a dead one is replaced.

    Cancelling the awaiting task abandons the sessions still being opened."""
    hub = _hub()  # first: it reports an unsupported platform before the native symbol is looked up
    future, request, opening = _core.aio_create_pool(
        hub,
        connection_str,
        min_size,
        max_size,
        autocommit,
        timeout,
        tuple_rows,
        reset,
        max_inactive_connection_lifetime,
        max_lifetime,
        acquire_timeout,
        validate_after,
    )
    native = await _wait(opening, (future, request))
    return Pool(native)


def _interface_error(message: str):
    return InterfaceError(driver_error=message, ddbc_error=message)


def _programming_error(message: str):
    return ProgrammingError(driver_error=message, ddbc_error=message)
