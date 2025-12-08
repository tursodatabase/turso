from __future__ import annotations

import asyncio
from queue import SimpleQueue
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from .lib import (
    Connection as BlockingConnection,
)
from .lib import (
    Cursor as BlockingCursor,
)
from .lib import (
    ProgrammingError,
)
from .lib import (
    connect as blocking_connect,
)
from .worker import STOP_RUNNING_SENTINEL, Worker


# Connection goes FIRST
class Connection:
    def __init__(self, connector: Callable[[], BlockingConnection]) -> None:
        # Event loop and per-connection worker thread state
        self._loop = asyncio.get_event_loop()
        self._queue: SimpleQueue[tuple[asyncio.Future, Callable[[], Any]]] = SimpleQueue()
        self._worker = Worker(self._queue, self._loop)
        self._connector = connector

        # Underlying blocking connection created in worker thread
        self._conn: Optional[BlockingConnection] = None
        self._closed: bool = False

        # Schedule connection creation as the very first job in the worker
        self._open_future: asyncio.Future[Connection] = self._loop.create_future()

        def _open() -> Connection:
            # Create the blocking connection inside the worker thread once
            if self._conn is None:
                self._conn = self._connector()
            return self

        self._queue.put_nowait((self._open_future, _open))
        self._worker.start()

        # Cached properties mirrored to the underlying connection.
        # Setters will enqueue mutation jobs; we keep local cache for getters.
        self._isolation_level_cache: Optional[str] = None
        self._row_factory_cache: Any = None
        self._text_factory_cache: Any = None
        self._autocommit_cache: object | bool | None = None

    async def close(self) -> None:
        if self._closed:
            return

        # Ensure underlying Connection.close() is called in worker thread
        def _do_close() -> None:
            if self._conn is not None:
                self._conn.close()

        await self._run(lambda: (_do_close(), None)[1])  # schedule and await completion of close

        # Request worker stop; we must not block the event loop while waiting.
        # Note: STOP_RUNNING_SENTINEL item will terminate the worker loop.
        stop_future = self._loop.create_future()
        self._queue.put_nowait((stop_future, lambda: STOP_RUNNING_SENTINEL))

        # Wait for the worker thread to terminate without blocking the loop
        await self._loop.run_in_executor(None, self._worker.join)
        self._closed = True

    def __await__(self):
        async def _await_open() -> "Connection":
            await self._open_future
            return self

        return _await_open().__await__()

    async def __aenter__(self) -> "Connection":
        await self
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # Just close the connection - do not add any extra logic
        await self.close()

    # Internal helper: schedule a callable to run in the worker thread and await its result.
    async def _run(self, func: Callable[[], Any]) -> Any:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed connection")
        fut = self._loop.create_future()
        self._queue.put_nowait((fut, func))
        return await fut

    # Internal helper: enqueue a callable but do not await completion (used for property setters).
    def _run_nowait(self, func: Callable[[], Any]) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed connection")
        fut = self._loop.create_future()
        self._queue.put_nowait((fut, func))

    # Cursor factory returning async Cursor wrapper
    def cursor(self, factory: Optional[Callable[[BlockingConnection], BlockingCursor]] = None) -> "Cursor":
        # Creation of the underlying blocking cursor is enqueued to preserve thread affinity.
        return Cursor(self, factory=factory)

    # Helpers similar to aiosqlite
    async def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> "Cursor":
        cur = self.cursor()
        await cur.execute(sql, parameters)
        return cur

    async def executemany(self, sql: str, parameters: Iterable[Sequence[Any] | Mapping[str, Any]]) -> "Cursor":
        cur = self.cursor()
        await cur.executemany(sql, parameters)
        return cur

    async def executescript(self, sql_script: str) -> "Cursor":
        cur = self.cursor()
        await cur.executescript(sql_script)
        return cur

    async def commit(self) -> None:
        await self._run(lambda: self._conn.commit())  # type: ignore[union-attr]

    async def rollback(self) -> None:
        await self._run(lambda: self._conn.rollback())  # type: ignore[union-attr]

    # Read/write properties mirrored to the underlying blocking connection.
    # DB-API hints:
    # - isolation_level controls implicit transactions (BEGIN DEFERRED/IMMEDIATE/EXCLUSIVE or None for autocommit).
    @property
    def isolation_level(self) -> Optional[str]:
        return self._isolation_level_cache

    @isolation_level.setter
    def isolation_level(self, value: Optional[str]) -> None:
        self._isolation_level_cache = value

        def _set() -> None:
            # Will be applied in the worker thread before subsequent operations
            self._conn.isolation_level = value  # type: ignore[union-attr]

        self._run_nowait(_set)

    @property
    def row_factory(self) -> Any:
        return self._row_factory_cache

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        self._row_factory_cache = value

        def _set() -> None:
            self._conn.row_factory = value  # type: ignore[union-attr]

        self._run_nowait(_set)

    @property
    def text_factory(self) -> Any:
        return self._text_factory_cache

    @text_factory.setter
    def text_factory(self, value: Any) -> None:
        self._text_factory_cache = value

        def _set() -> None:
            self._conn.text_factory = value  # type: ignore[union-attr]

        self._run_nowait(_set)

    @property
    def autocommit(self) -> object | bool | None:
        return self._autocommit_cache

    @autocommit.setter
    def autocommit(self, value: object | bool) -> None:
        self._autocommit_cache = value

        def _set() -> None:
            self._conn.autocommit = value  # type: ignore[union-attr]

        self._run_nowait(_set)


# Cursor goes SECOND
class Cursor:
    def __init__(
            self,
            connection: Connection,
            factory: Optional[Callable[[BlockingConnection], BlockingCursor]] = None
        ):
        self._connection: Connection = connection
        self._loop = asyncio.get_event_loop()

        # Underlying blocking cursor and its creation job
        self._cursor_created: bool = False
        self._bcursor: Optional[BlockingCursor] = None

        # Cursor attributes (DB-API)
        self.arraysize: int = 1
        self._description: Optional[tuple[tuple[str, None, None, None, None, None, None], ...]] = None
        self._lastrowid: Optional[int] = None
        self._rowcount: int = -1
        self._closed: bool = False

        # Enqueue creation of the underlying blocking cursor in the worker thread
        def _create() -> None:
            if self._cursor_created:
                return
            if factory is None:
                self._bcursor = self._connection._conn.cursor()  # type: ignore[union-attr]
            else:
                # Use provided factory to create BlockingCursor from BlockingConnection
                self._bcursor = factory(self._connection._conn)  # type: ignore[union-attr]
            self._cursor_created = True
            # Apply initial arraysize if any
            self._bcursor.arraysize = self.arraysize  # type: ignore[union-attr]

        self._connection._run_nowait(_create)

    @property
    def connection(self) -> Connection:
        return self._connection

    async def close(self) -> None:
        if self._closed:
            return

        def _close() -> None:
            if self._bcursor is not None:
                self._bcursor.close()

        await self._connection._run(_close)
        self._closed = True

    # Internal helpers for updating cached metadata after execute-like calls
    def _update_meta_cache(self, description, lastrowid, rowcount) -> None:
        self._description = description
        self._lastrowid = lastrowid
        self._rowcount = rowcount if rowcount is not None else -1

    async def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] = ()) -> "Cursor":
        self._ensure_open()

        def _exec() -> tuple[Any, Any, Any]:
            # Perform the execute and collect metadata
            cur = self._bcursor  # type: ignore[assignment]
            cur.execute(sql, parameters)
            return (cur.description, cur.lastrowid, cur.rowcount)

        description, lastrowid, rowcount = await self._connection._run(_exec)
        self._update_meta_cache(description, lastrowid, rowcount)
        return self

    async def executemany(self, sql: str, parameters: Iterable[Sequence[Any] | Mapping[str, Any]]) -> "Cursor":
        self._ensure_open()

        def _execm() -> tuple[Any, Any, Any]:
            cur = self._bcursor  # type: ignore[assignment]
            cur.executemany(sql, parameters)
            return (cur.description, cur.lastrowid, cur.rowcount)

        description, lastrowid, rowcount = await self._connection._run(_execm)
        self._update_meta_cache(description, lastrowid, rowcount)
        return self

    async def executescript(self, sql_script: str) -> "Cursor":
        self._ensure_open()

        def _execs() -> tuple[Any, Any, Any]:
            cur = self._bcursor  # type: ignore[assignment]
            cur.executescript(sql_script)
            return (cur.description, cur.lastrowid, cur.rowcount)

        description, lastrowid, rowcount = await self._connection._run(_execs)
        self._update_meta_cache(description, lastrowid, rowcount)
        return self

    async def fetchone(self) -> Any:
        self._ensure_open()

        def _one() -> Any:
            return self._bcursor.fetchone()  # type: ignore[union-attr]

        return await self._connection._run(_one)

    async def fetchmany(self, size: Optional[int] = None) -> list[Any]:
        self._ensure_open()

        def _many() -> list[Any]:
            n = self.arraysize if size is None else size
            return list(self._bcursor.fetchmany(n))  # type: ignore[union-attr]

        return await self._connection._run(_many)

    async def fetchall(self) -> list[Any]:
        self._ensure_open()

        def _all() -> list[Any]:
            return list(self._bcursor.fetchall())  # type: ignore[union-attr]

        return await self._connection._run(_all)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor")

    # Properties reflecting DB-API attributes of the last executed statement
    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | None:
        return self._description

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        return self._rowcount

    # Make cursor usable as async context manager, similar to aiosqlite
    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


# connect is not async because it returns awaitable Connection
# same signature as in the lib.py
def connect(
    database: str,
    *,
    experimental_features: Optional[str] = None,
    isolation_level: Optional[str] = "DEFERRED",
    extra_io: Optional[Callable[[], None]] = None,
) -> Connection:
    # Create a connector that opens a blocking Connection using the existing driver.
    def _connector() -> BlockingConnection:
        conn = blocking_connect(
            database,
            experimental_features=experimental_features,
            isolation_level=isolation_level,
            extra_io=extra_io,
        )
        return conn

    return Connection(_connector)
