from __future__ import annotations

from typing import Callable, Optional, Union, cast

from anyio import to_thread

from .lib import OperationalError
from .lib_aio import (
    _DEFAULT_WORKER_TIMEOUT,
)
from .lib_aio import (
    Connection as NonBlockingConnection,
)
from .lib_sync import (
    ConnectionSync as BlockingConnectionSync,
)
from .lib_sync import (
    PartialSyncOpts,
    PyTursoSyncDatabaseStats,
)
from .lib_sync import (
    connect_sync as blocking_connect_sync,
)


class ConnectionSync(NonBlockingConnection):
    """Async wrapper for synchronized (remote-enabled) database connections.

    Extends the base async Connection with sync-specific operations (pull, push, etc.).
    Uses the same dedicated worker thread pattern as the parent class.
    """

    def __init__(self, connector: Callable[[], BlockingConnectionSync]) -> None:
        super().__init__(connector)

    async def close(self) -> None:
        await super().close()

    def __await__(self):
        """Allow `conn = await turso.aio.sync.connect(...)`."""

        async def _await_open() -> "ConnectionSync":
            completed = await to_thread.run_sync(
                lambda: self._open_item.event.wait(timeout=_DEFAULT_WORKER_TIMEOUT),
                abandon_on_cancel=True,
            )
            if not completed:
                raise OperationalError("Worker thread did not respond within timeout")
            if self._open_item.exception is not None:
                raise self._open_item.exception
            return self

        return _await_open().__await__()

    async def __aenter__(self) -> "ConnectionSync":
        await self
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # Synchronization API (async wrappers running work in worker thread)

    async def pull(self) -> bool:
        """Pull remote changes and apply locally; returns True if any updates were fetched."""
        return await self._run(lambda: cast(BlockingConnectionSync, self._conn).pull())

    async def push(self) -> None:
        """Push local changes to the remote."""
        await self._run(lambda: cast(BlockingConnectionSync, self._conn).push())

    async def checkpoint(self) -> None:
        """Checkpoint the WAL of the synced database."""
        await self._run(lambda: cast(BlockingConnectionSync, self._conn).checkpoint())

    async def stats(self) -> PyTursoSyncDatabaseStats:
        """Collect stats about the synced database."""
        return await self._run(lambda: cast(BlockingConnectionSync, self._conn).stats())


# connect_sync is not async because it returns an awaitable ConnectionSync.
# Use as: `conn = await connect_sync(...)` or `async with connect_sync(...) as conn:`
def connect_sync(
    path: str,
    remote_url: Union[str, Callable[[], Optional[str]]],
    *,
    auth_token: Optional[Union[str, Callable[[], Optional[str]]]] = None,
    client_name: Optional[str] = None,
    long_poll_timeout_ms: Optional[int] = None,
    bootstrap_if_empty: bool = True,
    partial_sync_opts: Optional[PartialSyncOpts] = None,
    experimental_features: Optional[str] = None,
    isolation_level: Optional[str] = "DEFERRED",
) -> ConnectionSync:
    """Connect to a synced database (async version).

    Same signature as lib_sync.connect_sync but returns an async-compatible connection.
    """

    def _connector() -> BlockingConnectionSync:
        return blocking_connect_sync(
            path,
            remote_url,
            auth_token=auth_token,
            client_name=client_name,
            long_poll_timeout_ms=long_poll_timeout_ms,
            bootstrap_if_empty=bootstrap_if_empty,
            partial_sync_opts=partial_sync_opts,
            experimental_features=experimental_features,
            isolation_level=isolation_level,
        )

    return ConnectionSync(_connector)
