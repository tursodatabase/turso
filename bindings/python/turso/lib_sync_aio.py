from __future__ import annotations

from typing import Callable, Optional, Union, cast

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
    def __init__(self, connector: Callable[[], BlockingConnectionSync]) -> None:
        # Use the non-blocking driver base - runs a background worker thread
        # that owns the underlying blocking connection instance.
        super().__init__(connector)

    async def close(self) -> None:
        # Ensure worker is shut down and underlying blocking connection closed
        await super().close()

    # Make ConnectionSync instance awaitable with correct return typing
    def __await__(self):
        async def _await_open() -> "ConnectionSync":
            await self._open_future
            return self  # the underlying connection is created at this point

        return _await_open().__await__()

    async def __aenter__(self) -> "ConnectionSync":
        await self
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # Synchronization API (async wrappers scheduling work on the worker thread)

    async def pull(self) -> bool:
        # Pull remote changes and apply locally; returns True if any updates were fetched
        return await self._run(lambda: cast(BlockingConnectionSync, self._conn).pull())  # type: ignore[union-attr]

    async def push(self) -> None:
        # Push local changes to the remote
        await self._run(lambda: cast(BlockingConnectionSync, self._conn).push())  # type: ignore[union-attr]

    async def checkpoint(self) -> None:
        # Checkpoint the WAL of the synced database
        await self._run(lambda: cast(BlockingConnectionSync, self._conn).checkpoint())  # type: ignore[union-attr]

    async def stats(self) -> PyTursoSyncDatabaseStats:
        # Collect stats about the synced database
        return await self._run(lambda: cast(BlockingConnectionSync, self._conn).stats())  # type: ignore[union-attr]


# connect is not async because it returns awaitable ConnectionSync
# Same signature as in the lib_sync.connect_sync
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
    # Connector creating the blocking synchronized connection in the worker thread
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

    # Return awaitable async wrapper with sync extras
    return ConnectionSync(_connector)
