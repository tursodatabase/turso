from __future__ import annotations

import os
import urllib.error

# for HTTP IO
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Tuple, Union

from ._turso import (
    Misuse,
    PyTursoAsyncOperation,
    PyTursoAsyncOperationResultKind,
    PyTursoConnection,
    PyTursoDatabaseConfig,
    PyTursoPartialSyncOpts,
    PyTursoSyncDatabase,
    PyTursoSyncDatabaseConfig,
    PyTursoSyncDatabaseStats,
    PyTursoSyncIoItem,
    PyTursoSyncIoItemRequestKind,
    py_turso_sync_new,
)
from .lib import Connection as _Connection

# Constants
_HTTP_CHUNK_SIZE = 64 * 1024  # 64 KiB


@dataclass
class PartialSyncPrefixBootstrap:
    # Bootstraps DB by fetching first N bytes/pages; enables partial sync
    length: int


@dataclass
class PartialSyncQueryBootstrap:
    # Bootstraps DB by fetching pages touched by given SQL query on server
    query: str

@dataclass
class PartialSyncOpts:
    bootstrap_strategy: Union[PartialSyncPrefixBootstrap, PartialSyncQueryBootstrap]
    segment_size: Optional[int] = None
    prefetch: Optional[bool] = None

class _HttpContext:
    """
    Resolved network/auth configuration used by sync engine IO handler.
    remote_url and auth_token can be static strings or callables (evaluated per request).
    """

    def __init__(
        self,
        remote_url: Union[str, Callable[[], Optional[str]]],
        auth_token: Optional[Union[str, Callable[[], Optional[str]]]],
        client_name: str,
    ) -> None:
        self.remote_url = remote_url
        self.auth_token = auth_token
        self.client_name = client_name

    def _eval(self, v: Union[str, Callable[[], Optional[str]]]) -> Optional[str]:
        if callable(v):
            return v()
        return v

    def base_url(self) -> str:
        url = self._eval(self.remote_url)
        if not url:
            raise RuntimeError("remote_url is not available")
        return url

    def token(self) -> Optional[str]:
        if self.auth_token is None:
            return None
        return self._eval(self.auth_token)


def _join_url(base: str, path: str) -> str:
    if not base:
        return path
    if base.endswith("/") and path.startswith("/"):
        return base[:-1] + path
    if not base.endswith("/") and not path.startswith("/"):
        return base + "/" + path
    return base + path


def _headers_iter_to_pairs(headers: Iterable[Tuple[str, str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for h in headers:
        try:
            k, v = h
        except Exception:
            # best-effort skip invalid headers
            continue
        pairs.append((str(k), str(v)))
    return pairs


# ruff: noqa: C901
def _process_http_item(
    sync: PyTursoSyncDatabase,
    io_item: PyTursoSyncIoItem,
    req_kind: Any,
    ctx: _HttpContext,
    current_op: Optional[PyTursoAsyncOperation],
) -> None:
    """
    Execute HTTP request, stream response to sync io completion.
    """
    # Access request fields
    method = req_kind.method
    path = req_kind.path
    body: Optional[bytes] = None
    if req_kind.body is not None:
        # req_kind.body is PyBytes -> bytes
        body = bytes(req_kind.body)

    headers_list = []
    if req_kind.headers is not None:
        headers_list = _headers_iter_to_pairs(req_kind.headers)  # list[(k,v)]

    try:
        base_url = ctx.base_url()
    except Exception as e:
        io_item.poison(f"remote url unavailable: {e}")
        return

    # Build full URL
    url = _join_url(base_url, path)

    # Build request
    request = urllib.request.Request(url=url, data=body, method=method)
    # Add provided headers
    seen_auth = False
    for k, v in headers_list:
        request.add_header(k, v)
        if k.lower() == "authorization":
            seen_auth = True

    # Add Authorization if not present and token provided
    token = None
    try:
        token = ctx.token()
    except Exception:
        # token resolver failure -> bubble up as IO error
        io_item.poison("auth token resolver failed")
        return

    if token is None and not seen_auth:
        # No token provided; some endpoints can be public; proceed without it.
        pass
    elif token is not None and not seen_auth:
        request.add_header("Authorization", f"Bearer {token}")

    # Add a clear user-agent to help server logs
    if "User-Agent" not in request.headers:
        request.add_header("User-Agent", f"{ctx.client_name}")

    # Perform request
    try:
        with urllib.request.urlopen(request) as resp:
            status = getattr(resp, "status", None)
            if status is None:
                try:
                    status = resp.getcode()
                except Exception:
                    status = 200
            io_item.status(int(status))
            # Stream response in chunks
            while True:
                chunk = resp.read(_HTTP_CHUNK_SIZE)
                if not chunk:
                    break
                io_item.push_buffer(chunk)
                if current_op is not None:
                    # The operation should still be waiting for IO
                    r = current_op.resume()
                    # Per contract, while streaming response operation must not finish
                    # We don't raise if it did, but assert in debug builds
                    try:
                        assert r is None
                    except Exception:
                        # continue anyway
                        pass
            io_item.done()
    except urllib.error.HTTPError as e:
        # HTTPError has a response body we may stream to completion
        status = getattr(e, "code", 500)
        io_item.status(int(status))
        try:
            # e.read() may not be available in all Python versions; use e.fp if present
            stream = e
            # Attempt to read the error body and forward it
            while True:
                chunk = stream.read(_HTTP_CHUNK_SIZE)
                if not chunk:
                    break
                io_item.push_buffer(chunk)
                if current_op is not None:
                    r = current_op.resume()
                    try:
                        assert r is None
                    except Exception:
                        pass
        except Exception:
            # ignore body read failures
            pass
        finally:
            io_item.done()
    except urllib.error.URLError as e:
        io_item.poison(f"network error: {e.reason}")
    except Exception as e:
        io_item.poison(f"http error: {e}")


def _process_full_read_item(io_item: PyTursoSyncIoItem, req_kind: Any) -> None:
    """
    Fulfill full file read request by streaming file content if exists.
    On not found - send empty response (not error).
    """
    path = req_kind.path
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(_HTTP_CHUNK_SIZE)
                if not chunk:
                    break
                io_item.push_buffer(chunk)
        io_item.done()
    except FileNotFoundError:
        # On not found engine expects empty response, not error
        io_item.done()
    except Exception as e:
        io_item.poison(f"fs read error: {e}")


def _process_full_write_item(io_item: PyTursoSyncIoItem, req_kind: Any) -> None:
    """
    Fulfill full file write request by writing provided content atomically.
    """
    path = req_kind.path
    content: bytes = bytes(req_kind.content) if req_kind.content is not None else b""
    # Ensure parent directory exists
    try:
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
    except Exception:
        # ignore directory creation errors, attempt to write anyway
        pass

    try:
        with open(path, "wb") as f:
            # Write in chunks if content is large
            view = memoryview(content)
            offset = 0
            length = len(view)
            while offset < length:
                end = min(offset + _HTTP_CHUNK_SIZE, length)
                f.write(view[offset:end])
                offset = end
        io_item.done()
    except Exception as e:
        io_item.poison(f"fs write error: {e}")


def _drain_sync_io(
    sync: PyTursoSyncDatabase,
    ctx: _HttpContext,
    *,
    current_op: Optional[PyTursoAsyncOperation] = None,
) -> None:
    """
    Drain all pending IO items from sync engine queue and process them.
    """
    while True:
        item = sync.take_io_item()
        try:
            # tricky: we must do step_io_callbacks even if there is no IO in the queue
            if item is None:
                break
            req = item.request()
            if req.kind == PyTursoSyncIoItemRequestKind.Http and req.http is not None:
                _process_http_item(sync, item, req.http, ctx, current_op)
            elif req.kind == PyTursoSyncIoItemRequestKind.FullRead and req.full_read is not None:
                _process_full_read_item(item, req.full_read)
            elif req.kind == PyTursoSyncIoItemRequestKind.FullWrite and req.full_write is not None:
                _process_full_write_item(item, req.full_write)
            else:
                item.poison("unknown io request kind")
        except Exception as e:
            # Safety net: poison unexpected failures
            try:
                item.poison(f"io processing error: {e}")
            except Exception:
                pass
        finally:
            # Allow engine to run any post-io callbacks
            sync.step_io_callbacks()


def _run_op(
    sync: PyTursoSyncDatabase,
    op: PyTursoAsyncOperation,
    ctx: _HttpContext,
) -> Any:
    """
    Drive async operation to completion, servicing sync engine IO in between.
    Returns operation result payload depending on kind:
      - No: returns None
      - Connection: returns PyTursoConnection
      - Changes: returns PyTursoSyncDatabaseChanges
      - Stats: returns PyTursoSyncDatabaseStats
    """
    while True:
        res = op.resume()
        if res is None:
            # Needs IO
            _drain_sync_io(sync, ctx, current_op=op)
            continue
        # Finished
        if res.kind == PyTursoAsyncOperationResultKind.No:
            return None
        if res.kind == PyTursoAsyncOperationResultKind.Connection and res.connection is not None:
            return res.connection
        if res.kind == PyTursoAsyncOperationResultKind.Changes and res.changes is not None:
            return res.changes
        if res.kind == PyTursoAsyncOperationResultKind.Stats and res.stats is not None:
            return res.stats
        # Unexpected; return None
        return None


class ConnectionSync(_Connection):
    """
    Synchronized connection that extends regular embedded driver with
    push/pull and remote bootstrap capabilities.
    """

    def __init__(
        self,
        conn: PyTursoConnection,
        *,
        sync: PyTursoSyncDatabase,
        http_ctx: _HttpContext,
        isolation_level: Optional[str] = "DEFERRED",
    ) -> None:
        # Provide extra_io hook so statements can make progress with sync engine (partial sync)
        def _extra_io() -> None:
            _drain_sync_io(sync, http_ctx, current_op=None)

        super().__init__(conn, isolation_level=isolation_level, extra_io=_extra_io)
        self._sync: PyTursoSyncDatabase = sync
        self._http_ctx: _HttpContext = http_ctx

    def pull(self) -> bool:
        """
        Pull remote changes and apply locally.
        Returns True if new updates were pulled; False otherwise.
        """
        # Wait for changes
        changes = _run_op(self._sync, self._sync.wait_changes(), self._http_ctx)
        # determine if empty before applying
        if changes is None:
            # Should not happen; treat as no changes
            return False
        is_empty = bool(changes.empty())
        if is_empty:
            return False
        # Apply non-empty changes
        op = self._sync.apply_changes(changes)
        _run_op(self._sync, op, self._http_ctx)
        return True

    def push(self) -> None:
        """
        Push local changes to remote.
        """
        _run_op(self._sync, self._sync.push_changes(), self._http_ctx)

    def checkpoint(self) -> None:
        """
        Checkpoint the WAL of the synced database.
        """
        _run_op(self._sync, self._sync.checkpoint(), self._http_ctx)

    def stats(self) -> PyTursoSyncDatabaseStats:
        """
        Collect stats about the synced database.
        """
        stats = _run_op(self._sync, self._sync.stats(), self._http_ctx)
        return stats


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
    """
    Create and open a synchronized database connection.

    - path: path to the main database file locally
    - remote_url: remote url for the sync - can be lambda evaluated on every http request
    - auth_token: optional token or lambda returning token, used as Authorization: Bearer <token>
    - client_name: optional unique client name (defaults to 'turso-sync-py')
    - long_poll_timeout_ms: timeout for long polling during pull
    - bootstrap_if_empty: if True and db empty, bootstrap from remote during create()
    - partial_sync_opts: optional partial sync configuration
    - experimental_features, isolation_level: passed to underlying connection
    """
    # Resolve client name
    cname = client_name or "turso-sync-py"
    if remote_url.startswith("libsql://"):
        remote_url = remote_url.replace("libsql://", "https://", 1)
    http_ctx = _HttpContext(remote_url=remote_url, auth_token=auth_token, client_name=cname)

    # Database config: async_io must be True to let Python drive IO
    db_cfg = PyTursoDatabaseConfig(
        path=path,
        experimental_features=experimental_features,
        async_io=True,
    )

    # Sync config with optional partial bootstrap strategy
    prefix_len: Optional[int] = None
    query_str: Optional[str] = None
    if partial_sync_opts is not None and isinstance(partial_sync_opts.bootstrap_strategy, PartialSyncPrefixBootstrap):
        prefix_len = int(partial_sync_opts.bootstrap_strategy.length)
    elif partial_sync_opts is not None and isinstance(partial_sync_opts.bootstrap_strategy, PartialSyncQueryBootstrap):
        query_str = str(partial_sync_opts.bootstrap_strategy.query)

    sync_cfg = PyTursoSyncDatabaseConfig(
        path=path,
        client_name=cname,
        long_poll_timeout_ms=long_poll_timeout_ms,
        bootstrap_if_empty=bootstrap_if_empty,
        reserved_bytes=None,
        partial_sync_opts=PyTursoPartialSyncOpts(
            bootstrap_strategy_prefix=prefix_len,
            bootstrap_strategy_query=query_str,
            segment_size=partial_sync_opts.segment_size,
            prefetch=partial_sync_opts.prefetch
        ) if partial_sync_opts is not None else None,
    )

    # Create sync database holder
    sync_db: PyTursoSyncDatabase = py_turso_sync_new(db_cfg, sync_cfg)

    # Prepare + open the database with create()
    _run_op(sync_db, sync_db.create(), http_ctx)

    # Connect to obtain PyTursoConnection
    conn_obj = _run_op(sync_db, sync_db.connect(), http_ctx)
    if not isinstance(conn_obj, PyTursoConnection):
        raise Misuse("sync connect did not return a connection")

    # Wrap into ConnectionSync that integrates sync IO into DB operations
    return ConnectionSync(conn_obj, sync=sync_db, http_ctx=http_ctx, isolation_level=isolation_level)
