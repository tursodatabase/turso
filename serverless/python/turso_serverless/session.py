"""HTTP session with baton management for hrana v3.

Ported from the gold-standard JavaScript driver
(``serverless/javascript/src/session.ts``). Uses urllib.request (stdlib, no
extra deps).

Transaction state is server-authoritative: every pipeline request carries a
``get_autocommit`` probe and every cursor request carries an ``is_autocommit``
gated batch step, so ``in_transaction`` reflects ``sqlite3_get_autocommit()``
on the server rather than any local guess about the SQL text. The baton is
always persisted across requests — a non-null baton does NOT imply a
transaction; the server also keeps the stream open for stored SQL or pragma
side effects.
"""

from __future__ import annotations

import json
import socket
import threading
import urllib.error
import urllib.request
from typing import Any, Optional

from ._dbapi_common import DatabaseError
from .protocol import build_cursor_request, build_pipeline_request

# Header carrying the base64-encoded encryption key for encrypted Turso Cloud
# databases. Mirrors ENCRYPTION_KEY_HEADER in the JS driver's protocol.ts.
ENCRYPTION_KEY_HEADER = "x-turso-encryption-key"


def normalize_url(url: str) -> str:
    """Rewrite turso:// and libsql:// to https://."""
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    if url.startswith("turso://"):
        return "https://" + url[len("turso://"):]
    return url


def normalize_batch_mode(mode: str) -> str:
    """Map a batch/transaction mode name to its SQL ``BEGIN <mode>`` keyword.

    Accepts the same values as the transaction variants of the gold-standard
    driver: ``write``/``read``/``deferred``/``immediate``/``exclusive``/
    ``concurrent`` (case-insensitive). Unknown values pass through uppercased.
    """
    m = str(mode).lower()
    if m == "write":
        return "IMMEDIATE"
    if m in ("read", "deferred"):
        return "DEFERRED"
    if m == "immediate":
        return "IMMEDIATE"
    if m == "exclusive":
        return "EXCLUSIVE"
    if m == "concurrent":
        return "CONCURRENT"
    return str(mode).upper()


def autocommit_probe_step() -> dict:
    """A trailing batch step gated on ``is_autocommit``, appended to every
    cursor request. The cursor endpoint cannot carry a ``get_autocommit``
    probe, so whether this step executed tells us the connection's transaction
    state without an extra round trip. Mirrors ``Session.autocommitProbeStep``.
    """
    return {
        "stmt": {"sql": "SELECT 1", "args": [], "named_args": [], "want_rows": False},
        "condition": {"type": "is_autocommit"},
    }


class Session:
    """Manages connection state (baton, autocommit) with a remote Turso server."""

    def __init__(
        self,
        url: str,
        auth_token: Optional[str] = None,
        *,
        remote_encryption_key: Optional[str] = None,
        request_headers: Optional[dict[str, str]] = None,
        default_query_timeout: Optional[float] = None,
    ) -> None:
        for name in request_headers or {}:
            # `Host` cannot be overridden reliably; reject it up front so the
            # caller learns the override never takes effect (matches JS).
            if name.lower() == "host":
                raise DatabaseError("overwriting the 'Host' header is not supported")
        self._auth_token = auth_token
        self._remote_encryption_key = remote_encryption_key
        self._request_headers = dict(request_headers) if request_headers else None
        # Default per-statement timeout in milliseconds (matches JS
        # `defaultQueryTimeout`); None or 0 disables it.
        self._default_query_timeout = default_query_timeout
        self._baton: Optional[str] = None
        self._base_url = normalize_url(url)
        # Cached autocommit status from the server's last answer. A fresh
        # connection is in autocommit (not in a transaction).
        self._autocommit = True
        # In-flight HTTP responses, so interrupt() can abort a blocked read
        # from another thread. Guarded by _lock.
        self._lock = threading.Lock()
        self._active_responses: set = set()
        self._interrupted = False

    @property
    def in_transaction(self) -> bool:
        """Whether the connection is inside a transaction, from the server's
        authoritative ``get_autocommit``/``is_autocommit`` answer."""
        return not self._autocommit

    def set_default_query_timeout(self, milliseconds: Optional[float]) -> None:
        self._default_query_timeout = milliseconds

    def get_default_query_timeout(self) -> Optional[float]:
        return self._default_query_timeout

    def _headers(self, query_headers: Optional[dict[str, str]] = None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"
        if self._remote_encryption_key:
            headers[ENCRYPTION_KEY_HEADER] = self._remote_encryption_key
        # Session-level custom headers override the standard ones; per-query
        # headers override the session ones.
        if self._request_headers:
            headers.update(self._request_headers)
        if query_headers:
            headers.update(query_headers)
        return headers

    def _timeout_seconds(self, query_timeout: Optional[float]) -> Optional[float]:
        ms = query_timeout if query_timeout is not None else self._default_query_timeout
        if ms is not None and ms > 0:
            return ms / 1000.0
        return None

    def _post(
        self,
        path: str,
        body: dict,
        query_headers: Optional[dict[str, str]] = None,
        query_timeout: Optional[float] = None,
    ) -> bytes:
        """POST JSON to the server and return raw response bytes."""
        url = f"{self._base_url}{path}"
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=self._headers(query_headers), method="POST")
        timeout = self._timeout_seconds(query_timeout)
        # Clear any stale interrupt request; interrupt() is a no-op when no
        # request is in flight (matches the embedded driver).
        self._interrupted = False
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            response_body = e.read().decode("utf-8", errors="replace")
            # Try to extract message from JSON error body
            try:
                parsed = json.loads(response_body)
                msg = parsed.get("message", response_body)
            except (json.JSONDecodeError, KeyError):
                msg = response_body or f"HTTP error! status: {e.code}"
            raise RuntimeError(msg) from None
        except (socket.timeout, TimeoutError):
            raise RuntimeError("query timed out") from None

        with self._lock:
            self._active_responses.add(resp)
        try:
            return resp.read()
        except Exception as e:
            # A concurrent interrupt() closes the response mid-read.
            if self._interrupted:
                raise RuntimeError("interrupted") from None
            raise RuntimeError(str(e)) from None
        finally:
            with self._lock:
                self._active_responses.discard(resp)
            try:
                resp.close()
            except Exception:
                pass

    def interrupt(self) -> None:
        """Abort any request currently reading a response on this session.

        Best-effort for an HTTP transport: closes the in-flight response so a
        blocked read raises ``RuntimeError("interrupted")``. A no-op when no
        request is in flight. Mirrors the embedded driver's ``interrupt()``.
        """
        self._interrupted = True
        with self._lock:
            responses = list(self._active_responses)
        for resp in responses:
            try:
                resp.close()
            except Exception:
                pass

    def execute_cursor(
        self,
        steps: list[dict],
        *,
        query_headers: Optional[dict[str, str]] = None,
        query_timeout: Optional[float] = None,
    ) -> list[dict]:
        """Execute a cursor request (streaming NDJSON) and return the caller's
        entries with the trailing autocommit probe filtered out.

        Appends an ``is_autocommit`` gated probe step, refreshes the cached
        transaction state from whether the probe ran, and always persists the
        returned baton. Mirrors ``executeRaw`` + ``trackAutocommit``.
        """
        probe_idx = len(steps)
        request = build_cursor_request(self._baton, list(steps) + [autocommit_probe_step()])

        try:
            raw = self._post("/v3/cursor", request, query_headers, query_timeout)
        except RuntimeError:
            self._baton = None
            self._autocommit = True
            raise

        text = raw.decode("utf-8")
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            self._baton = None
            self._autocommit = True
            raise RuntimeError("No cursor response received")

        # First line: CursorResponse (baton + optional base_url). The baton is
        # always persisted — it does not imply an open transaction.
        cursor_resp = json.loads(lines[0])
        self._baton = cursor_resp.get("baton")
        if cursor_resp.get("base_url"):
            self._base_url = cursor_resp["base_url"]

        entries = [json.loads(line) for line in lines[1:]]
        return self._track_autocommit(entries, probe_idx, query_timeout)

    def _track_autocommit(
        self, entries: list[dict], probe_idx: int, query_timeout: Optional[float]
    ) -> list[dict]:
        """Filter the probe step's entries out of a cursor stream and update the
        cached transaction state from whether the probe executed. The probe is
        always the last step, so everything after its ``step_begin`` belongs to
        it. If the stream ended abnormally (fatal error or a probe error) the
        answer is unreliable, so the state is refreshed with a fallback request.
        """
        real: list[dict] = []
        saw_probe = False
        unreliable = False
        skipping_probe = False
        for entry in entries:
            etype = entry.get("type")
            if etype == "step_begin" and entry.get("step") == probe_idx:
                saw_probe = True
                skipping_probe = True
                continue
            if skipping_probe and etype in ("row", "step_end"):
                continue
            if etype == "error" or (etype == "step_error" and entry.get("step") == probe_idx):
                unreliable = True
                if etype == "step_error":
                    continue
            real.append(entry)

        if not unreliable:
            self._autocommit = saw_probe
        else:
            self._refresh_autocommit(query_timeout)
        return real

    def _refresh_autocommit(self, query_timeout: Optional[float] = None) -> None:
        """Refresh the cached transaction state with a standalone
        ``get_autocommit`` pipeline request. Errors are swallowed — a dead
        stream means the server rolled back, so the state resets to autocommit.
        """
        request = build_pipeline_request(self._baton, [{"type": "get_autocommit"}])
        try:
            raw = self._post("/v3/pipeline", request, None, query_timeout)
        except RuntimeError:
            self._baton = None
            self._autocommit = True
            return
        resp = json.loads(raw)
        self._baton = resp.get("baton")
        if resp.get("base_url"):
            self._base_url = resp["base_url"]
        self._update_autocommit(resp)

    def _update_autocommit(self, resp: dict) -> None:
        """Read the ``get_autocommit`` answer out of a pipeline response."""
        for result in resp.get("results") or []:
            if result.get("type") == "ok":
                response = result.get("response") or {}
                if response.get("type") == "get_autocommit" and isinstance(
                    response.get("is_autocommit"), bool
                ):
                    self._autocommit = response["is_autocommit"]
                    return

    def execute_pipeline(
        self,
        requests: list[dict],
        *,
        append_get_autocommit: bool = True,
        query_headers: Optional[dict[str, str]] = None,
        query_timeout: Optional[float] = None,
    ) -> dict:
        """Execute a pipeline request (JSON request/response).

        Appends a ``get_autocommit`` request (unless disabled), always persists
        the returned baton, and refreshes the cached transaction state.
        """
        reqs = list(requests)
        if append_get_autocommit:
            reqs.append({"type": "get_autocommit"})
        request = build_pipeline_request(self._baton, reqs)

        try:
            raw = self._post("/v3/pipeline", request, query_headers, query_timeout)
        except RuntimeError:
            self._baton = None
            self._autocommit = True
            raise

        resp: dict[str, Any] = json.loads(raw)
        self._baton = resp.get("baton")
        if resp.get("base_url"):
            self._base_url = resp["base_url"]
        if append_get_autocommit:
            self._update_autocommit(resp)
        return resp

    def close(self) -> None:
        """Close the session by sending a close request if a baton is active."""
        if self._baton is not None:
            try:
                request = build_pipeline_request(self._baton, [{"type": "close"}])
                self._post("/v3/pipeline", request)
            except Exception:
                pass
        self._baton = None
        self._base_url = ""
        self._autocommit = True
