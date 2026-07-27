"""HTTP session management: one session per connection, tracking the
stream baton, base URL, and transaction state."""

from __future__ import annotations

import http.client
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional

from .protocol import ProtocolError, ServerError, build_batch_step, decode_value


def normalize_url(url: str) -> str:
    """Rewrite libsql:// and turso:// URLs to https:// and strip any
    trailing slashes, since endpoint paths are appended with a leading
    slash."""
    if url.startswith("libsql://"):
        url = "https://" + url[len("libsql://"):]
    elif url.startswith("turso://"):
        url = "https://" + url[len("turso://"):]
    return url.rstrip("/")


def _server_error(error: Optional[dict]) -> ServerError:
    """Build a ServerError from a protocol error object (section 9.1),
    tolerating missing fields."""
    message = "unknown error"
    code = None
    extended_code = None
    if isinstance(error, dict):
        if isinstance(error.get("message"), str):
            message = error["message"]
        if isinstance(error.get("code"), str):
            code = error["code"]
        if isinstance(error.get("extended_code"), str):
            extended_code = error["extended_code"]
    return ServerError(message, code=code, extended_code=extended_code)


@dataclass
class StmtResult:
    """The decoded output of one executed statement."""

    columns: list[str]
    rows: list[tuple]
    affected_rows: int
    last_insert_rowid: Optional[int]


# Index of the autocommit probe step appended to every cursor batch.
_PROBE_STEP = 1


def _parse_cursor_body(raw: bytes) -> tuple[dict, list[dict]]:
    """Split a cursor response body (section 7.1) into the cursor response
    line and the decoded entries that follow it."""
    lines = [line for line in raw.decode("utf-8").splitlines() if line.strip()]
    if not lines:
        raise ProtocolError("cursor response body ended before the cursor response line")
    try:
        cursor_resp = json.loads(lines[0])
    except ValueError as e:
        raise ProtocolError(f"invalid cursor response: {e}") from None
    entries = []
    for line in lines[1:]:
        try:
            entries.append(json.loads(line))
        except ValueError as e:
            raise ProtocolError(f"invalid cursor entry: {e}") from None
    return cursor_resp, entries


class _CursorDecoder:
    """Folds streamed cursor entries (section 7.2) into a statement result,
    tracking the trailing autocommit probe step separately from the caller's
    step."""

    def __init__(self) -> None:
        self.result = StmtResult(columns=[], rows=[], affected_rows=0, last_insert_rowid=None)
        self.step_error: Optional[ServerError] = None
        self.fatal: Optional[ServerError] = None
        self.probe_executed = False
        self.probe_unreliable = False
        self._in_probe = False

    def feed(self, entry: dict) -> None:
        etype = entry.get("type")
        if etype == "step_begin":
            self._step_begin(entry)
        elif etype == "row":
            self._row(entry)
        elif etype == "step_end":
            self._step_end(entry)
        elif etype == "step_error":
            self._step_error(entry)
        elif etype == "error":
            self.fatal = _server_error(entry.get("error"))
        # replication_index (section 7.2.6) and unknown entries are
        # ignored; a cleanly ended body is complete without a terminator.

    def _step_begin(self, entry: dict) -> None:
        if entry.get("step") == _PROBE_STEP:
            self._in_probe = True
            self.probe_executed = True
        else:
            self._in_probe = False
            self.result.columns = [c.get("name") or "" for c in entry.get("cols") or []]

    def _row(self, entry: dict) -> None:
        if self._in_probe or self.step_error is not None:
            return
        self.result.rows.append(tuple(decode_value(v) for v in entry.get("row") or []))

    def _step_end(self, entry: dict) -> None:
        if self._in_probe:
            return
        self.result.affected_rows = entry.get("affected_row_count") or 0
        rowid = entry.get("last_insert_rowid")
        if rowid is not None:
            try:
                self.result.last_insert_rowid = int(rowid)
            except (TypeError, ValueError) as e:
                raise ProtocolError(f"invalid rowid in server response: {e}") from None

    def _step_error(self, entry: dict) -> None:
        if entry.get("step") == _PROBE_STEP:
            self.probe_unreliable = True
        elif self.step_error is None:
            self.step_error = _server_error(entry.get("error"))
        self._in_probe = False


class Session:
    """Manages one server-side stream: the baton, the base URL, and the
    server-reported transaction state."""

    def __init__(self, url: str, auth_token: Optional[str] = None) -> None:
        self._auth_token = auth_token
        self._base_url = normalize_url(url)
        self._baton: Optional[str] = None
        # Whether the connection is in autocommit mode, from the server's
        # most recent authoritative answer. A non-null baton does NOT imply
        # a transaction (section 4.3), so this is tracked explicitly: every
        # pipeline carries a `get_autocommit` request and every cursor batch
        # a trailing step gated on `is_autocommit`.
        self._autocommit = True

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    def _reset_stream(self) -> None:
        """Forget the stream. Any transaction on it was (or will be) rolled
        back by the server, so the connection is back in autocommit mode."""
        self._baton = None
        self._autocommit = True

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"
        return headers

    def _post(self, path: str, body: dict) -> bytes:
        """POST a request that carries the current baton. Any failure,
        including a non-200 response, is fatal for the stream (section 9.3):
        the error surfaces to the caller and the next statement starts a
        fresh stream. The driver never re-sends a request on its own;
        whether re-running a failed statement is safe is the application's
        call."""
        url = f"{self._base_url}{path}"
        # allow_nan=False so a non-finite float can never leave the client
        # as the invalid JSON tokens NaN/Infinity (section 8.2).
        data = json.dumps(body, allow_nan=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            self._reset_stream()
            message = None
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    for key in ("error", "message"):
                        if isinstance(parsed.get(key), str):
                            message = parsed[key]
                            break
            except ValueError:
                pass
            if message is not None:
                raise ProtocolError(f"HTTP status {e.code}: {message}") from None
            raise ProtocolError(f"HTTP status {e.code}") from None
        except urllib.error.URLError as e:
            self._reset_stream()
            raise ProtocolError(f"request to {url} failed: {e.reason}") from None
        except (http.client.HTTPException, OSError) as e:
            # Reading the body can fail after urlopen returned, e.g. with
            # IncompleteRead on a truncated chunked response or a connection
            # reset; URLError does not cover these, but they are equally
            # fatal for the stream.
            self._reset_stream()
            raise ProtocolError(f"request to {url} failed: {e!r}") from None

    def _update_stream(self, baton: Optional[str], base_url: Optional[str]) -> None:
        self._baton = baton
        if base_url:
            self._base_url = normalize_url(base_url)

    def execute_pipeline(self, requests: list[dict], track_autocommit: bool = True) -> list[dict]:
        """Execute a pipeline (section 5). When `track_autocommit` is set, a
        `get_autocommit` request is appended and its answer refreshes the
        cached transaction state; the returned results cover only the
        caller's requests."""
        reqs = list(requests)
        if track_autocommit:
            reqs.append({"type": "get_autocommit"})
        raw = self._post("/v3/pipeline", {"baton": self._baton, "requests": reqs})
        try:
            resp: dict[str, Any] = json.loads(raw)
        except ValueError as e:
            self._reset_stream()
            raise ProtocolError(f"invalid pipeline response: {e}") from None
        self._update_stream(resp.get("baton"), resp.get("base_url"))
        results = resp.get("results") or []
        # The protocol guarantees one result per request (section 5.2); a
        # mismatch would misattribute results to the wrong requests.
        if len(results) != len(reqs):
            raise ProtocolError(
                f"pipeline response has {len(results)} results for {len(reqs)} requests"
            )
        if track_autocommit:
            result = results.pop()
            if result.get("type") == "error":
                raise _server_error(result.get("error"))
            response = result.get("response") or {}
            if response.get("type") != "get_autocommit" or not isinstance(
                response.get("is_autocommit"), bool
            ):
                raise ProtocolError(
                    f"expected get_autocommit result in pipeline response, got {response}"
                )
            self._autocommit = response["is_autocommit"]
        return results

    def _refresh_autocommit(self) -> None:
        """Refresh the cached transaction state with a standalone
        `get_autocommit` request. Failures are swallowed: this runs on error
        paths where the original failure must not be masked, and a dead
        stream already reset the state to autocommit."""
        try:
            self.execute_pipeline([], track_autocommit=True)
        except Exception:
            pass

    @staticmethod
    def _autocommit_probe_step() -> dict:
        """The trailing batch step appended to every cursor batch: a no-op
        statement gated on `is_autocommit`. The cursor endpoint cannot carry
        a `get_autocommit` request, so whether this step executed tells us
        the connection's transaction state without an extra round trip."""
        return build_batch_step("SELECT 1", want_rows=False, condition={"type": "is_autocommit"})

    def execute_stmt(
        self,
        sql: str,
        args: Optional[list] = None,
        named_args: Optional[list[tuple[str, Any]]] = None,
        want_rows: bool = True,
    ) -> StmtResult:
        """Execute a single statement on the cursor endpoint (section 7),
        decoding the streamed entries into a buffered result."""
        steps = [
            build_batch_step(sql, args=args, named_args=named_args, want_rows=want_rows),
            self._autocommit_probe_step(),
        ]
        raw = self._post("/v3/cursor", {"baton": self._baton, "batch": {"steps": steps}})
        decoder = _CursorDecoder()
        try:
            cursor_resp, entries = _parse_cursor_body(raw)
            self._update_stream(cursor_resp.get("baton"), cursor_resp.get("base_url"))
            for entry in entries:
                decoder.feed(entry)
                if decoder.fatal is not None:
                    break
        except ProtocolError:
            # A malformed body or value from the server; abandon the stream
            # rather than resume it in an unknown position.
            self._reset_stream()
            raise
        if decoder.fatal is not None:
            # The cursor died but the stream may survive; ask the server
            # for the authoritative transaction state.
            self._refresh_autocommit()
            raise decoder.step_error or decoder.fatal
        if decoder.probe_unreliable:
            self._refresh_autocommit()
        else:
            self._autocommit = decoder.probe_executed
        if decoder.step_error is not None:
            raise decoder.step_error
        return decoder.result

    def close(self) -> None:
        """Close the stream (section 6.8). Errors are ignored: the stream
        may already have expired."""
        if self._baton is not None:
            try:
                self.execute_pipeline([{"type": "close"}], track_autocommit=False)
            except Exception:
                pass
        self._reset_stream()
