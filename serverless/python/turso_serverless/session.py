"""HTTP session with baton management for hrana v3.

Ported from remote/src/session.rs. Uses urllib.request (stdlib, no extra deps).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Optional

from .protocol import build_cursor_request, build_pipeline_request


def normalize_url(url: str) -> str:
    """Rewrite turso:// and libsql:// to https://."""
    if url.startswith("libsql://"):
        return "https://" + url[len("libsql://"):]
    if url.startswith("turso://"):
        return "https://" + url[len("turso://"):]
    return url


class Session:
    """Manages connection state (baton) with a remote Turso server."""

    def __init__(self, url: str, auth_token: Optional[str] = None) -> None:
        self._auth_token = auth_token
        self._baton: Optional[str] = None
        self._base_url = normalize_url(url)
        self.keep_alive: bool = False

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"
        return headers

    def _post(self, path: str, body: dict) -> bytes:
        """POST JSON to the server and return raw response bytes."""
        url = f"{self._base_url}{path}"
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            response_body = e.read().decode("utf-8", errors="replace")
            # Try to extract message from JSON error body
            try:
                parsed = json.loads(response_body)
                msg = parsed.get("message", response_body)
            except (json.JSONDecodeError, KeyError):
                msg = response_body or f"HTTP error! status: {e.code}"
            raise RuntimeError(msg) from None

    def execute_cursor(self, steps: list[dict]) -> list[dict]:
        """Execute a cursor request (streaming NDJSON for queries with rows).

        Returns a list of CursorEntry dicts.
        """
        baton = self._baton if self.keep_alive else None
        request = build_cursor_request(baton, steps)
        self._baton = None  # consumed

        raw = self._post("/v3/cursor", request)
        text = raw.decode("utf-8")
        lines = [line for line in text.splitlines() if line.strip()]

        if not lines:
            raise RuntimeError("No cursor response received")

        # First line: CursorResponse (baton + optional base_url)
        cursor_resp = json.loads(lines[0])
        if self.keep_alive:
            self._baton = cursor_resp.get("baton")
        else:
            self._baton = None
        if cursor_resp.get("base_url"):
            self._base_url = cursor_resp["base_url"]

        # Remaining lines: CursorEntry objects
        entries: list[dict] = []
        for line in lines[1:]:
            entries.append(json.loads(line))
        return entries

    def execute_pipeline(self, requests: list[dict]) -> dict:
        """Execute a pipeline request (JSON request/response).

        Returns the PipelineResponse dict.
        """
        reqs = list(requests)
        if not self.keep_alive:
            # Append close unless the requests already include one.
            if not any(r.get("type") == "close" for r in requests):
                reqs.append({"type": "close"})
        baton = self._baton if self.keep_alive else None
        request = build_pipeline_request(baton, reqs)
        self._baton = None  # consumed

        raw = self._post("/v3/pipeline", request)
        resp: dict[str, Any] = json.loads(raw)

        if self.keep_alive:
            self._baton = resp.get("baton")
        else:
            self._baton = None
        if resp.get("base_url"):
            self._base_url = resp["base_url"]

        return resp

    def close(self) -> None:
        """Close the session by sending a close request if baton is active."""
        if self._baton is not None:
            try:
                self.execute_pipeline([{"type": "close"}])
            except Exception:
                pass
        self._baton = None
