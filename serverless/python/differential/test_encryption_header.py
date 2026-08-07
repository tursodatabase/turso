"""Property test for the remote encryption key header (PROTOCOL.md section
3.1, `tests.encryption_header` in the shared spec).

For any key drawn from the spec's alphabet, a driver configured with it
must attach `x-turso-encryption-key: <key>` to every HTTP request — on both
the pipeline and the cursor endpoint — and a driver with no key must never
send the header.

Unlike the parity tests, this property runs against a local stub HTTP
server that records request headers and speaks just enough of the protocol
for a statement to complete. It needs no live database, no pyturso, and no
environment configuration: it never skips.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings
from turso_serverless import connect
from turso_serverless.session import ENCRYPTION_KEY_HEADER

_SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "conformance" / "differential" / "spec" / "ops.json"
)
with open(_SPEC_PATH) as _f:
    _SPEC = json.load(_f)

_ENC_SPEC = _SPEC["tests"]["encryption_header"]


# ---------------------------------------------------------------------------
# Stub server: records the encryption-key header of every request and
# answers both endpoints per PROTOCOL.md sections 5 and 7.
# ---------------------------------------------------------------------------


def _pipeline_result(request: dict) -> dict:
    """One `ok` result per pipeline request (section 5.2)."""
    rtype = request.get("type")
    if rtype == "execute":
        response = {
            "type": "execute",
            "result": {
                "cols": [],
                "rows": [],
                "affected_row_count": 0,
                "last_insert_rowid": None,
            },
        }
    elif rtype == "get_autocommit":
        response = {"type": "get_autocommit", "is_autocommit": True}
    else:
        # sequence, close, ... — a bare response echoing the type suffices.
        response = {"type": rtype}
    return {"type": "ok", "response": response}


def _pipeline_body(body: dict) -> bytes:
    resp = {
        "baton": None,
        "base_url": None,
        "results": [_pipeline_result(r) for r in body.get("requests", [])],
    }
    return json.dumps(resp).encode("utf-8")


def _cursor_body(body: dict) -> bytes:
    """A JSON-lines cursor response (section 7.2): every step — including
    the driver's trailing autocommit probe — begins and ends successfully,
    with one row for steps that want rows."""
    lines = [{"baton": None, "base_url": None}]
    for i, step in enumerate(body.get("batch", {}).get("steps", [])):
        lines.append({"type": "step_begin", "step": i, "cols": [{"name": "c", "decltype": None}]})
        if step.get("stmt", {}).get("want_rows", True):
            lines.append({"type": "row", "row": [{"type": "integer", "value": "1"}]})
        lines.append({"type": "step_end", "affected_row_count": 0, "last_insert_rowid": None})
    lines.append({"type": "replication_index", "replication_index": None})
    return "\n".join(json.dumps(line) for line in lines).encode("utf-8")


class _StubHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        # email.message.Message lookup is case-insensitive; missing → None.
        self.server.requests.append(
            (self.path, self.headers.get(_ENC_SPEC["header"]))
        )
        if self.path == "/v3/pipeline":
            payload = _pipeline_body(body)
        elif self.path == "/v3/cursor":
            payload = _cursor_body(body)
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:
        pass


class _StubServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _StubHandler)
        # (path, encryption-key header value or None) per request received.
        self.requests: list[tuple[str, str | None]] = []

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


@pytest.fixture(scope="module")
def stub_server():
    server = _StubServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join()


def _run_statements(server: _StubServer, key: str | None) -> list[tuple[str, str | None]]:
    """Run one query-style statement (cursor endpoint) and one exec-style
    script (pipeline endpoint), returning the requests the server saw."""
    server.requests.clear()
    conn = connect(server.url, auth_token="test-token", remote_encryption_key=key)
    try:
        assert conn.execute("SELECT 1").fetchall() == [(1,)]
        conn.executescript("CREATE TABLE t (x)")
    finally:
        conn.close()
    return list(server.requests)


# ---------------------------------------------------------------------------
# The property
# ---------------------------------------------------------------------------

encryption_keys = st.tuples(
    st.text(
        alphabet=_ENC_SPEC["key_alphabet"],
        min_size=_ENC_SPEC["key_min_len"],
        max_size=_ENC_SPEC["key_max_len"],
    ),
    st.sampled_from(["", "=", "=="]),
).map(lambda parts: parts[0] + parts[1])


def test_header_name_matches_spec():
    assert ENCRYPTION_KEY_HEADER == _ENC_SPEC["header"]


@given(key=encryption_keys)
@settings(max_examples=_ENC_SPEC["num_examples"], deadline=None, database=None)
def test_encryption_key_sent_on_every_request(stub_server, key):
    requests = _run_statements(stub_server, key)
    assert {path for path, _ in requests} == {"/v3/cursor", "/v3/pipeline"}
    for path, header_value in requests:
        assert header_value == key, (
            f"request to {path} carried {_ENC_SPEC['header']}={header_value!r}, "
            f"expected {key!r}"
        )


def test_no_key_sends_no_header(stub_server):
    requests = _run_statements(stub_server, None)
    assert {path for path, _ in requests} == {"/v3/cursor", "/v3/pipeline"}
    for path, header_value in requests:
        assert header_value is None, (
            f"request to {path} carried {_ENC_SPEC['header']}={header_value!r} "
            f"with no key configured"
        )
