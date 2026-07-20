"""Hrana v3 protocol encoding/decoding.

Ported from remote/src/protocol.rs. Uses plain dicts for JSON serialization.
"""

from __future__ import annotations

import base64
from typing import Any, Optional


def encode_value(value: Any) -> dict:
    """Encode a Python value to a hrana ProtoValue dict."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": str(int(value))}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    if isinstance(value, str):
        return {"type": "text", "value": value}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Unsupported value type: {type(value).__name__}")


def decode_value(pv: dict) -> Any:  # noqa: C901
    """Decode a hrana ProtoValue dict to a Python value."""
    typ = pv.get("type", "null")
    if typ == "null":
        return None
    if typ == "integer":
        v = pv.get("value")
        if isinstance(v, str):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        return None
    if typ == "float":
        v = pv.get("value")
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            return float(v)
        return 0.0
    if typ == "text":
        return pv.get("value", "")
    if typ == "blob":
        b64 = pv.get("base64")
        if b64 is not None:
            # Server may return base64 with or without padding
            try:
                return base64.b64decode(b64)
            except Exception:
                # Add padding and retry
                padded = b64 + "=" * (-len(b64) % 4)
                return base64.b64decode(padded)
        return b""
    return None


def build_batch_step(
    sql: str,
    args: Optional[list] = None,
    named_args: Optional[list[tuple[str, Any]]] = None,
    want_rows: bool = True,
    condition: Optional[dict] = None,
) -> dict:
    """Build a batch step for a cursor request."""
    encoded_args = [encode_value(a) for a in args] if args else []
    encoded_named = (
        [{"name": name, "value": encode_value(val)} for name, val in named_args]
        if named_args
        else []
    )
    step: dict = {
        "stmt": {
            "sql": sql,
            "args": encoded_args,
            "named_args": encoded_named,
            "want_rows": want_rows,
        },
    }
    if condition is not None:
        step["condition"] = condition
    return step


def build_cursor_request(baton: Optional[str], steps: list[dict]) -> dict:
    """Build a cursor request body."""
    return {
        "baton": baton,
        "batch": {"steps": steps},
    }


def build_pipeline_request(
    baton: Optional[str],
    requests: list[dict],
) -> dict:
    """Build a pipeline request body."""
    return {
        "baton": baton,
        "requests": requests,
    }
