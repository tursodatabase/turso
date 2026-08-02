"""Value encoding and request building for the SQL over HTTP protocol."""

from __future__ import annotations

import base64
import math
from typing import Any, Optional


class ServerError(RuntimeError):
    """An error the server reported for a statement or stream request,
    carrying the machine-readable error codes (section 9.1) when the
    server sent them."""

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        extended_code: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.extended_code = extended_code


class ProtocolError(RuntimeError):
    """A transport failure or a response that violates the protocol."""


def encode_value(value: Any) -> dict:
    """Encode a Python value to a protocol value (section 8)."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": str(int(value))}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        # The protocol forbids sending non-finite floats (section 8.2).
        if math.isnan(value):
            # SQLite binds NaN as NULL; JSON cannot carry it.
            return {"type": "null"}
        if math.isinf(value):
            raise ValueError("infinite float values cannot be sent over the protocol")
        return {"type": "float", "value": value}
    if isinstance(value, str):
        return {"type": "text", "value": value}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Unsupported value type: {type(value).__name__}")


def decode_value(pv: dict) -> Any:
    """Decode a protocol value (section 8) to a Python value."""
    try:
        typ = pv["type"]
        if typ == "null":
            return None
        if typ == "integer":
            return int(pv["value"])
        if typ == "float":
            raw = pv["value"]
            # A null value encodes a non-finite float (section 8.2); the
            # spec says to decode it as NaN.
            if raw is None:
                return math.nan
            return float(raw)
        if typ == "text":
            return pv["value"]
        if typ == "blob":
            # The server may omit base64 padding (section 8).
            b64 = pv["base64"]
            return base64.b64decode(b64 + "=" * (-len(b64) % 4))
    except (KeyError, TypeError, ValueError) as e:
        raise ProtocolError(f"invalid value in server response: {e}") from None
    raise ProtocolError(f"unsupported value type in server response: {typ!r}")


def build_batch_step(
    sql: str,
    args: Optional[list] = None,
    named_args: Optional[list[tuple[str, Any]]] = None,
    want_rows: bool = True,
    condition: Optional[dict] = None,
) -> dict:
    """Build a batch step for a cursor request (section 7)."""
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
