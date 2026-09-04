"""
Stub for antithesis.assertions — converts always() failures into
non-zero exits so the parent runner can detect them.
"""
import json
import os
import sys


def _emit(kind: str, message: str, details: dict) -> None:
    record = {
        "kind": kind,
        "pid": os.getpid(),
        "message": message,
        "details": details,
    }
    sys.stderr.write("ANTITHESIS_FAIL " + json.dumps(record) + "\n")
    sys.stderr.flush()


def always(condition: bool, message: str, details: dict) -> None:
    if not condition:
        _emit("always", message, details)
        # Hard-stop the worker so the parent runner sees a non-zero exit.
        os._exit(91)


def sometimes(condition: bool, message: str, details: dict) -> None:
    # Stub — antithesis tracks coverage; we just no-op locally.
    pass


def reachable(message: str, details: dict) -> None:
    pass


def unreachable(message: str, details: dict) -> None:
    _emit("unreachable", message, details)
    os._exit(92)
