"""A stand-in for the stdlib `sqlite3` module that opens connections with `turso`.

ORM backends import this module in place of `sqlite3`. It adds only what is
needed to open a connection: sqlite3 constants, and no-op versions of the
sqlite3 APIs that `turso` lacks (user functions, adapters, converters, ...).
The no-ops do not emulate anything, so a test that depends on one of them
fails on its own ("no such function: ...", "unexpected parameter value")
instead of every connection failing. The calls are counted in MISSING_CALLS.
"""

import collections
import os

import turso
from turso import *  # noqa: F401,F403
from turso import Cursor, NotSupportedError, Row  # noqa: F401

PARSE_DECLTYPES = 1
PARSE_COLNAMES = 2
SQLITE_LIMIT_LENGTH = 0
SQLITE_LIMIT_COLUMN = 2
SQLITE_LIMIT_VARIABLE_NUMBER = 9
# SQLite's compile-time defaults, reported by the getlimit() stub.
_DEFAULT_LIMITS = {SQLITE_LIMIT_LENGTH: 1_000_000_000, SQLITE_LIMIT_COLUMN: 2000, SQLITE_LIMIT_VARIABLE_NUMBER: 32766}

MISSING_CALLS = collections.Counter()
EXPERIMENTAL_FEATURES = os.environ.get("TURSO_EXPERIMENTAL_FEATURES") or None


def _missing(name, result=None):
    def stub(*args, **kwargs):
        MISSING_CALLS[name] += 1
        return result

    stub.__name__ = name
    return stub


register_adapter = _missing("register_adapter")
register_converter = _missing("register_converter")
enable_callback_tracebacks = _missing("enable_callback_tracebacks")


class Connection(turso.Connection):
    create_function = _missing("create_function")
    create_aggregate = _missing("create_aggregate")
    create_window_function = _missing("create_window_function")
    create_collation = _missing("create_collation")
    set_trace_callback = _missing("set_trace_callback")
    set_authorizer = _missing("set_authorizer")
    set_progress_handler = _missing("set_progress_handler")
    enable_load_extension = _missing("enable_load_extension")

    def getlimit(self, category):
        MISSING_CALLS["getlimit"] += 1
        return _DEFAULT_LIMITS[category]


# sqlite3.connect() arguments that turso.connect() does not accept. They are
# dropped: `uri` because "file:" names are rejected below, `timeout` and
# `check_same_thread` because they do not change results in single-threaded tests.
_IGNORED_CONNECT_ARGS = ("detect_types", "check_same_thread", "uri", "timeout", "cached_statements", "factory")


def connect(database, isolation_level="", autocommit="LEGACY", **kwargs):
    database = os.fspath(database)
    if kwargs.get("uri") and database.startswith("file:"):
        # turso would open a file literally named "file:...?mode=ro".
        MISSING_CALLS["connect(uri=True)"] += 1
        raise NotSupportedError("turso does not support URI filenames")
    for name in _IGNORED_CONNECT_ARGS:
        kwargs.pop(name, None)
    if kwargs:
        raise TypeError(f"turso_sqlite3.connect: unsupported arguments {sorted(kwargs)}")
    conn = turso.connect(database, experimental_features=EXPERIMENTAL_FEATURES, isolation_level=isolation_level)
    conn.__class__ = Connection
    if autocommit != "LEGACY":
        conn.autocommit = autocommit
    return conn
