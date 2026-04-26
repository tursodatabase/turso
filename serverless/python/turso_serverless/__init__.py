"""Pure Python serverless Turso client (DB-API 2.0).

Connects to a Turso database over HTTP using the hrana v3 protocol.
No Rust FFI required — uses urllib.request from stdlib.

Usage:
    import turso_serverless

    conn = turso_serverless.connect("turso://my-db.turso.io", auth_token="...")
    cursor = conn.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    conn.close()
"""

from ._dbapi_common import (
    # Exception classes
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    # Helpers
    Row,
    Warning,
)
from .connection import Connection, Cursor, connect

# DB-API 2.0 module-level attributes
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

# Not available without the Rust engine
sqlite_version = "3.45.0"
sqlite_version_info = (3, 45, 0)

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Row",
    # DB-API 2.0 module attributes
    "apilevel",
    "paramstyle",
    "threadsafety",
    "sqlite_version",
    "sqlite_version_info",
    # Exception classes
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
