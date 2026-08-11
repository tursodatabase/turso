"""Pure Python serverless Turso client (DB-API 2.0).

Connects to a Turso database over HTTP using the SQL over HTTP protocol.
No Rust FFI required — uses urllib.request from stdlib.

Usage:
    import turso_serverless

    conn = turso_serverless.connect("libsql://my-db.turso.io", auth_token="...")
    cursor = conn.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    conn.close()
"""

from .connection import Connection, Cursor, connect
from .dbapi import (
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

# DB-API 2.0 module-level attributes
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Row",
    # DB-API 2.0 module attributes
    "apilevel",
    "paramstyle",
    "threadsafety",
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
