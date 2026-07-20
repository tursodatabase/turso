"""SQLAlchemy dialect for pyturso.

This module provides SQLAlchemy integration for pyturso:
- TursoDialect: Basic local database connections (sqlite+turso://)
- AioTursoDialect: Basic local database connections for async engines (sqlite+aioturso://)
- TursoSyncDialect: Sync-enabled connections with remote support (sqlite+turso_sync://)
- get_sync_connection: Helper to access sync methods from SQLAlchemy connections

Usage:
    from sqlalchemy import create_engine, text

    # Basic local connection
    engine = create_engine("sqlite+turso:///app.db")

    # Sync-enabled connection with remote
    engine = create_engine(
        "sqlite+turso_sync:///local.db"
        "?remote_url=https://my-db.turso.io"
        "&auth_token=your-token"
    )

    # Access sync operations
    from turso.sqlalchemy import get_sync_connection

    with engine.connect() as conn:
        sync = get_sync_connection(conn)
        sync.pull()  # Pull remote changes
        result = conn.execute(text("SELECT * FROM users"))
        conn.commit()
        sync.push()  # Push local changes
"""

import re

import sqlalchemy

# SQLAlchemy 2.0.42 first shipped AsyncAdapt_dbapi_module (imported by .dialect),
# and 2.0.45 fixed SQLite reflection of multiple CHECK constraints, so 2.0.45 is
# the oldest release the dialects fully work on. This guard must run before
# importing .dialect, which fails on releases before 2.0.42 with an ImportError
# about SQLAlchemy internals instead of a clear message.
_MIN_SQLALCHEMY_VERSION = (2, 0, 45)

if tuple(int(n) for n in re.findall(r"\d+", sqlalchemy.__version__)[:3]) < _MIN_SQLALCHEMY_VERSION:
    raise ImportError(
        f"pyturso's SQLAlchemy dialects require SQLAlchemy >= 2.0.45 "
        f"(found {sqlalchemy.__version__}). Upgrade with: pip install 'sqlalchemy>=2.0.45'"
    )

from .dialect import (  # noqa: E402
    AioTursoDialect,
    TursoDialect,
    TursoServerlessDialect,
    TursoSyncDialect,
    get_sync_connection,
)

__all__ = [
    "AioTursoDialect",
    "TursoDialect",
    "TursoServerlessDialect",
    "TursoSyncDialect",
    "get_sync_connection",
]
