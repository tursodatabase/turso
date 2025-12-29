"""SQLAlchemy dialect for pyturso.

This module provides SQLAlchemy integration for pyturso:
- TursoDialect: Basic local database connections (sqlite+turso://)
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

from .dialect import TursoDialect, TursoSyncDialect, get_sync_connection

__all__ = [
    "TursoDialect",
    "TursoSyncDialect",
    "get_sync_connection",
]
