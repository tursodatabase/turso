"""SQLAlchemy dialects for pyturso.

This module provides two SQLAlchemy dialects:
- TursoDialect: Basic local database connections (sqlite+turso://)
- TursoSyncDialect: Sync-enabled connections with remote support (sqlite+turso_sync://)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from sqlalchemy import pool
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import URL
from sqlalchemy.engine.reflection import ObjectKind

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ConnectArgsType, ReflectedForeignKeyConstraint, ReflectedIndex
    from sqlalchemy.pool import Pool

logger = logging.getLogger(__name__)


class _TursoDialectMixin:
    """
    Mixin providing Turso-specific overrides for SQLAlchemy dialect.

    Turso doesn't support all SQLite PRAGMAs. This mixin overrides methods
    that would otherwise fail due to unsupported PRAGMAs:
    - PRAGMA foreign_key_list (not supported)
    - PRAGMA index_list (not supported)
    """

    def get_foreign_keys(
        self,
        connection,
        table_name,
        schema=None,
        **kw,
    ) -> List[ReflectedForeignKeyConstraint]:
        """
        Return foreign keys for a table.

        Turso doesn't support PRAGMA foreign_key_list, so we return an empty list.
        Foreign key constraints are still enforced at write time if defined.
        """
        logger.debug(
            "PRAGMA foreign_key_list not supported; foreign key reflection unavailable for table '%s'",
            table_name,
        )
        return []

    def get_indexes(
        self,
        connection,
        table_name,
        schema=None,
        **kw,
    ) -> List[ReflectedIndex]:
        """
        Return indexes for a table.

        Turso doesn't support PRAGMA index_list, so we return an empty list.
        Indexes still exist and are used for query optimization.
        """
        logger.debug(
            "PRAGMA index_list not supported; index reflection unavailable for table '%s'",
            table_name,
        )
        return []

    def get_unique_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kw,
    ) -> List[Dict[str, Any]]:
        """
        Return unique constraints for a table.

        This also relies on PRAGMA index_list which Turso doesn't support.
        """
        logger.debug(
            "PRAGMA index_list not supported; unique constraint reflection unavailable for table '%s'",
            table_name,
        )
        return []

    def get_check_constraints(
        self,
        connection,
        table_name,
        schema=None,
        **kw,
    ) -> List[Dict[str, Any]]:
        """
        Return check constraints for a table.

        SQLite stores these in sqlite_master which Turso may not fully support.
        """
        logger.debug(
            "check constraint reflection not supported for table '%s'",
            table_name,
        )
        return []

    def get_multi_indexes(
        self,
        connection,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=None,
        **kw,
    ) -> Dict[Any, List[ReflectedIndex]]:
        """Return indexes for multiple tables."""
        logger.debug("PRAGMA index_list not supported; multi-index reflection unavailable")
        return {}

    def get_multi_unique_constraints(
        self,
        connection,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=None,
        **kw,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Return unique constraints for multiple tables."""
        logger.debug("PRAGMA index_list not supported; multi-unique-constraint reflection unavailable")
        return {}

    def get_multi_foreign_keys(
        self,
        connection,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=None,
        **kw,
    ) -> Dict[Any, List[ReflectedForeignKeyConstraint]]:
        """Return foreign keys for multiple tables."""
        logger.debug("PRAGMA foreign_key_list not supported; multi-foreign-key reflection unavailable")
        return {}

    def get_multi_check_constraints(
        self,
        connection,
        schema=None,
        filter_names=None,
        kind=ObjectKind.TABLE,
        scope=None,
        **kw,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Return check constraints for multiple tables."""
        logger.debug("multi-check-constraint reflection not supported")
        return {}

    def get_temp_table_names(self, connection, **kw) -> List[str]:
        """Return temporary table names.

        Turso doesn't support sqlite_temp_master, so we return an empty list.
        """
        logger.debug("sqlite_temp_master not supported; temp table reflection unavailable")
        return []

    def get_temp_view_names(self, connection, **kw) -> List[str]:
        """Return temporary view names.

        Turso doesn't support sqlite_temp_master, so we return an empty list.
        """
        logger.debug("sqlite_temp_master not supported; temp view reflection unavailable")
        return []


class TursoDialect(_TursoDialectMixin, SQLiteDialect_pysqlite):
    """
    SQLAlchemy dialect for pyturso local database connections.

    This dialect uses turso.connect() for local SQLite-compatible databases.

    Usage:
        from sqlalchemy import create_engine

        # File-based database
        engine = create_engine("sqlite+turso:///path/to/database.db")

        # In-memory database
        engine = create_engine("sqlite+turso:///:memory:")

        # With options
        engine = create_engine(
            "sqlite+turso:///db.db",
            connect_args={"isolation_level": "IMMEDIATE"}
        )
    """

    name = "sqlite"
    driver = "turso"

    # Enable statement caching for better performance
    supports_statement_cache = True
    # Disable native_datetime since turso handles datetime differently
    supports_native_datetime = False

    @classmethod
    def import_dbapi(cls):
        """Import the turso module as DBAPI."""
        import turso

        return turso

    def on_connect(self):
        """
        Return a callable to run on each new connection.

        We override this to skip the REGEXP function setup that pysqlite does,
        since turso doesn't support create_function.
        """
        # Skip the parent's on_connect which tries to register REGEXP
        # Return None to indicate no special connection setup needed
        return None

    def get_isolation_level(self, dbapi_connection):
        """
        Return the current isolation level.

        Turso doesn't support PRAGMA read_uncommitted, so we return
        SERIALIZABLE as the default (which is what SQLite uses).
        """
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection, level):
        """
        Set the isolation level.

        Turso handles isolation through the isolation_level connection parameter,
        not through PRAGMA statements. This is a no-op since the isolation level
        is set at connection time.
        """
        # No-op: turso handles isolation via connection parameter
        pass

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        """
        Create connection arguments from SQLAlchemy URL.

        The URL format is:
            sqlite+turso:///path/to/database.db

        Query parameters supported:
            - isolation_level: Transaction isolation level
            - experimental_features: Comma-separated feature flags
        """
        opts = url.translate_connect_args()

        # 'database' key becomes the positional argument
        database = opts.pop("database", ":memory:")

        # Remove unsupported URL components
        opts.pop("username", None)
        opts.pop("password", None)
        opts.pop("host", None)
        opts.pop("port", None)

        # Extract query parameters
        query_params = dict(url.query)

        kwargs: Dict[str, Any] = {}

        # Handle isolation_level
        isolation_level = query_params.pop("isolation_level", None)
        if isolation_level:
            if isolation_level.upper() == "AUTOCOMMIT":
                kwargs["isolation_level"] = None
            else:
                kwargs["isolation_level"] = isolation_level

        # Handle experimental_features
        experimental_features = query_params.pop("experimental_features", None)
        if experimental_features:
            kwargs["experimental_features"] = experimental_features

        return ([database], kwargs)

    def get_pool_class(self, url: URL) -> type[Pool]:
        """Return the connection pool class."""
        if url.database == ":memory:":
            return pool.SingletonThreadPool
        return pool.QueuePool


class TursoSyncDialect(_TursoDialectMixin, SQLiteDialect_pysqlite):
    """
    SQLAlchemy dialect for pyturso sync-enabled connections.

    This dialect uses turso.sync.connect() which provides:
    - Local SQLite database with remote sync capabilities
    - pull() - Pull changes from remote
    - push() - Push changes to remote
    - checkpoint() - Checkpoint the WAL
    - stats() - Get sync statistics

    Usage:
        from sqlalchemy import create_engine

        engine = create_engine(
            "sqlite+turso_sync:///local.db"
            "?remote_url=https://your-db.turso.io"
            "&auth_token=your-token"
        )

        # Or with connect_args:
        engine = create_engine(
            "sqlite+turso_sync:///local.db",
            connect_args={
                "remote_url": "https://your-db.turso.io",
                "auth_token": "your-token",
            }
        )

        # Access sync operations:
        from turso.sqlalchemy import get_sync_connection

        with engine.connect() as conn:
            sync = get_sync_connection(conn)
            sync.pull()  # Pull remote changes
            # ... execute queries ...
            sync.push()  # Push local changes
    """

    name = "sqlite"
    driver = "turso_sync"

    # Enable statement caching for better performance
    supports_statement_cache = True
    # Disable native_datetime since turso handles datetime differently
    supports_native_datetime = False

    @classmethod
    def import_dbapi(cls):
        """Import the turso.sync module as DBAPI."""
        import turso.sync

        return turso.sync

    def connect(self, *cargs, **cparams):
        """Remap sync_url to remote_url for libsql-sqlalchemy compatibility."""
        if "sync_url" in cparams and "remote_url" not in cparams:
            cparams["remote_url"] = cparams.pop("sync_url")
        return super().connect(*cargs, **cparams)

    def on_connect(self):
        """
        Return a callable to run on each new connection.

        We override this to skip the REGEXP function setup that pysqlite does,
        since turso doesn't support create_function.
        """
        return None

    def get_isolation_level(self, dbapi_connection):
        """
        Return the current isolation level.

        Turso doesn't support PRAGMA read_uncommitted, so we return
        SERIALIZABLE as the default.
        """
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection, level):
        """
        Set the isolation level.

        Turso handles isolation through the isolation_level connection parameter.
        This is a no-op since the isolation level is set at connection time.
        """
        pass

    @staticmethod
    def _validate_sync_url(opts: Dict[str, Any]) -> None:
        """Reject URL components that TursoSyncDialect doesn't support."""
        if opts.get("username") or opts.get("password"):
            raise ValueError(
                "TursoSyncDialect does not support username/password in URL. "
                "Use auth_token query parameter or connect_args instead."
            )
        if opts.get("host") or opts.get("port"):
            raise ValueError(
                "TursoSyncDialect does not support host/port in URL. "
                "The local database path goes after ':///', and remote_url "
                "is specified as a query parameter."
            )

    @staticmethod
    def _extract_sync_params(query_params: Dict[str, str]) -> Dict[str, Any]:
        """Extract and convert sync-specific query parameters into kwargs."""
        kwargs: Dict[str, Any] = {}

        auth_token = query_params.pop("auth_token", None)
        if auth_token:
            kwargs["auth_token"] = auth_token

        client_name = query_params.pop("client_name", None)
        kwargs["client_name"] = client_name or "turso-sqlalchemy"

        long_poll_timeout_ms = query_params.pop("long_poll_timeout_ms", None)
        if long_poll_timeout_ms:
            kwargs["long_poll_timeout_ms"] = int(long_poll_timeout_ms)

        bootstrap_if_empty = query_params.pop("bootstrap_if_empty", None)
        if bootstrap_if_empty is not None:
            kwargs["bootstrap_if_empty"] = bootstrap_if_empty.lower() in (
                "true",
                "1",
                "yes",
            )

        return kwargs

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        """
        Create connection arguments from SQLAlchemy URL.

        The URL format is:
            sqlite+turso_sync:///path/to/local.db?remote_url=...&auth_token=...

        Query parameters:
            - remote_url (required): Remote Turso/libsql server URL
            - auth_token: Authentication token
            - client_name: Client identifier (default: turso-sqlalchemy)
            - long_poll_timeout_ms: Long poll timeout in milliseconds
            - bootstrap_if_empty: Bootstrap from remote if local empty (default: true)
            - isolation_level: Transaction isolation level
            - experimental_features: Comma-separated feature flags
        """
        opts = url.translate_connect_args()
        path = opts.pop("database", ":memory:")
        self._validate_sync_url(opts)

        query_params = dict(url.query)
        # Accept both remote_url and sync_url (libsql-sqlalchemy compat)
        remote_url = query_params.pop("remote_url", None) or query_params.pop("sync_url", None)
        kwargs = self._extract_sync_params(query_params)

        # Handle isolation_level
        isolation_level = query_params.pop("isolation_level", None)
        if isolation_level:
            if isolation_level.upper() == "AUTOCOMMIT":
                kwargs["isolation_level"] = None
            else:
                kwargs["isolation_level"] = isolation_level

        # Handle experimental_features
        experimental_features = query_params.pop("experimental_features", None)
        if experimental_features:
            kwargs["experimental_features"] = experimental_features

        # Warn about unused query parameters
        if query_params:
            import warnings

            warnings.warn(
                f"Unrecognized query parameters ignored: {list(query_params.keys())}",
                UserWarning,
                stacklevel=2,
            )

        # Return (args, kwargs) for turso.sync.connect(path, remote_url, **kwargs)
        if remote_url:
            return ([path, remote_url], kwargs)
        else:
            # If no remote_url provided, let turso.sync.connect raise the error
            # This allows connect_args to provide remote_url instead
            return ([path], kwargs)

    def get_pool_class(self, url: URL) -> type[Pool]:
        """
        Return the connection pool class.

        For sync connections with file databases, use QueuePool.
        For :memory: databases, use SingletonThreadPool.
        """
        if url.database == ":memory:":
            return pool.SingletonThreadPool
        return pool.QueuePool


def get_sync_connection(connection):
    """
    Get the underlying turso.sync.ConnectionSync from a SQLAlchemy connection.

    This provides access to sync-specific methods:
    - pull() - Pull changes from remote, returns True if updates were pulled
    - push() - Push changes to remote
    - checkpoint() - Checkpoint the WAL
    - stats() - Get sync statistics

    Usage:
        from turso.sqlalchemy import get_sync_connection

        with engine.connect() as conn:
            sync = get_sync_connection(conn)

            # Pull latest changes before querying
            sync.pull()

            result = conn.execute(text("SELECT * FROM users"))

            # After modifications, push to remote
            conn.execute(text("INSERT INTO users ..."))
            conn.commit()
            sync.push()

    Args:
        connection: A SQLAlchemy Connection object

    Returns:
        The underlying turso.sync.ConnectionSync object

    Raises:
        TypeError: If the connection is not a Turso sync connection
    """
    from turso.lib_sync import ConnectionSync

    # Get the raw DBAPI connection
    # SQLAlchemy 2.0: connection.connection.dbapi_connection
    # SQLAlchemy 1.4: connection.connection
    raw_conn = getattr(connection, "connection", None)
    if raw_conn is None:
        raise TypeError("Cannot get raw connection from SQLAlchemy connection")

    # Handle SQLAlchemy 2.0 pooled connection wrapper
    dbapi_conn = getattr(raw_conn, "dbapi_connection", raw_conn)

    if not isinstance(dbapi_conn, ConnectionSync):
        raise TypeError(
            f"Expected turso.sync.ConnectionSync, got {type(dbapi_conn).__name__}. "
            "This function only works with sqlite+turso_sync:// connections."
        )

    return dbapi_conn
