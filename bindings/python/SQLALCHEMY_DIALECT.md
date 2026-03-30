# SQLAlchemy Dialect for Pyturso

This document describes the SQLAlchemy dialect implementation for pyturso.

## Status: Implemented

The SQLAlchemy dialect is fully implemented with two dialects:
- `sqlite+turso://` - Basic local database connections
- `sqlite+turso_sync://` - Sync-enabled connections with remote database support

## Installation

```bash
pip install pyturso[sqlalchemy]
```

## Quick Start

### Basic Local Connection

```python
from sqlalchemy import create_engine, text

# In-memory database
engine = create_engine("sqlite+turso:///:memory:")

# File-based database
engine = create_engine("sqlite+turso:///path/to/database.db")

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
    conn.execute(text("INSERT INTO users (name) VALUES ('Alice')"))
    conn.commit()

    result = conn.execute(text("SELECT * FROM users"))
    for row in result:
        print(row)
```

### Sync-Enabled Connection (Remote Sync)

```python
from sqlalchemy import create_engine, text
from turso.sqlalchemy import get_sync_connection

# Via URL query parameters
engine = create_engine(
    "sqlite+turso_sync:///local.db"
    "?remote_url=https://your-db.turso.io"
    "&auth_token=your-token"
)

# Or via connect_args (supports callables for dynamic tokens)
engine = create_engine(
    "sqlite+turso_sync:///local.db",
    connect_args={
        "remote_url": "https://your-db.turso.io",
        "auth_token": lambda: get_fresh_token(),
    }
)

with engine.connect() as conn:
    # Access sync operations
    sync = get_sync_connection(conn)
    sync.pull()  # Pull changes from remote

    result = conn.execute(text("SELECT * FROM users"))

    conn.execute(text("INSERT INTO users (name) VALUES ('Bob')"))
    conn.commit()
    sync.push()  # Push changes to remote
```

### ORM Usage

```python
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(100))

engine = create_engine("sqlite+turso:///:memory:")
Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="Alice"))
    session.commit()

    users = session.query(User).all()
```

## URL Formats

### Basic Dialect (`sqlite+turso://`)

```
sqlite+turso:///path/to/database.db
sqlite+turso:///:memory:
sqlite+turso:///db.db?isolation_level=IMMEDIATE
```

Query parameters:
- `isolation_level` - Transaction isolation level (DEFERRED, IMMEDIATE, EXCLUSIVE, AUTOCOMMIT)
- `experimental_features` - Comma-separated feature flags

### Sync Dialect (`sqlite+turso_sync://`)

```
sqlite+turso_sync:///local.db?remote_url=https://db.turso.io&auth_token=xxx
```

Query parameters:
- `remote_url` (required) - Remote Turso/libsql server URL
- `auth_token` - Authentication token
- `client_name` - Client identifier (default: turso-sqlalchemy)
- `long_poll_timeout_ms` - Long poll timeout in milliseconds
- `bootstrap_if_empty` - Bootstrap from remote if local empty (default: true)
- `isolation_level` - Transaction isolation level
- `experimental_features` - Comma-separated feature flags

URL validation:
- Username/password in URL raises `ValueError` (use `auth_token` instead)
- Host/port in URL raises `ValueError` (use `remote_url` query param instead)
- Unrecognized query parameters emit a `UserWarning`

## Sync Operations

The `get_sync_connection()` helper provides access to sync-specific methods:

```python
from turso.sqlalchemy import get_sync_connection

with engine.connect() as conn:
    sync = get_sync_connection(conn)

    # Pull changes from remote (returns True if updates were pulled)
    if sync.pull():
        print("Pulled new changes!")

    # Push local changes to remote
    sync.push()

    # Checkpoint the WAL
    sync.checkpoint()

    # Get sync statistics
    stats = sync.stats()
    print(f"Network received: {stats.network_received_bytes} bytes")
```

`get_sync_connection()` raises `TypeError` if called on a non-sync connection (e.g. a plain `sqlite+turso://` or standard `sqlite://` engine).

## Architecture

```
_TursoDialectMixin (reflection overrides)
        │
        │   SQLiteDialect_pysqlite (SQLAlchemy built-in)
        │           │
        ├───────────┤
        │           │
        ├── TursoDialect (sqlite+turso://)
        │       ├── uses turso.connect()
        │       └── pool: SingletonThreadPool (:memory:) / QueuePool (file)
        │
        └── TursoSyncDialect (sqlite+turso_sync://)
                ├── uses turso.sync.connect()
                ├── pool: SingletonThreadPool (:memory:) / QueuePool (file)
                └── get_sync_connection() → ConnectionSync (pull/push/checkpoint/stats)
```

Both dialects use Python MRO: `_TursoDialectMixin` provides PRAGMA-related overrides, `SQLiteDialect_pysqlite` provides core SQLite dialect behavior.

## What Pyturso Provides

| Requirement | Status |
|-------------|--------|
| `apilevel = "2.0"` | Provided |
| `threadsafety = 1` | Provided |
| `paramstyle = "qmark"` | Provided |
| `sqlite_version` | Provided |
| `sqlite_version_info` | Provided |
| `connect()` function | Provided |
| `Connection` class | Provided |
| `Cursor` class | Provided |
| Exception hierarchy | Provided |

Both `turso` and `turso.sync` modules expose the full DB-API 2.0 interface including exception hierarchy (`Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`).

## Dialect Overrides

Both dialects share these overrides via `_TursoDialectMixin` and direct method implementations:

### Class Attributes

- `supports_statement_cache = True` - Enables SQLAlchemy statement caching for performance
- `supports_native_datetime = False` - Turso handles datetime as strings, not native types

### Method Overrides

- `import_dbapi()` - Returns `turso` or `turso.sync` module
- `create_connect_args()` - Parses URL to connection arguments
- `on_connect()` - Returns `None` (skips REGEXP function setup that pysqlite does, since turso doesn't support `create_function`)
- `get_isolation_level()` - Returns `SERIALIZABLE` (turso doesn't support `PRAGMA read_uncommitted`)
- `set_isolation_level()` - No-op (isolation set at connection time via `isolation_level` param)
- `get_pool_class()` - Returns `SingletonThreadPool` for `:memory:`, `QueuePool` for file databases

### Reflection Overrides (via `_TursoDialectMixin`)

Single-table methods (return empty list):
- `get_foreign_keys()` - `PRAGMA foreign_key_list` not supported
- `get_indexes()` - `PRAGMA index_list` not supported
- `get_unique_constraints()` - Relies on `PRAGMA index_list`
- `get_check_constraints()` - `sqlite_master` parsing not fully supported

Multi-table methods (return empty dict):
- `get_multi_indexes()`
- `get_multi_unique_constraints()`
- `get_multi_foreign_keys()`
- `get_multi_check_constraints()`

## Limitations

### Table Reflection

Turso doesn't support some SQLite PRAGMAs used for table reflection:
- `PRAGMA foreign_key_list` - Foreign key introspection
- `PRAGMA index_list` - Index introspection

This means:
- `inspector.get_foreign_keys()` returns empty list
- `inspector.get_indexes()` returns empty list
- `inspector.get_unique_constraints()` returns empty list
- `inspector.get_check_constraints()` returns empty list
- Foreign keys, indexes, and constraints still **work** at runtime, just can't be introspected
- `inspector.get_table_names()` and `inspector.get_columns()` work normally

This doesn't affect normal usage including:
- Pandas `df.to_sql()` with `if_exists='replace'`
- SQLAlchemy ORM operations
- Alembic migrations (when using `--autogenerate`, manually verify FK/index changes)

### Native Datetime

`supports_native_datetime` is set to `False`. Datetime columns should use `String` type and store ISO format strings. SQLAlchemy's `DateTime` type will still work but values are stored/retrieved as strings.

## Entry Points

Dialects are registered via `pyproject.toml` entry points:

```toml
[project.entry-points."sqlalchemy.dialects"]
"sqlite.turso" = "turso.sqlalchemy:TursoDialect"
"sqlite.turso_sync" = "turso.sqlalchemy:TursoSyncDialect"
```

## Files

- `turso/sqlalchemy/__init__.py` - Module exports (`TursoDialect`, `TursoSyncDialect`, `get_sync_connection`)
- `turso/sqlalchemy/dialect.py` - Dialect implementations and `_TursoDialectMixin`
- `tests/test_sqlalchemy.py` - Tests (28 tests across 8 test classes)

## References

- [SQLAlchemy SQLite Dialect Docs](https://docs.sqlalchemy.org/en/20/dialects/sqlite.html)
- [SQLAlchemy Dialect Creation Guide](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst)
- [pysqlite Dialect Source](https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/dialects/sqlite/pysqlite.py)
