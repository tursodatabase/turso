# SQLAlchemy Dialect for Pyturso

This document describes the SQLAlchemy dialect implementation for pyturso.

## Status: Implemented ✅

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

## Architecture

```
SQLiteDialect_pysqlite (SQLAlchemy built-in)
        │
        ├── TursoDialect (sqlite+turso://)
        │       └── uses turso.connect()
        │
        └── TursoSyncDialect (sqlite+turso_sync://)
                └── uses turso.sync.connect()
                        └── ConnectionSync (pull/push/checkpoint/stats)
```

## What Pyturso Provides

| Requirement | Status |
|-------------|--------|
| `apilevel = "2.0"` | ✅ |
| `threadsafety = 1` | ✅ |
| `paramstyle = "qmark"` | ✅ |
| `sqlite_version` | ✅ |
| `sqlite_version_info` | ✅ |
| `connect()` function | ✅ |
| `Connection` class | ✅ |
| `Cursor` class | ✅ |
| Exception hierarchy | ✅ |

## Dialect Overrides

The dialects override several methods from `SQLiteDialect_pysqlite`:

- `import_dbapi()` - Return `turso` or `turso.sync` module
- `create_connect_args()` - Parse URL to connection arguments
- `on_connect()` - Disabled (turso doesn't support `create_function`)
- `get_isolation_level()` - Return SERIALIZABLE (turso doesn't support PRAGMA read_uncommitted)
- `set_isolation_level()` - No-op (isolation set at connection time)
- `get_foreign_keys()` - Returns empty (PRAGMA foreign_key_list not supported)
- `get_indexes()` - Returns empty (PRAGMA index_list not supported)
- `get_unique_constraints()` - Returns empty (relies on unsupported PRAGMA)

## Limitations

### Table Reflection

Turso doesn't support some SQLite PRAGMAs used for table reflection:
- `PRAGMA foreign_key_list` - Foreign key introspection
- `PRAGMA index_list` - Index introspection

This means:
- `inspector.get_foreign_keys()` returns empty list
- `inspector.get_indexes()` returns empty list
- Foreign keys and indexes still **work** at runtime, just can't be introspected

This doesn't affect normal usage including:
- Pandas `df.to_sql()` with `if_exists='replace'`
- SQLAlchemy ORM operations
- Alembic migrations (when using `--autogenerate`, manually verify FK/index changes)

## Files

- `turso/sqlalchemy/__init__.py` - Module exports
- `turso/sqlalchemy/dialect.py` - Dialect implementations
- `tests/test_sqlalchemy.py` - Tests

## References

- [SQLAlchemy SQLite Dialect Docs](https://docs.sqlalchemy.org/en/20/dialects/sqlite.html)
- [SQLAlchemy Dialect Creation Guide](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst)
- [pysqlite Dialect Source](https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/dialects/sqlite/pysqlite.py)
