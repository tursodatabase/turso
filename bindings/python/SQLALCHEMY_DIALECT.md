# SQLAlchemy Dialect for Pyturso

This document summarizes research on implementing a SQLAlchemy dialect for pyturso.

## Feasibility: Easy

Creating a SQLAlchemy dialect for pyturso is straightforward because:

1. **Pyturso is DB-API 2.0 compliant** - SQLAlchemy can use it as a standard DBAPI driver
2. **SQLite-compatible SQL** - No SQL compilation changes needed
3. **Can inherit from existing dialect** - Reuse `SQLiteDialect_pysqlite`, override 2-3 methods

## Architecture

```
SQLiteDialect_pysqlite (SQLAlchemy built-in)
        │
        ▼
TursoDialect (extends, overrides import_dbapi)
        │
        ▼
turso module (DB-API 2.0: connect, Connection, Cursor)
```

## Minimal Implementation

```python
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite


class TursoDialect(SQLiteDialect_pysqlite):
    """SQLAlchemy dialect for pyturso."""

    driver = "turso"

    @classmethod
    def import_dbapi(cls):
        """Import the pyturso DB-API 2.0 module."""
        import turso
        return turso

    def create_connect_args(self, url):
        """Parse SQLAlchemy URL into turso.connect() arguments."""
        opts = url.translate_connect_args()
        opts.update(url.query)
        return [[], opts]
```

That's it - ~20 lines of code for a working dialect.

## Registration

### Option 1: Runtime Registration

```python
from sqlalchemy.dialects import registry

registry.register(
    "sqlite+turso",
    "turso.sqlalchemy",
    "TursoDialect"
)
```

### Option 2: Package Entry Point

In `pyproject.toml`:
```toml
[project.entry-points."sqlalchemy.dialects"]
"sqlite.turso" = "turso.sqlalchemy:TursoDialect"
```

## Usage

```python
from sqlalchemy import create_engine, text

# File-based database
engine = create_engine("sqlite+turso:///path/to/database.db")

# In-memory database
engine = create_engine("sqlite+turso:///:memory:")

# With options
engine = create_engine(
    "sqlite+turso:///db.db",
    connect_args={"isolation_level": "DEFERRED"}
)

# Use normally
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM users"))
    for row in result:
        print(row)
```

## What Pyturso Already Provides

The dialect works because `turso` module exports everything SQLAlchemy needs:

| Requirement | Pyturso Status |
|-------------|----------------|
| `apilevel = "2.0"` | ✅ Provided |
| `threadsafety = 1` | ✅ Provided |
| `paramstyle = "qmark"` | ✅ Provided |
| `connect()` function | ✅ Provided |
| `Connection` class | ✅ Provided |
| `Cursor` class | ✅ Provided |
| Exception hierarchy | ✅ Complete (Error, DatabaseError, OperationalError, etc.) |

## Methods Inherited vs Overridden

### Inherited (from SQLiteDialect_pysqlite)
- All SQL compilation (SQLite syntax is identical)
- Type handling (INTEGER, TEXT, REAL, BLOB)
- Connection pooling logic
- Transaction management
- Parameter binding

### Overridden (2-3 methods)
- `import_dbapi()` - Return `turso` module instead of `sqlite3`
- `create_connect_args()` - Parse URL to `turso.connect()` kwargs
- `on_connect()` - Optional post-connection setup

## Effort Estimate

| Component | Lines of Code | Notes |
|-----------|---------------|-------|
| Core dialect | ~30 | Inherit + override |
| Registration | ~10 | Entry point or runtime |
| Tests | ~100-200 | CRUD, transactions, types |
| Async variant | ~50 | If needed, similar pattern |

## Optional Enhancements

### Async Support

For `turso.aio`, create an async dialect:

```python
from sqlalchemy.dialects.sqlite.aiosqlite import SQLiteDialect_aiosqlite

class TursoAsyncDialect(SQLiteDialect_aiosqlite):
    driver = "turso_aio"

    @classmethod
    def import_dbapi(cls):
        import turso.aio
        return turso.aio
```

### Turso-Specific Features

Could expose pyturso-specific features via `connect_args`:
- Encryption options
- Experimental features
- VFS selection

## References

- [SQLAlchemy SQLite Dialect Docs](https://docs.sqlalchemy.org/en/20/dialects/sqlite.html)
- [SQLAlchemy Dialect Creation Guide](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst)
- [pysqlite Dialect Source](https://github.com/sqlalchemy/sqlalchemy/blob/main/lib/sqlalchemy/dialects/sqlite/pysqlite.py)
