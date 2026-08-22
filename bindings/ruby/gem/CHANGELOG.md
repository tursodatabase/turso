# Changelog

## 0.1.2

- Production-ready low-level driver rewrite.
- Ruby 3.2+ required; GVL released via fiber scheduler blocking operations.
- `sqlite3`-style API: `Turso::Database`, `Turso::Statement`, `Turso::ResultSet`.
- Added open options: `readonly`, `file_must_exist`, `busy_timeout`, `query_timeout`, `experimental_features`, `encryption`, `vfs`.
- Added named parameters, statement helpers (`run`, `get`, `all`, `iterate`), and batch execution.
- Added `sqlite3`-compatible exception hierarchy.
