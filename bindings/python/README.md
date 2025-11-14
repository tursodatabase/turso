<p align="center">
  <h1 align="center">Turso Database for Python</h1>
</p>

<p align="center">
  <a title="Python" target="_blank" href="https://pypi.org/project/pyturso/"><img alt="PyPI" src="https://img.shields.io/pypi/v/pyturso"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/turso/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
</p>
<p align="center">
  <a title="Users Discord" target="_blank" href="https://tur.so/discord"><img alt="Chat with other users of Turso on Discord" src="https://img.shields.io/discord/933071162680958986?label=Discord&logo=Discord&style=social"></a>
</p>

---

## About

> **⚠️ Warning:** This software is in BETA. It may still contain bugs and unexpected behavior. Use caution with production data and ensure you have backups.

## Features

- **SQLite compatible:** SQLite query language and file format support ([status](https://github.com/tursodatabase/turso/blob/main/COMPAT.md)).
- **In-process**: No network overhead, runs directly in your Python process
- **Cross-platform**: Supports Linux, macOS, Windows

## Installation
```bash
uv pip install pyturso
```

## Getting Started
```python
import turso

# Create/open a database
# con = turso.connect(":memory:") # For memory mode
con = turso.connect("sqlite.db")
cur = con.cursor()

# Create a table
cur.execute("""
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL
  )
""")
con.commit()

# Insert data
cur.execute("INSERT INTO users (username) VALUES (?)", ("alice",))
cur.execute("INSERT INTO users (username) VALUES (?)", ("bob",))
con.commit()

# Query data
res = cur.execute("SELECT * FROM users")
users = res.fetchall()
print(users)
# Output: [(1, 'alice'), (2, 'bob')]
```

## License

This project is licensed under the [MIT license](../../LICENSE.md).

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://tur.so/discord)
