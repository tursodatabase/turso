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
- **Remote partial sync**: Bootstrap from a remote database, pull remote changes, and push local changes when online &mdash; all while enjoying a fully operational database offline.
- **Asyncio support**: Built-in integration with asyncio to ensure queries won’t block your event loop

## Installation

```bash
uv pip install pyturso
```

## Database driver

A minimal DB‑API 2.0 example using an in‑memory database:

```python
import turso

# Standard DB-API usage
conn = turso.connect(":memory:")
cur = conn.cursor()

cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)")
cur.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")

cur.execute("SELECT * FROM users ORDER BY id")
rows = cur.fetchall()
print(rows)  # [(1, 'alice'), (2, 'bob')]

conn.close()
```

## Database driver (asyncio)

Non-blocking access with asyncio:

```python
import asyncio
import turso.aio

async def main():
    # Connect and use as an async context manager
    async with turso.aio.connect(":memory:") as conn:
        # Executes multiple statements
        await conn.executescript("""
            CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO t(name) VALUES ('alice'), ('bob');
        """)

        # Use a cursor for parameterized queries
        cur = conn.cursor()
        await cur.execute("SELECT COUNT(*) FROM t WHERE name LIKE ?", ("a%",))
        count = (await cur.fetchone())[0]
        print(count)  # 1

        # JSON and generate_series also available
        cur = conn.cursor()
        await cur.execute("SELECT SUM(value) FROM generate_series(1, 10)")
        print((await cur.fetchone())[0])  # 55

asyncio.run(main())
```

## Synchronization driver

Use a remote Turso database while working locally. You can bootstrap local state from the remote, pull remote changes, and push local commits.

Note: You need a Turso remote URL. See the Turso docs for provisioning and authentication.

```python
import turso.sync

# Connect a local database to a remote Turso database
conn = turso.sync.connect(
    ":memory:",                          # local db path (or a file path)
    remote_url="https://<db>.<region>.turso.io"  # your remote URL
)

# Read data (fetched from remote if not present locally yet)
rows = conn.execute("SELECT * FROM t").fetchall()
print(rows)

# Pull new changes from remote into local
changed = conn.pull()
print("Pulled:", changed)  # True if there were new remote changes

# Make local changes
conn.execute("INSERT INTO t VALUES ('push works')")
conn.commit()

# Push local commits to remote
conn.push()

# Optional: inspect and manage sync state
stats = conn.stats()
print("Network received (bytes):", stats.network_received_bytes)
conn.checkpoint()  # compact local WAL after many writes

conn.close()
```

Partial bootstrap to reduce initial network cost:

```python
import turso.sync

conn = turso.sync.connect(
    "local.db",
    remote_url="https://<db>.<region>.turso.io",
    # fetch first 128 KiB upfront
    partial_sync_opts=turso.sync.PartialSyncOpts(
      bootstrap_strategy=turso.sync.PartialSyncPrefixBootstrap(length=128 * 1024),
    ),
)
```

## Synchronization driver (asyncio)

The same sync primitives, but fully async:

```python
import asyncio

async def main():
    conn = await turso.aio.sync.connect(":memory:", remote_url="https://<db>.<region>.turso.io")

    # Read data
    rows = await (await conn.execute("SELECT * FROM t")).fetchall()
    print(rows)

    # Pull and push
    await conn.pull()
    await conn.execute("INSERT INTO t VALUES ('hello from asyncio')")
    await conn.commit()
    await conn.push()

    # Stats and maintenance
    stats = await conn.stats()
    print("Main WAL size:", stats.main_wal_size)
    await conn.checkpoint()

    await conn.close()

asyncio.run(main())
```

## License

This project is licensed under the [MIT license](../../LICENSE.md).

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://tur.so/discord)
