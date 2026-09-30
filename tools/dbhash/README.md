# Turso dbhash

This is a rewrite of [dbhash](https://github.com/sqlite/sqlite/blob/ca0b14a49/tool/dbhash.c) in Rust. It computes the SHA1 hash of SQLite database content, compatible with SQLite's `dbhash` utility.

The tool hashes database content, not the physical file format, making it deterministic across different database configurations. Identical data produces identical hashes regardless of page size, text encoding, freelist state, or vacuum history.

## Usage

```bash
turso-dbhash database.db

# Filter tables by pattern
turso-dbhash --like "user%" database.db

# Hash only schema (no data)
turso-dbhash --schema-only database.db

# Hash only data (no schema)
turso-dbhash --without-schema database.db
```