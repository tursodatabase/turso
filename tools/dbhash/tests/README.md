# DBHash Compatibility Tests

These tests verify that `turso-dbhash` produces identical hashes to SQLite's `dbhash` tool.

## Building SQLite's dbhash

Tests depend on the original `dbhash` utility from SQLite.

### Linux

```bash

sudo apt install libsqlite3-dev  # Debian/Ubuntu
sudo dnf install sqlite-devel     # Fedora/RHEL

git clone https://github.com/sqlite/sqlite.git
cd sqlite
gcc -o dbhash tool/dbhash.c -lsqlite3

sudo mv dbhash /usr/local/bin/
```

### macOS

```bash
# ensure Xcode CLI tools are installed
xcode-select --install

git clone https://github.com/sqlite/sqlite.git
cd sqlite
gcc -o dbhash tool/dbhash.c -lsqlite3

sudo mv dbhash /usr/local/bin/
```

## Running Tests

```bash
cargo test -p turso-dbhash -- --ignored
```
