# Turso Ruby Gem

Ruby bindings for [Turso](https://turso.tech), the SQLite-compatible database built for production workloads.

**Requires Ruby >= 3.2.0**

## Installation

```bash
gem install turso
```

Platform-specific native gems are available for Linux, macOS, and Windows.

## Quick Start

```ruby
require "turso"

db = Turso::Database.new(":memory:")
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
db.execute("INSERT INTO users (name) VALUES (?)", "Alice")
row = db.get_first_row("SELECT * FROM users WHERE id = ?", 1)
p row
```

## Open Options

| Option | Type | Description |
|--------|------|-------------|
| `readonly` | Boolean | Open database in read-only mode |
| `file_must_exist` | Boolean | Fail if database file does not exist |
| `busy_timeout` | Integer | Busy timeout in milliseconds |
| `query_timeout` | Integer | Query timeout in milliseconds |
| `experimental_features` | String | Comma-separated experimental features |
| `encryption` | Hash | Encryption config: `{ cipher: "aes256", hexkey: "..." }` |
| `vfs` | Symbol/String | VFS backend: `:memory`, `:syscall`, `:io_uring` |

```ruby
db = Turso::Database.new("my.db", readonly: true, busy_timeout: 5000)
```

## Named Parameters

```ruby
db.execute("INSERT INTO users (name) VALUES (:name)", name: "Alice")
row = db.get_first_row("SELECT * FROM users WHERE x = :name", name: "Alice")
```

## Exception Hierarchy

```
Turso::Exception (base)
├── Turso::BusyException
├── Turso::ConstraintException
├── Turso::CorruptException
├── Turso::MisuseException
├── Turso::ReadonlyException
├── Turso::InterruptException
├── Turso::DatabaseFullException
├── Turso::NotADatabaseException
├── Turso::BusySnapshotException
└── Turso::IoException
```

`Turso::Error` is an alias for `Turso::Exception`.

## License

MIT License. See [LICENSE](LICENSE).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/tursodatabase/turso.
