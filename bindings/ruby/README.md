# Turso Ruby Bindings

Ruby bindings for [Turso](https://turso.tech), an in-process OLTP database compatible with SQLite.

## Installation

Add to your Gemfile:

```ruby
gem 'turso'
```

Then run:

```bash
bundle install
```

Or install directly:

```bash
gem install turso
```

## Requirements

- Ruby 3.1+
- Rust toolchain (for building the native extension)

## Usage

### Basic Usage

```ruby
require 'turso'

Turso.open(":memory:") do |conn|
  conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
  conn.execute("INSERT INTO users (name) VALUES (?)", "Alice")
  
  results = conn.execute("SELECT * FROM users")
  results.each { |row| puts row[:name] }
end
```

### Transactions

```ruby
Turso.open("mydb.db") do |conn|
  conn.transaction do
    conn.execute("INSERT INTO accounts (balance) VALUES (?)", 100)
    conn.execute("UPDATE accounts SET balance = balance - 10 WHERE id = ?", 1)
  end
end
```

### Prepared Statements

```ruby
conn = Turso.open("mydb.db")
stmt = conn.prepare("SELECT * FROM users WHERE age > ?")
results = stmt.execute(21)
stmt.finalize!
conn.close
```

## Development

```bash
cd bindings/ruby
bundle install
bundle exec rake compile  # Build native extension
bundle exec rake spec     # Run tests
bundle exec rake example  # Run example
```

## License

MIT License - see [LICENSE](LICENSE) for details.
