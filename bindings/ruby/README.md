# Limbo driver for Ruby

## Project Status

The project is actively developed. Feel free to open issues and contribute.

## How to use

### [sqlite3](https://github.com/sparklemotion/sqlite3-ruby) compatibility

```ruby
require 'limbo'

SQLite3 = Limbo::Compat::SQLite3

# Open a database
db = SQLite3::Database.new "test.db"

# Create a table
rows = db.execute <<-SQL
  create table numbers (
    name varchar(30),
    val int
  );
SQL

# Execute a few inserts
{
  "one" => 1,
  "two" => 2,
}.each do |pair|
  db.execute "insert into numbers values ( ?, ? )", pair
end
```

## Build the Gem

```sh
bundle install
rake compile # will generate lib/limbo/limbo.so
rake test
gem build limbo.gemspec
```
