# Limbo driver for Ruby

## Project Status

The project is actively developed. Feel free to open issues and contribute.

## How to use

### Build the Ruby binding

To build the Ruby binding, you need to have Rust and Cargo installed. You can follow the instructions [here](https://www.rust-lang.org/tools/install) to install Rust and Cargo.

Once you have Rust and Cargo installed, navigate to the `bindings/ruby` directory in the limbo repository and run the following command:

```shell
$ cargo build --release
```

This will generate a shared library file (`.so` or `.dll` depending on your platform) in the `target/release` directory.

### Use the Ruby binding

To use the Ruby binding, you need to require the generated shared library file in your Ruby script. Here is an example:

```ruby
require 'rutie'
require_relative 'path/to/your/shared/library/file'

module Limbo
  class Connection
    def initialize(path)
      @connection = RubyConnection.new(path)
    end

    def execute(sql)
      @connection.ruby_execute(sql)
    end
  end
end

# Example usage
connection = Limbo::Connection.new("path/to/your/database/file")
result = connection.execute("SELECT * FROM users")
puts result
```

In this example, replace `path/to/your/shared/library/file` with the actual path to the generated shared library file and `path/to/your/database/file` with the actual path to your database file.

## Examples

Here are some examples of how to use the Ruby binding:

### Connect to a database and execute a query

```ruby
require 'rutie'
require_relative 'path/to/your/shared/library/file'

module Limbo
  class Connection
    def initialize(path)
      @connection = RubyConnection.new(path)
    end

    def execute(sql)
      @connection.ruby_execute(sql)
    end
  end
end

# Connect to the database
connection = Limbo::Connection.new("path/to/your/database/file")

# Execute a query
result = connection.execute("SELECT * FROM users")
puts result
```

### Handle errors

```ruby
require 'rutie'
require_relative 'path/to/your/shared/library/file'

module Limbo
  class Connection
    def initialize(path)
      @connection = RubyConnection.new(path)
    end

    def execute(sql)
      begin
        @connection.ruby_execute(sql)
      rescue StandardError => e
        puts "Error: #{e.message}"
      end
    end
  end
end

# Connect to the database
connection = Limbo::Connection.new("path/to/your/database/file")

# Execute a query
result = connection.execute("SELECT * FROM users")
puts result
```

In this example, if an error occurs while executing the query, it will be caught and printed to the console.
