# Turso Test Runner Documentation

This directory contains documentation for the test-runner crate.

## Quick Start

```bash
# Build
cargo build --release

# Check test file syntax
./target/release/test-runner check tests/

# Run tests
./target/release/test-runner run tests/ --binary ./target/release/tursodb
```

## Documentation Index

### User Documentation

| Document | Description |
|----------|-------------|
| [DSL Specification](dsl-spec.md) | Complete specification of the `.sqltest` file format |
| [CLI Usage](cli-usage.md) | Command-line interface reference |

### Architecture & Design

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | System architecture and backend trait design |
| [Parallelism](parallelism.md) | Parallel execution strategy and implementation |

### Backend Documentation

| Document | Description |
|----------|-------------|
| [CLI Backend](backends/cli.md) | CLI backend implementation details |
| [Adding Backends](adding-backends.md) | Guide for implementing new SDK backends |

## Examples

Example test files are available in the `examples/` directory:

- `examples/basic.sqltest` - Basic DSL syntax examples
- `examples/joins.sqltest` - Setup composition and join tests

## Crate Structure

```
test-runner/
├── src/
│   ├── lib.rs              # Public API exports
│   ├── main.rs             # CLI binary entry point
│   ├── parser/             # DSL parser (Logos lexer)
│   │   ├── mod.rs          # Parser entry point
│   │   ├── lexer.rs        # Tokenizer
│   │   └── ast.rs          # AST types
│   ├── backends/           # SQL execution backends
│   │   ├── mod.rs          # SqlBackend trait
│   │   └── cli.rs          # CLI subprocess backend
│   ├── runner/             # Test execution
│   │   ├── mod.rs          # TestRunner, parallel execution
│   │   └── executor.rs     # Single test executor
│   ├── comparison/         # Result comparison
│   │   ├── mod.rs          # Comparison dispatcher
│   │   ├── exact.rs        # Exact match (with diff)
│   │   ├── pattern.rs      # Regex pattern match
│   │   └── unordered.rs    # Set comparison
│   └── output/             # Output formatting
│       ├── mod.rs          # OutputFormat trait
│       ├── pretty.rs       # Colored terminal output
│       └── json.rs         # JSON output for CI
├── docs/                   # Documentation (this directory)
├── examples/               # Example test files
└── tests/                  # Integration tests
```

## Key Concepts

### Test File Structure

```sql
# Database configuration
@database :memory:

# Named setup blocks
setup users {
    CREATE TABLE users (id INT, name TEXT);
}

# Test with setup decorator
@setup users
test select-users {
    SELECT * FROM users;
}
expect {
    1|Alice
}
```

### Isolation Model

All tests are isolated by default:
- Each test gets a fresh database instance
- Tests run in parallel
- No shared state between tests

### Backend System

The runner uses a trait-based backend system for extensibility:

```rust
trait SqlBackend {
    fn create_database(&self, config: &DatabaseConfig)
        -> Result<Box<dyn DatabaseInstance>>;
}

trait DatabaseInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult>;
    async fn close(self: Box<Self>) -> Result<()>;
}
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `clap` | CLI argument parsing |
| `logos` | Lexer generation |
| `tokio` | Async runtime & parallelism |
| `similar` | Diff generation for comparisons |
| `colored` | Terminal colors |
| `serde_json` | JSON output |

## License

MIT
