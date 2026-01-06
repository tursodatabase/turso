# Claude Code Guidelines for Turso

## Project Overview

Turso is a rewrite of SQLite in Rust. This is a large workspace with 40+ crates including:
- `core` - The main database engine
- `parser` - SQL parser
- `cli` - The `tursodb` command-line interface
- `bindings/*` - Language bindings (Python, JavaScript, Java, Rust, Dart, .NET)
- `extensions/*` - SQL extensions (crypto, regexp, csv, etc.)
- `simulator` - Deterministic simulation testing
- `whopper` - Concurrent query execution DST
- `sync/*` - Sync engine components

## Build & Test Commands

```bash
# Build the project
cargo build

# Build with C API (required for some tests)
cargo build -p turso_sqlite3 --features capi

# Run all tests
cargo test

# Run tests with make (recommended)
make test

# Run the CLI
cargo run --package turso_cli --bin tursodb database.db

# Format code (ALWAYS run before committing)
cargo fmt

# Lint (ALWAYS run before committing)
cargo clippy --workspace --all-features --all-targets -- --deny=warnings

# Run benchmarks
cargo bench --profile bench-profile --bench benchmark
```

## Code Style Requirements

1. **Always run `cargo fmt`** before committing
2. **Always run `cargo clippy`** with `--deny=warnings` - no warnings allowed
3. Follow existing patterns in the codebase
4. Keep commits atomic and focused (see CONTRIBUTING.md)
5. Don't mix logic changes with formatting/refactoring

## Testing Philosophy

- Turso aims for SQLite compatibility
- Use `EXPLAIN` to compare bytecode between SQLite and Turso when debugging
- The `testing/all.test` file contains compatibility tests which can be run with `make test`
- Files named `testing/*.test` can be run individually by first compiling Turso (`cargo build --bin tursodb`), then executing them with `SQLITE_EXEC=scripts/limbo-sqlite3`
- Every functional change must be accompanied by a test, preferably a SQL test, that fails without the change, and passes when the change is applied.
- Favour writing tests first.
- Deterministic simulation testing using the `simulator` and `whopper` tools

## PR Guidelines

- Keep PRs focused and as small as possible
- Each commit should be atomic and tell a story
- Run the full test suite before submitting
- CI checks: formatting, Clippy warnings, test failures

## Debugging Tips

1. Compare bytecode with SQLite using `EXPLAIN <query>`
2. If bytecode differs: fix code generation
3. If bytecode matches but results differ: bug is in VM interpreter or storage layer
4. For threading issues: use stress tests with ThreadSanitizer

## Architecture Notes

- The virtual machine executes SQLite-compatible bytecode
- Storage layer handles B-tree operations and paging
- Parser generates AST from SQL strings
- Code generator produces bytecode from AST

## Third-Party Dependencies

When adding new dependencies:
1. Add licenses under `licenses/` directory
2. Update `NOTICE.md` with dependency info
