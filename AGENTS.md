# Guidelines for AI Agents

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

# Running a single TCL test module (*.test). TEST basepath defaults to ./testing/
make test-single TEST=modulename.test

# Run test with make (.sqltest)
make -C turso-test-runner run-cli

# Run the CLI
cargo run --package turso_cli -q -m list --bin tursodb database.db

# Format code (ALWAYS run before committing)
cargo fmt

# Lint (ALWAYS run before committing)
cargo clippy --workspace --all-features --all-targets -- --deny=warnings

# Run benchmarks
cargo bench --profile bench-profile --bench benchmark
```

## If you are running inside a CI environment

- If you are run as part of a CI action (e.g. a Github Action), you will have a limit of autonomous actions you can take.
  At the present moment, this can be looked up from [this file](.github/workflows/claude.yml), specifically the `max-turns`
  parameter.
- If you get close to reaching your "turn limit" (autonomous action limit), you are allowed to commit and push your changes
  in a WIP state and even open a WIP PR, after which you can continue in another CI action if necessary.
- Because of the max-turns limitation, be mindful not to spiral into rabbit hole investigations and remind yourself what
  your key focus is.

## Code Style Requirements

1. **Always run `cargo fmt`** before committing
2. **Always run `cargo clippy`** with `--deny=warnings` - no warnings allowed
3. Follow existing patterns in the codebase
4. Keep commits atomic and focused (see CONTRIBUTING.md)
5. Don't mix logic changes with formatting/refactoring
6. CRITICAL: this is a production database, not a toy. Correctness is absolutely paramount. No workarounds or quick hacks. All errors must be handled,
   invariants must be checked. Assert often. Never silently fail or swallow edge cases. It is more important that the program CRASHES if it reaches an
   invalid state that might endanger data integrity, than that it continues execution in some undefined state.
7. Use the power of the Rust language to your advantage. Make illegal states unrepresentable. Use exhaustive pattern matching, and prefer enums over
   strings or other "sentinel" types.
8. Minimize heap allocations and write CPU-friendly code. This is code where a microsecond is a long time.
9. Your objective is to produce robust, correct code, not to "make it work". Consider race conditions, data races and edge cases. On a long enough
   timeline, ALL bugs that can happen WILL happen.
10. If you write an if-statement, it must be a case where both branches can expectedly be reached. If it's a case where only one branch should ever be
    hit, then either an internal error should be returned or an assertion be made instead of the if-statement. Choose assertion if reaching the invalid
    branch risks data corruption or incorrect results. Don't "hedge your bets" by writing if-statements just to stay on the happy path.

## Writing comments

- Do NOT write comments that just repeat what the code does, unless the code is intrinsically very complex and readers would benefit
  from a clear summary. Prefer to comment WHY something is done.
- ALWAYS document functions, structs, enums, struct members and enum variants. Apply the same principle as above, though: focus on
  why a given struct/enum/variant is necessary.
- Do NOT EVER write comments that reference a conversation between a human and an AI agent, e.g.
    * "This test should trigger the bug"
    * "Phase 1 (existing code)" <- "existing code" is completley meaningless to a reader who encounters this code in the codebase
    * "Header validation (added)" <- "added" when? it makes no sense to a reader.

## Testing Practices

- Turso aims for SQLite compatibility
- Every functional change must be accompanied by a test, preferably a SQL test, that fails without the change, and passes when the change is applied.
- Favour writing tests first.
- For simple compatibility tests, prefer .sqltest test (./turso-test-runner/tests/*.sqltest) or TCL tests (./testing/*.test) when a sqltest file does not exist for your type of queries. Also prefer to use the `@database :memory:` (.sqltest) or `do_execsql_test_on_specific_db {:memory:}` (.test) format where an in-memory DB is used.
- For regression tests, a good choice is to use Rust integration tests, found in e.g. ./tests/integration/test_transactions.rs
- For complex features, fuzz tests are a good idea, found in e.g. ./tests/fuzz/mod.rs
- CRITICAL: do NOT invent new test formats: add tests to existing test modules that follow existing patterns. If you add a new test module file,
  make sure its tests follow patterns from other, existing files.
- For manually inspecting `turso` output, you can use e.g. `cargo run --bin tursodb :memory: 'your statement here'`, including `EXPLAIN` queries
  to inspect the bytecode plan.

## PR Guidelines

- Keep PRs focused and as small as possible
- Each commit should be atomic and tell a story
- Run any relevant tests before submitting
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

## Working with External APIs and Tools

- **Never guess API parameters, CLI arguments, or configuration options.** If you are unsure about the correct syntax, parameter names, or available options for an external tool or API, search the official documentation first.
- Use web search to find and verify documentation before making changes that involve external services, GitHub Actions, third-party libraries, or unfamiliar APIs.
- When documentation is ambiguous or outdated, prefer to ask for clarification rather than making assumptions that could lead to broken configurations.