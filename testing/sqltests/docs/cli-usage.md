# CLI Usage

This document describes the command-line interface for the test runner.

## Installation

```bash
# Build from source
cargo build --release

# The binary will be at:
./target/release/test-runner
```

## Commands

### `run` - Execute Tests

Run SQL tests from files or directories.

```bash
test-runner run <PATHS>... [OPTIONS]
```

#### Arguments

- `<PATHS>...` - One or more test files (`.sqltest`) or directories to scan

#### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--binary <PATH>` | | `tursodb` | Path to the tursodb CLI binary |
| `--filter <PATTERN>` | `-f` | | Filter tests by name (glob pattern) |
| `--jobs <N>` | `-j` | CPU count | Maximum concurrent test jobs |
| `--output <FORMAT>` | `-o` | `pretty` | Output format: `pretty` or `json` |
| `--timeout <SECS>` | | `30` | Timeout per query in seconds |

#### Examples

```bash
# Run all tests in a directory
test-runner run tests/

# Run a specific test file
test-runner run tests/select.sqltest

# Run multiple paths
test-runner run tests/select.sqltest tests/insert/

# Custom binary path
test-runner run tests/ --binary ./target/release/tursodb

# Filter by test name
test-runner run tests/ -f "select-*"
test-runner run tests/ -f "*-join-*"

# Limit concurrent jobs
test-runner run tests/ -j 4

# JSON output for CI
test-runner run tests/ -o json
```

### `check` - Validate Syntax

Validate test file syntax without executing tests.

```bash
test-runner check <PATHS>...
```

#### Examples

```bash
# Check a single file
test-runner check tests/select.sqltest

# Check all files in a directory
test-runner check tests/
```

## Output Formats

### Pretty (Default)

Human-readable colored output with progress indication:

```
Running tests...

tests/select.sqltest
  [PASS] select-const-1                     (2ms)
  [PASS] select-users                       (5ms)
  [FAIL] select-join                        (3ms)
         expected: 1|Alice
         actual:   1|Bob
  [SKIP] select-buggy                       (known issue #123)

tests/insert.sqltest
  [PASS] insert-basic                       (4ms)
  [ERROR] insert-readonly                   (1ms)
          setup 'users' failed: table users already exists

Summary:
  4 passed, 1 failed, 1 skipped, 1 error
  Total time: 127ms
```

### JSON

Machine-readable output for CI integration:

```json
{
  "files": [
    {
      "path": "tests/select.sqltest",
      "results": [
        {
          "name": "select-const-1",
          "outcome": "passed",
          "duration_ms": 2
        },
        {
          "name": "select-join",
          "outcome": "failed",
          "reason": "expected: 1|Alice\nactual: 1|Bob",
          "duration_ms": 3
        }
      ],
      "duration_ms": 15
    }
  ],
  "summary": {
    "total": 6,
    "passed": 4,
    "failed": 1,
    "skipped": 1,
    "errors": 0,
    "duration_ms": 127
  }
}
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All tests passed (or skipped) |
| 1 | One or more tests failed or errored |
| 2 | Invalid arguments or configuration error |

## Filter Patterns

The `--filter` option supports simple glob patterns:

| Pattern | Matches |
|---------|---------|
| `select-*` | Tests starting with "select-" |
| `*-test` | Tests ending with "-test" |
| `*join*` | Tests containing "join" |
| `select-const-1` | Exact match |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TURSO_TEST_BINARY` | Default path to tursodb binary (overridden by `--binary`) |
| `NO_COLOR` | Disable colored output |

## CI Integration

### GitHub Actions

```yaml
- name: Run SQL tests
  run: |
    test-runner run tests/ \
      --binary ./target/release/tursodb \
      --output json \
      > test-results.json

- name: Upload results
  uses: actions/upload-artifact@v3
  with:
    name: test-results
    path: test-results.json
```

### Exit on First Failure

Currently, all tests run to completion. A `--fail-fast` option may be added in the future.

## Performance Tips

1. **Increase parallelism**: Use `-j` to match your CPU count for maximum throughput
2. **Use filters during development**: Run only relevant tests with `-f`
3. **Prefer :memory: databases**: In-memory databases are faster than temp files
4. **Batch queries in setups**: Combine multiple setup statements into one block

---

## Implementation Details

### CLI Argument Parsing (`src/main.rs`)

Uses `clap` with derive macros for argument parsing:

```rust
#[derive(Parser)]
#[command(name = "test-runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run { /* ... */ },
    Check { /* ... */ },
}
```

### Main Entry Point

The `main` function is async using `#[tokio::main]`:

```rust
#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run { .. } => run_tests(...).await,
        Commands::Check { paths } => check_files(paths),
    }
}
```

### Output Module (`src/output/`)

The output module provides a trait-based abstraction for formatting:

```rust
pub trait OutputFormat {
    fn write_test(&mut self, result: &TestResult);
    fn write_file(&mut self, result: &FileResult);
    fn write_summary(&mut self, summary: &RunSummary);
    fn flush(&mut self);
}

pub enum Format {
    Pretty,
    Json,
}
```

### Pretty Output (`src/output/pretty.rs`)

- Uses `colored` crate for terminal colors
- Groups output by file
- Shows colored status symbols: green PASS, red FAIL, yellow SKIP, red bold ERROR
- Displays failure reasons inline
- Respects `NO_COLOR` environment variable

### JSON Output (`src/output/json.rs`)

- Uses `serde` and `serde_json` for serialization
- Collects all results then outputs at summary time
- Includes structured file/test/summary hierarchy
- Skips `reason` field when null (using `#[serde(skip_serializing_if)]`)

### Integration Flow

```
1. Parse CLI arguments (clap)
2. Create CliBackend with binary path and timeout
3. Create RunnerConfig with jobs and filter
4. Create TestRunner<CliBackend>
5. Call runner.run_paths(&paths)
6. Create output formatter (Pretty or Json)
7. Write file results
8. Write summary
9. Return exit code based on success
```

### Check Command

The check command validates syntax without running tests:

```rust
fn check_files(paths: Vec<PathBuf>) -> ExitCode {
    for path in paths {
        if path.is_dir() {
            // Glob for *.sqltest and check each
        } else {
            // Parse single file
        }
    }
    // Return success if no parse errors
}
```

### Exit Code Handling

```rust
if summary.is_success() {
    ExitCode::SUCCESS  // 0
} else {
    ExitCode::from(1)  // 1 for test failures
}
// 2 is returned for invalid arguments (before running)
```
