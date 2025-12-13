# Architecture

This document describes the architecture of the test runner, including the backend trait system and result comparison.

## Overview

The test runner is designed with extensibility in mind. The core components are:

1. **Parser**: Parses `.sqltest` files into an AST (covered in `dsl-spec.md`)
2. **Backend Trait**: Abstraction for executing SQL against different targets (CLI, SDKs)
3. **Comparison**: Compares actual results against expected results
4. **Runner**: Orchestrates test execution with parallelism

```
┌─────────────────────────────────────────────────────────────┐
│                        Test Runner                          │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌─────────────┐ │
│  │ Parser  │──│ Executor │──│ Comparison│──│   Output    │ │
│  └─────────┘  └────┬─────┘  └───────────┘  └─────────────┘ │
│                    │                                        │
│              ┌─────┴─────┐                                  │
│              │  Backend  │                                  │
│              └─────┬─────┘                                  │
│       ┌────────────┼────────────┐                          │
│       ▼            ▼            ▼                          │
│   ┌───────┐   ┌────────┐   ┌────────┐                      │
│   │  CLI  │   │ Python │   │   Go   │  ...                 │
│   └───────┘   └────────┘   └────────┘                      │
└─────────────────────────────────────────────────────────────┘
```

## Backend Trait System

### `SqlBackend` Trait

The core abstraction for any SQL execution target:

```rust
#[async_trait]
pub trait SqlBackend: Send + Sync {
    /// Name of this backend (for filtering and display)
    fn name(&self) -> &str;

    /// Create a new isolated database instance
    async fn create_database(&self, config: &DatabaseConfig) -> Result<Box<dyn DatabaseInstance>>;
}
```

### `DatabaseInstance` Trait

Represents a single database connection:

```rust
#[async_trait]
pub trait DatabaseInstance: Send + Sync {
    /// Execute SQL and return results
    async fn execute(&mut self, sql: &str) -> Result<QueryResult>;

    /// Close and cleanup the database
    async fn close(self: Box<Self>) -> Result<()>;
}
```

### `QueryResult` Struct

Standardized result format:

```rust
pub struct QueryResult {
    /// Rows returned, each row is a vector of string-formatted columns
    pub rows: Vec<Vec<String>>,
    /// Error message if the query failed
    pub error: Option<String>,
}
```

## Comparison Modes

The runner supports multiple ways to compare expected vs actual results:

### Exact Match

Row-by-row, column-by-column comparison:

```rust
pub fn compare_exact(actual: &[Vec<String>], expected: &[String]) -> ComparisonResult
```

- Rows must be in the same order
- Expected format: pipe-separated columns, one row per line
- Empty expected = empty result expected

### Pattern Match

Regex matching against the output:

```rust
pub fn compare_pattern(actual: &[Vec<String>], pattern: &str) -> ComparisonResult
```

- Pattern is matched against the formatted output
- Useful for random values, timestamps, etc.

### Unordered Match

Set comparison (order-independent):

```rust
pub fn compare_unordered(actual: &[Vec<String>], expected: &[String]) -> ComparisonResult
```

- Rows can be in any order
- Each expected row must appear exactly once

### Error Match

Verify an error occurred:

```rust
pub fn compare_error(actual_error: Option<&str>, expected_pattern: Option<&str>) -> ComparisonResult
```

- If pattern is `None`, any error is accepted
- If pattern is `Some`, the error message must contain the pattern

## Result Types

### `ComparisonResult`

```rust
pub enum ComparisonResult {
    /// Results match
    Match,
    /// Results don't match
    Mismatch { reason: String },
}
```

### `TestResult`

```rust
pub struct TestResult {
    /// Name of the test
    pub name: String,
    /// Outcome of the test
    pub outcome: TestOutcome,
    /// Duration of the test
    pub duration: Duration,
}

pub enum TestOutcome {
    Passed,
    Failed { reason: String },
    Skipped { reason: String },
    Error { message: String },
}
```

## Test Execution Flow

For each test case:

1. **Setup Phase**
   - Create fresh database instance
   - Execute setup SQL blocks in order

2. **Execution Phase**
   - Execute test SQL
   - Capture result or error

3. **Comparison Phase**
   - Compare actual result against expectation
   - Apply appropriate comparison mode

4. **Cleanup Phase**
   - Close database instance
   - Report result

```
┌─────────────────────────────────────────────┐
│              For each test:                 │
│  ┌─────────┐                                │
│  │ Create  │                                │
│  │   DB    │                                │
│  └────┬────┘                                │
│       ▼                                     │
│  ┌─────────┐                                │
│  │  Run    │ (for each @setup decorator)    │
│  │ Setups  │                                │
│  └────┬────┘                                │
│       ▼                                     │
│  ┌─────────┐                                │
│  │Execute  │                                │
│  │  Test   │                                │
│  └────┬────┘                                │
│       ▼                                     │
│  ┌─────────┐                                │
│  │Compare  │                                │
│  │ Result  │                                │
│  └────┬────┘                                │
│       ▼                                     │
│  ┌─────────┐                                │
│  │ Close   │                                │
│  │   DB    │                                │
│  └─────────┘                                │
└─────────────────────────────────────────────┘
```

## Error Handling

Errors are categorized as:

1. **Parse Errors**: Invalid test file syntax
2. **Backend Errors**: Failed to create database or execute SQL
3. **Comparison Failures**: Results don't match expectations
4. **Test Errors**: Unexpected exceptions during execution

All errors include context (file, test name, line number) for debugging.

---

## Implementation Notes

### Backend Module (`src/backends/mod.rs`)

The backend module defines the core traits:

```rust
#[async_trait]
pub trait SqlBackend: Send + Sync {
    fn name(&self) -> &str;
    async fn create_database(&self, config: &DatabaseConfig)
        -> Result<Box<dyn DatabaseInstance>, BackendError>;
}

#[async_trait]
pub trait DatabaseInstance: Send + Sync {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError>;
    async fn close(self: Box<Self>) -> Result<(), BackendError>;
}
```

**`QueryResult`** provides a uniform result format:
- `rows`: Vector of rows, each row is a vector of string-formatted column values
- `error`: Optional error message if the query failed

**`BackendError`** covers all backend failure modes:
- `CreateDatabase`: Failed to create/open database
- `Execute`: Failed to execute SQL
- `Close`: Failed to close database
- `NotAvailable`: Backend not installed/configured
- `Timeout`: Query execution timed out

### Comparison Module (`src/comparison/`)

The comparison module uses the `similar` crate for generating diffs:

**Exact Comparison** (`exact.rs`):
- Compares row-by-row, column-by-column
- On mismatch, produces unified diff output
- Empty expected = empty result expected

**Pattern Comparison** (`pattern.rs`):
- Uses Rust `regex` crate
- Pattern matched against formatted output (rows joined by newlines, columns by pipes)
- Invalid regex patterns are reported as errors

**Unordered Comparison** (`unordered.rs`):
- Converts rows to sets for comparison
- Reports missing and extra rows separately
- Useful for queries with non-deterministic ordering

**Error Comparison** (in `mod.rs`):
- If pattern is `None`, any error is accepted
- If pattern is `Some`, error message must contain the pattern

### Diff Output

On mismatch, exact comparison produces output like:

```
--- expected
+++ actual
 1|Alice
-2|Bob
+2|Charlie
 3|Dave
```

This makes it easy to identify exactly what changed.
