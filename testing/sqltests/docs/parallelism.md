# Parallel Execution

This document describes the parallel execution strategy for the test runner.

## Overview

The test runner executes tests in parallel at multiple levels using tokio for async concurrency:

```
┌─────────────────────────────────────────────────────────────┐
│                    Test Runner                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              File Discovery (glob)                   │    │
│  │         *.sqltest files in test directory           │    │
│  └───────────────────────┬─────────────────────────────┘    │
│                          │                                   │
│            ┌─────────────┼─────────────┐                    │
│            ▼             ▼             ▼                    │
│      ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│      │ file1.   │  │ file2.   │  │ file3.   │   parallel   │
│      │ sqltest  │  │ sqltest  │  │ sqltest  │              │
│      └────┬─────┘  └────┬─────┘  └────┬─────┘              │
│           │             │             │                     │
│     ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐              │
│     ▼     ▼     ▼ ▼     ▼     ▼ ▼     ▼     ▼              │
│   ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐ ┌───┐         │
│   │db1│ │db2│ │db1│ │db2│ │db1│ │db2│ │...│ │...│ parallel│
│   └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘ └───┘ └───┘         │
│     │     │     │     │     │     │                         │
│   tests tests tests tests tests tests        parallel       │
└─────────────────────────────────────────────────────────────┘
```

## Parallelism Levels

### Level 1: File Parallelism

Each `.sqltest` file is processed in parallel via `tokio::spawn`. Files are completely independent.

### Level 2: Database Parallelism

Within each file, tests run against all specified `@database` entries in parallel. Each database configuration spawns its own set of test executions.

### Level 3: Test Parallelism

Within each file+database combination, all tests run in parallel. Each test gets a fresh, isolated database instance.

## Isolation Model

**All tests are isolated by default.** There is no concept of sequential tests or shared state between tests.

For each test execution:
1. Create a fresh database instance
2. Run setup blocks (if any `@setup` decorators)
3. Execute test SQL
4. Compare results
5. Close database instance

```
┌─────────────────────────────────────────────┐
│              For each test:                 │
│  ┌─────────────────────────────────────┐   │
│  │     Create Fresh DB Instance        │   │
│  └──────────────┬──────────────────────┘   │
│                 ▼                           │
│  ┌─────────────────────────────────────┐   │
│  │   Execute @setup blocks (in order)  │   │
│  └──────────────┬──────────────────────┘   │
│                 ▼                           │
│  ┌─────────────────────────────────────┐   │
│  │        Execute Test SQL             │   │
│  └──────────────┬──────────────────────┘   │
│                 ▼                           │
│  ┌─────────────────────────────────────┐   │
│  │      Compare with Expectation       │   │
│  └──────────────┬──────────────────────┘   │
│                 ▼                           │
│  ┌─────────────────────────────────────┐   │
│  │         Close DB Instance           │   │
│  └─────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

## Database Modes

| Mode | Description | Fresh Per Test |
|------|-------------|----------------|
| `:memory:` | In-memory database | Yes |
| `:temp:` | Temp file database | Yes |
| `path readonly` | Existing file (read-only) | Shared file, fresh connection |

## File Type Rules

Files are classified as either "writable" or "readonly":

| File Type | Allowed Databases | Setups Allowed |
|-----------|-------------------|----------------|
| Writable | `:memory:`, `:temp:` | Yes |
| Readonly | `path readonly` only | No |

**Cannot mix** writable and readonly databases in the same file.

## Concurrency Control

### Job Limit

The `-j` / `--jobs` flag controls maximum concurrent tasks:

```bash
test-runner run tests/ -j 8  # Max 8 concurrent tests
```

Default: Number of CPU cores.

### Semaphore-based Limiting

A tokio `Semaphore` limits concurrent test executions:

```rust
let semaphore = Arc::new(Semaphore::new(max_jobs));

for test in tests {
    let permit = semaphore.clone().acquire_owned().await?;
    tokio::spawn(async move {
        let result = run_test(test).await;
        drop(permit);  // Release permit
        result
    });
}
```

## Runner Architecture

### TestRunner

Main orchestrator that:
1. Discovers test files
2. Parses each file
3. Spawns parallel execution tasks
4. Collects and aggregates results

### FileRunner

Handles execution for a single `.sqltest` file:
1. Creates tasks for each database × test combination
2. Manages setup block execution
3. Reports per-file results

### TestExecutor

Executes a single test:
1. Creates database instance
2. Runs setups
3. Executes SQL
4. Compares results
5. Returns `TestResult`

## Result Aggregation

Results are collected as they complete using `FuturesUnordered`:

```rust
let mut futures = FuturesUnordered::new();

for task in tasks {
    futures.push(task);
}

while let Some(result) = futures.next().await {
    results.push(result?);
}
```

## Error Handling

- **Parse errors**: File skipped, error reported
- **Backend errors**: Test marked as error (not failure)
- **Comparison failures**: Test marked as failed
- **Timeouts**: Test marked as error with timeout message

All errors include context: file name, test name, database config.

## Performance Considerations

1. **Memory**: Each parallel test holds a database connection. Limit jobs to avoid memory exhaustion.

2. **File handles**: Temp file databases consume file descriptors. Monitor with `-j` flag.

3. **Subprocess overhead**: CLI backend spawns a process per query. Consider batching for performance-critical scenarios.

4. **Readonly optimization**: Readonly databases can share the underlying file, only connections are isolated.

---

## Implementation Details

### Core Types (`src/runner/mod.rs`)

```rust
/// Test runner configuration
pub struct RunnerConfig {
    pub max_jobs: usize,           // Default: num_cpus::get()
    pub filter: Option<String>,    // Glob pattern for test names
}

/// Main test runner - generic over backend
pub struct TestRunner<B: SqlBackend> {
    backend: Arc<B>,
    config: RunnerConfig,
    semaphore: Arc<Semaphore>,  // Shared across all tasks
}

/// Result types
pub struct TestResult {
    pub name: String,
    pub file: PathBuf,
    pub database: DatabaseConfig,
    pub outcome: TestOutcome,
    pub duration: Duration,
}

pub enum TestOutcome {
    Passed,
    Failed { reason: String },
    Skipped { reason: String },
    Error { message: String },
}
```

### Flat Task Spawning

All tests across all files are spawned into a single `FuturesUnordered` for maximum parallelism:

```rust
pub async fn run_paths(&self, paths: &[PathBuf]) -> Result<Vec<FileResult>, BackendError> {
    // 1. Discover and parse all files (sync)
    let mut test_files: Vec<(PathBuf, TestFile)> = Vec::new();
    // ... collect files ...

    // 2. Spawn ALL tasks from ALL files at once
    let mut all_futures: FuturesUnordered<_> = FuturesUnordered::new();

    for (path, test_file) in &test_files {
        let file_futures = self.spawn_file_tests(path, test_file);
        for future in file_futures {
            let path = path.clone();
            all_futures.push(async move { (path, future.await) });
        }
    }

    // 3. Collect results as they complete
    let mut results_by_file: HashMap<PathBuf, Vec<TestResult>> = HashMap::new();

    while let Some((path, result)) = all_futures.next().await {
        results_by_file.entry(path).or_default().push(result?);
    }

    // 4. Convert to FileResults
    // ...
}
```

### Task Spawning per File

Each file spawns `databases × tests` tasks:

```rust
fn spawn_file_tests(&self, path: &Path, test_file: &TestFile)
    -> FuturesUnordered<JoinHandle<TestResult>>
{
    let futures = FuturesUnordered::new();

    for db_config in &test_file.databases {
        for test in &test_file.tests {
            // Apply filter
            if !matches_filter(&test.name, &self.config.filter) {
                continue;
            }

            let semaphore = Arc::clone(&self.semaphore);
            // Clone other data...

            futures.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                run_single_test(backend, file_path, db_config, test, setups).await
            }));
        }
    }

    futures
}
```

### Single Test Execution

```rust
async fn run_single_test<B: SqlBackend>(
    backend: Arc<B>,
    file_path: PathBuf,
    db_config: DatabaseConfig,
    test: TestCase,
    setups: HashMap<String, String>,
) -> TestResult {
    let start = Instant::now();

    // 1. Check if skipped
    if let Some(reason) = &test.skip {
        return TestResult { outcome: Skipped { reason }, ... };
    }

    // 2. Create database
    let mut db = backend.create_database(&db_config).await?;

    // 3. Run setups in order
    for setup_name in &test.setups {
        db.execute(setups.get(setup_name)?).await?;
    }

    // 4. Execute test SQL
    let result = db.execute(&test.sql).await?;

    // 5. Close and compare
    db.close().await;
    let comparison = compare(&result, &test.expectation);

    TestResult {
        outcome: match comparison {
            Match => Passed,
            Mismatch { reason } => Failed { reason },
        },
        duration: start.elapsed(),
        ...
    }
}
```

### Filter Matching

Simple glob pattern matching for test name filtering:

```rust
fn matches_filter(name: &str, pattern: &str) -> bool {
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            name.starts_with(parts[0]) && name.ends_with(parts[1])
        } else {
            parts.iter().all(|p| p.is_empty() || name.contains(p))
        }
    } else {
        name == pattern
    }
}
```

### Summary Aggregation

```rust
pub fn summarize(results: &[FileResult]) -> RunSummary {
    let mut summary = RunSummary::default();

    for file_result in results {
        for test_result in &file_result.results {
            summary.add(&test_result.outcome);
        }
    }

    summary
}
```

### Unit Tests

The module includes tests for:
- `test_matches_filter_exact` - Exact name matching
- `test_matches_filter_prefix` - Prefix glob (`select-*`)
- `test_matches_filter_suffix` - Suffix glob (`*-test`)
- `test_matches_filter_contains` - Contains glob (`*join*`)
- `test_summary_add` - Summary aggregation

### Key Design Decisions

1. **Flat parallelism**: All tasks spawned into one `FuturesUnordered` rather than nested parallelism. This maximizes throughput and simplifies semaphore management.

2. **Shared semaphore**: A single `Arc<Semaphore>` is shared across all tasks, providing global concurrency control regardless of file boundaries.

3. **FuturesUnordered over JoinSet**: Using `FuturesUnordered` allows results to be collected as they complete rather than waiting for all tasks.

4. **Backend as Arc**: The backend is wrapped in `Arc` so it can be shared across all spawned tasks without cloning.

5. **Permit acquisition inside task**: The semaphore permit is acquired at the start of each task, not before spawning. This allows all tasks to be spawned immediately while the semaphore controls actual execution.
