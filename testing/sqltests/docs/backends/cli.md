# CLI Backend

The CLI backend executes SQL by spawning the `tursodb` CLI tool as a subprocess.

## Overview

```
┌─────────────────────────────────────────┐
│            Test Runner                  │
│  ┌───────────────────────────────────┐  │
│  │          CliBackend               │  │
│  │  ┌─────────────────────────────┐  │  │
│  │  │    CliDatabaseInstance      │  │  │
│  │  │  ┌───────────────────────┐  │  │  │
│  │  │  │  tursodb subprocess   │  │  │  │
│  │  │  │  stdin/stdout pipes   │  │  │  │
│  │  │  └───────────────────────┘  │  │  │
│  │  └─────────────────────────────┘  │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

## Configuration

```rust
pub struct CliBackend {
    /// Path to the tursodb binary
    binary_path: PathBuf,
    /// Working directory for the CLI
    working_dir: Option<PathBuf>,
    /// Timeout for query execution
    timeout: Duration,
}
```

## Usage

```rust
let backend = CliBackend::new("./target/debug/tursodb")
    .with_timeout(Duration::from_secs(30));

let config = DatabaseConfig {
    location: DatabaseLocation::Memory,
    readonly: false,
};

let mut db = backend.create_database(&config).await?;
let result = db.execute("SELECT 1;").await?;
db.close().await?;
```

## How It Works

### Database Creation

1. For `:memory:` databases: Use the CLI with `:memory:` path
2. For `:temp:` databases: Create a temp file and use it as the database path
3. For readonly databases: Open the existing file in read-only mode

### Query Execution

The backend communicates with `tursodb` using:

1. **Mode**: List mode (`-m list`) for pipe-separated output
2. **Input**: SQL sent via stdin
3. **Output**: Results read from stdout (pipe-separated columns)
4. **Errors**: Error messages read from stderr

### Output Parsing

List mode produces output like:
```
column1|column2|column3
value1|value2|value3
```

The backend parses this into `Vec<Vec<String>>`.

### Error Detection

Errors are detected by:
1. Non-zero exit code
2. Error messages in stderr
3. "Error:" prefix in output

## Example Session

```bash
# What the backend does internally:
echo "SELECT 1, 'hello';" | tursodb :memory: -m list

# Output:
1|hello
```

## Timeout Handling

- Default timeout: 30 seconds per query
- Configurable via `with_timeout()`
- On timeout: Process is killed, `BackendError::Timeout` returned

## Limitations

1. **Interactive commands**: Dot commands (`.tables`, `.schema`) may not work as expected
2. **Multi-statement**: Each `execute()` call is a separate CLI invocation
3. **Transactions**: Not persisted across `execute()` calls for `:memory:` databases

## Implementation Notes

### Process Management

Each `execute()` call spawns a new `tursodb` process. This ensures:
- Clean state for each query
- No connection pooling issues
- Isolation between tests

For better performance with multiple queries, consider:
- Batching queries with `;` separator
- Using an embedded backend instead

### Temp File Management

For `:temp:` databases:
- Temp files are created in system temp directory
- Files are deleted when `DatabaseInstance::close()` is called
- Uses `tempfile` crate for safe cleanup

---

## Implementation Details

### Struct Definitions

```rust
pub struct CliBackend {
    binary_path: PathBuf,
    working_dir: Option<PathBuf>,
    timeout: Duration,  // Default: 30 seconds
}

pub struct CliDatabaseInstance {
    binary_path: PathBuf,
    working_dir: Option<PathBuf>,
    db_path: String,
    readonly: bool,
    timeout: Duration,
    _temp_file: Option<NamedTempFile>,  // Keeps temp file alive
}
```

### Key Implementation Notes

1. **Setup Buffering for Memory Databases**: For `:memory:` databases, `execute_setup()` buffers SQL instead of executing immediately. This is necessary because each CLI invocation creates a fresh in-memory database. The buffered SQL is combined with the test query in `execute()`.

2. **Temp File Lifetime**: The `_temp_file` field keeps the `NamedTempFile` alive for the duration of the database instance. The underscore prefix indicates it's intentionally unused directly - its presence prevents the temp file from being deleted prematurely.

3. **Stdin/Stdout Communication**: SQL is written to stdin, then stdin is closed to signal end of input. Results are read from stdout after process completion. The `-q` flag suppresses the banner output.

4. **Error Detection Strategy**:
   - Check stderr for "Error" or "error" substrings
   - Check stdout for error markers like "× " (tursodb error format) or "error:"
   - Check process exit status
   - Return errors as `QueryResult::error()` rather than `BackendError` to allow error expectation tests

5. **Output Parsing**: List mode (`-m list`) produces pipe-separated values which are split and collected into `Vec<Vec<String>>`.

### Builder Pattern

```rust
let backend = CliBackend::new("./target/debug/tursodb")
    .with_working_dir("/path/to/workdir")
    .with_timeout(Duration::from_secs(60));
```

### Async Execution Flow

```rust
async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
    // 1. Build command with args
    let mut cmd = Command::new(&self.binary_path);
    cmd.arg(&self.db_path).arg("-m").arg("list");

    // 2. Set up pipes
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());

    // 3. Spawn and write SQL
    let mut child = cmd.spawn()?;
    child.stdin.as_mut().unwrap().write_all(sql.as_bytes()).await?;
    child.stdin.take();  // Close stdin

    // 4. Wait with timeout
    let output = timeout(self.timeout, child.wait_with_output()).await??;

    // 5. Parse and return
    Ok(QueryResult::success(parse_list_output(&stdout)))
}
```

### Unit Tests

The module includes tests for output parsing:
- `test_parse_list_output_empty` - Empty output returns empty vec
- `test_parse_list_output_single_column` - Single values per row
- `test_parse_list_output_multiple_columns` - Pipe-separated columns
- `test_parse_list_output_empty_values` - Handles empty columns (`1||3`)
- `test_parse_list_output_trailing_newline` - Ignores trailing newlines
