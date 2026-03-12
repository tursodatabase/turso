# Adding New Backends

This guide explains how to add new SQL execution backends to the test runner, such as SDK bindings for Python, Go, JavaScript, etc.

## Overview

The test runner uses a trait-based backend system. To add a new backend:

1. Implement the `SqlBackend` trait
2. Implement the `DatabaseInstance` trait
3. Register the backend in the CLI

## Core Traits

### `SqlBackend`

Located in `src/backends/mod.rs`:

```rust
#[async_trait]
pub trait SqlBackend: Send + Sync {
    /// Name of this backend (for filtering and display)
    fn name(&self) -> &str;

    /// Create a new isolated database instance
    async fn create_database(&self, config: &DatabaseConfig)
        -> Result<Box<dyn DatabaseInstance>, BackendError>;
}
```

### `DatabaseInstance`

```rust
#[async_trait]
pub trait DatabaseInstance: Send + Sync {
    /// Execute SQL and return results
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError>;

    /// Close and cleanup the database
    async fn close(self: Box<Self>) -> Result<(), BackendError>;
}
```

### `QueryResult`

```rust
pub struct QueryResult {
    /// Rows returned, each row is a vector of string-formatted columns
    pub rows: Vec<Vec<String>>,
    /// Error message if the query failed
    pub error: Option<String>,
}

impl QueryResult {
    pub fn success(rows: Vec<Vec<String>>) -> Self;
    pub fn error(message: impl Into<String>) -> Self;
    pub fn is_error(&self) -> bool;
}
```

### `BackendError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("failed to create database: {0}")]
    CreateDatabase(String),

    #[error("failed to execute query: {0}")]
    Execute(String),

    #[error("failed to close database: {0}")]
    Close(String),

    #[error("backend not available: {0}")]
    NotAvailable(String),

    #[error("query timed out after {0:?}")]
    Timeout(Duration),
}
```

## Example: Python SDK Backend

Here's a complete example of implementing a Python SDK backend:

### Step 1: Create the Backend File

Create `src/backends/python.rs`:

```rust
use super::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
use crate::parser::ast::{DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::timeout;

/// Python SDK backend
pub struct PythonBackend {
    /// Path to python executable
    python_path: PathBuf,
    /// Path to the bridge script
    bridge_script: PathBuf,
    /// Query timeout
    timeout: Duration,
}

impl PythonBackend {
    pub fn new(python_path: impl Into<PathBuf>, bridge_script: impl Into<PathBuf>) -> Self {
        Self {
            python_path: python_path.into(),
            bridge_script: bridge_script.into(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl SqlBackend for PythonBackend {
    fn name(&self) -> &str {
        "python"
    }

    async fn create_database(&self, config: &DatabaseConfig)
        -> Result<Box<dyn DatabaseInstance>, BackendError>
    {
        let (db_path, temp_file) = match &config.location {
            DatabaseLocation::Memory => (":memory:".to_string(), None),
            DatabaseLocation::TempFile => {
                let temp = NamedTempFile::new()
                    .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
                let path = temp.path().to_string_lossy().to_string();
                (path, Some(temp))
            }
            DatabaseLocation::Path(path) => (path.to_string_lossy().to_string(), None),
        };

        // Spawn Python process with the bridge script
        let mut cmd = Command::new(&self.python_path);
        cmd.arg(&self.bridge_script);
        cmd.arg(&db_path);

        if config.readonly {
            cmd.arg("--readonly");
        }

        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let child = cmd.spawn()
            .map_err(|e| BackendError::CreateDatabase(format!("failed to spawn python: {}", e)))?;

        Ok(Box::new(PythonDatabaseInstance {
            child,
            timeout: self.timeout,
            _temp_file: temp_file,
        }))
    }
}

pub struct PythonDatabaseInstance {
    child: Child,
    timeout: Duration,
    _temp_file: Option<NamedTempFile>,
}

#[async_trait]
impl DatabaseInstance for PythonDatabaseInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        // Send SQL as JSON command
        let request = serde_json::json!({
            "type": "execute",
            "sql": sql
        });

        let stdin = self.child.stdin.as_mut()
            .ok_or_else(|| BackendError::Execute("stdin not available".to_string()))?;

        stdin.write_all(request.to_string().as_bytes()).await
            .map_err(|e| BackendError::Execute(e.to_string()))?;
        stdin.write_all(b"\n").await
            .map_err(|e| BackendError::Execute(e.to_string()))?;

        // Read response
        let stdout = self.child.stdout.as_mut()
            .ok_or_else(|| BackendError::Execute("stdout not available".to_string()))?;

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();

        let result = timeout(self.timeout, reader.read_line(&mut line))
            .await
            .map_err(|_| BackendError::Timeout(self.timeout))?
            .map_err(|e| BackendError::Execute(e.to_string()))?;

        if result == 0 {
            return Err(BackendError::Execute("unexpected EOF".to_string()));
        }

        // Parse JSON response
        let response: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| BackendError::Execute(format!("invalid response: {}", e)))?;

        if let Some(error) = response.get("error").and_then(|e| e.as_str()) {
            return Ok(QueryResult::error(error));
        }

        let rows: Vec<Vec<String>> = response.get("rows")
            .and_then(|r| serde_json::from_value(r.clone()).ok())
            .unwrap_or_default();

        Ok(QueryResult::success(rows))
    }

    async fn close(mut self: Box<Self>) -> Result<(), BackendError> {
        // Send close command
        if let Some(stdin) = self.child.stdin.as_mut() {
            let _ = stdin.write_all(b"{\"type\":\"close\"}\n").await;
        }

        // Wait for process to exit
        let _ = self.child.wait().await;
        Ok(())
    }
}
```

### Step 2: Create the Bridge Script

Create `scripts/python_bridge.py`:

```python
#!/usr/bin/env python3
import sys
import json
import turso  # or whatever the SDK module is called

def main():
    db_path = sys.argv[1]
    readonly = "--readonly" in sys.argv

    # Connect to database
    conn = turso.connect(db_path, readonly=readonly)

    # Process commands from stdin
    for line in sys.stdin:
        try:
            cmd = json.loads(line.strip())

            if cmd["type"] == "close":
                conn.close()
                break

            if cmd["type"] == "execute":
                sql = cmd["sql"]
                try:
                    cursor = conn.execute(sql)
                    rows = [[str(col) for col in row] for row in cursor.fetchall()]
                    print(json.dumps({"rows": rows}))
                except Exception as e:
                    print(json.dumps({"error": str(e)}))

            sys.stdout.flush()

        except Exception as e:
            print(json.dumps({"error": f"bridge error: {e}"}))
            sys.stdout.flush()

if __name__ == "__main__":
    main()
```

### Step 3: Register the Backend

Update `src/backends/mod.rs`:

```rust
pub mod cli;
pub mod python;  // Add this

pub use cli::CliBackend;
pub use python::PythonBackend;  // Add this
```

### Step 4: Add CLI Option

Update `src/main.rs` to support the new backend:

```rust
#[derive(Subcommand)]
enum Commands {
    Run {
        // ...existing options...

        /// Backend to use (cli, python)
        #[arg(long, default_value = "cli")]
        backend: String,

        /// Path to Python executable (for python backend)
        #[arg(long)]
        python: Option<PathBuf>,
    },
}
```

## Design Patterns for SDK Backends

### 1. Process-based (Recommended for Dynamic Languages)

Spawn a child process running the SDK, communicate via JSON over stdin/stdout.

**Pros:**
- Simple implementation
- Isolated from test runner process
- Easy to debug

**Cons:**
- Process spawn overhead
- Serialization overhead

### 2. FFI-based (For Native SDKs)

Use Rust FFI to call the SDK directly.

**Pros:**
- Maximum performance
- Direct memory access

**Cons:**
- Complex implementation
- Platform-specific issues
- Potential memory safety issues

### 3. HTTP-based (For Remote Testing)

Start an HTTP server with the SDK, send requests.

**Pros:**
- Language-independent
- Easy to test manually
- Can test remote databases

**Cons:**
- HTTP overhead
- More complex setup

## Testing Your Backend

### Unit Tests

Add tests in your backend file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_memory_database() {
        let backend = YourBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };

        let db = backend.create_database(&config).await;
        assert!(db.is_ok());
    }

    #[tokio::test]
    async fn test_execute_simple_query() {
        let backend = YourBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };

        let mut db = backend.create_database(&config).await.unwrap();
        let result = db.execute("SELECT 1").await.unwrap();

        assert!(!result.is_error());
        assert_eq!(result.rows, vec![vec!["1".to_string()]]);
    }
}
```

### Integration Tests

Create test files and run with your backend:

```bash
test-runner run tests/ --backend python --python /usr/bin/python3
```

## Checklist for New Backends

- [ ] Implement `SqlBackend` trait
- [ ] Implement `DatabaseInstance` trait
- [ ] Handle all `DatabaseLocation` variants (Memory, TempFile, Path)
- [ ] Support readonly mode
- [ ] Implement proper timeout handling
- [ ] Clean up resources in `close()`
- [ ] Return errors as `QueryResult::error()` not `BackendError` for SQL errors
- [ ] Add unit tests
- [ ] Add documentation
- [ ] Register in CLI
