use super::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
use crate::{
    backends::DefaultDatabaseResolver,
    parser::ast::{DatabaseConfig, DatabaseLocation},
};
use async_trait::async_trait;
use std::sync::Arc;
use tempfile::NamedTempFile;
use turso::{Builder, Connection, Database, Value};

/// Native Rust backend using Turso bindings directly
pub struct RustBackend {
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
}

impl RustBackend {
    pub fn new() -> Self {
        Self {
            default_db_resolver: None,
        }
    }

    /// Set the default database resolver
    pub fn with_default_db_resolver(mut self, resolver: Arc<dyn DefaultDatabaseResolver>) -> Self {
        self.default_db_resolver = Some(resolver);
        self
    }
}

impl Default for RustBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SqlBackend for RustBackend {
    fn name(&self) -> &str {
        "rust"
    }

    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        let (db_path, temp_file) = match &config.location {
            DatabaseLocation::Memory => (":memory:".to_string(), None),
            DatabaseLocation::TempFile => {
                let temp = NamedTempFile::new()
                    .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
                let path = temp.path().to_string_lossy().to_string();
                (path, Some(temp))
            }
            DatabaseLocation::Path(path) => (path.to_string_lossy().to_string(), None),
            DatabaseLocation::Default | DatabaseLocation::DefaultNoRowidAlias => {
                // Resolve the path using the resolver
                let resolved = self
                    .default_db_resolver
                    .as_ref()
                    .and_then(|r| r.resolve(&config.location))
                    .ok_or_else(|| {
                        BackendError::CreateDatabase(
                            "default database not generated - no resolver configured".to_string(),
                        )
                    })?;
                (resolved.to_string_lossy().to_string(), None)
            }
        };

        // Create the database using the Turso builder
        let db = Builder::new_local(&db_path)
            .build()
            .await
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;

        // Connect to the database
        let conn = db
            .connect()
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;

        Ok(Box::new(RustDatabaseInstance {
            _db: db,
            conn,
            _temp_file: temp_file,
        }))
    }
}

/// A database instance backed by Turso Rust bindings
pub struct RustDatabaseInstance {
    /// The database handle (kept alive for the connection)
    _db: Database,
    /// The connection to execute queries on
    conn: Connection,
    /// Keep temp file alive - deleted when dropped
    _temp_file: Option<NamedTempFile>,
}

impl RustDatabaseInstance {
    /// Execute SQL (which may contain multiple statements) and collect results
    /// Results are returned from all SELECT/PRAGMA statements, concatenated together
    async fn execute_query(&self, sql: &str) -> Result<QueryResult, turso::Error> {
        let mut all_rows = Vec::new();
        let mut remaining = sql;

        // FIXME: Overhead of having to parse the query twice, but at least we are correct
        // Use turso_parser to properly split SQL into statements
        while !remaining.trim().is_empty() {
            let mut parser = turso_parser::parser::Parser::new(remaining.as_bytes());

            match parser.next() {
                Some(Ok(_cmd)) => {
                    // Get the offset where the parser stopped (after this statement)
                    let offset = parser.offset();

                    // Extract the statement SQL (everything up to the offset)
                    let stmt_sql = &remaining[..offset].trim();

                    if !stmt_sql.is_empty() {
                        // Prepare and execute the statement
                        let mut stmt = self.conn.prepare(stmt_sql).await?;

                        // Use query() which works for both SELECT and non-SELECT statements
                        let mut rows_result = stmt.query(()).await?;
                        while let Some(row) = rows_result.next().await? {
                            let mut row_values = Vec::new();
                            let col_count = row.column_count();
                            for i in 0..col_count {
                                let value = row.get_value(i)?;
                                row_values.push(value_to_string(&value));
                            }
                            all_rows.push(row_values);
                        }
                    }

                    // Move to the remaining SQL
                    remaining = &remaining[offset..];
                }
                Some(Err(e)) => {
                    return Err(turso::Error::Error(format!("Parse error: {}", e)));
                }
                None => {
                    // No more statements
                    break;
                }
            }
        }

        Ok(QueryResult::success(all_rows))
    }
}

#[async_trait]
impl DatabaseInstance for RustDatabaseInstance {
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        // Use execute_batch for setup SQL (may contain multiple statements)
        self.conn
            .execute_batch(sql)
            .await
            .map_err(|e| BackendError::Execute(e.to_string()))?;
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        // Try to execute as a query to capture results
        match self.execute_query(sql).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Return error as QueryResult (not BackendError)
                // This matches how the test framework expects errors
                Ok(QueryResult::error(e.to_string()))
            }
        }
    }

    async fn close(self: Box<Self>) -> Result<(), BackendError> {
        // Connection and database are dropped automatically
        // Temp file will be deleted when _temp_file is dropped
        Ok(())
    }
}

/// Convert a Turso Value to its string representation
///
/// This matches the output format expected by the test framework:
/// - NULL becomes empty string (matching CLI list mode)
/// - Integer and Real use standard formatting
/// - Text is used as-is
/// - Blob is converted to UTF-8 string (matching CLI list mode behavior)
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format_real(*f),
        Value::Text(s) => s.clone(),
        Value::Blob(bytes) => String::from_utf8_lossy(bytes).to_string(),
    }
}

/// Format a floating-point number to match SQLite's output
/// SQLite uses %!.15g format (15 significant digits, no trailing zeros)
fn format_real(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }

    // Use exponential notation for very large or very small numbers (like SQLite's %g)
    let abs_f = f.abs();
    if abs_f != 0.0 && (abs_f >= 1e15 || abs_f < 1e-4) {
        // Use exponential notation with up to 15 significant digits
        let formatted = format!("{:.14e}", f);
        // Clean up: remove trailing zeros in mantissa and unnecessary + in exponent
        let formatted = clean_exponential(&formatted);
        return formatted;
    }

    // Check if it's a whole number that can be represented exactly
    if f.fract() == 0.0 {
        format!("{}.0", f as i64)
    } else {
        // Use default formatting which should match SQLite for most cases
        format!("{}", f)
    }
}

/// Clean up exponential notation to match SQLite's format
fn clean_exponential(s: &str) -> String {
    // Input format: "-1.23456789012340e+18" or "1.23456789012340e-05"
    if let Some(e_pos) = s.find('e') {
        let mantissa = &s[..e_pos];
        let exponent = &s[e_pos + 1..];

        // Remove trailing zeros from mantissa (but keep at least one digit after decimal)
        let mantissa = mantissa.trim_end_matches('0');
        let mantissa = if mantissa.ends_with('.') {
            format!("{}0", mantissa)
        } else {
            mantissa.to_string()
        };

        // Parse and format exponent (remove leading zeros and +)
        let exp_num: i32 = exponent.parse().unwrap_or(0);
        format!("{}e{:+}", mantissa, exp_num)
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_to_string_null() {
        assert_eq!(value_to_string(&Value::Null), "");
    }

    #[test]
    fn test_value_to_string_integer() {
        assert_eq!(value_to_string(&Value::Integer(42)), "42");
        assert_eq!(value_to_string(&Value::Integer(-1)), "-1");
        assert_eq!(value_to_string(&Value::Integer(0)), "0");
    }

    #[test]
    fn test_value_to_string_real() {
        assert_eq!(value_to_string(&Value::Real(3.14)), "3.14");
        assert_eq!(value_to_string(&Value::Real(1.0)), "1.0");
        assert_eq!(value_to_string(&Value::Real(-2.5)), "-2.5");
    }

    #[test]
    fn test_value_to_string_text() {
        assert_eq!(value_to_string(&Value::Text("hello".into())), "hello");
        assert_eq!(value_to_string(&Value::Text("".into())), "");
    }

    #[test]
    fn test_value_to_string_blob() {
        // Blobs are converted to UTF-8 strings (matching CLI list mode)
        assert_eq!(value_to_string(&Value::Blob(b"hello".to_vec())), "hello");
        assert_eq!(
            value_to_string(&Value::Blob(b"independent_jaeckle".to_vec())),
            "independent_jaeckle"
        );
        // Valid UTF-8 sequences are decoded properly
        // 0xDE 0xAD is valid UTF-8 for U+07AD
        assert_eq!(value_to_string(&Value::Blob(vec![0xDE, 0xAD])), "\u{07AD}");
        // Invalid UTF-8 uses replacement character (U+FFFD)
        let result = value_to_string(&Value::Blob(vec![0xFF, 0xFE]));
        assert!(result.contains('\u{FFFD}'));
    }

    #[tokio::test]
    async fn test_create_memory_database() {
        let backend = RustBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };
        let instance = backend.create_database(&config).await;
        assert!(instance.is_ok());
    }

    #[tokio::test]
    async fn test_execute_simple_query() {
        let backend = RustBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };
        let mut instance = backend.create_database(&config).await.unwrap();

        instance
            .execute_setup("CREATE TABLE t(x INTEGER)")
            .await
            .unwrap();
        instance
            .execute_setup("INSERT INTO t VALUES (1), (2)")
            .await
            .unwrap();

        let result = instance
            .execute("SELECT x FROM t ORDER BY x")
            .await
            .unwrap();
        assert!(!result.is_error());
        assert_eq!(result.rows, vec![vec!["1"], vec!["2"]]);
    }

    #[tokio::test]
    async fn test_execute_error_returns_query_result() {
        let backend = RustBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };
        let mut instance = backend.create_database(&config).await.unwrap();

        let result = instance.execute("SELECT * FROM nonexistent").await.unwrap();
        assert!(result.is_error());
        assert!(result.error.unwrap().contains("no such table"));
    }
}
