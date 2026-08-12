use super::{BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend};
use crate::{
    backends::DefaultDatabaseResolver,
    parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation},
};
use async_trait::async_trait;
use query_result_oracle::{ResultSet, Row as TypedRow, Value as TypedValue, diff_result_sets};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::NamedTempFile;
use turso::{Builder, Connection, Database, Value};
use turso_parser::ast::{Cmd, Stmt};

const TURSO_RUST_EXPERIMENTAL_FEATURES: &[&str] = &[
    "attach",
    "index_method",
    "views",
    "custom_types",
    "generated_columns",
    "vacuum",
    "without_rowid",
];

fn apply_turso_experimental_features(mut builder: Builder) -> Builder {
    for feature in TURSO_RUST_EXPERIMENTAL_FEATURES {
        builder = match *feature {
            "attach" => builder.experimental_attach(true),
            "index_method" => builder.experimental_index_method(true),
            "views" => builder.experimental_materialized_views(true),
            "custom_types" => builder.experimental_custom_types(true),
            "generated_columns" => builder.experimental_generated_columns(true),
            "vacuum" => builder.experimental_vacuum(true),
            "without_rowid" => builder.experimental_without_rowid(true),
            _ => unreachable!("unexpected sqltests Rust backend experimental feature"),
        };
    }
    builder
}

/// Native Rust backend using Turso bindings directly
pub struct RustBackend {
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
    /// Enable MVCC mode
    mvcc: bool,
}

impl RustBackend {
    pub fn new() -> Self {
        Self {
            default_db_resolver: None,
            mvcc: false,
        }
    }

    /// Set the default database resolver
    pub fn with_default_db_resolver(mut self, resolver: Arc<dyn DefaultDatabaseResolver>) -> Self {
        self.default_db_resolver = Some(resolver);
        self
    }

    /// Enable MVCC mode (experimental journal mode)
    pub fn with_mvcc(mut self, mvcc: bool) -> Self {
        self.mvcc = mvcc;
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

    fn backend_type(&self) -> Backend {
        Backend::Rust
    }

    fn capabilities(&self) -> HashSet<Capability> {
        HashSet::from_iter([
            Capability::Trigger,
            Capability::MaterializedViews,
            Capability::CustomTypes,
        ])
    }

    fn supports_snapshots(&self) -> bool {
        true
    }

    fn supports_materialized_view_verification(&self) -> bool {
        true
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
        let db = apply_turso_experimental_features(Builder::new_local(&db_path))
            .build()
            .await
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;

        // Connect to the database
        let conn = db
            .connect()
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;

        // Prepend MVCC pragma if enabled (skip for readonly databases; the generated readonly DBs are already in MVCC mode).
        if self.mvcc && !config.readonly {
            let mut rows = conn
                .query("PRAGMA journal_mode = 'mvcc'", ())
                .await
                .map_err(|e| {
                    BackendError::CreateDatabase(format!("failed to enable MVCC mode: {e}"))
                })?;
            // Consume the result row
            let _ = rows.next().await;
        }

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

struct MaterializedViewComparison {
    name: String,
    definition: String,
    stored: ResultSet,
    fresh: ResultSet,
}

fn materialized_view_mismatches(
    comparisons: impl IntoIterator<Item = MaterializedViewComparison>,
) -> Vec<String> {
    comparisons
        .into_iter()
        .filter_map(|comparison| {
            let diff = diff_result_sets(&comparison.stored, &comparison.fresh);
            (!diff.is_empty()).then(|| {
                format!(
                    "materialized view {} differs from its defining SELECT\n  definition: {}\n{}",
                    comparison.name,
                    comparison.definition,
                    diff.describe("materialized view", "fresh SELECT", 20)
                )
            })
        })
        .collect()
}

impl RustDatabaseInstance {
    /// Execute SQL (which may contain multiple statements) and collect results
    /// Results are returned from all SELECT/PRAGMA statements, concatenated together
    async fn execute_query(
        &self,
        sql: &str,
        verify_materialized_views: bool,
    ) -> Result<QueryResult, turso::Error> {
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
                        {
                            // Drop the statement and its row cursor before the
                            // invariant queries use the same connection.
                            let mut stmt = self.conn.prepare(stmt_sql).await?;
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

                        if verify_materialized_views {
                            self.verify_materialized_views(stmt_sql).await?;
                        }
                    }

                    // Move to the remaining SQL
                    remaining = &remaining[offset..];
                }
                Some(Err(e)) => {
                    return Err(turso::Error::Error(format!("Parse error: {e}")));
                }
                None => {
                    // No more statements
                    break;
                }
            }
        }

        Ok(QueryResult::success(all_rows))
    }

    async fn collect_typed_result(&self, sql: &str) -> Result<ResultSet, turso::Error> {
        let mut stmt = self.conn.prepare(sql).await?;
        let column_count = stmt.column_count();
        let mut query_rows = stmt.query(()).await?;
        let mut rows = Vec::new();
        while let Some(row) = query_rows.next().await? {
            let mut values = Vec::with_capacity(column_count);
            for column in 0..column_count {
                values.push(to_typed_value(row.get_value(column)?));
            }
            rows.push(TypedRow(values));
        }
        ResultSet::new(column_count, rows)
            .map_err(|error| turso::Error::Error(format!("invalid typed query result: {error}")))
    }

    async fn materialized_view_definitions(&self) -> Result<Vec<(String, String)>, turso::Error> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name, sql \
                 FROM sqlite_schema \
                 WHERE type = 'view' AND rootpage <> 0 AND sql IS NOT NULL \
                 ORDER BY name",
            )
            .await?;
        let mut query_rows = stmt.query(()).await?;
        let mut definitions = Vec::new();
        while let Some(row) = query_rows.next().await? {
            let name = match row.get_value(0)? {
                Value::Text(name) => name,
                value => {
                    return Err(turso::Error::Error(format!(
                        "materialized-view schema name is not text: {value:?}"
                    )));
                }
            };
            let create_sql = match row.get_value(1)? {
                Value::Text(sql) => sql,
                value => {
                    return Err(turso::Error::Error(format!(
                        "materialized-view schema SQL is not text for {name}: {value:?}"
                    )));
                }
            };
            let command = turso_parser::parser::Parser::new(create_sql.as_bytes())
                .next_cmd()
                .map_err(|error| {
                    turso::Error::Error(format!(
                        "cannot parse stored materialized-view SQL for {name}: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    turso::Error::Error(format!("stored materialized-view SQL for {name} is empty"))
                })?;
            let Cmd::Stmt(Stmt::CreateMaterializedView { select, .. }) = command else {
                return Err(turso::Error::Error(format!(
                    "schema row for materialized view {name} is not CREATE MATERIALIZED VIEW"
                )));
            };
            definitions.push((name, select.to_string()));
        }
        Ok(definitions)
    }

    async fn verify_materialized_views(&self, checkpoint_sql: &str) -> Result<(), turso::Error> {
        let definitions = self.materialized_view_definitions().await?;
        let mut failures = Vec::new();
        let mut comparisons = Vec::new();
        for (name, definition) in definitions {
            let quoted_name = name.replace('"', "\"\"");
            let stored_sql = format!("SELECT * FROM \"{quoted_name}\"");
            let stored = match self.collect_typed_result(&stored_sql).await {
                Ok(result) => result,
                Err(error) => {
                    failures.push(format!(
                        "reading materialized view {name} failed: {error}\n  definition: {definition}"
                    ));
                    continue;
                }
            };
            let fresh = match self.collect_typed_result(&definition).await {
                Ok(result) => result,
                Err(error) => {
                    failures.push(format!(
                        "evaluating the defining SELECT for {name} failed: {error}\n  definition: {definition}"
                    ));
                    continue;
                }
            };
            comparisons.push(MaterializedViewComparison {
                name,
                definition,
                stored,
                fresh,
            });
        }
        failures.extend(materialized_view_mismatches(comparisons));

        if failures.is_empty() {
            Ok(())
        } else {
            Err(turso::Error::Error(format!(
                "materialized-view invariant failed after statement:\n  {checkpoint_sql}\n\n{}",
                failures.join("\n\n")
            )))
        }
    }
}

fn to_typed_value(value: Value) -> TypedValue {
    match value {
        Value::Null => TypedValue::Null,
        Value::Integer(value) => TypedValue::Integer(value),
        Value::Real(value) => TypedValue::Real(value),
        Value::Text(value) => TypedValue::Text(value),
        Value::Blob(value) => TypedValue::Blob(value),
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
        match self.execute_query(sql, false).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Return error as QueryResult (not BackendError)
                // This matches how the test framework expects errors
                Ok(QueryResult::error(e.to_string()))
            }
        }
    }

    async fn execute_with_materialized_view_verification(
        &mut self,
        sql: &str,
    ) -> Result<QueryResult, BackendError> {
        match self.execute_query(sql, true).await {
            Ok(result) => Ok(result),
            Err(error) => Ok(QueryResult::error(error.to_string())),
        }
    }

    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        // Connection and database are dropped automatically
        match self._temp_file {
            Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
            None => Ok(DatabaseFileHandle::none()),
        }
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
/// Also used by the matrix oracle so both engines' reals are rendered
/// through the identical formatter, making text comparison value-exact.
pub(crate) fn format_real(f: f64) -> String {
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
    if abs_f != 0.0 && !(1e-4..1e15).contains(&abs_f) {
        // Use exponential notation with up to 15 significant digits
        let formatted = format!("{f:.14e}");
        // Clean up: remove trailing zeros in mantissa and unnecessary + in exponent
        let formatted = clean_exponential(&formatted);
        return formatted;
    }

    // Check if it's a whole number that can be represented exactly
    if f.fract() == 0.0 {
        format!("{}.0", f as i64)
    } else {
        // Format with 15 significant digits to match SQLite's %!.15g format
        // Then remove trailing zeros after the decimal point
        format_with_significant_digits(f, 15)
    }
}

/// Format a float with a specific number of significant digits, removing trailing zeros
fn format_with_significant_digits(f: f64, sig_digits: usize) -> String {
    if f == 0.0 {
        return "0.0".to_string();
    }

    let abs_f = f.abs();
    // Count digits before decimal point
    let digits_before_decimal = if abs_f >= 1.0 {
        (abs_f.log10().floor() as usize) + 1
    } else {
        0
    };

    // Calculate decimal places needed for the desired significant digits
    let decimal_places = sig_digits.saturating_sub(digits_before_decimal);

    // Format with calculated decimal places
    let formatted = format!("{f:.decimal_places$}");

    // Remove trailing zeros after decimal point, but keep at least one digit after decimal
    if formatted.contains('.') {
        let trimmed = formatted.trim_end_matches('0');
        if trimmed.ends_with('.') {
            format!("{trimmed}0")
        } else {
            trimmed.to_string()
        }
    } else {
        formatted
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
            format!("{mantissa}0")
        } else {
            mantissa.to_string()
        };

        // Parse and format exponent (remove leading zeros and +)
        let exp_num: i32 = exponent.parse().unwrap_or(0);
        format!("{mantissa}e{exp_num:+}")
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed_result(column_count: usize, rows: Vec<Vec<TypedValue>>) -> ResultSet {
        ResultSet::new(
            column_count,
            rows.into_iter().map(TypedRow).collect::<Vec<_>>(),
        )
        .unwrap()
    }

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
        assert_eq!(value_to_string(&Value::Real(3.15)), "3.15");
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

    #[tokio::test]
    async fn test_execute_multi_statement_insert_then_select() {
        let backend = RustBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };
        let mut instance = backend.create_database(&config).await.unwrap();

        let result = instance
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, col_a TEXT, col_b TEXT, col_c TEXT, col_d TEXT); INSERT INTO test (col_b, col_d, col_a, col_c) VALUES ('1', '2', '3', '4'); SELECT * FROM test;")
            .await
            .unwrap();

        assert!(!result.is_error());
        assert_eq!(result.rows, vec![vec!["1", "3", "1", "4", "2"]]);
    }

    #[tokio::test]
    async fn test_materialized_view_verification_runs_between_statements() {
        let backend = RustBackend::new();
        let config = DatabaseConfig {
            location: DatabaseLocation::Memory,
            readonly: false,
        };
        let mut instance = backend.create_database(&config).await.unwrap();

        let result = instance
            .execute_with_materialized_view_verification(
                "CREATE TABLE t(g TEXT, x INTEGER);
                 INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', NULL);
                 CREATE MATERIALIZED VIEW v AS
                   SELECT g, COUNT(x), SUM(x) FROM t GROUP BY g;
                 BEGIN;
                 INSERT INTO t VALUES ('b', 4), ('b', 4);
                 DELETE FROM t WHERE g = 'a' AND x = 1;
                 SELECT * FROM v ORDER BY g;
                 ROLLBACK;
                 SELECT * FROM v ORDER BY g;",
            )
            .await
            .unwrap();

        assert!(!result.is_error(), "{:?}", result.error);
        assert_eq!(
            result.rows,
            vec![
                vec!["a", "1", "2"],
                vec!["b", "2", "8"],
                vec!["a", "2", "3"],
                vec!["b", "0", ""],
            ]
        );
    }

    #[test]
    fn materialized_view_verifier_reports_all_exact_result_mismatches() {
        let comparisons = [
            MaterializedViewComparison {
                name: "multiplicity_view".into(),
                definition: "SELECT DISTINCT x FROM t".into(),
                stored: typed_result(
                    1,
                    vec![vec![TypedValue::Integer(1)], vec![TypedValue::Integer(1)]],
                ),
                fresh: typed_result(1, vec![vec![TypedValue::Integer(1)]]),
            },
            MaterializedViewComparison {
                name: "null_view".into(),
                definition: "SELECT '' FROM t".into(),
                stored: typed_result(1, vec![vec![TypedValue::Null]]),
                fresh: typed_result(1, vec![vec![TypedValue::Text(String::new())]]),
            },
            MaterializedViewComparison {
                name: "numeric_type_view".into(),
                definition: "SELECT CAST(x AS REAL) FROM t".into(),
                stored: typed_result(1, vec![vec![TypedValue::Integer(1)]]),
                fresh: typed_result(1, vec![vec![TypedValue::Real(1.0)]]),
            },
            MaterializedViewComparison {
                name: "empty_arity_view".into(),
                definition: "SELECT x, x FROM t WHERE 0".into(),
                stored: typed_result(1, vec![]),
                fresh: typed_result(2, vec![]),
            },
        ];

        let failures = materialized_view_mismatches(comparisons);
        assert_eq!(failures.len(), 4);
        for name in [
            "multiplicity_view",
            "null_view",
            "numeric_type_view",
            "empty_arity_view",
        ] {
            assert!(
                failures.iter().any(|failure| failure.contains(name)),
                "missing failure for {name}: {failures:#?}"
            );
        }
        assert!(failures[0].contains("1 row occurrence(s)"));
        assert!(failures[1].contains("null"));
        assert!(failures[1].contains("text(\"\")"));
        assert!(failures[2].contains("integer(1)"));
        assert!(failures[2].contains("real(1.0)"));
        assert!(
            failures[3]
                .contains("column count differs: materialized view has 1, fresh SELECT has 2")
        );
    }
}
