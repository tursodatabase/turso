//! Oracles for matrix cases.
//!
//! Matrix expansions have no stored expectations: each case runs its setup
//! and statement against an oracle engine, and the invariant is simply that
//! the engine under test and the oracle agree — same rows in the same order,
//! or both rejecting the statement (error messages are not compared).
//!
//! On the Rust backend the oracle is the bundled SQLite (rusqlite). Its
//! version is pinned by Cargo.lock, so expected behavior only moves when the
//! dependency is deliberately upgraded. Values are rendered through the same
//! formatting the Rust backend applies to Turso values (notably
//! `format_real`'s `%!.15g`), so comparing the resulting strings compares
//! typed values, not incidental text formatting.
//!
//! On the tursopg backend the oracle is a real PostgreSQL server. Each case
//! gets a new database on it, and both servers return rows as text over the
//! same wire-protocol client, so the rows compare as the same text.

use crate::backends::postgres::PostgresBackend;
use crate::backends::rust::format_real;
use crate::backends::{DatabaseInstance, SqlBackend};
use crate::parser::ast::DatabaseConfig;
use rusqlite::types::ValueRef;
use std::sync::Arc;

/// Outcome of running one statement against the oracle.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleOutcome {
    /// Rows, each pipe-joined with the shared value formatting.
    Rows(Vec<Vec<String>>),
    /// The oracle rejected the statement.
    Error(String),
}

/// The engine that matrix cases are checked against.
#[derive(Clone)]
pub enum MatrixOracle {
    /// The bundled SQLite, for the Rust backend.
    Sqlite,
    /// A real PostgreSQL server, for the tursopg backend.
    Postgres(Arc<PostgresBackend>),
}

impl MatrixOracle {
    pub fn name(&self) -> &'static str {
        match self {
            MatrixOracle::Sqlite => "SQLite",
            MatrixOracle::Postgres(_) => "PostgreSQL",
        }
    }

    /// Run `setups` then `sql` on a new database of the oracle.
    /// Setup failures are harness errors (`Err`); statement failures are an
    /// ordinary `OracleOutcome::Error` to be matched against the engine's.
    pub async fn run(
        &self,
        db_config: &DatabaseConfig,
        setups: &[String],
        sql: &str,
    ) -> Result<OracleOutcome, String> {
        match self {
            MatrixOracle::Sqlite => run_oracle(setups, sql),
            MatrixOracle::Postgres(backend) => {
                run_postgres_oracle(backend, db_config, setups, sql).await
            }
        }
    }
}

impl std::fmt::Debug for MatrixOracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Run `setups` then `sql` on a fresh in-memory bundled-SQLite connection.
/// Setup failures are harness errors (`Err`); statement failures are an
/// ordinary `OracleOutcome::Error` to be matched against Turso's.
pub fn run_oracle(setups: &[String], sql: &str) -> Result<OracleOutcome, String> {
    let conn = rusqlite::Connection::open_in_memory()
        .map_err(|e| format!("oracle: failed to open in-memory sqlite: {e}"))?;
    for setup in setups {
        conn.execute_batch(setup)
            .map_err(|e| format!("oracle: setup failed: {e}"))?;
    }

    let mut stmt = match conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(e) => return Ok(OracleOutcome::Error(e.to_string())),
    };
    let column_count = stmt.column_count();
    let mut rows = match stmt.query([]) {
        Ok(rows) => rows,
        Err(e) => return Ok(OracleOutcome::Error(e.to_string())),
    };

    let mut out = Vec::new();
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                let mut formatted = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value = row
                        .get_ref(i)
                        .map_err(|e| format!("oracle: failed to read column {i}: {e}"))?;
                    formatted.push(format_value(value));
                }
                out.push(formatted);
            }
            Ok(None) => break,
            // A statement can emit rows and then fail (e.g. sum() integer
            // overflow at value time); like the harness's own comparison,
            // an error anywhere makes the whole statement an error case.
            Err(e) => return Ok(OracleOutcome::Error(e.to_string())),
        }
    }
    Ok(OracleOutcome::Rows(out))
}

/// Mirror of the Rust backend's `value_to_string` for rusqlite values.
fn format_value(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => String::new(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => format_real(f),
        ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).to_string(),
        ValueRef::Blob(bytes) => String::from_utf8_lossy(bytes).to_string(),
    }
}

async fn run_postgres_oracle(
    backend: &PostgresBackend,
    db_config: &DatabaseConfig,
    setups: &[String],
    sql: &str,
) -> Result<OracleOutcome, String> {
    let mut db = backend
        .create_database(db_config)
        .await
        .map_err(|e| format!("oracle: {e}"))?;
    let outcome = run_on_database(db.as_mut(), setups, sql).await;
    let closed = db.close().await;
    let outcome = outcome?;
    closed.map_err(|e| format!("oracle: {e}"))?;
    Ok(outcome)
}

async fn run_on_database(
    db: &mut dyn DatabaseInstance,
    setups: &[String],
    sql: &str,
) -> Result<OracleOutcome, String> {
    for setup in setups {
        db.execute_setup(setup)
            .await
            .map_err(|e| format!("oracle: setup failed: {e}"))?;
    }
    let result = db.execute(sql).await.map_err(|e| format!("oracle: {e}"))?;
    Ok(match result.error {
        Some(error) => OracleOutcome::Error(error),
        None => OracleOutcome::Rows(result.rows),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{BackendError, DatabaseFileHandle, QueryResult};

    #[test]
    fn oracle_runs_setup_and_query() {
        let setups =
            vec!["CREATE TABLE t(a, b); INSERT INTO t VALUES (1, 2.5), (NULL, 'x');".to_string()];
        let result = run_oracle(&setups, "SELECT a, b FROM t ORDER BY rowid").unwrap();
        assert_eq!(
            result,
            OracleOutcome::Rows(vec![
                vec!["1".to_string(), "2.5".to_string()],
                vec!["".to_string(), "x".to_string()],
            ])
        );
    }

    #[test]
    fn oracle_reports_statement_errors() {
        let result = run_oracle(&[], "SELECT no_such_col FROM missing").unwrap();
        assert!(matches!(result, OracleOutcome::Error(_)));
    }

    #[tokio::test]
    async fn database_oracle_returns_the_rows_or_the_error_of_the_statement() {
        let mut db = ScriptedDatabase::new(vec![
            QueryResult::success(vec![]),
            QueryResult::success(vec![vec!["1".to_string(), "".to_string()]]),
        ]);
        let result = run_on_database(&mut db, &["CREATE TABLE t(a int)".to_string()], "SELECT")
            .await
            .unwrap();
        assert_eq!(
            result,
            OracleOutcome::Rows(vec![vec!["1".to_string(), "".to_string()]])
        );

        let mut db = ScriptedDatabase::new(vec![QueryResult::error("division by zero")]);
        let result = run_on_database(&mut db, &[], "SELECT 1 / 0").await.unwrap();
        assert_eq!(result, OracleOutcome::Error("division by zero".to_string()));
    }

    #[tokio::test]
    async fn database_oracle_treats_a_failed_setup_as_a_harness_error() {
        let mut db = ScriptedDatabase::new(vec![QueryResult::error("syntax error")]);
        let result = run_on_database(&mut db, &["CREATE TABLE".to_string()], "SELECT 1").await;
        assert_eq!(
            result,
            Err("oracle: setup failed: failed to execute SQL: syntax error".to_string())
        );
    }

    /// A database that answers each query with the next scripted result.
    struct ScriptedDatabase {
        results: std::collections::VecDeque<QueryResult>,
    }

    impl ScriptedDatabase {
        fn new(results: Vec<QueryResult>) -> Self {
            Self {
                results: results.into(),
            }
        }
    }

    #[async_trait::async_trait]
    impl DatabaseInstance for ScriptedDatabase {
        async fn execute(&mut self, _sql: &str) -> Result<QueryResult, BackendError> {
            Ok(self.results.pop_front().expect("no scripted result left"))
        }

        async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
            Ok(DatabaseFileHandle::none())
        }
    }
}
