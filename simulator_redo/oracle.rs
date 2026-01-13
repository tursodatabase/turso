//! Oracle implementations for validating database behavior.
//!
//! Oracles are predicates that verify properties of database execution.
//! The primary oracle is the DifferentialOracle which compares Turso
//! results against SQLite.

use std::sync::Arc;

use anyhow::Result;
use sql_gen_prop::SqlStatement;
use sql_gen_prop::SqlValue;
use sql_gen_prop::result::diff_results;
use turso_core::Value;

/// Result of an oracle check.
#[derive(Debug, Clone)]
pub enum OracleResult {
    /// The oracle check passed.
    Pass,
    /// The oracle check passed but with a warning (e.g., LIMIT without ORDER BY).
    Warning(String),
    /// The oracle check failed with a reason.
    Fail(String),
}

impl OracleResult {
    pub fn is_pass(&self) -> bool {
        matches!(self, OracleResult::Pass)
    }

    pub fn is_warning(&self) -> bool {
        matches!(self, OracleResult::Warning(_))
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, OracleResult::Fail(_))
    }
}

/// A row of values from a query result.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Row(pub Vec<SqlValue>);

/// Trait for oracles that can check database properties.
pub trait Oracle {
    /// Check the oracle after executing a statement.
    ///
    /// Returns Pass if the property holds, Warning for non-fatal issues,
    /// or Fail with a reason otherwise.
    fn check(
        &self,
        stmt: &SqlStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult;
}

/// Result of executing a query on a database.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// Query executed successfully with rows.
    Rows(Vec<Row>),
    /// Query executed successfully with no rows (e.g., INSERT, UPDATE, DELETE).
    Ok,
    /// Query failed with an error.
    Error(String),
}

impl QueryResult {
    pub fn is_error(&self) -> bool {
        matches!(self, QueryResult::Error(_))
    }
}

/// Differential oracle that compares Turso results with SQLite.
///
/// This oracle verifies that Turso produces the same results as SQLite
/// for all queries. It's the primary correctness check for the simulator.
pub struct DifferentialOracle;

impl Oracle for DifferentialOracle {
    fn check(
        &self,
        stmt: &SqlStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult {
        let has_unordered_limit = stmt.has_unordered_limit();

        match (turso_result, sqlite_result) {
            (QueryResult::Rows(turso_rows), QueryResult::Rows(sqlite_rows)) => {
                let diff = diff_results(turso_rows, sqlite_rows);
                if !diff.is_empty() {
                    // For LIMIT without ORDER BY, the result set may legitimately differ
                    // since the order is undefined. Return a warning instead of failure.
                    if has_unordered_limit {
                        return OracleResult::Warning(format!(
                            "Row set mismatch for unordered LIMIT query (results may vary due to undefined order):\n  SQL: {stmt}\n  Only in Turso: {:?}\n  Only in SQLite: {:?}",
                            diff.only_in_first, diff.only_in_second
                        ));
                    }
                    return OracleResult::Fail(format!(
                        "Row set mismatch:\n  SQL: {stmt}\n  Only in Turso: {:?}\n  Only in SQLite: {:?}",
                        diff.only_in_first, diff.only_in_second
                    ));
                }

                OracleResult::Pass
            }
            (QueryResult::Ok, QueryResult::Ok) => OracleResult::Pass,
            (QueryResult::Error(turso_err), QueryResult::Error(_sqlite_err)) => {
                // Both errored - this is acceptable (both rejected invalid SQL)
                tracing::debug!("Both databases errored on: {stmt}: {turso_err}");
                OracleResult::Pass
            }
            (QueryResult::Error(turso_err), _) => OracleResult::Fail(format!(
                "Turso errored but SQLite succeeded:\n  SQL: {stmt}\n  Error: {turso_err}"
            )),
            (_, QueryResult::Error(sqlite_err)) => OracleResult::Fail(format!(
                "SQLite errored but Turso succeeded:\n  SQL: {stmt}\n  Error: {sqlite_err}"
            )),
            (QueryResult::Rows(rows), QueryResult::Ok) => {
                if rows.is_empty() {
                    OracleResult::Pass
                } else {
                    OracleResult::Fail(format!(
                        "Turso returned {} rows but SQLite returned Ok:\n  SQL: {stmt}",
                        rows.len()
                    ))
                }
            }
            (QueryResult::Ok, QueryResult::Rows(rows)) => {
                if rows.is_empty() {
                    OracleResult::Pass
                } else {
                    OracleResult::Fail(format!(
                        "SQLite returned {} rows but Turso returned Ok:\n  SQL: {stmt}",
                        rows.len()
                    ))
                }
            }
        }
    }
}

impl DifferentialOracle {
    /// Execute a query on Turso and return the result.
    pub fn execute_turso(conn: &Arc<turso_core::Connection>, sql: &str) -> QueryResult {
        match conn.query(sql) {
            Ok(Some(mut stmt)) => {
                let mut rows = Vec::new();
                let result = stmt.run_with_row_callback(|row| {
                    let mut values = Vec::new();
                    for i in 0..row.len() {
                        let value = Self::convert_turso_value(row.get_value(i).clone());
                        values.push(value);
                    }
                    rows.push(Row(values));
                    Ok(())
                });

                match result {
                    Ok(()) => {
                        if rows.is_empty() {
                            QueryResult::Ok
                        } else {
                            QueryResult::Rows(rows)
                        }
                    }
                    Err(e) => QueryResult::Error(e.to_string()),
                }
            }
            Ok(None) => QueryResult::Ok,
            Err(e) => QueryResult::Error(e.to_string()),
        }
    }

    /// Execute a query on SQLite and return the result.
    pub fn execute_sqlite(conn: &rusqlite::Connection, sql: &str) -> QueryResult {
        // First try as a query that returns rows
        match conn.prepare(sql) {
            Ok(mut stmt) => {
                let column_count = stmt.column_count();
                if column_count == 0 {
                    // Statement doesn't return rows (INSERT, UPDATE, DELETE, etc.)
                    match stmt.execute([]) {
                        Ok(_) => QueryResult::Ok,
                        Err(e) => QueryResult::Error(e.to_string()),
                    }
                } else {
                    match stmt.query([]) {
                        Ok(mut query_rows) => {
                            let mut rows = Vec::new();
                            loop {
                                match query_rows.next() {
                                    Ok(Some(row)) => {
                                        let mut values = Vec::new();
                                        for i in 0..column_count {
                                            let value =
                                                Self::convert_sqlite_value(row.get_ref(i).ok());
                                            values.push(value);
                                        }
                                        rows.push(Row(values));
                                    }
                                    Ok(None) => break,
                                    Err(e) => return QueryResult::Error(e.to_string()),
                                }
                            }
                            if rows.is_empty() {
                                QueryResult::Ok
                            } else {
                                QueryResult::Rows(rows)
                            }
                        }
                        Err(e) => QueryResult::Error(e.to_string()),
                    }
                }
            }
            Err(e) => QueryResult::Error(e.to_string()),
        }
    }

    fn convert_turso_value(value: Value) -> SqlValue {
        match value {
            Value::Null => SqlValue::Null,
            Value::Integer(i) => SqlValue::Integer(i),
            Value::Float(f) => SqlValue::Real(f),
            Value::Text(s) => SqlValue::Text(s.as_str().to_string()),
            Value::Blob(b) => SqlValue::Blob(b),
        }
    }

    fn convert_sqlite_value(value: Option<rusqlite::types::ValueRef<'_>>) -> SqlValue {
        match value {
            None => SqlValue::Null,
            Some(rusqlite::types::ValueRef::Null) => SqlValue::Null,
            Some(rusqlite::types::ValueRef::Integer(i)) => SqlValue::Integer(i),
            Some(rusqlite::types::ValueRef::Real(f)) => SqlValue::Real(f),
            Some(rusqlite::types::ValueRef::Text(s)) => {
                SqlValue::Text(String::from_utf8_lossy(s).to_string())
            }
            Some(rusqlite::types::ValueRef::Blob(b)) => SqlValue::Blob(b.to_vec()),
        }
    }
}

/// Execute a statement on both databases and check the differential oracle.
pub fn check_differential(
    turso_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    stmt: &SqlStatement,
) -> Result<OracleResult> {
    let sql = stmt.to_string();
    let turso_result = DifferentialOracle::execute_turso(turso_conn, &sql);
    let sqlite_result = DifferentialOracle::execute_sqlite(sqlite_conn, &sql);

    let oracle = DifferentialOracle;
    Ok(oracle.check(stmt, &turso_result, &sqlite_result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_value_equality() {
        assert_eq!(SqlValue::Null, SqlValue::Null);
        assert_eq!(SqlValue::Integer(42), SqlValue::Integer(42));
        assert_ne!(SqlValue::Integer(42), SqlValue::Integer(43));
        assert_eq!(
            SqlValue::Text("hello".into()),
            SqlValue::Text("hello".into())
        );
        assert_eq!(SqlValue::Real(3.14), SqlValue::Real(3.14));
    }

    #[test]
    fn test_oracle_result() {
        assert!(OracleResult::Pass.is_pass());
        assert!(!OracleResult::Pass.is_fail());
        assert!(!OracleResult::Pass.is_warning());

        assert!(OracleResult::Warning("test".into()).is_warning());
        assert!(!OracleResult::Warning("test".into()).is_pass());
        assert!(!OracleResult::Warning("test".into()).is_fail());

        assert!(OracleResult::Fail("test".into()).is_fail());
        assert!(!OracleResult::Fail("test".into()).is_pass());
        assert!(!OracleResult::Fail("test".into()).is_warning());
    }
}
