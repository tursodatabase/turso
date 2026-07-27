//! Oracle implementations for validating database behavior.
//!
//! Oracles are predicates that verify properties of database execution.
//! The primary oracle is the DifferentialOracle which compares Turso
//! results against SQLite.

use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::Result;
use query_result_oracle::diff_result_sets;
pub use query_result_oracle::{ResultSet, Row, Value as QueryValue};
use sql_gen::Schema;
use turso_core::{Numeric, Value};

use crate::generate::GeneratedStatement;

/// Result of an oracle check.
#[derive(Debug, Clone)]
pub enum OracleResult {
    /// The oracle check passed.
    Pass,
    /// EXPLAIN failed in at least one engine, so neither engine ran the statement.
    Skipped(String),
    /// The oracle check passed but with a warning (e.g., LIMIT without ORDER BY).
    Warning(String),
    /// The oracle check failed with a reason.
    Fail(String),
}

impl OracleResult {
    pub fn is_pass(&self) -> bool {
        matches!(self, OracleResult::Pass)
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, OracleResult::Skipped(_))
    }

    pub fn is_warning(&self) -> bool {
        matches!(self, OracleResult::Warning(_))
    }

    pub fn is_fail(&self) -> bool {
        matches!(self, OracleResult::Fail(_))
    }
}

/// Trait for oracles that can check database properties.
pub trait Oracle {
    /// Check the oracle after executing a statement.
    ///
    /// Returns Pass if the property holds, Warning for non-fatal issues,
    /// or Fail with a reason otherwise.
    fn check(
        &self,
        stmt: &GeneratedStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult;
}

/// Result of executing a query on a database.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// A statement with a result schema, including an empty SELECT.
    Rows(ResultSet),
    /// A statement with no result columns (e.g., INSERT, UPDATE, DELETE).
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
/// for all queries. It's the primary correctness check for the fuzzer.
pub struct DifferentialOracle;

impl Oracle for DifferentialOracle {
    fn check(
        &self,
        stmt: &GeneratedStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult {
        let has_unordered_limit = stmt.has_unordered_limit;

        match (turso_result, sqlite_result) {
            (QueryResult::Rows(turso_rows), QueryResult::Rows(sqlite_rows)) => {
                let diff = diff_result_sets(turso_rows, sqlite_rows);
                if !diff.is_empty() {
                    // For non-deterministic LIMIT queries, the result set may legitimately differ
                    // since the chosen rows are not stable across engines. Return a warning instead
                    // of failure.
                    if has_unordered_limit {
                        return OracleResult::Warning(format_nondet_limit_warning(
                            stmt,
                            "row_set_mismatch",
                            turso_rows.len(),
                            sqlite_rows.len(),
                            diff.total_only_in_left as usize,
                            diff.total_only_in_right as usize,
                        ));
                    }
                    return OracleResult::Fail(format!(
                        "Row set mismatch:\n  SQL: {stmt}\n{}",
                        diff.describe("Turso", "SQLite", 20)
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
                if has_unordered_limit {
                    OracleResult::Warning(format_nondet_limit_warning(
                        stmt,
                        "rows_vs_ok",
                        rows.len(),
                        0,
                        rows.len(),
                        0,
                    ))
                } else {
                    OracleResult::Fail(format!(
                        "Turso returned a {}-column result with {} rows but SQLite returned no result columns:\n  SQL: {stmt}",
                        rows.column_count(),
                        rows.len(),
                    ))
                }
            }
            (QueryResult::Ok, QueryResult::Rows(rows)) => {
                if has_unordered_limit {
                    OracleResult::Warning(format_nondet_limit_warning(
                        stmt,
                        "ok_vs_rows",
                        0,
                        rows.len(),
                        0,
                        rows.len(),
                    ))
                } else {
                    OracleResult::Fail(format!(
                        "SQLite returned a {}-column result with {} rows but Turso returned no result columns:\n  SQL: {stmt}",
                        rows.column_count(),
                        rows.len(),
                    ))
                }
            }
        }
    }
}

fn sql_hash(sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn short_sql(sql: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (i, ch) in sql.chars().enumerate() {
        if i >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

fn format_skipped_statement(
    stmt: &GeneratedStatement,
    turso_error: Option<&str>,
    sqlite_error: Option<&str>,
) -> String {
    let error_prefix = |error: &str| {
        let error = error
            .find(&stmt.sql)
            .map(|sql_start| error[..sql_start].trim_end_matches(" in ").trim())
            .unwrap_or(error);
        short_sql(error, 240)
    };
    let turso_error = turso_error.map(error_prefix);
    let sqlite_error = sqlite_error.map(error_prefix);
    format!(
        "Statement skipped because EXPLAIN failed: sql_hash={:016x} Turso error={turso_error:?} SQLite error={sqlite_error:?}\n  SQL: {}",
        sql_hash(&stmt.sql),
        short_sql(&stmt.sql, 240),
    )
}

fn format_nondet_limit_warning(
    stmt: &GeneratedStatement,
    kind: &str,
    turso_rows: usize,
    sqlite_rows: usize,
    only_in_turso: usize,
    only_in_sqlite: usize,
) -> String {
    let reason = stmt
        .unordered_limit_reason
        .as_deref()
        .unwrap_or("unordered_limit");
    format!(
        "NONDET_LIMIT_WARNING reason={reason} kind={kind} sql_hash={:016x} turso_rows={turso_rows} sqlite_rows={sqlite_rows} only_in_turso={only_in_turso} only_in_sqlite={only_in_sqlite}\n  SQL(prefix): {}",
        sql_hash(&stmt.sql),
        short_sql(&stmt.sql, 240),
    )
}

impl DifferentialOracle {
    /// Execute a query on Turso and return the result.
    pub fn execute_turso(conn: &Arc<turso_core::Connection>, sql: &str) -> QueryResult {
        let execute = || {
            let mut stmt = conn.prepare(sql)?;
            let column_count = stmt.num_columns();

            let mut rows = Vec::new();
            stmt.run_with_row_callback(|row| {
                let mut values = Vec::new();
                for i in 0..row.len() {
                    let value = Self::convert_turso_value(row.get_value(i).clone());
                    values.push(value);
                }
                rows.push(Row(values));
                Ok(())
            })?;

            let res =
                if column_count == 0 {
                    QueryResult::Ok
                } else {
                    QueryResult::Rows(ResultSet::new(column_count, rows).map_err(|error| {
                        turso_core::LimboError::InternalError(error.to_string())
                    })?)
                };
            Ok(res)
        };
        let result: Result<QueryResult, turso_core::LimboError> = execute();
        match result {
            Ok(res) => res,
            Err(e) => QueryResult::Error(e.to_string()),
        }
    }

    /// Execute a query on SQLite and return the result.
    pub fn execute_sqlite(conn: &rusqlite::Connection, sql: &str) -> QueryResult {
        // First try as a query that returns rows
        let execute = || {
            let mut stmt = conn.prepare(sql)?;
            let column_count = stmt.column_count();
            let res =
                if column_count == 0 {
                    // Statement doesn't return rows (INSERT, UPDATE, DELETE, etc.)
                    stmt.execute([])?;
                    QueryResult::Ok
                } else {
                    let mut query_rows = stmt.query([])?;
                    let mut rows = Vec::new();
                    while let Some(row) = query_rows.next()? {
                        let mut values = Vec::new();
                        for i in 0..column_count {
                            let value = Self::convert_sqlite_value(row.get_ref(i).ok());
                            values.push(value);
                        }
                        rows.push(Row(values));
                    }
                    QueryResult::Rows(ResultSet::new(column_count, rows).map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?)
                };
            stmt.finalize()?;
            Ok(res)
        };
        let result: Result<QueryResult, rusqlite::Error> = execute();
        match result {
            Ok(res) => res,
            Err(e) => QueryResult::Error(e.to_string()),
        }
    }

    fn convert_turso_value(value: Value) -> QueryValue {
        match value {
            Value::Null => QueryValue::Null,
            Value::Numeric(Numeric::Integer(i)) => QueryValue::Integer(i),
            Value::Numeric(Numeric::Float(f)) => QueryValue::Real(f64::from(f)),
            Value::Text(s) => QueryValue::Text(s.as_str().to_string()),
            Value::Blob(b) => QueryValue::Blob(b),
        }
    }

    fn convert_sqlite_value(value: Option<rusqlite::types::ValueRef<'_>>) -> QueryValue {
        match value {
            None => QueryValue::Null,
            Some(rusqlite::types::ValueRef::Null) => QueryValue::Null,
            Some(rusqlite::types::ValueRef::Integer(i)) => QueryValue::Integer(i),
            Some(rusqlite::types::ValueRef::Real(f)) => QueryValue::Real(f),
            Some(rusqlite::types::ValueRef::Text(s)) => {
                QueryValue::Text(String::from_utf8_lossy(s).to_string())
            }
            Some(rusqlite::types::ValueRef::Blob(b)) => QueryValue::Blob(b.to_vec()),
        }
    }

    fn snapshot_query(table: &sql_gen::Table) -> String {
        format!(
            "SELECT rowid, * FROM {} ORDER BY rowid",
            table.qualified_name()
        )
    }

    fn verify_table_snapshots(
        turso_conn: &Arc<turso_core::Connection>,
        sqlite_conn: &rusqlite::Connection,
        schema: &Schema,
        stmt: &GeneratedStatement,
    ) -> OracleResult {
        for table in &schema.tables {
            let snapshot_sql = Self::snapshot_query(table);
            let turso_rows = Self::execute_turso(turso_conn, &snapshot_sql);
            let sqlite_rows = Self::execute_sqlite(sqlite_conn, &snapshot_sql);
            match (turso_rows, sqlite_rows) {
                (QueryResult::Rows(turso_rows), QueryResult::Rows(sqlite_rows)) => {
                    let diff = diff_result_sets(&turso_rows, &sqlite_rows);
                    if !diff.is_empty() {
                        return OracleResult::Fail(format!(
                            "Post-DML table snapshot mismatch for {}:\n  SQL: {stmt}\n{}",
                            table.qualified_name(),
                            diff.describe("Turso", "SQLite", 20),
                        ));
                    }
                }
                (QueryResult::Ok, QueryResult::Ok) => {}
                (QueryResult::Error(turso_err), QueryResult::Error(sqlite_err)) => {
                    return OracleResult::Fail(format!(
                        "Post-DML snapshot failed on both engines for {}:\n  SQL: {stmt}\n  Turso: {turso_err}\n  SQLite: {sqlite_err}",
                        table.qualified_name()
                    ));
                }
                (QueryResult::Error(turso_err), _) => {
                    return OracleResult::Fail(format!(
                        "Turso snapshot failed for {} after DML:\n  SQL: {stmt}\n  Error: {turso_err}",
                        table.qualified_name()
                    ));
                }
                (_, QueryResult::Error(sqlite_err)) => {
                    return OracleResult::Fail(format!(
                        "SQLite snapshot failed for {} after DML:\n  SQL: {stmt}\n  Error: {sqlite_err}",
                        table.qualified_name()
                    ));
                }
                (QueryResult::Rows(turso_rows), QueryResult::Ok) => {
                    return OracleResult::Fail(format!(
                        "Turso snapshot returned a {}-column result for {} but SQLite returned no result columns:\n  SQL: {stmt}",
                        turso_rows.column_count(),
                        table.qualified_name()
                    ));
                }
                (QueryResult::Ok, QueryResult::Rows(sqlite_rows)) => {
                    return OracleResult::Fail(format!(
                        "SQLite snapshot returned a {}-column result for {} but Turso returned no result columns:\n  SQL: {stmt}",
                        sqlite_rows.column_count(),
                        table.qualified_name()
                    ));
                }
            }
        }

        OracleResult::Pass
    }
}

/// Execute a statement on both databases and check the differential oracle.
pub fn check_differential(
    turso_conn: &Arc<turso_core::Connection>,
    sqlite_conn: &rusqlite::Connection,
    schema: &Schema,
    stmt: &GeneratedStatement,
) -> OracleResult {
    // Generated SQL can contain an error in a branch that never runs. SQLite
    // may remove that branch before checking it, while Turso may reject it.
    // If the accepted statement writes data, running it in only one database
    // would spoil every comparison that follows. EXPLAIN asks each engine to
    // prepare the statement without changing data. Run it only if both agree
    // that it can run.
    let explain_sql = format!("EXPLAIN {}", stmt.sql);
    let turso_explain = DifferentialOracle::execute_turso(turso_conn, &explain_sql);
    let sqlite_explain = DifferentialOracle::execute_sqlite(sqlite_conn, &explain_sql);
    match (&turso_explain, &sqlite_explain) {
        (QueryResult::Error(turso_error), QueryResult::Error(sqlite_error)) => {
            return OracleResult::Skipped(format_skipped_statement(
                stmt,
                Some(turso_error),
                Some(sqlite_error),
            ));
        }
        (QueryResult::Error(turso_error), _) => {
            return OracleResult::Skipped(format_skipped_statement(stmt, Some(turso_error), None));
        }
        (_, QueryResult::Error(sqlite_error)) => {
            return OracleResult::Skipped(format_skipped_statement(stmt, None, Some(sqlite_error)));
        }
        _ => {}
    }

    let turso_result = DifferentialOracle::execute_turso(turso_conn, &stmt.sql);
    let sqlite_result = DifferentialOracle::execute_sqlite(sqlite_conn, &stmt.sql);

    let oracle = DifferentialOracle;
    let direct_result = oracle.check(stmt, &turso_result, &sqlite_result);
    if !stmt.mutates_data || !direct_result.is_pass() {
        return direct_result;
    }

    DifferentialOracle::verify_table_snapshots(turso_conn, sqlite_conn, schema, stmt)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use turso_core::SqliteDialect;

    use core::f64;

    use super::*;
    use crate::memory::MemorySimIO;
    use sql_gen::{ColumnDef, DataType, SchemaBuilder, Table};
    use turso_core::Database;

    #[test]
    fn test_query_value_equality() {
        assert_eq!(QueryValue::Null, QueryValue::Null);
        assert_eq!(QueryValue::Integer(42), QueryValue::Integer(42));
        assert_ne!(QueryValue::Integer(42), QueryValue::Integer(43));
        assert_eq!(
            QueryValue::Text("hello".into()),
            QueryValue::Text("hello".into())
        );
        assert_eq!(
            QueryValue::Real(f64::consts::PI),
            QueryValue::Real(f64::consts::PI)
        );
    }

    #[test]
    fn test_oracle_result() {
        assert!(OracleResult::Pass.is_pass());
        assert!(!OracleResult::Pass.is_fail());
        assert!(!OracleResult::Pass.is_skipped());
        assert!(!OracleResult::Pass.is_warning());

        assert!(OracleResult::Skipped("test".into()).is_skipped());
        assert!(!OracleResult::Skipped("test".into()).is_pass());
        assert!(!OracleResult::Skipped("test".into()).is_fail());
        assert!(!OracleResult::Skipped("test".into()).is_warning());

        assert!(OracleResult::Warning("test".into()).is_warning());
        assert!(!OracleResult::Warning("test".into()).is_pass());
        assert!(!OracleResult::Warning("test".into()).is_fail());

        assert!(OracleResult::Fail("test".into()).is_fail());
        assert!(!OracleResult::Fail("test".into()).is_pass());
        assert!(!OracleResult::Fail("test".into()).is_warning());
    }

    #[test]
    fn test_nondet_warning_is_structured_and_reasoned() {
        let stmt = GeneratedStatement {
            sql: "SELECT 1 LIMIT 1".to_string(),
            is_ddl: false,
            mutates_data: false,
            has_unordered_limit: true,
            unordered_limit_reason: Some("limit_order_by_scalar_subquery".to_string()),
        };
        let turso =
            QueryResult::Rows(ResultSet::new(1, vec![Row(vec![QueryValue::Integer(1)])]).unwrap());
        let sqlite =
            QueryResult::Rows(ResultSet::new(1, vec![Row(vec![QueryValue::Integer(2)])]).unwrap());

        let oracle = DifferentialOracle;
        let res = oracle.check(&stmt, &turso, &sqlite);
        match res {
            OracleResult::Warning(msg) => {
                assert!(msg.contains("NONDET_LIMIT_WARNING"));
                assert!(msg.contains("reason=limit_order_by_scalar_subquery"));
                assert!(msg.contains("kind=row_set_mismatch"));
                assert!(msg.contains("sql_hash="));
                assert!(msg.contains("SQL(prefix): SELECT 1 LIMIT 1"));
            }
            other => panic!("expected warning, got {other:?}"),
        }
    }

    #[test]
    fn test_check_differential_fails_on_hidden_table_state_mismatch() {
        let io = Arc::new(MemorySimIO::new(123));
        let turso_db = Database::open_file_with_flags(
            io,
            "oracle-state-mismatch.db",
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let turso_conn = turso_db.connect().unwrap();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let schema = SchemaBuilder::new()
            .table(Table::new(
                "t",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("v", DataType::Integer),
                ],
            ))
            .build();

        for sql in [
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            "INSERT INTO t VALUES (1, 10)",
        ] {
            assert!(matches!(
                DifferentialOracle::execute_turso(&turso_conn, sql),
                QueryResult::Ok
            ));
            assert!(matches!(
                DifferentialOracle::execute_sqlite(&sqlite_conn, sql),
                QueryResult::Ok
            ));
        }

        assert!(matches!(
            DifferentialOracle::execute_turso(&turso_conn, "UPDATE t SET v = 11 WHERE id = 1"),
            QueryResult::Ok
        ));

        let stmt = GeneratedStatement {
            sql: "UPDATE t SET v = v WHERE id = 999".to_string(),
            is_ddl: false,
            mutates_data: true,
            has_unordered_limit: false,
            unordered_limit_reason: None,
        };

        let result = check_differential(&turso_conn, &sqlite_conn, &schema, &stmt);
        assert!(
            result.is_fail(),
            "post-DML state verification should catch hidden row mismatches"
        );
    }

    #[test]
    fn statement_rejected_by_one_engine_is_skipped() {
        let io = Arc::new(MemorySimIO::new(456));
        let turso_db = Database::open_file_with_flags(
            io,
            "oracle-validation-skip.db",
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let turso_conn = turso_db.connect().unwrap();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "t",
                vec![ColumnDef::new("a", DataType::Integer)],
            ))
            .build();

        for sql in ["CREATE TABLE t(a)", "INSERT INTO t VALUES (1)"] {
            assert!(matches!(
                DifferentialOracle::execute_turso(&turso_conn, sql),
                QueryResult::Ok
            ));
            assert!(matches!(
                DifferentialOracle::execute_sqlite(&sqlite_conn, sql),
                QueryResult::Ok
            ));
        }

        let stmt = GeneratedStatement {
            sql: "WITH cte(x) AS (SELECT 1, 2) \
                  UPDATE t SET a = 2 WHERE 0 AND EXISTS (SELECT * FROM cte)"
                .to_string(),
            is_ddl: false,
            mutates_data: true,
            has_unordered_limit: false,
            unordered_limit_reason: None,
        };

        let result = check_differential(&turso_conn, &sqlite_conn, &schema, &stmt);
        match result {
            OracleResult::Skipped(reason) => {
                assert!(reason.contains("Statement skipped because EXPLAIN failed"));
                assert!(reason.contains("Turso error=Some"));
                assert!(reason.contains("SQLite error=None"));
            }
            other => panic!("expected skipped statement, got {other:?}"),
        }
    }
}
