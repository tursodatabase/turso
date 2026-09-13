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
use sql_gen::Schema;
use sql_gen_prop::SqlValue;
use sql_gen_prop::result::diff_results;
use turso_core::SubqueryUnnestingMode;
use turso_core::{Numeric, Value};

use crate::generate::GeneratedStatement;

/// Result of an oracle check.
#[derive(Debug, Clone)]
pub enum OracleResult {
    /// The oracle check passed.
    Pass,
    /// The oracle passed after comparing distinct forced and disabled plans.
    PassWithUnnestingInvariant,
    /// SQLite validated an independent join, and both Turso queries matched it.
    PassWithJoinedEquivalent { unnesting: bool },
    /// EXPLAIN failed in at least one engine, so neither engine ran the statement.
    Skipped(String),
    /// The oracle check passed but with a warning (e.g., LIMIT without ORDER BY).
    Warning(String),
    /// The oracle check failed with a reason.
    Fail(String),
}

impl OracleResult {
    pub fn is_pass(&self) -> bool {
        matches!(
            self,
            OracleResult::Pass
                | OracleResult::PassWithUnnestingInvariant
                | OracleResult::PassWithJoinedEquivalent { .. }
        )
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
        stmt: &GeneratedStatement,
        turso_result: &QueryResult,
        sqlite_result: &QueryResult,
    ) -> OracleResult;
}

/// Result of executing a query on a database.
#[derive(Debug, Clone, PartialEq, Eq)]
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
                let diff = diff_results(turso_rows, sqlite_rows);
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
                            diff.only_in_first.len(),
                            diff.only_in_second.len(),
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
                } else if has_unordered_limit {
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
                        "Turso returned {} rows but SQLite returned no rows:\n  SQL: {stmt}",
                        rows.len()
                    ))
                }
            }
            (QueryResult::Ok, QueryResult::Rows(rows)) => {
                if rows.is_empty() {
                    OracleResult::Pass
                } else if has_unordered_limit {
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
                        "SQLite returned {} rows but Turso returned no rows:\n  SQL: {stmt}",
                        rows.len()
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

            let res = if rows.is_empty() {
                QueryResult::Ok
            } else {
                QueryResult::Rows(rows)
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
            let res = if column_count == 0 {
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
                if rows.is_empty() {
                    QueryResult::Ok
                } else {
                    QueryResult::Rows(rows)
                }
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

    fn convert_turso_value(value: Value) -> SqlValue {
        match value {
            Value::Null => SqlValue::Null,
            Value::Numeric(Numeric::Integer(i)) => SqlValue::Integer(i),
            Value::Numeric(Numeric::Float(f)) => SqlValue::Real(f64::from(f)),
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
                    let diff = diff_results(&turso_rows, &sqlite_rows);
                    if !diff.is_empty() {
                        return OracleResult::Fail(format!(
                            "Post-DML table snapshot mismatch for {}:\n  SQL: {stmt}\n  Only in Turso: {:?}\n  Only in SQLite: {:?}",
                            table.qualified_name(),
                            diff.only_in_first,
                            diff.only_in_second
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
                    if !turso_rows.is_empty() {
                        return OracleResult::Fail(format!(
                            "Turso snapshot returned rows for {} but SQLite returned none:\n  SQL: {stmt}",
                            table.qualified_name()
                        ));
                    }
                }
                (QueryResult::Ok, QueryResult::Rows(sqlite_rows)) => {
                    if !sqlite_rows.is_empty() {
                        return OracleResult::Fail(format!(
                            "SQLite snapshot returned rows for {} but Turso returned none:\n  SQL: {stmt}",
                            table.qualified_name()
                        ));
                    }
                }
            }
        }

        OracleResult::Pass
    }
}

struct RestoreAutomaticUnnesting<'a>(&'a turso_core::Connection);

impl Drop for RestoreAutomaticUnnesting<'_> {
    fn drop(&mut self) {
        self.0
            .set_subquery_unnesting_mode(SubqueryUnnestingMode::Auto);
    }
}

fn format_explain_query_plan(result: &QueryResult) -> String {
    match result {
        QueryResult::Rows(rows) => {
            let Some(SqlValue::Text(json)) = rows.first().and_then(|row| row.0.first()) else {
                return format!("unexpected EXPLAIN JSON rows: {rows:?}");
            };
            let plan: serde_json::Value =
                serde_json::from_str(json).expect("EXPLAIN emits valid JSON");
            let nodes = plan["nodes"]
                .as_array()
                .expect("EXPLAIN JSON has physical nodes");
            let signature: Vec<_> = nodes
                .iter()
                .map(|node| {
                    let mut operator = node["op"].clone();
                    operator
                        .as_object_mut()
                        .expect("physical operator is an object")
                        .remove("estimate");
                    let parent = nodes
                        .iter()
                        .position(|candidate| candidate["id"] == node["parent"]);
                    serde_json::json!({ "parent": parent, "operator": operator })
                })
                .collect();
            serde_json::to_string_pretty(&signature).expect("plan signature is JSON")
        }
        QueryResult::Ok => "OK".to_string(),
        QueryResult::Error(error) => format!("ERROR: {error}"),
    }
}

fn check_subquery_unnesting_invariant(
    conn: &Arc<turso_core::Connection>,
    stmt: &GeneratedStatement,
) -> Option<OracleResult> {
    let _restore = RestoreAutomaticUnnesting(conn);
    let explain_sql = format!("EXPLAIN QUERY PLAN FORMAT=JSON {}", stmt.sql);
    conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Forced);
    let rewritten_plan = DifferentialOracle::execute_turso(conn, &explain_sql);
    conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Disabled);
    let correlated_plan = DifferentialOracle::execute_turso(conn, &explain_sql);
    let rewritten_plan_output = format_explain_query_plan(&rewritten_plan);
    let correlated_plan_output = format_explain_query_plan(&correlated_plan);
    if rewritten_plan_output == correlated_plan_output {
        return None;
    }
    tracing::debug!(
        target: "subquery_unnesting",
        "Checking distinct subquery plans:\n  SQL: {}\n  Forced EQP:\n    {}\n  Disabled EQP:\n    {}",
        stmt.sql,
        rewritten_plan_output,
        correlated_plan_output
    );

    conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Forced);
    let rewritten = DifferentialOracle::execute_turso(conn, &stmt.sql);
    conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Disabled);
    let correlated = DifferentialOracle::execute_turso(conn, &stmt.sql);

    Some(compare_unnesting_results(
        &stmt.sql,
        &rewritten,
        &correlated,
    ))
}

fn compare_unnesting_results(
    stmt: &str,
    rewritten: &QueryResult,
    correlated: &QueryResult,
) -> OracleResult {
    match (rewritten, correlated) {
        (QueryResult::Rows(rewritten), QueryResult::Rows(correlated)) => {
            let diff = diff_results(rewritten, correlated);
            if diff.is_empty() {
                OracleResult::Pass
            } else {
                OracleResult::Fail(format!(
                    "Subquery unnesting changed the result:\n  SQL: {stmt}\n  Only with forced unnesting: {:?}\n  Only with unnesting disabled: {:?}",
                    diff.only_in_first, diff.only_in_second
                ))
            }
        }
        (QueryResult::Ok, QueryResult::Ok) => OracleResult::Pass,
        (QueryResult::Error(rewritten), QueryResult::Error(correlated))
            if rewritten == correlated =>
        {
            OracleResult::Pass
        }
        (QueryResult::Rows(rows), QueryResult::Ok) | (QueryResult::Ok, QueryResult::Rows(rows))
            if rows.is_empty() =>
        {
            OracleResult::Pass
        }
        _ => OracleResult::Fail(format!(
            "Subquery unnesting changed the result or error:\n  SQL: {stmt}\n  Forced unnesting: {rewritten:?}\n  Unnesting disabled: {correlated:?}"
        )),
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
    if !direct_result.is_pass() {
        return direct_result;
    }

    let mut unnesting = false;
    if stmt.check_unnesting_invariant
        && !stmt.is_ddl
        && !stmt.mutates_data
        && !stmt.has_unordered_limit
    {
        if let Some(invariant_result) = check_subquery_unnesting_invariant(turso_conn, stmt) {
            if !invariant_result.is_pass() {
                return invariant_result;
            }
            unnesting = true;
        }
    }

    if let Some(joined) = &stmt.joined_equivalent {
        let sqlite_joined = DifferentialOracle::execute_sqlite(sqlite_conn, joined);
        let turso_joined = DifferentialOracle::execute_turso(turso_conn, joined);
        if sqlite_joined != sqlite_result {
            return OracleResult::Fail(format!(
                "Joined equivalent disagrees with the original in SQLite:\n  Original: {stmt}\n  Joined: {joined}\n  Original result: {sqlite_result:?}\n  Joined result: {sqlite_joined:?}"
            ));
        }
        if turso_result != sqlite_result || turso_joined != sqlite_result {
            return OracleResult::Fail(format!(
                "Joined equivalent changed ordered results:\n  Original: {stmt}\n  Joined: {joined}\n  SQLite: {sqlite_result:?}\n  Turso original: {turso_result:?}\n  Turso joined: {turso_joined:?}"
            ));
        }
        return OracleResult::PassWithJoinedEquivalent { unnesting };
    }
    if unnesting {
        return OracleResult::PassWithUnnestingInvariant;
    }

    if !stmt.mutates_data {
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
    fn test_sql_value_equality() {
        assert_eq!(SqlValue::Null, SqlValue::Null);
        assert_eq!(SqlValue::Integer(42), SqlValue::Integer(42));
        assert_ne!(SqlValue::Integer(42), SqlValue::Integer(43));
        assert_eq!(
            SqlValue::Text("hello".into()),
            SqlValue::Text("hello".into())
        );
        assert_eq!(
            SqlValue::Real(f64::consts::PI),
            SqlValue::Real(f64::consts::PI)
        );
    }

    #[test]
    fn test_oracle_result() {
        assert!(OracleResult::Pass.is_pass());
        assert!(!OracleResult::Pass.is_fail());
        assert!(!OracleResult::Pass.is_skipped());
        assert!(!OracleResult::Pass.is_warning());
        assert!(OracleResult::PassWithUnnestingInvariant.is_pass());

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
            check_unnesting_invariant: false,
            joined_equivalent: None,
        };
        let turso = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(1)])]);
        let sqlite = QueryResult::Rows(vec![Row(vec![SqlValue::Integer(2)])]);

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
            check_unnesting_invariant: false,
            joined_equivalent: None,
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
            check_unnesting_invariant: false,
            joined_equivalent: None,
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

    #[test]
    fn unnesting_comparison_rejects_different_errors() {
        let overflow = QueryResult::Error("integer overflow".to_owned());
        let json = QueryResult::Error("malformed JSON".to_owned());
        assert!(compare_unnesting_results("SELECT ...", &overflow, &overflow).is_pass());
        assert!(compare_unnesting_results("SELECT ...", &overflow, &json).is_fail());
    }

    #[test]
    fn every_supported_unnesting_form_returns_the_same_rows() {
        let io = Arc::new(MemorySimIO::new(789));
        let turso_db = Database::open_file_with_flags(
            io,
            "oracle-subquery-unnesting.db",
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = turso_db.connect().unwrap();
        for sql in [
            "CREATE TABLE outer_rows(id INTEGER, key1 INTEGER, amount INTEGER)",
            "CREATE TABLE inner_rows(key1 INTEGER, amount INTEGER)",
            "CREATE TABLE empty_rows(key1 INTEGER)",
            "INSERT INTO outer_rows VALUES (1, 1, 15), (2, 2, 5), (3, 3, NULL)",
            "INSERT INTO inner_rows VALUES (1, 7), (1, 8), (2, NULL), (3, 2)",
            "CREATE TABLE outer_types(k, tag TEXT)",
            "INSERT INTO outer_types VALUES (1, 'integer'), (1.0, 'real'), ('01', 'zero'), ('A', 'upper'), ('a', 'lower'), (NULL, 'null'), (X'41', 'blob')",
            "CREATE TABLE inner_types(k TEXT COLLATE NOCASE)",
            "INSERT INTO inner_types VALUES ('1'), ('A'), (NULL)",
        ] {
            assert!(matches!(
                DifferentialOracle::execute_turso(&conn, sql),
                QueryResult::Ok
            ));
        }
        let queries = [
            (
                "scalar aggregate",
                "SELECT o.id FROM outer_rows o
                 WHERE o.amount >= (
                     SELECT sum(i.amount) FROM inner_rows i WHERE i.key1 = o.key1
                 )",
            ),
            (
                "EXISTS",
                "SELECT o.id FROM outer_rows o
                 WHERE EXISTS (
                     SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1
                 )",
            ),
            (
                "NOT EXISTS",
                "SELECT o.id FROM outer_rows o
                 WHERE NOT EXISTS (
                     SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1
                 )",
            ),
            (
                "IN",
                "SELECT o.id FROM outer_rows o
                 WHERE o.amount IN (
                     SELECT i.amount FROM inner_rows i WHERE i.key1 = o.key1
                 )",
            ),
            (
                "EXISTS inequality",
                "SELECT o.id FROM outer_rows o
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 < o.key1)",
            ),
            (
                "NOT EXISTS disjunction",
                "SELECT o.id FROM outer_rows o
                 WHERE NOT EXISTS (SELECT 1 FROM inner_rows i
                     WHERE i.key1 < o.key1 OR i.amount IS o.amount)",
            ),
            (
                "EXISTS over an independent materialized CTE",
                "WITH shared AS MATERIALIZED (SELECT key1 FROM inner_rows)
                 SELECT o.id FROM outer_rows o
                 WHERE EXISTS (SELECT 1 FROM shared s WHERE s.key1 > o.key1)",
            ),
            (
                "NOT EXISTS over an independent materialized CTE",
                "WITH shared AS MATERIALIZED (SELECT key1 FROM inner_rows)
                 SELECT o.id FROM outer_rows o
                 WHERE NOT EXISTS (SELECT 1 FROM shared s WHERE s.key1 > o.key1)",
            ),
            (
                "EXISTS over two references to a rewritten shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ) SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT 1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                    WHERE a.key1 IS o.key1
                 ) ORDER BY o.id",
            ),
            (
                "rewritten shared producer read outside and inside EXISTS",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ) SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM shared c WHERE c.key1 > a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "NOT EXISTS over nested rewritten shared producers",
                "WITH first_shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ), second_shared AS MATERIALIZED (
                    SELECT a.key1 FROM first_shared a
                    WHERE NOT EXISTS (SELECT 1 FROM empty_rows e WHERE e.key1 IS a.key1)
                 ) SELECT o.id FROM outer_rows o WHERE NOT EXISTS (
                    SELECT 1 FROM second_shared a JOIN second_shared b ON a.key1 IS b.key1
                    WHERE a.key1 IS o.key1
                 ) ORDER BY o.id",
            ),
            (
                "EXISTS over a joined input",
                "SELECT o.id FROM outer_rows o
                 WHERE EXISTS (
                     SELECT 1 FROM inner_rows i JOIN inner_rows j ON i.key1 IS j.key1
                     WHERE i.key1 > o.key1 OR j.amount IS o.amount
                 )",
            ),
            (
                "NOT EXISTS over a joined input",
                "SELECT o.id FROM outer_rows o
                 WHERE NOT EXISTS (
                     SELECT 1 FROM inner_rows i JOIN inner_rows j ON i.key1 IS j.key1
                     WHERE i.key1 > o.key1 OR j.amount IS o.amount
                 )",
            ),
            (
                "joined input with text affinity and collation",
                "SELECT o.tag FROM outer_types o WHERE EXISTS (
                    SELECT 1 FROM inner_types i CROSS JOIN inner_rows j
                    WHERE i.k = o.k AND j.key1 = 1
                 ) ORDER BY o.tag",
            ),
            (
                "joined anti input with text affinity and collation",
                "SELECT o.tag FROM outer_types o WHERE NOT EXISTS (
                    SELECT 1 FROM inner_types i CROSS JOIN inner_rows j
                    WHERE i.k = o.k AND j.key1 = 1
                 ) ORDER BY o.tag",
            ),
            (
                "EXISTS inside a limited derived input",
                "SELECT d.id FROM (
                     SELECT o.id, o.key1 FROM outer_rows o
                     WHERE EXISTS (SELECT ?7 FROM inner_rows i WHERE i.key1 > o.key1)
                     ORDER BY o.id DESC LIMIT 1
                 ) d WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 > d.key1)",
            ),
            (
                "DISTINCT after a correlated filter",
                "SELECT DISTINCT i.key1 FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ORDER BY i.key1",
            ),
            (
                "DISTINCT before ordered LIMIT and OFFSET",
                "SELECT DISTINCT i.key1 + 1 AS next_key FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ORDER BY next_key DESC LIMIT 1 OFFSET 1",
            ),
            (
                "DISTINCT with storage classes in its key",
                "SELECT DISTINCT o.k, typeof(o.k) FROM outer_types o
                 WHERE EXISTS (SELECT 1 FROM inner_types i WHERE i.k IS NOT o.k)",
            ),
            (
                "DISTINCT with text collation and NULLs",
                "SELECT DISTINCT i.k FROM inner_types i
                 WHERE EXISTS (SELECT 1 FROM outer_types o WHERE o.k IS NOT i.k)
                 ORDER BY i.k",
            ),
            (
                "rewritten DISTINCT shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT DISTINCT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ) SELECT a.key1, b.key1 FROM shared a CROSS JOIN shared b
                 ORDER BY a.key1, b.key1",
            ),
            (
                "aggregate over a correlated filter",
                "SELECT count(*), sum(i.amount) FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)",
            ),
            (
                "bare columns of a minmax aggregate",
                "SELECT max(i.amount), i.amount FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)",
            ),
            (
                "a bare column inside an aggregate result expression",
                "SELECT max(i.amount) + i.amount FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)",
            ),
            (
                "HAVING reads the selected aggregate input row",
                "SELECT max(i.amount), i.amount FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 HAVING i.amount = max(i.amount)",
            ),
            (
                "grouping and HAVING after a correlated filter",
                "SELECT i.key1, count(*) AS n FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 GROUP BY i.key1 HAVING count(*) > 1 ORDER BY i.key1",
            ),
            (
                "aggregate over an empty correlated filter",
                "SELECT count(*), sum(i.amount), total(i.amount) FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1 AND j.key1 > 20)
                 HAVING count(*) = 0",
            ),
            (
                "ordered and limited aggregate groups",
                "SELECT i.key1, count(*) AS n FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 GROUP BY i.key1 ORDER BY -n, i.key1 LIMIT 1",
            ),
            (
                "DISTINCT aggregate arguments and FILTER",
                "SELECT count(DISTINCT i.key1), sum(i.amount) FILTER (WHERE i.amount > 7)
                 FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)",
            ),
            (
                "rewritten aggregate shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1, count(*) AS n FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    GROUP BY i.key1
                 ) SELECT a.key1, a.n, b.n FROM shared a JOIN shared b ON a.key1 IS b.key1
                 ORDER BY a.key1",
            ),
            (
                "DISTINCT after grouped aggregate output",
                "SELECT DISTINCT count(*) AS n FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 IS NOT i.key1)
                 GROUP BY i.key1 ORDER BY n DESC LIMIT 1 OFFSET 1",
            ),
            (
                "DISTINCT after an empty aggregate input",
                "SELECT DISTINCT count(*), sum(i.amount) FROM inner_rows i
                 WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1 AND j.key1 > 20)",
            ),
            (
                "rewritten DISTINCT aggregate shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT DISTINCT count(*) AS n FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 IS NOT i.key1)
                    GROUP BY i.key1
                 ) SELECT a.n, b.n FROM shared a JOIN shared b ON a.n = b.n ORDER BY a.n",
            ),
            (
                "rewritten filter inside a dependent aggregate",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT count(*) FROM inner_rows i WHERE i.key1 IS o.key1
                    AND EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ) ORDER BY o.id",
            ),
            (
                "dependent aggregate HAVING after a rewritten filter",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT count(*) FROM inner_rows i WHERE i.key1 IS o.key1
                    AND EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    HAVING count(*) = 0
                 ) ORDER BY o.id",
            ),
            (
                "dependent grouped aggregate with LIMIT and OFFSET",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT count(*) FROM inner_rows i WHERE i.key1 IS NOT o.key1
                    AND EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 IS NOT i.key1)
                    GROUP BY i.key1 LIMIT 1 OFFSET 1
                 ) ORDER BY o.id",
            ),
            (
                "rewritten union all shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    UNION ALL SELECT key1 FROM inner_rows WHERE amount > 7
                    ORDER BY 1 DESC LIMIT 4 OFFSET 1
                 ) SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 > a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "rewritten union shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    UNION SELECT key1 FROM inner_rows WHERE amount > 7
                    ORDER BY 1 DESC LIMIT 4 OFFSET 1
                 ) SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 > a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "rewritten intersect shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    INTERSECT SELECT key1 FROM inner_rows WHERE amount > 7
                    ORDER BY 1 DESC LIMIT 4 OFFSET 1
                 ) SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 > a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "rewritten except shared producer",
                "WITH shared AS MATERIALIZED (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    EXCEPT SELECT key1 FROM inner_rows WHERE amount > 7
                    ORDER BY 1 DESC LIMIT 4 OFFSET 1
                 ) SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 > a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "rewritten compound derived input with mixed operators",
                "SELECT q.key1 FROM (
                    SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                    UNION ALL SELECT key1 FROM inner_rows
                    EXCEPT SELECT key1 FROM inner_rows WHERE amount > 7
                 ) q WHERE EXISTS (SELECT 1 FROM outer_rows o WHERE o.key1 IS q.key1)
                 ORDER BY q.key1",
            ),
            (
                "VALUES shared producer with duplicate rows",
                "WITH shared(key1) AS MATERIALIZED (VALUES (1), (1), (2), (NULL))
                 SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.key1 > a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "VALUES derived right input under a semi filter",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT 1 FROM (VALUES (1), (1), (2), (NULL)) v WHERE v.column1 IS o.key1
                 ) ORDER BY o.id",
            ),
            (
                "VALUES derived right input under an anti filter",
                "SELECT o.id FROM outer_rows o WHERE NOT EXISTS (
                    SELECT 1 FROM (VALUES (1), (1), (2), (NULL)) v WHERE v.column1 IS o.key1
                 ) ORDER BY o.id",
            ),
            (
                "VALUES inside a rewritten compound producer",
                "WITH shared(key1) AS MATERIALIZED (
                    VALUES (1), (NULL)
                    UNION ALL SELECT i.key1 FROM inner_rows i
                    WHERE EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                 ) SELECT a.key1, b.key1 FROM shared a JOIN shared b ON a.key1 IS b.key1
                 WHERE EXISTS (SELECT 1 FROM outer_rows o WHERE o.key1 IS a.key1)
                 ORDER BY a.key1, b.key1",
            ),
            (
                "nested EXISTS inequality and disjunction",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT 1 FROM inner_rows i WHERE (i.key1 > o.key1 OR i.amount IS o.amount)
                    AND EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                ) ORDER BY o.id",
            ),
            (
                "nested anti input",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT 1 FROM inner_rows i WHERE (i.key1 > o.key1 OR i.amount IS o.amount)
                    AND NOT EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                ) ORDER BY o.id",
            ),
            (
                "nested EXISTS under an anti filter",
                "SELECT o.id FROM outer_rows o WHERE NOT EXISTS (
                    SELECT 1 FROM inner_rows i WHERE (i.key1 > o.key1 OR i.amount IS o.amount)
                    AND EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                ) ORDER BY o.id",
            ),
            (
                "nested anti input under an anti filter",
                "SELECT o.id FROM outer_rows o WHERE NOT EXISTS (
                    SELECT 1 FROM inner_rows i WHERE (i.key1 > o.key1 OR i.amount IS o.amount)
                    AND NOT EXISTS (SELECT 1 FROM inner_rows j WHERE j.key1 > i.key1)
                ) ORDER BY o.id",
            ),
            (
                "empty nested anti input",
                "SELECT o.id FROM outer_rows o WHERE EXISTS (
                    SELECT 1 FROM inner_rows i WHERE (i.key1 > o.key1 OR i.amount IS o.amount)
                    AND NOT EXISTS (SELECT 1 FROM empty_rows j WHERE j.key1 > i.key1)
                ) ORDER BY o.id",
            ),
            (
                "empty nested semi input under an anti filter",
                "SELECT o.id FROM outer_rows o WHERE NOT EXISTS (
                    SELECT 1 FROM inner_rows i WHERE (i.key1 > o.key1 OR i.amount IS o.amount)
                    AND EXISTS (SELECT 1 FROM empty_rows j WHERE j.key1 > i.key1)
                ) ORDER BY o.id",
            ),
        ];

        for (form, sql) in queries {
            let stmt = GeneratedStatement {
                sql: sql.to_string(),
                is_ddl: false,
                mutates_data: false,
                has_unordered_limit: false,
                unordered_limit_reason: None,
                check_unnesting_invariant: true,
                joined_equivalent: None,
            };
            let result = check_subquery_unnesting_invariant(&conn, &stmt);

            conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Forced);
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "{form}: {sql}"
            );
            let forced_plan = DifferentialOracle::execute_turso(
                &conn,
                &format!("EXPLAIN QUERY PLAN FORMAT=JSON {sql}"),
            );
            conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Disabled);
            assert!(
                !DifferentialOracle::execute_turso(&conn, sql).is_error(),
                "{form}: {sql}"
            );
            let correlated_plan = DifferentialOracle::execute_turso(
                &conn,
                &format!("EXPLAIN QUERY PLAN FORMAT=JSON {sql}"),
            );
            conn.set_subquery_unnesting_mode(SubqueryUnnestingMode::Auto);
            let forced_plan_output = format_explain_query_plan(&forced_plan);
            let correlated_plan_output = format_explain_query_plan(&correlated_plan);
            assert_ne!(
                forced_plan_output, correlated_plan_output,
                "the {form} test must compare distinct plan forms:\n\
                 forced:\n{forced_plan_output}\n\
                 disabled:\n{correlated_plan_output}"
            );
            assert!(
                result.as_ref().is_some_and(|result| result.is_pass()),
                "expected a passing {form} invariant, got {result:?}"
            );
        }

        let non_equality = GeneratedStatement {
            sql: queries[0].1.replace("i.key1 = o.key1", "i.key1 < o.key1"),
            is_ddl: false,
            mutates_data: false,
            has_unordered_limit: false,
            unordered_limit_reason: None,
            check_unnesting_invariant: true,
            joined_equivalent: None,
        };
        assert!(
            check_subquery_unnesting_invariant(&conn, &non_equality).is_none(),
            "an unsupported correlation must not count as a rewrite invariant"
        );
    }
}
