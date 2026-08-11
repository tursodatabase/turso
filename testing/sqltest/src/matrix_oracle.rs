//! In-process SQLite oracle for matrix cases.
//!
//! Matrix expansions have no stored expectations: each case runs its setup
//! and statement against the bundled SQLite (rusqlite) and the invariant is
//! simply that Turso and SQLite agree — same rows in the same order, or
//! both rejecting the statement (error messages are not compared).
//!
//! The bundled SQLite version is pinned by Cargo.lock, so expected behavior
//! only moves when the dependency is deliberately upgraded.
//!
//! Values are rendered through the same formatting the Rust backend applies
//! to Turso values (notably `format_real`'s `%!.15g`), so comparing the
//! resulting strings compares typed values, not incidental text formatting.

use crate::backends::rust::format_real;
use rusqlite::types::ValueRef;

/// Outcome of running one statement against the oracle.
#[derive(Debug, Clone, PartialEq)]
pub enum OracleOutcome {
    /// Rows, each pipe-joined with the shared value formatting.
    Rows(Vec<Vec<String>>),
    /// SQLite rejected the statement (prepare- or step-time).
    Error(String),
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
