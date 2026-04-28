/// Per-connection stream state tracked by the proxy.
#[derive(Debug, Default, Clone)]
pub struct StreamState {
    /// Baton from the last server response.
    pub last_baton: Option<String>,
    /// Whether the connection is inside a transaction.
    pub in_transaction: bool,
    /// Whether the last pipeline included a close request.
    pub last_had_close: bool,
    /// Whether the last pipeline got an HTTP-level error (non-200).
    pub last_had_http_error: bool,
}

/// Check whether a SQL statement begins a transaction.
pub fn sql_begins_transaction(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("BEGIN") || upper.starts_with("SAVEPOINT")
}

/// Check whether a SQL statement ends a transaction.
pub fn sql_ends_transaction(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK")
        || upper.starts_with("END")
        || (upper.starts_with("RELEASE") && !upper.contains("SAVEPOINT"))
}

/// Detect transaction boundaries from a list of SQL statements.
/// Returns (any_begins, any_ends).
pub fn detect_transaction_boundaries(sqls: &[&str]) -> (bool, bool) {
    let mut any_begins = false;
    let mut any_ends = false;
    for sql in sqls {
        if sql_begins_transaction(sql) {
            any_begins = true;
        }
        if sql_ends_transaction(sql) {
            any_ends = true;
        }
    }
    (any_begins, any_ends)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begins_transaction() {
        assert!(sql_begins_transaction("BEGIN"));
        assert!(sql_begins_transaction("BEGIN TRANSACTION"));
        assert!(sql_begins_transaction("  begin immediate"));
        assert!(sql_begins_transaction("SAVEPOINT sp1"));
        assert!(!sql_begins_transaction("SELECT 1"));
        assert!(!sql_begins_transaction("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn test_ends_transaction() {
        assert!(sql_ends_transaction("COMMIT"));
        assert!(sql_ends_transaction("ROLLBACK"));
        assert!(sql_ends_transaction("END"));
        assert!(sql_ends_transaction("  end transaction"));
        assert!(!sql_ends_transaction("SELECT 1"));
        assert!(!sql_ends_transaction("BEGIN"));
    }
}
