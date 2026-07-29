//! Transaction control statements.
//!
//! Includes BEGIN, COMMIT, ROLLBACK, SAVEPOINT, and RELEASE.

use proptest::prelude::*;
use std::fmt;

/// Transaction type for BEGIN statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    Deferred,
    Immediate,
    Exclusive,
}

impl fmt::Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionType::Deferred => write!(f, "DEFERRED"),
            TransactionType::Immediate => write!(f, "IMMEDIATE"),
            TransactionType::Exclusive => write!(f, "EXCLUSIVE"),
        }
    }
}

/// BEGIN statement.
#[derive(Debug, Clone)]
pub struct BeginStatement {
    /// Transaction type (DEFERRED, IMMEDIATE, EXCLUSIVE).
    pub transaction_type: Option<TransactionType>,
}

impl fmt::Display for BeginStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BEGIN")?;
        if let Some(tt) = &self.transaction_type {
            write!(f, " {tt}")?;
        }
        Ok(())
    }
}

/// COMMIT statement.
#[derive(Debug, Clone)]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

/// ROLLBACK statement.
#[derive(Debug, Clone)]
pub struct RollbackStatement {
    /// Optional savepoint name to rollback to.
    pub savepoint_name: Option<String>,
}

impl fmt::Display for RollbackStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ROLLBACK")?;
        if let Some(name) = &self.savepoint_name {
            write!(f, " TO SAVEPOINT {name}")?;
        }
        Ok(())
    }
}

/// SAVEPOINT statement.
#[derive(Debug, Clone)]
pub struct SavepointStatement {
    /// Savepoint name.
    pub name: String,
}

impl fmt::Display for SavepointStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SAVEPOINT {}", self.name)
    }
}

/// RELEASE statement.
#[derive(Debug, Clone)]
pub struct ReleaseStatement {
    /// Savepoint name to release.
    pub name: String,
}

impl fmt::Display for ReleaseStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RELEASE SAVEPOINT {}", self.name)
    }
}

/// Generate a transaction type.
pub fn transaction_type() -> impl Strategy<Value = TransactionType> {
    prop_oneof![
        Just(TransactionType::Deferred),
        Just(TransactionType::Immediate),
        Just(TransactionType::Exclusive),
    ]
}

/// Generate a BEGIN statement.
pub fn begin() -> impl Strategy<Value = BeginStatement> {
    proptest::option::of(transaction_type())
        .prop_map(|transaction_type| BeginStatement { transaction_type })
}

/// Generate a COMMIT statement.
pub fn commit() -> impl Strategy<Value = CommitStatement> {
    Just(CommitStatement)
}

/// Generate a ROLLBACK statement (without savepoint).
pub fn rollback() -> impl Strategy<Value = RollbackStatement> {
    Just(RollbackStatement {
        savepoint_name: None,
    })
}

/// Generate a SAVEPOINT statement.
pub fn savepoint() -> impl Strategy<Value = SavepointStatement> {
    crate::create_table::identifier().prop_map(|name| SavepointStatement { name })
}

/// Generate a RELEASE statement for a given savepoint name.
pub fn release(savepoint_name: String) -> impl Strategy<Value = ReleaseStatement> {
    Just(ReleaseStatement {
        name: savepoint_name,
    })
}

/// Generate a ROLLBACK TO SAVEPOINT statement.
pub fn rollback_to_savepoint(savepoint_name: String) -> impl Strategy<Value = RollbackStatement> {
    Just(RollbackStatement {
        savepoint_name: Some(savepoint_name),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prop_assert, prop_assert_eq};

    proptest::proptest! {
        #[test]
        fn begin_generates_valid_sql(stmt in begin()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("BEGIN"));
        }

        #[test]
        fn commit_generates_valid_sql(stmt in commit()) {
            let sql = stmt.to_string();
            prop_assert_eq!(sql, "COMMIT");
        }

        #[test]
        fn rollback_generates_valid_sql(stmt in rollback()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("ROLLBACK"));
        }

        #[test]
        fn savepoint_generates_valid_sql(stmt in savepoint()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("SAVEPOINT"));
        }
    }
}
