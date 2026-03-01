use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::SimValue;

use super::select::Select;

/// Conflict resolution action for INSERT statements.
/// Maps to SQLite's INSERT OR {action} syntax.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConflictAction {
    /// INSERT OR REPLACE - replaces the existing row on conflict
    Replace,
    /// INSERT OR IGNORE - silently ignores the insert on conflict
    Ignore,
    /// INSERT OR ABORT - aborts the current statement (default behavior)
    Abort,
    /// INSERT OR ROLLBACK - rolls back the entire transaction on conflict
    Rollback,
    /// INSERT OR FAIL - fails but keeps prior changes in the transaction
    Fail,
}

impl Display for ConflictAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictAction::Replace => write!(f, "REPLACE"),
            ConflictAction::Ignore => write!(f, "IGNORE"),
            ConflictAction::Abort => write!(f, "ABORT"),
            ConflictAction::Rollback => write!(f, "ROLLBACK"),
            ConflictAction::Fail => write!(f, "FAIL"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OnConflict {
    pub target_column: String,
    pub assignments: Vec<UpdateSetItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateSetItem {
    pub column: String,
    pub excluded_column: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
        #[serde(default)]
        on_conflict: Option<OnConflict>,
        /// Optional conflict resolution (INSERT OR REPLACE, etc.)
        conflict: Option<ConflictAction>,
    },
    Select {
        table: String,
        select: Box<Select>,
        /// Optional conflict resolution (INSERT OR REPLACE, etc.)
        conflict: Option<ConflictAction>,
    },
}

impl Insert {
    pub fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
        }
    }

    pub fn rows(&self) -> &[Vec<SimValue>] {
        match self {
            Insert::Values { values, .. } => values,
            Insert::Select { .. } => unreachable!(),
        }
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert::Values {
                table,
                values,
                on_conflict,
                conflict,
            } => {
                write!(f, "INSERT ")?;
                if let Some(action) = conflict {
                    write!(f, "OR {action} ")?;
                }
                write!(f, "INTO {table} VALUES ")?;
                for (i, row) in values.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, value) in row.iter().enumerate() {
                        if j != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{value}")?;
                    }
                    write!(f, ")")?;
                }
                if let Some(on_conflict) = &on_conflict {
                    write!(f, " {on_conflict}")?;
                }
                Ok(())
            }
            Insert::Select {
                table,
                select,
                conflict,
            } => {
                write!(f, "INSERT ")?;
                if let Some(action) = conflict {
                    write!(f, "OR {action} ")?;
                }
                write!(f, "INTO {table} ")?;
                write!(f, "{select}")
            }
        }
    }
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ON CONFLICT({}) DO UPDATE SET ", self.target_column)?;
        for (i, a) in self.assignments.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{a}")?;
        }
        Ok(())
    }
}

impl Display for UpdateSetItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = excluded.{}", self.column, self.excluded_column)
    }
}
