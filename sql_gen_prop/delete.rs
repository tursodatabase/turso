//! DELETE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{Condition, ConditionProfile, optional_where_clause};
use crate::schema::{Schema, TableRef};

// =============================================================================
// DELETE STATEMENT PROFILE
// =============================================================================

/// Profile for controlling DELETE statement generation.
#[derive(Debug, Clone, Default)]
pub struct DeleteProfile {
    /// Condition profile for WHERE clause.
    pub condition_profile: ConditionProfile,
}

impl DeleteProfile {
    /// Create a profile for simple DELETE queries.
    pub fn simple() -> Self {
        Self {
            condition_profile: ConditionProfile::simple(),
        }
    }

    /// Builder method to set condition profile.
    pub fn with_condition_profile(mut self, profile: ConditionProfile) -> Self {
        self.condition_profile = profile;
        self
    }
}

/// A DELETE statement.
#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Condition>,
}

impl fmt::Display for DeleteStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DELETE FROM \"{}\"", self.table)?;

        if let Some(cond) = &self.where_clause {
            write!(f, " WHERE {cond}")?;
        }

        Ok(())
    }
}

/// Generate a DELETE statement for a table with profile.
pub fn delete_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &DeleteProfile,
) -> BoxedStrategy<DeleteStatement> {
    let table_name = table.name.clone();
    let condition_profile = &profile.condition_profile;

    optional_where_clause(table, schema, condition_profile)
        .prop_map(move |where_clause| DeleteStatement {
            table: table_name.clone(),
            where_clause,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::condition::ComparisonOp;
    use crate::value::SqlValue;

    #[test]
    fn test_delete_display() {
        let stmt = DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(Condition::SimpleComparison {
                column: "id".to_string(),
                op: ComparisonOp::Eq,
                value: SqlValue::Integer(1),
            }),
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = 1");
    }
}
