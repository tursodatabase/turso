//! DELETE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::expression::Expression;
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};
use crate::select::optional_where_clause;

// =============================================================================
// DELETE STATEMENT PROFILE
// =============================================================================

/// Profile for controlling DELETE statement generation.
///
/// DELETE statements use the condition settings from the global
/// `StatementProfile.generation.expression.base` for WHERE clause generation.
#[derive(Debug, Clone, Default)]
pub struct DeleteProfile;

impl DeleteProfile {
    /// Create a profile for simple DELETE queries.
    pub fn simple() -> Self {
        Self
    }
}

/// A DELETE statement.
#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
}

impl fmt::Display for DeleteStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DELETE FROM {}", self.table)?;

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
    profile: &StatementProfile,
) -> BoxedStrategy<DeleteStatement> {
    let table_name = table.qualified_name();

    optional_where_clause(table, schema, profile)
        .prop_map(move |where_clause| DeleteStatement {
            table: table_name.clone(),
            where_clause,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::BinaryOperator;
    use crate::value::SqlValue;

    #[test]
    fn test_delete_display() {
        let stmt = DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(Expression::binary(
                Expression::Column("id".to_string()),
                BinaryOperator::Eq,
                Expression::Value(SqlValue::Integer(1)),
            )),
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "DELETE FROM users WHERE id = 1");
    }
}
