//! DELETE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{Condition, optional_where_clause};
use crate::schema::Table;

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

/// Generate a DELETE statement for a table.
pub fn delete_for_table(table: &Table) -> BoxedStrategy<DeleteStatement> {
    let table_name = table.name.clone();

    optional_where_clause(table)
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
