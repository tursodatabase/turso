//! DROP TABLE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::create_table::identifier;
use crate::schema::{Schema, TableRef};

/// A DROP TABLE statement.
#[derive(Debug, Clone)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TABLE ")?;

        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }

        write!(f, "\"{}\"", self.table_name)
    }
}

/// Generate a DROP TABLE statement for a specific table.
pub fn drop_table_for_table(table: &TableRef) -> BoxedStrategy<DropTableStatement> {
    let table_name = table.name.clone();

    any::<bool>()
        .prop_map(move |if_exists| DropTableStatement {
            table_name: table_name.clone(),
            if_exists,
        })
        .boxed()
}

/// Generate a DROP TABLE statement for any table in a schema.
pub fn drop_table_for_schema(schema: &Schema) -> BoxedStrategy<DropTableStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let table_names: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();

    (proptest::sample::select(table_names), any::<bool>())
        .prop_map(|(table_name, if_exists)| DropTableStatement {
            table_name,
            if_exists,
        })
        .boxed()
}

/// Generate a DROP TABLE statement with an arbitrary table name.
pub fn drop_table() -> BoxedStrategy<DropTableStatement> {
    (identifier(), any::<bool>())
        .prop_map(|(table_name, if_exists)| DropTableStatement {
            table_name,
            if_exists,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drop_table_display() {
        let stmt = DropTableStatement {
            table_name: "users".to_string(),
            if_exists: false,
        };

        assert_eq!(stmt.to_string(), "DROP TABLE \"users\"");
    }

    #[test]
    fn test_drop_table_if_exists() {
        let stmt = DropTableStatement {
            table_name: "old_data".to_string(),
            if_exists: true,
        };

        assert_eq!(stmt.to_string(), "DROP TABLE IF EXISTS \"old_data\"");
    }
}
