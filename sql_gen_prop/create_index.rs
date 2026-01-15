//! CREATE INDEX statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::create_table::identifier_excluding;
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};

// =============================================================================
// CREATE INDEX PROFILE
// =============================================================================

/// Profile for controlling CREATE INDEX statement generation.
#[derive(Debug, Clone)]
pub struct CreateIndexProfile {
    /// Maximum number of columns in an index.
    pub max_columns: usize,
}

impl Default for CreateIndexProfile {
    fn default() -> Self {
        Self { max_columns: 4 }
    }
}

impl CreateIndexProfile {
    /// Builder method to set max columns.
    pub fn with_max_columns(mut self, count: usize) -> Self {
        self.max_columns = count;
        self
    }
}

/// A column reference in an index.
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub direction: Option<OrderDirection>,
}

impl fmt::Display for IndexColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.name)?;
        if let Some(dir) = &self.direction {
            write!(f, " {dir}")?;
        }
        Ok(())
    }
}

/// A CREATE INDEX statement.
#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE ")?;

        if self.unique {
            write!(f, "UNIQUE ")?;
        }

        write!(f, "INDEX ")?;

        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }

        write!(f, "\"{}\" ON \"{}\" (", self.index_name, self.table_name)?;

        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{})", cols.join(", "))
    }
}

/// Generate an index column with optional direction.
pub fn index_column(col_name: String) -> impl Strategy<Value = IndexColumn> {
    proptest::option::of(prop_oneof![
        Just(OrderDirection::Asc),
        Just(OrderDirection::Desc),
    ])
    .prop_map(move |direction| IndexColumn {
        name: col_name.clone(),
        direction,
    })
}

/// Generate a CREATE INDEX statement for a table with profile.
pub fn create_index_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateIndexStatement> {
    let table_name = table.name.clone();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let existing_indexes = schema.index_names();

    // Extract profile values from the CreateIndexProfile
    let max_columns = profile.create_index_profile().max_columns;

    if col_names.is_empty() {
        return Just(CreateIndexStatement {
            index_name: "idx_empty".to_string(),
            table_name,
            columns: vec![],
            unique: false,
            if_not_exists: true,
        })
        .boxed();
    }

    (
        identifier_excluding(existing_indexes),
        any::<bool>(), // unique
        any::<bool>(), // if_not_exists
        proptest::sample::subsequence(col_names.clone(), 1..=col_names.len().min(max_columns)),
    )
        .prop_flat_map(
            move |(index_suffix, unique, if_not_exists, selected_cols)| {
                let table_name = table_name.clone();

                let col_strategies: Vec<_> = selected_cols
                    .into_iter()
                    .map(|name| index_column(name).boxed())
                    .collect();

                col_strategies.prop_map(move |columns| CreateIndexStatement {
                    index_name: format!("idx_{table_name}_{index_suffix}"),
                    table_name: table_name.clone(),
                    columns,
                    unique,
                    if_not_exists,
                })
            },
        )
        .boxed()
}

/// Generate a CREATE INDEX statement for any table with profile.
pub fn create_index(
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateIndexStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    // Extract profile values from the CreateIndexProfile
    let max_columns = profile.create_index_profile().max_columns;

    let existing_indexes = schema.index_names();
    let tables = (*schema.tables).clone();

    proptest::sample::select(tables)
        .prop_flat_map(move |table| {
            let table_name = table.name.clone();
            let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
            let existing = existing_indexes.clone();

            if col_names.is_empty() {
                return Just(CreateIndexStatement {
                    index_name: "idx_empty".to_string(),
                    table_name,
                    columns: vec![],
                    unique: false,
                    if_not_exists: true,
                })
                .boxed();
            }

            (
                identifier_excluding(existing),
                any::<bool>(), // unique
                any::<bool>(), // if_not_exists
                proptest::sample::subsequence(
                    col_names.clone(),
                    1..=col_names.len().min(max_columns),
                ),
            )
                .prop_flat_map(
                    move |(index_suffix, unique, if_not_exists, selected_cols)| {
                        let table_name = table_name.clone();

                        let col_strategies: Vec<_> = selected_cols
                            .into_iter()
                            .map(|name| index_column(name).boxed())
                            .collect();

                        col_strategies.prop_map(move |columns| CreateIndexStatement {
                            index_name: format!("idx_{table_name}_{index_suffix}"),
                            table_name: table_name.clone(),
                            columns,
                            unique,
                            if_not_exists,
                        })
                    },
                )
                .boxed()
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_index_display() {
        let stmt = CreateIndexStatement {
            index_name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec![IndexColumn {
                name: "email".to_string(),
                direction: Some(OrderDirection::Asc),
            }],
            unique: true,
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE UNIQUE INDEX \"idx_users_email\" ON \"users\" (\"email\" ASC)"
        );
    }

    #[test]
    fn test_create_index_multiple_columns() {
        let stmt = CreateIndexStatement {
            index_name: "idx_composite".to_string(),
            table_name: "orders".to_string(),
            columns: vec![
                IndexColumn {
                    name: "user_id".to_string(),
                    direction: None,
                },
                IndexColumn {
                    name: "created_at".to_string(),
                    direction: Some(OrderDirection::Desc),
                },
            ],
            unique: false,
            if_not_exists: true,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX IF NOT EXISTS \"idx_composite\" ON \"orders\" (\"user_id\", \"created_at\" DESC)"
        );
    }
}
