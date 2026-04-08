//! CREATE INDEX statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::create_table::{TemporaryKeyword, identifier_excluding};
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};
use crate::select::OrderDirection;

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
        write!(f, "{}", self.name)?;
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
    pub temporary: Option<TemporaryKeyword>,
}

impl fmt::Display for CreateIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE ")?;
        if let Some(keyword) = self.temporary {
            write!(f, "{keyword} ")?;
        }

        if self.unique {
            write!(f, "UNIQUE ")?;
        }

        write!(f, "INDEX ")?;

        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }

        write!(f, "{} ON {} (", self.index_name, self.table_name)?;

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
    let index_database = table.database.clone();
    let table_name = table.unqualified_name().to_string();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let existing_indexes = schema.index_names_in_database(index_database.as_deref());

    // Extract profile values from the CreateIndexProfile
    let max_columns = profile.create_index_profile().max_columns;

    if col_names.is_empty() {
        let temporary = match index_database.as_deref() {
            Some("temp") => Some(TemporaryKeyword::Temp),
            _ => None,
        };
        let index_name = match index_database.as_deref() {
            Some("temp") => "idx_empty".to_string(),
            Some(db) => format!("{db}.idx_empty"),
            None => "idx_empty".to_string(),
        };
        return Just(CreateIndexStatement {
            index_name,
            table_name,
            columns: vec![],
            unique: false,
            if_not_exists: true,
            temporary,
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
                let index_database = index_database.clone();
                let table_name = table_name.clone();

                let col_strategies: Vec<_> = selected_cols
                    .into_iter()
                    .map(|name| index_column(name).boxed())
                    .collect();

                col_strategies.prop_map(move |columns| {
                    let index_name = format!("idx_{table_name}_{index_suffix}");
                    let temporary = match index_database.as_deref() {
                        Some("temp") => Some(if if_not_exists {
                            TemporaryKeyword::Temporary
                        } else {
                            TemporaryKeyword::Temp
                        }),
                        _ => None,
                    };
                    let qualified_index_name = match index_database.as_deref() {
                        Some("temp") => index_name,
                        Some(db) => format!("{db}.{index_name}"),
                        None => index_name,
                    };

                    CreateIndexStatement {
                        index_name: qualified_index_name,
                        table_name: table_name.clone(),
                        columns,
                        unique,
                        if_not_exists,
                        temporary,
                    }
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

    let existing_indexes_by_database: Vec<(Option<String>, std::collections::HashSet<String>)> =
        std::iter::once(None)
            .chain(schema.attached_databases.iter().cloned().map(Some))
            .map(|db| {
                let existing = schema.index_names_in_database(db.as_deref());
                (db, existing)
            })
            .collect();
    let tables = (*schema.tables).clone();

    proptest::sample::select(tables)
        .prop_flat_map(move |table| {
            let index_database = table.database.clone();
            let table_name = table.unqualified_name().to_string();
            let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
            let existing = existing_indexes_by_database
                .iter()
                .find(|(db, _)| *db == index_database)
                .map(|(_, names)| names.clone())
                .unwrap_or_default();

            if col_names.is_empty() {
                let temporary = match index_database.as_deref() {
                    Some("temp") => Some(TemporaryKeyword::Temp),
                    _ => None,
                };
                let index_name = match index_database.as_deref() {
                    Some("temp") => "idx_empty".to_string(),
                    Some(db) => format!("{db}.idx_empty"),
                    None => "idx_empty".to_string(),
                };
                return Just(CreateIndexStatement {
                    index_name,
                    table_name,
                    columns: vec![],
                    unique: false,
                    if_not_exists: true,
                    temporary,
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
                        let index_database = index_database.clone();
                        let table_name = table_name.clone();

                        let col_strategies: Vec<_> = selected_cols
                            .into_iter()
                            .map(|name| index_column(name).boxed())
                            .collect();

                        col_strategies.prop_map(move |columns| {
                            let index_name = format!("idx_{table_name}_{index_suffix}");
                            let temporary = match index_database.as_deref() {
                                Some("temp") => Some(if if_not_exists {
                                    TemporaryKeyword::Temporary
                                } else {
                                    TemporaryKeyword::Temp
                                }),
                                _ => None,
                            };
                            let qualified_index_name = match index_database.as_deref() {
                                Some("temp") => index_name,
                                Some(db) => format!("{db}.{index_name}"),
                                None => index_name,
                            };

                            CreateIndexStatement {
                                index_name: qualified_index_name,
                                table_name: table_name.clone(),
                                columns,
                                unique,
                                if_not_exists,
                                temporary,
                            }
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
            temporary: None,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE UNIQUE INDEX idx_users_email ON users (email ASC)"
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
            temporary: None,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX IF NOT EXISTS idx_composite ON orders (user_id, created_at DESC)"
        );
    }

    #[test]
    fn test_create_index_with_temp_schema_name() {
        let stmt = CreateIndexStatement {
            index_name: "temp.idx_temp_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec![IndexColumn {
                name: "email".to_string(),
                direction: None,
            }],
            unique: false,
            if_not_exists: false,
            temporary: None,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX temp.idx_temp_users_email ON users (email)"
        );
    }

    #[test]
    fn test_create_temp_index_display() {
        let stmt = CreateIndexStatement {
            index_name: "idx_temp_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec![IndexColumn {
                name: "email".to_string(),
                direction: None,
            }],
            unique: false,
            if_not_exists: false,
            temporary: Some(TemporaryKeyword::Temp),
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TEMP INDEX idx_temp_users_email ON users (email)"
        );
    }
}
