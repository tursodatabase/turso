//! CREATE TABLE statement type and generation strategy.

use proptest::prelude::*;
use std::collections::HashSet;
use std::fmt;
use std::ops::RangeInclusive;

use crate::schema::{ColumnDef, DataType, Schema};

// =============================================================================
// CREATE TABLE PROFILE
// =============================================================================

/// Profile for controlling CREATE TABLE statement generation.
#[derive(Debug, Clone)]
pub struct CreateTableProfile {
    /// Pattern for table name generation (regex).
    pub identifier_pattern: String,
    /// Range for number of non-PK columns.
    pub column_count_range: RangeInclusive<usize>,
}

impl Default for CreateTableProfile {
    fn default() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 0..=10,
        }
    }
}

impl CreateTableProfile {
    /// Create a profile for small tables.
    pub fn small() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,15}".to_string(),
            column_count_range: 1..=3,
        }
    }

    /// Create a profile for large tables.
    pub fn large() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 5..=20,
        }
    }

    /// Builder method to set identifier pattern.
    pub fn with_identifier_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.identifier_pattern = pattern.into();
        self
    }

    /// Builder method to set column count range.
    pub fn with_column_count_range(mut self, range: RangeInclusive<usize>) -> Self {
        self.column_count_range = range;
        self
    }
}

/// A CREATE TABLE statement.
#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE ")?;

        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }

        write!(f, "\"{}\" (", self.table_name)?;

        let col_defs: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{})", col_defs.join(", "))
    }
}

/// Generate a valid SQL identifier.
pub fn identifier() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,30}".prop_filter("must not be empty", |s| !s.is_empty())
}

/// Generate a valid SQL identifier that is not in the excluded set.
pub fn identifier_excluding(excluded: HashSet<String>) -> impl Strategy<Value = String> {
    identifier().prop_filter_map("must not be in excluded set", move |s| {
        if excluded.contains(&s) { None } else { Some(s) }
    })
}

/// Generate a data type.
pub fn data_type() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Integer),
        Just(DataType::Real),
        Just(DataType::Text),
        Just(DataType::Blob),
    ]
}

/// Generate a column definition.
pub fn column_def() -> impl Strategy<Value = ColumnDef> {
    (
        identifier(),
        data_type(),
        any::<bool>(), // nullable
        any::<bool>(), // unique
    )
        .prop_map(|(name, data_type, nullable, unique)| ColumnDef {
            name,
            data_type,
            nullable,
            primary_key: false,
            unique,
            default: None,
        })
}

/// Generate a primary key column definition.
pub fn primary_key_column_def() -> impl Strategy<Value = ColumnDef> {
    (identifier(), data_type()).prop_map(|(name, data_type)| ColumnDef {
        name,
        data_type,
        nullable: false,
        primary_key: true,
        unique: false,
        default: None,
    })
}

/// Generate a CREATE TABLE statement with profile.
pub fn create_table(
    schema: &Schema,
    profile: &CreateTableProfile,
) -> BoxedStrategy<CreateTableStatement> {
    let existing_names = schema.table_names();

    // Extract profile values
    let column_count_range = profile.column_count_range.clone();

    (
        identifier_excluding(existing_names),
        any::<bool>(), // if_not_exists
        primary_key_column_def(),
        proptest::collection::vec(column_def(), column_count_range),
    )
        .prop_map(|(table_name, if_not_exists, pk_col, other_cols)| {
            let mut columns = vec![pk_col];
            columns.extend(other_cols);
            CreateTableStatement {
                table_name,
                columns,
                if_not_exists,
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_display() {
        let stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: true,
                    default: None,
                },
            ],
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE \"users\" (\"id\" INTEGER PRIMARY KEY, \"name\" TEXT NOT NULL, \"email\" TEXT UNIQUE)"
        );
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let stmt = CreateTableStatement {
            table_name: "test".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default: None,
            }],
            if_not_exists: true,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE IF NOT EXISTS \"test\" (\"id\" INTEGER PRIMARY KEY)"
        );
    }
}
