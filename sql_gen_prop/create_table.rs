//! CREATE TABLE statement type and generation strategy.

use proptest::prelude::*;
use std::collections::HashSet;
use std::fmt;

use crate::schema::{DataType, Schema};

/// A column definition for CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\" {}", self.name, self.data_type)?;

        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }

        if !self.nullable && !self.primary_key {
            write!(f, " NOT NULL")?;
        }

        if self.unique && !self.primary_key {
            write!(f, " UNIQUE")?;
        }

        if let Some(default) = &self.default {
            write!(f, " DEFAULT {default}")?;
        }

        Ok(())
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

/// Generate a CREATE TABLE statement that avoids existing table names in the schema.
pub fn create_table(schema: &Schema) -> BoxedStrategy<CreateTableStatement> {
    let existing_names = schema.table_names();

    (
        identifier_excluding(existing_names),
        any::<bool>(), // if_not_exists
        primary_key_column_def(),
        proptest::collection::vec(column_def(), 0..10),
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
