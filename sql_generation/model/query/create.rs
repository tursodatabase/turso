use std::fmt::Display;

use serde::{Deserialize, Serialize};
use turso_parser::ast::{ColumnConstraint, CreateTableBody, QualifiedName};

use crate::model::table::{Column, ColumnType, Table};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Create {
    pub table: Table,
}

impl From<(bool, bool, QualifiedName, CreateTableBody)> for Create {
    fn from(
        (_temporary, _if_not_exists, tbl_name, body): (bool, bool, QualifiedName, CreateTableBody),
    ) -> Self {
        match body {
            CreateTableBody::ColumnsAndConstraints {
                columns,
                constraints: _,
                options: _,
            } => Create {
                table: Table {
                    name: tbl_name.to_string(),
                    columns: columns
                        .into_iter()
                        .map(|col_def| Column {
                            name: col_def.col_name.to_string(),
                            column_type: col_def
                                .col_type
                                .map(|t| match t.name.as_str() {
                                    "INT" | "INTEGER" => ColumnType::Integer,
                                    "FLOAT" | "REAL" | "DOUBLE" => ColumnType::Float,
                                    "TEXT" | "VARCHAR" | "CHAR" => ColumnType::Text,
                                    "BLOB" => ColumnType::Blob,
                                    _ => ColumnType::Text,
                                })
                                .unwrap_or(ColumnType::Text),
                            primary: col_def.constraints.iter().any(|c| {
                                matches!(c.constraint, ColumnConstraint::PrimaryKey { .. })
                            }),
                            unique: col_def
                                .constraints
                                .iter()
                                .any(|c| matches!(c.constraint, ColumnConstraint::Unique { .. })),
                        })
                        .collect(),
                    rows: Vec::new(),
                    indexes: Vec::new(),
                },
            },
            CreateTableBody::AsSelect(_) => {
                todo!("CREATE TABLE AS SELECT not implemented")
            }
        }
    }
}

impl Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table.name)?;

        for (i, column) in self.table.columns.iter().enumerate() {
            if i != 0 {
                write!(f, ",")?;
            }
            write!(f, "{} {}", column.name, column.column_type)?;
        }

        write!(f, ")")
    }
}
