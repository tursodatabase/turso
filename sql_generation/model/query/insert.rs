use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::SimValue;

use super::select::Select;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
    },
    /// Insert with explicit column list (required when skipping generated columns)
    ValuesWithColumns {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<SimValue>>,
    },
    Select {
        table: String,
        select: Box<Select>,
    },
}

impl Insert {
    pub fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. }
            | Insert::ValuesWithColumns { table, .. }
            | Insert::Select { table, .. } => table,
        }
    }

    pub fn rows(&self) -> &[Vec<SimValue>] {
        match self {
            Insert::Values { values, .. } | Insert::ValuesWithColumns { values, .. } => values,
            Insert::Select { .. } => unreachable!(),
        }
    }

    /// Returns the column names for ValuesWithColumns, or None for other variants
    pub fn columns(&self) -> Option<&[String]> {
        match self {
            Insert::ValuesWithColumns { columns, .. } => Some(columns),
            _ => None,
        }
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert::Values { table, values } => {
                write!(f, "INSERT INTO {table} VALUES ")?;
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
                Ok(())
            }
            Insert::ValuesWithColumns {
                table,
                columns,
                values,
            } => {
                write!(f, "INSERT INTO {table} (")?;
                for (i, col) in columns.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{col}")?;
                }
                write!(f, ") VALUES ")?;
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
                Ok(())
            }
            Insert::Select { table, select } => {
                write!(f, "INSERT INTO {table} ")?;
                write!(f, "{select}")
            }
        }
    }
}
