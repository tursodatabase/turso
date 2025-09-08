use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::Table;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Create {
    pub table: Table,
}

impl Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table.name)?;

        for (i, column) in self.table.columns.iter().enumerate() {
            if i != 0 {
                write!(f, ",")?;
            }
            write!(f, "{} {}", column.name, column.column_type)?;
            if column.primary {
                write!(f, " PRIMARY KEY")?;
            }
            if column.unique && !column.primary {
                write!(f, " UNIQUE")?;
            }
        }

        write!(f, ")")
    }
}
