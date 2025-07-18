use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{
    generation::Shadow,
    model::table::{SimValue, Table},
    runner::env::SimulatorTables,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Create {
    pub(crate) table: Table,
}

impl Shadow for Create {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        if !tables.iter().any(|t| t.name == self.table.name) {
            tables.push(self.table.clone());
            Ok(vec![])
        } else {
            Err(anyhow::anyhow!(
                "Table {} already exists. CREATE TABLE statement ignored.",
                self.table.name
            ))
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
