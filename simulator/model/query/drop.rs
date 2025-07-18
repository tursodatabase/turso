use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{generation::Shadow, model::table::SimValue, runner::env::SimulatorTables};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Drop {
    pub(crate) table: String,
}

impl Shadow for Drop {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        if !tables.iter().any(|t| t.name == self.table) {
            // If the table does not exist, we return an error
            return Err(anyhow::anyhow!(
                "Table {} does not exist. DROP statement ignored.",
                self.table
            ));
        }

        tables.tables.retain(|t| t.name != self.table);

        Ok(vec![])
    }
}

impl Display for Drop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE {}", self.table)
    }
}
