use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{generation::Shadow, model::table::SimValue, runner::env::SimulatorTables};

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delete {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

impl Shadow for Delete {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        let table = tables.tables.iter_mut().find(|t| t.name == self.table);

        if let Some(table) = table {
            // If the table exists, we can delete from it
            let t2 = table.clone();
            table.rows.retain_mut(|r| !self.predicate.test(r, &t2));
        } else {
            // If the table does not exist, we return an error
            return Err(anyhow::anyhow!(
                "Table {} does not exist. DELETE statement ignored.",
                self.table
            ));
        }

        Ok(vec![])
    }
}

impl Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.predicate)
    }
}
