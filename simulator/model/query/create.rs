use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::{
    table::{SimValue, Table},
    Shadow, SimulatorEnv,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Create {
    pub table: Table,
}

impl Shadow for Create {
    fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>> {
        if !env.tables().iter().any(|t| t.name == self.table.name) {
            env.add_table(self.table.clone());
        }

        vec![]
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
