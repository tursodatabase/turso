use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::{table::SimValue, Shadow, SimulatorEnv};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Drop {
    pub table: String,
}

impl Shadow for Drop {
    fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>> {
        env.remove_table(&self.table);
        vec![]
    }
}

impl Display for Drop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE {}", self.table)
    }
}
