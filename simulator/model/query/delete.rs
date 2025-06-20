use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::{table::SimValue, Shadow, SimulatorEnv};

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub predicate: Predicate,
}

impl Shadow for Delete {
    fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>> {
        let table = env
            .tables_mut()
            .iter_mut()
            .find(|t| t.name == self.table)
            .unwrap();

        let t2 = table.clone();

        table.rows.retain_mut(|r| !self.predicate.test(r, &t2));

        vec![]
    }
}

impl Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.predicate)
    }
}
