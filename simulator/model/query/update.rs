use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{model::table::Value, SimulatorEnv};

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Update {
    pub(crate) table: String,
    pub(crate) set_values: Vec<(String, Value)>, // Pair of value for set expressions => SET name=value
    pub(crate) predicate: Predicate,
}

impl Update {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        let table = env
            .tables
            .iter_mut()
            .find(|t| t.name == self.table)
            .unwrap();
        let t2 = table.clone();
        for row in table
            .rows
            .iter_mut()
            .filter(|r| self.predicate.test(r, &t2))
        {
            for (column, set_value) in &self.set_values {
                let (idx, _) = table
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| &c.name == column)
                    .unwrap();
                row[idx] = set_value.clone();
            }
        }

        vec![]
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;
        for (i, (name, value)) in self.set_values.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} = {}", name, value)?;
        }
        write!(f, " WHERE {}", self.predicate)?;
        Ok(())
    }
}
