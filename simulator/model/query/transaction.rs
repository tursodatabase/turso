use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{generation::Shadow, model::table::SimValue, runner::env::SimulatorTables};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Begin {
    pub(crate) immediate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Commit;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Rollback;

impl Shadow for Begin {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        tables.snapshot = Some(tables.tables.clone());
        vec![]
    }
}

impl Shadow for Commit {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        tables.snapshot = None;
        vec![]
    }
}

impl Shadow for Rollback {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        if let Some(tables_) = tables.snapshot.take() {
            tables.tables = tables_;
        }
        vec![]
    }
}

impl Display for Begin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BEGIN {}", if self.immediate { "IMMEDIATE" } else { "" })
    }
}

impl Display for Commit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COMMIT")
    }
}

impl Display for Rollback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROLLBACK")
    }
}
