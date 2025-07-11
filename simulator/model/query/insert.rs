use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{generation::Shadow, model::table::SimValue, runner::env::SimulatorTables};

use super::select::Select;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
    },
    Select {
        table: String,
        select: Box<Select>,
    },
}

impl Shadow for Insert {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result {
        match self {
            Insert::Values { table, values } => {
                if let Some(t) = tables.tables.iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(values.clone());
                } else {
                    return Err(anyhow::anyhow!(
                        "Table {} does not exist. INSERT statement ignored.",
                        table
                    ));
                }
            }
            Insert::Select { table, select } => {
                let rows = select.shadow(tables)?;
                if let Some(t) = tables.tables.iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(rows);
                } else {
                    return Err(anyhow::anyhow!(
                        "Table {} does not exist. INSERT statement ignored.",
                        table
                    ));
                }
            }
        }

        Ok(vec![])
    }
}

impl Insert {
    pub(crate) fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
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
            Insert::Select { table, select } => {
                write!(f, "INSERT INTO {table} ")?;
                write!(f, "{select}")
            }
        }
    }
}
