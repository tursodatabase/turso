use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::SimValue;

use super::select::Select;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OnConflict {
    pub target_column: String,
    pub assignments: Vec<UpdateSetItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateSetItem {
    pub column: String,
    pub excluded_column: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
        #[serde(default)]
        on_conflict: Option<OnConflict>,
    },
    Select {
        table: String,
        select: Box<Select>,
    },
}

impl Insert {
    pub fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
        }
    }

    pub fn rows(&self) -> &[Vec<SimValue>] {
        match self {
            Insert::Values { values, .. } => values,
            Insert::Select { .. } => unreachable!(),
        }
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert::Values {
                table,
                values,
                on_conflict,
            } => {
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
                if let Some(on_conflict) = &on_conflict {
                    write!(f, " {on_conflict}")?;
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

impl Display for OnConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ON CONFLICT({}) DO UPDATE SET ", self.target_column)?;
        for (i, a) in self.assignments.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{a}")?;
        }
        Ok(())
    }
}

impl Display for UpdateSetItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = excluded.{}", self.column, self.excluded_column)
    }
}
