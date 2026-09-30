use std::fmt::Display;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::model::table::Table;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Create {
    pub table: Table,
}

impl Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table.name)?;

        let cols = self
            .table
            .columns
            .iter()
            .map(|column| column.to_string())
            .join(", ");

        write!(f, "{cols})")
    }
}
