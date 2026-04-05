use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::model::table::{Index, IndexColumnKind};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateIndex {
    pub index: Index,
}

impl Deref for CreateIndex {
    type Target = Index;

    fn deref(&self) -> &Self::Target {
        &self.index
    }
}

impl DerefMut for CreateIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.index
    }
}

impl std::fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // In SQLite, CREATE INDEX syntax uses: CREATE INDEX [db.]index_name ON table_name (...)
        // The database prefix goes on the index name, and the table name is always unqualified.
        let (db_prefix, bare_table) = self
            .index
            .table_name
            .split_once('.')
            .map(|(db, tbl)| (format!("{db}."), tbl))
            .unwrap_or_default();
        let bare_table = if bare_table.is_empty() {
            self.index.table_name.as_str()
        } else {
            bare_table
        };
        write!(
            f,
            "CREATE INDEX {}{} ON {} ({})",
            db_prefix,
            self.index.index_name,
            bare_table,
            self.index
                .columns
                .iter()
                .map(|col| match &col.kind {
                    IndexColumnKind::Column { name } => format!("{name} {}", col.order),
                    IndexColumnKind::Expr { expr } => format!("({expr}) {}", col.order),
                })
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
