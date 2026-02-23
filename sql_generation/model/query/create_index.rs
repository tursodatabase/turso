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
        write!(
            f,
            "CREATE INDEX {} ON {} ({})",
            self.index.index_name,
            self.index.table_name,
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
