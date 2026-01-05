use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DropView {
    pub name: String,
}

impl Display for DropView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP VIEW {}", self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DropMaterializedView {
    pub name: String,
}

impl Display for DropMaterializedView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP VIEW {}", self.name)
    }
}

impl DropMaterializedView {
    /// Convert to a regular DropView (for rusqlite oracle)
    pub fn to_regular_drop_view(&self) -> DropView {
        DropView {
            name: self.name.clone(),
        }
    }
}
