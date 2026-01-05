use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::select::Select;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateMaterializedView {
    pub name: String,
    pub select: Select,
}

impl Display for CreateMaterializedView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE MATERIALIZED VIEW {} AS {}",
            self.name, self.select
        )
    }
}

impl CreateMaterializedView {
    /// Convert to a regular CreateView (for rusqlite oracle)
    pub fn to_regular_view(&self) -> super::create_view::CreateView {
        super::create_view::CreateView {
            name: self.name.clone(),
            select: self.select.clone(),
        }
    }
}
