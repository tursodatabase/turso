use serde::{Deserialize, Serialize};

use super::query::select::Select;

/// Represents a view (regular or materialized) that has been created.
/// Used for tracking views in the shadow state so we can generate
/// DROP VIEW / DROP MATERIALIZED VIEW statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    pub select: Select,
    pub materialized: bool,
}

impl View {
    pub fn new(name: String, select: Select, materialized: bool) -> Self {
        Self {
            name,
            select,
            materialized,
        }
    }

    pub fn materialized(name: String, select: Select) -> Self {
        Self::new(name, select, true)
    }

    pub fn regular(name: String, select: Select) -> Self {
        Self::new(name, select, false)
    }
}
