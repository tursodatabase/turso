use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::select::Select;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateView {
    pub name: String,
    pub select: Select,
}

impl Display for CreateView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE VIEW {} AS {}", self.name, self.select)
    }
}
