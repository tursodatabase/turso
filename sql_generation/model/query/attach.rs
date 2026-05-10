use std::fmt::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attach {
    pub database_path: String,
    pub alias: String,
}

impl Display for Attach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ATTACH DATABASE '{}' AS {}", self.database_path, self.alias)
    }
}
