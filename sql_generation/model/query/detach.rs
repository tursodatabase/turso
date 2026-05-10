use std::fmt::Display;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Detach {
    pub alias: String,
}

impl Display for Detach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DETACH DATABASE {}", self.alias)
    }
}
