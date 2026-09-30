use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Drop {
    pub table: String,
}

impl Display for Drop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP TABLE {}", self.table)
    }
}
