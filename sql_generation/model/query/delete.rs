use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub predicate: Predicate,
    #[serde(default)]
    pub returning: Option<Vec<String>>,
}

impl Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.predicate)?;
        if let Some(cols) = &self.returning {
            if cols.is_empty() {
                write!(f, " RETURNING *")?;
            } else {
                write!(f, " RETURNING {}", cols.join(", "))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_formats_returning_columns() {
        let q = Delete {
            table: "t".to_string(),
            predicate: Predicate::true_(),
            returning: Some(vec!["id".to_string(), "k".to_string()]),
        };

        assert!(q.to_string().contains("RETURNING id, k"));
    }
}
