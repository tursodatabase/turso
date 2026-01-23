//! DROP INDEX statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::create_table::identifier;

/// A DROP INDEX statement.
#[derive(Debug, Clone)]
pub struct DropIndexStatement {
    pub index_name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP INDEX ")?;

        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }

        write!(f, "{}", self.index_name)
    }
}

/// Generate a DROP INDEX statement for a specific index name.
pub fn drop_index_named(index_name: &str) -> BoxedStrategy<DropIndexStatement> {
    let name = index_name.to_string();

    any::<bool>()
        .prop_map(move |if_exists| DropIndexStatement {
            index_name: name.clone(),
            if_exists,
        })
        .boxed()
}

/// Generate a DROP INDEX statement with an arbitrary index name.
pub fn drop_index() -> BoxedStrategy<DropIndexStatement> {
    (identifier(), any::<bool>())
        .prop_map(|(index_name, if_exists)| DropIndexStatement {
            index_name,
            if_exists,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drop_index_display() {
        let stmt = DropIndexStatement {
            index_name: "idx_users_email".to_string(),
            if_exists: false,
        };

        assert_eq!(stmt.to_string(), "DROP INDEX idx_users_email");
    }

    #[test]
    fn test_drop_index_if_exists() {
        let stmt = DropIndexStatement {
            index_name: "idx_old".to_string(),
            if_exists: true,
        };

        assert_eq!(stmt.to_string(), "DROP INDEX IF EXISTS idx_old");
    }
}
