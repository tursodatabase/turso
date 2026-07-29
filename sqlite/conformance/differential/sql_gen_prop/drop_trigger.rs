//! DROP TRIGGER statement generation.

use std::fmt;

use proptest::prelude::*;

use crate::schema::Schema;

/// DROP TRIGGER statement.
#[derive(Debug, Clone)]
pub struct DropTriggerStatement {
    /// Trigger name to drop.
    pub trigger_name: String,
    /// Whether to use IF EXISTS.
    pub if_exists: bool,
}

impl fmt::Display for DropTriggerStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TRIGGER ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.trigger_name)
    }
}

/// Generate a DROP TRIGGER statement with a random name.
pub fn drop_trigger() -> BoxedStrategy<DropTriggerStatement> {
    (crate::create_table::identifier(), any::<bool>())
        .prop_map(|(name, if_exists)| DropTriggerStatement {
            trigger_name: name,
            if_exists,
        })
        .boxed()
}

/// Generate a DROP TRIGGER statement for triggers in the schema.
///
/// If triggers exist in the schema, selects from existing trigger names.
/// Otherwise, generates a DROP TRIGGER IF EXISTS with a random name.
pub fn drop_trigger_for_schema(schema: &Schema) -> BoxedStrategy<DropTriggerStatement> {
    if schema.triggers.is_empty() {
        // No triggers exist, generate DROP TRIGGER IF EXISTS with random name
        (crate::create_table::identifier(), Just(true))
            .prop_map(|(name, if_exists)| DropTriggerStatement {
                trigger_name: name,
                if_exists,
            })
            .boxed()
    } else {
        let trigger_names: Vec<String> = schema.triggers.iter().map(|t| t.name.clone()).collect();
        (proptest::sample::select(trigger_names), any::<bool>())
            .prop_map(|(name, if_exists)| DropTriggerStatement {
                trigger_name: name,
                if_exists,
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn drop_trigger_generates_valid_sql(stmt in drop_trigger()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("DROP TRIGGER"));
        }
    }
}
