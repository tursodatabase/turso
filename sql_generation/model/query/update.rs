use std::fmt::Display;

use serde::{Deserialize, Serialize};
use turso_parser::ast::ResolveType;

use crate::model::table::SimValue;

use super::predicate::Predicate;

/// Value assignment in UPDATE SET clause
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SetValue {
    /// Simple assignment: col = value
    Simple(SimValue),
    /// Conditional assignment: col = CASE WHEN cond THEN val ELSE col END
    /// Used for partial updates that should fail on constraint violation
    CaseWhen {
        condition: Box<Predicate>,
        then_value: SimValue,
        else_column: String,
    },
}

impl Display for SetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetValue::Simple(v) => write!(f, "{v}"),
            SetValue::CaseWhen {
                condition,
                then_value,
                else_column,
            } => {
                // else_column is printed as identifier (no quotes)
                write!(
                    f,
                    "CASE WHEN {condition} THEN {then_value} ELSE {else_column} END"
                )
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    #[serde(default)]
    pub or_conflict: Option<ResolveType>,
    pub set_values: Vec<(String, SetValue)>, // Pair of value for set expressions => SET name=value
    pub predicate: Predicate,
}

impl Update {
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE")?;
        if let Some(or_conflict) = self.or_conflict {
            let kw = match or_conflict {
                ResolveType::Rollback => "ROLLBACK",
                ResolveType::Abort => "ABORT",
                ResolveType::Fail => "FAIL",
                ResolveType::Ignore => "IGNORE",
                ResolveType::Replace => "REPLACE",
            };
            write!(f, " OR {kw}")?;
        }
        write!(f, " {} SET ", self.table)?;
        for (i, (name, value)) in self.set_values.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name} = {value}")?;
        }
        write!(f, " WHERE {}", self.predicate)?;
        Ok(())
    }
}
