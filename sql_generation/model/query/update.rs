use std::fmt::Display;

use serde::{Deserialize, Serialize};

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
        condition: Predicate,
        then_value: SimValue,
        else_column: String,
    },
}

impl SetValue {
    /// Returns the simple value
    pub fn simple_value(&self) -> &SimValue {
        match self {
            SetValue::Simple(v) => v,
            SetValue::CaseWhen { .. } => {
                panic!("simple_value() called on CaseWhen variant")
            }
        }
    }

    pub fn is_case_when(&self) -> bool {
        matches!(self, SetValue::CaseWhen { .. })
    }
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
    pub set_values: Vec<(String, SetValue)>, // Pair of value for set expressions => SET name=value
    pub predicate: Predicate,
}

impl Update {
    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn has_case_when(&self) -> bool {
        self.set_values.iter().any(|(_, v)| v.is_case_when())
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;
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
