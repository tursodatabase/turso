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
        condition: Box<Predicate>,
        then_value: SimValue,
        else_column: String,
    },
    SelfRefSubquery(SelfRefSubquery),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SelfRefSubquery {
    MatchByColumn {
        source_column: String,
        key_column: String,
    },
    PreviousByOrder {
        source_column: String,
        order_column: String,
    },
}

impl SetValue {
    pub fn to_sql_with_context(&self, outer_table: &str) -> String {
        match self {
            SetValue::Simple(v) => format!("{v}"),
            SetValue::CaseWhen {
                condition,
                then_value,
                else_column,
            } => {
                format!("CASE WHEN {condition} THEN {then_value} ELSE {else_column} END")
            }
            SetValue::SelfRefSubquery(SelfRefSubquery::MatchByColumn {
                source_column,
                key_column,
            }) => {
                format!(
                    "(SELECT t2.{source_column} FROM {outer_table} AS t2 WHERE t2.{key_column} = {outer_table}.{key_column} LIMIT 1)"
                )
            }
            SetValue::SelfRefSubquery(SelfRefSubquery::PreviousByOrder {
                source_column,
                order_column,
            }) => {
                format!(
                    "(SELECT t2.{source_column} FROM {outer_table} AS t2 WHERE t2.{order_column} < {outer_table}.{order_column} ORDER BY t2.{order_column} DESC LIMIT 1)"
                )
            }
        }
    }
}

impl Display for SetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql_with_context("t"))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub set_values: Vec<(String, SetValue)>, // Pair of value for set expressions => SET name=value
    pub predicate: Predicate,
    #[serde(default)]
    pub returning: Option<Vec<String>>,
}

impl Update {
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;
        for (i, (name, value)) in self.set_values.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name} = {}", value.to_sql_with_context(&self.table))?;
        }
        write!(f, " WHERE {}", self.predicate)?;
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
    fn update_formats_self_ref_subquery() {
        let update = Update {
            table: "t".to_string(),
            set_values: vec![(
                "v".to_string(),
                SetValue::SelfRefSubquery(SelfRefSubquery::MatchByColumn {
                    source_column: "v".to_string(),
                    key_column: "k".to_string(),
                }),
            )],
            predicate: Predicate::true_(),
            returning: None,
        };

        let sql = update.to_string();
        assert!(sql.contains("UPDATE t SET"));
        assert!(sql.contains("SELECT t2.v FROM t AS t2"));
        assert!(sql.contains("t2.k = t.k"));
    }

    #[test]
    fn update_formats_returning_star() {
        let update = Update {
            table: "t".to_string(),
            set_values: vec![("v".to_string(), SetValue::Simple(SimValue::TRUE))],
            predicate: Predicate::true_(),
            returning: Some(vec![]),
        };

        assert!(update.to_string().ends_with("RETURNING *"));
    }
}
