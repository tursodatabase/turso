use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::SimValue;

use super::select::Select;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OnConflict {
    pub target_column: String,
    pub assignments: Vec<UpdateSetItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateSetItem {
    pub column: String,
    pub excluded_column: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
        #[serde(default)]
        on_conflict: Option<OnConflict>,
        #[serde(default)]
        returning: Option<Vec<String>>,
    },
    Select {
        table: String,
        select: Box<Select>,
        #[serde(default)]
        returning: Option<Vec<String>>,
    },
}

impl Insert {
    pub fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
        }
    }

    pub fn rows(&self) -> &[Vec<SimValue>] {
        match self {
            Insert::Values { values, .. } => values,
            Insert::Select { .. } => unreachable!(),
        }
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert::Values {
                table,
                values,
                on_conflict,
                returning,
            } => {
                write!(f, "INSERT INTO {table} VALUES ")?;
                for (i, row) in values.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, value) in row.iter().enumerate() {
                        if j != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{value}")?;
                    }
                    write!(f, ")")?;
                }
                if let Some(on_conflict) = &on_conflict {
                    write!(f, " {on_conflict}")?;
                }
                if let Some(cols) = returning {
                    if cols.is_empty() {
                        write!(f, " RETURNING *")?;
                    } else {
                        write!(f, " RETURNING {}", cols.join(", "))?;
                    }
                }
                Ok(())
            }
            Insert::Select {
                table,
                select,
                returning,
            } => {
                write!(f, "INSERT INTO {table} ")?;
                write!(f, "{select}")?;
                if let Some(cols) = returning {
                    if cols.is_empty() {
                        write!(f, " RETURNING *")?;
                    } else {
                        write!(f, " RETURNING {}", cols.join(", "))?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ON CONFLICT({}) DO UPDATE SET ", self.target_column)?;
        for (i, a) in self.assignments.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{a}")?;
        }
        Ok(())
    }
}

impl Display for UpdateSetItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = excluded.{}", self.column, self.excluded_column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::query::predicate::Predicate;

    #[test]
    fn insert_values_formats_returning() {
        let q = Insert::Values {
            table: "t".to_string(),
            values: vec![vec![SimValue::TRUE]],
            on_conflict: None,
            returning: Some(vec![]),
        };

        assert!(q.to_string().ends_with("RETURNING *"));
    }

    #[test]
    fn insert_select_formats_returning_columns() {
        let q = Insert::Select {
            table: "t".to_string(),
            select: Box::new(Select::simple("t".to_string(), Predicate::true_())),
            returning: Some(vec!["id".to_string(), "k".to_string()]),
        };

        assert!(q.to_string().contains("RETURNING id, k"));
    }
}
