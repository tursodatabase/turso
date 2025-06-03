use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{model::table::Value, SimulatorEnv};

use super::predicate::Predicate;

/// `SELECT` distinctness
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Distinctness {
    /// `DISTINCT`
    Distinct,
    /// `ALL`
    All,
}

/// `SELECT` or `RETURNING` result column
// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResultColumn {
    /// expression
    Expr(Predicate),
    /// `*`
    Star,
    /// column name
    Column(String),
}

impl Display for ResultColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultColumn::Expr(expr) => write!(f, "({})", expr),
            ResultColumn::Star => write!(f, "*"),
            ResultColumn::Column(name) => write!(f, "{}", name),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Select {
    pub(crate) table: String,
    pub(crate) result_columns: Vec<ResultColumn>,
    pub(crate) predicate: Predicate,
    pub(crate) distinct: Distinctness,
    pub(crate) limit: Option<usize>,
}

impl Select {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<Value>> {
        let table = env.tables.iter().find(|t| t.name == self.table.as_str());
        if let Some(table) = table {
            table
                .rows
                .iter()
                .filter(|row| self.predicate.test(row, table))
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SELECT {} FROM {} WHERE {}{}",
            self.result_columns
                .iter()
                .map(ResultColumn::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            self.table,
            self.predicate,
            self.limit
                .map_or("".to_string(), |l| format!(" LIMIT {}", l))
        )
    }
}
