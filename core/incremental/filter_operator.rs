#![allow(dead_code)]
// Filter operator for DBSP-style incremental computation
// This operator filters rows based on predicates

use crate::incremental::dbsp::{Delta, DeltaPair};
use crate::incremental::operator::{
    ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::types::{IOResult, Text};
use crate::{Result, Value};
use std::sync::{Arc, Mutex};
use turso_parser::ast::{Expr, Literal, OneSelect, Operator};

/// Filter predicate for filtering rows
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// Column = value
    Equals { column: String, value: Value },
    /// Column != value
    NotEquals { column: String, value: Value },
    /// Column > value
    GreaterThan { column: String, value: Value },
    /// Column >= value
    GreaterThanOrEqual { column: String, value: Value },
    /// Column < value
    LessThan { column: String, value: Value },
    /// Column <= value
    LessThanOrEqual { column: String, value: Value },
    /// Logical AND of two predicates
    And(Box<FilterPredicate>, Box<FilterPredicate>),
    /// Logical OR of two predicates
    Or(Box<FilterPredicate>, Box<FilterPredicate>),
    /// No predicate (accept all rows)
    None,
}

impl FilterPredicate {
    /// Parse a SQL AST expression into a FilterPredicate
    /// This centralizes all SQL-to-predicate parsing logic
    pub fn from_sql_expr(expr: &turso_parser::ast::Expr) -> crate::Result<Self> {
        let Expr::Binary(lhs, op, rhs) = expr else {
            return Err(crate::LimboError::ParseError(
                "Unsupported WHERE clause for incremental views: not a binary expression"
                    .to_string(),
            ));
        };

        // Handle AND/OR logical operators
        match op {
            Operator::And => {
                let left = Self::from_sql_expr(lhs)?;
                let right = Self::from_sql_expr(rhs)?;
                return Ok(FilterPredicate::And(Box::new(left), Box::new(right)));
            }
            Operator::Or => {
                let left = Self::from_sql_expr(lhs)?;
                let right = Self::from_sql_expr(rhs)?;
                return Ok(FilterPredicate::Or(Box::new(left), Box::new(right)));
            }
            _ => {}
        }

        // Handle comparison operators
        let Expr::Id(column_name) = &**lhs else {
            return Err(crate::LimboError::ParseError(
                "Unsupported WHERE clause for incremental views: left-hand-side is not a column reference".to_string(),
            ));
        };

        let column = column_name.as_str().to_string();

        // Parse the right-hand side value
        let value = match &**rhs {
            Expr::Literal(Literal::String(s)) => {
                // Strip quotes from string literals
                let cleaned = s.trim_matches('\'').trim_matches('"');
                Value::Text(Text::new(cleaned))
            }
            Expr::Literal(Literal::Numeric(n)) => {
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    return Err(crate::LimboError::ParseError(
                        "Unsupported WHERE clause for incremental views: right-hand-side is not a numeric literal".to_string(),
                    ));
                }
            }
            Expr::Literal(Literal::Null) => Value::Null,
            Expr::Literal(Literal::Blob(_)) => {
                // Blob comparison not yet supported
                return Err(crate::LimboError::ParseError(
                    "Unsupported WHERE clause for incremental views: comparison with blob literals is not supported".to_string(),
                ));
            }
            other => {
                // Complex expressions not yet supported
                return Err(crate::LimboError::ParseError(
                    format!("Unsupported WHERE clause for incremental views: comparison with {other:?} is not supported"),
                ));
            }
        };

        // Create the appropriate predicate based on operator
        match op {
            Operator::Equals => Ok(FilterPredicate::Equals { column, value }),
            Operator::NotEquals => Ok(FilterPredicate::NotEquals { column, value }),
            Operator::Greater => Ok(FilterPredicate::GreaterThan { column, value }),
            Operator::GreaterEquals => Ok(FilterPredicate::GreaterThanOrEqual { column, value }),
            Operator::Less => Ok(FilterPredicate::LessThan { column, value }),
            Operator::LessEquals => Ok(FilterPredicate::LessThanOrEqual { column, value }),
            other => Err(crate::LimboError::ParseError(
                format!("Unsupported WHERE clause for incremental views: comparison operator {other:?} is not supported"),
            )),
        }
    }

    /// Parse a WHERE clause from a SELECT statement
    pub fn from_select(select: &turso_parser::ast::Select) -> crate::Result<Self> {
        if let OneSelect::Select {
            ref where_clause, ..
        } = select.body.select
        {
            if let Some(where_clause) = where_clause {
                Self::from_sql_expr(where_clause)
            } else {
                Ok(FilterPredicate::None)
            }
        } else {
            Err(crate::LimboError::ParseError(
                "Unsupported WHERE clause for incremental views: not a single SELECT statement"
                    .to_string(),
            ))
        }
    }
}

/// Filter operator - filters rows based on predicate
#[derive(Debug)]
pub struct FilterOperator {
    predicate: FilterPredicate,
    column_names: Vec<String>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
}

impl FilterOperator {
    pub fn new(predicate: FilterPredicate, column_names: Vec<String>) -> Self {
        Self {
            predicate,
            column_names,
            tracker: None,
        }
    }

    /// Get the predicate for this filter
    pub fn predicate(&self) -> &FilterPredicate {
        &self.predicate
    }

    pub fn evaluate_predicate(&self, values: &[Value]) -> bool {
        match &self.predicate {
            FilterPredicate::None => true,
            FilterPredicate::Equals { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        return v == value;
                    }
                }
                false
            }
            FilterPredicate::NotEquals { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        return v != value;
                    }
                }
                false
            }
            FilterPredicate::GreaterThan { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        // Compare based on value types
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a > b,
                            (Value::Float(a), Value::Float(b)) => return a > b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() > b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::GreaterThanOrEqual { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a >= b,
                            (Value::Float(a), Value::Float(b)) => return a >= b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() >= b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::LessThan { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a < b,
                            (Value::Float(a), Value::Float(b)) => return a < b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() < b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::LessThanOrEqual { column, value } => {
                if let Some(idx) = self.column_names.iter().position(|c| c == column) {
                    if let Some(v) = values.get(idx) {
                        match (v, value) {
                            (Value::Integer(a), Value::Integer(b)) => return a <= b,
                            (Value::Float(a), Value::Float(b)) => return a <= b,
                            (Value::Text(a), Value::Text(b)) => return a.as_str() <= b.as_str(),
                            _ => {}
                        }
                    }
                }
                false
            }
            FilterPredicate::And(left, right) => {
                // Temporarily create sub-filters to evaluate
                let left_filter = FilterOperator::new((**left).clone(), self.column_names.clone());
                let right_filter =
                    FilterOperator::new((**right).clone(), self.column_names.clone());
                left_filter.evaluate_predicate(values) && right_filter.evaluate_predicate(values)
            }
            FilterPredicate::Or(left, right) => {
                let left_filter = FilterOperator::new((**left).clone(), self.column_names.clone());
                let right_filter =
                    FilterOperator::new((**right).clone(), self.column_names.clone());
                left_filter.evaluate_predicate(values) || right_filter.evaluate_predicate(values)
            }
        }
    }
}

impl IncrementalOperator for FilterOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        let delta = match state {
            EvalState::Init { deltas } => {
                // Filter operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "FilterOperator expects right_delta to be empty"
                );
                std::mem::take(&mut deltas.left)
            }
            _ => unreachable!(
                "FilterOperator doesn't execute the state machine. Should be in Init state"
            ),
        };

        let mut output_delta = Delta::new();

        // Process the delta through the filter
        for (row, weight) in delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_filter();
            }

            // Only pass through rows that satisfy the filter predicate
            // For deletes (weight < 0), we only pass them if the row values
            // would have passed the filter (meaning it was in the view)
            if self.evaluate_predicate(&row.values) {
                output_delta.changes.push((row, weight));
            }
        }

        *state = EvalState::Done;
        Ok(IOResult::Done(output_delta))
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Filter operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "FilterOperator expects right delta to be empty in commit"
        );

        let mut output_delta = Delta::new();

        // Commit the delta to our internal state
        // Only pass through and track rows that satisfy the filter predicate
        for (row, weight) in deltas.left.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_filter();
            }

            // Only track and output rows that pass the filter
            // For deletes, this means the row was in the view (its values pass the filter)
            // For inserts, this means the row should be in the view
            if self.evaluate_predicate(&row.values) {
                output_delta.changes.push((row, weight));
            }
        }

        Ok(IOResult::Done(output_delta))
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}
