#![allow(dead_code)]
// Operator DAG for DBSP-style incremental computation
// Based on Feldera DBSP design but adapted for Turso's architecture

use crate::incremental::expr_compiler::CompiledExpression;
use crate::incremental::hashable_row::HashableRow;
use crate::types::Text;
use crate::{Connection, Database, SymbolTable, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display};
use std::sync::Arc;
use std::sync::Mutex;
use turso_sqlite3_parser::ast::*;

/// Tracks computation counts to verify incremental behavior (for tests now), and in the future
/// should be used to provide statistics.
#[derive(Debug, Default, Clone)]
pub struct ComputationTracker {
    pub filter_evaluations: usize,
    pub project_operations: usize,
    pub join_lookups: usize,
    pub aggregation_updates: usize,
    pub full_scans: usize,
}

impl ComputationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_filter(&mut self) {
        self.filter_evaluations += 1;
    }

    pub fn record_project(&mut self) {
        self.project_operations += 1;
    }

    pub fn record_join_lookup(&mut self) {
        self.join_lookups += 1;
    }

    pub fn record_aggregation(&mut self) {
        self.aggregation_updates += 1;
    }

    pub fn record_full_scan(&mut self) {
        self.full_scans += 1;
    }

    pub fn total_computations(&self) -> usize {
        self.filter_evaluations
            + self.project_operations
            + self.join_lookups
            + self.aggregation_updates
    }
}

/// A delta represents ordered changes to data
#[derive(Debug, Clone, Default)]
pub struct Delta {
    /// Ordered list of changes: (row, weight) where weight is +1 for insert, -1 for delete
    /// It is crucial that this is ordered. Imagine the case of an update, which becomes a delete +
    /// insert. If this is not ordered, it would be applied in arbitrary order and break the view.
    pub changes: Vec<(HashableRow, isize)>,
}

impl Delta {
    pub fn new() -> Self {
        Self {
            changes: Vec::new(),
        }
    }

    pub fn insert(&mut self, row_key: i64, values: Vec<Value>) {
        let row = HashableRow::new(row_key, values);
        self.changes.push((row, 1));
    }

    pub fn delete(&mut self, row_key: i64, values: Vec<Value>) {
        let row = HashableRow::new(row_key, values);
        self.changes.push((row, -1));
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// Merge another delta into this one
    /// This preserves the order of operations - no consolidation is done
    /// to maintain the full history of changes
    pub fn merge(&mut self, other: &Delta) {
        // Simply append all changes from other, preserving order
        self.changes.extend(other.changes.iter().cloned());
    }

    /// Consolidate changes by combining entries with the same HashableRow
    pub fn consolidate(&mut self) {
        if self.changes.is_empty() {
            return;
        }

        // Use a HashMap to accumulate weights
        let mut consolidated: HashMap<HashableRow, isize> = HashMap::new();

        for (row, weight) in self.changes.drain(..) {
            *consolidated.entry(row).or_insert(0) += weight;
        }

        // Convert back to vec, filtering out zero weights
        self.changes = consolidated
            .into_iter()
            .filter(|(_, weight)| *weight != 0)
            .collect();
    }
}

#[cfg(test)]
mod hashable_row_tests {
    use super::*;

    #[test]
    fn test_hashable_row_delta_operations() {
        let mut delta = Delta::new();

        // Test INSERT
        delta.insert(1, vec![Value::Integer(1), Value::Integer(100)]);
        assert_eq!(delta.len(), 1);

        // Test UPDATE (DELETE + INSERT) - order matters!
        delta.delete(1, vec![Value::Integer(1), Value::Integer(100)]);
        delta.insert(1, vec![Value::Integer(1), Value::Integer(200)]);
        assert_eq!(delta.len(), 3); // Should have 3 operations before consolidation

        // Verify order is preserved
        let ops: Vec<_> = delta.changes.iter().collect();
        assert_eq!(ops[0].1, 1); // First insert
        assert_eq!(ops[1].1, -1); // Delete
        assert_eq!(ops[2].1, 1); // Second insert

        // Test consolidation
        delta.consolidate();
        // After consolidation, the first insert and delete should cancel out
        // leaving only the second insert
        assert_eq!(delta.len(), 1);

        let final_row = &delta.changes[0];
        assert_eq!(final_row.0.rowid, 1);
        assert_eq!(
            final_row.0.values,
            vec![Value::Integer(1), Value::Integer(200)]
        );
        assert_eq!(final_row.1, 1);
    }

    #[test]
    fn test_duplicate_row_consolidation() {
        let mut delta = Delta::new();

        // Insert same row twice
        delta.insert(2, vec![Value::Integer(2), Value::Integer(300)]);
        delta.insert(2, vec![Value::Integer(2), Value::Integer(300)]);

        assert_eq!(delta.len(), 2);

        delta.consolidate();
        assert_eq!(delta.len(), 1);

        // Weight should be 2 (sum of both inserts)
        let final_row = &delta.changes[0];
        assert_eq!(final_row.0.rowid, 2);
        assert_eq!(final_row.1, 2);
    }
}

/// Represents an operator in the dataflow graph
#[derive(Debug, Clone)]
pub enum QueryOperator {
    /// Table scan - source of data
    TableScan {
        table_name: String,
        column_names: Vec<String>,
    },

    /// Filter rows based on predicate
    Filter {
        predicate: FilterPredicate,
        input: usize, // Index of input operator
    },

    /// Project columns (select specific columns)
    Project {
        columns: Vec<ProjectColumn>,
        input: usize,
    },

    /// Join two inputs
    Join {
        join_type: JoinType,
        on_column: String,
        left_input: usize,
        right_input: usize,
    },

    /// Aggregate
    Aggregate {
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        input: usize,
    },
}

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
    pub fn from_sql_expr(expr: &turso_sqlite3_parser::ast::Expr) -> crate::Result<Self> {
        use turso_sqlite3_parser::ast::*;

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
    pub fn from_select(select: &turso_sqlite3_parser::ast::Select) -> crate::Result<Self> {
        use turso_sqlite3_parser::ast::*;

        if let OneSelect::Select(select_stmt) = &*select.body.select {
            if let Some(where_clause) = &select_stmt.where_clause {
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

#[derive(Debug, Clone)]
pub struct ProjectColumn {
    /// The original SQL expression (for debugging/fallback)
    pub expr: turso_sqlite3_parser::ast::Expr,
    /// Optional alias for the column
    pub alias: Option<String>,
    /// Compiled expression (handles both trivial columns and complex expressions)
    pub compiled: CompiledExpression,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT(*)"),
            AggregateFunction::Sum(col) => write!(f, "SUM({col})"),
            AggregateFunction::Avg(col) => write!(f, "AVG({col})"),
            AggregateFunction::Min(col) => write!(f, "MIN({col})"),
            AggregateFunction::Max(col) => write!(f, "MAX({col})"),
        }
    }
}

impl AggregateFunction {
    /// Get the default output column name for this aggregate function
    #[inline]
    pub fn default_output_name(&self) -> String {
        self.to_string()
    }

    /// Create an AggregateFunction from a SQL function and its arguments
    /// Returns None if the function is not a supported aggregate
    pub fn from_sql_function(
        func: &crate::function::Func,
        input_column: Option<String>,
    ) -> Option<Self> {
        use crate::function::{AggFunc, Func};

        match func {
            Func::Agg(agg_func) => {
                match agg_func {
                    AggFunc::Count | AggFunc::Count0 => Some(AggregateFunction::Count),
                    AggFunc::Sum => input_column.map(AggregateFunction::Sum),
                    AggFunc::Avg => input_column.map(AggregateFunction::Avg),
                    AggFunc::Min => input_column.map(AggregateFunction::Min),
                    AggFunc::Max => input_column.map(AggregateFunction::Max),
                    _ => None, // Other aggregate functions not yet supported in DBSP
                }
            }
            _ => None, // Not an aggregate function
        }
    }
}

/// Operator DAG (Directed Acyclic Graph)
/// Base trait for incremental operators
pub trait IncrementalOperator: Debug {
    /// Initialize with base data
    fn initialize(&mut self, data: Delta);

    /// Process a delta (incremental update)
    fn process_delta(&mut self, delta: Delta) -> Delta;

    /// Get current accumulated state
    fn get_current_state(&self) -> Delta;

    /// Set computation tracker
    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>);
}

/// Filter operator - filters rows based on predicate
#[derive(Debug)]
pub struct FilterOperator {
    predicate: FilterPredicate,
    current_state: Delta,
    column_names: Vec<String>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
}

impl FilterOperator {
    pub fn new(predicate: FilterPredicate, column_names: Vec<String>) -> Self {
        Self {
            predicate,
            current_state: Delta::new(),
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
    fn initialize(&mut self, data: Delta) {
        // Process initial data through filter
        for (row, weight) in data.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_filter();
            }

            if self.evaluate_predicate(&row.values) {
                self.current_state.changes.push((row, weight));
            }
        }
    }

    fn process_delta(&mut self, delta: Delta) -> Delta {
        let mut output_delta = Delta::new();

        // Process only the delta, not the entire state
        for (row, weight) in delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_filter();
            }

            if self.evaluate_predicate(&row.values) {
                output_delta.changes.push((row.clone(), weight));

                // Update our state
                self.current_state.changes.push((row, weight));
            }
        }

        output_delta
    }

    fn get_current_state(&self) -> Delta {
        self.current_state.clone()
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

/// Project operator - selects/transforms columns
#[derive(Clone)]
pub struct ProjectOperator {
    columns: Vec<ProjectColumn>,
    input_column_names: Vec<String>,
    output_column_names: Vec<String>,
    current_state: Delta,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
    // Internal in-memory connection for expression evaluation
    // Programs are very dependent on having a connection, so give it one.
    //
    // We could in theory pass the current connection, but there are a host of problems with that.
    // For example: during a write transaction, where views are usually updated, we have autocommit
    // on. When the program we are executing calls Halt, it will try to commit the current
    // transaction, which is absolutely incorrect.
    //
    // There are other ways to solve this, but a read-only connection to an empty in-memory
    // database gives us the closest environment we need to execute expressions.
    internal_conn: Arc<Connection>,
}

impl std::fmt::Debug for ProjectOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectOperator")
            .field("columns", &self.columns)
            .field("input_column_names", &self.input_column_names)
            .field("output_column_names", &self.output_column_names)
            .field("current_state", &self.current_state)
            .field("tracker", &self.tracker)
            .finish_non_exhaustive()
    }
}

impl ProjectOperator {
    /// Create a new ProjectOperator from a SELECT statement, extracting projection columns
    pub fn from_select(
        select: &turso_sqlite3_parser::ast::Select,
        input_column_names: Vec<String>,
        schema: &crate::schema::Schema,
    ) -> crate::Result<Self> {
        use turso_sqlite3_parser::ast::*;

        // Set up internal connection for expression evaluation
        let io = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(
            io, ":memory:", false, // no MVCC needed for expression evaluation
            false, // no indexes needed
        )?;
        let internal_conn = db.connect()?;
        // Set to read-only mode and disable auto-commit since we're only evaluating expressions
        internal_conn.query_only.set(true);
        internal_conn.auto_commit.set(false);

        let temp_syms = SymbolTable::new();

        // Extract columns from SELECT statement
        let columns = if let OneSelect::Select(ref select_stmt) = &*select.body.select {
            let mut columns = Vec::new();
            for result_col in &select_stmt.columns {
                match result_col {
                    ResultColumn::Expr(expr, alias) => {
                        let alias_str = if let Some(As::As(alias_name)) = alias {
                            Some(alias_name.as_str().to_string())
                        } else {
                            None
                        };
                        // Try to compile the expression (handles both columns and complex expressions)
                        let compiled = CompiledExpression::compile(
                            expr,
                            &input_column_names,
                            schema,
                            &temp_syms,
                            internal_conn.clone(),
                        )?;
                        columns.push(ProjectColumn {
                            expr: expr.clone(),
                            alias: alias_str,
                            compiled,
                        });
                    }
                    ResultColumn::Star => {
                        // Select all columns - create trivial column references
                        for name in &input_column_names {
                            // Create an Id expression for the column
                            let expr = Expr::Id(Name::Ident(name.clone()));
                            let compiled = CompiledExpression::compile(
                                &expr,
                                &input_column_names,
                                schema,
                                &temp_syms,
                                internal_conn.clone(),
                            )?;
                            columns.push(ProjectColumn {
                                expr,
                                alias: None,
                                compiled,
                            });
                        }
                    }
                    x => {
                        return Err(crate::LimboError::ParseError(format!(
                            "Unsupported {x:?} clause when compiling project operator",
                        )));
                    }
                }
            }

            if columns.is_empty() {
                return Err(crate::LimboError::ParseError(
                    "No columns found when compiling project operator".to_string(),
                ));
            }
            columns
        } else {
            return Err(crate::LimboError::ParseError(
                "Expression is not a valid SELECT expression".to_string(),
            ));
        };

        // Generate output column names based on aliases or expressions
        let output_column_names = columns
            .iter()
            .map(|c| {
                c.alias.clone().unwrap_or_else(|| match &c.expr {
                    Expr::Id(name) => name.as_str().to_string(),
                    Expr::Qualified(table, column) => {
                        format!("{}.{}", table.as_str(), column.as_str())
                    }
                    Expr::DoublyQualified(db, table, column) => {
                        format!("{}.{}.{}", db.as_str(), table.as_str(), column.as_str())
                    }
                    _ => c.expr.to_string(),
                })
            })
            .collect();

        Ok(Self {
            columns,
            input_column_names,
            output_column_names,
            current_state: Delta::new(),
            tracker: None,
            internal_conn,
        })
    }

    /// Get the columns for this projection
    pub fn columns(&self) -> &[ProjectColumn] {
        &self.columns
    }

    fn project_values(&self, values: &[Value]) -> Vec<Value> {
        let mut output = Vec::new();

        for col in &self.columns {
            // Use the internal connection's pager for expression evaluation
            let internal_pager = self.internal_conn.pager.borrow().clone();

            // Execute the compiled expression (handles both columns and complex expressions)
            let result = col
                .compiled
                .execute(values, internal_pager)
                .expect("Failed to execute compiled expression for the Project operator");
            output.push(result);
        }

        output
    }

    fn evaluate_expression(
        &self,
        expr: &turso_sqlite3_parser::ast::Expr,
        values: &[Value],
    ) -> Value {
        match expr {
            Expr::Id(name) => {
                if let Some(idx) = self
                    .input_column_names
                    .iter()
                    .position(|c| c == name.as_str())
                {
                    if let Some(v) = values.get(idx) {
                        return v.clone();
                    }
                }
                Value::Null
            }
            Expr::Literal(lit) => {
                match lit {
                    Literal::Numeric(n) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Value::Integer(i)
                        } else if let Ok(f) = n.parse::<f64>() {
                            Value::Float(f)
                        } else {
                            Value::Null
                        }
                    }
                    Literal::String(s) => {
                        let cleaned = s.trim_matches('\'').trim_matches('"');
                        Value::Text(Text::new(cleaned))
                    }
                    Literal::Null => Value::Null,
                    Literal::Blob(_)
                    | Literal::Keyword(_)
                    | Literal::CurrentDate
                    | Literal::CurrentTime
                    | Literal::CurrentTimestamp => Value::Null, // Not supported yet
                }
            }
            Expr::Binary(left, op, right) => {
                let left_val = self.evaluate_expression(left, values);
                let right_val = self.evaluate_expression(right, values);

                match op {
                    Operator::Add => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                        (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 + b),
                        (Value::Float(a), Value::Integer(b)) => Value::Float(a + *b as f64),
                        _ => Value::Null,
                    },
                    Operator::Subtract => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                        (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 - b),
                        (Value::Float(a), Value::Integer(b)) => Value::Float(a - *b as f64),
                        _ => Value::Null,
                    },
                    Operator::Multiply => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                        (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 * b),
                        (Value::Float(a), Value::Integer(b)) => Value::Float(a * *b as f64),
                        _ => Value::Null,
                    },
                    Operator::Divide => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => {
                            if *b != 0 {
                                Value::Integer(a / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Float(a), Value::Float(b)) => {
                            if *b != 0.0 {
                                Value::Float(a / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Integer(a), Value::Float(b)) => {
                            if *b != 0.0 {
                                Value::Float(*a as f64 / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Float(a), Value::Integer(b)) => {
                            if *b != 0 {
                                Value::Float(a / *b as f64)
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    },
                    _ => Value::Null, // Other operators not supported yet
                }
            }
            Expr::FunctionCall { name, args, .. } => {
                match name.as_str().to_lowercase().as_str() {
                    "hex" => {
                        if let Some(arg_list) = args {
                            if arg_list.len() == 1 {
                                let arg_val = self.evaluate_expression(&arg_list[0], values);
                                match arg_val {
                                    Value::Integer(i) => Value::Text(Text::new(&format!("{i:X}"))),
                                    _ => Value::Null,
                                }
                            } else {
                                Value::Null
                            }
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null, // Other functions not supported yet
                }
            }
            Expr::Parenthesized(inner) => {
                assert!(
                    inner.len() <= 1,
                    "Parenthesized expressions with multiple elements are not supported"
                );
                if !inner.is_empty() {
                    self.evaluate_expression(&inner[0], values)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null, // Other expression types not supported yet
        }
    }
}

impl IncrementalOperator for ProjectOperator {
    fn initialize(&mut self, data: Delta) {
        for (row, weight) in &data.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }

            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            self.current_state.changes.push((projected_row, *weight));
        }
    }

    fn process_delta(&mut self, delta: Delta) -> Delta {
        let mut output_delta = Delta::new();

        for (row, weight) in &delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }

            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);

            output_delta.changes.push((projected_row.clone(), *weight));
            self.current_state.changes.push((projected_row, *weight));
        }

        output_delta
    }

    fn get_current_state(&self) -> Delta {
        self.current_state.clone()
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

/// Join operator - performs incremental joins using DBSP formula
/// ∂(A ⋈ B) = A ⋈ ∂B + ∂A ⋈ B + ∂A ⋈ ∂B
#[derive(Debug)]
pub struct JoinOperator {
    join_type: JoinType,
    pub left_on_column: String,
    pub right_on_column: String,
    left_column_names: Vec<String>,
    right_column_names: Vec<String>,
    // Current accumulated state for both sides
    left_state: Delta,
    right_state: Delta,
    // Index for efficient lookups: column_value_as_string -> vec of row_keys
    // We use String representation of values since Value doesn't implement Hash
    left_index: HashMap<String, Vec<i64>>,
    right_index: HashMap<String, Vec<i64>>,
    // Result state
    current_state: Delta,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
    // For generating unique keys for join results
    next_result_key: i64,
}

impl JoinOperator {
    pub fn new(
        join_type: JoinType,
        left_on_column: String,
        right_on_column: String,
        left_column_names: Vec<String>,
        right_column_names: Vec<String>,
    ) -> Self {
        Self {
            join_type,
            left_on_column,
            right_on_column,
            left_column_names,
            right_column_names,
            left_state: Delta::new(),
            right_state: Delta::new(),
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            current_state: Delta::new(),
            tracker: None,
            next_result_key: 0,
        }
    }

    pub fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }

    /// Build index for a side of the join
    fn build_index(
        state: &Delta,
        column_names: &[String],
        on_column: &str,
    ) -> HashMap<String, Vec<i64>> {
        let mut index = HashMap::new();

        // Find the column index
        let col_idx = column_names.iter().position(|c| c == on_column);
        if col_idx.is_none() {
            return index;
        }
        let col_idx = col_idx.unwrap();

        // Build the index
        for (row, weight) in &state.changes {
            // Include rows with positive weight in the index
            if *weight > 0 {
                if let Some(key_value) = row.values.get(col_idx) {
                    // Convert value to string for indexing
                    let key_str = format!("{key_value:?}");
                    index
                        .entry(key_str)
                        .or_insert_with(Vec::new)
                        .push(row.rowid);
                }
            }
        }

        index
    }

    /// Join two deltas
    fn join_deltas(&self, left_delta: &Delta, right_delta: &Delta, next_key: &mut i64) -> Delta {
        let mut result = Delta::new();

        // Find column indices
        let left_col_idx = self
            .left_column_names
            .iter()
            .position(|c| c == &self.left_on_column)
            .unwrap_or(0);
        let right_col_idx = self
            .right_column_names
            .iter()
            .position(|c| c == &self.right_on_column)
            .unwrap_or(0);

        // For each row in left_delta
        for (left_row, left_weight) in &left_delta.changes {
            // Process both inserts and deletes

            let left_join_value = left_row.values.get(left_col_idx);
            if left_join_value.is_none() {
                continue;
            }
            let left_join_value = left_join_value.unwrap();

            // Look up matching rows in right_delta
            for (right_row, right_weight) in &right_delta.changes {
                // Process both inserts and deletes

                let right_join_value = right_row.values.get(right_col_idx);
                if right_join_value.is_none() {
                    continue;
                }
                let right_join_value = right_join_value.unwrap();

                // Check if values match
                if left_join_value == right_join_value {
                    // Record the join lookup
                    if let Some(tracker) = &self.tracker {
                        tracker.lock().unwrap().record_join_lookup();
                    }

                    // Create joined row
                    let mut joined_values = left_row.values.clone();
                    joined_values.extend(right_row.values.clone());

                    // Generate a unique key for the result
                    let result_key = *next_key;
                    *next_key += 1;

                    let joined_row = HashableRow::new(result_key, joined_values);
                    result
                        .changes
                        .push((joined_row, left_weight * right_weight));
                }
            }
        }

        result
    }

    /// Join a delta with the full state using the index
    fn join_delta_with_state(
        &self,
        delta: &Delta,
        state: &Delta,
        delta_on_left: bool,
        next_key: &mut i64,
    ) -> Delta {
        let mut result = Delta::new();

        let (delta_col_idx, state_col_names) = if delta_on_left {
            (
                self.left_column_names
                    .iter()
                    .position(|c| c == &self.left_on_column)
                    .unwrap_or(0),
                &self.right_column_names,
            )
        } else {
            (
                self.right_column_names
                    .iter()
                    .position(|c| c == &self.right_on_column)
                    .unwrap_or(0),
                &self.left_column_names,
            )
        };

        // Use index for efficient lookup
        let state_index = Self::build_index(
            state,
            state_col_names,
            if delta_on_left {
                &self.right_on_column
            } else {
                &self.left_on_column
            },
        );

        for (delta_row, delta_weight) in &delta.changes {
            // Process both inserts and deletes

            let delta_join_value = delta_row.values.get(delta_col_idx);
            if delta_join_value.is_none() {
                continue;
            }
            let delta_join_value = delta_join_value.unwrap();

            // Use index to find matching rows
            let delta_key_str = format!("{delta_join_value:?}");
            if let Some(matching_keys) = state_index.get(&delta_key_str) {
                for state_key in matching_keys {
                    // Look up in the state - find the row with this rowid
                    let state_row_opt = state
                        .changes
                        .iter()
                        .find(|(row, weight)| row.rowid == *state_key && *weight > 0);

                    if let Some((state_row, state_weight)) = state_row_opt {
                        // Record the join lookup
                        if let Some(tracker) = &self.tracker {
                            tracker.lock().unwrap().record_join_lookup();
                        }

                        // Create joined row
                        let joined_values = if delta_on_left {
                            let mut v = delta_row.values.clone();
                            v.extend(state_row.values.clone());
                            v
                        } else {
                            let mut v = state_row.values.clone();
                            v.extend(delta_row.values.clone());
                            v
                        };

                        let result_key = *next_key;
                        *next_key += 1;

                        let joined_row = HashableRow::new(result_key, joined_values);
                        result
                            .changes
                            .push((joined_row, delta_weight * state_weight));
                    }
                }
            }
        }

        result
    }

    /// Initialize both sides of the join
    pub fn initialize_both(&mut self, left_data: Delta, right_data: Delta) {
        self.left_state = left_data.clone();
        self.right_state = right_data.clone();

        // Build indices
        self.left_index = Self::build_index(
            &self.left_state,
            &self.left_column_names,
            &self.left_on_column,
        );
        self.right_index = Self::build_index(
            &self.right_state,
            &self.right_column_names,
            &self.right_on_column,
        );

        // Perform initial join
        let mut next_key = self.next_result_key;
        self.current_state = self.join_deltas(&self.left_state, &self.right_state, &mut next_key);
        self.next_result_key = next_key;
    }

    /// Process deltas for both sides using DBSP formula
    /// ∂(A ⋈ B) = A ⋈ ∂B + ∂A ⋈ B + ∂A ⋈ ∂B
    pub fn process_both_deltas(&mut self, left_delta: Delta, right_delta: Delta) -> Delta {
        let mut result = Delta::new();
        let mut next_key = self.next_result_key;

        // A ⋈ ∂B (existing left with new right)
        let a_join_db =
            self.join_delta_with_state(&right_delta, &self.left_state, false, &mut next_key);
        result.merge(&a_join_db);

        // ∂A ⋈ B (new left with existing right)
        let da_join_b =
            self.join_delta_with_state(&left_delta, &self.right_state, true, &mut next_key);
        result.merge(&da_join_b);

        // ∂A ⋈ ∂B (new left with new right)
        let da_join_db = self.join_deltas(&left_delta, &right_delta, &mut next_key);
        result.merge(&da_join_db);

        // Update the next key counter
        self.next_result_key = next_key;

        // Update state
        self.left_state.merge(&left_delta);
        self.right_state.merge(&right_delta);
        self.current_state.merge(&result);

        // Rebuild indices if needed
        self.left_index = Self::build_index(
            &self.left_state,
            &self.left_column_names,
            &self.left_on_column,
        );
        self.right_index = Self::build_index(
            &self.right_state,
            &self.right_column_names,
            &self.right_on_column,
        );

        result
    }

    pub fn get_current_state(&self) -> &Delta {
        &self.current_state
    }

    /// Process a delta from the left table only
    pub fn process_left_delta(&mut self, left_delta: Delta) -> Delta {
        let empty_delta = Delta::new();
        self.process_both_deltas(left_delta, empty_delta)
    }

    /// Process a delta from the right table only  
    pub fn process_right_delta(&mut self, right_delta: Delta) -> Delta {
        let empty_delta = Delta::new();
        self.process_both_deltas(empty_delta, right_delta)
    }
}

/// Aggregate operator - performs incremental aggregation with GROUP BY
/// Maintains running totals/counts that are updated incrementally
#[derive(Debug, Clone)]
pub struct AggregateOperator {
    // GROUP BY columns
    group_by: Vec<String>,
    // Aggregate functions to compute
    aggregates: Vec<AggregateFunction>,
    // Column names from input
    pub input_column_names: Vec<String>,
    // Aggregation state: group_key_str -> aggregate values
    // For each group, we store the aggregate results
    // We use String representation of group keys since Value doesn't implement Hash
    group_states: HashMap<String, AggregateState>,
    // Map to keep track of actual group key values for output
    group_key_values: HashMap<String, Vec<Value>>,
    // Current output state as a Delta
    current_state: Delta,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,
}

/// State for a single group's aggregates
#[derive(Debug, Clone)]
struct AggregateState {
    // For COUNT: just the count
    count: i64,
    // For SUM: column_name -> sum value
    sums: HashMap<String, f64>,
    // For AVG: column_name -> (sum, count) for computing average
    avgs: HashMap<String, (f64, i64)>,
    // For MIN: column_name -> min value
    mins: HashMap<String, Value>,
    // For MAX: column_name -> max value
    maxs: HashMap<String, Value>,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            count: 0,
            sums: HashMap::new(),
            avgs: HashMap::new(),
            mins: HashMap::new(),
            maxs: HashMap::new(),
        }
    }

    /// Apply a delta to this aggregate state
    fn apply_delta(
        &mut self,
        values: &[Value],
        weight: isize,
        aggregates: &[AggregateFunction],
        column_names: &[String],
    ) {
        // Update COUNT
        self.count += weight as i64;

        // Update other aggregates
        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    // Already handled above
                }
                AggregateFunction::Sum(col_name) => {
                    if let Some(idx) = column_names.iter().position(|c| c == col_name) {
                        if let Some(val) = values.get(idx) {
                            let num_val = match val {
                                Value::Integer(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => 0.0,
                            };
                            *self.sums.entry(col_name.clone()).or_insert(0.0) +=
                                num_val * weight as f64;
                        }
                    }
                }
                AggregateFunction::Avg(col_name) => {
                    if let Some(idx) = column_names.iter().position(|c| c == col_name) {
                        if let Some(val) = values.get(idx) {
                            let num_val = match val {
                                Value::Integer(i) => *i as f64,
                                Value::Float(f) => *f,
                                _ => 0.0,
                            };
                            let (sum, count) =
                                self.avgs.entry(col_name.clone()).or_insert((0.0, 0));
                            *sum += num_val * weight as f64;
                            *count += weight as i64;
                        }
                    }
                }
                AggregateFunction::Min(col_name) => {
                    // MIN/MAX are more complex for incremental updates
                    // For now, we'll need to recompute from the full state
                    // This is a limitation we can improve later
                    if weight > 0 {
                        // Only update on insert
                        if let Some(idx) = column_names.iter().position(|c| c == col_name) {
                            if let Some(val) = values.get(idx) {
                                self.mins
                                    .entry(col_name.clone())
                                    .and_modify(|existing| {
                                        if val < existing {
                                            *existing = val.clone();
                                        }
                                    })
                                    .or_insert_with(|| val.clone());
                            }
                        }
                    }
                }
                AggregateFunction::Max(col_name) => {
                    if weight > 0 {
                        // Only update on insert
                        if let Some(idx) = column_names.iter().position(|c| c == col_name) {
                            if let Some(val) = values.get(idx) {
                                self.maxs
                                    .entry(col_name.clone())
                                    .and_modify(|existing| {
                                        if val > existing {
                                            *existing = val.clone();
                                        }
                                    })
                                    .or_insert_with(|| val.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Convert aggregate state to output values
    fn to_values(&self, aggregates: &[AggregateFunction]) -> Vec<Value> {
        let mut result = Vec::new();

        for agg in aggregates {
            match agg {
                AggregateFunction::Count => {
                    result.push(Value::Integer(self.count));
                }
                AggregateFunction::Sum(col_name) => {
                    let sum = self.sums.get(col_name).copied().unwrap_or(0.0);
                    // Return as integer if it's a whole number, otherwise as float
                    if sum.fract() == 0.0 {
                        result.push(Value::Integer(sum as i64));
                    } else {
                        result.push(Value::Float(sum));
                    }
                }
                AggregateFunction::Avg(col_name) => {
                    if let Some((sum, count)) = self.avgs.get(col_name) {
                        if *count > 0 {
                            result.push(Value::Float(sum / *count as f64));
                        } else {
                            result.push(Value::Null);
                        }
                    } else {
                        result.push(Value::Null);
                    }
                }
                AggregateFunction::Min(col_name) => {
                    result.push(self.mins.get(col_name).cloned().unwrap_or(Value::Null));
                }
                AggregateFunction::Max(col_name) => {
                    result.push(self.maxs.get(col_name).cloned().unwrap_or(Value::Null));
                }
            }
        }

        result
    }
}

impl AggregateOperator {
    pub fn new(
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        input_column_names: Vec<String>,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            input_column_names,
            group_states: HashMap::new(),
            group_key_values: HashMap::new(),
            current_state: Delta::new(),
            tracker: None,
        }
    }

    pub fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }

    /// Extract group key values from a row
    fn extract_group_key(&self, values: &[Value]) -> Vec<Value> {
        let mut key = Vec::new();

        for group_col in &self.group_by {
            if let Some(idx) = self.input_column_names.iter().position(|c| c == group_col) {
                if let Some(val) = values.get(idx) {
                    key.push(val.clone());
                } else {
                    key.push(Value::Null);
                }
            } else {
                key.push(Value::Null);
            }
        }

        key
    }

    /// Convert group key to string for indexing (since Value doesn't implement Hash)
    fn group_key_to_string(key: &[Value]) -> String {
        key.iter()
            .map(|v| format!("{v:?}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Process a delta and update aggregate state incrementally
    pub fn process_delta(&mut self, delta: Delta) -> Delta {
        let mut output_delta = Delta::new();

        // Track which groups were modified
        let mut modified_groups = HashSet::new();

        // Process each change in the delta
        for (row, weight) in &delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_aggregation();
            }

            // Extract group key
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);
            modified_groups.insert(group_key_str.clone());

            // Store the actual group key values
            self.group_key_values
                .insert(group_key_str.clone(), group_key.clone());

            // Get or create aggregate state for this group
            let state = self
                .group_states
                .entry(group_key_str.clone())
                .or_insert_with(AggregateState::new);

            // Apply the delta to the aggregate state
            state.apply_delta(
                &row.values,
                *weight,
                &self.aggregates,
                &self.input_column_names,
            );
        }

        // Generate output delta for modified groups
        for group_key_str in modified_groups {
            // Get the actual group key values
            let group_key = self
                .group_key_values
                .get(&group_key_str)
                .cloned()
                .unwrap_or_default();

            if let Some(state) = self.group_states.get(&group_key_str) {
                // Build output row: group_by columns + aggregate values
                let mut output_values = group_key.clone();
                output_values.extend(state.to_values(&self.aggregates));

                // Generate a unique key for this group
                // We use a hash of the group key to ensure consistency
                let result_key = group_key_str
                    .bytes()
                    .fold(0i64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as i64));

                // Check if group should be removed (count is 0)
                if state.count > 0 {
                    // Add to output delta with positive weight
                    let output_row = HashableRow::new(result_key, output_values.clone());
                    output_delta.changes.push((output_row.clone(), 1));

                    // Update current state
                    self.current_state.changes.push((output_row, 1));
                } else {
                    // Add to output delta with negative weight (deletion)
                    let output_row = HashableRow::new(result_key, output_values);
                    output_delta.changes.push((output_row.clone(), -1));

                    // Mark for removal in current state
                    self.current_state.changes.push((output_row, -1));
                    self.group_states.remove(&group_key_str);
                    self.group_key_values.remove(&group_key_str);
                }
            }
        }

        // Consolidate current state to handle removals
        self.current_state.consolidate();

        output_delta
    }

    pub fn get_current_state(&self) -> &Delta {
        &self.current_state
    }
}

impl IncrementalOperator for AggregateOperator {
    fn initialize(&mut self, data: Delta) {
        // Process all initial data
        self.process_delta(data);
    }

    fn process_delta(&mut self, delta: Delta) -> Delta {
        self.process_delta(delta)
    }

    fn get_current_state(&self) -> Delta {
        self.current_state.clone()
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Text;
    use crate::Value;
    use std::sync::{Arc, Mutex};

    /// Assert that we're doing incremental work, not full recomputation
    fn assert_incremental(tracker: &ComputationTracker, expected_ops: usize, data_size: usize) {
        assert!(
            tracker.total_computations() <= expected_ops,
            "Expected <= {} operations for incremental update, got {}",
            expected_ops,
            tracker.total_computations()
        );
        assert!(
            tracker.total_computations() < data_size,
            "Computation count {} suggests full recomputation (data size: {})",
            tracker.total_computations(),
            data_size
        );
        assert_eq!(
            tracker.full_scans, 0,
            "Incremental computation should not perform full scans"
        );
    }

    // Join tests
    #[test]
    fn test_join_uses_delta_formula() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create join operator
        let mut join = JoinOperator::new(
            JoinType::Inner,
            "user_id".to_string(),
            "user_id".to_string(),
            vec!["user_id".to_string(), "email".to_string()],
            vec![
                "login_id".to_string(),
                "user_id".to_string(),
                "timestamp".to_string(),
            ],
        );
        join.set_tracker(tracker.clone());

        // Initial data: emails table
        let mut emails = Delta::new();
        emails.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("alice@example.com")),
            ],
        );
        emails.insert(
            2,
            vec![Value::Integer(2), Value::Text(Text::new("bob@example.com"))],
        );

        // Initial data: logins table
        let mut logins = Delta::new();
        logins.insert(
            1,
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(1000)],
        );
        logins.insert(
            2,
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(2000)],
        );

        // Initialize join
        join.initialize_both(emails.clone(), logins.clone());

        // Reset tracker for delta processing
        tracker.lock().unwrap().join_lookups = 0;

        // Add one login for bob (user_id=2)
        let mut delta_logins = Delta::new();
        delta_logins.insert(
            3,
            vec![Value::Integer(3), Value::Integer(2), Value::Integer(3000)],
        );

        // Process delta - should use incremental formula
        let empty_delta = Delta::new();
        let output = join.process_both_deltas(empty_delta, delta_logins);

        // Should have one join result (bob's new login)
        assert_eq!(output.len(), 1);

        // Verify we used index lookups, not nested loops
        // Should have done 1 lookup (finding bob's email for the new login)
        let lookups = tracker.lock().unwrap().join_lookups;
        assert_eq!(lookups, 1, "Should use index lookup, not scan all emails");

        // Verify incremental behavior - we processed only the delta
        let t = tracker.lock().unwrap();
        assert_incremental(&t, 1, 3); // 1 operation for 3 total rows
    }

    #[test]
    fn test_join_maintains_index() {
        // Create join operator
        let mut join = JoinOperator::new(
            JoinType::Inner,
            "id".to_string(),
            "ref_id".to_string(),
            vec!["id".to_string(), "name".to_string()],
            vec!["ref_id".to_string(), "value".to_string()],
        );

        // Initial data
        let mut left = Delta::new();
        left.insert(1, vec![Value::Integer(1), Value::Text(Text::new("A"))]);
        left.insert(2, vec![Value::Integer(2), Value::Text(Text::new("B"))]);

        let mut right = Delta::new();
        right.insert(1, vec![Value::Integer(1), Value::Integer(100)]);

        // Initialize - should build index
        join.initialize_both(left.clone(), right.clone());

        // Verify initial join worked
        let state = join.get_current_state();
        assert_eq!(state.changes.len(), 1); // One match: id=1

        // Add new item to left
        let mut delta_left = Delta::new();
        delta_left.insert(3, vec![Value::Integer(3), Value::Text(Text::new("C"))]);

        // Add matching item to right
        let mut delta_right = Delta::new();
        delta_right.insert(2, vec![Value::Integer(3), Value::Integer(300)]);

        // Process deltas
        let output = join.process_both_deltas(delta_left, delta_right);

        // Should have new join result
        assert_eq!(output.len(), 1);

        // Verify the join result has the expected values
        assert!(!output.changes.is_empty());
        let (result, _weight) = &output.changes[0];
        assert_eq!(result.values.len(), 4); // id, name, ref_id, value
    }

    #[test]
    fn test_join_formula_correctness() {
        // Test the DBSP formula: ∂(A ⋈ B) = A ⋈ ∂B + ∂A ⋈ B + ∂A ⋈ ∂B
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        let mut join = JoinOperator::new(
            JoinType::Inner,
            "x".to_string(),
            "x".to_string(),
            vec!["x".to_string(), "a".to_string()],
            vec!["x".to_string(), "b".to_string()],
        );
        join.set_tracker(tracker.clone());

        // Initial state A
        let mut a = Delta::new();
        a.insert(1, vec![Value::Integer(1), Value::Text(Text::new("a1"))]);
        a.insert(2, vec![Value::Integer(2), Value::Text(Text::new("a2"))]);

        // Initial state B
        let mut b = Delta::new();
        b.insert(1, vec![Value::Integer(1), Value::Text(Text::new("b1"))]);
        b.insert(2, vec![Value::Integer(2), Value::Text(Text::new("b2"))]);

        join.initialize_both(a.clone(), b.clone());

        // Reset tracker
        tracker.lock().unwrap().join_lookups = 0;

        // Delta for A (add x=3)
        let mut delta_a = Delta::new();
        delta_a.insert(3, vec![Value::Integer(3), Value::Text(Text::new("a3"))]);

        // Delta for B (add x=3 and x=1)
        let mut delta_b = Delta::new();
        delta_b.insert(3, vec![Value::Integer(3), Value::Text(Text::new("b3"))]);
        delta_b.insert(4, vec![Value::Integer(1), Value::Text(Text::new("b1_new"))]);

        let output = join.process_both_deltas(delta_a, delta_b);

        // Expected results:
        // A ⋈ ∂B: (1,a1) ⋈ (1,b1_new) = 1 result
        // ∂A ⋈ B: (3,a3) ⋈ nothing = 0 results
        // ∂A ⋈ ∂B: (3,a3) ⋈ (3,b3) = 1 result
        // Total: 2 results
        assert_eq!(output.len(), 2);

        // Verify we're doing incremental work
        let lookups = tracker.lock().unwrap().join_lookups;
        assert!(lookups <= 4, "Should use efficient index lookups");
    }

    // Aggregation tests
    #[test]
    fn test_count_increments_not_recounts() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create COUNT(*) GROUP BY category
        let mut agg = AggregateOperator::new(
            vec!["category".to_string()],
            vec![AggregateFunction::Count],
            vec![
                "item_id".to_string(),
                "category".to_string(),
                "price".to_string(),
            ],
        );
        agg.set_tracker(tracker.clone());

        // Initial: 100 items in 10 categories (10 items each)
        let mut initial = Delta::new();
        for i in 0..100 {
            let category = format!("cat_{}", i / 10);
            initial.insert(
                i,
                vec![
                    Value::Integer(i),
                    Value::Text(Text::new(&category)),
                    Value::Integer(i * 10),
                ],
            );
        }
        agg.initialize(initial);

        // Reset tracker for delta processing
        tracker.lock().unwrap().aggregation_updates = 0;

        // Add one item to category 'cat_0'
        let mut delta = Delta::new();
        delta.insert(
            100,
            vec![
                Value::Integer(100),
                Value::Text(Text::new("cat_0")),
                Value::Integer(1000),
            ],
        );

        let output = agg.process_delta(delta);

        // Should only update one group (cat_0), not recount all groups
        assert_eq!(tracker.lock().unwrap().aggregation_updates, 1);

        // Output should show cat_0 now has count 11
        assert_eq!(output.len(), 1);
        assert!(!output.changes.is_empty());
        let (change_row, _weight) = &output.changes[0];
        assert_eq!(change_row.values[0], Value::Text(Text::new("cat_0")));
        assert_eq!(change_row.values[1], Value::Integer(11));

        // Verify incremental behavior
        let t = tracker.lock().unwrap();
        assert_incremental(&t, 1, 101);
    }

    #[test]
    fn test_sum_updates_incrementally() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create SUM(amount) GROUP BY product
        let mut agg = AggregateOperator::new(
            vec!["product".to_string()],
            vec![AggregateFunction::Sum("amount".to_string())],
            vec![
                "sale_id".to_string(),
                "product".to_string(),
                "amount".to_string(),
            ],
        );
        agg.set_tracker(tracker.clone());

        // Initial sales
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("Widget")),
                Value::Integer(100),
            ],
        );
        initial.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text(Text::new("Gadget")),
                Value::Integer(200),
            ],
        );
        initial.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text(Text::new("Widget")),
                Value::Integer(150),
            ],
        );
        agg.initialize(initial);

        // Check initial state: Widget=250, Gadget=200
        let state = agg.get_current_state();
        let widget_sum = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Text(Text::new("Widget")))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(widget_sum.values[1], Value::Integer(250));

        // Reset tracker
        tracker.lock().unwrap().aggregation_updates = 0;

        // Add sale of 50 for Widget
        let mut delta = Delta::new();
        delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text(Text::new("Widget")),
                Value::Integer(50),
            ],
        );

        let output = agg.process_delta(delta);

        // Should only update Widget group
        assert_eq!(tracker.lock().unwrap().aggregation_updates, 1);
        assert_eq!(output.len(), 1);

        // Widget should now be 300 (250 + 50)
        assert!(!output.changes.is_empty());
        let (change, _weight) = &output.changes[0];
        assert_eq!(change.values[0], Value::Text(Text::new("Widget")));
        assert_eq!(change.values[1], Value::Integer(300));
    }

    #[test]
    fn test_count_and_sum_together() {
        // Test the example from DBSP_ROADMAP: COUNT(*) and SUM(amount) GROUP BY user_id
        let mut agg = AggregateOperator::new(
            vec!["user_id".to_string()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("amount".to_string()),
            ],
            vec![
                "order_id".to_string(),
                "user_id".to_string(),
                "amount".to_string(),
            ],
        );

        // Initial orders
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(100)],
        );
        initial.insert(
            2,
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(200)],
        );
        initial.insert(
            3,
            vec![Value::Integer(3), Value::Integer(2), Value::Integer(150)],
        );
        agg.initialize(initial);

        // Check initial state
        // User 1: count=2, sum=300
        // User 2: count=1, sum=150
        let state = agg.get_current_state();
        assert_eq!(state.changes.len(), 2);

        let user1 = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Integer(1))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(user1.values[1], Value::Integer(2)); // count
        assert_eq!(user1.values[2], Value::Integer(300)); // sum

        let user2 = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Integer(2))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(user2.values[1], Value::Integer(1)); // count
        assert_eq!(user2.values[2], Value::Integer(150)); // sum

        // Add order for user 1
        let mut delta = Delta::new();
        delta.insert(
            4,
            vec![Value::Integer(4), Value::Integer(1), Value::Integer(50)],
        );
        let output = agg.process_delta(delta);

        // Should only update user 1
        assert_eq!(output.len(), 1);
        assert!(!output.changes.is_empty());
        let (change, _weight) = &output.changes[0];
        assert_eq!(change.values[0], Value::Integer(1)); // user_id
        assert_eq!(change.values[1], Value::Integer(3)); // count: 2 + 1
        assert_eq!(change.values[2], Value::Integer(350)); // sum: 300 + 50
    }

    #[test]
    fn test_avg_maintains_sum_and_count() {
        // Test AVG aggregation
        let mut agg = AggregateOperator::new(
            vec!["category".to_string()],
            vec![AggregateFunction::Avg("value".to_string())],
            vec![
                "id".to_string(),
                "category".to_string(),
                "value".to_string(),
            ],
        );

        // Initial data
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("A")),
                Value::Integer(10),
            ],
        );
        initial.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text(Text::new("A")),
                Value::Integer(20),
            ],
        );
        initial.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text(Text::new("B")),
                Value::Integer(30),
            ],
        );
        agg.initialize(initial);

        // Check initial averages
        // Category A: avg = (10 + 20) / 2 = 15
        // Category B: avg = 30 / 1 = 30
        let state = agg.get_current_state();
        let cat_a = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Text(Text::new("A")))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(cat_a.values[1], Value::Float(15.0));

        let cat_b = state
            .changes
            .iter()
            .find(|(c, _)| c.values[0] == Value::Text(Text::new("B")))
            .map(|(c, _)| c)
            .unwrap();
        assert_eq!(cat_b.values[1], Value::Float(30.0));

        // Add value to category A
        let mut delta = Delta::new();
        delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text(Text::new("A")),
                Value::Integer(30),
            ],
        );
        let output = agg.process_delta(delta);

        // Category A avg should now be (10 + 20 + 30) / 3 = 20
        assert!(!output.changes.is_empty());
        let (change, _weight) = &output.changes[0];
        assert_eq!(change.values[0], Value::Text(Text::new("A")));
        assert_eq!(change.values[1], Value::Float(20.0));
    }

    #[test]
    fn test_delete_updates_aggregates() {
        // Test that deletes (negative weights) properly update aggregates
        let mut agg = AggregateOperator::new(
            vec!["category".to_string()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("value".to_string()),
            ],
            vec![
                "id".to_string(),
                "category".to_string(),
                "value".to_string(),
            ],
        );

        // Initial data
        let mut initial = Delta::new();
        initial.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("A")),
                Value::Integer(100),
            ],
        );
        initial.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text(Text::new("A")),
                Value::Integer(200),
            ],
        );
        agg.initialize(initial);

        // Check initial state: count=2, sum=300
        let state = agg.get_current_state();
        assert!(!state.changes.is_empty());
        let (row, _weight) = &state.changes[0];
        assert_eq!(row.values[1], Value::Integer(2)); // count
        assert_eq!(row.values[2], Value::Integer(300)); // sum

        // Delete one row
        let mut delta = Delta::new();
        delta.delete(
            1,
            vec![
                Value::Integer(1),
                Value::Text(Text::new("A")),
                Value::Integer(100),
            ],
        );

        let output = agg.process_delta(delta);

        // Should update to count=1, sum=200
        assert!(!output.changes.is_empty());
        let (change_row, _weight) = &output.changes[0];
        assert_eq!(change_row.values[0], Value::Text(Text::new("A")));
        assert_eq!(change_row.values[1], Value::Integer(1)); // count: 2 - 1
        assert_eq!(change_row.values[2], Value::Integer(200)); // sum: 300 - 100
    }
}
