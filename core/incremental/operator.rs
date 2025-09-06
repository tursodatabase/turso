#![allow(dead_code)]
// Operator DAG for DBSP-style incremental computation
// Based on Feldera DBSP design but adapted for Turso's architecture

use crate::function::{AggFunc, Func};
use crate::incremental::dbsp::Delta;
use crate::incremental::expr_compiler::CompiledExpression;
use crate::incremental::hashable_row::HashableRow;
use crate::storage::btree::{BTreeCursor, BTreeKey};
use crate::types::{IOResult, SeekKey, SeekOp, SeekResult, Text};
use crate::{
    return_and_restore_if_io, return_if_io, Connection, Database, Result, SymbolTable, Value,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Display};
use std::sync::{Arc, Mutex};
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::{As, Expr, Literal, Name, OneSelect, Operator, ResultColumn};

#[derive(Debug)]
pub enum ReadRecord {
    GetRecord,
    Done { state: Option<AggregateState> },
}

impl ReadRecord {
    fn new() -> Self {
        ReadRecord::GetRecord
    }

    fn read_record(
        &mut self,
        key: SeekKey,
        aggregates: &[AggregateFunction],
        cursor: &mut BTreeCursor,
    ) -> Result<IOResult<Option<AggregateState>>> {
        loop {
            match self {
                ReadRecord::GetRecord => {
                    let res = return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    if !matches!(res, SeekResult::Found) {
                        *self = ReadRecord::Done { state: None };
                    } else {
                        let record = return_if_io!(cursor.record());
                        let r = record.ok_or_else(|| {
                            crate::LimboError::InternalError(format!(
                                "Found key {key:?} in aggregate storage but could not read record"
                            ))
                        })?;
                        let values = r.get_values();
                        let blob = values[1].to_owned();

                        let (state, _group_key) = match blob {
                            Value::Blob(blob) => AggregateState::from_blob(&blob, aggregates)
                                .ok_or_else(|| {
                                    crate::LimboError::InternalError(format!(
                                        "Cannot deserialize aggregate state {blob:?}",
                                    ))
                                }),
                            _ => Err(crate::LimboError::ParseError(
                                "Value in aggregator not blob".to_string(),
                            )),
                        }?;
                        *self = ReadRecord::Done { state: Some(state) }
                    }
                }
                ReadRecord::Done { state } => return Ok(IOResult::Done(state.clone())),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum WriteRecord {
    GetRecord,
    Delete { final_weight: isize },
    Insert { final_weight: isize },
    Done,
}
impl WriteRecord {
    fn new() -> Self {
        WriteRecord::GetRecord
    }

    fn write_record(
        &mut self,
        key: SeekKey,
        record: HashableRow,
        weight: isize,
        cursor: &mut BTreeCursor,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                WriteRecord::GetRecord => {
                    let res = return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    if !matches!(res, SeekResult::Found) {
                        *self = WriteRecord::Insert {
                            final_weight: weight,
                        };
                    } else {
                        let existing_record = return_if_io!(cursor.record());
                        let r = existing_record.ok_or_else(|| {
                            crate::LimboError::InternalError(format!(
                                "Found key {key:?} in aggregate storage but could not read record"
                            ))
                        })?;
                        let values = r.get_values();
                        // values[2] should contain the weight
                        let existing_weight = match values[2].to_owned() {
                            Value::Integer(w) => w as isize,
                            _ => {
                                return Err(crate::LimboError::InternalError(format!(
                                    "Invalid weight value in aggregate storage for key {key:?}"
                                )))
                            }
                        };
                        let final_weight = existing_weight + weight;
                        if final_weight <= 0 {
                            *self = WriteRecord::Delete { final_weight }
                        } else {
                            *self = WriteRecord::Insert { final_weight }
                        }
                    }
                }
                WriteRecord::Delete { final_weight: _ } => {
                    let res = return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    if !matches!(res, SeekResult::Found) {
                        return Err(crate::LimboError::InternalError(format!(
                            "record not found for {key:?}, but we had just GetRecord! Should not be possible"
                        )));
                    }
                    // Done - row was deleted and weights cancel out.
                    // If we iniated the delete we will complete, so Done has to be set
                    // before so we don't come back here.
                    *self = WriteRecord::Done;
                    return_if_io!(cursor.delete());
                }
                WriteRecord::Insert { final_weight } => {
                    return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    // Build the key and insert the record
                    let key_i64 = match key {
                        SeekKey::TableRowId(id) => id,
                        _ => {
                            return Err(crate::LimboError::InternalError(
                                "Expected TableRowId for aggregate storage".to_string(),
                            ))
                        }
                    };
                    // Create the record values: key, blob, weight
                    let record_values = vec![
                        Value::Integer(key_i64),
                        record.values[0].clone(), // The blob with serialized state
                        Value::Integer(*final_weight as i64),
                    ];

                    // Create an ImmutableRecord from the values
                    let immutable_record = crate::types::ImmutableRecord::from_values(
                        &record_values,
                        record_values.len(),
                    );
                    let btree_key = BTreeKey::new_table_rowid(key_i64, Some(&immutable_record));

                    *self = WriteRecord::Done;
                    return_if_io!(cursor.insert(&btree_key));
                }
                WriteRecord::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

type ComputedStates = HashMap<String, (Vec<Value>, AggregateState)>; // group_key_str -> (group_key, state)
#[derive(Debug)]
enum AggregateCommitState {
    Idle,
    Eval {
        eval_state: EvalState,
    },
    PersistDelta {
        delta: Delta,
        computed_states: ComputedStates,
        current_idx: usize,
        write_record: WriteRecord,
    },
    Done {
        delta: Delta,
    },
    Invalid,
}

// eval() has uncommitted data, so it can't be a member attribute of the Operator.
// The state has to be kept by the caller
#[derive(Debug)]
pub enum EvalState {
    Uninitialized,
    Init {
        delta: Delta,
    },
    FetchData {
        delta: Delta, // Keep original delta for merge operation
        current_idx: usize,
        groups_to_read: Vec<(String, Vec<Value>)>, // Changed to Vec for index-based access
        existing_groups: HashMap<String, AggregateState>,
        old_values: HashMap<String, Vec<Value>>,
        read_record_state: Box<ReadRecord>,
    },
    Done,
}

impl From<Delta> for EvalState {
    fn from(delta: Delta) -> Self {
        EvalState::Init { delta }
    }
}

impl EvalState {
    fn from_delta(delta: Delta) -> Self {
        Self::Init { delta }
    }

    fn delta_ref(&self) -> &Delta {
        match self {
            EvalState::Init { delta } => delta,
            _ => panic!("delta_ref() can only be called when in Init state",),
        }
    }
    pub fn extract_delta(&mut self) -> Delta {
        match self {
            EvalState::Init { delta } => {
                let extracted = std::mem::take(delta);
                *self = EvalState::Uninitialized;
                extracted
            }
            _ => panic!("extract_delta() can only be called when in Init state"),
        }
    }

    fn advance(&mut self, groups_to_read: BTreeMap<String, Vec<Value>>) {
        let delta = match self {
            EvalState::Init { delta } => std::mem::take(delta),
            _ => panic!("advance() can only be called when in Init state, current state: {self:?}"),
        };

        let _ = std::mem::replace(
            self,
            EvalState::FetchData {
                delta,
                current_idx: 0,
                groups_to_read: groups_to_read.into_iter().collect(), // Convert BTreeMap to Vec
                existing_groups: HashMap::new(),
                old_values: HashMap::new(),
                read_record_state: Box::new(ReadRecord::new()),
            },
        );
    }
    fn process_delta(
        &mut self,
        operator: &mut AggregateOperator,
        cursor: &mut BTreeCursor,
    ) -> Result<IOResult<(Delta, ComputedStates)>> {
        loop {
            match self {
                EvalState::Uninitialized => {
                    panic!("Cannot process_delta with Uninitialized state");
                }
                EvalState::Init { .. } => {
                    panic!("State machine not supposed to reach the init state! advance() should have been called");
                }
                EvalState::FetchData {
                    delta,
                    current_idx,
                    groups_to_read,
                    existing_groups,
                    old_values,
                    read_record_state,
                } => {
                    if *current_idx >= groups_to_read.len() {
                        // All groups processed, compute final output
                        let result =
                            operator.merge_delta_with_existing(delta, existing_groups, old_values);
                        *self = EvalState::Done;
                        return Ok(IOResult::Done(result));
                    } else {
                        // Get the current group to read
                        let (group_key_str, group_key) = &groups_to_read[*current_idx];

                        let seek_key = operator.generate_storage_key(group_key_str);
                        let key = SeekKey::TableRowId(seek_key);

                        let state = return_if_io!(read_record_state.read_record(
                            key,
                            &operator.aggregates,
                            cursor
                        ));

                        // Anything that mutates state has to happen after return_if_io!
                        // Unfortunately there's no good way to enforce that without turning
                        // this into a hot mess of mem::takes.
                        if let Some(state) = state {
                            let mut old_row = group_key.clone();
                            old_row.extend(state.to_values(&operator.aggregates));
                            old_values.insert(group_key_str.clone(), old_row);
                            existing_groups.insert(group_key_str.clone(), state.clone());
                        }

                        // All attributes mutated in place.
                        *current_idx += 1;
                        *read_record_state = Box::new(ReadRecord::new());
                    }
                }
                EvalState::Done => {
                    return Ok(IOResult::Done((Delta::new(), HashMap::new())));
                }
            }
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct ProjectColumn {
    /// The original SQL expression (for debugging/fallback)
    pub expr: turso_parser::ast::Expr,
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

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum(String),
    Avg(String),
    // MIN and MAX are not supported - see comment in compiler.rs for explanation
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT(*)"),
            AggregateFunction::Sum(col) => write!(f, "SUM({col})"),
            AggregateFunction::Avg(col) => write!(f, "AVG({col})"),
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
        match func {
            Func::Agg(agg_func) => {
                match agg_func {
                    AggFunc::Count | AggFunc::Count0 => Some(AggregateFunction::Count),
                    AggFunc::Sum => input_column.map(AggregateFunction::Sum),
                    AggFunc::Avg => input_column.map(AggregateFunction::Avg),
                    // MIN and MAX are not supported in incremental views - see compiler.rs
                    AggFunc::Min | AggFunc::Max => None,
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
    /// Evaluate the operator with a state, without modifying internal state
    /// This is used during query execution to compute results
    /// May need to read from storage to get current state (e.g., for aggregates)
    ///
    /// # Arguments
    /// * `state` - The evaluation state (may be in progress from a previous I/O operation)
    /// * `cursor` - Cursor for reading operator state from storage
    ///
    /// # Returns
    /// The output delta from the evaluation
    fn eval(&mut self, state: &mut EvalState, cursor: &mut BTreeCursor) -> Result<IOResult<Delta>>;

    /// Commit a delta to the operator's internal state and return the output
    /// This is called when a transaction commits, making changes permanent
    /// Returns the output delta (what downstream operators should see)
    /// The cursor parameter is for operators that need to persist state
    fn commit(&mut self, delta: Delta, cursor: &mut BTreeCursor) -> Result<IOResult<Delta>>;

    /// Set computation tracker
    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>);
}

/// Input operator - passes through input data unchanged
/// This operator is used for input nodes in the circuit to provide a uniform interface
#[derive(Debug)]
pub struct InputOperator {
    name: String,
}

impl InputOperator {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl IncrementalOperator for InputOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursor: &mut BTreeCursor,
    ) -> Result<IOResult<Delta>> {
        match state {
            EvalState::Init { delta } => {
                let output = std::mem::take(delta);
                *state = EvalState::Done;
                Ok(IOResult::Done(output))
            }
            _ => unreachable!(
                "InputOperator doesn't execute the state machine. Should be in Init state"
            ),
        }
    }

    fn commit(&mut self, delta: Delta, _cursor: &mut BTreeCursor) -> Result<IOResult<Delta>> {
        // Input operator passes through the delta unchanged during commit
        Ok(IOResult::Done(delta))
    }

    fn set_tracker(&mut self, _tracker: Arc<Mutex<ComputationTracker>>) {
        // Input operator doesn't need tracking
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
        _cursor: &mut BTreeCursor,
    ) -> Result<IOResult<Delta>> {
        let delta = match state {
            EvalState::Init { delta } => std::mem::take(delta),
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

    fn commit(&mut self, delta: Delta, _cursor: &mut BTreeCursor) -> Result<IOResult<Delta>> {
        let mut output_delta = Delta::new();

        // Commit the delta to our internal state
        // Only pass through and track rows that satisfy the filter predicate
        for (row, weight) in delta.changes {
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

/// Project operator - selects/transforms columns
#[derive(Clone)]
pub struct ProjectOperator {
    columns: Vec<ProjectColumn>,
    input_column_names: Vec<String>,
    output_column_names: Vec<String>,
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
            .field("tracker", &self.tracker)
            .finish_non_exhaustive()
    }
}

impl ProjectOperator {
    /// Create a new ProjectOperator from a SELECT statement, extracting projection columns
    pub fn from_select(
        select: &turso_parser::ast::Select,
        input_column_names: Vec<String>,
        schema: &crate::schema::Schema,
    ) -> crate::Result<Self> {
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
        let columns = if let OneSelect::Select {
            columns: ref select_columns,
            ..
        } = &select.body.select
        {
            let mut columns = Vec::new();
            for result_col in select_columns {
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
                            expr: (**expr).clone(),
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
            tracker: None,
            internal_conn,
        })
    }

    /// Create a ProjectOperator from pre-compiled expressions
    pub fn from_compiled(
        compiled_exprs: Vec<CompiledExpression>,
        aliases: Vec<Option<String>>,
        input_column_names: Vec<String>,
        output_column_names: Vec<String>,
    ) -> crate::Result<Self> {
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

        // Create ProjectColumn structs from compiled expressions
        let columns: Vec<ProjectColumn> = compiled_exprs
            .into_iter()
            .zip(aliases)
            .map(|(compiled, alias)| ProjectColumn {
                // Create a placeholder AST expression since we already have the compiled version
                expr: turso_parser::ast::Expr::Literal(turso_parser::ast::Literal::Null),
                alias,
                compiled,
            })
            .collect();

        Ok(Self {
            columns,
            input_column_names,
            output_column_names,
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

    fn evaluate_expression(&self, expr: &turso_parser::ast::Expr, values: &[Value]) -> Value {
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
                let name_bytes = name.as_str().as_bytes();
                match_ignore_ascii_case!(match name_bytes {
                    b"hex" => {
                        if args.len() == 1 {
                            let arg_val = self.evaluate_expression(&args[0], values);
                            match arg_val {
                                Value::Integer(i) => Value::Text(Text::new(&format!("{i:X}"))),
                                _ => Value::Null,
                            }
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null, // Other functions not supported yet
                })
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
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursor: &mut BTreeCursor,
    ) -> Result<IOResult<Delta>> {
        let delta = match state {
            EvalState::Init { delta } => std::mem::take(delta),
            _ => unreachable!(
                "ProjectOperator doesn't execute the state machine. Should be in Init state"
            ),
        };

        let mut output_delta = Delta::new();

        for (row, weight) in delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }

            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            output_delta.changes.push((projected_row, weight));
        }

        *state = EvalState::Done;
        Ok(IOResult::Done(output_delta))
    }

    fn commit(&mut self, delta: Delta, _cursor: &mut BTreeCursor) -> Result<IOResult<Delta>> {
        let mut output_delta = Delta::new();

        // Commit the delta to our internal state and build output
        for (row, weight) in &delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_project();
            }
            let projected = self.project_values(&row.values);
            let projected_row = HashableRow::new(row.rowid, projected);
            output_delta.changes.push((projected_row, *weight));
        }

        Ok(crate::types::IOResult::Done(output_delta))
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

/// Aggregate operator - performs incremental aggregation with GROUP BY
/// Maintains running totals/counts that are updated incrementally
///
/// Note that the AggregateOperator essentially implements a ZSet, even
/// though the ZSet structure is never used explicitly. The on-disk btree
/// plays the role of the set!
#[derive(Debug)]
pub struct AggregateOperator {
    // Unique operator ID for indexing in persistent storage
    operator_id: usize,
    // GROUP BY columns
    group_by: Vec<String>,
    // Aggregate functions to compute
    aggregates: Vec<AggregateFunction>,
    // Column names from input
    pub input_column_names: Vec<String>,
    tracker: Option<Arc<Mutex<ComputationTracker>>>,

    // State machine for commit operation
    commit_state: AggregateCommitState,
}

/// State for a single group's aggregates
#[derive(Debug, Clone)]
pub struct AggregateState {
    // For COUNT: just the count
    count: i64,
    // For SUM: column_name -> sum value
    sums: HashMap<String, f64>,
    // For AVG: column_name -> (sum, count) for computing average
    avgs: HashMap<String, (f64, i64)>,
    // MIN/MAX are not supported - they require O(n) storage overhead for handling deletions
    // correctly. See comment in apply_delta() for details.
}

impl AggregateState {
    fn new() -> Self {
        Self {
            count: 0,
            sums: HashMap::new(),
            avgs: HashMap::new(),
        }
    }

    // Serialize the aggregate state to a binary blob including group key values
    // The reason we serialize it like this, instead of just writing the actual values, is that
    // The same table may have different aggregators in the circuit. They will all have different
    // columns.
    fn to_blob(&self, aggregates: &[AggregateFunction], group_key: &[Value]) -> Vec<u8> {
        let mut blob = Vec::new();

        // Write version byte for future compatibility
        blob.push(1u8);

        // Write number of group key values
        blob.extend_from_slice(&(group_key.len() as u32).to_le_bytes());

        // Write each group key value
        for value in group_key {
            // Write value type tag
            match value {
                Value::Null => blob.push(0u8),
                Value::Integer(i) => {
                    blob.push(1u8);
                    blob.extend_from_slice(&i.to_le_bytes());
                }
                Value::Float(f) => {
                    blob.push(2u8);
                    blob.extend_from_slice(&f.to_le_bytes());
                }
                Value::Text(s) => {
                    blob.push(3u8);
                    let text_str = s.as_str();
                    let bytes = text_str.as_bytes();
                    blob.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    blob.extend_from_slice(bytes);
                }
                Value::Blob(b) => {
                    blob.push(4u8);
                    blob.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    blob.extend_from_slice(b);
                }
            }
        }

        // Write count as 8 bytes (little-endian)
        blob.extend_from_slice(&self.count.to_le_bytes());

        // Write each aggregate's state
        for agg in aggregates {
            match agg {
                AggregateFunction::Sum(col_name) => {
                    let sum = self.sums.get(col_name).copied().unwrap_or(0.0);
                    blob.extend_from_slice(&sum.to_le_bytes());
                }
                AggregateFunction::Avg(col_name) => {
                    let (sum, count) = self.avgs.get(col_name).copied().unwrap_or((0.0, 0));
                    blob.extend_from_slice(&sum.to_le_bytes());
                    blob.extend_from_slice(&count.to_le_bytes());
                }
                AggregateFunction::Count => {
                    // Count is already written above
                }
            }
        }

        blob
    }

    /// Deserialize aggregate state from a binary blob
    /// Returns the aggregate state and the group key values
    fn from_blob(blob: &[u8], aggregates: &[AggregateFunction]) -> Option<(Self, Vec<Value>)> {
        let mut cursor = 0;

        // Check version byte
        if blob.get(cursor) != Some(&1u8) {
            return None;
        }
        cursor += 1;

        // Read number of group key values
        let num_group_keys =
            u32::from_le_bytes(blob.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
        cursor += 4;

        // Read group key values
        let mut group_key = Vec::new();
        for _ in 0..num_group_keys {
            let value_type = *blob.get(cursor)?;
            cursor += 1;

            let value = match value_type {
                0 => Value::Null,
                1 => {
                    let i = i64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    Value::Integer(i)
                }
                2 => {
                    let f = f64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    Value::Float(f)
                }
                3 => {
                    let len =
                        u32::from_le_bytes(blob.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                    cursor += 4;
                    let bytes = blob.get(cursor..cursor + len)?;
                    cursor += len;
                    let text_str = std::str::from_utf8(bytes).ok()?;
                    Value::Text(text_str.to_string().into())
                }
                4 => {
                    let len =
                        u32::from_le_bytes(blob.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                    cursor += 4;
                    let bytes = blob.get(cursor..cursor + len)?;
                    cursor += len;
                    Value::Blob(bytes.to_vec())
                }
                _ => return None,
            };
            group_key.push(value);
        }

        // Read count
        let count = i64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
        cursor += 8;

        let mut state = Self::new();
        state.count = count;

        // Read each aggregate's state
        for agg in aggregates {
            match agg {
                AggregateFunction::Sum(col_name) => {
                    let sum = f64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    state.sums.insert(col_name.clone(), sum);
                }
                AggregateFunction::Avg(col_name) => {
                    let sum = f64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    let count = i64::from_le_bytes(blob.get(cursor..cursor + 8)?.try_into().ok()?);
                    cursor += 8;
                    state.avgs.insert(col_name.clone(), (sum, count));
                }
                AggregateFunction::Count => {
                    // Count was already read above
                }
            }
        }

        Some((state, group_key))
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
            }
        }

        result
    }
}

impl AggregateOperator {
    pub fn new(
        operator_id: usize,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        input_column_names: Vec<String>,
    ) -> Self {
        Self {
            operator_id,
            group_by,
            aggregates,
            input_column_names,
            tracker: None,
            commit_state: AggregateCommitState::Idle,
        }
    }

    fn eval_internal(
        &mut self,
        state: &mut EvalState,
        cursor: &mut BTreeCursor,
    ) -> Result<IOResult<(Delta, ComputedStates)>> {
        match state {
            EvalState::Uninitialized => {
                panic!("Cannot eval AggregateOperator with Uninitialized state");
            }
            EvalState::Init { delta } => {
                if delta.changes.is_empty() {
                    *state = EvalState::Done;
                    return Ok(IOResult::Done((Delta::new(), HashMap::new())));
                }

                let mut groups_to_read = BTreeMap::new();
                for (row, _weight) in &delta.changes {
                    // Extract group key using cloned fields
                    let group_key = self.extract_group_key(&row.values);
                    let group_key_str = Self::group_key_to_string(&group_key);
                    groups_to_read.insert(group_key_str, group_key);
                }
                state.advance(groups_to_read);
            }
            EvalState::FetchData { .. } => {
                // Already in progress, continue processing on process_delta below.
            }
            EvalState::Done => {
                panic!("unreachable state! should have returned");
            }
        }

        // Process the delta through the state machine
        let result = return_if_io!(state.process_delta(self, cursor));
        Ok(IOResult::Done(result))
    }

    fn merge_delta_with_existing(
        &mut self,
        delta: &Delta,
        existing_groups: &mut HashMap<String, AggregateState>,
        old_values: &mut HashMap<String, Vec<Value>>,
    ) -> (Delta, HashMap<String, (Vec<Value>, AggregateState)>) {
        let mut output_delta = Delta::new();
        let mut temp_keys: HashMap<String, Vec<Value>> = HashMap::new();

        // Process each change in the delta
        for (row, weight) in &delta.changes {
            if let Some(tracker) = &self.tracker {
                tracker.lock().unwrap().record_aggregation();
            }

            // Extract group key
            let group_key = self.extract_group_key(&row.values);
            let group_key_str = Self::group_key_to_string(&group_key);

            let state = existing_groups
                .entry(group_key_str.clone())
                .or_insert_with(AggregateState::new);

            temp_keys.insert(group_key_str.clone(), group_key.clone());

            // Apply the delta to the temporary state
            state.apply_delta(
                &row.values,
                *weight,
                &self.aggregates,
                &self.input_column_names,
            );
        }

        // Generate output delta from temporary states and collect final states
        let mut final_states = HashMap::new();

        for (group_key_str, state) in existing_groups {
            let group_key = temp_keys.get(group_key_str).cloned().unwrap_or_default();

            // Generate a unique rowid for this group
            let result_key = self.generate_group_rowid(group_key_str);

            if let Some(old_row_values) = old_values.get(group_key_str) {
                let old_row = HashableRow::new(result_key, old_row_values.clone());
                output_delta.changes.push((old_row, -1));
            }

            // Always store the state for persistence (even if count=0, we need to delete it)
            final_states.insert(group_key_str.clone(), (group_key.clone(), state.clone()));

            // Only include groups with count > 0 in the output delta
            if state.count > 0 {
                // Build output row: group_by columns + aggregate values
                let mut output_values = group_key.clone();
                output_values.extend(state.to_values(&self.aggregates));

                let output_row = HashableRow::new(result_key, output_values);
                output_delta.changes.push((output_row, 1));
            }
        }
        (output_delta, final_states)
    }

    pub fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }

    /// Generate a rowid for a group
    /// For no GROUP BY: always returns 0
    /// For GROUP BY: returns a hash of the group key string
    fn generate_group_rowid(&self, group_key_str: &str) -> i64 {
        if self.group_by.is_empty() {
            0
        } else {
            group_key_str
                .bytes()
                .fold(0i64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as i64))
        }
    }

    /// Generate the composite key for BTree storage
    /// Combines operator_id and group hash
    fn generate_storage_key(&self, group_key_str: &str) -> i64 {
        let group_hash = self.generate_group_rowid(group_key_str);
        (self.operator_id as i64) << 32 | (group_hash & 0xFFFFFFFF)
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

    fn seek_key_from_str(&self, group_key_str: &str) -> SeekKey {
        // Calculate the composite key for seeking
        let key_i64 = self.generate_storage_key(group_key_str);
        SeekKey::TableRowId(key_i64)
    }

    fn seek_key(&self, row: HashableRow) -> SeekKey {
        // Extract group key for first row
        let group_key = self.extract_group_key(&row.values);
        let group_key_str = Self::group_key_to_string(&group_key);
        self.seek_key_from_str(&group_key_str)
    }
}

impl IncrementalOperator for AggregateOperator {
    fn eval(&mut self, state: &mut EvalState, cursor: &mut BTreeCursor) -> Result<IOResult<Delta>> {
        let (delta, _) = return_if_io!(self.eval_internal(state, cursor));
        Ok(IOResult::Done(delta))
    }

    fn commit(&mut self, delta: Delta, cursor: &mut BTreeCursor) -> Result<IOResult<Delta>> {
        loop {
            // Note: because we std::mem::replace here (without it, the borrow checker goes nuts,
            // because we call self.eval_interval, which requires a mutable borrow), we have to
            // restore the state if we return I/O. So we can't use return_if_io!
            let mut state =
                std::mem::replace(&mut self.commit_state, AggregateCommitState::Invalid);
            match &mut state {
                AggregateCommitState::Invalid => {
                    panic!("Reached invalid state! State was replaced, and not replaced back");
                }
                AggregateCommitState::Idle => {
                    let eval_state = EvalState::from_delta(delta.clone());
                    self.commit_state = AggregateCommitState::Eval { eval_state };
                }
                AggregateCommitState::Eval { ref mut eval_state } => {
                    let (output_delta, computed_states) = return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        self.eval_internal(eval_state, cursor)
                    );
                    self.commit_state = AggregateCommitState::PersistDelta {
                        delta: output_delta,
                        computed_states,
                        current_idx: 0,
                        write_record: WriteRecord::new(),
                    };
                }
                AggregateCommitState::PersistDelta {
                    delta,
                    computed_states,
                    current_idx,
                    write_record,
                } => {
                    let states_vec: Vec<_> = computed_states.iter().collect();

                    if *current_idx >= states_vec.len() {
                        self.commit_state = AggregateCommitState::Done {
                            delta: delta.clone(),
                        };
                    } else {
                        let (group_key_str, (group_key, agg_state)) = states_vec[*current_idx];

                        let seek_key = self.seek_key_from_str(group_key_str);

                        // Determine weight: -1 to delete (cancels existing weight=1), 1 to insert/update
                        let weight = if agg_state.count == 0 { -1 } else { 1 };

                        // Serialize the aggregate state with group key (even for deletion, we need a row)
                        let state_blob = agg_state.to_blob(&self.aggregates, group_key);
                        let blob_row = HashableRow::new(0, vec![Value::Blob(state_blob)]);

                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            write_record.write_record(seek_key, blob_row, weight, cursor)
                        );

                        let delta = std::mem::take(delta);
                        let computed_states = std::mem::take(computed_states);

                        self.commit_state = AggregateCommitState::PersistDelta {
                            delta,
                            computed_states,
                            current_idx: *current_idx + 1,
                            write_record: WriteRecord::new(), // Reset for next write
                        };
                    }
                }
                AggregateCommitState::Done { delta } => {
                    self.commit_state = AggregateCommitState::Idle;
                    let delta = std::mem::take(delta);
                    return Ok(IOResult::Done(delta));
                }
            }
        }
    }

    fn set_tracker(&mut self, tracker: Arc<Mutex<ComputationTracker>>) {
        self.tracker = Some(tracker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pager::CreateBTreeFlags;
    use crate::types::Text;
    use crate::util::IOExt;
    use crate::Value;
    use crate::{Database, MemoryIO, IO};
    use std::sync::{Arc, Mutex};

    /// Create a test pager for operator tests
    fn create_test_pager() -> (std::rc::Rc<crate::Pager>, usize) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", false, false).unwrap();
        let conn = db.connect().unwrap();

        let pager = conn.pager.borrow().clone();

        // Allocate page 1 first (database header)
        let _ = pager.io.block(|| pager.allocate_page1());

        // Properly create a BTree for aggregate state using the pager API
        let root_page_id = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .expect("Failed to create BTree for aggregate state")
            as usize;

        (pager, root_page_id)
    }

    /// Read the current state from the BTree (for testing)
    /// Returns a Delta with all the current aggregate values
    fn get_current_state_from_btree(
        agg: &AggregateOperator,
        pager: &std::rc::Rc<crate::Pager>,
        cursor: &mut BTreeCursor,
    ) -> Delta {
        let mut result = Delta::new();

        // Rewind to start of table
        pager.io.block(|| cursor.rewind()).unwrap();

        loop {
            // Check if cursor is empty (no more rows)
            if cursor.is_empty() {
                break;
            }

            // Get the record at this position
            let record = pager
                .io
                .block(|| cursor.record())
                .unwrap()
                .unwrap()
                .to_owned();

            let values_ref = record.get_values();
            let values: Vec<Value> = values_ref.into_iter().map(|x| x.to_owned()).collect();

            // Check if this record belongs to our operator
            if let Some(Value::Integer(key)) = values.first() {
                let operator_part = (key >> 32) as usize;

                // Skip if not our operator
                if operator_part != agg.operator_id {
                    pager.io.block(|| cursor.next()).unwrap();
                    continue;
                }

                // Get the blob data
                if let Some(Value::Blob(blob)) = values.get(1) {
                    // Deserialize the state
                    if let Some((state, group_key)) =
                        AggregateState::from_blob(blob, &agg.aggregates)
                    {
                        // Should not have made it this far.
                        assert!(state.count != 0);
                        // Build output row: group_by columns + aggregate values
                        let mut output_values = group_key.clone();
                        output_values.extend(state.to_values(&agg.aggregates));

                        let group_key_str = AggregateOperator::group_key_to_string(&group_key);
                        let rowid = agg.generate_group_rowid(&group_key_str);

                        let output_row = HashableRow::new(rowid, output_values);
                        result.changes.push((output_row, 1));
                    }
                }
            }

            pager.io.block(|| cursor.next()).unwrap();
        }

        result.consolidate();
        result
    }

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

    // Aggregate tests
    #[test]
    fn test_aggregate_incremental_update_emits_retraction() {
        // This test verifies that when an aggregate value changes,
        // the operator emits both a retraction (-1) of the old value
        // and an insertion (+1) of the new value.

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        // Create an aggregate operator for SUM(age) with no GROUP BY
        let mut agg = AggregateOperator::new(
            1,      // operator_id for testing
            vec![], // No GROUP BY
            vec![AggregateFunction::Sum("age".to_string())],
            vec!["id".to_string(), "name".to_string(), "age".to_string()],
        );

        // Initial data: 3 users
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".to_string().into()),
                Value::Integer(25),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".to_string().into()),
                Value::Integer(30),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".to_string().into()),
                Value::Integer(35),
            ],
        );

        // Initialize with initial data
        pager
            .io
            .block(|| agg.commit(initial_delta.clone(), &mut cursor))
            .unwrap();

        // Verify initial state: SUM(age) = 25 + 30 + 35 = 90
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes.len(), 1, "Should have one aggregate row");
        let (row, weight) = &state.changes[0];
        assert_eq!(*weight, 1, "Aggregate row should have weight 1");
        assert_eq!(row.values[0], Value::Float(90.0), "SUM should be 90");

        // Now add a new user (incremental update)
        let mut update_delta = Delta::new();
        update_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".to_string().into()),
                Value::Integer(40),
            ],
        );

        // Process the incremental update
        let output_delta = pager
            .io
            .block(|| agg.commit(update_delta.clone(), &mut cursor))
            .unwrap();

        // CRITICAL: The output delta should contain TWO changes:
        // 1. Retraction of old aggregate value (90) with weight -1
        // 2. Insertion of new aggregate value (130) with weight +1
        assert_eq!(
            output_delta.changes.len(),
            2,
            "Expected 2 changes (retraction + insertion), got {}: {:?}",
            output_delta.changes.len(),
            output_delta.changes
        );

        // Verify the retraction comes first
        let (retraction_row, retraction_weight) = &output_delta.changes[0];
        assert_eq!(
            *retraction_weight, -1,
            "First change should be a retraction"
        );
        assert_eq!(
            retraction_row.values[0],
            Value::Float(90.0),
            "Retracted value should be the old sum (90)"
        );

        // Verify the insertion comes second
        let (insertion_row, insertion_weight) = &output_delta.changes[1];
        assert_eq!(*insertion_weight, 1, "Second change should be an insertion");
        assert_eq!(
            insertion_row.values[0],
            Value::Float(130.0),
            "Inserted value should be the new sum (130)"
        );

        // Both changes should have the same row ID (since it's the same aggregate group)
        assert_eq!(
            retraction_row.rowid, insertion_row.rowid,
            "Retraction and insertion should have the same row ID"
        );
    }

    #[test]
    fn test_aggregate_with_group_by_emits_retractions() {
        // This test verifies that when aggregate values change for grouped data,
        // the operator emits both retractions and insertions correctly for each group.

        // Create an aggregate operator for SUM(score) GROUP BY team
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1,                        // operator_id for testing
            vec!["team".to_string()], // GROUP BY team
            vec![AggregateFunction::Sum("score".to_string())],
            vec![
                "id".to_string(),
                "team".to_string(),
                "player".to_string(),
                "score".to_string(),
            ],
        );

        // Initial data: players on different teams
        let mut initial_delta = Delta::new();
        initial_delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("red".to_string().into()),
                Value::Text("Alice".to_string().into()),
                Value::Integer(10),
            ],
        );
        initial_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("blue".to_string().into()),
                Value::Text("Bob".to_string().into()),
                Value::Integer(15),
            ],
        );
        initial_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("red".to_string().into()),
                Value::Text("Charlie".to_string().into()),
                Value::Integer(20),
            ],
        );

        // Initialize with initial data
        pager
            .io
            .block(|| agg.commit(initial_delta.clone(), &mut cursor))
            .unwrap();

        // Verify initial state: red team = 30, blue team = 15
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes.len(), 2, "Should have two groups");

        // Find the red and blue team aggregates
        let mut red_sum = None;
        let mut blue_sum = None;
        for (row, weight) in &state.changes {
            assert_eq!(*weight, 1);
            if let Value::Text(team) = &row.values[0] {
                if team.as_str() == "red" {
                    red_sum = Some(&row.values[1]);
                } else if team.as_str() == "blue" {
                    blue_sum = Some(&row.values[1]);
                }
            }
        }
        assert_eq!(
            red_sum,
            Some(&Value::Float(30.0)),
            "Red team sum should be 30"
        );
        assert_eq!(
            blue_sum,
            Some(&Value::Float(15.0)),
            "Blue team sum should be 15"
        );

        // Now add a new player to the red team (incremental update)
        let mut update_delta = Delta::new();
        update_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("red".to_string().into()),
                Value::Text("David".to_string().into()),
                Value::Integer(25),
            ],
        );

        // Process the incremental update
        let output_delta = pager
            .io
            .block(|| agg.commit(update_delta.clone(), &mut cursor))
            .unwrap();

        // Should have 2 changes: retraction of old red team sum, insertion of new red team sum
        // Blue team should NOT be affected
        assert_eq!(
            output_delta.changes.len(),
            2,
            "Expected 2 changes for red team only, got {}: {:?}",
            output_delta.changes.len(),
            output_delta.changes
        );

        // Both changes should be for the red team
        let mut found_retraction = false;
        let mut found_insertion = false;

        for (row, weight) in &output_delta.changes {
            if let Value::Text(team) = &row.values[0] {
                assert_eq!(team.as_str(), "red", "Only red team should have changes");

                if *weight == -1 {
                    // Retraction of old value
                    assert_eq!(
                        row.values[1],
                        Value::Float(30.0),
                        "Should retract old sum of 30"
                    );
                    found_retraction = true;
                } else if *weight == 1 {
                    // Insertion of new value
                    assert_eq!(
                        row.values[1],
                        Value::Float(55.0),
                        "Should insert new sum of 55"
                    );
                    found_insertion = true;
                }
            }
        }

        assert!(found_retraction, "Should have found retraction");
        assert!(found_insertion, "Should have found insertion");
    }

    // Aggregation tests
    #[test]
    fn test_count_increments_not_recounts() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        // Create COUNT(*) GROUP BY category
        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
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
        pager
            .io
            .block(|| agg.commit(initial.clone(), &mut cursor))
            .unwrap();

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

        pager
            .io
            .block(|| agg.commit(delta.clone(), &mut cursor))
            .unwrap();

        assert_eq!(tracker.lock().unwrap().aggregation_updates, 1);

        // Check the final state - cat_0 should now have count 11
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let cat_0 = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("cat_0")))
            .unwrap();
        assert_eq!(cat_0.0.values[1], Value::Integer(11));

        // Verify incremental behavior - we process the delta twice (eval + commit)
        let t = tracker.lock().unwrap();
        assert_incremental(&t, 2, 101);
    }

    #[test]
    fn test_sum_updates_incrementally() {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Create SUM(amount) GROUP BY product
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
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
        pager
            .io
            .block(|| agg.commit(initial.clone(), &mut cursor))
            .unwrap();

        // Check initial state: Widget=250, Gadget=200
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
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

        pager
            .io
            .block(|| agg.commit(delta.clone(), &mut cursor))
            .unwrap();

        assert_eq!(tracker.lock().unwrap().aggregation_updates, 1);

        // Check final state - Widget should now be 300 (250 + 50)
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let widget = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("Widget")))
            .unwrap();
        assert_eq!(widget.0.values[1], Value::Integer(300));
    }

    #[test]
    fn test_count_and_sum_together() {
        // Test the example from DBSP_ROADMAP: COUNT(*) and SUM(amount) GROUP BY user_id
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
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
        pager
            .io
            .block(|| agg.commit(initial.clone(), &mut cursor))
            .unwrap();

        // Check initial state
        // User 1: count=2, sum=300
        // User 2: count=1, sum=150
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
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
        pager
            .io
            .block(|| agg.commit(delta.clone(), &mut cursor))
            .unwrap();

        // Check final state - user 1 should have updated count and sum
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let user1 = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Integer(1))
            .unwrap();
        assert_eq!(user1.0.values[1], Value::Integer(3)); // count: 2 + 1
        assert_eq!(user1.0.values[2], Value::Integer(350)); // sum: 300 + 50
    }

    #[test]
    fn test_avg_maintains_sum_and_count() {
        // Test AVG aggregation
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
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
        pager
            .io
            .block(|| agg.commit(initial.clone(), &mut cursor))
            .unwrap();

        // Check initial averages
        // Category A: avg = (10 + 20) / 2 = 15
        // Category B: avg = 30 / 1 = 30
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
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
        pager
            .io
            .block(|| agg.commit(delta.clone(), &mut cursor))
            .unwrap();

        // Check final state - Category A avg should now be (10 + 20 + 30) / 3 = 20
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let cat_a = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("A")))
            .unwrap();
        assert_eq!(cat_a.0.values[1], Value::Float(20.0));
    }

    #[test]
    fn test_delete_updates_aggregates() {
        // Test that deletes (negative weights) properly update aggregates
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
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
        pager
            .io
            .block(|| agg.commit(initial.clone(), &mut cursor))
            .unwrap();

        // Check initial state: count=2, sum=300
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
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

        pager
            .io
            .block(|| agg.commit(delta.clone(), &mut cursor))
            .unwrap();

        // Check final state - should update to count=1, sum=200
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let cat_a = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text(Text::new("A")))
            .unwrap();
        assert_eq!(cat_a.0.values[1], Value::Integer(1)); // count: 2 - 1
        assert_eq!(cat_a.0.values[2], Value::Integer(200)); // sum: 300 - 100
    }

    #[test]
    fn test_count_aggregation_with_deletions() {
        let aggregates = vec![AggregateFunction::Count];
        let group_by = vec!["category".to_string()];
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(10)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(20)]);
        init_data.insert(3, vec![Value::Text("B".into()), Value::Integer(30)]);
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Check initial counts
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes.len(), 2);

        // Find group A and B
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        let group_b = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("B".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(2)); // COUNT = 2 for A
        assert_eq!(group_b.0.values[1], Value::Integer(1)); // COUNT = 1 for B

        // Delete one row from group A
        let mut delete_delta = Delta::new();
        delete_delta.delete(1, vec![Value::Text("A".into()), Value::Integer(10)]);

        let output = pager
            .io
            .block(|| agg.commit(delete_delta.clone(), &mut cursor))
            .unwrap();

        // Should emit retraction for old count and insertion for new count
        assert_eq!(output.changes.len(), 2);

        // Check final state
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let group_a_final = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(group_a_final.0.values[1], Value::Integer(1)); // COUNT = 1 for A after deletion

        // Delete all rows from group B
        let mut delete_all_b = Delta::new();
        delete_all_b.delete(3, vec![Value::Text("B".into()), Value::Integer(30)]);

        let output_b = pager
            .io
            .block(|| agg.commit(delete_all_b.clone(), &mut cursor))
            .unwrap();
        assert_eq!(output_b.changes.len(), 1); // Only retraction, no new row
        assert_eq!(output_b.changes[0].1, -1); // Retraction

        // Final state should not have group B
        let final_state2 = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(final_state2.changes.len(), 1); // Only group A remains
        assert_eq!(final_state2.changes[0].0.values[0], Value::Text("A".into()));
    }

    #[test]
    fn test_sum_aggregation_with_deletions() {
        let aggregates = vec![AggregateFunction::Sum("value".to_string())];
        let group_by = vec!["category".to_string()];
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(10)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(20)]);
        init_data.insert(3, vec![Value::Text("B".into()), Value::Integer(30)]);
        init_data.insert(4, vec![Value::Text("B".into()), Value::Integer(15)]);
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Check initial sums
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        let group_b = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("B".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(30)); // SUM = 30 for A (10+20)
        assert_eq!(group_b.0.values[1], Value::Integer(45)); // SUM = 45 for B (30+15)

        // Delete one row from group A
        let mut delete_delta = Delta::new();
        delete_delta.delete(2, vec![Value::Text("A".into()), Value::Integer(20)]);

        pager
            .io
            .block(|| agg.commit(delete_delta.clone(), &mut cursor))
            .unwrap();

        // Check updated sum
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(group_a.0.values[1], Value::Integer(10)); // SUM = 10 for A after deletion

        // Delete all from group B
        let mut delete_all_b = Delta::new();
        delete_all_b.delete(3, vec![Value::Text("B".into()), Value::Integer(30)]);
        delete_all_b.delete(4, vec![Value::Text("B".into()), Value::Integer(15)]);

        pager
            .io
            .block(|| agg.commit(delete_all_b.clone(), &mut cursor))
            .unwrap();

        // Group B should be gone
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(final_state.changes.len(), 1); // Only group A remains
        assert_eq!(final_state.changes[0].0.values[0], Value::Text("A".into()));
    }

    #[test]
    fn test_avg_aggregation_with_deletions() {
        let aggregates = vec![AggregateFunction::Avg("value".to_string())];
        let group_by = vec!["category".to_string()];
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(10)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(20)]);
        init_data.insert(3, vec![Value::Text("A".into()), Value::Integer(30)]);
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Check initial average
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.values[1], Value::Float(20.0)); // AVG = (10+20+30)/3 = 20

        // Delete the middle value
        let mut delete_delta = Delta::new();
        delete_delta.delete(2, vec![Value::Text("A".into()), Value::Integer(20)]);

        pager
            .io
            .block(|| agg.commit(delete_delta.clone(), &mut cursor))
            .unwrap();

        // Check updated average
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes[0].0.values[1], Value::Float(20.0)); // AVG = (10+30)/2 = 20 (same!)

        // Delete another to change the average
        let mut delete_another = Delta::new();
        delete_another.delete(3, vec![Value::Text("A".into()), Value::Integer(30)]);

        pager
            .io
            .block(|| agg.commit(delete_another.clone(), &mut cursor))
            .unwrap();

        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes[0].0.values[1], Value::Float(10.0)); // AVG = 10/1 = 10
    }

    #[test]
    fn test_multiple_aggregations_with_deletions() {
        // Test COUNT, SUM, and AVG together
        let aggregates = vec![
            AggregateFunction::Count,
            AggregateFunction::Sum("value".to_string()),
            AggregateFunction::Avg("value".to_string()),
        ];
        let group_by = vec!["category".to_string()];
        let input_columns = vec!["category".to_string(), "value".to_string()];

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            group_by,
            aggregates.clone(),
            input_columns,
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Text("A".into()), Value::Integer(100)]);
        init_data.insert(2, vec![Value::Text("A".into()), Value::Integer(200)]);
        init_data.insert(3, vec![Value::Text("B".into()), Value::Integer(50)]);
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Check initial state
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(2)); // COUNT = 2
        assert_eq!(group_a.0.values[2], Value::Integer(300)); // SUM = 300
        assert_eq!(group_a.0.values[3], Value::Float(150.0)); // AVG = 150

        // Delete one row from group A
        let mut delete_delta = Delta::new();
        delete_delta.delete(1, vec![Value::Text("A".into()), Value::Integer(100)]);

        pager
            .io
            .block(|| agg.commit(delete_delta.clone(), &mut cursor))
            .unwrap();

        // Check all aggregates updated correctly
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(1)); // COUNT = 1
        assert_eq!(group_a.0.values[2], Value::Integer(200)); // SUM = 200
        assert_eq!(group_a.0.values[3], Value::Float(200.0)); // AVG = 200

        // Insert a new row with floating point value
        let mut insert_delta = Delta::new();
        insert_delta.insert(4, vec![Value::Text("A".into()), Value::Float(50.5)]);

        pager
            .io
            .block(|| agg.commit(insert_delta.clone(), &mut cursor))
            .unwrap();

        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let group_a = state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();

        assert_eq!(group_a.0.values[1], Value::Integer(2)); // COUNT = 2
        assert_eq!(group_a.0.values[2], Value::Float(250.5)); // SUM = 250.5
        assert_eq!(group_a.0.values[3], Value::Float(125.25)); // AVG = 125.25
    }

    #[test]
    fn test_filter_operator_rowid_update() {
        // When a row's rowid changes (e.g., UPDATE t SET a=1 WHERE a=3 on INTEGER PRIMARY KEY),
        // the operator should properly consolidate the state

        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut filter = FilterOperator::new(
            FilterPredicate::GreaterThan {
                column: "b".to_string(),
                value: Value::Integer(2),
            },
            vec!["a".to_string(), "b".to_string()],
        );

        // Initialize with a row (rowid=3, values=[3, 3])
        let mut init_data = Delta::new();
        init_data.insert(3, vec![Value::Integer(3), Value::Integer(3)]);
        let state = pager
            .io
            .block(|| filter.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Check initial state
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.rowid, 3);
        assert_eq!(
            state.changes[0].0.values,
            vec![Value::Integer(3), Value::Integer(3)]
        );

        // Simulate an UPDATE that changes rowid from 3 to 1
        // This is sent as: delete(3) + insert(1)
        let mut update_delta = Delta::new();
        update_delta.delete(3, vec![Value::Integer(3), Value::Integer(3)]);
        update_delta.insert(1, vec![Value::Integer(1), Value::Integer(3)]);

        let output = pager
            .io
            .block(|| filter.commit(update_delta.clone(), &mut cursor))
            .unwrap();

        // The output delta should have both changes (both pass the filter b > 2)
        assert_eq!(output.changes.len(), 2);
        assert_eq!(output.changes[0].1, -1); // delete weight
        assert_eq!(output.changes[1].1, 1); // insert weight
    }

    // ============================================================================
    // EVAL/COMMIT PATTERN TESTS
    // These tests verify that the eval/commit pattern works correctly:
    // - eval() computes results without modifying state
    // - eval() with uncommitted data returns correct results
    // - commit() updates internal state
    // - State remains unchanged when eval() is called with uncommitted data
    // ============================================================================

    #[test]
    fn test_filter_eval_with_uncommitted() {
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut filter = FilterOperator::new(
            FilterPredicate::GreaterThan {
                column: "age".to_string(),
                value: Value::Integer(25),
            },
            vec!["id".to_string(), "name".to_string(), "age".to_string()],
        );

        // Initialize with some data
        let mut init_data = Delta::new();
        init_data.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(30),
            ],
        );
        init_data.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(20),
            ],
        );
        let state = pager
            .io
            .block(|| filter.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Verify initial state (only Alice passes filter)
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.rowid, 1);

        // Create uncommitted changes
        let mut uncommitted = Delta::new();
        uncommitted.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(35),
            ],
        );
        uncommitted.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(15),
            ],
        );

        // Eval with uncommitted - should return filtered uncommitted rows
        let mut eval_state = uncommitted.clone().into();
        let result = pager
            .io
            .block(|| filter.eval(&mut eval_state, &mut cursor))
            .unwrap();
        assert_eq!(
            result.changes.len(),
            1,
            "Only Charlie (35) should pass filter"
        );
        assert_eq!(result.changes[0].0.rowid, 3);

        // Now commit the changes
        let state = pager
            .io
            .block(|| filter.commit(uncommitted.clone(), &mut cursor))
            .unwrap();

        // State should now include Charlie (who passes filter)
        assert_eq!(
            state.changes.len(),
            1,
            "State should now have Alice and Charlie"
        );
    }

    #[test]
    fn test_aggregate_eval_with_uncommitted_preserves_state() {
        // This is the critical test - aggregations must not modify internal state during eval
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            vec!["category".to_string()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("amount".to_string()),
            ],
            vec![
                "id".to_string(),
                "category".to_string(),
                "amount".to_string(),
            ],
        );

        // Initialize with data
        let mut init_data = Delta::new();
        init_data.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("A".into()),
                Value::Integer(100),
            ],
        );
        init_data.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("A".into()),
                Value::Integer(200),
            ],
        );
        init_data.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("B".into()),
                Value::Integer(150),
            ],
        );
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Check initial state: A -> (count=2, sum=300), B -> (count=1, sum=150)
        let initial_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(initial_state.changes.len(), 2);

        // Store initial state for comparison
        let initial_a = initial_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(initial_a.0.values[1], Value::Integer(2)); // count
        assert_eq!(initial_a.0.values[2], Value::Float(300.0)); // sum

        // Create uncommitted changes
        let mut uncommitted = Delta::new();
        uncommitted.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("A".into()),
                Value::Integer(50),
            ],
        );
        uncommitted.insert(
            5,
            vec![
                Value::Integer(5),
                Value::Text("C".into()),
                Value::Integer(75),
            ],
        );

        // Eval with uncommitted should return the delta (changes to aggregates)
        let mut eval_state = uncommitted.clone().into();
        let result = pager
            .io
            .block(|| agg.eval(&mut eval_state, &mut cursor))
            .unwrap();

        // Result should contain updates for A and new group C
        // For A: retraction of old (2, 300) and insertion of new (3, 350)
        // For C: insertion of (1, 75)
        assert!(!result.changes.is_empty(), "Should have aggregate changes");

        // CRITICAL: Verify internal state hasn't changed
        let state_after_eval = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(
            state_after_eval.changes.len(),
            2,
            "State should still have only A and B"
        );

        let a_after_eval = state_after_eval
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(
            a_after_eval.0.values[1],
            Value::Integer(2),
            "A count should still be 2"
        );
        assert_eq!(
            a_after_eval.0.values[2],
            Value::Float(300.0),
            "A sum should still be 300"
        );

        // Now commit the changes
        pager
            .io
            .block(|| agg.commit(uncommitted.clone(), &mut cursor))
            .unwrap();

        // State should now be updated
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(final_state.changes.len(), 3, "Should now have A, B, and C");

        let a_final = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("A".into()))
            .unwrap();
        assert_eq!(
            a_final.0.values[1],
            Value::Integer(3),
            "A count should now be 3"
        );
        assert_eq!(
            a_final.0.values[2],
            Value::Float(350.0),
            "A sum should now be 350"
        );

        let c_final = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("C".into()))
            .unwrap();
        assert_eq!(
            c_final.0.values[1],
            Value::Integer(1),
            "C count should be 1"
        );
        assert_eq!(
            c_final.0.values[2],
            Value::Float(75.0),
            "C sum should be 75"
        );
    }

    #[test]
    fn test_aggregate_eval_multiple_times_without_commit() {
        // Test that calling eval multiple times with different uncommitted data
        // doesn't pollute the internal state
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1,      // operator_id for testing
            vec![], // No GROUP BY
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum("value".to_string()),
            ],
            vec!["id".to_string(), "value".to_string()],
        );

        // Initialize
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Integer(1), Value::Integer(100)]);
        init_data.insert(2, vec![Value::Integer(2), Value::Integer(200)]);
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Initial state: count=2, sum=300
        let initial_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(initial_state.changes.len(), 1);
        assert_eq!(initial_state.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(initial_state.changes[0].0.values[1], Value::Float(300.0));

        // First eval with uncommitted
        let mut uncommitted1 = Delta::new();
        uncommitted1.insert(3, vec![Value::Integer(3), Value::Integer(50)]);
        let mut eval_state1 = uncommitted1.clone().into();
        let _ = pager
            .io
            .block(|| agg.eval(&mut eval_state1, &mut cursor))
            .unwrap();

        // State should be unchanged
        let state1 = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state1.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(state1.changes[0].0.values[1], Value::Float(300.0));

        // Second eval with different uncommitted
        let mut uncommitted2 = Delta::new();
        uncommitted2.insert(4, vec![Value::Integer(4), Value::Integer(75)]);
        uncommitted2.insert(5, vec![Value::Integer(5), Value::Integer(25)]);
        let mut eval_state2 = uncommitted2.clone().into();
        let _ = pager
            .io
            .block(|| agg.eval(&mut eval_state2, &mut cursor))
            .unwrap();

        // State should STILL be unchanged
        let state2 = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state2.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(state2.changes[0].0.values[1], Value::Float(300.0));

        // Third eval with deletion as uncommitted
        let mut uncommitted3 = Delta::new();
        uncommitted3.delete(1, vec![Value::Integer(1), Value::Integer(100)]);
        let mut eval_state3 = uncommitted3.clone().into();
        let _ = pager
            .io
            .block(|| agg.eval(&mut eval_state3, &mut cursor))
            .unwrap();

        // State should STILL be unchanged
        let state3 = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state3.changes[0].0.values[0], Value::Integer(2));
        assert_eq!(state3.changes[0].0.values[1], Value::Float(300.0));
    }

    #[test]
    fn test_aggregate_eval_with_mixed_committed_and_uncommitted() {
        // Test eval with both committed delta and uncommitted changes
        // Create a persistent pager for the test
        let (pager, root_page_id) = create_test_pager();
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page_id, 10);

        let mut agg = AggregateOperator::new(
            1, // operator_id for testing
            vec!["type".to_string()],
            vec![AggregateFunction::Count],
            vec!["id".to_string(), "type".to_string()],
        );

        // Initialize
        let mut init_data = Delta::new();
        init_data.insert(1, vec![Value::Integer(1), Value::Text("X".into())]);
        init_data.insert(2, vec![Value::Integer(2), Value::Text("Y".into())]);
        pager
            .io
            .block(|| agg.commit(init_data.clone(), &mut cursor))
            .unwrap();

        // Create a committed delta (to be processed)
        let mut committed_delta = Delta::new();
        committed_delta.insert(3, vec![Value::Integer(3), Value::Text("X".into())]);

        // Create uncommitted changes
        let mut uncommitted = Delta::new();
        uncommitted.insert(4, vec![Value::Integer(4), Value::Text("Y".into())]);
        uncommitted.insert(5, vec![Value::Integer(5), Value::Text("Z".into())]);

        // Eval with both - should process both but not commit
        let mut combined = committed_delta.clone();
        combined.merge(&uncommitted);
        let mut eval_state = combined.clone().into();
        let result = pager
            .io
            .block(|| agg.eval(&mut eval_state, &mut cursor))
            .unwrap();

        // Result should reflect changes from both
        assert!(!result.changes.is_empty(), "Result should not be empty");

        // Verify the DBSP pattern: retraction (-1) followed by insertion (1) for updates,
        // and just insertion (1) for new groups

        // We expect exactly 5 changes:
        // - X: retraction + insertion (was 1, now 2)
        // - Y: retraction + insertion (was 1, now 2)
        // - Z: insertion only (new group with count 1)
        assert_eq!(
            result.changes.len(),
            5,
            "Should have 5 changes (2 retractions + 3 insertions)"
        );

        // Sort by group name then by weight to get predictable order
        let mut sorted_changes: Vec<_> = result.changes.iter().collect();
        sorted_changes.sort_by(|a, b| {
            let a_group = &a.0.values[0];
            let b_group = &b.0.values[0];
            match a_group.partial_cmp(b_group).unwrap() {
                std::cmp::Ordering::Equal => a.1.cmp(&b.1), // Sort by weight if same group
                other => other,
            }
        });

        // Check X group: should have retraction (-1) for count=1, then insertion (1) for count=2
        assert_eq!(sorted_changes[0].0.values[0], Value::Text("X".into()));
        assert_eq!(sorted_changes[0].0.values[1], Value::Integer(1)); // old count
        assert_eq!(sorted_changes[0].1, -1); // retraction

        assert_eq!(sorted_changes[1].0.values[0], Value::Text("X".into()));
        assert_eq!(sorted_changes[1].0.values[1], Value::Integer(2)); // new count
        assert_eq!(sorted_changes[1].1, 1); // insertion

        // Check Y group: should have retraction (-1) for count=1, then insertion (1) for count=2
        assert_eq!(sorted_changes[2].0.values[0], Value::Text("Y".into()));
        assert_eq!(sorted_changes[2].0.values[1], Value::Integer(1)); // old count
        assert_eq!(sorted_changes[2].1, -1); // retraction

        assert_eq!(sorted_changes[3].0.values[0], Value::Text("Y".into()));
        assert_eq!(sorted_changes[3].0.values[1], Value::Integer(2)); // new count
        assert_eq!(sorted_changes[3].1, 1); // insertion

        // Check Z group: should only have insertion (1) for count=1 (new group)
        assert_eq!(sorted_changes[4].0.values[0], Value::Text("Z".into()));
        assert_eq!(sorted_changes[4].0.values[1], Value::Integer(1)); // new count
        assert_eq!(sorted_changes[4].1, 1); // insertion only (no retraction as it's new);

        // But internal state should be unchanged
        let state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        assert_eq!(state.changes.len(), 2, "Should still have only X and Y");

        // Now commit only the committed_delta
        pager
            .io
            .block(|| agg.commit(committed_delta.clone(), &mut cursor))
            .unwrap();

        // State should now have X count=2, Y count=1
        let final_state = get_current_state_from_btree(&agg, &pager, &mut cursor);
        let x = final_state
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::Text("X".into()))
            .unwrap();
        assert_eq!(x.0.values[1], Value::Integer(2));
    }
}
