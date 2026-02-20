//! DBSP Compiler: Converts Logical Plans to DBSP Circuits
//!
//! This module implements compilation from SQL logical plans to DBSP circuits.
//! The initial version supports only filter and projection operators.
//!
//! Based on the DBSP paper: "DBSP: Automatic Incremental View Maintenance for Rich Query Languages"

use crate::incremental::aggregate_operator::AggregateOperator;
use crate::incremental::dbsp::{Delta, DeltaPair};
use crate::incremental::expr_compiler::CompiledExpression;
use crate::incremental::operator::{
    create_dbsp_state_index, DbspStateCursors, EvalState, FilterOperator, FilterPredicate,
    IncrementalOperator, InputOperator, JoinOperator, JoinType, ProjectOperator,
};
use crate::schema::Type;
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
// Note: logical module must be made pub(crate) in translate/mod.rs
use crate::numeric::Numeric;
use crate::sync::{atomic::Ordering, Arc};
use crate::translate::logical::{
    BinaryOperator, Column, ColumnInfo, JoinType as LogicalJoinType, LogicalExpr, LogicalPlan,
    LogicalSchema, SchemaRef,
};
use crate::types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult, Value};
use crate::Pager;
use crate::{return_and_restore_if_io, return_if_io, LimboError, Result};
use rustc_hash::FxHashMap as HashMap;
use std::fmt::{self, Display, Formatter};

// The state table has 5 columns: operator_id, zset_id, element_id, value, weight
const OPERATOR_COLUMNS: usize = 5;

/// State machine for writing rows to simple materialized views (table-only, no index)
#[derive(Debug, Default)]
pub enum WriteRowView {
    #[default]
    GetRecord,
    Delete,
    Insert {
        final_weight: isize,
    },
    Done,
}

impl WriteRowView {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write a row with weight management for table-only storage.
    ///
    /// # Arguments
    /// * `cursor` - BTree cursor for the storage
    /// * `key` - The key to seek (TableRowId)
    /// * `build_record` - Function that builds the record values to insert.
    ///   Takes the final_weight and returns the complete record values.
    /// * `weight` - The weight delta to apply
    pub fn write_row(
        &mut self,
        cursor: &mut BTreeCursor,
        key: SeekKey,
        build_record: impl Fn(isize) -> Vec<Value>,
        weight: isize,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                WriteRowView::GetRecord => {
                    let res = return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    if !matches!(res, SeekResult::Found) {
                        *self = WriteRowView::Insert {
                            final_weight: weight,
                        };
                    } else {
                        let existing_record = return_if_io!(cursor.record());
                        let r = existing_record.ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "Found key {key:?} in storage but could not read record"
                            ))
                        })?;
                        let last = r.iter()?.last();

                        // Weight is always the last value
                        let existing_weight = match last {
                            Some(val) => match val?.to_owned() {
                                Value::Numeric(Numeric::Integer(w)) => w as isize,
                                _ => {
                                    return Err(LimboError::InternalError(format!(
                                        "Invalid weight value in storage for key {key:?}"
                                    )))
                                }
                            },
                            None => {
                                return Err(LimboError::InternalError(format!(
                                    "No weight value found in storage for key {key:?}"
                                )))
                            }
                        };

                        let final_weight = existing_weight + weight;
                        if final_weight <= 0 {
                            *self = WriteRowView::Delete
                        } else {
                            *self = WriteRowView::Insert { final_weight }
                        }
                    }
                }
                WriteRowView::Delete => {
                    // Mark as Done before delete to avoid retry on I/O
                    *self = WriteRowView::Done;
                    return_if_io!(cursor.delete());
                }
                WriteRowView::Insert { final_weight } => {
                    return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));

                    // Extract the row ID from the key
                    let key_i64 = match key {
                        SeekKey::TableRowId(id) => id,
                        _ => {
                            return Err(LimboError::InternalError(
                                "Expected TableRowId for storage".to_string(),
                            ))
                        }
                    };

                    // Build the record values using the provided function
                    let record_values = build_record(*final_weight);

                    // Create an ImmutableRecord from the values
                    let immutable_record =
                        ImmutableRecord::from_values(&record_values, record_values.len());
                    let btree_key = BTreeKey::new_table_rowid(key_i64, Some(&immutable_record));

                    // Mark as Done before insert to avoid retry on I/O
                    *self = WriteRowView::Done;
                    return_if_io!(cursor.insert(&btree_key));
                }
                WriteRowView::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

/// State machine for commit operations
pub enum CommitState {
    /// Initial state - ready to start commit
    Init,

    /// Running circuit with commit_operators flag set to true
    CommitOperators {
        /// Execute state for running the circuit
        execute_state: Box<ExecuteState>,
        /// Persistent cursors for operator state (table and index)
        state_cursors: Box<DbspStateCursors>,
    },

    /// Updating the materialized view with the delta
    UpdateView {
        /// Delta to write to the view
        delta: Delta,
        /// Current index in delta.changes being processed
        current_index: usize,
        /// State for writing individual rows
        write_row_state: WriteRowView,
        /// Cursor for view data btree - created fresh for each row
        view_cursor: Box<BTreeCursor>,
    },
}

impl std::fmt::Debug for CommitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init => write!(f, "Init"),
            Self::CommitOperators { execute_state, .. } => f
                .debug_struct("CommitOperators")
                .field("execute_state", execute_state)
                .field("has_state_table_cursor", &true)
                .field("has_state_index_cursor", &true)
                .finish(),
            Self::UpdateView {
                delta,
                current_index,
                write_row_state,
                ..
            } => f
                .debug_struct("UpdateView")
                .field("delta", delta)
                .field("current_index", current_index)
                .field("write_row_state", write_row_state)
                .field("has_view_cursor", &true)
                .finish(),
        }
    }
}

/// State machine for circuit execution across I/O operations
/// Similar to EvalState but for tracking execution state through the circuit
#[derive(Debug)]
pub enum ExecuteState {
    /// Empty state so we can allocate the space without executing
    Uninitialized,

    /// Initial state - starting circuit execution
    Init {
        /// Input deltas to process
        input_data: DeltaSet,
    },

    /// Processing multiple inputs (for recursive node processing)
    ProcessingInputs {
        /// Collection of (node_id, state) pairs to process
        input_states: Vec<(i64, ExecuteState)>,
        /// Current index being processed
        current_index: usize,
        /// Collected deltas from processed inputs
        input_deltas: Vec<Delta>,
    },

    /// Processing a specific node in the circuit
    ProcessingNode {
        /// Node's evaluation state (includes the delta in its Init state)
        eval_state: Box<EvalState>,
    },
}

/// A set of deltas for multiple tables/operators
/// This provides a cleaner API for passing deltas through circuit execution
#[derive(Debug, Clone, Default)]
pub struct DeltaSet {
    /// Deltas keyed by table/operator name
    deltas: HashMap<String, Delta>,
}

impl DeltaSet {
    /// Create a new empty delta set
    pub fn new() -> Self {
        Self {
            deltas: HashMap::default(),
        }
    }

    /// Create an empty delta set (more semantic for "no changes")
    pub fn empty() -> Self {
        Self {
            deltas: HashMap::default(),
        }
    }

    /// Create a DeltaSet from a HashMap
    pub fn from_map(deltas: HashMap<String, Delta>) -> Self {
        Self { deltas }
    }

    /// Add a delta for a table
    pub fn insert(&mut self, table_name: String, delta: Delta) {
        self.deltas.insert(table_name, delta);
    }

    /// Get delta for a table, returns empty delta if not found
    pub fn get(&self, table_name: &str) -> Delta {
        self.deltas
            .get(table_name)
            .cloned()
            .unwrap_or_else(Delta::new)
    }

    /// Convert DeltaSet into the underlying HashMap
    pub fn into_map(self) -> HashMap<String, Delta> {
        self.deltas
    }

    /// Check if all deltas in the set are empty
    pub fn is_empty(&self) -> bool {
        self.deltas.values().all(|d| d.is_empty())
    }
}

/// Represents a DBSP operator in the compiled circuit
#[derive(Debug, Clone, PartialEq)]
pub enum DbspOperator {
    /// Filter operator (σ) - filters records based on a predicate
    Filter { predicate: DbspExpr },
    /// Projection operator (π) - projects specific columns
    Projection {
        exprs: Vec<DbspExpr>,
        schema: SchemaRef,
    },
    /// Aggregate operator (γ) - performs grouping and aggregation
    Aggregate {
        group_exprs: Vec<DbspExpr>,
        aggr_exprs: Vec<crate::incremental::operator::AggregateFunction>,
        schema: SchemaRef,
    },
    /// Join operator (⋈) - joins two relations
    Join {
        join_type: JoinType,
        on_exprs: Vec<(DbspExpr, DbspExpr)>,
        schema: SchemaRef,
    },
    /// Input operator - source of data
    Input { name: String, schema: SchemaRef },
    /// Merge operator for combining streams (used in recursive CTEs and UNION)
    Merge { schema: SchemaRef },
    /// Distinct operator - removes duplicates
    Distinct { schema: SchemaRef },
}

/// Represents an expression in DBSP
#[derive(Debug, Clone, PartialEq)]
pub enum DbspExpr {
    /// Column reference
    Column(String),
    /// Literal value
    Literal(Value),
    /// Binary expression
    BinaryExpr {
        left: Box<DbspExpr>,
        op: BinaryOperator,
        right: Box<DbspExpr>,
    },
}

/// A node in the DBSP circuit DAG
pub struct DbspNode {
    /// Unique identifier for this node
    pub id: i64,
    /// The operator metadata
    pub operator: DbspOperator,
    /// Input nodes (edges in the DAG)
    pub inputs: Vec<i64>,
    /// The actual executable operator
    pub executable: Box<dyn IncrementalOperator>,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for DbspNode {}
unsafe impl Sync for DbspNode {}
crate::assert::assert_send_sync!(DbspNode);

impl std::fmt::Debug for DbspNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbspNode")
            .field("id", &self.id)
            .field("operator", &self.operator)
            .field("inputs", &self.inputs)
            .field("has_executable", &true)
            .finish()
    }
}

impl DbspNode {
    fn process_node(
        &mut self,
        eval_state: &mut EvalState,
        commit_operators: bool,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Process delta using the executable operator
        let op = &mut self.executable;

        let state = if commit_operators {
            // Clone the deltas from eval_state - don't extract them
            // in case we need to re-execute due to I/O
            let deltas = match eval_state {
                EvalState::Init { deltas } => deltas.clone(),
                _ => panic!("commit can only be called when eval_state is in Init state"),
            };
            let result = return_if_io!(op.commit(deltas, cursors));
            // After successful commit, move state to Done
            *eval_state = EvalState::Done;
            result
        } else {
            return_if_io!(op.eval(eval_state, cursors))
        };
        Ok(IOResult::Done(state))
    }
}

/// Version number for the DBSP circuit format
/// This should be incremented when the circuit structure changes
pub const DBSP_CIRCUIT_VERSION: u32 = 1;

/// Represents a complete DBSP circuit (DAG of operators)
#[derive(Debug)]
pub struct DbspCircuit {
    /// All nodes in the circuit, indexed by their ID
    pub(super) nodes: HashMap<i64, DbspNode>,
    /// Counter for generating unique node IDs
    next_id: i64,
    /// Root node ID (the final output)
    pub(super) root: Option<i64>,
    /// Output schema of the circuit (schema of the root node)
    pub(super) output_schema: SchemaRef,

    /// State machine for commit operation
    commit_state: CommitState,

    /// Root page for the main materialized view data
    pub(super) main_data_root: i64,
    /// Root page for internal DBSP state table
    pub(super) internal_state_root: i64,
    /// Root page for the DBSP state table's primary key index
    pub(super) internal_state_index_root: i64,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for DbspCircuit {}
unsafe impl Sync for DbspCircuit {}
crate::assert::assert_send_sync!(DbspCircuit);

impl DbspCircuit {
    /// Create a new empty circuit with initial empty schema
    /// The actual output schema will be set when the root node is established
    pub fn new(
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Self {
        // Start with an empty schema - will be updated when root is set
        let empty_schema = Arc::new(LogicalSchema::new(vec![]));
        Self {
            nodes: HashMap::default(),
            next_id: 1, // Start from 1 to reserve 0 for metadata
            root: None,
            output_schema: empty_schema,
            commit_state: CommitState::Init,
            main_data_root,
            internal_state_root,
            internal_state_index_root,
        }
    }

    /// Set the root node and update the output schema
    fn set_root(&mut self, root_id: i64, schema: SchemaRef) {
        self.root = Some(root_id);
        self.output_schema = schema;
    }

    /// Get the current materialized state by reading from btree
    /// Add a node to the circuit
    fn add_node(
        &mut self,
        operator: DbspOperator,
        inputs: Vec<i64>,
        executable: Box<dyn IncrementalOperator>,
    ) -> i64 {
        let id = self.next_id;
        self.next_id += 1;

        let node = DbspNode {
            id,
            operator,
            inputs,
            executable,
        };

        self.nodes.insert(id, node);
        id
    }

    pub fn run_circuit(
        &mut self,
        execute_state: &mut ExecuteState,
        pager: &Arc<Pager>,
        state_cursors: &mut DbspStateCursors,
        commit_operators: bool,
    ) -> Result<IOResult<Delta>> {
        if let Some(root_id) = self.root {
            self.execute_node(
                root_id,
                pager.clone(),
                execute_state,
                commit_operators,
                state_cursors,
            )
        } else {
            Err(LimboError::ParseError(
                "Circuit has no root node".to_string(),
            ))
        }
    }

    /// Execute the circuit with incremental input data (deltas).
    ///
    /// # Arguments
    /// * `pager` - Pager for btree access
    /// * `context` - Execution context for tracking operator states
    /// * `execute_state` - State machine containing input deltas and tracking execution progress
    pub fn execute(
        &mut self,
        pager: Arc<Pager>,
        execute_state: &mut ExecuteState,
    ) -> Result<IOResult<Delta>> {
        if let Some(root_id) = self.root {
            // Create temporary cursors for execute (non-commit) operations
            let table_cursor =
                BTreeCursor::new_table(pager.clone(), self.internal_state_root, OPERATOR_COLUMNS);
            let index_def = create_dbsp_state_index(self.internal_state_index_root);
            let index_cursor = BTreeCursor::new_index(
                pager.clone(),
                self.internal_state_index_root,
                &index_def,
                3,
            );
            let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);
            self.execute_node(root_id, pager, execute_state, false, &mut cursors)
        } else {
            Err(LimboError::ParseError(
                "Circuit has no root node".to_string(),
            ))
        }
    }

    /// Commit deltas to the circuit, updating internal operator state and persisting to btree.
    /// This should be called after execute() when you want to make changes permanent.
    ///
    /// # Arguments
    /// * `input_data` - The deltas to commit (same as what was passed to execute)
    /// * `pager` - Pager for creating cursors to the btrees
    pub fn commit(
        &mut self,
        input_data: HashMap<String, Delta>,
        pager: Arc<Pager>,
    ) -> Result<IOResult<Delta>> {
        // No root means nothing to commit
        if self.root.is_none() {
            return Ok(IOResult::Done(Delta::new()));
        }

        // Get btree root pages
        let main_data_root = self.main_data_root;

        // Add 1 for the weight column that we store in the btree
        let num_columns = self.output_schema.columns.len() + 1;

        // Convert input_data to DeltaSet once, outside the loop
        let input_delta_set = DeltaSet::from_map(input_data);

        loop {
            // Take ownership of the state for processing, to avoid borrow checker issues (we have
            // to call run_circuit, which takes &mut self. Because of that, cannot use
            // return_if_io. We have to use the version that restores the state before returning.
            let mut state = std::mem::replace(&mut self.commit_state, CommitState::Init);
            match &mut state {
                CommitState::Init => {
                    // Create state cursors when entering CommitOperators state
                    let state_table_cursor = BTreeCursor::new_table(
                        pager.clone(),
                        self.internal_state_root,
                        OPERATOR_COLUMNS,
                    );
                    let index_def = create_dbsp_state_index(self.internal_state_index_root);
                    let state_index_cursor = BTreeCursor::new_index(
                        pager.clone(),
                        self.internal_state_index_root,
                        &index_def,
                        3, // Index on first 3 columns
                    );

                    let state_cursors = Box::new(DbspStateCursors::new(
                        state_table_cursor,
                        state_index_cursor,
                    ));

                    self.commit_state = CommitState::CommitOperators {
                        execute_state: Box::new(ExecuteState::Init {
                            input_data: input_delta_set.clone(),
                        }),
                        state_cursors,
                    };
                }
                CommitState::CommitOperators {
                    ref mut execute_state,
                    ref mut state_cursors,
                } => {
                    let delta = return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        self.run_circuit(execute_state, &pager, state_cursors, true,)
                    );

                    // Create view cursor when entering UpdateView state
                    let view_cursor = Box::new(BTreeCursor::new_table(
                        pager.clone(),
                        main_data_root,
                        num_columns,
                    ));

                    self.commit_state = CommitState::UpdateView {
                        delta,
                        current_index: 0,
                        write_row_state: WriteRowView::new(),
                        view_cursor,
                    };
                }
                CommitState::UpdateView {
                    delta,
                    current_index,
                    write_row_state,
                    view_cursor,
                } => {
                    if *current_index >= delta.changes.len() {
                        self.commit_state = CommitState::Init;
                        let delta = std::mem::take(delta);
                        return Ok(IOResult::Done(delta));
                    } else {
                        let (row, weight) = delta.changes[*current_index].clone();

                        // If we're starting a new row (GetRecord state), we need a fresh cursor
                        // due to btree cursor state machine limitations
                        if matches!(write_row_state, WriteRowView::GetRecord) {
                            *view_cursor = Box::new(BTreeCursor::new_table(
                                pager.clone(),
                                main_data_root,
                                num_columns,
                            ));
                        }

                        // Build the view row format: row values + weight
                        let key = SeekKey::TableRowId(row.rowid);
                        let row_values = row.values.clone();
                        let build_fn = move |final_weight: isize| -> Vec<Value> {
                            let mut values = row_values.clone();
                            values.push(Value::from_i64(final_weight as i64));
                            values
                        };

                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            write_row_state.write_row(view_cursor, key, build_fn, weight)
                        );

                        // Move to next row
                        let delta = std::mem::take(delta);
                        // Take ownership of view_cursor - we'll create a new one for next row if needed
                        let view_cursor = std::mem::replace(
                            view_cursor,
                            Box::new(BTreeCursor::new_table(
                                pager.clone(),
                                main_data_root,
                                num_columns,
                            )),
                        );

                        self.commit_state = CommitState::UpdateView {
                            delta,
                            current_index: *current_index + 1,
                            write_row_state: WriteRowView::new(),
                            view_cursor,
                        };
                    }
                }
            }
        }
    }

    /// Execute a specific node in the circuit
    fn execute_node(
        &mut self,
        node_id: i64,
        pager: Arc<Pager>,
        execute_state: &mut ExecuteState,
        commit_operators: bool,
        cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        loop {
            match execute_state {
                ExecuteState::Uninitialized => {
                    panic!("Trying to execute an uninitialized ExecuteState state machine");
                }
                ExecuteState::Init { input_data } => {
                    let node = self
                        .nodes
                        .get(&node_id)
                        .ok_or_else(|| LimboError::ParseError("Node not found".to_string()))?;

                    // Check if this is an Input node
                    match &node.operator {
                        DbspOperator::Input { name, .. } => {
                            // Input nodes get their delta directly from input_data
                            let delta = input_data.get(name);
                            *execute_state = ExecuteState::ProcessingNode {
                                eval_state: Box::new(EvalState::Init {
                                    deltas: delta.into(),
                                }),
                            };
                        }
                        _ => {
                            // Non-input nodes need to process their inputs
                            let input_data = std::mem::take(input_data);
                            let input_node_ids = node.inputs.clone();

                            let input_states: Vec<(i64, ExecuteState)> = input_node_ids
                                .iter()
                                .map(|&input_id| {
                                    (
                                        input_id,
                                        ExecuteState::Init {
                                            input_data: input_data.clone(),
                                        },
                                    )
                                })
                                .collect();

                            *execute_state = ExecuteState::ProcessingInputs {
                                input_states,
                                current_index: 0,
                                input_deltas: Vec::new(),
                            };
                        }
                    }
                }
                ExecuteState::ProcessingInputs {
                    input_states,
                    current_index,
                    input_deltas,
                } => {
                    if *current_index >= input_states.len() {
                        // All inputs processed
                        let left_delta = input_deltas.first().cloned().unwrap_or_else(Delta::new);
                        let right_delta = input_deltas.get(1).cloned().unwrap_or_else(Delta::new);

                        *execute_state = ExecuteState::ProcessingNode {
                            eval_state: Box::new(EvalState::Init {
                                deltas: DeltaPair::new(left_delta, right_delta),
                            }),
                        };
                    } else {
                        // Get the (node_id, state) pair for the current index
                        let (input_node_id, input_state) = &mut input_states[*current_index];

                        // Create temporary cursors for the recursive call
                        let temp_table_cursor = BTreeCursor::new_table(
                            pager.clone(),
                            self.internal_state_root,
                            OPERATOR_COLUMNS,
                        );
                        let index_def = create_dbsp_state_index(self.internal_state_index_root);
                        let temp_index_cursor = BTreeCursor::new_index(
                            pager.clone(),
                            self.internal_state_index_root,
                            &index_def,
                            3,
                        );
                        let mut temp_cursors =
                            DbspStateCursors::new(temp_table_cursor, temp_index_cursor);

                        let delta = return_if_io!(self.execute_node(
                            *input_node_id,
                            pager.clone(),
                            input_state,
                            commit_operators,
                            &mut temp_cursors
                        ));
                        input_deltas.push(delta);
                        *current_index += 1;
                    }
                }
                ExecuteState::ProcessingNode { eval_state } => {
                    // Get mutable reference to node for eval
                    let node = self
                        .nodes
                        .get_mut(&node_id)
                        .ok_or_else(|| LimboError::ParseError("Node not found".to_string()))?;

                    let output_delta =
                        return_if_io!(node.process_node(eval_state, commit_operators, cursors));
                    return Ok(IOResult::Done(output_delta));
                }
            }
        }
    }
}

impl Display for DbspCircuit {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "DBSP Circuit:")?;
        if let Some(root_id) = self.root {
            self.fmt_node(f, root_id, 0)?;
        }
        Ok(())
    }
}

impl DbspCircuit {
    fn fmt_node(&self, f: &mut Formatter, node_id: i64, depth: usize) -> fmt::Result {
        let indent = "  ".repeat(depth);
        if let Some(node) = self.nodes.get(&node_id) {
            match &node.operator {
                DbspOperator::Filter { predicate } => {
                    writeln!(f, "{indent}Filter[{node_id}]: {predicate:?}")?;
                }
                DbspOperator::Projection { exprs, .. } => {
                    writeln!(f, "{indent}Projection[{node_id}]: {exprs:?}")?;
                }
                DbspOperator::Aggregate {
                    group_exprs,
                    aggr_exprs,
                    ..
                } => {
                    writeln!(
                        f,
                        "{indent}Aggregate[{node_id}]: GROUP BY {group_exprs:?}, AGGR {aggr_exprs:?}"
                    )?;
                }
                DbspOperator::Join {
                    join_type,
                    on_exprs,
                    ..
                } => {
                    writeln!(f, "{indent}Join[{node_id}]: {join_type:?} ON {on_exprs:?}")?;
                }
                DbspOperator::Input { name, .. } => {
                    writeln!(f, "{indent}Input[{node_id}]: {name}")?;
                }
                DbspOperator::Merge { schema } => {
                    writeln!(
                        f,
                        "{indent}Merge[{node_id}]: UNION/Recursive (schema: {} columns)",
                        schema.columns.len()
                    )?;
                }
                DbspOperator::Distinct { schema } => {
                    writeln!(
                        f,
                        "{indent}Distinct[{node_id}]: (schema: {} columns)",
                        schema.columns.len()
                    )?;
                }
            }

            for input_id in &node.inputs {
                self.fmt_node(f, *input_id, depth + 1)?;
            }
        }
        Ok(())
    }
}

/// Compiler from LogicalPlan to DBSP Circuit
pub struct DbspCompiler {
    circuit: DbspCircuit,
}

impl DbspCompiler {
    /// Create a new DBSP compiler
    pub fn new(
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Self {
        Self {
            circuit: DbspCircuit::new(
                main_data_root,
                internal_state_root,
                internal_state_index_root,
            ),
        }
    }

    /// Resolve join condition columns to determine which side each column belongs to.
    ///
    /// Returns (left_column, left_index, right_column, right_index) where:
    /// - left_column/right_column are the Column references
    /// - left_index/right_index are the column indices in their respective schemas
    ///
    /// Handles cases where:
    /// - Columns are in normal order (left table column = right table column)
    /// - Columns are swapped (right table column = left table column)
    /// - One or both columns have table qualifiers
    /// - Column names exist in both tables but are disambiguated by qualifiers
    fn resolve_join_columns(
        first_col: &Column,
        second_col: &Column,
        left_schema: &LogicalSchema,
        right_schema: &LogicalSchema,
    ) -> Result<(Column, usize, Column, usize)> {
        // Check all four possibilities to handle ambiguous column names
        let first_in_left = left_schema.find_column(&first_col.name, first_col.table.as_deref());
        let first_in_right = right_schema.find_column(&first_col.name, first_col.table.as_deref());
        let second_in_left = left_schema.find_column(&second_col.name, second_col.table.as_deref());
        let second_in_right =
            right_schema.find_column(&second_col.name, second_col.table.as_deref());

        // Determine the correct pairing: one column must be from left, one from right
        if first_in_left.is_some() && second_in_right.is_some() {
            // first is from left, second is from right
            let (left_idx, _) = first_in_left.ok_or_else(|| {
                LimboError::InternalError("first_in_left should exist".to_string())
            })?;
            let (right_idx, _) = second_in_right.ok_or_else(|| {
                LimboError::InternalError("second_in_right should exist".to_string())
            })?;
            Ok((first_col.clone(), left_idx, second_col.clone(), right_idx))
        } else if first_in_right.is_some() && second_in_left.is_some() {
            // first is from right, second is from left
            let (left_idx, _) = second_in_left.ok_or_else(|| {
                LimboError::InternalError("second_in_left should exist".to_string())
            })?;
            let (right_idx, _) = first_in_right.ok_or_else(|| {
                LimboError::InternalError("first_in_right should exist".to_string())
            })?;
            Ok((second_col.clone(), left_idx, first_col.clone(), right_idx))
        } else {
            // Provide specific error messages for different failure cases
            if first_in_left.is_none() && first_in_right.is_none() {
                Err(LimboError::ParseError(format!(
                    "Join condition column '{}' not found in either input",
                    first_col.name
                )))
            } else if second_in_left.is_none() && second_in_right.is_none() {
                Err(LimboError::ParseError(format!(
                    "Join condition column '{}' not found in either input",
                    second_col.name
                )))
            } else {
                Err(LimboError::ParseError(format!(
                    "Join condition columns '{}' and '{}' must come from different input tables",
                    first_col.name, second_col.name
                )))
            }
        }
    }

    /// Compile a logical plan to a DBSP circuit
    pub fn compile(mut self, plan: &LogicalPlan) -> Result<DbspCircuit> {
        let root_id = self.compile_plan(plan)?;
        let output_schema = plan.schema().clone();
        self.circuit.set_root(root_id, output_schema);
        Ok(self.circuit)
    }

    /// Recursively compile a logical plan node
    fn compile_plan(&mut self, plan: &LogicalPlan) -> Result<i64> {
        match plan {
            LogicalPlan::Projection(proj) => {
                // Compile the input first
                let input_id = self.compile_plan(&proj.input)?;

                // Get input column names for the ProjectOperator
                let input_schema = proj.input.schema();
                let input_column_names: Vec<String> = input_schema.columns.iter()
                    .map(|col| col.name.clone())
                    .collect();

                // Convert logical expressions to DBSP expressions
                let dbsp_exprs = proj.exprs.iter()
                    .map(Self::compile_expr)
                    .collect::<Result<Vec<_>>>()?;

                // Compile logical expressions to CompiledExpressions
                let mut compiled_exprs = Vec::new();
                let mut aliases = Vec::new();
                for expr in &proj.exprs {
                    let (compiled, alias) = Self::compile_expression(expr, input_schema)?;
                    compiled_exprs.push(compiled);
                    aliases.push(alias);
                }

                // Get output column names from the projection schema
                let output_column_names: Vec<String> = proj.schema.columns.iter()
                    .map(|col| col.name.clone())
                    .collect();

                // Create the ProjectOperator
                let executable: Box<dyn IncrementalOperator> =
                    Box::new(ProjectOperator::from_compiled(compiled_exprs, aliases, input_column_names, output_column_names)?);

                // Create projection node
                let node_id = self.circuit.add_node(
                    DbspOperator::Projection {
                        exprs: dbsp_exprs,
                        schema: proj.schema.clone(),
                    },
                    vec![input_id],
                    executable,
                );
                Ok(node_id)
            }
            LogicalPlan::Filter(filter) => {
                // Compile the input first
                let input_id = self.compile_plan(&filter.input)?;

                // Get input schema for column resolution
                let input_schema = filter.input.schema();

                // Check if the predicate contains expressions that need to be computed
                if Self::predicate_needs_projection(&filter.predicate) {
                    // Complex expression in WHERE clause - need to add projection first
                    // 1. Create projection that adds the computed expression as a new column

                    // First, get all existing columns
                    let mut projection_exprs = Vec::new();
                    let mut dbsp_exprs = Vec::new();

                    for col in &input_schema.columns {
                        projection_exprs.push(LogicalExpr::Column(Column {
                            name: col.name.clone(),
                            table: None,
                        }));
                        dbsp_exprs.push(DbspExpr::Column(col.name.clone()));
                    }

                    // Now add the expression as a computed column
                    let temp_column_name = "__temp_filter_expr";
                    let computed_expr = Self::extract_expression_from_predicate(&filter.predicate)?;
                    projection_exprs.push(computed_expr);

                    // Compile the projection expressions
                    let mut compiled_exprs = Vec::new();
                    let mut aliases = Vec::new();
                    let mut output_names = Vec::new();
                    for (i, expr) in projection_exprs.iter().enumerate() {
                        let (compiled, _alias) = Self::compile_expression(expr, input_schema)?;
                        compiled_exprs.push(compiled);
                        if i < input_schema.columns.len() {
                            aliases.push(None);
                            output_names.push(input_schema.columns[i].name.clone());
                        } else {
                            aliases.push(Some(temp_column_name.to_string()));
                            output_names.push(temp_column_name.to_string());
                        }
                    }

                    // Get input column names for ProjectOperator
                    let input_column_names: Vec<String> = input_schema.columns.iter()
                        .map(|col| col.name.clone())
                        .collect();

                    // Create projection operator
                    let proj_executable: Box<dyn IncrementalOperator> =
                        Box::new(ProjectOperator::from_compiled(
                            compiled_exprs.clone(),
                            aliases.clone(),
                            input_column_names,
                            output_names.clone()
                        )?);

                    // Create updated schema for the projection output
                    let mut proj_schema_columns = input_schema.columns.clone();
                    proj_schema_columns.push(ColumnInfo {
                        name: temp_column_name.to_string(),
                        table: None,
                        database: None,
                        table_alias: None,
                        ty: Type::Integer,  // Computed expressions default to Integer
                    });
                    let proj_schema = SchemaRef::new(LogicalSchema {
                        columns: proj_schema_columns,
                    });

                    // Add projection node
                    let proj_id = self.circuit.add_node(
                        DbspOperator::Projection {
                            exprs: dbsp_exprs.clone(),
                            schema: proj_schema.clone(),
                        },
                        vec![input_id],
                        proj_executable,
                    );

                    // Now create a filter that replaces the complex expression with the temp column
                    // but keeps all other conditions intact
                    let replaced_predicate = Self::replace_complex_with_temp(&filter.predicate, temp_column_name)?;
                    let filter_predicate = Self::compile_filter_predicate(&replaced_predicate, &proj_schema)?;

                    let filter_executable: Box<dyn IncrementalOperator> =
                        Box::new(FilterOperator::new(filter_predicate));

                    // Create filter node
                    let filter_id = self.circuit.add_node(
                        DbspOperator::Filter { predicate: Self::compile_expr(&replaced_predicate)? },
                        vec![proj_id],
                        filter_executable,
                    );

                    // Finally, project again to remove the temporary column
                    let mut final_exprs = Vec::new();
                    let mut final_aliases = Vec::new();
                    let mut final_names = Vec::new();
                    let mut final_dbsp_exprs = Vec::new();

                    for (i, column) in input_schema.columns.iter().enumerate() {
                        let col_name = &column.name;
                        final_exprs.push(compiled_exprs[i].clone());
                        final_aliases.push(None);
                        final_names.push(col_name.clone());
                        final_dbsp_exprs.push(DbspExpr::Column(col_name.clone()));
                    }

                    // Input names for the final projection include the temp column
                    let filter_output_names = output_names.clone();

                    let final_proj_executable: Box<dyn IncrementalOperator> =
                        Box::new(ProjectOperator::from_compiled(
                            final_exprs,
                            final_aliases,
                            filter_output_names,
                            final_names.clone()
                        )?);

                    let final_id = self.circuit.add_node(
                        DbspOperator::Projection {
                            exprs: final_dbsp_exprs,
                            schema: input_schema.clone(),  // Back to original schema
                        },
                        vec![filter_id],
                        final_proj_executable,
                    );

                    Ok(final_id)
                } else {
                    // Simple filter - use existing implementation
                    // Convert predicate to DBSP expression
                    let dbsp_predicate = Self::compile_expr(&filter.predicate)?;

                    // Convert to FilterPredicate
                    let filter_predicate = Self::compile_filter_predicate(&filter.predicate, input_schema)?;

                    // Create executable operator
                    let executable: Box<dyn IncrementalOperator> =
                        Box::new(FilterOperator::new(filter_predicate));

                    // Create filter node
                    let node_id = self.circuit.add_node(
                        DbspOperator::Filter { predicate: dbsp_predicate },
                        vec![input_id],
                        executable,
                    );
                    Ok(node_id)
                }
            }
            LogicalPlan::Aggregate(agg) => {
                // Compile the input first
                let input_id = self.compile_plan(&agg.input)?;

                // Get input column names
                let input_schema = agg.input.schema();
                let input_column_names: Vec<String> = input_schema.columns.iter()
                    .map(|col| col.name.clone())
                    .collect();

                // Compile group by expressions to column indices
                let mut group_by_indices = Vec::new();
                let mut dbsp_group_exprs = Vec::new();
                for expr in &agg.group_expr {
                    // For now, only support simple column references in GROUP BY
                    if let LogicalExpr::Column(col) = expr {
                        // Find the column index in the input schema using qualified lookup
                        let (col_idx, _) = input_schema.find_column(&col.name, col.table.as_deref())
                            .ok_or_else(|| LimboError::ParseError(
                                format!("GROUP BY column '{}' not found in input", col.name)
                            ))?;
                        group_by_indices.push(col_idx);
                        dbsp_group_exprs.push(DbspExpr::Column(col.name.clone()));
                    } else {
                        return Err(LimboError::ParseError(
                            "Only column references are supported in GROUP BY for incremental views".to_string()
                        ));
                    }
                }

                // Compile aggregate expressions (both DISTINCT and regular)
                let mut aggregate_functions = Vec::new();
                for expr in &agg.aggr_expr {
                    if let LogicalExpr::AggregateFunction { fun, args, distinct } = expr {
                        use crate::function::AggFunc;
                        use crate::incremental::aggregate_operator::AggregateFunction;

                        match fun {
                            AggFunc::Count | AggFunc::Count0 => {
                                if *distinct {
                                    // COUNT(DISTINCT col)
                                    if args.is_empty() {
                                        return Err(LimboError::ParseError("COUNT(DISTINCT) requires an argument".to_string()));
                                    }
                                    if let LogicalExpr::Column(col) = &args[0] {
                                        let (col_idx, _) = input_schema.find_column(&col.name, col.table.as_deref())
                                            .ok_or_else(|| LimboError::ParseError(
                                                format!("COUNT(DISTINCT) column '{}' not found in input", col.name)
                                            ))?;
                                        aggregate_functions.push(AggregateFunction::CountDistinct(col_idx));
                                    } else {
                                        return Err(LimboError::ParseError(
                                            "Only column references are supported in aggregate functions for incremental views".to_string()
                                        ));
                                    }
                                } else {
                                    aggregate_functions.push(AggregateFunction::Count);
                                }
                            }
                            AggFunc::Sum => {
                                if args.is_empty() {
                                    return Err(LimboError::ParseError("SUM requires an argument".to_string()));
                                }
                                // Extract column index from the argument
                                if let LogicalExpr::Column(col) = &args[0] {
                                    let (col_idx, _) = input_schema.find_column(&col.name, col.table.as_deref())
                                        .ok_or_else(|| LimboError::ParseError(
                                            format!("SUM column '{}' not found in input", col.name)
                                        ))?;
                                    if *distinct {
                                        aggregate_functions.push(AggregateFunction::SumDistinct(col_idx));
                                    } else {
                                        aggregate_functions.push(AggregateFunction::Sum(col_idx));
                                    }
                                } else {
                                    return Err(LimboError::ParseError(
                                        "Only column references are supported in aggregate functions for incremental views".to_string()
                                    ));
                                }
                            }
                            AggFunc::Avg => {
                                if args.is_empty() {
                                    return Err(LimboError::ParseError("AVG requires an argument".to_string()));
                                }
                                if let LogicalExpr::Column(col) = &args[0] {
                                    let (col_idx, _) = input_schema.find_column(&col.name, col.table.as_deref())
                                        .ok_or_else(|| LimboError::ParseError(
                                            format!("AVG column '{}' not found in input", col.name)
                                        ))?;
                                    if *distinct {
                                        aggregate_functions.push(AggregateFunction::AvgDistinct(col_idx));
                                    } else {
                                        aggregate_functions.push(AggregateFunction::Avg(col_idx));
                                    }
                                } else {
                                    return Err(LimboError::ParseError(
                                        "Only column references are supported in aggregate functions for incremental views".to_string()
                                    ));
                                }
                            }
                            AggFunc::Min => {
                                if args.is_empty() {
                                    return Err(LimboError::ParseError("MIN requires an argument".to_string()));
                                }
                                if let LogicalExpr::Column(col) = &args[0] {
                                    let (col_idx, _) = input_schema.find_column(&col.name, col.table.as_deref())
                                        .ok_or_else(|| LimboError::ParseError(
                                            format!("MIN column '{}' not found in input", col.name)
                                        ))?;
                                    aggregate_functions.push(AggregateFunction::Min(col_idx));
                                } else {
                                    return Err(LimboError::ParseError(
                                        "Only column references are supported in MIN for incremental views".to_string()
                                    ));
                                }
                            }
                            AggFunc::Max => {
                                if args.is_empty() {
                                    return Err(LimboError::ParseError("MAX requires an argument".to_string()));
                                }
                                if let LogicalExpr::Column(col) = &args[0] {
                                    let (col_idx, _) = input_schema.find_column(&col.name, col.table.as_deref())
                                        .ok_or_else(|| LimboError::ParseError(
                                            format!("MAX column '{}' not found in input", col.name)
                                        ))?;
                                    aggregate_functions.push(AggregateFunction::Max(col_idx));
                                } else {
                                    return Err(LimboError::ParseError(
                                        "Only column references are supported in MAX for incremental views".to_string()
                                    ));
                                }
                            }
                            _ => {
                                return Err(LimboError::ParseError(
                                    format!("Unsupported aggregate function in DBSP compiler: {fun:?}")
                                ));
                            }
                        }
                    } else {
                        return Err(LimboError::ParseError(
                            "Expected aggregate function in aggregate expressions".to_string()
                        ));
                    }
                }

                let operator_id = self.circuit.next_id;

                use crate::incremental::aggregate_operator::AggregateOperator;
                let executable: Box<dyn IncrementalOperator> = Box::new(AggregateOperator::new(
                    operator_id,
                    group_by_indices.clone(),
                    aggregate_functions.clone(),
                    input_column_names,
                )?);

                let result_node_id = self.circuit.add_node(
                    DbspOperator::Aggregate {
                        group_exprs: dbsp_group_exprs,
                        aggr_exprs: aggregate_functions,
                        schema: agg.schema.clone(),
                    },
                    vec![input_id],
                    executable,
                );

                Ok(result_node_id)
            }
            LogicalPlan::Join(join) => {
                // Compile left and right inputs
                let left_id = self.compile_plan(&join.left)?;
                let right_id = self.compile_plan(&join.right)?;

                // Get schemas from inputs
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();

                // Get column names from left and right
                let left_columns: Vec<String> = left_schema.columns.iter()
                    .map(|col| col.name.clone())
                    .collect();
                let right_columns: Vec<String> = right_schema.columns.iter()
                    .map(|col| col.name.clone())
                    .collect();

                // Check if there are any non-equijoin conditions in the filter
                if join.filter.is_some() {
                    return Err(LimboError::ParseError(
                        "Non-equijoin conditions are not supported in materialized views. Only equality joins (=) are allowed.".to_string()
                    ));
                }

                // Check if we have at least one equijoin condition
                if join.on.is_empty() {
                    return Err(LimboError::ParseError(
                        "Joins in materialized views must have at least one equality condition.".to_string()
                    ));
                }

                // Extract join key indices from join conditions
                // For now, we only support equijoin conditions
                let mut left_key_indices = Vec::new();
                let mut right_key_indices = Vec::new();
                let mut dbsp_on_exprs = Vec::new();

                for (left_expr, right_expr) in &join.on {
                    // Extract column indices from join expressions
                    // We expect simple column references in join conditions
                    if let (LogicalExpr::Column(first_col), LogicalExpr::Column(second_col)) = (left_expr, right_expr) {
                        let (actual_left_col, actual_left_idx, actual_right_col, actual_right_idx) =
                            Self::resolve_join_columns(first_col, second_col, left_schema, right_schema)?;

                        left_key_indices.push(actual_left_idx);
                        right_key_indices.push(actual_right_idx);

                        // Convert to DBSP expressions
                        dbsp_on_exprs.push((
                            DbspExpr::Column(actual_left_col.name.clone()),
                            DbspExpr::Column(actual_right_col.name.clone())
                        ));
                    } else {
                        return Err(LimboError::ParseError(
                            "Only simple column references are supported in join conditions for incremental views".to_string()
                        ));
                    }
                }

                // Convert logical join type to operator join type
                let operator_join_type = match join.join_type {
                    LogicalJoinType::Inner => JoinType::Inner,
                    LogicalJoinType::Left => JoinType::Left,
                    LogicalJoinType::Right => JoinType::Right,
                    LogicalJoinType::Full => JoinType::Full,
                    LogicalJoinType::Cross => JoinType::Cross,
                };

                // Create JoinOperator
                let operator_id = self.circuit.next_id;
                let executable: Box<dyn IncrementalOperator> = Box::new(JoinOperator::new(
                    operator_id,
                    operator_join_type.clone(),
                    left_key_indices,
                    right_key_indices,
                    left_columns,
                    right_columns,
                )?);

                // Create join node
                let node_id = self.circuit.add_node(
                    DbspOperator::Join {
                        join_type: operator_join_type,
                        on_exprs: dbsp_on_exprs,
                        schema: join.schema.clone(),
                    },
                    vec![left_id, right_id],
                    executable,
                );
                Ok(node_id)
            }
            LogicalPlan::TableScan(scan) => {
                // Create input node with InputOperator for uniform handling
                let executable: Box<dyn IncrementalOperator> =
                    Box::new(InputOperator::new(scan.table_name.clone()));

                let node_id = self.circuit.add_node(
                    DbspOperator::Input {
                        name: scan.table_name.clone(),
                        schema: scan.schema.clone(),
                    },
                    vec![],
                    executable,
                );
                Ok(node_id)
            }
            LogicalPlan::Union(union) => {
                // Handle UNION and UNION ALL
                self.compile_union(union)
            }
            LogicalPlan::Distinct(distinct) => {
                // DISTINCT is implemented as GROUP BY all columns with a special aggregate
                let input_id = self.compile_plan(&distinct.input)?;
                let input_schema = distinct.input.schema();

                // Create GROUP BY indices for all columns
                let group_by: Vec<usize> = (0..input_schema.columns.len()).collect();

                // Column names for the operator
                let input_column_names: Vec<String> = input_schema.columns.iter()
                    .map(|col| col.name.clone())
                    .collect();

                // Create the aggregate operator with DISTINCT mode
                let operator_id = self.circuit.next_id;
                let executable: Box<dyn IncrementalOperator> = Box::new(
                    AggregateOperator::new(
                        operator_id,
                        group_by,
                        vec![], // Empty aggregates indicates plain DISTINCT
                        input_column_names,
                    )?,
                );

                // Add the node to the circuit
                let node_id = self.circuit.add_node(
                    DbspOperator::Distinct {
                        schema: input_schema.clone(),
                    },
                    vec![input_id],
                    executable,
                );

                Ok(node_id)
            }
            _ => Err(LimboError::ParseError(
                format!("Unsupported operator in DBSP compiler: only Filter, Projection, Join, Aggregate, and Union are supported, got: {:?}",
                    match plan {
                        LogicalPlan::Sort(_) => "Sort",
                        LogicalPlan::Limit(_) => "Limit",
                        LogicalPlan::Union(_) => "Union",
                                    LogicalPlan::EmptyRelation(_) => "EmptyRelation",
                        LogicalPlan::Values(_) => "Values",
                        LogicalPlan::WithCTE(_) => "WithCTE",
                        LogicalPlan::CTERef(_) => "CTERef",
                        _ => "Unknown",
                    }
                )
            )),
        }
    }

    /// Extract a representative table name from a logical plan (for UNION ALL identification)
    /// Returns a string that uniquely identifies the source of the data
    fn extract_source_identifier(plan: &LogicalPlan) -> String {
        match plan {
            LogicalPlan::TableScan(scan) => {
                // Direct table scan - use the table name
                scan.table_name.clone()
            }
            LogicalPlan::Projection(proj) => {
                // Pass through to input
                Self::extract_source_identifier(&proj.input)
            }
            LogicalPlan::Filter(filter) => {
                // Pass through to input
                Self::extract_source_identifier(&filter.input)
            }
            LogicalPlan::Aggregate(agg) => {
                // Aggregate of a table
                format!("agg_{}", Self::extract_source_identifier(&agg.input))
            }
            LogicalPlan::Sort(sort) => {
                // Pass through to input
                Self::extract_source_identifier(&sort.input)
            }
            LogicalPlan::Limit(limit) => {
                // Pass through to input
                Self::extract_source_identifier(&limit.input)
            }
            LogicalPlan::Join(join) => {
                // Join of two sources - combine their identifiers
                let left_id = Self::extract_source_identifier(&join.left);
                let right_id = Self::extract_source_identifier(&join.right);
                format!("join_{left_id}_{right_id}")
            }
            LogicalPlan::Union(union) => {
                // Union of multiple sources
                if union.inputs.is_empty() {
                    "union_empty".to_string()
                } else {
                    let identifiers: Vec<String> = union
                        .inputs
                        .iter()
                        .map(|input| Self::extract_source_identifier(input))
                        .collect();
                    format!("union_{}", identifiers.join("_"))
                }
            }
            LogicalPlan::Distinct(distinct) => {
                // Distinct of a source
                format!(
                    "distinct_{}",
                    Self::extract_source_identifier(&distinct.input)
                )
            }
            LogicalPlan::WithCTE(with_cte) => {
                // CTE body
                Self::extract_source_identifier(&with_cte.body)
            }
            LogicalPlan::CTERef(cte_ref) => {
                // CTE reference - use the CTE name
                format!("cte_{}", cte_ref.name)
            }
            LogicalPlan::EmptyRelation(_) => "empty".to_string(),
            LogicalPlan::Values(_) => "values".to_string(),
        }
    }

    /// Compile a UNION operator
    fn compile_union(&mut self, union: &crate::translate::logical::Union) -> Result<i64> {
        if union.inputs.len() != 2 {
            return Err(LimboError::ParseError(format!(
                "UNION requires exactly 2 inputs, got {}",
                union.inputs.len()
            )));
        }

        // Extract source identifiers from each input (for UNION ALL)
        let left_source = Self::extract_source_identifier(&union.inputs[0]);
        let right_source = Self::extract_source_identifier(&union.inputs[1]);

        // Compile left and right inputs
        let left_id = self.compile_plan(&union.inputs[0])?;
        let right_id = self.compile_plan(&union.inputs[1])?;

        use crate::incremental::merge_operator::{MergeOperator, UnionMode};

        // Create a merge operator that handles the rowid transformation
        let operator_id = self.circuit.next_id;
        let mode = if union.all {
            // For UNION ALL, pass the source identifiers
            UnionMode::All {
                left_table: left_source,
                right_table: right_source,
            }
        } else {
            UnionMode::Distinct
        };
        let merge_operator = Box::new(MergeOperator::new(operator_id, mode));

        let merge_id = self.circuit.add_node(
            DbspOperator::Merge {
                schema: union.schema.clone(),
            },
            vec![left_id, right_id],
            merge_operator,
        );

        Ok(merge_id)
    }

    /// Convert a logical expression to a DBSP expression
    fn compile_expr(expr: &LogicalExpr) -> Result<DbspExpr> {
        match expr {
            LogicalExpr::Column(col) => Ok(DbspExpr::Column(col.name.clone())),

            LogicalExpr::Literal(val) => Ok(DbspExpr::Literal(val.clone())),

            LogicalExpr::BinaryExpr { left, op, right } => {
                let left_expr = Self::compile_expr(left)?;
                let right_expr = Self::compile_expr(right)?;

                Ok(DbspExpr::BinaryExpr {
                    left: Box::new(left_expr),
                    op: *op,
                    right: Box::new(right_expr),
                })
            }

            LogicalExpr::Alias { expr, .. } => {
                // For aliases, compile the underlying expression
                Self::compile_expr(expr)
            }

            // For complex expressions (functions, etc), we can't represent them as DbspExpr
            // but that's OK - they'll be handled by the ProjectOperator's VDBE compilation
            // For now, just use a placeholder
            _ => {
                // Use a literal null as placeholder - the actual execution will use the compiled VDBE
                Ok(DbspExpr::Literal(Value::Null))
            }
        }
    }

    /// Compile a logical expression to a CompiledExpression and optional alias
    fn compile_expression(
        expr: &LogicalExpr,
        input_schema: &LogicalSchema,
    ) -> Result<(CompiledExpression, Option<String>)> {
        // Check for alias first
        if let LogicalExpr::Alias { expr, alias } = expr {
            // For aliases, compile the underlying expression and return with alias
            let (compiled, _) = Self::compile_expression(expr, input_schema)?;
            return Ok((compiled, Some(alias.clone())));
        }

        // Convert LogicalExpr to AST Expr with proper column resolution
        let ast_expr = Self::logical_to_ast_expr_with_schema(expr, input_schema)?;

        // Extract column names from schema for CompiledExpression::compile
        let input_column_names: Vec<String> = input_schema
            .columns
            .iter()
            .map(|col| col.name.clone())
            .collect();

        // For all expressions (simple or complex), use CompiledExpression::compile
        // This handles both trivial cases and complex VDBE compilation
        // We need to set up the necessary context
        use crate::sync::Arc;
        use crate::{Database, MemoryIO, SymbolTable};

        // Create an internal connection for expression compilation
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:")?;
        let internal_conn = db.connect()?;
        internal_conn.set_query_only(true);
        internal_conn.auto_commit.store(false, Ordering::SeqCst);

        // Create temporary symbol table
        let temp_syms = SymbolTable::new();

        // Get a minimal schema for compilation (we don't need the full schema for expressions)
        let schema = crate::schema::Schema::new();

        // Compile the expression using the existing CompiledExpression::compile
        let compiled = CompiledExpression::compile(
            &ast_expr,
            &input_column_names,
            &schema,
            &temp_syms,
            internal_conn,
        )?;

        Ok((compiled, None))
    }

    /// Convert LogicalExpr to AST Expr with qualified column resolution
    fn logical_to_ast_expr_with_schema(
        expr: &LogicalExpr,
        schema: &LogicalSchema,
    ) -> Result<turso_parser::ast::Expr> {
        use turso_parser::ast;

        match expr {
            LogicalExpr::Column(col) => {
                // Find the column index using qualified lookup
                let (idx, _) = schema
                    .find_column(&col.name, col.table.as_deref())
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "Column '{}' with table {:?} not found in schema",
                            col.name, col.table
                        ))
                    })?;
                // Return a Register expression with the correct index
                Ok(ast::Expr::Register(idx))
            }
            LogicalExpr::Literal(val) => {
                let lit = match val {
                    Value::Numeric(Numeric::Integer(i)) => ast::Literal::Numeric(i.to_string()),
                    Value::Numeric(Numeric::Float(f)) => {
                        ast::Literal::Numeric(f64::from(*f).to_string())
                    }
                    Value::Text(t) => {
                        // Add quotes for string literals as translate_expr expects them
                        // Also escape any single quotes in the string
                        let escaped = t.to_string().replace('\'', "''");
                        ast::Literal::String(format!("'{escaped}'"))
                    }
                    Value::Blob(b) => ast::Literal::Blob(format!("{b:?}")),
                    Value::Null => ast::Literal::Null,
                };
                Ok(ast::Expr::Literal(lit))
            }
            LogicalExpr::BinaryExpr { left, op, right } => {
                let left_expr = Self::logical_to_ast_expr_with_schema(left, schema)?;
                let right_expr = Self::logical_to_ast_expr_with_schema(right, schema)?;
                Ok(ast::Expr::Binary(
                    Box::new(left_expr),
                    *op,
                    Box::new(right_expr),
                ))
            }
            LogicalExpr::ScalarFunction { fun, args } => {
                let ast_args: Result<Vec<_>> = args
                    .iter()
                    .map(|arg| Self::logical_to_ast_expr_with_schema(arg, schema))
                    .collect();
                let ast_args: Vec<Box<ast::Expr>> = ast_args?.into_iter().map(Box::new).collect();
                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::exact(fun.clone()),
                    distinctness: None,
                    args: ast_args,
                    order_by: Vec::new(),
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                })
            }
            LogicalExpr::Alias { expr, .. } => {
                // For conversion to AST, ignore the alias and convert the inner expression
                Self::logical_to_ast_expr_with_schema(expr, schema)
            }
            LogicalExpr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                // Convert aggregate function to AST
                let ast_args: Result<Vec<_>> = args
                    .iter()
                    .map(|arg| Self::logical_to_ast_expr_with_schema(arg, schema))
                    .collect();
                let ast_args: Vec<Box<ast::Expr>> = ast_args?.into_iter().map(Box::new).collect();

                // Get the function name based on the aggregate type
                let func_name = match fun {
                    crate::function::AggFunc::Count => "COUNT",
                    crate::function::AggFunc::Sum => "SUM",
                    crate::function::AggFunc::Avg => "AVG",
                    crate::function::AggFunc::Min => "MIN",
                    crate::function::AggFunc::Max => "MAX",
                    _ => {
                        return Err(LimboError::ParseError(format!(
                            "Unsupported aggregate function: {fun:?}"
                        )))
                    }
                };

                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::exact(func_name.to_string()),
                    distinctness: if *distinct {
                        Some(ast::Distinctness::Distinct)
                    } else {
                        None
                    },
                    args: ast_args,
                    order_by: Vec::new(),
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                })
            }
            LogicalExpr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // BETWEEN x AND y is rewritten as (expr >= x AND expr <= y)
                // NOT BETWEEN x AND y is rewritten as (expr < x OR expr > y)
                let expr_ast = Self::logical_to_ast_expr_with_schema(expr, schema)?;
                let low_ast = Self::logical_to_ast_expr_with_schema(low, schema)?;
                let high_ast = Self::logical_to_ast_expr_with_schema(high, schema)?;

                if *negated {
                    // NOT BETWEEN: (expr < low OR expr > high)
                    Ok(ast::Expr::Binary(
                        Box::new(ast::Expr::Binary(
                            Box::new(expr_ast.clone()),
                            ast::Operator::Less,
                            Box::new(low_ast),
                        )),
                        ast::Operator::Or,
                        Box::new(ast::Expr::Binary(
                            Box::new(expr_ast),
                            ast::Operator::Greater,
                            Box::new(high_ast),
                        )),
                    ))
                } else {
                    // BETWEEN: (expr >= low AND expr <= high)
                    Ok(ast::Expr::Binary(
                        Box::new(ast::Expr::Binary(
                            Box::new(expr_ast.clone()),
                            ast::Operator::GreaterEquals,
                            Box::new(low_ast),
                        )),
                        ast::Operator::And,
                        Box::new(ast::Expr::Binary(
                            Box::new(expr_ast),
                            ast::Operator::LessEquals,
                            Box::new(high_ast),
                        )),
                    ))
                }
            }
            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => {
                let lhs = Box::new(Self::logical_to_ast_expr_with_schema(expr, schema)?);
                let values: Result<Vec<_>> = list
                    .iter()
                    .map(|item| {
                        let ast_expr = Self::logical_to_ast_expr_with_schema(item, schema)?;
                        Ok(Box::new(ast_expr))
                    })
                    .collect();
                Ok(ast::Expr::InList {
                    lhs,
                    not: *negated,
                    rhs: values?,
                })
            }
            LogicalExpr::Like {
                expr,
                pattern,
                escape,
                negated,
            } => {
                let lhs = Box::new(Self::logical_to_ast_expr_with_schema(expr, schema)?);
                let rhs = Box::new(Self::logical_to_ast_expr_with_schema(pattern, schema)?);
                let escape_expr = escape
                    .map(|c| Box::new(ast::Expr::Literal(ast::Literal::String(c.to_string()))));
                Ok(ast::Expr::Like {
                    lhs,
                    not: *negated,
                    op: ast::LikeOperator::Like,
                    rhs,
                    escape: escape_expr,
                })
            }
            LogicalExpr::IsNull { expr, negated } => {
                let inner_expr = Box::new(Self::logical_to_ast_expr_with_schema(expr, schema)?);
                if *negated {
                    // IS NOT NULL needs to be represented differently
                    Ok(ast::Expr::Unary(
                        ast::UnaryOperator::Not,
                        Box::new(ast::Expr::IsNull(inner_expr)),
                    ))
                } else {
                    Ok(ast::Expr::IsNull(inner_expr))
                }
            }
            LogicalExpr::Cast { expr, type_name } => {
                let inner_expr = Box::new(Self::logical_to_ast_expr_with_schema(expr, schema)?);
                Ok(ast::Expr::Cast {
                    expr: inner_expr,
                    type_name: type_name.clone(),
                })
            }
            _ => Err(LimboError::ParseError(format!(
                "Cannot convert LogicalExpr to AST Expr: {expr:?}"
            ))),
        }
    }

    /// Check if a predicate contains expressions that need projection
    fn predicate_needs_projection(expr: &LogicalExpr) -> bool {
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => {
                // Only these specific simple patterns DON'T need projection
                match (left.as_ref(), right.as_ref()) {
                    // Simple column to literal comparisons
                    (LogicalExpr::Column(_), LogicalExpr::Literal(_))
                        if matches!(
                            op,
                            BinaryOperator::Equals
                                | BinaryOperator::NotEquals
                                | BinaryOperator::Greater
                                | BinaryOperator::GreaterEquals
                                | BinaryOperator::Less
                                | BinaryOperator::LessEquals
                        ) =>
                    {
                        false
                    }

                    // Simple column to column comparisons
                    (LogicalExpr::Column(_), LogicalExpr::Column(_))
                        if matches!(
                            op,
                            BinaryOperator::Equals
                                | BinaryOperator::NotEquals
                                | BinaryOperator::Greater
                                | BinaryOperator::GreaterEquals
                                | BinaryOperator::Less
                                | BinaryOperator::LessEquals
                        ) =>
                    {
                        false
                    }

                    // AND/OR of simple expressions - check recursively
                    _ if matches!(op, BinaryOperator::And | BinaryOperator::Or) => {
                        Self::predicate_needs_projection(left)
                            || Self::predicate_needs_projection(right)
                    }

                    // Everything else needs projection
                    _ => true,
                }
            }
            // These simple cases don't need projection
            LogicalExpr::Column(_) | LogicalExpr::Literal(_) => false,

            // Default: assume we need projection for safety
            // This includes: Between, InList, Like, IsNull, Cast, ScalarFunction, Case,
            // InSubquery, Exists, ScalarSubquery, and any future expression types
            _ => true,
        }
    }

    /// Extract the expression part from a predicate that needs to be computed
    fn extract_expression_from_predicate(expr: &LogicalExpr) -> Result<LogicalExpr> {
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => {
                // Handle AND/OR - recursively find the complex expression
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    // Check left side first
                    if Self::predicate_needs_projection(left) {
                        return Self::extract_expression_from_predicate(left);
                    }
                    // Then check right side
                    if Self::predicate_needs_projection(right) {
                        return Self::extract_expression_from_predicate(right);
                    }
                    // Neither side needs projection (shouldn't happen if predicate_needs_projection was true)
                    return Ok(expr.clone());
                }

                // For comparison expressions, check if we need to extract a subexpression
                if matches!(
                    op,
                    BinaryOperator::Greater
                        | BinaryOperator::GreaterEquals
                        | BinaryOperator::Less
                        | BinaryOperator::LessEquals
                        | BinaryOperator::Equals
                        | BinaryOperator::NotEquals
                ) {
                    // If the left side is complex (not a column), extract it
                    if !matches!(
                        left.as_ref(),
                        LogicalExpr::Column(_) | LogicalExpr::Literal(_)
                    ) {
                        return Ok((**left).clone());
                    }
                    // If the right side is complex (not a literal), extract it
                    if !matches!(
                        right.as_ref(),
                        LogicalExpr::Column(_) | LogicalExpr::Literal(_)
                    ) {
                        return Ok((**right).clone());
                    }
                    // Both sides are simple but the expression as a whole might need projection
                    // (e.g., for arithmetic operations)
                    Ok(expr.clone())
                } else {
                    // For other binary operators (arithmetic, etc.), return the whole expression
                    Ok(expr.clone())
                }
            }
            // For non-binary expressions (BETWEEN, IN, LIKE, functions, etc.),
            // we need to compute the whole expression as a boolean
            _ => Ok(expr.clone()),
        }
    }

    /// Replace complex expressions in the predicate with references to the temp column
    fn replace_complex_with_temp(
        expr: &LogicalExpr,
        temp_column_name: &str,
    ) -> Result<LogicalExpr> {
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => {
                // Handle AND/OR - recursively process both sides
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    let new_left = Self::replace_complex_with_temp(left, temp_column_name)?;
                    let new_right = Self::replace_complex_with_temp(right, temp_column_name)?;
                    return Ok(LogicalExpr::BinaryExpr {
                        left: Box::new(new_left),
                        op: *op,
                        right: Box::new(new_right),
                    });
                }

                // Check if this is a complex comparison that needs replacement
                if Self::predicate_needs_projection(expr) {
                    // Determine which side is complex and needs replacement
                    let left_is_simple = matches!(
                        left.as_ref(),
                        LogicalExpr::Column(_) | LogicalExpr::Literal(_)
                    );
                    let right_is_simple = matches!(
                        right.as_ref(),
                        LogicalExpr::Column(_) | LogicalExpr::Literal(_)
                    );

                    if !left_is_simple {
                        // Left side is complex - replace it with temp column
                        return Ok(LogicalExpr::BinaryExpr {
                            left: Box::new(LogicalExpr::Column(Column {
                                name: temp_column_name.to_string(),
                                table: None,
                            })),
                            op: *op,
                            right: right.clone(),
                        });
                    } else if !right_is_simple {
                        // Right side is complex - replace it with temp column
                        return Ok(LogicalExpr::BinaryExpr {
                            left: left.clone(),
                            op: *op,
                            right: Box::new(LogicalExpr::Column(Column {
                                name: temp_column_name.to_string(),
                                table: None,
                            })),
                        });
                    } else {
                        // Both sides are simple, but the expression as a whole needs projection
                        // This shouldn't happen normally, but keep the expression as-is
                        return Ok(expr.clone());
                    }
                }

                // Simple comparison - keep as is
                Ok(expr.clone())
            }
            // For non-binary expressions that need projection (BETWEEN, IN, etc.),
            // replace the whole expression with a column reference to the temp column
            // The temp column will hold the boolean result of evaluating the expression
            _ if Self::predicate_needs_projection(expr) => {
                // The complex expression result is in the temp column
                // We need to check if it's true (non-zero)
                Ok(LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(Column {
                        name: temp_column_name.to_string(),
                        table: None,
                    })),
                    op: BinaryOperator::Equals,
                    right: Box::new(LogicalExpr::Literal(Value::from_i64(1))), // true = 1 in SQL
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    /// Compile a logical expression to a FilterPredicate for execution
    fn compile_filter_predicate(
        expr: &LogicalExpr,
        schema: &LogicalSchema,
    ) -> Result<FilterPredicate> {
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => {
                // Extract column name and value for simple predicates
                // First check for column-to-column comparisons
                if let (LogicalExpr::Column(left_col), LogicalExpr::Column(right_col)) =
                    (left.as_ref(), right.as_ref())
                {
                    // Resolve both column names to indices
                    let left_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == left_col.name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "Column '{}' not found in schema for filter",
                                left_col.name
                            ))
                        })?;

                    let right_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == right_col.name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "Column '{}' not found in schema for filter",
                                right_col.name
                            ))
                        })?;

                    match op {
                        BinaryOperator::Equals => Ok(FilterPredicate::ColumnEquals {
                            left_idx,
                            right_idx,
                        }),
                        BinaryOperator::NotEquals => Ok(FilterPredicate::ColumnNotEquals {
                            left_idx,
                            right_idx,
                        }),
                        BinaryOperator::Greater => Ok(FilterPredicate::ColumnGreaterThan {
                            left_idx,
                            right_idx,
                        }),
                        BinaryOperator::GreaterEquals => {
                            Ok(FilterPredicate::ColumnGreaterThanOrEqual {
                                left_idx,
                                right_idx,
                            })
                        }
                        BinaryOperator::Less => Ok(FilterPredicate::ColumnLessThan {
                            left_idx,
                            right_idx,
                        }),
                        BinaryOperator::LessEquals => Ok(FilterPredicate::ColumnLessThanOrEqual {
                            left_idx,
                            right_idx,
                        }),
                        BinaryOperator::And | BinaryOperator::Or => {
                            // Handle logical operators recursively
                            let left_pred = Self::compile_filter_predicate(left, schema)?;
                            let right_pred = Self::compile_filter_predicate(right, schema)?;
                            match op {
                                BinaryOperator::And => Ok(FilterPredicate::And(
                                    Box::new(left_pred),
                                    Box::new(right_pred),
                                )),
                                BinaryOperator::Or => Ok(FilterPredicate::Or(
                                    Box::new(left_pred),
                                    Box::new(right_pred),
                                )),
                                _ => unreachable!(),
                            }
                        }
                        _ => Err(LimboError::ParseError(format!(
                            "Unsupported operator in filter: {op:?}"
                        ))),
                    }
                } else if let (LogicalExpr::Column(col), LogicalExpr::Literal(val)) =
                    (left.as_ref(), right.as_ref())
                {
                    // Column-to-literal comparisons
                    let column_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == col.name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "Column '{}' not found in schema for filter",
                                col.name
                            ))
                        })?;

                    match op {
                        BinaryOperator::Equals => Ok(FilterPredicate::Equals {
                            column_idx,
                            value: val.clone(),
                        }),
                        BinaryOperator::NotEquals => Ok(FilterPredicate::NotEquals {
                            column_idx,
                            value: val.clone(),
                        }),
                        BinaryOperator::Greater => Ok(FilterPredicate::GreaterThan {
                            column_idx,
                            value: val.clone(),
                        }),
                        BinaryOperator::GreaterEquals => Ok(FilterPredicate::GreaterThanOrEqual {
                            column_idx,
                            value: val.clone(),
                        }),
                        BinaryOperator::Less => Ok(FilterPredicate::LessThan {
                            column_idx,
                            value: val.clone(),
                        }),
                        BinaryOperator::LessEquals => Ok(FilterPredicate::LessThanOrEqual {
                            column_idx,
                            value: val.clone(),
                        }),
                        BinaryOperator::And => {
                            // Handle AND of two predicates
                            let left_pred = Self::compile_filter_predicate(left, schema)?;
                            let right_pred = Self::compile_filter_predicate(right, schema)?;
                            Ok(FilterPredicate::And(
                                Box::new(left_pred),
                                Box::new(right_pred),
                            ))
                        }
                        BinaryOperator::Or => {
                            // Handle OR of two predicates
                            let left_pred = Self::compile_filter_predicate(left, schema)?;
                            let right_pred = Self::compile_filter_predicate(right, schema)?;
                            Ok(FilterPredicate::Or(
                                Box::new(left_pred),
                                Box::new(right_pred),
                            ))
                        }
                        _ => Err(LimboError::ParseError(format!(
                            "Unsupported operator in filter: {op:?}"
                        ))),
                    }
                } else if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    // Handle logical operators
                    let left_pred = Self::compile_filter_predicate(left, schema)?;
                    let right_pred = Self::compile_filter_predicate(right, schema)?;
                    match op {
                        BinaryOperator::And => Ok(FilterPredicate::And(
                            Box::new(left_pred),
                            Box::new(right_pred),
                        )),
                        BinaryOperator::Or => Ok(FilterPredicate::Or(
                            Box::new(left_pred),
                            Box::new(right_pred),
                        )),
                        _ => unreachable!(),
                    }
                } else {
                    Err(LimboError::ParseError(
                        "Filter predicate must be column op value or column op column".to_string(),
                    ))
                }
            }
            LogicalExpr::IsNull { expr, negated } => {
                // Extract column index from the inner expression
                if let LogicalExpr::Column(col) = expr.as_ref() {
                    let column_idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == col.name)
                        .ok_or_else(|| {
                            LimboError::ParseError(format!(
                                "Column '{}' not found in schema for IS NULL filter",
                                col.name
                            ))
                        })?;

                    if *negated {
                        Ok(FilterPredicate::IsNotNull { column_idx })
                    } else {
                        Ok(FilterPredicate::IsNull { column_idx })
                    }
                } else {
                    Err(LimboError::ParseError(
                        "IS NULL/IS NOT NULL expects a column reference".to_string(),
                    ))
                }
            }
            _ => Err(LimboError::ParseError(format!(
                "Unsupported filter expression: {expr:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental::dbsp::Delta;
    use crate::incremental::operator::{FilterOperator, FilterPredicate};
    use crate::schema::{BTreeTable, ColDef, Column as SchemaColumn, Schema, Type};
    use crate::storage::pager::CreateBTreeFlags;
    use crate::sync::Arc;
    use crate::translate::logical::{ColumnInfo, LogicalPlanBuilder, LogicalSchema};
    use crate::util::IOExt;
    use crate::{Database, MemoryIO, Pager, IO};
    use rustc_hash::FxHashSet as HashSet;
    use turso_parser::ast;
    use turso_parser::parser::Parser;

    // Macro to create a test schema with a users table
    macro_rules! test_schema {
        () => {{
            let mut schema = Schema::new();
            let users_table = BTreeTable {
                name: "users".to_string(),
                root_page: 2,
                primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
                columns: vec![
                    SchemaColumn::new(
                        Some("id".to_string()),
                        "INTEGER".to_string(),
                        None,
                        None,
                        Type::Integer,
                        None,
                        ColDef {
                            primary_key: true,
                            rowid_alias: true,
                            notnull: true,
                            ..Default::default()
                        },
                    ),
                    SchemaColumn::new_default_text(
                        Some("name".to_string()),
                        "TEXT".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("age".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(users_table))
                .expect("Test setup: failed to add users table");

            // Add products table for join tests
            let products_table = BTreeTable {
                name: "products".to_string(),
                root_page: 3,
                primary_key_columns: vec![(
                    "product_id".to_string(),
                    turso_parser::ast::SortOrder::Asc,
                )],
                columns: vec![
                    SchemaColumn::new(
                        Some("product_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                        None,
                        Type::Integer,
                        None,
                        ColDef {
                            primary_key: true,
                            rowid_alias: true,
                            notnull: true,
                            ..Default::default()
                        },
                    ),
                    SchemaColumn::new_default_text(
                        Some("product_name".to_string()),
                        "TEXT".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("price".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(products_table))
                .expect("Test setup: failed to add products table");

            // Add orders table for join tests
            let orders_table = BTreeTable {
                name: "orders".to_string(),
                root_page: 4,
                primary_key_columns: vec![(
                    "order_id".to_string(),
                    turso_parser::ast::SortOrder::Asc,
                )],
                columns: vec![
                    SchemaColumn::new(
                        Some("order_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                        None,
                        Type::Integer,
                        None,
                        ColDef {
                            primary_key: true,
                            rowid_alias: true,
                            notnull: true,
                            ..Default::default()
                        },
                    ),
                    SchemaColumn::new_default_integer(
                        Some("user_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("product_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("quantity".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                has_autoincrement: false,
                is_strict: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(orders_table))
                .expect("Test setup: failed to add orders table");

            // Add customers table with id and name for testing column ambiguity
            let customers_table = BTreeTable {
                name: "customers".to_string(),
                root_page: 6,
                primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
                columns: vec![
                    SchemaColumn::new(
                        Some("id".to_string()),
                        "INTEGER".to_string(),
                        None,
                        None,
                        Type::Integer,
                        None,
                        ColDef {
                            primary_key: true,
                            rowid_alias: true,
                            notnull: true,
                            ..Default::default()
                        },
                    ),
                    SchemaColumn::new_default_text(
                        Some("name".to_string()),
                        "TEXT".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(customers_table))
                .expect("Test setup: failed to add customers table");

            // Add purchases table (junction table for three-way join)
            let purchases_table = BTreeTable {
                name: "purchases".to_string(),
                root_page: 7,
                primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
                columns: vec![
                    SchemaColumn::new(
                        Some("id".to_string()),
                        "INTEGER".to_string(),
                        None,
                        None,
                        Type::Integer,
                        None,
                        ColDef {
                            primary_key: true,
                            rowid_alias: true,
                            notnull: true,
                            ..Default::default()
                        },
                    ),
                    SchemaColumn::new_default_integer(
                        Some("customer_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("vendor_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("quantity".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(purchases_table))
                .expect("Test setup: failed to add purchases table");

            // Add vendors table with id, name, and price (ambiguous columns with customers)
            let vendors_table = BTreeTable {
                name: "vendors".to_string(),
                root_page: 8,
                primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
                columns: vec![
                    SchemaColumn::new(
                        Some("id".to_string()),
                        "INTEGER".to_string(),
                        None,
                        None,
                        Type::Integer,
                        None,
                        ColDef {
                            primary_key: true,
                            rowid_alias: true,
                            notnull: true,
                            ..Default::default()
                        },
                    ),
                    SchemaColumn::new_default_text(
                        Some("name".to_string()),
                        "TEXT".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("price".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(vendors_table))
                .expect("Test setup: failed to add vendors table");

            let sales_table = BTreeTable {
                name: "sales".to_string(),
                root_page: 2,
                primary_key_columns: vec![],
                columns: vec![
                    SchemaColumn::new_default_integer(
                        Some("product_id".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                    SchemaColumn::new_default_integer(
                        Some("amount".to_string()),
                        "INTEGER".to_string(),
                        None,
                    ),
                ],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                unique_sets: vec![],
                foreign_keys: vec![],
                check_constraints: vec![],
                stored_gen_col_order: vec![],
            };
            schema
                .add_btree_table(Arc::new(sales_table))
                .expect("Test setup: failed to add sales table");

            schema
        }};
    }

    fn setup_btree_for_circuit() -> (Arc<Pager>, i64, i64, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();

        let _ = pager.io.block(|| pager.allocate_page1()).unwrap();

        let main_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as i64;

        let dbsp_state_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as i64;

        let dbsp_state_index_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
            .unwrap() as i64;

        (
            pager,
            main_root_page,
            dbsp_state_page,
            dbsp_state_index_page,
        )
    }

    // Macro to compile SQL to DBSP circuit
    macro_rules! compile_sql {
        ($sql:expr) => {{
            let (pager, main_root_page, dbsp_state_page, dbsp_state_index_page) =
                setup_btree_for_circuit();
            let schema = test_schema!();
            let mut parser = Parser::new($sql.as_bytes());
            let cmd = parser
                .next()
                .unwrap() // This returns Option<Result<Cmd, Error>>
                .unwrap(); // This unwraps the Result

            match cmd {
                ast::Cmd::Stmt(stmt) => {
                    let mut builder = LogicalPlanBuilder::new(&schema);
                    let logical_plan = builder.build_statement(&stmt).unwrap();
                    (
                        DbspCompiler::new(main_root_page, dbsp_state_page, dbsp_state_index_page)
                            .compile(&logical_plan)
                            .unwrap(),
                        pager,
                    )
                }
                _ => panic!("Only SQL statements are supported"),
            }
        }};
    }

    // Macro to assert circuit structure
    macro_rules! assert_circuit {
        ($circuit:expr, depth: $depth:expr, root: $root_type:ident) => {
            assert_eq!($circuit.nodes.len(), $depth);
            let node = get_node_at_level(&$circuit, 0);
            assert!(matches!(node.operator, DbspOperator::$root_type { .. }));
        };
    }

    // Macro to assert operator properties
    macro_rules! assert_operator {
        ($circuit:expr, $level:expr, Input { name: $name:expr }) => {{
            let node = get_node_at_level(&$circuit, $level);
            match &node.operator {
                DbspOperator::Input { name, .. } => assert_eq!(name, $name),
                _ => panic!("Expected Input operator at level {}", $level),
            }
        }};
        ($circuit:expr, $level:expr, Filter) => {{
            let node = get_node_at_level(&$circuit, $level);
            assert!(matches!(node.operator, DbspOperator::Filter { .. }));
        }};
        ($circuit:expr, $level:expr, Projection { columns: [$($col:expr),*] }) => {{
            let node = get_node_at_level(&$circuit, $level);
            match &node.operator {
                DbspOperator::Projection { exprs, .. } => {
                    let expected_cols = vec![$($col),*];
                    let actual_cols: Vec<String> = exprs.iter().map(|e| {
                        match e {
                            DbspExpr::Column(name) => name.clone(),
                            _ => "expr".to_string(),
                        }
                    }).collect();
                    assert_eq!(actual_cols, expected_cols);
                }
                _ => panic!("Expected Projection operator at level {}", $level),
            }
        }};
    }

    // Macro to assert filter predicate
    macro_rules! assert_filter_predicate {
        ($circuit:expr, $level:expr, $col:literal > $val:literal) => {{
            let node = get_node_at_level(&$circuit, $level);
            match &node.operator {
                DbspOperator::Filter { predicate } => match predicate {
                    DbspExpr::BinaryExpr { left, op, right } => {
                        assert!(matches!(op, ast::Operator::Greater));
                        assert!(matches!(&**left, DbspExpr::Column(name) if name == $col));
                        assert!(matches!(&**right, DbspExpr::Literal(Value::Numeric(Numeric::Integer($val)))));
                    }
                    _ => panic!("Expected binary expression in filter"),
                },
                _ => panic!("Expected Filter operator at level {}", $level),
            }
        }};
        ($circuit:expr, $level:expr, $col:literal < $val:literal) => {{
            let node = get_node_at_level(&$circuit, $level);
            match &node.operator {
                DbspOperator::Filter { predicate } => match predicate {
                    DbspExpr::BinaryExpr { left, op, right } => {
                        assert!(matches!(op, ast::Operator::Less));
                        assert!(matches!(&**left, DbspExpr::Column(name) if name == $col));
                        assert!(matches!(&**right, DbspExpr::Literal(Value::Numeric(Numeric::Integer($val)))));
                    }
                    _ => panic!("Expected binary expression in filter"),
                },
                _ => panic!("Expected Filter operator at level {}", $level),
            }
        }};
        ($circuit:expr, $level:expr, $col:literal = $val:literal) => {{
            let node = get_node_at_level(&$circuit, $level);
            match &node.operator {
                DbspOperator::Filter { predicate } => match predicate {
                    DbspExpr::BinaryExpr { left, op, right } => {
                        assert!(matches!(op, ast::Operator::Equals));
                        assert!(matches!(&**left, DbspExpr::Column(name) if name == $col));
                        assert!(matches!(&**right, DbspExpr::Literal(Value::Numeric(Numeric::Integer($val)))));
                    }
                    _ => panic!("Expected binary expression in filter"),
                },
                _ => panic!("Expected Filter operator at level {}", $level),
            }
        }};
    }

    // Helper to get node at specific level from root
    fn get_node_at_level(circuit: &DbspCircuit, level: usize) -> &DbspNode {
        let mut current_id = circuit.root.expect("Circuit has no root");
        for _ in 0..level {
            let node = circuit.nodes.get(&current_id).expect("Node not found");
            if node.inputs.is_empty() {
                panic!("No more levels available, requested level {level}");
            }
            current_id = node.inputs[0];
        }
        circuit.nodes.get(&current_id).expect("Node not found")
    }

    // Helper function for tests to execute circuit and extract the Delta result
    #[cfg(test)]
    fn test_execute(
        circuit: &mut DbspCircuit,
        inputs: HashMap<String, Delta>,
        pager: Arc<Pager>,
    ) -> Result<Delta> {
        let mut execute_state = ExecuteState::Init {
            input_data: DeltaSet::from_map(inputs),
        };
        match circuit.execute(pager, &mut execute_state)? {
            IOResult::Done(delta) => Ok(delta),
            IOResult::IO(_) => panic!("Unexpected I/O in test"),
        }
    }

    // Helper to get the committed BTree state from main_data_root
    // This reads the actual persisted data from the BTree
    #[cfg(test)]
    fn get_current_state(pager: Arc<Pager>, circuit: &DbspCircuit) -> Result<Delta> {
        use crate::storage::btree::CursorTrait;

        let mut delta = Delta::new();

        let main_data_root = circuit.main_data_root;
        let num_columns = circuit.output_schema.columns.len() + 1;

        // Create a cursor to read the btree
        let mut btree_cursor = BTreeCursor::new_table(pager.clone(), main_data_root, num_columns);

        // Rewind to the beginning
        pager.io.block(|| btree_cursor.rewind())?;

        // Read all rows from the BTree
        loop {
            // Check if cursor is empty (no more rows)
            if btree_cursor.is_empty() {
                break;
            }

            // Get the rowid
            let rowid = pager.io.block(|| btree_cursor.rowid()).unwrap().unwrap();

            // Get the record at this position
            let record = loop {
                match btree_cursor.record().unwrap() {
                    IOResult::Done(r) => break r,
                    IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
                }
            }
            .unwrap()
            .to_owned();

            let num_data_columns = record.column_count() - 1;

            let mut values = Vec::with_capacity(num_data_columns);
            let mut values_iter = record.iter()?;

            for _ in 0..num_data_columns {
                let value = values_iter.next().expect("we already checked bounds")?;
                values.push(value.to_owned());
            }

            delta.insert(rowid, values);
            pager.io.block(|| btree_cursor.next()).unwrap();
        }
        Ok(delta)
    }

    #[test]
    fn test_simple_projection() {
        let (circuit, _) = compile_sql!("SELECT name FROM users");

        // Circuit has 2 nodes with Projection at root
        assert_circuit!(circuit, depth: 2, root: Projection);

        // Verify operators at each level
        assert_operator!(circuit, 0, Projection { columns: ["name"] });
        assert_operator!(circuit, 1, Input { name: "users" });
    }

    #[test]
    fn test_filter_with_projection() {
        let (circuit, _) = compile_sql!("SELECT name FROM users WHERE age > 18");

        // Circuit has 3 nodes with Projection at root
        assert_circuit!(circuit, depth: 3, root: Projection);

        // Verify operators at each level
        assert_operator!(circuit, 0, Projection { columns: ["name"] });
        assert_operator!(circuit, 1, Filter);
        assert_filter_predicate!(circuit, 1, "age" > 18);
        assert_operator!(circuit, 2, Input { name: "users" });
    }

    #[test]
    fn test_select_star() {
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Should have all rows with all columns
        assert_eq!(result.changes.len(), 2);

        // Verify both rows are present with all columns
        for (row, weight) in &result.changes {
            assert_eq!(*weight, 1);
            assert_eq!(row.values.len(), 3); // id, name, age
        }
    }

    #[test]
    fn test_execute_filter() {
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(30),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Should only have Alice and Charlie (age > 18)
        assert_eq!(
            result.changes.len(),
            2,
            "Expected 2 rows after filtering, got {}",
            result.changes.len()
        );

        // Check that the filtered rows are correct
        let names: Vec<String> = result
            .changes
            .iter()
            .filter_map(|(row, weight)| {
                if *weight > 0 && row.values.len() > 1 {
                    if let Value::Text(name) = &row.values[1] {
                        Some(name.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        assert!(
            names.contains(&"Alice".to_string()),
            "Alice should be in results"
        );
        assert!(
            names.contains(&"Charlie".to_string()),
            "Charlie should be in results"
        );
        assert!(
            !names.contains(&"Bob".to_string()),
            "Bob should not be in results"
        );
    }

    #[test]
    fn test_simple_column_projection() {
        let (mut circuit, pager) = compile_sql!("SELECT name, age FROM users");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Should have all rows but only 2 columns (name, age)
        assert_eq!(result.changes.len(), 2);

        for (row, _) in &result.changes {
            assert_eq!(row.values.len(), 2); // Only name and age
                                             // First value should be name (Text)
            assert!(matches!(&row.values[0], Value::Text(_)));
            // Second value should be age (Integer)
            assert!(matches!(
                &row.values[1],
                Value::Numeric(Numeric::Integer(_))
            ));
        }
    }

    #[test]
    fn test_simple_aggregation() {
        // Test COUNT(*) with GROUP BY
        let (mut circuit, pager) = compile_sql!("SELECT age, COUNT(*) FROM users GROUP BY age");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(30),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Should have 2 groups: age 25 with count 2, age 30 with count 1
        assert_eq!(result.changes.len(), 2);

        // Check the results
        let mut found_25 = false;
        let mut found_30 = false;

        for (row, weight) in &result.changes {
            assert_eq!(*weight, 1);
            assert_eq!(row.values.len(), 2); // age, count

            if let (
                Value::Numeric(Numeric::Integer(age)),
                Value::Numeric(Numeric::Integer(count)),
            ) = (&row.values[0], &row.values[1])
            {
                if *age == 25 {
                    assert_eq!(*count, 2, "Age 25 should have count 2");
                    found_25 = true;
                } else if *age == 30 {
                    assert_eq!(*count, 1, "Age 30 should have count 1");
                    found_30 = true;
                }
            }
        }

        assert!(found_25, "Should have group for age 25");
        assert!(found_30, "Should have group for age 30");
    }

    #[test]
    fn test_sum_aggregation() {
        // Test SUM with GROUP BY
        let (mut circuit, pager) = compile_sql!("SELECT name, SUM(age) FROM users GROUP BY name");

        // Create test data - some names appear multiple times
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Alice".into()),
                Value::from_i64(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Bob".into()),
                Value::from_i64(20),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Should have 2 groups: Alice with sum 55, Bob with sum 20
        assert_eq!(result.changes.len(), 2);

        for (row, weight) in &result.changes {
            assert_eq!(*weight, 1);
            assert_eq!(row.values.len(), 2); // name, sum

            if let (Value::Text(name), Value::Numeric(Numeric::Float(sum))) =
                (&row.values[0], &row.values[1])
            {
                if name.as_str() == "Alice" {
                    assert_eq!(*sum, 55.0, "Alice should have sum 55");
                } else if name.as_str() == "Bob" {
                    assert_eq!(*sum, 20.0, "Bob should have sum 20");
                }
            }
        }
    }

    #[test]
    fn test_aggregation_without_group_by() {
        // Test aggregation without GROUP BY - should produce a single row
        let (mut circuit, pager) = compile_sql!("SELECT COUNT(*), SUM(age), AVG(age) FROM users");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(20),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Should have exactly 1 row with all aggregates
        assert_eq!(
            result.changes.len(),
            1,
            "Should have exactly one result row"
        );

        let (row, weight) = result.changes.first().unwrap();
        assert_eq!(*weight, 1);
        assert_eq!(row.values.len(), 3); // count, sum, avg

        // Check aggregate results
        // COUNT should be Integer
        if let Value::Numeric(Numeric::Integer(count)) = &row.values[0] {
            assert_eq!(*count, 3, "COUNT(*) should be 3");
        } else {
            panic!("COUNT should be Integer, got {:?}", row.values[0]);
        }

        // SUM can be Integer (if whole number) or Float
        match &row.values[1] {
            Value::Numeric(Numeric::Integer(sum)) => assert_eq!(*sum, 75, "SUM(age) should be 75"),
            Value::Numeric(Numeric::Float(sum)) => {
                assert_eq!(f64::from(*sum), 75.0, "SUM(age) should be 75.0")
            }
            other => panic!("SUM should be Integer or Float, got {other:?}"),
        }

        // AVG should be Float
        if let Value::Numeric(Numeric::Float(avg)) = &row.values[2] {
            assert_eq!(f64::from(*avg), 25.0, "AVG(age) should be 25.0");
        } else {
            panic!("AVG should be Float, got {:?}", row.values[2]);
        }
    }

    #[test]
    fn test_expression_projection_execution() {
        // Test that complex expressions work through VDBE compilation
        let (mut circuit, pager) = compile_sql!("SELECT hex(id) FROM users");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(255),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        assert_eq!(result.changes.len(), 2);

        let hex_values: HashMap<i64, String> = result
            .changes
            .iter()
            .map(|(row, _)| {
                let rowid = row.rowid;
                if let Value::Text(text) = &row.values[0] {
                    (rowid, text.to_string())
                } else {
                    panic!("Expected Text value for hex() result");
                }
            })
            .collect();

        assert_eq!(
            hex_values.get(&1).unwrap(),
            "31",
            "hex(1) should return '31' (hex of ASCII '1')"
        );

        assert_eq!(
            hex_values.get(&2).unwrap(),
            "323535",
            "hex(255) should return '323535' (hex of ASCII '2', '5', '5')"
        );
    }

    // TODO: This test currently fails on incremental updates.
    // The initial execution works correctly, but incremental updates produce
    // incorrect results (3 changes instead of 2, with wrong values).
    // This tests that the aggregate operator correctly handles incremental
    // updates when it's sandwiched between projection operators.
    #[test]
    fn test_projection_aggregation_projection_pattern() {
        // Test pattern: projection -> aggregation -> projection
        // Query: SELECT HEX(SUM(age + 2)) FROM users
        let (mut circuit, pager) = compile_sql!("SELECT HEX(SUM(age + 2)) FROM users");

        // Initial input data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".to_string().into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".to_string().into()),
                Value::from_i64(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".to_string().into()),
                Value::from_i64(35),
            ],
        );

        let mut input_data = HashMap::default();
        input_data.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, input_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(input_data.clone(), pager.clone()))
            .unwrap();

        // Expected: SUM(age + 2) = (25+2) + (30+2) + (35+2) = 27 + 32 + 37 = 96
        // HEX(96) should be the hex representation of the string "96" = "3936"
        assert_eq!(result.changes.len(), 1);
        let (row, _weight) = &result.changes[0];
        assert_eq!(row.values.len(), 1);

        // The hex function converts the number to string first, then to hex
        // SUM now returns Float, so 96.0 as string is "96.0", which in hex is "39362E30"
        // (hex of ASCII '9', '6', '.', '0')
        assert_eq!(
            row.values[0],
            Value::Text("39362E30".to_string().into()),
            "HEX(SUM(age + 2)) should return '39362E30' for sum of 96.0"
        );

        // Test incremental update: add a new user
        let mut input_delta = Delta::new();
        input_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::Text("David".to_string().into()),
                Value::from_i64(40),
            ],
        );

        let mut input_data = HashMap::default();
        input_data.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, input_data, pager).unwrap();

        // Expected: new SUM(age + 2) = 96.0 + (40+2) = 138.0
        // HEX(138.0) = hex of "138.0" = "3133382E30"
        assert_eq!(result.changes.len(), 2);

        // First change: remove old aggregate (96.0)
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, -1);
        assert_eq!(row.values[0], Value::Text("39362E30".to_string().into()));

        // Second change: add new aggregate (138.0)
        let (row, weight) = &result.changes[1];
        assert_eq!(*weight, 1);
        assert_eq!(
            row.values[0],
            Value::Text("3133382E30".to_string().into()),
            "HEX(SUM(age + 2)) should return '3133382E30' for sum of 138.0"
        );
    }

    #[test]
    fn test_nested_projection_with_groupby() {
        // Test pattern: projection -> aggregation with GROUP BY -> projection
        // Query: SELECT name, HEX(SUM(age * 2)) FROM users GROUP BY name
        let (mut circuit, pager) =
            compile_sql!("SELECT name, HEX(SUM(age * 2)) FROM users GROUP BY name");

        // Initial input data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".to_string().into()),
                Value::from_i64(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".to_string().into()),
                Value::from_i64(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Alice".to_string().into()),
                Value::from_i64(35),
            ],
        );

        let mut input_data = HashMap::default();
        input_data.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, input_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(input_data.clone(), pager.clone()))
            .unwrap();

        // Expected results:
        // Alice: SUM(25*2 + 35*2) = 50 + 70 = 120.0, HEX("120.0") = "3132302E30"
        // Bob: SUM(30*2) = 60.0, HEX("60.0") = "36302E30"
        assert_eq!(result.changes.len(), 2);

        let results: HashMap<String, String> = result
            .changes
            .iter()
            .map(|(row, _weight)| {
                let name = match &row.values[0] {
                    Value::Text(t) => t.to_string(),
                    _ => panic!("Expected text for name"),
                };
                let hex_sum = match &row.values[1] {
                    Value::Text(t) => t.to_string(),
                    _ => panic!("Expected text for hex value"),
                };
                (name, hex_sum)
            })
            .collect();

        assert_eq!(
            results.get("Alice").unwrap(),
            "3132302E30",
            "Alice's HEX(SUM(age * 2)) should be '3132302E30' (120.0)"
        );
        assert_eq!(
            results.get("Bob").unwrap(),
            "36302E30",
            "Bob's HEX(SUM(age * 2)) should be '36302E30' (60.0)"
        );
    }

    #[test]
    fn test_transaction_context() {
        // Test that uncommitted changes are visible within a transaction
        // but don't affect the operator's internal state
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        let state = pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial delta : only Alice (age > 18)
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.values[1], Value::Text("Alice".into()));

        // Create uncommitted changes that would be visible in a transaction
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        // Add Charlie (age 30) - should be visible in transaction
        uncommitted_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(30),
            ],
        );
        // Add David (age 15) - should NOT be visible (filtered out)
        uncommitted_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::Text("David".into()),
                Value::from_i64(15),
            ],
        );
        uncommitted.insert("users".to_string(), uncommitted_delta);

        // Execute with uncommitted data - this simulates processing the uncommitted changes
        // through the circuit to see what would be visible
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // The result should show Charlie being added (passes filter, age > 18)
        // David is filtered out (age 15 < 18)
        assert_eq!(tx_result.changes.len(), 1, "Should see Charlie added");
        assert_eq!(
            tx_result.changes[0].0.values[1],
            Value::Text("Charlie".into())
        );

        // Now actually commit Charlie (without uncommitted context)
        let mut commit_data = HashMap::default();
        let mut commit_delta = Delta::new();
        commit_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(30),
            ],
        );
        commit_data.insert("users".to_string(), commit_delta);

        let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

        // The commit result should show Charlie being added
        assert_eq!(commit_result.changes.len(), 1, "Should see Charlie added");
        assert_eq!(
            commit_result.changes[0].0.values[1],
            Value::Text("Charlie".into())
        );

        // Commit the change to make it permanent
        pager
            .io
            .block(|| circuit.commit(commit_data.clone(), pager.clone()))
            .unwrap();

        // Now if we execute again with no changes, we should see no delta
        let empty_result = test_execute(&mut circuit, HashMap::default(), pager).unwrap();
        assert_eq!(empty_result.changes.len(), 0, "No changes when no new data");
    }

    #[test]
    fn test_uncommitted_delete() {
        // Test that uncommitted deletes are handled correctly without affecting operator state
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(20),
            ],
        );
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        let state = pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial delta: Alice, Bob, Charlie (all age > 18)
        assert_eq!(state.changes.len(), 3);

        // Create uncommitted delete for Bob
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        uncommitted.insert("users".to_string(), uncommitted_delta);

        // Execute with uncommitted delete
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // Result should show the deleted row that passed the filter
        assert_eq!(
            tx_result.changes.len(),
            1,
            "Should see the uncommitted delete"
        );

        // Verify operator's internal state is unchanged (still has all 3 users)
        let state_after = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(
            state_after.changes.len(),
            3,
            "Internal state should still have all 3 users"
        );

        // Now actually commit the delete
        let mut commit_data = HashMap::default();
        let mut commit_delta = Delta::new();
        commit_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        commit_data.insert("users".to_string(), commit_delta);

        let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

        // Actually commit the delete to update operator state
        pager
            .io
            .block(|| circuit.commit(commit_data.clone(), pager.clone()))
            .unwrap();

        // The commit result should show Bob being deleted
        assert_eq!(commit_result.changes.len(), 1, "Should see Bob deleted");
        assert_eq!(
            commit_result.changes[0].1, -1,
            "Delete should have weight -1"
        );
        assert_eq!(
            commit_result.changes[0].0.values[1],
            Value::Text("Bob".into())
        );

        // After commit, internal state should have only Alice and Charlie
        let final_state = get_current_state(pager, &circuit).unwrap();
        assert_eq!(
            final_state.changes.len(),
            2,
            "After commit, should have Alice and Charlie"
        );

        let names: Vec<String> = final_state
            .changes
            .iter()
            .map(|(row, _)| {
                if let Value::Text(name) = &row.values[1] {
                    name.to_string()
                } else {
                    panic!("Expected text value");
                }
            })
            .collect();
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Charlie".to_string()));
        assert!(!names.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_uncommitted_update() {
        // Test that uncommitted updates (delete + insert) are handled correctly
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        ); // Bob is 17, filtered out
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Create uncommitted update: Bob turns 19 (update from 17 to 19)
        // This is modeled as delete + insert
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );
        uncommitted_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(19),
            ],
        );
        uncommitted.insert("users".to_string(), uncommitted_delta);

        // Execute with uncommitted update
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // Bob should now appear in the result (age 19 > 18)
        // Consolidate to see the final state
        let mut final_result = tx_result;
        final_result.consolidate();

        assert_eq!(final_result.changes.len(), 1, "Bob should now be in view");
        assert_eq!(
            final_result.changes[0].0.values[1],
            Value::Text("Bob".into())
        );
        assert_eq!(final_result.changes[0].0.values[2], Value::from_i64(19));

        // Now actually commit the update
        let mut commit_data = HashMap::default();
        let mut commit_delta = Delta::new();
        commit_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        );
        commit_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(19),
            ],
        );
        commit_data.insert("users".to_string(), commit_delta);

        // Commit the update
        pager
            .io
            .block(|| circuit.commit(commit_data.clone(), pager.clone()))
            .unwrap();

        // After committing, Bob should be in the view's state
        let state = get_current_state(pager, &circuit).unwrap();
        let mut consolidated_state = state;
        consolidated_state.consolidate();

        // Should have both Alice and Bob now
        assert_eq!(
            consolidated_state.changes.len(),
            2,
            "Should have Alice and Bob"
        );

        let names: Vec<String> = consolidated_state
            .changes
            .iter()
            .map(|(row, _)| {
                if let Value::Text(name) = &row.values[1] {
                    name.as_str().to_string()
                } else {
                    panic!("Expected text value");
                }
            })
            .collect();
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_uncommitted_filtered_delete() {
        // Test deleting a row that doesn't pass the filter
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with mixed data
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(15),
            ],
        ); // Bob doesn't pass filter
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Create uncommitted delete for Bob (who isn't in the view because age=15)
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(15),
            ],
        );
        uncommitted.insert("users".to_string(), uncommitted_delta);

        // Execute with uncommitted delete - should produce no output changes
        let tx_result = test_execute(&mut circuit, uncommitted, pager.clone()).unwrap();

        // Bob wasn't in the view, so deleting him produces no output
        assert_eq!(
            tx_result.changes.len(),
            0,
            "Deleting filtered row produces no changes"
        );

        // The view state should still only have Alice
        let state = get_current_state(pager, &circuit).unwrap();
        assert_eq!(state.changes.len(), 1, "View still has only Alice");
        assert_eq!(state.changes[0].0.values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_uncommitted_mixed_operations() {
        // Test multiple uncommitted operations together
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial state
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(state.changes.len(), 2);

        // Create uncommitted changes:
        // - Delete Alice
        // - Update Bob's age to 35
        // - Insert Charlie (age 40)
        // - Insert David (age 16, filtered out)
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        // Delete Alice
        uncommitted_delta.delete(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        // Update Bob (delete + insert)
        uncommitted_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        uncommitted_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(35),
            ],
        );
        // Insert Charlie
        uncommitted_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(40),
            ],
        );
        // Insert David (will be filtered)
        uncommitted_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::Text("David".into()),
                Value::from_i64(16),
            ],
        );
        uncommitted.insert("users".to_string(), uncommitted_delta);

        // Execute with uncommitted changes
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // Result should show all changes: delete Alice, update Bob, insert Charlie and David
        assert_eq!(
            tx_result.changes.len(),
            4,
            "Should see all uncommitted mixed operations"
        );

        // Verify operator's internal state is unchanged
        let state_after = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(state_after.changes.len(), 2, "Still has Alice and Bob");

        // Commit all changes
        let mut commit_data = HashMap::default();
        let mut commit_delta = Delta::new();
        commit_delta.delete(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        commit_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        commit_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(35),
            ],
        );
        commit_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(40),
            ],
        );
        commit_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::Text("David".into()),
                Value::from_i64(16),
            ],
        );
        commit_data.insert("users".to_string(), commit_delta);

        let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

        // Should see: Alice deleted, Bob deleted, Bob inserted, Charlie inserted
        // (David filtered out)
        assert_eq!(commit_result.changes.len(), 4, "Should see 4 changes");

        // Actually commit the changes to update operator state
        pager
            .io
            .block(|| circuit.commit(commit_data.clone(), pager.clone()))
            .unwrap();

        // After all commits, execute with no changes should return empty delta
        let empty_result = test_execute(&mut circuit, HashMap::default(), pager).unwrap();
        assert_eq!(empty_result.changes.len(), 0, "No changes when no new data");
    }

    #[test]
    fn test_uncommitted_aggregation() {
        // Test that aggregations work correctly with uncommitted changes
        // This tests the specific scenario where a transaction adds new data
        // and we need to see correct aggregation results within the transaction

        // Create a sales table schema for testing
        let _ = test_schema!();

        let (mut circuit, pager) = compile_sql!("SELECT product_id, SUM(amount) as total, COUNT(*) as cnt FROM sales GROUP BY product_id");

        // Initialize with base data: (1, 100), (1, 200), (2, 150), (2, 250)
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(1, vec![Value::from_i64(1), Value::from_i64(100)]);
        delta.insert(2, vec![Value::from_i64(1), Value::from_i64(200)]);
        delta.insert(3, vec![Value::from_i64(2), Value::from_i64(150)]);
        delta.insert(4, vec![Value::from_i64(2), Value::from_i64(250)]);
        init_data.insert("sales".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial state: product 1 total=300, product 2 total=400
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(state.changes.len(), 2, "Should have 2 product groups");

        // Build a map of product_id -> (total, count)
        let initial_results: HashMap<i64, (i64, i64)> = state
            .changes
            .iter()
            .map(|(row, _)| {
                // SUM might return Integer or Float, COUNT returns Integer
                let product_id = match &row.values[0] {
                    Value::Numeric(Numeric::Integer(id)) => *id,
                    _ => panic!("Product ID should be Integer, got {:?}", row.values[0]),
                };

                let total = match &row.values[1] {
                    Value::Numeric(Numeric::Integer(t)) => *t,
                    Value::Numeric(Numeric::Float(t)) => f64::from(*t) as i64,
                    _ => panic!("Total should be numeric, got {:?}", row.values[1]),
                };

                let count = match &row.values[2] {
                    Value::Numeric(Numeric::Integer(c)) => *c,
                    _ => panic!("Count should be Integer, got {:?}", row.values[2]),
                };

                (product_id, (total, count))
            })
            .collect();

        assert_eq!(
            initial_results.get(&1).unwrap(),
            &(300, 2),
            "Product 1 should have total=300, count=2"
        );
        assert_eq!(
            initial_results.get(&2).unwrap(),
            &(400, 2),
            "Product 2 should have total=400, count=2"
        );

        // Create uncommitted changes: INSERT (1, 50), (3, 300)
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.insert(5, vec![Value::from_i64(1), Value::from_i64(50)]); // Add to product 1
        uncommitted_delta.insert(6, vec![Value::from_i64(3), Value::from_i64(300)]); // New product 3
        uncommitted.insert("sales".to_string(), uncommitted_delta);

        // Execute with uncommitted data - simulating a read within transaction
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // Result should show the aggregate changes from uncommitted data
        // Product 1: retraction of (300, 2) and insertion of (350, 3)
        // Product 3: insertion of (300, 1) - new product
        assert_eq!(
            tx_result.changes.len(),
            3,
            "Should see aggregate changes from uncommitted data"
        );

        // IMPORTANT: Verify operator's internal state is unchanged
        let state_after = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(
            state_after.changes.len(),
            2,
            "Internal state should still have 2 groups"
        );

        // Verify the internal state still has original values
        let state_results: HashMap<i64, (i64, i64)> = state_after
            .changes
            .iter()
            .map(|(row, _)| {
                let product_id = match &row.values[0] {
                    Value::Numeric(Numeric::Integer(id)) => *id,
                    _ => panic!("Product ID should be Integer"),
                };

                let total = match &row.values[1] {
                    Value::Numeric(Numeric::Integer(t)) => *t,
                    Value::Numeric(Numeric::Float(t)) => f64::from(*t) as i64,
                    _ => panic!("Total should be numeric"),
                };

                let count = match &row.values[2] {
                    Value::Numeric(Numeric::Integer(c)) => *c,
                    _ => panic!("Count should be Integer"),
                };

                (product_id, (total, count))
            })
            .collect();

        assert_eq!(
            state_results.get(&1).unwrap(),
            &(300, 2),
            "Product 1 unchanged"
        );
        assert_eq!(
            state_results.get(&2).unwrap(),
            &(400, 2),
            "Product 2 unchanged"
        );
        assert!(
            !state_results.contains_key(&3),
            "Product 3 should not be in committed state"
        );

        // Now actually commit the changes
        let mut commit_data = HashMap::default();
        let mut commit_delta = Delta::new();
        commit_delta.insert(5, vec![Value::from_i64(1), Value::from_i64(50)]);
        commit_delta.insert(6, vec![Value::from_i64(3), Value::from_i64(300)]);
        commit_data.insert("sales".to_string(), commit_delta);

        let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

        // Should see changes for product 1 (updated) and product 3 (new)
        assert_eq!(
            commit_result.changes.len(),
            3,
            "Should see 3 changes (delete old product 1, insert new product 1, insert product 3)"
        );

        // Actually commit the changes to update operator state
        pager
            .io
            .block(|| circuit.commit(commit_data.clone(), pager.clone()))
            .unwrap();

        // After commit, verify final state
        let final_state = get_current_state(pager, &circuit).unwrap();
        assert_eq!(
            final_state.changes.len(),
            3,
            "Should have 3 product groups after commit"
        );

        let final_results: HashMap<i64, (i64, i64)> = final_state
            .changes
            .iter()
            .map(|(row, _)| {
                let product_id = match &row.values[0] {
                    Value::Numeric(Numeric::Integer(id)) => *id,
                    _ => panic!("Product ID should be Integer"),
                };

                let total = match &row.values[1] {
                    Value::Numeric(Numeric::Integer(t)) => *t,
                    Value::Numeric(Numeric::Float(t)) => f64::from(*t) as i64,
                    _ => panic!("Total should be numeric"),
                };

                let count = match &row.values[2] {
                    Value::Numeric(Numeric::Integer(c)) => *c,
                    _ => panic!("Count should be Integer"),
                };

                (product_id, (total, count))
            })
            .collect();

        assert_eq!(
            final_results.get(&1).unwrap(),
            &(350, 3),
            "Product 1 should have total=350, count=3"
        );
        assert_eq!(
            final_results.get(&2).unwrap(),
            &(400, 2),
            "Product 2 should have total=400, count=2"
        );
        assert_eq!(
            final_results.get(&3).unwrap(),
            &(300, 1),
            "Product 3 should have total=300, count=1"
        );
    }

    #[test]
    fn test_uncommitted_data_visible_in_transaction() {
        // Test that uncommitted INSERTs are visible within the same transaction
        // This simulates: BEGIN; INSERT ...; SELECT * FROM view; COMMIT;

        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data - need to match the schema (id, name, age)
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial state
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(
            state.len(),
            2,
            "Should have 2 users initially (both pass age > 18 filter)"
        );

        // Simulate a transaction: INSERT new users that pass the filter - match schema (id, name, age)
        let mut uncommitted = HashMap::default();
        let mut tx_delta = Delta::new();
        tx_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(35),
            ],
        );
        tx_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::Text("David".into()),
                Value::from_i64(20),
            ],
        );
        uncommitted.insert("users".to_string(), tx_delta);

        // Execute with uncommitted data - this should return the uncommitted changes
        // that passed through the filter (age > 18)
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // IMPORTANT: tx_result should contain the filtered uncommitted changes!
        // Both Charlie (35) and David (20) should pass the age > 18 filter
        assert_eq!(
            tx_result.len(),
            2,
            "Should see 2 uncommitted rows that pass filter"
        );

        // Verify the uncommitted results contain the expected rows
        let has_charlie = tx_result.changes.iter().any(|(row, _)| row.rowid == 3);
        assert!(
            has_charlie,
            "Should find Charlie (rowid=3) in uncommitted results"
        );

        let has_david = tx_result.changes.iter().any(|(row, _)| row.rowid == 4);
        assert!(
            has_david,
            "Should find David (rowid=4) in uncommitted results"
        );

        // CRITICAL: Verify the operator state wasn't modified by uncommitted execution
        let state_after_uncommitted = get_current_state(pager, &circuit).unwrap();
        assert_eq!(
            state_after_uncommitted.len(),
            2,
            "State should STILL be 2 after uncommitted execution - only Alice and Bob"
        );

        // The state should not contain Charlie or David
        let has_charlie_in_state = state_after_uncommitted
            .changes
            .iter()
            .any(|(row, _)| row.rowid == 3);
        let has_david_in_state = state_after_uncommitted
            .changes
            .iter()
            .any(|(row, _)| row.rowid == 4);
        assert!(
            !has_charlie_in_state,
            "Charlie should NOT be in operator state (uncommitted)"
        );
        assert!(
            !has_david_in_state,
            "David should NOT be in operator state (uncommitted)"
        );
    }

    #[test]
    fn test_uncommitted_aggregation_with_rollback() {
        // Test that rollback properly discards uncommitted aggregation changes
        // Similar to test_uncommitted_aggregation but explicitly tests rollback semantics

        // Create a simple aggregation circuit
        let (mut circuit, pager) =
            compile_sql!("SELECT age, COUNT(*) as cnt FROM users GROUP BY age");

        // Initialize with some data
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::Text("David".into()),
                Value::from_i64(30),
            ],
        );
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial state: age 25 count=2, age 30 count=2
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(state.changes.len(), 2);

        let initial_counts: HashMap<i64, i64> = state
            .changes
            .iter()
            .map(|(row, _)| {
                if let (
                    Value::Numeric(Numeric::Integer(age)),
                    Value::Numeric(Numeric::Integer(count)),
                ) = (&row.values[0], &row.values[1])
                {
                    (*age, *count)
                } else {
                    panic!("Unexpected value types");
                }
            })
            .collect();

        assert_eq!(initial_counts.get(&25).unwrap(), &2);
        assert_eq!(initial_counts.get(&30).unwrap(), &2);

        // Create uncommitted changes that would affect aggregations
        let mut uncommitted = HashMap::default();
        let mut uncommitted_delta = Delta::new();
        // Add more people aged 25
        uncommitted_delta.insert(
            5,
            vec![
                Value::from_i64(5),
                Value::Text("Eve".into()),
                Value::from_i64(25),
            ],
        );
        uncommitted_delta.insert(
            6,
            vec![
                Value::from_i64(6),
                Value::Text("Frank".into()),
                Value::from_i64(25),
            ],
        );
        // Add person aged 35 (new group)
        uncommitted_delta.insert(
            7,
            vec![
                Value::from_i64(7),
                Value::Text("Grace".into()),
                Value::from_i64(35),
            ],
        );
        // Delete Bob (age 30)
        uncommitted_delta.delete(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );
        uncommitted.insert("users".to_string(), uncommitted_delta);

        // Execute with uncommitted changes
        let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

        // Should see the aggregate changes from uncommitted data
        // Age 25: retraction of count 1 and insertion of count 2
        // Age 30: insertion of count 1 (Bob is new for age 30)
        assert!(
            !tx_result.changes.is_empty(),
            "Should see aggregate changes from uncommitted data"
        );

        // Verify internal state is unchanged (simulating rollback by not committing)
        let state_after_rollback = get_current_state(pager, &circuit).unwrap();
        assert_eq!(
            state_after_rollback.changes.len(),
            2,
            "Should still have 2 age groups"
        );

        let rollback_counts: HashMap<i64, i64> = state_after_rollback
            .changes
            .iter()
            .map(|(row, _)| {
                if let (
                    Value::Numeric(Numeric::Integer(age)),
                    Value::Numeric(Numeric::Integer(count)),
                ) = (&row.values[0], &row.values[1])
                {
                    (*age, *count)
                } else {
                    panic!("Unexpected value types");
                }
            })
            .collect();

        // Verify counts are unchanged after rollback
        assert_eq!(
            rollback_counts.get(&25).unwrap(),
            &2,
            "Age 25 count unchanged"
        );
        assert_eq!(
            rollback_counts.get(&30).unwrap(),
            &2,
            "Age 30 count unchanged"
        );
        assert!(
            !rollback_counts.contains_key(&35),
            "Age 35 should not exist"
        );
    }

    #[test]
    fn test_circuit_rowid_update_consolidation() {
        let (pager, p1, p2, p3) = setup_btree_for_circuit();

        // Test that circuit properly consolidates state when rowid changes
        let mut circuit = DbspCircuit::new(p1, p2, p3);

        // Create a simple filter node
        let schema = Arc::new(LogicalSchema::new(vec![
            ColumnInfo {
                name: "id".to_string(),
                ty: Type::Integer,
                database: None,
                table: None,
                table_alias: None,
            },
            ColumnInfo {
                name: "value".to_string(),
                ty: Type::Integer,
                database: None,
                table: None,
                table_alias: None,
            },
        ]));

        // First create an input node with InputOperator
        let input_id = circuit.add_node(
            DbspOperator::Input {
                name: "test".to_string(),
                schema: schema.clone(),
            },
            vec![],
            Box::new(InputOperator::new("test".to_string())),
        );

        let filter_op = FilterOperator::new(FilterPredicate::GreaterThan {
            column_idx: 1, // "value" is at index 1
            value: Value::from_i64(10),
        });

        // Create the filter predicate using DbspExpr
        let predicate = DbspExpr::BinaryExpr {
            left: Box::new(DbspExpr::Column("value".to_string())),
            op: ast::Operator::Greater,
            right: Box::new(DbspExpr::Literal(Value::from_i64(10))),
        };

        let filter_id = circuit.add_node(
            DbspOperator::Filter { predicate },
            vec![input_id], // Filter takes input from the input node
            Box::new(filter_op),
        );

        circuit.set_root(filter_id, schema);

        // Initialize with a row
        let mut init_data = HashMap::default();
        let mut delta = Delta::new();
        delta.insert(5, vec![Value::from_i64(5), Value::from_i64(20)]);
        init_data.insert("test".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Verify initial state
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(state.changes.len(), 1);
        assert_eq!(state.changes[0].0.rowid, 5);

        // Now update the rowid from 5 to 3
        let mut update_data = HashMap::default();
        let mut update_delta = Delta::new();
        update_delta.delete(5, vec![Value::from_i64(5), Value::from_i64(20)]);
        update_delta.insert(3, vec![Value::from_i64(3), Value::from_i64(20)]);
        update_data.insert("test".to_string(), update_delta);

        test_execute(&mut circuit, update_data.clone(), pager.clone()).unwrap();

        // Commit the changes to update operator state
        pager
            .io
            .block(|| circuit.commit(update_data.clone(), pager.clone()))
            .unwrap();

        // The circuit should consolidate the state properly
        let final_state = get_current_state(pager, &circuit).unwrap();
        assert_eq!(
            final_state.changes.len(),
            1,
            "Circuit should consolidate to single row"
        );
        assert_eq!(final_state.changes[0].0.rowid, 3);
        assert_eq!(
            final_state.changes[0].0.values,
            vec![Value::from_i64(3), Value::from_i64(20)]
        );
        assert_eq!(final_state.changes[0].1, 1);
    }

    #[test]
    fn test_circuit_respects_multiplicities() {
        let (mut circuit, pager) = compile_sql!("SELECT * from users");

        // Insert same row twice (multiplicity 2)
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );

        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), delta);
        test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // Delete once (should leave multiplicity 1)
        let mut delete_one = Delta::new();
        delete_one.delete(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );

        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), delete_one);
        test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // With proper DBSP: row still exists (weight 2 - 1 = 1)
        let state = get_current_state(pager, &circuit).unwrap();
        let mut consolidated = state;
        consolidated.consolidate();
        assert_eq!(
            consolidated.len(),
            1,
            "Row should still exist with multiplicity 1"
        );
    }

    #[test]
    fn test_join_with_aggregation() {
        // Test join followed by aggregation - verifying actual output
        let (mut circuit, pager) = compile_sql!(
            "SELECT u.name, SUM(o.quantity) as total_quantity
             FROM users u
             JOIN orders o ON u.id = o.user_id
             GROUP BY u.name"
        );

        // Create test data for users
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(30),
            ],
        );
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(25),
            ],
        );

        // Create test data for orders (order_id, user_id, product_id, quantity)
        let mut orders_delta = Delta::new();
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::from_i64(1),
                Value::from_i64(101),
                Value::from_i64(5),
            ],
        ); // Alice: 5
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::from_i64(1),
                Value::from_i64(102),
                Value::from_i64(3),
            ],
        ); // Alice: 3
        orders_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::from_i64(2),
                Value::from_i64(101),
                Value::from_i64(7),
            ],
        ); // Bob: 7
        orders_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::from_i64(1),
                Value::from_i64(103),
                Value::from_i64(2),
            ],
        ); // Alice: 2
        let inputs = HashMap::from_iter([
            ("users".to_string(), users_delta),
            ("orders".to_string(), orders_delta),
        ]);

        let result = test_execute(&mut circuit, inputs, pager).unwrap();

        // Should have 2 results: Alice with total 10, Bob with total 7
        assert_eq!(
            result.len(),
            2,
            "Should have aggregated results for Alice and Bob"
        );

        // Check the results
        let mut results_map: HashMap<String, f64> = HashMap::default();
        for (row, weight) in result.changes {
            assert_eq!(weight, 1);
            assert_eq!(row.values.len(), 2); // name and total_quantity

            if let (Value::Text(name), Value::Numeric(Numeric::Float(total))) =
                (&row.values[0], &row.values[1])
            {
                results_map.insert(name.to_string(), f64::from(*total));
            } else {
                panic!("Unexpected value types in result");
            }
        }

        assert_eq!(
            results_map.get("Alice"),
            Some(&10.0),
            "Alice should have total quantity 10"
        );
        assert_eq!(
            results_map.get("Bob"),
            Some(&7.0),
            "Bob should have total quantity 7"
        );
    }

    #[test]
    fn test_join_aggregate_with_filter() {
        // Test complex query with join, filter, and aggregation - verifying output
        let (mut circuit, pager) = compile_sql!(
            "SELECT u.name, SUM(o.quantity) as total
             FROM users u
             JOIN orders o ON u.id = o.user_id
             WHERE u.age > 18
             GROUP BY u.name"
        );

        // Create test data for users
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(30),
            ],
        ); // age > 18
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(17),
            ],
        ); // age <= 18
        users_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(25),
            ],
        ); // age > 18

        // Create test data for orders (order_id, user_id, product_id, quantity)
        let mut orders_delta = Delta::new();
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::from_i64(1),
                Value::from_i64(101),
                Value::from_i64(5),
            ],
        ); // Alice: 5
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::from_i64(2),
                Value::from_i64(102),
                Value::from_i64(10),
            ],
        ); // Bob: 10 (should be filtered)
        orders_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::from_i64(3),
                Value::from_i64(101),
                Value::from_i64(7),
            ],
        ); // Charlie: 7
        orders_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::from_i64(1),
                Value::from_i64(103),
                Value::from_i64(3),
            ],
        ); // Alice: 3

        let inputs = HashMap::from_iter([
            ("users".to_string(), users_delta),
            ("orders".to_string(), orders_delta),
        ]);

        let result = test_execute(&mut circuit, inputs, pager).unwrap();

        // Should only have results for Alice and Charlie (Bob filtered out due to age <= 18)
        assert_eq!(
            result.len(),
            2,
            "Should only have results for users with age > 18"
        );

        // Check the results
        let mut results_map: HashMap<String, f64> = HashMap::default();
        for (row, weight) in result.changes {
            assert_eq!(weight, 1);
            assert_eq!(row.values.len(), 2); // name and total

            if let (Value::Text(name), Value::Numeric(Numeric::Float(total))) =
                (&row.values[0], &row.values[1])
            {
                results_map.insert(name.to_string(), f64::from(*total));
            }
        }

        assert_eq!(
            results_map.get("Alice"),
            Some(&8.0),
            "Alice should have total 8"
        );
        assert_eq!(
            results_map.get("Charlie"),
            Some(&7.0),
            "Charlie should have total 7"
        );
        assert_eq!(results_map.get("Bob"), None, "Bob should be filtered out");
    }

    #[test]
    fn test_three_way_join_execution() {
        // Test executing a 3-way join with aggregation
        let (mut circuit, pager) = compile_sql!(
            "SELECT u.name, p.product_name, SUM(o.quantity) as total
             FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN products p ON o.product_id = p.product_id
             GROUP BY u.name, p.product_name"
        );

        // Create test data for users
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );

        // Create test data for products
        let mut products_delta = Delta::new();
        products_delta.insert(
            100,
            vec![
                Value::from_i64(100),
                Value::Text("Widget".into()),
                Value::from_i64(50),
            ],
        );
        products_delta.insert(
            101,
            vec![
                Value::from_i64(101),
                Value::Text("Gadget".into()),
                Value::from_i64(75),
            ],
        );
        products_delta.insert(
            102,
            vec![
                Value::from_i64(102),
                Value::Text("Doohickey".into()),
                Value::from_i64(25),
            ],
        );

        // Create test data for orders joining users and products
        let mut orders_delta = Delta::new();
        // Alice orders 5 Widgets
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::from_i64(1),
                Value::from_i64(100),
                Value::from_i64(5),
            ],
        );
        // Alice orders 3 Gadgets
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::from_i64(1),
                Value::from_i64(101),
                Value::from_i64(3),
            ],
        );
        // Bob orders 7 Widgets
        orders_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::from_i64(2),
                Value::from_i64(100),
                Value::from_i64(7),
            ],
        );
        // Bob orders 2 Doohickeys
        orders_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::from_i64(2),
                Value::from_i64(102),
                Value::from_i64(2),
            ],
        );
        // Alice orders 4 more Widgets
        orders_delta.insert(
            5,
            vec![
                Value::from_i64(5),
                Value::from_i64(1),
                Value::from_i64(100),
                Value::from_i64(4),
            ],
        );

        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), users_delta);
        inputs.insert("products".to_string(), products_delta);
        inputs.insert("orders".to_string(), orders_delta);

        // Execute the 3-way join with aggregation
        let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

        // We should get aggregated results for each user-product combination
        // Expected results:
        // - Alice, Widget: 9 (5 + 4)
        // - Alice, Gadget: 3
        // - Bob, Widget: 7
        // - Bob, Doohickey: 2
        assert_eq!(result.len(), 4, "Should have 4 aggregated results");

        // Verify aggregation results
        let mut found_results = HashSet::default();
        for (row, weight) in result.changes.iter() {
            assert_eq!(*weight, 1);
            // Row should have name, product_name, and sum columns
            assert_eq!(row.values.len(), 3);

            if let (
                Value::Text(name),
                Value::Text(product),
                Value::Numeric(Numeric::Float(total)),
            ) = (&row.values[0], &row.values[1], &row.values[2])
            {
                let key = format!("{}-{}", name.as_ref(), product.as_ref());
                found_results.insert(key.clone());

                match key.as_str() {
                    "Alice-Widget" => {
                        assert_eq!(*total, 9.0, "Alice should have ordered 9 Widgets total")
                    }
                    "Alice-Gadget" => {
                        assert_eq!(*total, 3.0, "Alice should have ordered 3 Gadgets")
                    }
                    "Bob-Widget" => assert_eq!(*total, 7.0, "Bob should have ordered 7 Widgets"),
                    "Bob-Doohickey" => {
                        assert_eq!(*total, 2.0, "Bob should have ordered 2 Doohickeys")
                    }
                    _ => panic!("Unexpected result: {key}"),
                }
            } else {
                panic!("Unexpected value types in result");
            }
        }

        // Ensure we found all expected combinations
        assert!(found_results.contains("Alice-Widget"));
        assert!(found_results.contains("Alice-Gadget"));
        assert!(found_results.contains("Bob-Widget"));
        assert!(found_results.contains("Bob-Doohickey"));
    }

    #[test]
    fn test_join_execution() {
        let (mut circuit, pager) = compile_sql!(
            "SELECT u.name, o.quantity FROM users u JOIN orders o ON u.id = o.user_id"
        );

        // Create test data for users
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );

        // Create test data for orders
        let mut orders_delta = Delta::new();
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::from_i64(1),
                Value::from_i64(100),
                Value::from_i64(5),
            ],
        );
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::from_i64(1),
                Value::from_i64(101),
                Value::from_i64(3),
            ],
        );
        orders_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::from_i64(2),
                Value::from_i64(102),
                Value::from_i64(7),
            ],
        );

        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), users_delta);
        inputs.insert("orders".to_string(), orders_delta);

        // Execute the join
        let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

        // We should get 3 results (2 orders for Alice, 1 for Bob)
        assert_eq!(result.len(), 3, "Should have 3 join results");

        // Verify the join results contain the correct data
        let results: Vec<_> = result.changes.iter().collect();

        // Check that we have the expected joined rows
        for (row, weight) in results {
            assert_eq!(*weight, 1); // All weights should be 1 for insertions
                                    // Row should have name and quantity columns
            assert_eq!(row.values.len(), 2);
        }
    }

    #[test]
    fn test_three_way_join_with_column_ambiguity() {
        // Test three-way join with aggregation where multiple tables have columns with the same name
        // Ensures that column references are correctly resolved to their respective tables
        // Tables: customers(id, name), purchases(id, customer_id, vendor_id, quantity), vendors(id, name, price)
        // Note: both customers and vendors have 'id' and 'name' columns which can cause ambiguity

        let sql = "SELECT c.name as customer_name, v.name as vendor_name,
                          SUM(p.quantity) as total_quantity,
                          SUM(p.quantity * v.price) as total_value
                   FROM customers c
                   JOIN purchases p ON c.id = p.customer_id
                   JOIN vendors v ON p.vendor_id = v.id
                   GROUP BY c.name, v.name";

        let (mut circuit, pager) = compile_sql!(sql);

        // Create test data for customers (id, name)
        let mut customers_delta = Delta::new();
        customers_delta.insert(1, vec![Value::from_i64(1), Value::Text("Alice".into())]);
        customers_delta.insert(2, vec![Value::from_i64(2), Value::Text("Bob".into())]);

        // Create test data for vendors (id, name, price)
        let mut vendors_delta = Delta::new();
        vendors_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Widget Co".into()),
                Value::from_i64(10),
            ],
        );
        vendors_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Gadget Inc".into()),
                Value::from_i64(20),
            ],
        );

        // Create test data for purchases (id, customer_id, vendor_id, quantity)
        let mut purchases_delta = Delta::new();
        // Alice purchases 5 units from Widget Co
        purchases_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::from_i64(1), // customer_id: Alice
                Value::from_i64(1), // vendor_id: Widget Co
                Value::from_i64(5),
            ],
        );
        // Alice purchases 3 units from Gadget Inc
        purchases_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::from_i64(1), // customer_id: Alice
                Value::from_i64(2), // vendor_id: Gadget Inc
                Value::from_i64(3),
            ],
        );
        // Bob purchases 2 units from Widget Co
        purchases_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::from_i64(2), // customer_id: Bob
                Value::from_i64(1), // vendor_id: Widget Co
                Value::from_i64(2),
            ],
        );
        // Alice purchases 4 more units from Widget Co
        purchases_delta.insert(
            4,
            vec![
                Value::from_i64(4),
                Value::from_i64(1), // customer_id: Alice
                Value::from_i64(1), // vendor_id: Widget Co
                Value::from_i64(4),
            ],
        );

        let inputs = HashMap::from_iter([
            ("customers".to_string(), customers_delta),
            ("purchases".to_string(), purchases_delta),
            ("vendors".to_string(), vendors_delta),
        ]);

        let result = test_execute(&mut circuit, inputs, pager).unwrap();

        // Expected results:
        // Alice|Gadget Inc|3|60    (3 units * 20 price = 60)
        // Alice|Widget Co|9|90     (9 units * 10 price = 90)
        // Bob|Widget Co|2|20       (2 units * 10 price = 20)

        assert_eq!(result.len(), 3, "Should have 3 aggregated results");

        // Sort results for consistent testing
        let mut results: Vec<_> = result.changes.into_iter().collect();
        results.sort_by(|a, b| {
            let a_cust = &a.0.values[0];
            let a_vend = &a.0.values[1];
            let b_cust = &b.0.values[0];
            let b_vend = &b.0.values[1];
            (a_cust, a_vend).cmp(&(b_cust, b_vend))
        });

        // Verify Alice's Gadget Inc purchases
        assert_eq!(results[0].0.values[0], Value::Text("Alice".into()));
        assert_eq!(results[0].0.values[1], Value::Text("Gadget Inc".into()));
        assert_eq!(results[0].0.values[2], Value::from_i64(3)); // total_quantity
        assert_eq!(results[0].0.values[3], Value::from_i64(60)); // total_value

        // Verify Alice's Widget Co purchases
        assert_eq!(results[1].0.values[0], Value::Text("Alice".into()));
        assert_eq!(results[1].0.values[1], Value::Text("Widget Co".into()));
        assert_eq!(results[1].0.values[2], Value::from_i64(9)); // total_quantity
        assert_eq!(results[1].0.values[3], Value::from_i64(90)); // total_value

        // Verify Bob's Widget Co purchases
        assert_eq!(results[2].0.values[0], Value::Text("Bob".into()));
        assert_eq!(results[2].0.values[1], Value::Text("Widget Co".into()));
        assert_eq!(results[2].0.values[2], Value::from_i64(2)); // total_quantity
        assert_eq!(results[2].0.values[3], Value::from_i64(20)); // total_value
    }

    #[test]
    fn test_projection_with_function_and_ambiguous_columns() {
        // Test projection with functions operating on potentially ambiguous columns
        // Uses HEX() function on sum of columns from different tables with same names
        // Tables: customers(id, name), vendors(id, name, price), purchases(id, customer_id, vendor_id, quantity)
        // This test ensures column references are correctly resolved to their respective tables

        let sql = "SELECT HEX(c.id + v.id) as hex_sum,
                          UPPER(c.name) as customer_upper,
                          LOWER(v.name) as vendor_lower,
                          c.id * v.price as product_value
                   FROM customers c
                   JOIN vendors v ON c.id = v.id";

        let (mut circuit, pager) = compile_sql!(sql);

        // Create test data for customers (id, name)
        let mut customers_delta = Delta::new();
        customers_delta.insert(1, vec![Value::from_i64(1), Value::Text("Alice".into())]);
        customers_delta.insert(2, vec![Value::from_i64(2), Value::Text("Bob".into())]);
        customers_delta.insert(3, vec![Value::from_i64(3), Value::Text("Charlie".into())]);

        // Create test data for vendors (id, name, price)
        let mut vendors_delta = Delta::new();
        vendors_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Widget Co".into()),
                Value::from_i64(10),
            ],
        );
        vendors_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Gadget Inc".into()),
                Value::from_i64(20),
            ],
        );
        vendors_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Tool Corp".into()),
                Value::from_i64(30),
            ],
        );

        let inputs = HashMap::from_iter([
            ("customers".to_string(), customers_delta),
            ("vendors".to_string(), vendors_delta),
        ]);

        let result = test_execute(&mut circuit, inputs, pager).unwrap();

        // Expected results:
        // For customer 1 (Alice) + vendor 1:
        //   - HEX(1 + 1) = HEX(2) = "32"
        //   - UPPER("Alice") = "ALICE"
        //   - LOWER("Widget Co") = "widget co"
        //   - 1 * 10 = 10
        assert_eq!(result.len(), 3, "Should have 3 join results");

        let mut results = result.changes;
        results.sort_by_key(|(row, _)| {
            // Sort by the product_value column for predictable ordering
            match &row.values[3] {
                Value::Numeric(Numeric::Integer(n)) => *n,
                _ => 0,
            }
        });

        // First result: Alice + Widget Co
        assert_eq!(results[0].0.values[0], Value::Text("32".into())); // HEX(2)
        assert_eq!(results[0].0.values[1], Value::Text("ALICE".into()));
        assert_eq!(results[0].0.values[2], Value::Text("widget co".into()));
        assert_eq!(results[0].0.values[3], Value::from_i64(10)); // 1 * 10

        // Second result: Bob + Gadget Inc
        assert_eq!(results[1].0.values[0], Value::Text("34".into())); // HEX(4)
        assert_eq!(results[1].0.values[1], Value::Text("BOB".into()));
        assert_eq!(results[1].0.values[2], Value::Text("gadget inc".into()));
        assert_eq!(results[1].0.values[3], Value::from_i64(40)); // 2 * 20

        // Third result: Charlie + Tool Corp
        assert_eq!(results[2].0.values[0], Value::Text("36".into())); // HEX(6)
        assert_eq!(results[2].0.values[1], Value::Text("CHARLIE".into()));
        assert_eq!(results[2].0.values[2], Value::Text("tool corp".into()));
        assert_eq!(results[2].0.values[3], Value::from_i64(90)); // 3 * 30
    }

    #[test]
    fn test_projection_column_selection_after_join() {
        // Test selecting specific columns after a join, especially with overlapping column names
        // This ensures the projection correctly picks columns by their qualified references

        let sql = "SELECT c.id as customer_id,
                          c.name as customer_name,
                          o.order_id,
                          o.quantity,
                          p.product_name
                   FROM users c
                   JOIN orders o ON c.id = o.user_id
                   JOIN products p ON o.product_id = p.product_id
                   WHERE o.quantity > 2";

        let (mut circuit, pager) = compile_sql!(sql);

        // Create test data for users (id, name, age)
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );

        // Create test data for orders (order_id, user_id, product_id, quantity)
        let mut orders_delta = Delta::new();
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(101),
                Value::from_i64(1),   // Alice
                Value::from_i64(201), // Widget
                Value::from_i64(5),   // quantity > 2
            ],
        );
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(102),
                Value::from_i64(2),   // Bob
                Value::from_i64(202), // Gadget
                Value::from_i64(1),   // quantity <= 2, filtered out
            ],
        );
        orders_delta.insert(
            3,
            vec![
                Value::from_i64(103),
                Value::from_i64(1),   // Alice
                Value::from_i64(202), // Gadget
                Value::from_i64(3),   // quantity > 2
            ],
        );

        // Create test data for products (product_id, product_name, price)
        let mut products_delta = Delta::new();
        products_delta.insert(
            201,
            vec![
                Value::from_i64(201),
                Value::Text("Widget".into()),
                Value::from_i64(10),
            ],
        );
        products_delta.insert(
            202,
            vec![
                Value::from_i64(202),
                Value::Text("Gadget".into()),
                Value::from_i64(20),
            ],
        );

        let inputs = HashMap::from_iter([
            ("users".to_string(), users_delta),
            ("orders".to_string(), orders_delta),
            ("products".to_string(), products_delta),
        ]);

        let result = test_execute(&mut circuit, inputs, pager).unwrap();

        // Should have 2 results (orders with quantity > 2)
        assert_eq!(result.len(), 2, "Should have 2 results after filtering");

        let mut results = result.changes;
        results.sort_by_key(|(row, _)| {
            match &row.values[2] {
                // Sort by order_id
                Value::Numeric(Numeric::Integer(n)) => *n,
                _ => 0,
            }
        });

        // First result: Alice's order 101 for Widget
        assert_eq!(results[0].0.values[0], Value::from_i64(1)); // customer_id
        assert_eq!(results[0].0.values[1], Value::Text("Alice".into())); // customer_name
        assert_eq!(results[0].0.values[2], Value::from_i64(101)); // order_id
        assert_eq!(results[0].0.values[3], Value::from_i64(5)); // quantity
        assert_eq!(results[0].0.values[4], Value::Text("Widget".into())); // product_name

        // Second result: Alice's order 103 for Gadget
        assert_eq!(results[1].0.values[0], Value::from_i64(1)); // customer_id
        assert_eq!(results[1].0.values[1], Value::Text("Alice".into())); // customer_name
        assert_eq!(results[1].0.values[2], Value::from_i64(103)); // order_id
        assert_eq!(results[1].0.values[3], Value::from_i64(3)); // quantity
        assert_eq!(results[1].0.values[4], Value::Text("Gadget".into())); // product_name
    }

    #[test]
    fn test_projection_column_reordering_and_duplication() {
        // Test that projection can reorder columns and select the same column multiple times
        // This is important for views that need specific column arrangements

        let sql = "SELECT o.quantity,
                          u.name,
                          u.id,
                          o.quantity * 2 as double_quantity,
                          u.id as user_id_again
                   FROM users u
                   JOIN orders o ON u.id = o.user_id
                   WHERE u.id = 1";

        let (mut circuit, pager) = compile_sql!(sql);

        // Create test data for users
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );

        // Create test data for orders
        let mut orders_delta = Delta::new();
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(101),
                Value::from_i64(1),   // user_id
                Value::from_i64(201), // product_id
                Value::from_i64(5),   // quantity
            ],
        );
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(102),
                Value::from_i64(1),   // user_id
                Value::from_i64(202), // product_id
                Value::from_i64(3),   // quantity
            ],
        );

        let inputs = HashMap::from_iter([
            ("users".to_string(), users_delta),
            ("orders".to_string(), orders_delta),
        ]);

        let result = test_execute(&mut circuit, inputs, pager).unwrap();

        assert_eq!(result.len(), 2, "Should have 2 results for user 1");

        // Check that columns are in the right order and values are correct
        for (row, _) in &result.changes {
            // Column 0: o.quantity (5 or 3)
            assert!(matches!(
                row.values[0],
                Value::Numeric(Numeric::Integer(5)) | Value::Numeric(Numeric::Integer(3))
            ));
            // Column 1: u.name
            assert_eq!(row.values[1], Value::Text("Alice".into()));
            // Column 2: u.id
            assert_eq!(row.values[2], Value::from_i64(1));
            // Column 3: o.quantity * 2 (10 or 6)
            assert!(matches!(
                row.values[3],
                Value::Numeric(Numeric::Integer(10)) | Value::Numeric(Numeric::Integer(6))
            ));
            // Column 4: u.id again
            assert_eq!(row.values[4], Value::from_i64(1));
        }
    }

    #[test]
    fn test_join_with_aggregate_execution() {
        let (mut circuit, pager) = compile_sql!(
            "SELECT u.name, SUM(o.quantity) as total_quantity
             FROM users u
             JOIN orders o ON u.id = o.user_id
             GROUP BY u.name"
        );

        // Create test data for users
        let mut users_delta = Delta::new();
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(25),
            ],
        );
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(30),
            ],
        );

        // Create test data for orders
        let mut orders_delta = Delta::new();
        orders_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::from_i64(1),
                Value::from_i64(100),
                Value::from_i64(5),
            ],
        );
        orders_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::from_i64(1),
                Value::from_i64(101),
                Value::from_i64(3),
            ],
        );
        orders_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::from_i64(2),
                Value::from_i64(102),
                Value::from_i64(7),
            ],
        );

        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), users_delta);
        inputs.insert("orders".to_string(), orders_delta);

        // Execute the join with aggregation
        let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

        // We should get 2 aggregated results (one for Alice, one for Bob)
        assert_eq!(result.len(), 2, "Should have 2 aggregated results");

        // Verify aggregation results
        for (row, weight) in result.changes.iter() {
            assert_eq!(*weight, 1);
            // Row should have name and sum columns
            assert_eq!(row.values.len(), 2);

            // Check the aggregated values
            if let Value::Text(name) = &row.values[0] {
                if name.as_ref() == "Alice" {
                    // Alice should have total quantity of 8 (5 + 3)
                    assert_eq!(row.values[1], Value::from_i64(8));
                } else if name.as_ref() == "Bob" {
                    // Bob should have total quantity of 7
                    assert_eq!(row.values[1], Value::from_i64(7));
                }
            }
        }
    }

    #[test]
    fn test_filter_with_qualified_columns_in_join() {
        // Test that filters correctly handle qualified column names in joins
        // when multiple tables have columns with the SAME names.
        // Both users and customers tables have 'id' and 'name' columns which can be ambiguous.

        let (mut circuit, pager) = compile_sql!(
            "SELECT users.id, users.name, customers.id, customers.name
             FROM users
             JOIN customers ON users.id = customers.id
             WHERE users.id > 1 AND customers.id < 100"
        );

        // Create test data
        let mut users_delta = Delta::new();
        let mut customers_delta = Delta::new();

        // Users data: (id, name, age)
        users_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(30),
            ],
        ); // id = 1
        users_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(25),
            ],
        ); // id = 2
        users_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(35),
            ],
        ); // id = 3

        // Customers data: (id, name, email)
        customers_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Customer Alice".into()),
                Value::Text("alice@example.com".into()),
            ],
        ); // id = 1
        customers_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Customer Bob".into()),
                Value::Text("bob@example.com".into()),
            ],
        ); // id = 2
        customers_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Customer Charlie".into()),
                Value::Text("charlie@example.com".into()),
            ],
        ); // id = 3

        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), users_delta);
        inputs.insert("customers".to_string(), customers_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

        // Should get rows where users.id > 1 AND customers.id < 100
        // - users.id=2 (> 1) AND customers.id=2 (< 100) ✓
        // - users.id=3 (> 1) AND customers.id=3 (< 100) ✓
        // Alice excluded: users.id=1 (NOT > 1)
        assert_eq!(result.len(), 2, "Should have 2 filtered results");

        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, 1);
        assert_eq!(row.values.len(), 4, "Should have 4 columns");

        // Verify the filter correctly used qualified columns for Bob
        assert_eq!(row.values[0], Value::from_i64(2), "users.id should be 2");
        assert_eq!(
            row.values[1],
            Value::Text("Bob".into()),
            "users.name should be Bob"
        );
        assert_eq!(
            row.values[2],
            Value::from_i64(2),
            "customers.id should be 2"
        );
        assert_eq!(
            row.values[3],
            Value::Text("Customer Bob".into()),
            "customers.name should be Customer Bob"
        );
    }

    #[test]
    fn test_expression_in_where_clause() {
        // Test expressions in WHERE clauses like (quantity * price) >= 400
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE (age * 2) > 30");

        // Create test data
        let mut input_delta = Delta::new();
        input_delta.insert(
            1,
            vec![
                Value::from_i64(1),
                Value::Text("Alice".into()),
                Value::from_i64(20), // age * 2 = 40 > 30, should pass
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::from_i64(2),
                Value::Text("Bob".into()),
                Value::from_i64(10), // age * 2 = 20 <= 30, should be filtered out
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::from_i64(3),
                Value::Text("Charlie".into()),
                Value::from_i64(16), // age * 2 = 32 > 30, should pass
            ],
        );

        // Create input map
        let mut inputs = HashMap::default();
        inputs.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

        // Should only have Alice and Charlie (age * 2 > 30)
        assert_eq!(
            result.changes.len(),
            2,
            "Should have 2 rows after filtering"
        );

        // Check Alice
        let alice = result
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::from_i64(1))
            .expect("Alice should be in result");
        assert_eq!(alice.0.values[1], Value::Text("Alice".into()));
        assert_eq!(alice.0.values[2], Value::from_i64(20));

        // Check Charlie
        let charlie = result
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::from_i64(3))
            .expect("Charlie should be in result");
        assert_eq!(charlie.0.values[1], Value::Text("Charlie".into()));
        assert_eq!(charlie.0.values[2], Value::from_i64(16));

        // Bob should not be in result
        let bob = result
            .changes
            .iter()
            .find(|(row, _)| row.values[0] == Value::from_i64(2));
        assert!(bob.is_none(), "Bob should be filtered out");
    }

    fn make_column_info(name: &str, ty: Type, table: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            ty,
            database: None,
            table: Some(table.to_string()),
            table_alias: None,
        }
    }

    #[test]
    fn test_resolve_join_columns_normal_order() {
        // Normal case: left.id = right.id
        let left_schema = LogicalSchema::new(vec![
            ColumnInfo {
                name: "id".to_string(),
                ty: Type::Integer,
                database: None,
                table: Some("left".to_string()),
                table_alias: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                ty: Type::Text,
                database: None,
                table: Some("left".to_string()),
                table_alias: None,
            },
        ]);
        let right_schema = LogicalSchema::new(vec![
            ColumnInfo {
                name: "id".to_string(),
                ty: Type::Integer,
                database: None,
                table: Some("right".to_string()),
                table_alias: None,
            },
            ColumnInfo {
                name: "value".to_string(),
                ty: Type::Integer,
                database: None,
                table: Some("right".to_string()),
                table_alias: None,
            },
        ]);

        let left_col = Column {
            name: "id".to_string(),
            table: Some("left".to_string()),
        };
        let right_col = Column {
            name: "id".to_string(),
            table: Some("right".to_string()),
        };

        let result =
            DbspCompiler::resolve_join_columns(&left_col, &right_col, &left_schema, &right_schema);
        assert!(result.is_ok());
        let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
        assert_eq!(actual_left.name, "id");
        assert_eq!(actual_left.table, Some("left".to_string()));
        assert_eq!(left_idx, 0);
        assert_eq!(actual_right.name, "id");
        assert_eq!(actual_right.table, Some("right".to_string()));
        assert_eq!(right_idx, 0);
    }

    #[test]
    fn test_resolve_join_columns_swapped_order() {
        // Swapped case: right.id = left.id
        let left_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "left"),
            make_column_info("name", Type::Text, "left"),
        ]);
        let right_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "right"),
            make_column_info("value", Type::Integer, "right"),
        ]);

        let right_col = Column {
            name: "id".to_string(),
            table: Some("right".to_string()),
        };
        let left_col = Column {
            name: "id".to_string(),
            table: Some("left".to_string()),
        };

        let result =
            DbspCompiler::resolve_join_columns(&right_col, &left_col, &left_schema, &right_schema);
        assert!(result.is_ok());
        let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
        assert_eq!(actual_left.name, "id");
        assert_eq!(actual_left.table, Some("left".to_string()));
        assert_eq!(left_idx, 0);
        assert_eq!(actual_right.name, "id");
        assert_eq!(actual_right.table, Some("right".to_string()));
        assert_eq!(right_idx, 0);
    }

    #[test]
    fn test_resolve_join_columns_one_ambiguous_one_not() {
        // Both tables have 'id', but only left has 'other_id'
        let left_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "left"),
            make_column_info("other_id", Type::Integer, "left"),
        ]);
        let right_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "right"),
            make_column_info("value", Type::Integer, "right"),
        ]);

        // Unqualified 'id' with qualified 'left.other_id'
        let id_col = Column {
            name: "id".to_string(),
            table: None,
        };
        let other_id_col = Column {
            name: "other_id".to_string(),
            table: Some("left".to_string()),
        };

        // id from right, other_id from left
        let result =
            DbspCompiler::resolve_join_columns(&id_col, &other_id_col, &left_schema, &right_schema);
        assert!(result.is_ok());
        let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
        assert_eq!(actual_left.name, "other_id");
        assert_eq!(left_idx, 1);
        assert_eq!(actual_right.name, "id");
        assert_eq!(right_idx, 0);
    }

    #[test]
    fn test_resolve_join_columns_mixed_qualified() {
        // One qualified, one unqualified, column exists on both sides
        let left_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "left"),
            make_column_info("name", Type::Text, "left"),
        ]);
        let right_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "right"),
            make_column_info("name", Type::Text, "right"),
        ]);

        // Qualified left.id with unqualified name
        let left_id = Column {
            name: "id".to_string(),
            table: Some("left".to_string()),
        };
        let name_unqualified = Column {
            name: "name".to_string(),
            table: None,
        };

        let result = DbspCompiler::resolve_join_columns(
            &left_id,
            &name_unqualified,
            &left_schema,
            &right_schema,
        );
        // left.id is explicitly from left, so unqualified 'name' must be resolved from right
        assert!(result.is_ok());
        let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
        assert_eq!(actual_left.name, "id");
        assert_eq!(left_idx, 0);
        assert_eq!(actual_right.name, "name");
        assert_eq!(right_idx, 1);
    }

    #[test]
    fn test_resolve_join_columns_both_from_same_side() {
        // Both columns from left table - should fail
        let left_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "left"),
            make_column_info("other_id", Type::Integer, "left"),
        ]);
        let right_schema =
            LogicalSchema::new(vec![make_column_info("value", Type::Integer, "right")]);

        let left_id = Column {
            name: "id".to_string(),
            table: Some("left".to_string()),
        };
        let left_other_id = Column {
            name: "other_id".to_string(),
            table: Some("left".to_string()),
        };

        let result = DbspCompiler::resolve_join_columns(
            &left_id,
            &left_other_id,
            &left_schema,
            &right_schema,
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must come from different input tables"));
    }

    #[test]
    fn test_resolve_join_columns_nonexistent_column() {
        // Column doesn't exist in either table
        let left_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "left")]);
        let right_schema =
            LogicalSchema::new(vec![make_column_info("value", Type::Integer, "right")]);

        let id_col = Column {
            name: "id".to_string(),
            table: None,
        };
        let nonexistent_col = Column {
            name: "does_not_exist".to_string(),
            table: None,
        };

        let result = DbspCompiler::resolve_join_columns(
            &id_col,
            &nonexistent_col,
            &left_schema,
            &right_schema,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_join_columns_both_qualified() {
        // Both columns qualified - should work normally
        let left_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "left"),
            make_column_info("name", Type::Text, "left"),
        ]);
        let right_schema = LogicalSchema::new(vec![
            make_column_info("id", Type::Integer, "right"),
            make_column_info("value", Type::Integer, "right"),
        ]);

        let left_id = Column {
            name: "id".to_string(),
            table: Some("left".to_string()),
        };
        let right_id = Column {
            name: "id".to_string(),
            table: Some("right".to_string()),
        };

        let result =
            DbspCompiler::resolve_join_columns(&left_id, &right_id, &left_schema, &right_schema);
        assert!(result.is_ok());
        let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
        assert_eq!(actual_left.name, "id");
        assert_eq!(left_idx, 0);
        assert_eq!(actual_right.name, "id");
        assert_eq!(right_idx, 0);
    }

    #[test]
    fn test_resolve_join_columns_both_unqualified_same_name() {
        // Both columns unqualified with same name existing in both tables - should succeed
        // (first match wins based on order of checking)
        let left_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "left")]);
        let right_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "right")]);

        let id_col1 = Column {
            name: "id".to_string(),
            table: None,
        };
        let id_col2 = Column {
            name: "id".to_string(),
            table: None,
        };

        let result =
            DbspCompiler::resolve_join_columns(&id_col1, &id_col2, &left_schema, &right_schema);
        // Should succeed - unqualified 'id' matches in both schemas
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_join_columns_first_not_found() {
        // First column doesn't exist anywhere
        let left_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "left")]);
        let right_schema =
            LogicalSchema::new(vec![make_column_info("value", Type::Integer, "right")]);

        let missing_col = Column {
            name: "missing".to_string(),
            table: None,
        };
        let value_col = Column {
            name: "value".to_string(),
            table: None,
        };

        let result = DbspCompiler::resolve_join_columns(
            &missing_col,
            &value_col,
            &left_schema,
            &right_schema,
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in either input"));
    }

    #[test]
    fn test_resolve_join_columns_both_unqualified_different_names() {
        // Both unqualified, each exists in only one table
        let left_schema =
            LogicalSchema::new(vec![make_column_info("left_id", Type::Integer, "left")]);
        let right_schema =
            LogicalSchema::new(vec![make_column_info("right_id", Type::Integer, "right")]);

        let left_col = Column {
            name: "left_id".to_string(),
            table: None,
        };
        let right_col = Column {
            name: "right_id".to_string(),
            table: None,
        };

        let result =
            DbspCompiler::resolve_join_columns(&left_col, &right_col, &left_schema, &right_schema);
        assert!(result.is_ok());
        let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
        assert_eq!(actual_left.name, "left_id");
        assert_eq!(left_idx, 0);
        assert_eq!(actual_right.name, "right_id");
        assert_eq!(right_idx, 0);
    }
}
