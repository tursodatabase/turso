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
use crate::types::IOResultOr;
use crate::SqliteDialect;
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
///
/// Each arm issues exactly one cursor op and advances only after it returns `Done`:
/// `IOResult::IO` means "call me again", so advancing first abandons an in-flight
/// balance. The seek therefore gets its own arm.
#[derive(Debug, Default)]
pub enum WriteRowView {
    #[default]
    GetRecord,
    Delete,
    Insert {
        final_weight: isize,
    },
    InsertRow {
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
    ) -> IOResultOr<()> {
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
                            Some(val) => match val?.to_owned()? {
                                Value::Numeric(Numeric::Integer(w)) => w as isize,
                                _ => {
                                    return Err(LimboError::InternalError(format!(
                                        "Invalid weight value in storage for key {key:?}"
                                    ))
                                    .into())
                                }
                            },
                            None => {
                                return Err(LimboError::InternalError(format!(
                                    "No weight value found in storage for key {key:?}"
                                ))
                                .into())
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
                    return_if_io!(cursor.delete());
                    *self = WriteRowView::Done;
                }
                WriteRowView::Insert { final_weight } => {
                    return_if_io!(cursor.seek(key.clone(), SeekOp::GE { eq_only: true }));
                    *self = WriteRowView::InsertRow {
                        final_weight: *final_weight,
                    };
                }
                WriteRowView::InsertRow { final_weight } => {
                    // Extract the row ID from the key
                    let key_i64 = match key {
                        SeekKey::TableRowId(id) => id,
                        _ => {
                            return Err(LimboError::InternalError(
                                "Expected TableRowId for storage".to_string(),
                            )
                            .into())
                        }
                    };

                    // Build the record values using the provided function
                    let record_values = build_record(*final_weight);

                    // Create an ImmutableRecord from the values
                    let immutable_record =
                        ImmutableRecord::from_values(&record_values, record_values.len())?;
                    let btree_key = BTreeKey::new_table_rowid(key_i64, Some(&immutable_record));

                    return_if_io!(cursor.insert(&btree_key));
                    *self = WriteRowView::Done;
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
    ) -> IOResultOr<Delta> {
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
    ) -> IOResultOr<Delta> {
        if let Some(root_id) = self.root {
            self.execute_node(
                root_id,
                pager.clone(),
                execute_state,
                commit_operators,
                state_cursors,
            )
        } else {
            Err(LimboError::ParseError("Circuit has no root node".to_string()).into())
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
    ) -> IOResultOr<Delta> {
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
            )?;
            let mut cursors = DbspStateCursors::new(table_cursor, index_cursor);
            self.execute_node(root_id, pager, execute_state, false, &mut cursors)
        } else {
            Err(LimboError::ParseError("Circuit has no root node".to_string()).into())
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
    ) -> IOResultOr<Delta> {
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
                    )?;

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
    ) -> IOResultOr<Delta> {
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
                        )?;
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
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
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
                    Value::Blob(b) => ast::Literal::Blob(format!("X'{}'", hex::encode(b))),
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
                    within_group: vec![],
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
                    within_group: vec![],
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

            // `<col> IS [NOT] NULL` is handled natively by `compile_filter_predicate`
            // as `FilterPredicate::IsNull`/`IsNotNull`. Routing it through the
            // projection-rewrite path is wrong: that path only carries a single
            // complex sub-expression as a temp column and then rewrites *both*
            // sides of an AND/OR to reference that one temp column — silently
            // dropping every other null-check predicate in a compound WHERE.
            LogicalExpr::IsNull { expr, .. } if matches!(expr.as_ref(), LogicalExpr::Column(_)) => {
                false
            }

            // Default: assume we need projection for safety
            // This includes: Between, InList, Like, Cast, ScalarFunction, Case,
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
#[path = "../tests/unit/incremental/compiler/tests.rs"]
mod tests;
