//! DBSP Compiler: Converts Logical Plans to DBSP Circuits
//!
//! This module implements compilation from SQL logical plans to DBSP circuits.
//! The initial version supports only filter and projection operators.
//!
//! Based on the DBSP paper: "DBSP: Automatic Incremental View Maintenance for Rich Query Languages"

use crate::incremental::dbsp::Delta;
use crate::incremental::expr_compiler::CompiledExpression;
use crate::incremental::hashable_row::HashableRow;
use crate::incremental::operator::{
    EvalState, FilterOperator, FilterPredicate, IncrementalOperator, InputOperator, ProjectOperator,
};
use crate::storage::btree::{BTreeCursor, BTreeKey};
// Note: logical module must be made pub(crate) in translate/mod.rs
use crate::translate::logical::{
    BinaryOperator, LogicalExpr, LogicalPlan, LogicalSchema, SchemaRef,
};
use crate::types::{IOResult, SeekKey, SeekOp, SeekResult, Value};
use crate::Pager;
use crate::{return_and_restore_if_io, return_if_io, LimboError, Result};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::Arc;

// The state table is always a key-value store with 3 columns: key, state, and weight.
const OPERATOR_COLUMNS: usize = 3;

/// State machine for writing a row to the materialized view
#[derive(Debug)]
pub enum WriteViewRow {
    /// Initial empty state
    Empty,

    /// Reading existing record to get current weight
    GetRecord,

    /// Deleting the row (when final weight <= 0)
    Delete,

    /// Inserting/updating the row with new weight
    Insert {
        /// The final weight to write
        final_weight: isize,
    },

    /// Completed processing this row
    Done,
}

impl WriteViewRow {
    fn new() -> Self {
        Self::Empty
    }
    fn write_row(
        &mut self,
        cursor: &mut BTreeCursor,
        row: HashableRow,
        weight: isize,
    ) -> Result<IOResult<()>> {
        loop {
            match self {
                WriteViewRow::Empty => {
                    let key = SeekKey::TableRowId(row.rowid);
                    let res = return_if_io!(cursor.seek(key, SeekOp::GE { eq_only: true }));
                    match res {
                        SeekResult::Found => *self = WriteViewRow::GetRecord,
                        _ => {
                            *self = WriteViewRow::Insert {
                                final_weight: weight,
                            }
                        }
                    }
                }
                WriteViewRow::GetRecord => {
                    let existing_record = return_if_io!(cursor.record());
                    let r = existing_record.ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "Found rowid {} in storage but could not read record",
                            row.rowid
                        ))
                    })?;
                    let values = r.get_values();

                    // last value should contain the weight
                    let existing_weight = match values.last() {
                        Some(ref_val) => match ref_val.to_owned() {
                            Value::Integer(w) => w as isize,
                            _ => {
                                return Err(crate::LimboError::InternalError(format!(
                                    "Invalid weight value in storage for rowid {}",
                                    row.rowid
                                )))
                            }
                        },
                        None => {
                            return Err(crate::LimboError::InternalError(format!(
                                "No weight value found in storage for rowid {}",
                                row.rowid
                            )))
                        }
                    };
                    let final_weight = existing_weight + weight;
                    if final_weight <= 0 {
                        *self = WriteViewRow::Delete
                    } else {
                        *self = WriteViewRow::Insert { final_weight }
                    }
                }
                WriteViewRow::Delete => {
                    // Delete the row. Important: when delete returns I/O, the btree operation
                    // has already completed in memory, so mark as Done to avoid retry
                    *self = WriteViewRow::Done;
                    return_if_io!(cursor.delete());
                }
                WriteViewRow::Insert { final_weight } => {
                    let key = SeekKey::TableRowId(row.rowid);
                    return_if_io!(cursor.seek(key, SeekOp::GE { eq_only: true }));

                    // Create the record values: row values + weight
                    let mut values = row.values.clone();
                    values.push(Value::Integer(*final_weight as i64));

                    // Create an ImmutableRecord from the values
                    let immutable_record =
                        crate::types::ImmutableRecord::from_values(&values, values.len());
                    let btree_key = BTreeKey::new_table_rowid(row.rowid, Some(&immutable_record));
                    // Insert the row. Important: when insert returns I/O, the btree operation
                    // has already completed in memory, so mark as Done to avoid retry
                    *self = WriteViewRow::Done;
                    return_if_io!(cursor.insert(&btree_key));
                }
                WriteViewRow::Done => {
                    break;
                }
            }
        }
        Ok(IOResult::Done(()))
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
        /// Persistent cursor for operator state btree (internal_state_root)
        state_cursor: Box<BTreeCursor>,
    },

    /// Updating the materialized view with the delta
    UpdateView {
        /// Delta to write to the view
        delta: Delta,
        /// Current index in delta.changes being processed
        current_index: usize,
        /// State for writing individual rows
        write_row_state: WriteViewRow,
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
                .field("has_state_cursor", &true)
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
        input_states: Vec<(usize, ExecuteState)>,
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
            deltas: HashMap::new(),
        }
    }

    /// Create an empty delta set (more semantic for "no changes")
    pub fn empty() -> Self {
        Self {
            deltas: HashMap::new(),
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
    /// Input operator - source of data
    Input { name: String, schema: SchemaRef },
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
    pub id: usize,
    /// The operator metadata
    pub operator: DbspOperator,
    /// Input nodes (edges in the DAG)
    pub inputs: Vec<usize>,
    /// The actual executable operator
    pub executable: Box<dyn IncrementalOperator>,
}

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
        pager: Rc<Pager>,
        eval_state: &mut EvalState,
        root_page: usize,
        commit_operators: bool,
        state_cursor: Option<&mut Box<BTreeCursor>>,
    ) -> Result<IOResult<Delta>> {
        // Process delta using the executable operator
        let op = &mut self.executable;

        // Use provided cursor or create a local one
        let mut local_cursor;
        let cursor = if let Some(cursor) = state_cursor {
            cursor.as_mut()
        } else {
            // Create a local cursor if none was provided
            local_cursor = BTreeCursor::new_table(None, pager.clone(), root_page, OPERATOR_COLUMNS);
            &mut local_cursor
        };

        let state = if commit_operators {
            // Clone the delta from eval_state - don't extract it
            // in case we need to re-execute due to I/O
            let delta = match eval_state {
                EvalState::Init { delta } => delta.clone(),
                _ => panic!("commit can only be called when eval_state is in Init state"),
            };
            let result = return_if_io!(op.commit(delta, cursor));
            // After successful commit, move state to Done
            *eval_state = EvalState::Done;
            result
        } else {
            return_if_io!(op.eval(eval_state, cursor))
        };
        Ok(IOResult::Done(state))
    }
}

/// Represents a complete DBSP circuit (DAG of operators)
#[derive(Debug)]
pub struct DbspCircuit {
    /// All nodes in the circuit, indexed by their ID
    pub(super) nodes: HashMap<usize, DbspNode>,
    /// Counter for generating unique node IDs
    next_id: usize,
    /// Root node ID (the final output)
    pub(super) root: Option<usize>,
    /// Output schema of the circuit (schema of the root node)
    pub(super) output_schema: SchemaRef,

    /// State machine for commit operation
    commit_state: CommitState,

    /// Root page for the main materialized view data
    pub(super) main_data_root: usize,
    /// Root page for internal DBSP state
    pub(super) internal_state_root: usize,
}

impl DbspCircuit {
    /// Create a new empty circuit with initial empty schema
    /// The actual output schema will be set when the root node is established
    pub fn new(main_data_root: usize, internal_state_root: usize) -> Self {
        // Start with an empty schema - will be updated when root is set
        let empty_schema = Arc::new(LogicalSchema::new(vec![]));
        Self {
            nodes: HashMap::new(),
            next_id: 0,
            root: None,
            output_schema: empty_schema,
            commit_state: CommitState::Init,
            main_data_root,
            internal_state_root,
        }
    }

    /// Set the root node and update the output schema
    fn set_root(&mut self, root_id: usize, schema: SchemaRef) {
        self.root = Some(root_id);
        self.output_schema = schema;
    }

    /// Get the current materialized state by reading from btree
    /// Add a node to the circuit
    fn add_node(
        &mut self,
        operator: DbspOperator,
        inputs: Vec<usize>,
        executable: Box<dyn IncrementalOperator>,
    ) -> usize {
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
        pager: Rc<Pager>,
        execute_state: &mut ExecuteState,
        commit_operators: bool,
        state_cursor: &mut Box<BTreeCursor>,
    ) -> Result<IOResult<Delta>> {
        if let Some(root_id) = self.root {
            self.execute_node(
                root_id,
                pager,
                execute_state,
                commit_operators,
                Some(state_cursor),
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
        pager: Rc<Pager>,
        execute_state: &mut ExecuteState,
    ) -> Result<IOResult<Delta>> {
        if let Some(root_id) = self.root {
            self.execute_node(root_id, pager, execute_state, false, None)
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
        pager: Rc<Pager>,
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
                    // Create state cursor when entering CommitOperators state
                    let state_cursor = Box::new(BTreeCursor::new_table(
                        None,
                        pager.clone(),
                        self.internal_state_root,
                        OPERATOR_COLUMNS,
                    ));

                    self.commit_state = CommitState::CommitOperators {
                        execute_state: Box::new(ExecuteState::Init {
                            input_data: input_delta_set.clone(),
                        }),
                        state_cursor,
                    };
                }
                CommitState::CommitOperators {
                    ref mut execute_state,
                    ref mut state_cursor,
                } => {
                    let delta = return_and_restore_if_io!(
                        &mut self.commit_state,
                        state,
                        self.run_circuit(pager.clone(), execute_state, true, state_cursor)
                    );

                    // Create view cursor when entering UpdateView state
                    let view_cursor = Box::new(BTreeCursor::new_table(
                        None,
                        pager.clone(),
                        main_data_root,
                        num_columns,
                    ));

                    self.commit_state = CommitState::UpdateView {
                        delta,
                        current_index: 0,
                        write_row_state: WriteViewRow::new(),
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

                        // If we're starting a new row (Empty state), we need a fresh cursor
                        // due to btree cursor state machine limitations
                        if matches!(write_row_state, WriteViewRow::Empty) {
                            *view_cursor = Box::new(BTreeCursor::new_table(
                                None,
                                pager.clone(),
                                main_data_root,
                                num_columns,
                            ));
                        }

                        return_and_restore_if_io!(
                            &mut self.commit_state,
                            state,
                            write_row_state.write_row(view_cursor, row, weight)
                        );

                        // Move to next row
                        let delta = std::mem::take(delta);
                        // Take ownership of view_cursor - we'll create a new one for next row if needed
                        let view_cursor = std::mem::replace(
                            view_cursor,
                            Box::new(BTreeCursor::new_table(
                                None,
                                pager.clone(),
                                main_data_root,
                                num_columns,
                            )),
                        );

                        self.commit_state = CommitState::UpdateView {
                            delta,
                            current_index: *current_index + 1,
                            write_row_state: WriteViewRow::new(),
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
        node_id: usize,
        pager: Rc<Pager>,
        execute_state: &mut ExecuteState,
        commit_operators: bool,
        state_cursor: Option<&mut Box<BTreeCursor>>,
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
                                eval_state: Box::new(EvalState::Init { delta }),
                            };
                        }
                        _ => {
                            // Non-input nodes need to process their inputs
                            let input_data = std::mem::take(input_data);
                            let input_node_ids = node.inputs.clone();

                            let input_states: Vec<(usize, ExecuteState)> = input_node_ids
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
                        // All inputs processed, check we have exactly one delta
                        // (Input nodes never reach here since they go straight to ProcessingNode)
                        let delta = if input_deltas.is_empty() {
                            return Err(LimboError::InternalError(
                                "execute() cannot be called without a Delta".to_string(),
                            ));
                        } else if input_deltas.len() > 1 {
                            return Err(LimboError::InternalError(
                                format!("Until joins are supported, only one delta is expected. Got {} deltas", input_deltas.len()),
                            ));
                        } else {
                            input_deltas[0].clone()
                        };

                        *execute_state = ExecuteState::ProcessingNode {
                            eval_state: Box::new(EvalState::Init { delta }),
                        };
                    } else {
                        // Get the (node_id, state) pair for the current index
                        let (input_node_id, input_state) = &mut input_states[*current_index];

                        let delta = return_if_io!(self.execute_node(
                            *input_node_id,
                            pager.clone(),
                            input_state,
                            commit_operators,
                            None // Input nodes don't need state cursor
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

                    let output_delta = return_if_io!(node.process_node(
                        pager.clone(),
                        eval_state,
                        self.internal_state_root,
                        commit_operators,
                        state_cursor,
                    ));
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
    fn fmt_node(&self, f: &mut Formatter, node_id: usize, depth: usize) -> fmt::Result {
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
                DbspOperator::Input { name, .. } => {
                    writeln!(f, "{indent}Input[{node_id}]: {name}")?;
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
    pub fn new(main_data_root: usize, internal_state_root: usize) -> Self {
        Self {
            circuit: DbspCircuit::new(main_data_root, internal_state_root),
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
    fn compile_plan(&mut self, plan: &LogicalPlan) -> Result<usize> {
        match plan {
            LogicalPlan::Projection(proj) => {
                // Compile the input first
                let input_id = self.compile_plan(&proj.input)?;

                // Get input column names for the ProjectOperator
                let input_schema = proj.input.schema();
                let input_column_names: Vec<String> = input_schema.columns.iter()
                    .map(|(name, _)| name.clone())
                    .collect();

                // Convert logical expressions to DBSP expressions
                let dbsp_exprs = proj.exprs.iter()
                    .map(Self::compile_expr)
                    .collect::<Result<Vec<_>>>()?;

                // Compile logical expressions to CompiledExpressions
                let mut compiled_exprs = Vec::new();
                let mut aliases = Vec::new();
                for expr in &proj.exprs {
                    let (compiled, alias) = Self::compile_expression(expr, &input_column_names)?;
                    compiled_exprs.push(compiled);
                    aliases.push(alias);
                }

                // Get output column names from the projection schema
                let output_column_names: Vec<String> = proj.schema.columns.iter()
                    .map(|(name, _)| name.clone())
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

                // Get column names from input schema
                let input_schema = filter.input.schema();
                let column_names: Vec<String> = input_schema.columns.iter()
                    .map(|(name, _)| name.clone())
                    .collect();

                // Convert predicate to DBSP expression
                let dbsp_predicate = Self::compile_expr(&filter.predicate)?;

                // Convert to FilterPredicate
                let filter_predicate = Self::compile_filter_predicate(&filter.predicate)?;

                // Create executable operator
                let executable: Box<dyn IncrementalOperator> =
                    Box::new(FilterOperator::new(filter_predicate, column_names));

                // Create filter node
                let node_id = self.circuit.add_node(
                    DbspOperator::Filter { predicate: dbsp_predicate },
                    vec![input_id],
                    executable,
                );
                Ok(node_id)
            }
            LogicalPlan::Aggregate(agg) => {
                // Compile the input first
                let input_id = self.compile_plan(&agg.input)?;

                // Get input column names
                let input_schema = agg.input.schema();
                let input_column_names: Vec<String> = input_schema.columns.iter()
                    .map(|(name, _)| name.clone())
                    .collect();

                // Compile group by expressions to column names
                let mut group_by_columns = Vec::new();
                let mut dbsp_group_exprs = Vec::new();
                for expr in &agg.group_expr {
                    // For now, only support simple column references in GROUP BY
                    if let LogicalExpr::Column(col) = expr {
                        group_by_columns.push(col.name.clone());
                        dbsp_group_exprs.push(DbspExpr::Column(col.name.clone()));
                    } else {
                        return Err(LimboError::ParseError(
                            "Only column references are supported in GROUP BY for incremental views".to_string()
                        ));
                    }
                }

                // Compile aggregate expressions
                let mut aggregate_functions = Vec::new();
                for expr in &agg.aggr_expr {
                    if let LogicalExpr::AggregateFunction { fun, args, .. } = expr {
                        use crate::function::AggFunc;
                        use crate::incremental::operator::AggregateFunction;

                        let agg_fn = match fun {
                            AggFunc::Count | AggFunc::Count0 => {
                                AggregateFunction::Count
                            }
                            AggFunc::Sum => {
                                if args.is_empty() {
                                    return Err(LimboError::ParseError("SUM requires an argument".to_string()));
                                }
                                // Extract column name from the argument
                                if let LogicalExpr::Column(col) = &args[0] {
                                    AggregateFunction::Sum(col.name.clone())
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
                                    AggregateFunction::Avg(col.name.clone())
                                } else {
                                    return Err(LimboError::ParseError(
                                        "Only column references are supported in aggregate functions for incremental views".to_string()
                                    ));
                                }
                            }
                            // MIN and MAX are not supported in incremental views due to storage overhead.
                            // To correctly handle deletions, these operators would need to track all values
                            // in each group, resulting in O(n) storage overhead. This is prohibitive for
                            // large datasets. Alternative approaches like maintaining sorted indexes still
                            // require O(n) storage. Until a more efficient solution is found, MIN/MAX
                            // aggregations are not supported in materialized views.
                            AggFunc::Min => {
                                return Err(LimboError::ParseError(
                                    "MIN aggregation is not supported in incremental materialized views due to O(n) storage overhead required for handling deletions".to_string()
                                ));
                            }
                            AggFunc::Max => {
                                return Err(LimboError::ParseError(
                                    "MAX aggregation is not supported in incremental materialized views due to O(n) storage overhead required for handling deletions".to_string()
                                ));
                            }
                            _ => {
                                return Err(LimboError::ParseError(
                                    format!("Unsupported aggregate function in DBSP compiler: {fun:?}")
                                ));
                            }
                        };
                        aggregate_functions.push(agg_fn);
                    } else {
                        return Err(LimboError::ParseError(
                            "Expected aggregate function in aggregate expressions".to_string()
                        ));
                    }
                }

                // Create the AggregateOperator with a unique operator_id
                // Use the next_node_id as the operator_id to ensure uniqueness
                let operator_id = self.circuit.next_id;
                use crate::incremental::operator::AggregateOperator;
                let executable: Box<dyn IncrementalOperator> = Box::new(AggregateOperator::new(
                    operator_id,  // Use next_node_id as operator_id
                    group_by_columns,
                    aggregate_functions.clone(),
                    input_column_names,
                ));

                // Create aggregate node
                let node_id = self.circuit.add_node(
                    DbspOperator::Aggregate {
                        group_exprs: dbsp_group_exprs,
                        aggr_exprs: aggregate_functions,
                        schema: agg.schema.clone(),
                    },
                    vec![input_id],
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
            _ => Err(LimboError::ParseError(
                format!("Unsupported operator in DBSP compiler: only Filter, Projection and Aggregate are supported, got: {:?}",
                    match plan {
                        LogicalPlan::Sort(_) => "Sort",
                        LogicalPlan::Limit(_) => "Limit",
                        LogicalPlan::Union(_) => "Union",
                        LogicalPlan::Distinct(_) => "Distinct",
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
        input_column_names: &[String],
    ) -> Result<(CompiledExpression, Option<String>)> {
        // Check for alias first
        if let LogicalExpr::Alias { expr, alias } = expr {
            // For aliases, compile the underlying expression and return with alias
            let (compiled, _) = Self::compile_expression(expr, input_column_names)?;
            return Ok((compiled, Some(alias.clone())));
        }

        // Convert LogicalExpr to AST Expr
        let ast_expr = Self::logical_to_ast_expr(expr)?;

        // For all expressions (simple or complex), use CompiledExpression::compile
        // This handles both trivial cases and complex VDBE compilation
        // We need to set up the necessary context
        use crate::{Database, MemoryIO, SymbolTable};
        use std::sync::Arc;

        // Create an internal connection for expression compilation
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", false, false)?;
        let internal_conn = db.connect()?;
        internal_conn.query_only.set(true);
        internal_conn.auto_commit.set(false);

        // Create temporary symbol table
        let temp_syms = SymbolTable::new();

        // Get a minimal schema for compilation (we don't need the full schema for expressions)
        let schema = crate::schema::Schema::new(false);

        // Compile the expression using the existing CompiledExpression::compile
        let compiled = CompiledExpression::compile(
            &ast_expr,
            input_column_names,
            &schema,
            &temp_syms,
            internal_conn,
        )?;

        Ok((compiled, None))
    }

    /// Convert LogicalExpr to AST Expr
    fn logical_to_ast_expr(expr: &LogicalExpr) -> Result<turso_parser::ast::Expr> {
        use turso_parser::ast;

        match expr {
            LogicalExpr::Column(col) => Ok(ast::Expr::Id(ast::Name::Ident(col.name.clone()))),
            LogicalExpr::Literal(val) => {
                let lit = match val {
                    Value::Integer(i) => ast::Literal::Numeric(i.to_string()),
                    Value::Float(f) => ast::Literal::Numeric(f.to_string()),
                    Value::Text(t) => ast::Literal::String(t.to_string()),
                    Value::Blob(b) => ast::Literal::Blob(format!("{b:?}")),
                    Value::Null => ast::Literal::Null,
                };
                Ok(ast::Expr::Literal(lit))
            }
            LogicalExpr::BinaryExpr { left, op, right } => {
                let left_expr = Self::logical_to_ast_expr(left)?;
                let right_expr = Self::logical_to_ast_expr(right)?;
                Ok(ast::Expr::Binary(
                    Box::new(left_expr),
                    *op,
                    Box::new(right_expr),
                ))
            }
            LogicalExpr::ScalarFunction { fun, args } => {
                let ast_args: Result<Vec<_>> = args.iter().map(Self::logical_to_ast_expr).collect();
                let ast_args: Vec<Box<ast::Expr>> = ast_args?.into_iter().map(Box::new).collect();
                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::Ident(fun.clone()),
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
                Self::logical_to_ast_expr(expr)
            }
            LogicalExpr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                // Convert aggregate function to AST
                let ast_args: Result<Vec<_>> = args.iter().map(Self::logical_to_ast_expr).collect();
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
                    name: ast::Name::Ident(func_name.to_string()),
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
            _ => Err(LimboError::ParseError(format!(
                "Cannot convert LogicalExpr to AST Expr: {expr:?}"
            ))),
        }
    }

    /// Compile a logical expression to a FilterPredicate for execution
    fn compile_filter_predicate(expr: &LogicalExpr) -> Result<FilterPredicate> {
        match expr {
            LogicalExpr::BinaryExpr { left, op, right } => {
                // Extract column name and value for simple predicates
                if let (LogicalExpr::Column(col), LogicalExpr::Literal(val)) =
                    (left.as_ref(), right.as_ref())
                {
                    match op {
                        BinaryOperator::Equals => Ok(FilterPredicate::Equals {
                            column: col.name.clone(),
                            value: val.clone(),
                        }),
                        BinaryOperator::NotEquals => Ok(FilterPredicate::NotEquals {
                            column: col.name.clone(),
                            value: val.clone(),
                        }),
                        BinaryOperator::Greater => Ok(FilterPredicate::GreaterThan {
                            column: col.name.clone(),
                            value: val.clone(),
                        }),
                        BinaryOperator::GreaterEquals => Ok(FilterPredicate::GreaterThanOrEqual {
                            column: col.name.clone(),
                            value: val.clone(),
                        }),
                        BinaryOperator::Less => Ok(FilterPredicate::LessThan {
                            column: col.name.clone(),
                            value: val.clone(),
                        }),
                        BinaryOperator::LessEquals => Ok(FilterPredicate::LessThanOrEqual {
                            column: col.name.clone(),
                            value: val.clone(),
                        }),
                        BinaryOperator::And => {
                            // Handle AND of two predicates
                            let left_pred = Self::compile_filter_predicate(left)?;
                            let right_pred = Self::compile_filter_predicate(right)?;
                            Ok(FilterPredicate::And(
                                Box::new(left_pred),
                                Box::new(right_pred),
                            ))
                        }
                        BinaryOperator::Or => {
                            // Handle OR of two predicates
                            let left_pred = Self::compile_filter_predicate(left)?;
                            let right_pred = Self::compile_filter_predicate(right)?;
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
                    let left_pred = Self::compile_filter_predicate(left)?;
                    let right_pred = Self::compile_filter_predicate(right)?;
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
                        "Filter predicate must be column op value".to_string(),
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
    use crate::schema::{BTreeTable, Column as SchemaColumn, Schema, Type};
    use crate::storage::pager::CreateBTreeFlags;
    use crate::translate::logical::LogicalPlanBuilder;
    use crate::translate::logical::LogicalSchema;
    use crate::util::IOExt;
    use crate::{Database, MemoryIO, Pager, IO};
    use std::rc::Rc;
    use std::sync::Arc;
    use turso_parser::ast;
    use turso_parser::parser::Parser;

    // Macro to create a test schema with a users table
    macro_rules! test_schema {
        () => {{
            let mut schema = Schema::new(false);
            let users_table = BTreeTable {
                name: "users".to_string(),
                root_page: 2,
                primary_key_columns: vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
                columns: vec![
                    SchemaColumn {
                        name: Some("id".to_string()),
                        ty: Type::Integer,
                        ty_str: "INTEGER".to_string(),
                        primary_key: true,
                        is_rowid_alias: true,
                        notnull: true,
                        default: None,
                        unique: false,
                        collation: None,
                        hidden: false,
                    },
                    SchemaColumn {
                        name: Some("name".to_string()),
                        ty: Type::Text,
                        ty_str: "TEXT".to_string(),
                        primary_key: false,
                        is_rowid_alias: false,
                        notnull: false,
                        default: None,
                        unique: false,
                        collation: None,
                        hidden: false,
                    },
                    SchemaColumn {
                        name: Some("age".to_string()),
                        ty: Type::Integer,
                        ty_str: "INTEGER".to_string(),
                        primary_key: false,
                        is_rowid_alias: false,
                        notnull: false,
                        default: None,
                        unique: false,
                        collation: None,
                        hidden: false,
                    },
                ],
                has_rowid: true,
                is_strict: false,
                unique_sets: None,
            };
            schema.add_btree_table(Arc::new(users_table));
            let sales_table = BTreeTable {
                name: "sales".to_string(),
                root_page: 2,
                primary_key_columns: vec![],
                columns: vec![
                    SchemaColumn {
                        name: Some("product_id".to_string()),
                        ty: Type::Integer,
                        ty_str: "INTEGER".to_string(),
                        primary_key: false,
                        is_rowid_alias: false,
                        notnull: false,
                        default: None,
                        unique: false,
                        collation: None,
                        hidden: false,
                    },
                    SchemaColumn {
                        name: Some("amount".to_string()),
                        ty: Type::Integer,
                        ty_str: "INTEGER".to_string(),
                        primary_key: false,
                        is_rowid_alias: false,
                        notnull: false,
                        default: None,
                        unique: false,
                        collation: None,
                        hidden: false,
                    },
                ],
                has_rowid: true,
                is_strict: false,
                unique_sets: None,
            };
            schema.add_btree_table(Arc::new(sales_table));

            schema
        }};
    }

    fn setup_btree_for_circuit() -> (Rc<Pager>, usize, usize) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", false, false).unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.borrow().clone();

        let _ = pager.io.block(|| pager.allocate_page1()).unwrap();

        let main_root_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as usize;

        let dbsp_state_page = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as usize;

        (pager, main_root_page, dbsp_state_page)
    }

    // Macro to compile SQL to DBSP circuit
    macro_rules! compile_sql {
        ($sql:expr) => {{
            let (pager, main_root_page, dbsp_state_page) = setup_btree_for_circuit();
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
                        DbspCompiler::new(main_root_page, dbsp_state_page)
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
                        assert!(matches!(&**right, DbspExpr::Literal(Value::Integer($val))));
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
                        assert!(matches!(&**right, DbspExpr::Literal(Value::Integer($val))));
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
                        assert!(matches!(&**right, DbspExpr::Literal(Value::Integer($val))));
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
        pager: Rc<Pager>,
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
    fn get_current_state(pager: Rc<Pager>, circuit: &DbspCircuit) -> Result<Delta> {
        let mut delta = Delta::new();

        let main_data_root = circuit.main_data_root;
        let num_columns = circuit.output_schema.columns.len() + 1;

        // Create a cursor to read the btree
        let mut btree_cursor =
            BTreeCursor::new_table(None, pager.clone(), main_data_root, num_columns);

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
            let record = pager
                .io
                .block(|| btree_cursor.record())
                .unwrap()
                .unwrap()
                .to_owned();

            let values_ref = record.get_values();
            let num_data_columns = values_ref.len() - 1; // Get length before consuming
            let values: Vec<Value> = values_ref
                .into_iter()
                .take(num_data_columns) // Skip the weight column
                .map(|x| x.to_owned())
                .collect();
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(30),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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
            assert!(matches!(&row.values[1], Value::Integer(_)));
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(30),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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

            if let (Value::Integer(age), Value::Integer(count)) = (&row.values[0], &row.values[1]) {
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Alice".into()),
                Value::Integer(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Bob".into()),
                Value::Integer(20),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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

            if let (Value::Text(name), Value::Float(sum)) = (&row.values[0], &row.values[1]) {
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(20),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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
        if let Value::Integer(count) = &row.values[0] {
            assert_eq!(*count, 3, "COUNT(*) should be 3");
        } else {
            panic!("COUNT should be Integer, got {:?}", row.values[0]);
        }

        // SUM can be Integer (if whole number) or Float
        match &row.values[1] {
            Value::Integer(sum) => assert_eq!(*sum, 75, "SUM(age) should be 75"),
            Value::Float(sum) => assert_eq!(*sum, 75.0, "SUM(age) should be 75.0"),
            other => panic!("SUM should be Integer or Float, got {other:?}"),
        }

        // AVG should be Float
        if let Value::Float(avg) = &row.values[2] {
            assert_eq!(*avg, 25.0, "AVG(age) should be 25.0");
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(255),
                Value::Text("Bob".into()),
                Value::Integer(17),
            ],
        );

        // Create input map
        let mut inputs = HashMap::new();
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
                Value::Integer(1),
                Value::Text("Alice".to_string().into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".to_string().into()),
                Value::Integer(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".to_string().into()),
                Value::Integer(35),
            ],
        );

        let mut input_data = HashMap::new();
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
        // 96 as string is "96", which in hex is "3936" (hex of ASCII '9' and '6')
        assert_eq!(
            row.values[0],
            Value::Text("3936".to_string().into()),
            "HEX(SUM(age + 2)) should return '3936' for sum of 96"
        );

        // Test incremental update: add a new user
        let mut input_delta = Delta::new();
        input_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".to_string().into()),
                Value::Integer(40),
            ],
        );

        let mut input_data = HashMap::new();
        input_data.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, input_data, pager.clone()).unwrap();

        // Expected: new SUM(age + 2) = 96 + (40+2) = 138
        // HEX(138) = hex of "138" = "313338"
        assert_eq!(result.changes.len(), 2);

        // First change: remove old aggregate (96)
        let (row, weight) = &result.changes[0];
        assert_eq!(*weight, -1);
        assert_eq!(row.values[0], Value::Text("3936".to_string().into()));

        // Second change: add new aggregate (138)
        let (row, weight) = &result.changes[1];
        assert_eq!(*weight, 1);
        assert_eq!(
            row.values[0],
            Value::Text("313338".to_string().into()),
            "HEX(SUM(age + 2)) should return '313338' for sum of 138"
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
                Value::Integer(1),
                Value::Text("Alice".to_string().into()),
                Value::Integer(25),
            ],
        );
        input_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".to_string().into()),
                Value::Integer(30),
            ],
        );
        input_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Alice".to_string().into()),
                Value::Integer(35),
            ],
        );

        let mut input_data = HashMap::new();
        input_data.insert("users".to_string(), input_delta);

        let result = test_execute(&mut circuit, input_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(input_data.clone(), pager.clone()))
            .unwrap();

        // Expected results:
        // Alice: SUM(25*2 + 35*2) = 50 + 70 = 120, HEX("120") = "313230"
        // Bob: SUM(30*2) = 60, HEX("60") = "3630"
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
            "313230",
            "Alice's HEX(SUM(age * 2)) should be '313230' (120)"
        );
        assert_eq!(
            results.get("Bob").unwrap(),
            "3630",
            "Bob's HEX(SUM(age * 2)) should be '3630' (60)"
        );
    }

    #[test]
    fn test_transaction_context() {
        // Test that uncommitted changes are visible within a transaction
        // but don't affect the operator's internal state
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
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
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        // Add Charlie (age 30) - should be visible in transaction
        uncommitted_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(30),
            ],
        );
        // Add David (age 15) - should NOT be visible (filtered out)
        uncommitted_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(15),
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
        let mut commit_data = HashMap::new();
        let mut commit_delta = Delta::new();
        commit_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(30),
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
        let empty_result = test_execute(&mut circuit, HashMap::new(), pager.clone()).unwrap();
        assert_eq!(empty_result.changes.len(), 0, "No changes when no new data");
    }

    #[test]
    fn test_uncommitted_delete() {
        // Test that uncommitted deletes are handled correctly without affecting operator state
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
            ],
        );
        delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(20),
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
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
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
        let mut commit_data = HashMap::new();
        let mut commit_delta = Delta::new();
        commit_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
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
        let final_state = get_current_state(pager.clone(), &circuit).unwrap();
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
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
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
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
            ],
        );
        uncommitted_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(19),
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
        assert_eq!(final_result.changes[0].0.values[2], Value::Integer(19));

        // Now actually commit the update
        let mut commit_data = HashMap::new();
        let mut commit_delta = Delta::new();
        commit_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(17),
            ],
        );
        commit_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(19),
            ],
        );
        commit_data.insert("users".to_string(), commit_delta);

        // Commit the update
        pager
            .io
            .block(|| circuit.commit(commit_data.clone(), pager.clone()))
            .unwrap();

        // After committing, Bob should be in the view's state
        let state = get_current_state(pager.clone(), &circuit).unwrap();
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
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(15),
            ],
        ); // Bob doesn't pass filter
        init_data.insert("users".to_string(), delta);

        let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(init_data.clone(), pager.clone()))
            .unwrap();

        // Create uncommitted delete for Bob (who isn't in the view because age=15)
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(15),
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
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(state.changes.len(), 1, "View still has only Alice");
        assert_eq!(state.changes[0].0.values[1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_uncommitted_mixed_operations() {
        // Test multiple uncommitted operations together
        let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

        // Initialize with some data
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
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
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        // Delete Alice
        uncommitted_delta.delete(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        // Update Bob (delete + insert)
        uncommitted_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
            ],
        );
        uncommitted_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(35),
            ],
        );
        // Insert Charlie
        uncommitted_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(40),
            ],
        );
        // Insert David (will be filtered)
        uncommitted_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(16),
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
        let mut commit_data = HashMap::new();
        let mut commit_delta = Delta::new();
        commit_delta.delete(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        commit_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
            ],
        );
        commit_delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(35),
            ],
        );
        commit_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(40),
            ],
        );
        commit_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(16),
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
        let empty_result = test_execute(&mut circuit, HashMap::new(), pager.clone()).unwrap();
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
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(1, vec![Value::Integer(1), Value::Integer(100)]);
        delta.insert(2, vec![Value::Integer(1), Value::Integer(200)]);
        delta.insert(3, vec![Value::Integer(2), Value::Integer(150)]);
        delta.insert(4, vec![Value::Integer(2), Value::Integer(250)]);
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
                    Value::Integer(id) => *id,
                    _ => panic!("Product ID should be Integer, got {:?}", row.values[0]),
                };

                let total = match &row.values[1] {
                    Value::Integer(t) => *t,
                    Value::Float(t) => *t as i64,
                    _ => panic!("Total should be numeric, got {:?}", row.values[1]),
                };

                let count = match &row.values[2] {
                    Value::Integer(c) => *c,
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
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        uncommitted_delta.insert(5, vec![Value::Integer(1), Value::Integer(50)]); // Add to product 1
        uncommitted_delta.insert(6, vec![Value::Integer(3), Value::Integer(300)]); // New product 3
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
                    Value::Integer(id) => *id,
                    _ => panic!("Product ID should be Integer"),
                };

                let total = match &row.values[1] {
                    Value::Integer(t) => *t,
                    Value::Float(t) => *t as i64,
                    _ => panic!("Total should be numeric"),
                };

                let count = match &row.values[2] {
                    Value::Integer(c) => *c,
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
        let mut commit_data = HashMap::new();
        let mut commit_delta = Delta::new();
        commit_delta.insert(5, vec![Value::Integer(1), Value::Integer(50)]);
        commit_delta.insert(6, vec![Value::Integer(3), Value::Integer(300)]);
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
        let final_state = get_current_state(pager.clone(), &circuit).unwrap();
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
                    Value::Integer(id) => *id,
                    _ => panic!("Product ID should be Integer"),
                };

                let total = match &row.values[1] {
                    Value::Integer(t) => *t,
                    Value::Float(t) => *t as i64,
                    _ => panic!("Total should be numeric"),
                };

                let count = match &row.values[2] {
                    Value::Integer(c) => *c,
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
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
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
        let mut uncommitted = HashMap::new();
        let mut tx_delta = Delta::new();
        tx_delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(35),
            ],
        );
        tx_delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(20),
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
        let state_after_uncommitted = get_current_state(pager.clone(), &circuit).unwrap();
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
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
            ],
        );
        delta.insert(
            3,
            vec![
                Value::Integer(3),
                Value::Text("Charlie".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            4,
            vec![
                Value::Integer(4),
                Value::Text("David".into()),
                Value::Integer(30),
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
                if let (Value::Integer(age), Value::Integer(count)) =
                    (&row.values[0], &row.values[1])
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
        let mut uncommitted = HashMap::new();
        let mut uncommitted_delta = Delta::new();
        // Add more people aged 25
        uncommitted_delta.insert(
            5,
            vec![
                Value::Integer(5),
                Value::Text("Eve".into()),
                Value::Integer(25),
            ],
        );
        uncommitted_delta.insert(
            6,
            vec![
                Value::Integer(6),
                Value::Text("Frank".into()),
                Value::Integer(25),
            ],
        );
        // Add person aged 35 (new group)
        uncommitted_delta.insert(
            7,
            vec![
                Value::Integer(7),
                Value::Text("Grace".into()),
                Value::Integer(35),
            ],
        );
        // Delete Bob (age 30)
        uncommitted_delta.delete(
            2,
            vec![
                Value::Integer(2),
                Value::Text("Bob".into()),
                Value::Integer(30),
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
        let state_after_rollback = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(
            state_after_rollback.changes.len(),
            2,
            "Should still have 2 age groups"
        );

        let rollback_counts: HashMap<i64, i64> = state_after_rollback
            .changes
            .iter()
            .map(|(row, _)| {
                if let (Value::Integer(age), Value::Integer(count)) =
                    (&row.values[0], &row.values[1])
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
        let (pager, p1, p2) = setup_btree_for_circuit();

        // Test that circuit properly consolidates state when rowid changes
        let mut circuit = DbspCircuit::new(p1, p2);

        // Create a simple filter node
        let schema = Arc::new(LogicalSchema::new(vec![
            ("id".to_string(), Type::Integer),
            ("value".to_string(), Type::Integer),
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

        let filter_op = FilterOperator::new(
            FilterPredicate::GreaterThan {
                column: "value".to_string(),
                value: Value::Integer(10),
            },
            vec!["id".to_string(), "value".to_string()],
        );

        // Create the filter predicate using DbspExpr
        let predicate = DbspExpr::BinaryExpr {
            left: Box::new(DbspExpr::Column("value".to_string())),
            op: ast::Operator::Greater,
            right: Box::new(DbspExpr::Literal(Value::Integer(10))),
        };

        let filter_id = circuit.add_node(
            DbspOperator::Filter { predicate },
            vec![input_id], // Filter takes input from the input node
            Box::new(filter_op),
        );

        circuit.set_root(filter_id, schema.clone());

        // Initialize with a row
        let mut init_data = HashMap::new();
        let mut delta = Delta::new();
        delta.insert(5, vec![Value::Integer(5), Value::Integer(20)]);
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
        let mut update_data = HashMap::new();
        let mut update_delta = Delta::new();
        update_delta.delete(5, vec![Value::Integer(5), Value::Integer(20)]);
        update_delta.insert(3, vec![Value::Integer(3), Value::Integer(20)]);
        update_data.insert("test".to_string(), update_delta);

        test_execute(&mut circuit, update_data.clone(), pager.clone()).unwrap();

        // Commit the changes to update operator state
        pager
            .io
            .block(|| circuit.commit(update_data.clone(), pager.clone()))
            .unwrap();

        // The circuit should consolidate the state properly
        let final_state = get_current_state(pager.clone(), &circuit).unwrap();
        assert_eq!(
            final_state.changes.len(),
            1,
            "Circuit should consolidate to single row"
        );
        assert_eq!(final_state.changes[0].0.rowid, 3);
        assert_eq!(
            final_state.changes[0].0.values,
            vec![Value::Integer(3), Value::Integer(20)]
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );
        delta.insert(
            1,
            vec![
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );

        let mut inputs = HashMap::new();
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
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Integer(25),
            ],
        );

        let mut inputs = HashMap::new();
        inputs.insert("users".to_string(), delete_one);
        test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
        pager
            .io
            .block(|| circuit.commit(inputs.clone(), pager.clone()))
            .unwrap();

        // With proper DBSP: row still exists (weight 2 - 1 = 1)
        let state = get_current_state(pager.clone(), &circuit).unwrap();
        let mut consolidated = state;
        consolidated.consolidate();
        assert_eq!(
            consolidated.len(),
            1,
            "Row should still exist with multiplicity 1"
        );
    }
}
