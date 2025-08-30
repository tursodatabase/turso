use super::compiler::{DbspCircuit, DbspCompiler, DeltaSet};
use super::dbsp::{RowKeyStream, RowKeyZSet};
use super::operator::{ComputationTracker, Delta, FilterPredicate};
use crate::schema::{BTreeTable, Column, Schema};
use crate::translate::logical::LogicalPlanBuilder;
use crate::types::{IOCompletions, IOResult, Value};
use crate::util::extract_view_columns;
use crate::{io_yield_one, Completion, LimboError, Result, Statement};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use turso_parser::ast;
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

/// State machine for populating a view from its source table
pub enum PopulateState {
    /// Initial state - need to prepare the query
    Start,
    /// Actively processing rows from the query
    Processing {
        stmt: Box<Statement>,
        rows_processed: usize,
    },
    /// Population complete
    Done,
}

impl fmt::Debug for PopulateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopulateState::Start => write!(f, "Start"),
            PopulateState::Processing { rows_processed, .. } => f
                .debug_struct("Processing")
                .field("rows_processed", rows_processed)
                .finish(),
            PopulateState::Done => write!(f, "Done"),
        }
    }
}

/// Per-connection transaction state for incremental views
#[derive(Debug, Clone, Default)]
pub struct ViewTransactionState {
    // Per-connection delta for uncommitted changes (contains both weights and values)
    pub delta: Delta,
}

/// Incremental view that maintains a stream of row keys using DBSP-style computation
/// The actual row data is stored as transformed Values
///
/// This version keeps everything in-memory. This is acceptable for small views, since DBSP
/// doesn't have to track the history of changes. Still for very large views (think of the result
/// of create view v as select * from tbl where x > 1; and that having 1B values.
///
/// We should have a version of this that materializes the results. Materializing will also be good
/// for large aggregations, because then we don't have to re-compute when opening the database
/// again.
///
/// Uses DBSP circuits for incremental computation.
#[derive(Debug)]
pub struct IncrementalView {
    // Stream of row keys for this view
    stream: RowKeyStream,
    name: String,
    // Store the actual row data as Values, keyed by row_key
    // Using BTreeMap for ordered iteration
    pub records: BTreeMap<i64, Vec<Value>>,
    // WHERE clause predicate for filtering (kept for compatibility)
    pub where_predicate: FilterPredicate,
    // The SELECT statement that defines how to transform input data
    pub select_stmt: ast::Select,

    // DBSP circuit that encapsulates the computation
    circuit: DbspCircuit,
    // Track whether circuit has been initialized with data
    circuit_initialized: bool,

    // Tables referenced by this view (extracted from FROM clause and JOINs)
    base_table: Arc<BTreeTable>,
    // The view's output columns with their types
    pub columns: Vec<Column>,
    // State machine for population
    populate_state: PopulateState,
    // Computation tracker for statistics
    // We will use this one day to export rows_read, but for now, will just test that we're doing the expected amount of compute
    #[cfg_attr(not(test), allow(dead_code))]
    pub tracker: Arc<Mutex<ComputationTracker>>,
}

impl IncrementalView {
    /// Validate that a CREATE MATERIALIZED VIEW statement can be handled by IncrementalView
    /// This should be called early, before updating sqlite_master
    pub fn can_create_view(select: &ast::Select) -> Result<()> {
        // Check for JOINs
        let (join_tables, join_condition) = Self::extract_join_info(select);
        if join_tables.is_some() || join_condition.is_some() {
            return Err(LimboError::ParseError(
                "JOINs in views are not yet supported".to_string(),
            ));
        }

        Ok(())
    }

    /// Try to compile the SELECT statement into a DBSP circuit
    fn try_compile_circuit(
        select: &ast::Select,
        schema: &Schema,
        _base_table: &Arc<BTreeTable>,
    ) -> Result<DbspCircuit> {
        // Build the logical plan from the SELECT statement
        let mut builder = LogicalPlanBuilder::new(schema);
        // Convert Select to a Stmt for the builder
        let stmt = ast::Stmt::Select(select.clone());
        let logical_plan = builder.build_statement(&stmt)?;

        // Compile the logical plan to a DBSP circuit
        let compiler = DbspCompiler::new();
        let circuit = compiler.compile(&logical_plan)?;

        Ok(circuit)
    }

    /// Get an iterator over column names, using enumerated naming for unnamed columns
    pub fn column_names(&self) -> impl Iterator<Item = String> + '_ {
        self.columns.iter().enumerate().map(|(i, col)| {
            col.name
                .clone()
                .unwrap_or_else(|| format!("column{}", i + 1))
        })
    }

    /// Check if this view has the same SQL definition as the provided SQL string
    pub fn has_same_sql(&self, sql: &str) -> bool {
        // Parse the SQL to extract just the SELECT statement
        if let Ok(Some(Cmd::Stmt(Stmt::CreateMaterializedView { select, .. }))) =
            Parser::new(sql.as_bytes()).next_cmd()
        {
            // Compare the SELECT statements as SQL strings
            use turso_parser::ast::fmt::ToTokens;

            // Format both SELECT statements and compare
            if let (Ok(current_sql), Ok(provided_sql)) =
                (self.select_stmt.format(), select.format())
            {
                return current_sql == provided_sql;
            }
        }
        false
    }

    pub fn from_sql(sql: &str, schema: &Schema) -> Result<Self> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        let cmd = cmd.expect("View is an empty statement");
        match cmd {
            Cmd::Stmt(Stmt::CreateMaterializedView {
                if_not_exists: _,
                view_name,
                columns: _,
                select,
            }) => IncrementalView::from_stmt(view_name, select, schema),
            _ => Err(LimboError::ParseError(format!(
                "View is not a CREATE MATERIALIZED VIEW statement: {sql}"
            ))),
        }
    }

    pub fn from_stmt(
        view_name: ast::QualifiedName,
        select: ast::Select,
        schema: &Schema,
    ) -> Result<Self> {
        let name = view_name.name.as_str().to_string();

        let where_predicate = FilterPredicate::from_select(&select)?;

        // Extract output columns using the shared function
        let view_columns = extract_view_columns(&select, schema);

        let (join_tables, join_condition) = Self::extract_join_info(&select);
        if join_tables.is_some() || join_condition.is_some() {
            return Err(LimboError::ParseError(
                "JOINs in views are not yet supported".to_string(),
            ));
        }

        // Get the base table from FROM clause (when no joins)
        let base_table = if let Some(base_table_name) = Self::extract_base_table(&select) {
            if let Some(table) = schema.get_btree_table(&base_table_name) {
                table.clone()
            } else {
                return Err(LimboError::ParseError(format!(
                    "Table '{base_table_name}' not found in schema"
                )));
            }
        } else {
            return Err(LimboError::ParseError(
                "views without a base table not supported yet".to_string(),
            ));
        };

        Self::new(
            name,
            where_predicate,
            select.clone(),
            base_table,
            view_columns,
            schema,
        )
    }

    pub fn new(
        name: String,
        where_predicate: FilterPredicate,
        select_stmt: ast::Select,
        base_table: Arc<BTreeTable>,
        columns: Vec<Column>,
        schema: &Schema,
    ) -> Result<Self> {
        let records = BTreeMap::new();

        // Create the tracker that will be shared by all operators
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Compile the SELECT statement into a DBSP circuit
        let circuit = Self::try_compile_circuit(&select_stmt, schema, &base_table)?;

        // Circuit will be initialized when we first call merge_delta
        let circuit_initialized = false;

        Ok(Self {
            stream: RowKeyStream::from_zset(RowKeyZSet::new()),
            name,
            records,
            where_predicate,
            select_stmt,
            circuit,
            circuit_initialized,
            base_table,
            columns,
            populate_state: PopulateState::Start,
            tracker,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get all table names referenced by this view
    pub fn get_referenced_table_names(&self) -> Vec<String> {
        vec![self.base_table.name.clone()]
    }

    /// Get all tables referenced by this view
    pub fn get_referenced_tables(&self) -> Vec<Arc<BTreeTable>> {
        vec![self.base_table.clone()]
    }

    /// Extract the base table name from a SELECT statement (for non-join cases)
    fn extract_base_table(select: &ast::Select) -> Option<String> {
        if let ast::OneSelect::Select {
            from: Some(ref from),
            ..
        } = select.body.select
        {
            if let ast::SelectTable::Table(name, _, _) = from.select.as_ref() {
                return Some(name.name.as_str().to_string());
            }
        }
        None
    }

    /// Generate the SQL query for populating the view from its source table
    fn sql_for_populate(&self) -> crate::Result<String> {
        // Get the base table from referenced tables
        let table = &self.base_table;

        // Check if the table has a rowid alias (INTEGER PRIMARY KEY column)
        let has_rowid_alias = table.columns.iter().any(|col| col.is_rowid_alias);

        // For now, select all columns since we don't have the static operators
        // The circuit will handle filtering and projection
        // If there's a rowid alias, we don't need to select rowid separately
        let select_clause = if has_rowid_alias {
            "*".to_string()
        } else {
            "*, rowid".to_string()
        };

        // Build WHERE clause from the where_predicate
        let where_clause = self.build_where_clause(&self.where_predicate)?;

        // Construct the final query
        let query = if where_clause.is_empty() {
            format!("SELECT {} FROM {}", select_clause, table.name)
        } else {
            format!(
                "SELECT {} FROM {} WHERE {}",
                select_clause, table.name, where_clause
            )
        };
        Ok(query)
    }

    /// Build a WHERE clause from a FilterPredicate
    fn build_where_clause(&self, predicate: &FilterPredicate) -> crate::Result<String> {
        match predicate {
            FilterPredicate::None => Ok(String::new()),
            FilterPredicate::Equals { column, value } => {
                Ok(format!("{} = {}", column, self.value_to_sql(value)))
            }
            FilterPredicate::NotEquals { column, value } => {
                Ok(format!("{} != {}", column, self.value_to_sql(value)))
            }
            FilterPredicate::GreaterThan { column, value } => {
                Ok(format!("{} > {}", column, self.value_to_sql(value)))
            }
            FilterPredicate::GreaterThanOrEqual { column, value } => {
                Ok(format!("{} >= {}", column, self.value_to_sql(value)))
            }
            FilterPredicate::LessThan { column, value } => {
                Ok(format!("{} < {}", column, self.value_to_sql(value)))
            }
            FilterPredicate::LessThanOrEqual { column, value } => {
                Ok(format!("{} <= {}", column, self.value_to_sql(value)))
            }
            FilterPredicate::And(left, right) => {
                let left_clause = self.build_where_clause(left)?;
                let right_clause = self.build_where_clause(right)?;
                Ok(format!("({left_clause} AND {right_clause})"))
            }
            FilterPredicate::Or(left, right) => {
                let left_clause = self.build_where_clause(left)?;
                let right_clause = self.build_where_clause(right)?;
                Ok(format!("({left_clause} OR {right_clause})"))
            }
        }
    }

    /// Convert a Value to SQL literal representation
    fn value_to_sql(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(t) => format!("'{}'", t.as_str().replace('\'', "''")),
            Value::Blob(_) => "NULL".to_string(), // Blob literals not supported in WHERE clause yet
        }
    }

    /// Populate the view by scanning the source table using a state machine
    /// This can be called multiple times and will resume from where it left off
    pub fn populate_from_table(
        &mut self,
        conn: &std::sync::Arc<crate::Connection>,
    ) -> crate::Result<IOResult<()>> {
        // If already populated, return immediately
        if matches!(self.populate_state, PopulateState::Done) {
            return Ok(IOResult::Done(()));
        }

        const BATCH_SIZE: usize = 100; // Process 100 rows at a time before yielding

        loop {
            match &mut self.populate_state {
                PopulateState::Start => {
                    // Generate the SQL query for populating the view
                    // It is best to use a standard query than a cursor for two reasons:
                    // 1) Using a sql query will allow us to be much more efficient in cases where we only want
                    //    some rows, in particular for indexed filters
                    // 2) There are two types of cursors: index and table. In some situations (like for example
                    //    if the table has an integer primary key), the key will be exclusively in the index
                    //    btree and not in the table btree. Using cursors would force us to be aware of this
                    //    distinction (and others), and ultimately lead to reimplementing the whole query
                    //    machinery (next step is which index is best to use, etc)
                    let query = self.sql_for_populate()?;

                    // Prepare the statement
                    let stmt = conn.prepare(&query)?;

                    self.populate_state = PopulateState::Processing {
                        stmt: Box::new(stmt),
                        rows_processed: 0,
                    };
                    // Continue to next state
                }

                PopulateState::Processing {
                    stmt,
                    rows_processed,
                } => {
                    // Collect rows into a delta batch
                    let mut batch_delta = Delta::new();
                    let mut batch_count = 0;

                    loop {
                        if batch_count >= BATCH_SIZE {
                            // Process this batch through the standard pipeline
                            self.merge_delta(&batch_delta);
                            // Yield control after processing a batch
                            // TODO: currently this inner statement is the one that is tracking completions
                            // so as a stop gap we can just return a dummy completion here
                            io_yield_one!(Completion::new_dummy());
                        }

                        // This step() call resumes from where the statement left off
                        match stmt.step()? {
                            crate::vdbe::StepResult::Row => {
                                // Get the row
                                let row = stmt.row().unwrap();

                                // Extract values from the row
                                let all_values: Vec<crate::types::Value> =
                                    row.get_values().cloned().collect();

                                // Determine how to extract the rowid
                                // If there's a rowid alias (INTEGER PRIMARY KEY), the rowid is one of the columns
                                // Otherwise, it's the last value we explicitly selected
                                let (rowid, values) = if let Some((idx, _)) =
                                    self.base_table.get_rowid_alias_column()
                                {
                                    // The rowid is the value at the rowid alias column index
                                    let rowid = match all_values.get(idx) {
                                        Some(crate::types::Value::Integer(id)) => *id,
                                        _ => {
                                            // This shouldn't happen - rowid alias must be an integer
                                            *rows_processed += 1;
                                            batch_count += 1;
                                            continue;
                                        }
                                    };
                                    // All values are table columns (no separate rowid was selected)
                                    (rowid, all_values)
                                } else {
                                    // The last value is the explicitly selected rowid
                                    let rowid = match all_values.last() {
                                        Some(crate::types::Value::Integer(id)) => *id,
                                        _ => {
                                            // This shouldn't happen - rowid must be an integer
                                            *rows_processed += 1;
                                            batch_count += 1;
                                            continue;
                                        }
                                    };
                                    // Get all values except the rowid
                                    let values = all_values[..all_values.len() - 1].to_vec();
                                    (rowid, values)
                                };

                                // Add to batch delta - let merge_delta handle filtering and aggregation
                                batch_delta.insert(rowid, values);

                                *rows_processed += 1;
                                batch_count += 1;
                            }
                            crate::vdbe::StepResult::Done => {
                                // Process any remaining rows in the batch
                                self.merge_delta(&batch_delta);
                                // All rows processed, move to Done state
                                self.populate_state = PopulateState::Done;
                                return Ok(IOResult::Done(()));
                            }
                            crate::vdbe::StepResult::Interrupt | crate::vdbe::StepResult::Busy => {
                                return Err(LimboError::Busy);
                            }
                            crate::vdbe::StepResult::IO => {
                                // Process current batch before yielding
                                self.merge_delta(&batch_delta);
                                // The Statement needs to wait for IO
                                io_yield_one!(Completion::new_dummy());
                            }
                        }
                    }
                }

                PopulateState::Done => {
                    // Already populated
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Extract JOIN information from SELECT statement
    #[allow(clippy::type_complexity)]
    pub fn extract_join_info(
        select: &ast::Select,
    ) -> (Option<(String, String)>, Option<(String, String)>) {
        use turso_parser::ast::*;

        if let OneSelect::Select {
            from: Some(ref from),
            ..
        } = select.body.select
        {
            // Check if there are any joins
            if !from.joins.is_empty() {
                // Get the first (left) table name
                let left_table = match from.select.as_ref() {
                    SelectTable::Table(name, _, _) => Some(name.name.as_str().to_string()),
                    _ => None,
                };

                // Get the first join (right) table and condition
                if let Some(first_join) = from.joins.first() {
                    let right_table = match &first_join.table.as_ref() {
                        SelectTable::Table(name, _, _) => Some(name.name.as_str().to_string()),
                        _ => None,
                    };

                    // Extract join condition (simplified - assumes single equality)
                    let join_condition = if let Some(ref constraint) = &first_join.constraint {
                        match constraint {
                            JoinConstraint::On(expr) => Self::extract_join_columns_from_expr(expr),
                            _ => None,
                        }
                    } else {
                        None
                    };

                    if let (Some(left), Some(right)) = (left_table, right_table) {
                        return (Some((left, right)), join_condition);
                    }
                }
            }
        }

        (None, None)
    }

    /// Extract join column names from a join condition expression
    fn extract_join_columns_from_expr(expr: &ast::Expr) -> Option<(String, String)> {
        use turso_parser::ast::*;

        // Look for expressions like: t1.col = t2.col
        if let Expr::Binary(left, op, right) = expr {
            if matches!(op, Operator::Equals) {
                // Extract column names from both sides
                let left_col = match &**left {
                    Expr::Qualified(name, _) => Some(name.as_str().to_string()),
                    Expr::Id(name) => Some(name.as_str().to_string()),
                    _ => None,
                };

                let right_col = match &**right {
                    Expr::Qualified(name, _) => Some(name.as_str().to_string()),
                    Expr::Id(name) => Some(name.as_str().to_string()),
                    _ => None,
                };

                if let (Some(l), Some(r)) = (left_col, right_col) {
                    return Some((l, r));
                }
            }
        }

        None
    }

    /// Get the current records as an iterator - for cursor-based access
    pub fn iter(&self) -> impl Iterator<Item = (i64, Vec<Value>)> + '_ {
        self.stream.to_vec().into_iter().filter_map(move |row| {
            self.records
                .get(&row.rowid)
                .map(|values| (row.rowid, values.clone()))
        })
    }

    /// Get current data merged with transaction state
    pub fn current_data(&self, tx_state: Option<&ViewTransactionState>) -> Vec<(i64, Vec<Value>)> {
        if let Some(tx_state) = tx_state {
            // Use circuit to process uncommitted changes
            let mut uncommitted = DeltaSet::new();
            uncommitted.insert(self.base_table.name.clone(), tx_state.delta.clone());

            // Execute with uncommitted changes (won't affect circuit state)
            match self.circuit.execute(HashMap::new(), uncommitted) {
                Ok(processed_delta) => {
                    // Merge processed delta with committed records
                    let mut result_map: BTreeMap<i64, Vec<Value>> = self.records.clone();
                    for (row, weight) in &processed_delta.changes {
                        if *weight > 0 {
                            result_map.insert(row.rowid, row.values.clone());
                        } else if *weight < 0 {
                            result_map.remove(&row.rowid);
                        }
                    }
                    result_map.into_iter().collect()
                }
                Err(e) => {
                    // Return error or panic - no fallback
                    panic!("Failed to execute circuit with uncommitted data: {e:?}");
                }
            }
        } else {
            // No transaction state: return committed records
            self.records.clone().into_iter().collect()
        }
    }

    /// Merge a delta of changes into the view's current state
    pub fn merge_delta(&mut self, delta: &Delta) {
        // Early return if delta is empty
        if delta.is_empty() {
            return;
        }

        // Use the circuit to process the delta
        let mut input_data = HashMap::new();
        input_data.insert(self.base_table.name.clone(), delta.clone());

        // If circuit hasn't been initialized yet, initialize it first
        // This happens during populate_from_table
        if !self.circuit_initialized {
            // Initialize the circuit with empty state
            self.circuit
                .initialize(HashMap::new())
                .expect("Failed to initialize circuit");
            self.circuit_initialized = true;
        }

        // Execute the circuit to process the delta
        let current_delta = match self.circuit.execute(input_data.clone(), DeltaSet::empty()) {
            Ok(output) => {
                // Commit the changes to the circuit's internal state
                self.circuit
                    .commit(input_data)
                    .expect("Failed to commit to circuit");
                output
            }
            Err(e) => {
                panic!("Failed to execute circuit: {e:?}");
            }
        };

        // Update records and stream with the processed delta
        let mut zset_delta = RowKeyZSet::new();

        for (row, weight) in &current_delta.changes {
            if *weight > 0 {
                self.records.insert(row.rowid, row.values.clone());
                zset_delta.insert(row.clone(), 1);
            } else if *weight < 0 {
                self.records.remove(&row.rowid);
                zset_delta.insert(row.clone(), -1);
            }
        }

        self.stream.apply_delta(&zset_delta);
    }
}
