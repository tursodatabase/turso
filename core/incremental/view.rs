use super::compiler::{DbspCircuit, DbspCompiler, DeltaSet};
use super::dbsp::Delta;
use super::operator::{ComputationTracker, FilterPredicate};
use crate::schema::{BTreeTable, Column, Schema};
use crate::storage::btree::BTreeCursor;
use crate::translate::logical::LogicalPlanBuilder;
use crate::types::{IOResult, Value};
use crate::util::extract_view_columns;
use crate::{return_if_io, LimboError, Pager, Result, Statement};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
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
        /// If we're in the middle of processing a row (merge_delta returned I/O)
        pending_row: Option<(i64, Vec<Value>)>, // (rowid, values)
    },
    /// Population complete
    Done,
}

/// State machine for merge_delta to handle I/O operations
impl fmt::Debug for PopulateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopulateState::Start => write!(f, "Start"),
            PopulateState::Processing {
                rows_processed,
                pending_row,
                ..
            } => f
                .debug_struct("Processing")
                .field("rows_processed", rows_processed)
                .field("has_pending", &pending_row.is_some())
                .finish(),
            PopulateState::Done => write!(f, "Done"),
        }
    }
}

/// Per-connection transaction state for incremental views
#[derive(Debug, Clone, Default)]
pub struct ViewTransactionState {
    // Per-table deltas for uncommitted changes
    // Maps table_name -> Delta for that table
    // Using RefCell for interior mutability
    table_deltas: RefCell<HashMap<String, Delta>>,
}

impl ViewTransactionState {
    /// Create a new transaction state
    pub fn new() -> Self {
        Self {
            table_deltas: RefCell::new(HashMap::new()),
        }
    }

    /// Insert a row into the delta for a specific table
    pub fn insert(&self, table_name: &str, key: i64, values: Vec<Value>) {
        let mut deltas = self.table_deltas.borrow_mut();
        let delta = deltas.entry(table_name.to_string()).or_default();
        delta.insert(key, values);
    }

    /// Delete a row from the delta for a specific table
    pub fn delete(&self, table_name: &str, key: i64, values: Vec<Value>) {
        let mut deltas = self.table_deltas.borrow_mut();
        let delta = deltas.entry(table_name.to_string()).or_default();
        delta.delete(key, values);
    }

    /// Clear all changes in the delta
    pub fn clear(&self) {
        self.table_deltas.borrow_mut().clear();
    }

    /// Get deltas organized by table
    pub fn get_table_deltas(&self) -> HashMap<String, Delta> {
        self.table_deltas.borrow().clone()
    }

    /// Check if the delta is empty
    pub fn is_empty(&self) -> bool {
        self.table_deltas.borrow().values().all(|d| d.is_empty())
    }

    /// Returns how many elements exist in the delta.
    pub fn len(&self) -> usize {
        self.table_deltas.borrow().values().map(|d| d.len()).sum()
    }
}

/// Container for all view transaction states within a connection
/// Provides interior mutability for the map of view states
#[derive(Debug, Clone, Default)]
pub struct AllViewsTxState {
    states: Rc<RefCell<HashMap<String, Rc<ViewTransactionState>>>>,
}

impl AllViewsTxState {
    /// Create a new container for view transaction states
    pub fn new() -> Self {
        Self {
            states: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Get or create a transaction state for a view
    pub fn get_or_create(&self, view_name: &str) -> Rc<ViewTransactionState> {
        let mut states = self.states.borrow_mut();
        states
            .entry(view_name.to_string())
            .or_insert_with(|| Rc::new(ViewTransactionState::new()))
            .clone()
    }

    /// Get a transaction state for a view if it exists
    pub fn get(&self, view_name: &str) -> Option<Rc<ViewTransactionState>> {
        self.states.borrow().get(view_name).cloned()
    }

    /// Clear all transaction states
    pub fn clear(&self) {
        self.states.borrow_mut().clear();
    }

    /// Check if there are no transaction states
    pub fn is_empty(&self) -> bool {
        self.states.borrow().is_empty()
    }

    /// Get all view names that have transaction states
    pub fn get_view_names(&self) -> Vec<String> {
        self.states.borrow().keys().cloned().collect()
    }
}

/// Incremental view that maintains its state through a DBSP circuit
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
    name: String,
    // WHERE clause predicate for filtering (kept for compatibility)
    pub where_predicate: FilterPredicate,
    // The SELECT statement that defines how to transform input data
    pub select_stmt: ast::Select,

    // DBSP circuit that encapsulates the computation
    circuit: DbspCircuit,

    // All tables referenced by this view (from FROM clause and JOINs)
    referenced_tables: Vec<Arc<BTreeTable>>,
    // The view's output columns with their types
    pub columns: Vec<Column>,
    // State machine for population
    populate_state: PopulateState,
    // Computation tracker for statistics
    // We will use this one day to export rows_read, but for now, will just test that we're doing the expected amount of compute
    #[cfg_attr(not(test), allow(dead_code))]
    pub tracker: Arc<Mutex<ComputationTracker>>,
    // Root page of the btree storing the materialized state (0 for unmaterialized)
    root_page: usize,
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
        main_data_root: usize,
        internal_state_root: usize,
    ) -> Result<DbspCircuit> {
        // Build the logical plan from the SELECT statement
        let mut builder = LogicalPlanBuilder::new(schema);
        // Convert Select to a Stmt for the builder
        let stmt = ast::Stmt::Select(select.clone());
        let logical_plan = builder.build_statement(&stmt)?;

        // Compile the logical plan to a DBSP circuit with the storage roots
        let compiler = DbspCompiler::new(main_data_root, internal_state_root);
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
            return self.select_stmt == select;
        }
        false
    }

    /// Validate a SELECT statement and extract the columns it would produce
    /// This is used during CREATE MATERIALIZED VIEW to validate the view before storing it
    pub fn validate_and_extract_columns(
        select: &ast::Select,
        schema: &Schema,
    ) -> Result<Vec<crate::schema::Column>> {
        // For now, just extract columns from a simple select
        // This will need to be expanded to handle joins, aggregates, etc.

        // Get the base table name
        let base_table_name = Self::extract_base_table(select).ok_or_else(|| {
            LimboError::ParseError("Cannot extract base table from SELECT".to_string())
        })?;

        // Get the table from schema
        let table = schema
            .get_table(&base_table_name)
            .and_then(|t| t.btree())
            .ok_or_else(|| LimboError::ParseError(format!("Table {base_table_name} not found")))?;

        // For now, return all columns from the base table
        // In the future, this should parse the select list and handle projections
        Ok(table.columns.clone())
    }

    pub fn from_sql(
        sql: &str,
        schema: &Schema,
        main_data_root: usize,
        internal_state_root: usize,
    ) -> Result<Self> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        let cmd = cmd.expect("View is an empty statement");
        match cmd {
            Cmd::Stmt(Stmt::CreateMaterializedView {
                if_not_exists: _,
                view_name,
                columns: _,
                select,
            }) => IncrementalView::from_stmt(
                view_name,
                select,
                schema,
                main_data_root,
                internal_state_root,
            ),
            _ => Err(LimboError::ParseError(format!(
                "View is not a CREATE MATERIALIZED VIEW statement: {sql}"
            ))),
        }
    }

    pub fn from_stmt(
        view_name: ast::QualifiedName,
        select: ast::Select,
        schema: &Schema,
        main_data_root: usize,
        internal_state_root: usize,
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

        // Get all tables from FROM clause and JOINs
        let referenced_tables = Self::extract_all_tables(&select, schema)?;

        Self::new(
            name,
            where_predicate,
            select.clone(),
            referenced_tables,
            view_columns,
            schema,
            main_data_root,
            internal_state_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        where_predicate: FilterPredicate,
        select_stmt: ast::Select,
        referenced_tables: Vec<Arc<BTreeTable>>,
        columns: Vec<Column>,
        schema: &Schema,
        main_data_root: usize,
        internal_state_root: usize,
    ) -> Result<Self> {
        // Create the tracker that will be shared by all operators
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Compile the SELECT statement into a DBSP circuit
        let circuit =
            Self::try_compile_circuit(&select_stmt, schema, main_data_root, internal_state_root)?;

        Ok(Self {
            name,
            where_predicate,
            select_stmt,
            circuit,
            referenced_tables,
            columns,
            populate_state: PopulateState::Start,
            tracker,
            root_page: main_data_root,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Execute the circuit with uncommitted changes to get processed delta
    pub fn execute_with_uncommitted(
        &mut self,
        uncommitted: DeltaSet,
        pager: Rc<Pager>,
        execute_state: &mut crate::incremental::compiler::ExecuteState,
    ) -> crate::Result<crate::types::IOResult<Delta>> {
        // Initialize execute_state with the input data
        *execute_state = crate::incremental::compiler::ExecuteState::Init {
            input_data: uncommitted,
        };
        self.circuit.execute(pager, execute_state)
    }

    /// Get the root page for this materialized view's btree
    pub fn get_root_page(&self) -> usize {
        self.root_page
    }

    /// Get all table names referenced by this view
    pub fn get_referenced_table_names(&self) -> Vec<String> {
        self.referenced_tables
            .iter()
            .map(|t| t.name.clone())
            .collect()
    }

    /// Get all tables referenced by this view
    pub fn get_referenced_tables(&self) -> Vec<Arc<BTreeTable>> {
        self.referenced_tables.clone()
    }

    /// Extract all table names from a SELECT statement (including JOINs)
    fn extract_all_tables(select: &ast::Select, schema: &Schema) -> Result<Vec<Arc<BTreeTable>>> {
        let mut tables = Vec::new();

        if let ast::OneSelect::Select {
            from: Some(ref from),
            ..
        } = select.body.select
        {
            // Get the main table from FROM clause
            if let ast::SelectTable::Table(name, _, _) = from.select.as_ref() {
                let table_name = name.name.as_str();
                if let Some(table) = schema.get_btree_table(table_name) {
                    tables.push(table.clone());
                } else {
                    return Err(LimboError::ParseError(format!(
                        "Table '{table_name}' not found in schema"
                    )));
                }
            }

            // Get all tables from JOIN clauses
            for join in &from.joins {
                if let ast::SelectTable::Table(name, _, _) = join.table.as_ref() {
                    let table_name = name.name.as_str();
                    if let Some(table) = schema.get_btree_table(table_name) {
                        tables.push(table.clone());
                    } else {
                        return Err(LimboError::ParseError(format!(
                            "Table '{table_name}' not found in schema"
                        )));
                    }
                }
            }
        }

        if tables.is_empty() {
            return Err(LimboError::ParseError(
                "No tables found in SELECT statement".to_string(),
            ));
        }

        Ok(tables)
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
        // Get the first table from referenced tables
        if self.referenced_tables.is_empty() {
            return Err(LimboError::ParseError(
                "No tables to populate from".to_string(),
            ));
        }
        let table = &self.referenced_tables[0];

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
    /// This method is only for materialized views and will persist data to the btree
    pub fn populate_from_table(
        &mut self,
        conn: &std::sync::Arc<crate::Connection>,
        pager: &std::rc::Rc<crate::Pager>,
        _btree_cursor: &mut BTreeCursor,
    ) -> crate::Result<IOResult<()>> {
        // If already populated, return immediately
        if matches!(self.populate_state, PopulateState::Done) {
            return Ok(IOResult::Done(()));
        }

        // Assert that this is a materialized view with a root page
        assert!(
            self.root_page != 0,
            "populate_from_table should only be called for materialized views with root_page"
        );

        loop {
            // To avoid borrow checker issues, we need to handle state transitions carefully
            let needs_start = matches!(self.populate_state, PopulateState::Start);

            if needs_start {
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
                    pending_row: None,
                };
                // Continue to next state
                continue;
            }

            // Handle Done state
            if matches!(self.populate_state, PopulateState::Done) {
                return Ok(IOResult::Done(()));
            }

            // Handle Processing state - extract state to avoid borrow issues
            let (mut stmt, mut rows_processed, pending_row) =
                match std::mem::replace(&mut self.populate_state, PopulateState::Done) {
                    PopulateState::Processing {
                        stmt,
                        rows_processed,
                        pending_row,
                    } => (stmt, rows_processed, pending_row),
                    _ => unreachable!("We already handled Start and Done states"),
                };

            // If we have a pending row from a previous I/O interruption, process it first
            if let Some((rowid, values)) = pending_row {
                // Create a single-row delta for the pending row
                let mut single_row_delta = Delta::new();
                single_row_delta.insert(rowid, values.clone());

                // Create a DeltaSet with this delta for the first table (for now)
                let mut delta_set = DeltaSet::new();
                // TODO: When we support JOINs, determine which table this row came from
                delta_set.insert(self.referenced_tables[0].name.clone(), single_row_delta);

                // Process the pending row with the pager
                match self.merge_delta(delta_set, pager.clone())? {
                    IOResult::Done(_) => {
                        // Row processed successfully, continue to next row
                        rows_processed += 1;
                        // Continue to fetch next row from statement
                    }
                    IOResult::IO(io) => {
                        // Still not done, save state with pending row
                        self.populate_state = PopulateState::Processing {
                            stmt,
                            rows_processed,
                            pending_row: Some((rowid, values)), // Keep the pending row
                        };
                        return Ok(IOResult::IO(io));
                    }
                }
            }

            // Process rows one at a time - no batching
            loop {
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
                            self.referenced_tables[0].get_rowid_alias_column()
                        {
                            // The rowid is the value at the rowid alias column index
                            let rowid = match all_values.get(idx) {
                                Some(crate::types::Value::Integer(id)) => *id,
                                _ => {
                                    // This shouldn't happen - rowid alias must be an integer
                                    rows_processed += 1;
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
                                    rows_processed += 1;
                                    continue;
                                }
                            };
                            // Get all values except the rowid
                            let values = all_values[..all_values.len() - 1].to_vec();
                            (rowid, values)
                        };

                        // Create a single-row delta and process it immediately
                        let mut single_row_delta = Delta::new();
                        single_row_delta.insert(rowid, values.clone());

                        // Create a DeltaSet with this delta for the first table (for now)
                        let mut delta_set = DeltaSet::new();
                        // TODO: When we support JOINs, determine which table this row came from
                        delta_set.insert(self.referenced_tables[0].name.clone(), single_row_delta);

                        // Process this single row through merge_delta with the pager
                        match self.merge_delta(delta_set, pager.clone())? {
                            IOResult::Done(_) => {
                                // Row processed successfully, continue to next row
                                rows_processed += 1;
                            }
                            IOResult::IO(io) => {
                                // Save state and return I/O
                                // We'll resume at the SAME row when called again (don't increment rows_processed)
                                // The circuit still has unfinished work for this row
                                self.populate_state = PopulateState::Processing {
                                    stmt,
                                    rows_processed, // Don't increment - row not done yet!
                                    pending_row: Some((rowid, values)), // Save the row for resumption
                                };
                                return Ok(IOResult::IO(io));
                            }
                        }
                    }

                    crate::vdbe::StepResult::Done => {
                        // All rows processed, we're done
                        self.populate_state = PopulateState::Done;
                        return Ok(IOResult::Done(()));
                    }

                    crate::vdbe::StepResult::Interrupt | crate::vdbe::StepResult::Busy => {
                        // Save state before returning error
                        self.populate_state = PopulateState::Processing {
                            stmt,
                            rows_processed,
                            pending_row: None, // No pending row when interrupted between rows
                        };
                        return Err(LimboError::Busy);
                    }

                    crate::vdbe::StepResult::IO => {
                        // Statement needs I/O - save state and return
                        self.populate_state = PopulateState::Processing {
                            stmt,
                            rows_processed,
                            pending_row: None, // No pending row when interrupted between rows
                        };
                        // TODO: Get the actual I/O completion from the statement
                        let completion = crate::io::Completion::new_dummy();
                        return Ok(IOResult::IO(crate::types::IOCompletions::Single(
                            completion,
                        )));
                    }
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

    /// Merge a delta set of changes into the view's current state
    pub fn merge_delta(
        &mut self,
        delta_set: DeltaSet,
        pager: std::rc::Rc<crate::Pager>,
    ) -> crate::Result<IOResult<()>> {
        // Early return if all deltas are empty
        if delta_set.is_empty() {
            return Ok(IOResult::Done(()));
        }

        // Use the circuit to process the deltas and write to btree
        let input_data = delta_set.into_map();

        // The circuit now handles all btree I/O internally with the provided pager
        let _delta = return_if_io!(self.circuit.commit(input_data, pager));
        Ok(IOResult::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BTreeTable, Column as SchemaColumn, Schema, Type};
    use std::sync::Arc;
    use turso_parser::ast;
    use turso_parser::parser::Parser;

    // Helper function to create a test schema with multiple tables
    fn create_test_schema() -> Schema {
        let mut schema = Schema::new(false);

        // Create customers table
        let customers_table = BTreeTable {
            name: "customers".to_string(),
            root_page: 2,
            primary_key_columns: vec![("id".to_string(), ast::SortOrder::Asc)],
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
            ],
            has_rowid: true,
            is_strict: false,
            unique_sets: vec![],
        };

        // Create orders table
        let orders_table = BTreeTable {
            name: "orders".to_string(),
            root_page: 3,
            primary_key_columns: vec![("id".to_string(), ast::SortOrder::Asc)],
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
                    name: Some("customer_id".to_string()),
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
                    name: Some("total".to_string()),
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
            unique_sets: vec![],
        };

        // Create products table
        let products_table = BTreeTable {
            name: "products".to_string(),
            root_page: 4,
            primary_key_columns: vec![("id".to_string(), ast::SortOrder::Asc)],
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
            ],
            has_rowid: true,
            is_strict: false,
            unique_sets: vec![],
        };

        schema.add_btree_table(Arc::new(customers_table));
        schema.add_btree_table(Arc::new(orders_table));
        schema.add_btree_table(Arc::new(products_table));
        schema
    }

    // Helper to parse SQL and extract the SELECT statement
    fn parse_select(sql: &str) -> ast::Select {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next().unwrap().unwrap();
        match cmd {
            ast::Cmd::Stmt(ast::Stmt::Select(select)) => select,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_extract_single_table() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers");

        let tables = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "customers");
    }

    #[test]
    fn test_extract_tables_from_inner_join() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id",
        );

        let tables = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_from_multiple_joins() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers
             INNER JOIN orders ON customers.id = orders.customer_id
             INNER JOIN products ON orders.id = products.id",
        );

        let tables = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
        assert_eq!(tables[2].name, "products");
    }

    #[test]
    fn test_extract_tables_from_left_join() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers LEFT JOIN orders ON customers.id = orders.customer_id",
        );

        let tables = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_from_cross_join() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers CROSS JOIN orders");

        let tables = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_with_aliases() {
        let schema = create_test_schema();
        let select =
            parse_select("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id");

        let tables = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        // Should still extract the actual table names, not aliases
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_nonexistent_table_error() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM nonexistent");

        let result = IncrementalView::extract_all_tables(&select, &schema);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Table 'nonexistent' not found"));
    }

    #[test]
    fn test_extract_tables_nonexistent_join_table_error() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers INNER JOIN nonexistent ON customers.id = nonexistent.id",
        );

        let result = IncrementalView::extract_all_tables(&select, &schema);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Table 'nonexistent' not found"));
    }
}
