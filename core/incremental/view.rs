use super::compiler::{DbspCircuit, DbspCompiler, DeltaSet};
use super::dbsp::Delta;
use super::operator::ComputationTracker;
use crate::numeric::Numeric;
use crate::schema::{BTreeTable, ColDef, Column, Type};
use crate::storage::btree::CursorTrait;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::translate::logical::{LogicalPlan, LogicalPlanBuilder};
use crate::translate::semantic::{
    self,
    context::SemanticContext,
    hir::{CatalogObjectId, HirDocument, HirRoot, QueryId, SourceKind},
    AnalyzeInput,
};
use crate::types::{IOResult, Value};
use crate::util::{normalize_ident, quote_identifier, ViewColumn, ViewColumnSchema};
use crate::{return_if_io, LimboError, Pager, Result, Statement};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use turso_parser::ast;
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

/// State machine for populating a view from its source table
pub enum PopulateState {
    /// Initial state - need to prepare the query
    Start,
    /// All tables that need to be populated
    ProcessingAllTables {
        queries: Vec<String>,
        current_idx: usize,
    },
    /// Actively processing rows from the query
    ProcessingOneTable {
        queries: Vec<String>,
        current_idx: usize,
        stmt: Box<Statement>,
        rows_processed: usize,
        /// If we're in the middle of processing a row (merge_delta returned I/O)
        pending_row: Option<(i64, Vec<Value>)>, // (rowid, values)
    },
    /// Population complete
    Done,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for PopulateState {}
unsafe impl Sync for PopulateState {}
crate::assert::assert_send_sync!(PopulateState);

/// State machine for merge_delta to handle I/O operations
impl fmt::Debug for PopulateState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopulateState::Start => write!(f, "Start"),
            PopulateState::ProcessingAllTables {
                current_idx,
                queries,
            } => f
                .debug_struct("ProcessingAllTables")
                .field("current_idx", current_idx)
                .field("num_queries", &queries.len())
                .finish(),
            PopulateState::ProcessingOneTable {
                current_idx,
                rows_processed,
                pending_row,
                queries,
                ..
            } => f
                .debug_struct("ProcessingOneTable")
                .field("current_idx", current_idx)
                .field("rows_processed", rows_processed)
                .field("has_pending", &pending_row.is_some())
                .field("total_queries", &queries.len())
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
            table_deltas: RefCell::new(HashMap::default()),
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
    states: Rc<RefCell<HashMap<String, Arc<ViewTransactionState>>>>,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for AllViewsTxState {}
unsafe impl Sync for AllViewsTxState {}
crate::assert::assert_send_sync!(AllViewsTxState);

impl AllViewsTxState {
    /// Create a new container for view transaction states
    pub fn new() -> Self {
        Self {
            states: Rc::new(RefCell::new(HashMap::default())),
        }
    }

    /// Get or create a transaction state for a view
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn get_or_create(&self, view_name: &str) -> Arc<ViewTransactionState> {
        let mut states = self.states.borrow_mut();
        // ViewTransactionState uses RefCell (not Sync), but AllViewsTxState is
        // single-threaded (Rc-based). Arc is used for shared ownership, not
        // cross-thread sharing.
        states
            .entry(view_name.to_string())
            .or_insert_with(|| Arc::new(ViewTransactionState::new()))
            .clone()
    }

    /// Get a transaction state for a view if it exists
    pub fn get(&self, view_name: &str) -> Option<Arc<ViewTransactionState>> {
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

/// A materialized-view plan whose SQL meaning is already resolved, but whose
/// storage roots do not exist yet.
///
/// CREATE MATERIALIZED VIEW builds this during statement preparation. Schema
/// reload builds the same shape after parsing and analyzing the stored SQL.
/// Instantiation only supplies the three btree roots allocated for this copy.
#[derive(Clone, Debug)]
pub(crate) struct IncrementalViewTemplate {
    logical_plan: LogicalPlan,
    referenced_tables: Vec<Arc<BTreeTable>>,
    column_schema: ViewColumnSchema,
}

impl IncrementalViewTemplate {
    pub(crate) fn analyze_select(
        context: &SemanticContext<'_>,
        select: &ast::Select,
    ) -> Result<Self> {
        let statement = ast::Stmt::Select(select.clone());
        let document = semantic::analyze(context, AnalyzeInput::Statement(&statement))?;
        let HirRoot::Query(root) = &document.root else {
            return Err(LimboError::InternalError(
                "SELECT semantic analysis returned a non-query root".to_string(),
            ));
        };
        Self::from_hir(&document, root.query)
    }

    pub(crate) fn analyze_stored_sql(context: &SemanticContext<'_>, sql: &str) -> Result<Self> {
        let mut parser = Parser::new(sql.as_bytes());
        let command = parser
            .next_cmd()?
            .ok_or_else(|| LimboError::ParseError("materialized view SQL is empty".to_string()))?;
        let Cmd::Stmt(Stmt::CreateMaterializedView { select, .. }) = command else {
            return Err(LimboError::ParseError(format!(
                "View is not a CREATE MATERIALIZED VIEW statement: {sql}"
            )));
        };
        Self::analyze_select(context, &select)
    }

    fn from_hir(document: &HirDocument, query: QueryId) -> Result<Self> {
        let mut builder = LogicalPlanBuilder::new(document);
        let logical_plan = builder.build_query(query)?;
        let referenced_tables = Self::referenced_tables(document)?;
        let column_schema = Self::derive_column_schema(document, query)?;
        Ok(Self {
            logical_plan,
            referenced_tables,
            column_schema,
        })
    }

    fn referenced_tables(document: &HirDocument) -> Result<Vec<Arc<BTreeTable>>> {
        let mut seen = HashSet::<CatalogObjectId>::default();
        let mut tables = Vec::new();
        for source in &document.sources {
            let SourceKind::Table(table) = &source.kind else {
                continue;
            };
            if table
                .database()
                .is_none_or(|database| database.index() != crate::MAIN_DB_ID)
            {
                return Err(LimboError::ParseError(
                    "materialized views may only read tables in the main database".to_string(),
                ));
            }
            if !seen.insert(table.id()) {
                continue;
            }
            let Some(table) = table.value().btree() else {
                return Err(LimboError::ParseError(format!(
                    "materialized views cannot read virtual table '{}'",
                    table.value().get_name()
                )));
            };
            tables.push(table);
        }
        if tables.is_empty() {
            return Err(LimboError::ParseError(
                "No tables to populate from".to_string(),
            ));
        }
        Ok(tables)
    }

    fn derive_column_schema(document: &HirDocument, query: QueryId) -> Result<ViewColumnSchema> {
        let query = document.query(query).ok_or_else(|| {
            LimboError::InternalError(format!("semantic HIR has no query {query}"))
        })?;
        let mut name_counts = HashMap::<String, usize>::default();
        let mut columns = Vec::with_capacity(query.output.len());
        for output_id in &query.output {
            let output = document.output(*output_id).ok_or_else(|| {
                LimboError::InternalError(format!("semantic HIR has no output {output_id:?}"))
            })?;
            // Dynamic SQL expressions deliberately have no declared affinity.
            // The backing table still needs a physical type tag, so use BLOB as
            // the no-coercion storage default while keeping `ty_str` empty.
            let storage = output.type_fact.storage.unwrap_or(Type::Blob);
            let base_name = output.name.clone();
            let count = name_counts.entry(normalize_ident(&base_name)).or_insert(0);
            let name = if *count == 0 {
                base_name
            } else {
                format!("{base_name}:{count}")
            };
            *count += 1;

            let declared = output.type_fact.declared.as_ref();
            let ty_str = declared
                .map(|declared| declared.name.clone())
                .unwrap_or_else(|| {
                    if output.has_affinity {
                        storage.to_string()
                    } else {
                        String::new()
                    }
                });
            let collation = output
                .collation
                .as_ref()
                .map(|collation| *collation.value());
            let mut column = Column::new(
                Some(name),
                ty_str,
                None,
                None,
                storage,
                collation,
                ColDef::default(),
            );
            if let Some(declared) = declared {
                column.set_array_dimensions(declared.array_dimensions);
                if !declared.custom_chain.is_empty() {
                    column.set_base_affinity(output.affinity);
                }
            }
            columns.push(ViewColumn {
                table_index: usize::MAX,
                column,
            });
        }
        Ok(ViewColumnSchema {
            tables: Vec::new(),
            columns,
        })
    }

    pub(crate) fn column_schema(&self) -> &ViewColumnSchema {
        &self.column_schema
    }

    fn compile(
        &self,
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Result<DbspCircuit> {
        DbspCompiler::new(
            main_data_root,
            internal_state_root,
            internal_state_index_root,
        )
        .compile(&self.logical_plan)
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
    // DBSP circuit that encapsulates the computation
    circuit: DbspCircuit,

    // All tables referenced by this view (from FROM clause and JOINs)
    referenced_tables: Vec<Arc<BTreeTable>>,
    // The view's column schema with table relationships
    pub column_schema: ViewColumnSchema,
    // State machine for population
    populate_state: PopulateState,
    // Computation tracker for statistics
    // We will use this one day to export rows_read, but for now, will just test that we're doing the expected amount of compute
    #[cfg_attr(not(test), allow(dead_code))]
    pub tracker: Arc<Mutex<ComputationTracker>>,
    // Root page of the btree storing the materialized state (0 for unmaterialized)
    root_page: i64,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for IncrementalView {}
unsafe impl Sync for IncrementalView {}
crate::assert::assert_send_sync!(IncrementalView);

impl IncrementalView {
    pub(crate) fn from_template(
        name: String,
        template: &IncrementalViewTemplate,
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Result<Self> {
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));
        let circuit = template.compile(
            main_data_root,
            internal_state_root,
            internal_state_index_root,
        )?;
        Ok(Self {
            name,
            circuit,
            referenced_tables: template.referenced_tables.clone(),
            column_schema: template.column_schema.clone(),
            populate_state: PopulateState::Start,
            tracker,
            root_page: main_data_root,
        })
    }

    /// Get an iterator over column names, using enumerated naming for unnamed columns
    pub fn column_names(&self) -> impl Iterator<Item = String> + '_ {
        self.column_schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, vc)| {
                vc.column
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("column{}", i + 1))
            })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Execute the circuit with uncommitted changes to get processed delta
    pub fn execute_with_uncommitted(
        &mut self,
        uncommitted: DeltaSet,
        pager: Arc<Pager>,
        execute_state: &mut crate::incremental::compiler::ExecuteState,
    ) -> crate::Result<crate::types::IOResult<Delta>> {
        // Initialize execute_state with the input data
        *execute_state = crate::incremental::compiler::ExecuteState::Init {
            input_data: uncommitted,
        };
        self.circuit.execute(pager, execute_state)
    }

    /// Get the root page for this materialized view's btree
    pub fn get_root_page(&self) -> i64 {
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

    /// Generate one full scan per resolved base table. Filtering and projection
    /// belong to the circuit; population must not re-read parser expressions or
    /// reproduce name resolution.
    fn sql_for_populate(&self) -> crate::Result<Vec<String>> {
        if self.referenced_tables.is_empty() {
            return Err(LimboError::ParseError(
                "No tables to populate from".to_string(),
            ));
        }
        Ok(self
            .referenced_tables
            .iter()
            .map(|table| {
                let select_clause = if table.columns().iter().any(|column| column.is_rowid_alias())
                {
                    "*"
                } else {
                    "*, rowid"
                };
                let query = format!(
                    "SELECT {select_clause} FROM main.{}",
                    quote_identifier(&table.name)
                );
                tracing::debug!("populating materialized view with `{query}`");
                query
            })
            .collect())
    }

    /// Populate the view by scanning the source table using a state machine
    /// This can be called multiple times and will resume from where it left off
    /// This method is only for materialized views and will persist data to the btree
    pub fn populate_from_table(
        &mut self,
        conn: &crate::sync::Arc<crate::Connection>,
        pager: &crate::sync::Arc<crate::Pager>,
        _btree_cursor: &mut dyn CursorTrait,
    ) -> crate::Result<IOResult<()>> {
        // Assert that this is a materialized view with a root page
        assert!(
            self.root_page != 0,
            "populate_from_table should only be called for materialized views with root_page"
        );

        // Mark as nested for the duration of this call to prevent inner queries from
        // committing the outer transaction's dirty pages. We increment on every entry
        // and decrement on every exit (including IO yields and errors) so re-entrant
        // calls keep the counter balanced.
        conn.start_nested();
        let result = self.populate_from_table_inner(conn, pager, _btree_cursor);
        conn.end_nested();
        result
    }

    fn populate_from_table_inner(
        &mut self,
        conn: &crate::sync::Arc<crate::Connection>,
        pager: &crate::sync::Arc<crate::Pager>,
        _btree_cursor: &mut dyn CursorTrait,
    ) -> crate::Result<IOResult<()>> {
        'outer: loop {
            match std::mem::replace(&mut self.populate_state, PopulateState::Done) {
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
                    let queries = self.sql_for_populate()?;

                    self.populate_state = PopulateState::ProcessingAllTables {
                        queries,
                        current_idx: 0,
                    };
                }

                PopulateState::ProcessingAllTables {
                    queries,
                    current_idx,
                } => {
                    if current_idx >= queries.len() {
                        self.populate_state = PopulateState::Done;
                        return Ok(IOResult::Done(()));
                    }

                    let query = queries[current_idx].clone();
                    // Use the parent connection directly for reading.
                    // We need to use the same connection that has the uncommitted schema changes.
                    // Creating a new connection would cause schema version mismatch issues because
                    // the new connection's schema cookie check would fail (database file has old version).

                    // Prepare the statement using the parent connection
                    let stmt = conn.prepare(&query)?;

                    self.populate_state = PopulateState::ProcessingOneTable {
                        queries,
                        current_idx,
                        stmt: Box::new(stmt),
                        rows_processed: 0,
                        pending_row: None,
                    };
                }

                PopulateState::ProcessingOneTable {
                    queries,
                    current_idx,
                    mut stmt,
                    mut rows_processed,
                    pending_row,
                } => {
                    // If we have a pending row from a previous I/O interruption, process it first
                    if let Some((rowid, values)) = pending_row {
                        match self.process_one_row(
                            rowid,
                            values.clone(),
                            current_idx,
                            pager.clone(),
                        )? {
                            IOResult::Done(_) => {
                                // Row processed successfully, continue to next row
                                rows_processed += 1;
                            }
                            IOResult::IO(io) => {
                                // Still not done, restore state with pending row and return
                                self.populate_state = PopulateState::ProcessingOneTable {
                                    queries,
                                    current_idx,
                                    stmt,
                                    rows_processed,
                                    pending_row: Some((rowid, values)),
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
                                let row = stmt.row().ok_or_else(|| {
                                    LimboError::InternalError(
                                        "row should exist after StepResult::Row".to_string(),
                                    )
                                })?;

                                // Extract values from the row
                                let all_values: Vec<crate::types::Value> =
                                    row.get_values().cloned().collect();

                                // Extract rowid and values using helper
                                let (rowid, values) =
                                    match self.extract_rowid_and_values(all_values, current_idx) {
                                        Some(result) => result,
                                        None => {
                                            // Invalid rowid, skip this row
                                            rows_processed += 1;
                                            continue;
                                        }
                                    };

                                // Process this row
                                match self.process_one_row(
                                    rowid,
                                    values.clone(),
                                    current_idx,
                                    pager.clone(),
                                )? {
                                    IOResult::Done(_) => {
                                        // Row processed successfully, continue to next row
                                        rows_processed += 1;
                                    }
                                    IOResult::IO(io) => {
                                        // Save state and return I/O
                                        // We'll resume at the SAME row when called again (don't increment rows_processed)
                                        // The circuit still has unfinished work for this row
                                        self.populate_state = PopulateState::ProcessingOneTable {
                                            queries,
                                            current_idx,
                                            stmt,
                                            rows_processed, // Don't increment - row not done yet!
                                            pending_row: Some((rowid, values)), // Save the row for resumption
                                        };
                                        return Ok(IOResult::IO(io));
                                    }
                                }
                            }

                            crate::vdbe::StepResult::Done => {
                                // All rows processed from this table
                                // Move to next table
                                self.populate_state = PopulateState::ProcessingAllTables {
                                    queries,
                                    current_idx: current_idx + 1,
                                };
                                continue 'outer;
                            }

                            crate::vdbe::StepResult::Interrupt | crate::vdbe::StepResult::Busy => {
                                // Save state before returning error
                                self.populate_state = PopulateState::ProcessingOneTable {
                                    queries,
                                    current_idx,
                                    stmt,
                                    rows_processed,
                                    pending_row: None, // No pending row when interrupted between rows
                                };
                                return Err(LimboError::Busy);
                            }

                            crate::vdbe::StepResult::IO | crate::vdbe::StepResult::Yield => {
                                // Statement needs I/O - save state and return
                                self.populate_state = PopulateState::ProcessingOneTable {
                                    queries,
                                    current_idx,
                                    stmt,
                                    rows_processed,
                                    pending_row: None, // No pending row when interrupted between rows
                                };
                                // TODO: Get the actual I/O completion from the statement
                                let completion = crate::io::Completion::new_yield();
                                return Ok(IOResult::IO(crate::types::IOCompletions(completion)));
                            }
                        }
                    }
                }

                PopulateState::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Process a single row through the circuit
    fn process_one_row(
        &mut self,
        rowid: i64,
        values: Vec<Value>,
        table_idx: usize,
        pager: Arc<crate::Pager>,
    ) -> crate::Result<IOResult<()>> {
        // Create a single-row delta
        let mut single_row_delta = Delta::new();
        single_row_delta.insert(rowid, values);

        // Create a DeltaSet with this delta for the current table
        let mut delta_set = DeltaSet::new();
        let table_name = self.referenced_tables[table_idx].name.clone();
        delta_set.insert(table_name, single_row_delta);

        // Process through merge_delta
        self.merge_delta(delta_set, pager)
    }

    /// Extract rowid and values from a row
    fn extract_rowid_and_values(
        &self,
        all_values: Vec<Value>,
        table_idx: usize,
    ) -> Option<(i64, Vec<Value>)> {
        if let Some((idx, _)) = self.referenced_tables[table_idx].get_rowid_alias_column() {
            // The rowid is the value at the rowid alias column index
            let rowid = match all_values.get(idx) {
                Some(Value::Numeric(Numeric::Integer(id))) => *id,
                _ => return None, // Invalid rowid
            };
            // All values are table columns (no separate rowid was selected)
            Some((rowid, all_values))
        } else {
            // The last value is the explicitly selected rowid
            let rowid = match all_values.last() {
                Some(Value::Numeric(Numeric::Integer(id))) => *id,
                _ => return None, // Invalid rowid
            };
            // Get all values except the rowid
            let values = all_values[..all_values.len() - 1].to_vec();
            Some((rowid, values))
        }
    }

    /// Merge a delta set of changes into the view's current state
    pub fn merge_delta(
        &mut self,
        delta_set: DeltaSet,
        pager: Arc<crate::Pager>,
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
