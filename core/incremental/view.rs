use super::compiler::{DbspCircuit, DbspCompiler, DeltaSet};
use super::dbsp::Delta;
use super::operator::ComputationTracker;
use crate::numeric::Numeric;
use crate::schema::{BTreeTable, Schema};
use crate::storage::btree::CursorTrait;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::translate::logical::LogicalPlanBuilder;
use crate::types::IOResultOr;
use crate::types::{IOResult, Value};
use crate::util::{extract_view_columns, ViewColumnSchema};
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
    // The SELECT statement that defines how to transform input data
    pub select_stmt: ast::Select,

    // DBSP circuit that encapsulates the computation
    circuit: DbspCircuit,

    // All tables referenced by this view (from FROM clause and JOINs)
    referenced_tables: Vec<Arc<BTreeTable>>,
    // Mapping from table aliases to actual table names (e.g., "c" -> "customers")
    table_aliases: HashMap<String, String>,
    // Mapping from table name to fully qualified name (e.g., "customers" -> "main.customers")
    // This preserves database qualification from the original query
    qualified_table_names: HashMap<String, String>,
    // WHERE conditions for each table (accumulated from all occurrences)
    // Multiple conditions from UNION branches or duplicate references are stored as a vector
    table_conditions: HashMap<String, Vec<Option<ast::Expr>>>,
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
    /// Try to compile the SELECT statement into a DBSP circuit
    fn try_compile_circuit(
        select: &ast::Select,
        schema: &Schema,
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Result<DbspCircuit> {
        // Build the logical plan from the SELECT statement
        let mut builder = LogicalPlanBuilder::new(schema);
        // Convert Select to a Stmt for the builder
        let stmt = ast::Stmt::Select(select.clone());
        let logical_plan = builder.build_statement(&stmt)?;

        // Compile the logical plan to a DBSP circuit with the storage roots
        let compiler = DbspCompiler::new(
            main_data_root,
            internal_state_root,
            internal_state_index_root,
        );
        let circuit = compiler.compile(&logical_plan)?;

        Ok(circuit)
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
    ) -> Result<ViewColumnSchema> {
        crate::util::validate_select_for_unsupported_features(select)?;
        // Use the shared function to extract columns with full table context
        extract_view_columns(select, schema)
    }

    pub fn from_sql(
        sql: &str,
        schema: &Schema,
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
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
                internal_state_index_root,
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
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Result<Self> {
        let name = view_name.name.as_str().to_string();

        // Extract output columns using the shared function
        let column_schema = extract_view_columns(&select, schema)?;

        let mut referenced_tables = Vec::new();
        let mut table_aliases = HashMap::default();
        let mut qualified_table_names = HashMap::default();
        let mut table_conditions = HashMap::default();
        Self::extract_all_tables(
            &select,
            schema,
            &mut referenced_tables,
            &mut table_aliases,
            &mut qualified_table_names,
            &mut table_conditions,
        )?;

        Self::new(
            name,
            select.clone(),
            referenced_tables,
            table_aliases,
            qualified_table_names,
            table_conditions,
            column_schema,
            schema,
            main_data_root,
            internal_state_root,
            internal_state_index_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        select_stmt: ast::Select,
        referenced_tables: Vec<Arc<BTreeTable>>,
        table_aliases: HashMap<String, String>,
        qualified_table_names: HashMap<String, String>,
        table_conditions: HashMap<String, Vec<Option<ast::Expr>>>,
        column_schema: ViewColumnSchema,
        schema: &Schema,
        main_data_root: i64,
        internal_state_root: i64,
        internal_state_index_root: i64,
    ) -> Result<Self> {
        // Create the tracker that will be shared by all operators
        let tracker = Arc::new(Mutex::new(ComputationTracker::new()));

        // Compile the SELECT statement into a DBSP circuit
        let circuit = Self::try_compile_circuit(
            &select_stmt,
            schema,
            main_data_root,
            internal_state_root,
            internal_state_index_root,
        )?;

        Ok(Self {
            name,
            select_stmt,
            circuit,
            referenced_tables,
            table_aliases,
            qualified_table_names,
            table_conditions,
            column_schema,
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
        pager: Arc<Pager>,
        execute_state: &mut crate::incremental::compiler::ExecuteState,
    ) -> crate::types::IOResultOr<Delta> {
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

    /// Process a single table reference from a FROM or JOIN clause
    fn process_table_reference(
        name: &ast::QualifiedName,
        alias: &Option<ast::As>,
        schema: &Schema,
        table_map: &mut HashMap<String, Arc<BTreeTable>>,
        aliases: &mut HashMap<String, String>,
        qualified_names: &mut HashMap<String, String>,
        cte_names: &HashSet<String>,
    ) -> Result<()> {
        let table_name = name.name.as_str();

        // Build the fully qualified name
        let qualified_name = if let Some(ref db) = name.db_name {
            format!("{db}.{table_name}")
        } else {
            table_name.to_string()
        };

        // Skip CTEs - they're not real tables
        if !cte_names.contains(table_name) {
            if let Some(table) = schema.get_btree_table(table_name) {
                table_map.insert(table_name.to_string(), table);
                qualified_names.insert(table_name.to_string(), qualified_name);

                // Store the alias mapping if there is an alias
                if let Some(alias_enum) = alias {
                    aliases.insert(
                        alias_enum.name().as_str().to_string(),
                        table_name.to_string(),
                    );
                }
            } else {
                return Err(LimboError::ParseError(format!(
                    "Table '{table_name}' not found in schema"
                )));
            }
        }
        Ok(())
    }

    fn extract_one_statement(
        select: &ast::OneSelect,
        schema: &Schema,
        table_map: &mut HashMap<String, Arc<BTreeTable>>,
        aliases: &mut HashMap<String, String>,
        qualified_names: &mut HashMap<String, String>,
        table_conditions: &mut HashMap<String, Vec<Option<ast::Expr>>>,
        cte_names: &HashSet<String>,
    ) -> Result<()> {
        if let ast::OneSelect::Select {
            from: Some(ref from),
            ..
        } = select
        {
            // Get the main table from FROM clause
            if let ast::SelectTable::Table(name, alias, _) = from.select.as_ref() {
                Self::process_table_reference(
                    name,
                    alias,
                    schema,
                    table_map,
                    aliases,
                    qualified_names,
                    cte_names,
                )?;
            }

            // Get all tables from JOIN clauses
            for join in &from.joins {
                if let ast::SelectTable::Table(name, alias, _) = join.table.as_ref() {
                    Self::process_table_reference(
                        name,
                        alias,
                        schema,
                        table_map,
                        aliases,
                        qualified_names,
                        cte_names,
                    )?;
                }
            }
        }
        // Extract WHERE conditions for this SELECT
        let where_expr = if let ast::OneSelect::Select {
            where_clause: Some(ref where_expr),
            ..
        } = select
        {
            Some(where_expr.as_ref().clone())
        } else {
            None
        };

        // Ensure all tables have an entry in table_conditions (even if empty)
        for table_name in table_map.keys() {
            table_conditions.entry(table_name.clone()).or_default();
        }

        // Extract and store table-specific conditions from the WHERE clause
        if let Some(ref where_expr) = where_expr {
            for table_name in table_map.keys() {
                let all_tables: Vec<String> = table_map.keys().cloned().collect();
                let table_specific_condition = Self::extract_conditions_for_table(
                    where_expr,
                    table_name,
                    aliases,
                    &all_tables,
                    schema,
                );
                // Only add if there's actually a condition for this table
                if let Some(condition) = table_specific_condition {
                    let conditions = table_conditions.get_mut(table_name).ok_or_else(|| {
                        LimboError::InternalError(
                            "table_conditions should have entry for table_name".to_string(),
                        )
                    })?;
                    conditions.push(Some(condition));
                }
            }
        } else {
            // No WHERE clause - push None for all tables in this SELECT. It is a way
            // of signaling that we need all rows in the table. It is important we signal this
            // explicitly, because the same table may appear in many conditions - some of which
            // have filters that would otherwise be applied.
            for table_name in table_map.keys() {
                let conditions = table_conditions.get_mut(table_name).ok_or_else(|| {
                    LimboError::InternalError(
                        "table_conditions should have entry for table_name".to_string(),
                    )
                })?;
                conditions.push(None);
            }
        }

        Ok(())
    }

    /// Extract all tables and their aliases from the SELECT statement, handling CTEs
    /// Deduplicates tables and accumulates WHERE conditions
    fn extract_all_tables(
        select: &ast::Select,
        schema: &Schema,
        tables: &mut Vec<Arc<BTreeTable>>,
        aliases: &mut HashMap<String, String>,
        qualified_names: &mut HashMap<String, String>,
        table_conditions: &mut HashMap<String, Vec<Option<ast::Expr>>>,
    ) -> Result<()> {
        let mut table_map = HashMap::default();
        Self::extract_all_tables_inner(
            select,
            schema,
            &mut table_map,
            aliases,
            qualified_names,
            table_conditions,
            &HashSet::default(),
        )?;

        // Convert deduplicated table map to vector
        for (_name, table) in table_map {
            tables.push(table);
        }

        Ok(())
    }

    fn extract_all_tables_inner(
        select: &ast::Select,
        schema: &Schema,
        table_map: &mut HashMap<String, Arc<BTreeTable>>,
        aliases: &mut HashMap<String, String>,
        qualified_names: &mut HashMap<String, String>,
        table_conditions: &mut HashMap<String, Vec<Option<ast::Expr>>>,
        parent_cte_names: &HashSet<String>,
    ) -> Result<()> {
        let mut cte_names = parent_cte_names.clone();

        // First, collect CTE names and process any CTEs (WITH clauses)
        if let Some(ref with) = select.with {
            // First pass: collect all CTE names (needed for recursive CTEs)
            for cte in &with.ctes {
                cte_names.insert(cte.tbl_name.as_str().to_string());
            }

            // Second pass: extract tables from each CTE's SELECT statement
            for cte in &with.ctes {
                // Recursively extract tables from each CTE's SELECT statement
                Self::extract_all_tables_inner(
                    &cte.select,
                    schema,
                    table_map,
                    aliases,
                    qualified_names,
                    table_conditions,
                    &cte_names,
                )?;
            }
        }

        // Then process the main SELECT body
        Self::extract_one_statement(
            &select.body.select,
            schema,
            table_map,
            aliases,
            qualified_names,
            table_conditions,
            &cte_names,
        )?;

        // Process any compound selects (UNION, etc.)
        for c in &select.body.compounds {
            let ast::CompoundSelect { select, .. } = c;
            Self::extract_one_statement(
                select,
                schema,
                table_map,
                aliases,
                qualified_names,
                table_conditions,
                &cte_names,
            )?;
        }

        Ok(())
    }

    /// Generate SQL queries for populating the view from each source table
    /// Returns a vector of SQL statements, one for each referenced table
    /// Each query includes the WHERE conditions accumulated from all occurrences
    fn sql_for_populate(&self) -> crate::Result<Vec<String>> {
        Self::generate_populate_queries(
            &self.select_stmt,
            &self.referenced_tables,
            &self.table_aliases,
            &self.qualified_table_names,
            &self.table_conditions,
        )
    }

    pub fn generate_populate_queries(
        select_stmt: &ast::Select,
        referenced_tables: &[Arc<BTreeTable>],
        table_aliases: &HashMap<String, String>,
        qualified_table_names: &HashMap<String, String>,
        table_conditions: &HashMap<String, Vec<Option<ast::Expr>>>,
    ) -> crate::Result<Vec<String>> {
        if referenced_tables.is_empty() {
            return Err(LimboError::ParseError(
                "No tables to populate from".to_string(),
            ));
        }

        let mut queries = Vec::new();

        for table in referenced_tables {
            // Check if the table has a rowid alias (INTEGER PRIMARY KEY column)
            let has_rowid_alias = table.columns().iter().any(|col| col.is_rowid_alias());

            // Select all columns. The circuit will handle filtering and projection
            // If there's a rowid alias, we don't need to select rowid separately
            let select_clause = if has_rowid_alias {
                "*".to_string()
            } else {
                "*, rowid".to_string()
            };

            // Get accumulated WHERE conditions for this table
            let where_clause = if let Some(conditions) = table_conditions.get(&table.name) {
                // Combine multiple conditions with OR if there are multiple occurrences
                Self::combine_conditions(
                    select_stmt,
                    conditions,
                    &table.name,
                    referenced_tables,
                    table_aliases,
                )?
            } else {
                String::new()
            };

            // Use the qualified table name if available, otherwise just the table name
            let table_name = qualified_table_names
                .get(&table.name)
                .cloned()
                .unwrap_or_else(|| table.name.clone());

            // Construct the query for this table
            let query = if where_clause.is_empty() {
                format!("SELECT {select_clause} FROM {table_name}")
            } else {
                format!("SELECT {select_clause} FROM {table_name} WHERE {where_clause}")
            };
            tracing::debug!("populating materialized view with `{query}`");
            queries.push(query);
        }

        Ok(queries)
    }

    fn combine_conditions(
        _select_stmt: &ast::Select,
        conditions: &[Option<ast::Expr>],
        table_name: &str,
        _referenced_tables: &[Arc<BTreeTable>],
        table_aliases: &HashMap<String, String>,
    ) -> crate::Result<String> {
        // Check if any conditions are None (SELECTs without WHERE)
        let has_none = conditions.iter().any(|c| c.is_none());
        let non_empty: Vec<_> = conditions.iter().filter_map(|c| c.as_ref()).collect();

        // If we have both Some and None conditions, that means in some of the expressions where
        // this table appear we want all rows. So we need to fetch all rows.
        if has_none && !non_empty.is_empty() {
            return Ok(String::new());
        }

        if non_empty.is_empty() {
            return Ok(String::new());
        }

        if non_empty.len() == 1 {
            // Unqualify the expression before converting to string
            let unqualified = Self::unqualify_expression(non_empty[0], table_name, table_aliases);
            return Ok(unqualified.to_string());
        }

        // Multiple conditions - combine with OR
        // This happens in UNION ALL when the same table appears multiple times
        let mut combined_parts = Vec::new();
        for condition in non_empty {
            let unqualified = Self::unqualify_expression(condition, table_name, table_aliases);
            // Wrap each condition in parentheses to preserve precedence
            combined_parts.push(format!("({unqualified})"));
        }

        // Join all conditions with OR
        Ok(combined_parts.join(" OR "))
    }
    /// Resolve a table alias to the actual table name
    /// Check if an expression is a simple comparison that can be safely extracted
    /// This excludes subqueries, CASE expressions, function calls, etc.
    fn is_simple_comparison(expr: &ast::Expr) -> bool {
        match expr {
            // Simple column references and literals are OK
            ast::Expr::Column { .. } | ast::Expr::Literal(_) => true,

            // Simple binary operations between simple expressions are OK
            ast::Expr::Binary(left, op, right) => {
                match op {
                    // Logical operators
                    ast::Operator::And | ast::Operator::Or => {
                        Self::is_simple_comparison(left) && Self::is_simple_comparison(right)
                    }
                    // Comparison operators
                    ast::Operator::Equals
                    | ast::Operator::NotEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Is
                    | ast::Operator::IsNot => {
                        Self::is_simple_comparison(left) && Self::is_simple_comparison(right)
                    }
                    // String concatenation and other operations are NOT simple
                    ast::Operator::Concat => false,
                    // Arithmetic might be OK if operands are simple
                    ast::Operator::Add
                    | ast::Operator::Subtract
                    | ast::Operator::Multiply
                    | ast::Operator::Divide
                    | ast::Operator::Modulus => {
                        Self::is_simple_comparison(left) && Self::is_simple_comparison(right)
                    }
                    _ => false,
                }
            }

            // Unary operations might be OK
            ast::Expr::Unary(
                ast::UnaryOperator::Not
                | ast::UnaryOperator::Negative
                | ast::UnaryOperator::Positive,
                inner,
            ) => Self::is_simple_comparison(inner),
            ast::Expr::Unary(_, _) => false,

            // Complex expressions are NOT simple
            ast::Expr::Case { .. } => false,
            ast::Expr::Cast { .. } => false,
            ast::Expr::Collate { .. } => false,
            ast::Expr::Exists(_) => false,
            ast::Expr::FunctionCall { .. } => false,
            ast::Expr::InList { .. } => false,
            ast::Expr::InSelect { .. } => false,
            ast::Expr::Like { .. } => false,
            ast::Expr::NotNull(_) => true, // IS NOT NULL is simple enough
            ast::Expr::Parenthesized(exprs) => {
                // Parenthesized expression can contain multiple expressions
                // Only consider it simple if it has exactly one simple expression
                exprs.len() == 1 && Self::is_simple_comparison(&exprs[0])
            }
            ast::Expr::Subquery(_) => false,

            // BETWEEN might be OK if all operands are simple
            ast::Expr::Between { .. } => {
                // BETWEEN has a different structure, for safety just exclude it
                false
            }

            // Qualified references are simple
            ast::Expr::DoublyQualified(..) => true,
            ast::Expr::Qualified(_, _) => true,

            // These are simple
            ast::Expr::Id(_) => true,
            ast::Expr::Name(_) => true,

            // Anything else is not simple
            _ => false,
        }
    }

    /// Extract conditions from a WHERE clause that apply to a specific table
    fn extract_conditions_for_table(
        expr: &ast::Expr,
        table_name: &str,
        aliases: &HashMap<String, String>,
        all_tables: &[String],
        schema: &Schema,
    ) -> Option<ast::Expr> {
        match expr {
            ast::Expr::Binary(left, op, right) => {
                match op {
                    ast::Operator::And => {
                        // For AND, we can extract conditions independently
                        let left_cond = Self::extract_conditions_for_table(
                            left, table_name, aliases, all_tables, schema,
                        );
                        let right_cond = Self::extract_conditions_for_table(
                            right, table_name, aliases, all_tables, schema,
                        );

                        match (left_cond, right_cond) {
                            (Some(l), Some(r)) => Some(ast::Expr::Binary(
                                Box::new(l),
                                ast::Operator::And,
                                Box::new(r),
                            )),
                            (Some(l), None) => Some(l),
                            (None, Some(r)) => Some(r),
                            (None, None) => None,
                        }
                    }
                    ast::Operator::Or => {
                        // For OR, both sides must reference only our table
                        let left_tables =
                            Self::get_tables_in_expr(left, aliases, all_tables, schema);
                        let right_tables =
                            Self::get_tables_in_expr(right, aliases, all_tables, schema);

                        if left_tables.len() == 1
                            && left_tables.contains(&table_name.to_string())
                            && right_tables.len() == 1
                            && right_tables.contains(&table_name.to_string())
                            && Self::is_simple_comparison(expr)
                        {
                            Some(expr.clone())
                        } else {
                            None
                        }
                    }
                    _ => {
                        // For comparison operators, check if this condition only references our table
                        let referenced_tables =
                            Self::get_tables_in_expr(expr, aliases, all_tables, schema);
                        if referenced_tables.len() == 1
                            && referenced_tables.contains(&table_name.to_string())
                            && Self::is_simple_comparison(expr)
                        {
                            Some(expr.clone())
                        } else {
                            None
                        }
                    }
                }
            }
            _ => {
                // For other expressions, check if they only reference our table
                let referenced_tables = Self::get_tables_in_expr(expr, aliases, all_tables, schema);
                if referenced_tables.len() == 1
                    && referenced_tables.contains(&table_name.to_string())
                    && Self::is_simple_comparison(expr)
                {
                    Some(expr.clone())
                } else {
                    None
                }
            }
        }
    }

    /// Unqualify column references in an expression
    /// Removes table/alias prefixes from qualified column names
    fn unqualify_expression(
        expr: &ast::Expr,
        table_name: &str,
        aliases: &HashMap<String, String>,
    ) -> ast::Expr {
        match expr {
            ast::Expr::Binary(left, op, right) => ast::Expr::Binary(
                Box::new(Self::unqualify_expression(left, table_name, aliases)),
                *op,
                Box::new(Self::unqualify_expression(right, table_name, aliases)),
            ),
            ast::Expr::Qualified(table_or_alias, column) => {
                // Check if this qualification refers to our table
                let table_str = table_or_alias.as_str();
                let actual_table = if let Some(actual) = aliases.get(table_str) {
                    actual.clone()
                } else if table_str.contains('.') {
                    // Handle database.table format
                    table_str
                        .split('.')
                        .next_back()
                        .unwrap_or(table_str)
                        .to_string()
                } else {
                    table_str.to_string()
                };

                if actual_table == table_name {
                    // Remove the qualification
                    ast::Expr::Id(column.clone())
                } else {
                    // Keep the qualification (shouldn't happen if extraction worked correctly)
                    expr.clone()
                }
            }
            ast::Expr::DoublyQualified(_database, table, column) => {
                // Check if this refers to our table
                if table.as_str() == table_name {
                    // Remove the qualification, keep just the column
                    ast::Expr::Id(column.clone())
                } else {
                    // Keep the qualification (shouldn't happen if extraction worked correctly)
                    expr.clone()
                }
            }
            ast::Expr::Unary(op, inner) => ast::Expr::Unary(
                *op,
                Box::new(Self::unqualify_expression(inner, table_name, aliases)),
            ),
            ast::Expr::FunctionCall {
                name,
                args,
                distinctness,
                filter_over,
                order_by,
                within_group,
            } => ast::Expr::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| Box::new(Self::unqualify_expression(arg, table_name, aliases)))
                    .collect(),
                distinctness: *distinctness,
                filter_over: filter_over.clone(),
                order_by: order_by.clone(),
                within_group: within_group.clone(),
            },
            ast::Expr::InList { lhs, not, rhs } => ast::Expr::InList {
                lhs: Box::new(Self::unqualify_expression(lhs, table_name, aliases)),
                not: *not,
                rhs: rhs
                    .iter()
                    .map(|item| Box::new(Self::unqualify_expression(item, table_name, aliases)))
                    .collect(),
            },
            ast::Expr::Between {
                lhs,
                not,
                start,
                end,
            } => ast::Expr::Between {
                lhs: Box::new(Self::unqualify_expression(lhs, table_name, aliases)),
                not: *not,
                start: Box::new(Self::unqualify_expression(start, table_name, aliases)),
                end: Box::new(Self::unqualify_expression(end, table_name, aliases)),
            },
            _ => expr.clone(),
        }
    }

    /// Get all tables referenced in an expression
    fn get_tables_in_expr(
        expr: &ast::Expr,
        aliases: &HashMap<String, String>,
        all_tables: &[String],
        schema: &Schema,
    ) -> Vec<String> {
        let mut tables = Vec::new();
        Self::collect_tables_in_expr(expr, aliases, all_tables, schema, &mut tables);
        tables.sort();
        tables.dedup();
        tables
    }

    /// Recursively collect table references from an expression
    fn collect_tables_in_expr(
        expr: &ast::Expr,
        aliases: &HashMap<String, String>,
        all_tables: &[String],
        schema: &Schema,
        tables: &mut Vec<String>,
    ) {
        match expr {
            ast::Expr::Binary(left, _, right) => {
                Self::collect_tables_in_expr(left, aliases, all_tables, schema, tables);
                Self::collect_tables_in_expr(right, aliases, all_tables, schema, tables);
            }
            ast::Expr::Qualified(table_or_alias, _) => {
                // Handle database.table or just table/alias
                let table_str = table_or_alias.as_str();
                let table_name = if let Some(actual_table) = aliases.get(table_str) {
                    // It's an alias
                    actual_table.clone()
                } else if table_str.contains('.') {
                    // It might be database.table format, extract just the table name
                    table_str
                        .split('.')
                        .next_back()
                        .unwrap_or(table_str)
                        .to_string()
                } else {
                    // It's a direct table name
                    table_str.to_string()
                };
                tables.push(table_name);
            }
            ast::Expr::DoublyQualified(_database, table, _column) => {
                // For database.table.column, extract the table name
                tables.push(table.to_string());
            }
            ast::Expr::Id(column) => {
                // Unqualified column - try to find which table has this column
                if all_tables.len() == 1 {
                    tables.push(all_tables[0].clone());
                } else {
                    // Check which table has this column
                    for table_name in all_tables {
                        if let Some(table) = schema.get_btree_table(table_name) {
                            if table
                                .columns()
                                .iter()
                                .any(|col| col.name.as_deref() == Some(column.as_str()))
                            {
                                tables.push(table_name.clone());
                                break; // Found the table, stop looking
                            }
                        }
                    }
                }
            }
            ast::Expr::FunctionCall { args, .. } => {
                for arg in args {
                    Self::collect_tables_in_expr(arg, aliases, all_tables, schema, tables);
                }
            }
            ast::Expr::InList { lhs, rhs, .. } => {
                Self::collect_tables_in_expr(lhs, aliases, all_tables, schema, tables);
                for item in rhs {
                    Self::collect_tables_in_expr(item, aliases, all_tables, schema, tables);
                }
            }
            ast::Expr::InSelect { lhs, .. } => {
                Self::collect_tables_in_expr(lhs, aliases, all_tables, schema, tables);
            }
            ast::Expr::Between {
                lhs, start, end, ..
            } => {
                Self::collect_tables_in_expr(lhs, aliases, all_tables, schema, tables);
                Self::collect_tables_in_expr(start, aliases, all_tables, schema, tables);
                Self::collect_tables_in_expr(end, aliases, all_tables, schema, tables);
            }
            ast::Expr::Unary(_, expr) => {
                Self::collect_tables_in_expr(expr, aliases, all_tables, schema, tables);
            }
            _ => {
                // Literals, etc. don't reference tables
            }
        }
    }
    /// Populate the view by scanning the source table using a state machine
    /// This can be called multiple times and will resume from where it left off
    /// This method is only for materialized views and will persist data to the btree
    pub fn populate_from_table(
        &mut self,
        conn: &crate::sync::Arc<crate::Connection>,
        pager: &crate::sync::Arc<crate::Pager>,
        _btree_cursor: &mut dyn CursorTrait,
    ) -> IOResultOr<()> {
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
    ) -> IOResultOr<()> {
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
                                return Err(LimboError::Busy.into());
                            }

                            crate::vdbe::StepResult::IO
                            | crate::vdbe::StepResult::Yield
                            | crate::vdbe::StepResult::Sleep { .. } => {
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
    ) -> IOResultOr<()> {
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
    pub fn merge_delta(&mut self, delta_set: DeltaSet, pager: Arc<crate::Pager>) -> IOResultOr<()> {
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
#[path = "../tests/unit/incremental/view/tests.rs"]
mod tests;
