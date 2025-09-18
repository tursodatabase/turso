use super::compiler::{DbspCircuit, DbspCompiler, DeltaSet};
use super::dbsp::Delta;
use super::operator::ComputationTracker;
use crate::schema::{BTreeTable, Schema};
use crate::storage::btree::BTreeCursor;
use crate::translate::logical::LogicalPlanBuilder;
use crate::types::{IOResult, Value};
use crate::util::{extract_view_columns, ViewColumnSchema};
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
    states: Rc<RefCell<HashMap<String, Arc<ViewTransactionState>>>>,
}

impl AllViewsTxState {
    /// Create a new container for view transaction states
    pub fn new() -> Self {
        Self {
            states: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Get or create a transaction state for a view
    pub fn get_or_create(&self, view_name: &str) -> Arc<ViewTransactionState> {
        let mut states = self.states.borrow_mut();
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
    // The view's column schema with table relationships
    pub column_schema: ViewColumnSchema,
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
    /// Try to compile the SELECT statement into a DBSP circuit
    fn try_compile_circuit(
        select: &ast::Select,
        schema: &Schema,
        main_data_root: usize,
        internal_state_root: usize,
        internal_state_index_root: usize,
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
        // Use the shared function to extract columns with full table context
        extract_view_columns(select, schema)
    }

    pub fn from_sql(
        sql: &str,
        schema: &Schema,
        main_data_root: usize,
        internal_state_root: usize,
        internal_state_index_root: usize,
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
        main_data_root: usize,
        internal_state_root: usize,
        internal_state_index_root: usize,
    ) -> Result<Self> {
        let name = view_name.name.as_str().to_string();

        // Extract output columns using the shared function
        let column_schema = extract_view_columns(&select, schema)?;

        // Get all tables from FROM clause and JOINs, along with their aliases
        let (referenced_tables, table_aliases, qualified_table_names) =
            Self::extract_all_tables(&select, schema)?;

        Self::new(
            name,
            select.clone(),
            referenced_tables,
            table_aliases,
            qualified_table_names,
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
        column_schema: ViewColumnSchema,
        schema: &Schema,
        main_data_root: usize,
        internal_state_root: usize,
        internal_state_index_root: usize,
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

    /// Extract all tables and their aliases from the SELECT statement
    /// Returns a tuple of (tables, alias_map, qualified_names)
    /// where alias_map is alias -> table_name
    /// and qualified_names is table_name -> fully_qualified_name
    #[allow(clippy::type_complexity)]
    fn extract_all_tables(
        select: &ast::Select,
        schema: &Schema,
    ) -> Result<(
        Vec<Arc<BTreeTable>>,
        HashMap<String, String>,
        HashMap<String, String>,
    )> {
        let mut tables = Vec::new();
        let mut aliases = HashMap::new();
        let mut qualified_names = HashMap::new();

        if let ast::OneSelect::Select {
            from: Some(ref from),
            ..
        } = select.body.select
        {
            // Get the main table from FROM clause
            if let ast::SelectTable::Table(name, alias, _) = from.select.as_ref() {
                let table_name = name.name.as_str();

                // Build the fully qualified name
                let qualified_name = if let Some(ref db) = name.db_name {
                    format!("{db}.{table_name}")
                } else {
                    table_name.to_string()
                };

                if let Some(table) = schema.get_btree_table(table_name) {
                    tables.push(table.clone());
                    qualified_names.insert(table_name.to_string(), qualified_name);

                    // Store the alias mapping if there is an alias
                    if let Some(alias_name) = alias {
                        aliases.insert(alias_name.to_string(), table_name.to_string());
                    }
                } else {
                    return Err(LimboError::ParseError(format!(
                        "Table '{table_name}' not found in schema"
                    )));
                }
            }

            // Get all tables from JOIN clauses
            for join in &from.joins {
                if let ast::SelectTable::Table(name, alias, _) = join.table.as_ref() {
                    let table_name = name.name.as_str();

                    // Build the fully qualified name
                    let qualified_name = if let Some(ref db) = name.db_name {
                        format!("{db}.{table_name}")
                    } else {
                        table_name.to_string()
                    };

                    if let Some(table) = schema.get_btree_table(table_name) {
                        tables.push(table.clone());
                        qualified_names.insert(table_name.to_string(), qualified_name);

                        // Store the alias mapping if there is an alias
                        if let Some(alias_name) = alias {
                            aliases.insert(alias_name.to_string(), table_name.to_string());
                        }
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

        Ok((tables, aliases, qualified_names))
    }

    /// Generate SQL queries for populating the view from each source table
    /// Returns a vector of SQL statements, one for each referenced table
    /// Each query includes only the WHERE conditions relevant to that specific table
    fn sql_for_populate(&self) -> crate::Result<Vec<String>> {
        if self.referenced_tables.is_empty() {
            return Err(LimboError::ParseError(
                "No tables to populate from".to_string(),
            ));
        }

        let mut queries = Vec::new();

        for table in &self.referenced_tables {
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

            // Extract WHERE conditions for this specific table
            let where_clause = self.extract_where_clause_for_table(&table.name)?;

            // Use the qualified table name if available, otherwise just the table name
            let table_name = self
                .qualified_table_names
                .get(&table.name)
                .cloned()
                .unwrap_or_else(|| table.name.clone());

            // Construct the query for this table
            let query = if where_clause.is_empty() {
                format!("SELECT {select_clause} FROM {table_name}")
            } else {
                format!("SELECT {select_clause} FROM {table_name} WHERE {where_clause}")
            };
            queries.push(query);
        }

        Ok(queries)
    }

    /// Extract WHERE conditions that apply to a specific table
    /// This analyzes the WHERE clause in the SELECT statement and returns
    /// only the conditions that reference the given table
    fn extract_where_clause_for_table(&self, table_name: &str) -> crate::Result<String> {
        // For single table queries, return the entire WHERE clause (already unqualified)
        if self.referenced_tables.len() == 1 {
            if let ast::OneSelect::Select {
                where_clause: Some(ref where_expr),
                ..
            } = self.select_stmt.body.select
            {
                // For single table, the expression should already be unqualified or qualified with the single table
                // We need to unqualify it for the single-table query
                let unqualified = self.unqualify_expression(where_expr, table_name);
                return Ok(unqualified.to_string());
            }
            return Ok(String::new());
        }

        // For multi-table queries (JOINs), extract conditions for the specific table
        if let ast::OneSelect::Select {
            where_clause: Some(ref where_expr),
            ..
        } = self.select_stmt.body.select
        {
            // Extract conditions that reference only the specified table
            let table_conditions = self.extract_table_conditions(where_expr, table_name)?;
            if let Some(conditions) = table_conditions {
                // Unqualify the expression for single-table query
                let unqualified = self.unqualify_expression(&conditions, table_name);
                return Ok(unqualified.to_string());
            }
        }

        Ok(String::new())
    }

    /// Extract conditions from an expression that reference only the specified table
    fn extract_table_conditions(
        &self,
        expr: &ast::Expr,
        table_name: &str,
    ) -> crate::Result<Option<ast::Expr>> {
        match expr {
            ast::Expr::Binary(left, op, right) => {
                match op {
                    ast::Operator::And => {
                        // For AND, we can extract conditions independently
                        let left_cond = self.extract_table_conditions(left, table_name)?;
                        let right_cond = self.extract_table_conditions(right, table_name)?;

                        match (left_cond, right_cond) {
                            (Some(l), Some(r)) => {
                                // Both conditions apply to this table
                                Ok(Some(ast::Expr::Binary(
                                    Box::new(l),
                                    ast::Operator::And,
                                    Box::new(r),
                                )))
                            }
                            (Some(l), None) => Ok(Some(l)),
                            (None, Some(r)) => Ok(Some(r)),
                            (None, None) => Ok(None),
                        }
                    }
                    ast::Operator::Or => {
                        // For OR, both sides must reference the same table(s)
                        // If either side references multiple tables, we can't extract it
                        let left_tables = self.get_referenced_tables_in_expr(left)?;
                        let right_tables = self.get_referenced_tables_in_expr(right)?;

                        // If both sides only reference our table, include the whole OR
                        if left_tables.len() == 1
                            && left_tables.contains(&table_name.to_string())
                            && right_tables.len() == 1
                            && right_tables.contains(&table_name.to_string())
                        {
                            Ok(Some(expr.clone()))
                        } else {
                            // OR condition involves multiple tables, can't extract
                            Ok(None)
                        }
                    }
                    _ => {
                        // For comparison operators, check if this condition references only our table
                        // AND is simple enough to be pushed down (no complex expressions)
                        let referenced_tables = self.get_referenced_tables_in_expr(expr)?;
                        if referenced_tables.len() == 1
                            && referenced_tables.contains(&table_name.to_string())
                        {
                            // Check if this is a simple comparison that can be pushed down
                            // Complex expressions like (a * b) >= c should be handled by the circuit
                            if self.is_simple_comparison(expr) {
                                Ok(Some(expr.clone()))
                            } else {
                                // Complex expression - let the circuit handle it
                                Ok(None)
                            }
                        } else {
                            Ok(None)
                        }
                    }
                }
            }
            ast::Expr::Parenthesized(exprs) => {
                if exprs.len() == 1 {
                    self.extract_table_conditions(&exprs[0], table_name)
                } else {
                    Ok(None)
                }
            }
            _ => {
                // For other expressions, check if they reference only our table
                // AND are simple enough to be pushed down
                let referenced_tables = self.get_referenced_tables_in_expr(expr)?;
                if referenced_tables.len() == 1
                    && referenced_tables.contains(&table_name.to_string())
                    && self.is_simple_comparison(expr)
                {
                    Ok(Some(expr.clone()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Check if an expression is a simple comparison that can be pushed down to table scan
    /// Returns true for simple comparisons like "column = value" or "column > value"
    /// Returns false for complex expressions like "(a * b) > value"
    fn is_simple_comparison(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Binary(left, op, right) => {
                // Check if it's a comparison operator
                matches!(
                    op,
                    ast::Operator::Equals
                        | ast::Operator::NotEquals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals
                        | ast::Operator::Less
                        | ast::Operator::LessEquals
                ) && self.is_simple_operand(left)
                    && self.is_simple_operand(right)
            }
            _ => false,
        }
    }

    /// Check if an operand is simple (column reference or literal)
    fn is_simple_operand(&self, expr: &ast::Expr) -> bool {
        matches!(
            expr,
            ast::Expr::Id(_)
                | ast::Expr::Qualified(_, _)
                | ast::Expr::DoublyQualified(_, _, _)
                | ast::Expr::Literal(_)
        )
    }

    /// Get the set of table names referenced in an expression
    fn get_referenced_tables_in_expr(&self, expr: &ast::Expr) -> crate::Result<Vec<String>> {
        let mut tables = Vec::new();
        self.collect_referenced_tables(expr, &mut tables)?;
        // Deduplicate
        tables.sort();
        tables.dedup();
        Ok(tables)
    }

    /// Recursively collect table references from an expression
    fn collect_referenced_tables(
        &self,
        expr: &ast::Expr,
        tables: &mut Vec<String>,
    ) -> crate::Result<()> {
        match expr {
            ast::Expr::Binary(left, _, right) => {
                self.collect_referenced_tables(left, tables)?;
                self.collect_referenced_tables(right, tables)?;
            }
            ast::Expr::Qualified(table, _) => {
                // This is a qualified column reference (table.column or alias.column)
                // We need to resolve aliases to actual table names
                let actual_table = self.resolve_table_alias(table.as_str());
                tables.push(actual_table);
            }
            ast::Expr::Id(column) => {
                // Unqualified column reference
                if self.referenced_tables.len() > 1 {
                    // In a JOIN context, check which tables have this column
                    let mut tables_with_column = Vec::new();
                    for table in &self.referenced_tables {
                        if table
                            .columns
                            .iter()
                            .any(|c| c.name.as_ref() == Some(&column.to_string()))
                        {
                            tables_with_column.push(table.name.clone());
                        }
                    }

                    if tables_with_column.len() > 1 {
                        // Ambiguous column - this should have been caught earlier
                        // Return error to be safe
                        return Err(crate::LimboError::ParseError(format!(
                            "Ambiguous column name '{}' in WHERE clause - exists in tables: {}",
                            column,
                            tables_with_column.join(", ")
                        )));
                    } else if tables_with_column.len() == 1 {
                        // Unambiguous - only one table has this column
                        // This is allowed by SQLite
                        tables.push(tables_with_column[0].clone());
                    } else {
                        // Column doesn't exist in any table - this is an error
                        // but should be caught during compilation
                        return Err(crate::LimboError::ParseError(format!(
                            "Column '{column}' not found in any table"
                        )));
                    }
                } else {
                    // Single table context - unqualified columns belong to that table
                    if let Some(table) = self.referenced_tables.first() {
                        tables.push(table.name.clone());
                    }
                }
            }
            ast::Expr::DoublyQualified(_database, table, _column) => {
                // For database.table.column, resolve the table name
                let table_str = table.as_str();
                let actual_table = self.resolve_table_alias(table_str);
                tables.push(actual_table);
            }
            ast::Expr::Parenthesized(exprs) => {
                for e in exprs {
                    self.collect_referenced_tables(e, tables)?;
                }
            }
            _ => {
                // Literals and other expressions don't reference tables
            }
        }
        Ok(())
    }

    /// Convert a qualified expression to unqualified for single-table queries
    /// This removes table prefixes from column references since they're not needed
    /// when querying a single table
    fn unqualify_expression(&self, expr: &ast::Expr, table_name: &str) -> ast::Expr {
        match expr {
            ast::Expr::Binary(left, op, right) => {
                // Recursively unqualify both sides
                ast::Expr::Binary(
                    Box::new(self.unqualify_expression(left, table_name)),
                    *op,
                    Box::new(self.unqualify_expression(right, table_name)),
                )
            }
            ast::Expr::Qualified(table, column) => {
                // Convert qualified column to unqualified if it's for our table
                // Handle both "table.column" and "database.table.column" cases
                let table_str = table.as_str();

                // Check if this is a database.table reference
                let actual_table = if table_str.contains('.') {
                    // Split on '.' and take the last part as the table name
                    table_str
                        .split('.')
                        .next_back()
                        .unwrap_or(table_str)
                        .to_string()
                } else {
                    // Could be an alias or direct table name
                    self.resolve_table_alias(table_str)
                };

                if actual_table == table_name {
                    // Just return the column name without qualification
                    ast::Expr::Id(column.clone())
                } else {
                    // This shouldn't happen if extract_table_conditions worked correctly
                    // but keep it qualified just in case
                    expr.clone()
                }
            }
            ast::Expr::DoublyQualified(_database, table, column) => {
                // This is database.table.column format
                // Check if the table matches our target table
                let table_str = table.as_str();
                let actual_table = self.resolve_table_alias(table_str);

                if actual_table == table_name {
                    // Just return the column name without qualification
                    ast::Expr::Id(column.clone())
                } else {
                    // Keep it qualified if it's for a different table
                    expr.clone()
                }
            }
            ast::Expr::Parenthesized(exprs) => {
                // Recursively unqualify expressions in parentheses
                let unqualified_exprs: Vec<Box<ast::Expr>> = exprs
                    .iter()
                    .map(|e| Box::new(self.unqualify_expression(e, table_name)))
                    .collect();
                ast::Expr::Parenthesized(unqualified_exprs)
            }
            _ => {
                // Other expression types (literals, unqualified columns, etc.) stay as-is
                expr.clone()
            }
        }
    }

    /// Resolve a table alias to the actual table name
    fn resolve_table_alias(&self, alias: &str) -> String {
        // Check if there's an alias mapping in the FROM/JOIN clauses
        // For now, we'll do a simple check - if the alias matches a table name, use it
        // Otherwise, try to find it in the FROM clause

        // First check if it's an actual table name
        if self.referenced_tables.iter().any(|t| t.name == alias) {
            return alias.to_string();
        }

        // Check if it's an alias that maps to a table
        if let Some(table_name) = self.table_aliases.get(alias) {
            return table_name.clone();
        }

        // If we can't resolve it, return as-is (it might be a table name we don't know about)
        alias.to_string()
    }

    /// Populate the view by scanning the source table using a state machine
    /// This can be called multiple times and will resume from where it left off
    /// This method is only for materialized views and will persist data to the btree
    pub fn populate_from_table(
        &mut self,
        conn: &std::sync::Arc<crate::Connection>,
        pager: &std::sync::Arc<crate::Pager>,
        _btree_cursor: &mut BTreeCursor,
    ) -> crate::Result<IOResult<()>> {
        // Assert that this is a materialized view with a root page
        assert!(
            self.root_page != 0,
            "populate_from_table should only be called for materialized views with root_page"
        );

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
                    // Create a new connection for reading to avoid transaction conflicts
                    // This allows us to read from tables while the parent transaction is writing the view
                    // The statement holds a reference to this connection, keeping it alive
                    let read_conn = conn.db.connect()?;

                    // Prepare the statement using the read connection
                    let stmt = read_conn.prepare(&query)?;

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
                                let row = stmt.row().unwrap();

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

                            crate::vdbe::StepResult::IO => {
                                // Statement needs I/O - save state and return
                                self.populate_state = PopulateState::ProcessingOneTable {
                                    queries,
                                    current_idx,
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
                Some(Value::Integer(id)) => *id,
                _ => return None, // Invalid rowid
            };
            // All values are table columns (no separate rowid was selected)
            Some((rowid, all_values))
        } else {
            // The last value is the explicitly selected rowid
            let rowid = match all_values.last() {
                Some(Value::Integer(id)) => *id,
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
                SchemaColumn {
                    name: Some("price".to_string()),
                    ty: Type::Real,
                    ty_str: "REAL".to_string(),
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

        // Create logs table - without a rowid alias (no INTEGER PRIMARY KEY)
        let logs_table = BTreeTable {
            name: "logs".to_string(),
            root_page: 5,
            primary_key_columns: vec![], // No primary key, so no rowid alias
            columns: vec![
                SchemaColumn {
                    name: Some("message".to_string()),
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
                    name: Some("level".to_string()),
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
                    name: Some("timestamp".to_string()),
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
            has_rowid: true, // Has implicit rowid but no alias
            is_strict: false,
            unique_sets: vec![],
        };

        schema.add_btree_table(Arc::new(customers_table));
        schema.add_btree_table(Arc::new(orders_table));
        schema.add_btree_table(Arc::new(products_table));
        schema.add_btree_table(Arc::new(logs_table));
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

        let (tables, _, _) = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "customers");
    }

    #[test]
    fn test_extract_tables_from_inner_join() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id",
        );

        let (tables, _, _) = IncrementalView::extract_all_tables(&select, &schema).unwrap();

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

        let (tables, _, _) = IncrementalView::extract_all_tables(&select, &schema).unwrap();

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

        let (tables, _, _) = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_from_cross_join() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers CROSS JOIN orders");

        let (tables, _, _) = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_with_aliases() {
        let schema = create_test_schema();
        let select =
            parse_select("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id");

        let (tables, _, _) = IncrementalView::extract_all_tables(&select, &schema).unwrap();

        // Should still extract the actual table names, not aliases
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "customers");
        assert_eq!(tables[1].name, "orders");
    }

    #[test]
    fn test_extract_tables_nonexistent_table_error() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM nonexistent");

        let result =
            IncrementalView::extract_all_tables(&select, &schema).map(|(tables, _, _)| tables);

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

        let result =
            IncrementalView::extract_all_tables(&select, &schema).map(|(tables, _, _)| tables);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Table 'nonexistent' not found"));
    }

    #[test]
    fn test_sql_for_populate_simple_query_no_where() {
        // Test simple query with no WHERE clause
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers");

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 1);
        // customers has id as rowid alias, so no need for explicit rowid
        assert_eq!(queries[0], "SELECT * FROM customers");
    }

    #[test]
    fn test_sql_for_populate_simple_query_with_where() {
        // Test simple query with WHERE clause
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers WHERE id > 10");

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 1);
        // For single-table queries, we should get the full WHERE clause
        assert_eq!(queries[0], "SELECT * FROM customers WHERE id > 10");
    }

    #[test]
    fn test_sql_for_populate_join_with_where_on_both_tables() {
        // Test JOIN query with WHERE conditions on both tables
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE c.id > 10 AND o.total > 100",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);

        // With per-table WHERE extraction:
        // - customers table gets: c.id > 10
        // - orders table gets: o.total > 100
        assert_eq!(queries[0], "SELECT * FROM customers WHERE id > 10");
        assert_eq!(queries[1], "SELECT * FROM orders WHERE total > 100");
    }

    #[test]
    fn test_sql_for_populate_complex_join_with_mixed_conditions() {
        // Test complex JOIN with WHERE conditions mixing both tables
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE c.id > 10 AND o.total > 100 AND c.name = 'John' \
             AND o.customer_id = 5 AND (c.id = 15 OR o.total = 200)",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);

        // With per-table WHERE extraction:
        // - customers gets: c.id > 10 AND c.name = 'John'
        // - orders gets: o.total > 100 AND o.customer_id = 5
        // Note: The OR condition (c.id = 15 OR o.total = 200) involves both tables,
        // so it cannot be extracted to either table individually
        assert_eq!(
            queries[0],
            "SELECT * FROM customers WHERE id > 10 AND name = 'John'"
        );
        assert_eq!(
            queries[1],
            "SELECT * FROM orders WHERE total > 100 AND customer_id = 5"
        );
    }

    #[test]
    fn test_where_extraction_for_three_tables() {
        // Test that WHERE clause extraction correctly separates conditions for 3+ tables
        // This addresses the concern about conditions "piling up" as joins increase

        // Simulate a three-table scenario
        let schema = create_test_schema();

        // Parse a WHERE clause with conditions for three different tables
        let select = parse_select(
            "SELECT * FROM customers WHERE c.id > 10 AND o.total > 100 AND p.price > 50",
        );

        // Get the WHERE expression
        if let ast::OneSelect::Select {
            where_clause: Some(ref where_expr),
            ..
        } = select.body.select
        {
            // Create a view with three tables to test extraction
            let tables = vec![
                schema.get_btree_table("customers").unwrap(),
                schema.get_btree_table("orders").unwrap(),
                schema.get_btree_table("products").unwrap(),
            ];

            let mut aliases = HashMap::new();
            aliases.insert("c".to_string(), "customers".to_string());
            aliases.insert("o".to_string(), "orders".to_string());
            aliases.insert("p".to_string(), "products".to_string());

            // Create a minimal view just to test extraction logic
            let view = IncrementalView {
                name: "test".to_string(),
                select_stmt: select.clone(),
                circuit: DbspCircuit::new(1, 2, 3),
                referenced_tables: tables,
                table_aliases: aliases,
                qualified_table_names: HashMap::new(),
                column_schema: ViewColumnSchema {
                    columns: vec![],
                    tables: vec![],
                },
                populate_state: PopulateState::Start,
                tracker: Arc::new(Mutex::new(ComputationTracker::new())),
                root_page: 0,
            };

            // Test extraction for each table
            let customers_conds = view
                .extract_table_conditions(where_expr, "customers")
                .unwrap();
            let orders_conds = view.extract_table_conditions(where_expr, "orders").unwrap();
            let products_conds = view
                .extract_table_conditions(where_expr, "products")
                .unwrap();

            // Verify each table only gets its conditions
            if let Some(cond) = customers_conds {
                let sql = cond.to_string();
                assert!(sql.contains("id > 10"));
                assert!(!sql.contains("total"));
                assert!(!sql.contains("price"));
            }

            if let Some(cond) = orders_conds {
                let sql = cond.to_string();
                assert!(sql.contains("total > 100"));
                assert!(!sql.contains("id > 10")); // From customers
                assert!(!sql.contains("price"));
            }

            if let Some(cond) = products_conds {
                let sql = cond.to_string();
                assert!(sql.contains("price > 50"));
                assert!(!sql.contains("id > 10")); // From customers
                assert!(!sql.contains("total"));
            }
        } else {
            panic!("Failed to parse WHERE clause");
        }
    }

    #[test]
    fn test_alias_resolution_works_correctly() {
        // Test that alias resolution properly maps aliases to table names
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE c.id > 10 AND o.total > 100",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        // Verify that alias mappings were extracted correctly
        assert_eq!(view.table_aliases.get("c"), Some(&"customers".to_string()));
        assert_eq!(view.table_aliases.get("o"), Some(&"orders".to_string()));

        // Verify that SQL generation uses the aliases correctly
        let queries = view.sql_for_populate().unwrap();
        assert_eq!(queries.len(), 2);

        // Each query should use the actual table name, not the alias
        assert!(queries[0].contains("FROM customers") || queries[1].contains("FROM customers"));
        assert!(queries[0].contains("FROM orders") || queries[1].contains("FROM orders"));
    }

    #[test]
    fn test_sql_for_populate_table_without_rowid_alias() {
        // Test that tables without a rowid alias properly include rowid in SELECT
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM logs WHERE level > 2");

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 1);
        // logs table has no rowid alias, so we need to explicitly select rowid
        assert_eq!(queries[0], "SELECT *, rowid FROM logs WHERE level > 2");
    }

    #[test]
    fn test_sql_for_populate_join_with_and_without_rowid_alias() {
        // Test JOIN between a table with rowid alias and one without
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers c \
             JOIN logs l ON c.id = l.level \
             WHERE c.id > 10 AND l.level > 2",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);
        // customers has rowid alias (id), logs doesn't
        assert_eq!(queries[0], "SELECT * FROM customers WHERE id > 10");
        assert_eq!(queries[1], "SELECT *, rowid FROM logs WHERE level > 2");
    }

    #[test]
    fn test_sql_for_populate_with_database_qualified_names() {
        // Test that database.table.column references are handled correctly
        // The table name in FROM should keep the database prefix,
        // but column names in WHERE should be unqualified
        let schema = create_test_schema();

        // Test with single table using database qualification
        let select = parse_select("SELECT * FROM main.customers WHERE main.customers.id > 10");

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 1);
        // The FROM clause should preserve the database qualification,
        // but the WHERE clause should have unqualified column names
        assert_eq!(queries[0], "SELECT * FROM main.customers WHERE id > 10");
    }

    #[test]
    fn test_sql_for_populate_join_with_database_qualified_names() {
        // Test JOIN with database-qualified table and column references
        let schema = create_test_schema();

        let select = parse_select(
            "SELECT * FROM main.customers c \
             JOIN main.orders o ON c.id = o.customer_id \
             WHERE main.customers.id > 10 AND main.orders.total > 100",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);
        // The FROM clauses should preserve database qualification,
        // but WHERE clauses should have unqualified column names
        assert_eq!(queries[0], "SELECT * FROM main.customers WHERE id > 10");
        assert_eq!(queries[1], "SELECT * FROM main.orders WHERE total > 100");
    }

    #[test]
    fn test_sql_for_populate_unambiguous_unqualified_column() {
        // Test that unambiguous unqualified columns ARE extracted
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE total > 100", // 'total' only exists in orders table
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);

        // 'total' is unambiguous (only in orders), so it should be extracted
        assert_eq!(queries[0], "SELECT * FROM customers");
        assert_eq!(queries[1], "SELECT * FROM orders WHERE total > 100");
    }

    #[test]
    fn test_database_qualified_table_names() {
        let schema = create_test_schema();

        // Test with database-qualified table names
        let select = parse_select(
            "SELECT c.id, c.name, o.id, o.total
             FROM main.customers c
             JOIN main.orders o ON c.id = o.customer_id
             WHERE c.id > 10",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();

        // Check that qualified names are preserved
        assert!(qualified_names.contains_key("customers"));
        assert_eq!(qualified_names.get("customers").unwrap(), "main.customers");
        assert!(qualified_names.contains_key("orders"));
        assert_eq!(qualified_names.get("orders").unwrap(), "main.orders");

        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names.clone(),
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);

        // The FROM clause should contain the database-qualified name
        // But the WHERE clause should use unqualified column names
        assert_eq!(queries[0], "SELECT * FROM main.customers WHERE id > 10");
        assert_eq!(queries[1], "SELECT * FROM main.orders");
    }

    #[test]
    fn test_mixed_qualified_unqualified_tables() {
        let schema = create_test_schema();

        // Test with a mix of qualified and unqualified table names
        let select = parse_select(
            "SELECT c.id, c.name, o.id, o.total
             FROM main.customers c
             JOIN orders o ON c.id = o.customer_id
             WHERE c.id > 10 AND o.total < 1000",
        );

        let (tables, aliases, qualified_names) =
            IncrementalView::extract_all_tables(&select, &schema).unwrap();

        // Check that qualified names are preserved where specified
        assert_eq!(qualified_names.get("customers").unwrap(), "main.customers");
        // Unqualified tables should not have an entry (or have the bare name)
        assert!(
            !qualified_names.contains_key("orders")
                || qualified_names.get("orders").unwrap() == "orders"
        );

        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names.clone(),
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2);

        // The FROM clause should preserve qualification where specified
        assert_eq!(queries[0], "SELECT * FROM main.customers WHERE id > 10");
        assert_eq!(queries[1], "SELECT * FROM orders WHERE total < 1000");
    }
}
