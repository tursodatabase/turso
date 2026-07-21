use super::dbsp::Delta;
use crate::schema::{BTreeTable, Schema};
use crate::sync::Arc;
use crate::types::Value;
use crate::util::{extract_view_columns, ViewColumnSchema};
use crate::{LimboError, Result};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use turso_parser::ast;
use turso_parser::{
    ast::{Cmd, Stmt},
    parser::Parser,
};

/// Version of the materialized-view state storage format, embedded in the
/// internal state table name (`__turso_internal_dbsp_state_v<N>_<view>`).
/// Views created under a different version are unusable and must be
/// recreated.
///
/// v2: maintenance compiled to VDBE programs. Filter/project views have no
/// state table at all; GROUP BY views use a typed state table (group keys,
/// view rowid, aggregate payloads) instead of v1's generic blob layout.
pub const DBSP_CIRCUIT_VERSION: u32 = 2;

/// The automatic primary-key index of a view's internal state table, derived
/// from the state table's PRIMARY KEY (the group columns).
pub fn create_dbsp_state_index(
    root_page: i64,
    state_table: &BTreeTable,
) -> Result<crate::schema::Index> {
    use crate::schema::{Index, IndexColumn};
    let mut columns = Vec::with_capacity(state_table.primary_key_columns.len());
    for (name, order) in &state_table.primary_key_columns {
        let pos_in_table = state_table
            .columns()
            .iter()
            .position(|c| c.name.as_deref() == Some(name.as_str()))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "state table {} primary key column {name} not found",
                    state_table.name
                ))
            })?;
        columns.push(IndexColumn {
            name: name.clone(),
            order: *order,
            collation: None,
            pos_in_table,
            default: None,
            expr: None,
        });
    }
    Ok(Index {
        name: "dbsp_state_pk".to_string(),
        table_name: state_table.name.clone(),
        root_page,
        columns,
        unique: true,
        ephemeral: false,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    })
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

    /// Per-table delta lengths, recorded at statement start so a statement
    /// rollback can rewind captures made by the aborted statement.
    pub fn table_delta_lens(&self) -> HashMap<String, usize> {
        self.table_deltas
            .borrow()
            .iter()
            .map(|(table, delta)| (table.clone(), delta.len()))
            .collect()
    }

    /// Rewind each table's delta to the length recorded in `marks` (zero for
    /// tables that had no delta when the marks were taken).
    pub fn rewind_to_marks(&self, marks: &HashMap<String, usize>) {
        let mut deltas = self.table_deltas.borrow_mut();
        for (table, delta) in deltas.iter_mut() {
            delta.truncate(marks.get(table).copied().unwrap_or(0));
        }
    }
}

/// Per-view, per-table delta lengths recorded at statement start, used to
/// rewind captures made by an aborted statement subtransaction.
pub type ViewDeltaMarks = HashMap<String, HashMap<String, usize>>;

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

    /// Snapshot per-view, per-table delta lengths at statement start.
    ///
    /// Delta capture happens inside `op_insert`/`op_delete` as rows are
    /// written, so when a statement subtransaction rolls back, captures made
    /// by the aborted statement must be rewound or they would be applied to
    /// the views at commit as phantom changes.
    pub fn delta_marks(&self) -> ViewDeltaMarks {
        self.states
            .borrow()
            .iter()
            .map(|(view, state)| (view.clone(), state.table_delta_lens()))
            .collect()
    }

    /// Rewind all captured deltas to the recorded statement-start marks.
    pub fn rewind_to_marks(&self, marks: &ViewDeltaMarks) {
        static EMPTY: std::sync::LazyLock<HashMap<String, usize>> =
            std::sync::LazyLock::new(HashMap::default);
        for (view, state) in self.states.borrow().iter() {
            let view_marks = marks.get(view).unwrap_or(&EMPTY);
            state.rewind_to_marks(view_marks);
        }
    }
}

/// A materialized view: its defining SELECT, output schema, and storage root.
///
/// Maintenance is performed by compiled VDBE programs (see
/// `incremental::vdbe_maintenance`): the transaction's captured deltas are
/// merged into the view's btree at commit, initial population runs the same
/// program over the base table, and uncommitted same-transaction reads run it
/// in emit mode to overlay the btree contents.
#[derive(Debug)]
pub struct IncrementalView {
    name: String,
    // The SELECT statement that defines how to transform input data
    pub select_stmt: ast::Select,

    // All tables referenced by this view (from FROM clause and JOINs)
    referenced_tables: Vec<Arc<BTreeTable>>,
    // Mapping from table aliases to actual table names (e.g., "c" -> "customers").
    // Feeds populate-query generation, currently exercised only by tests.
    #[cfg_attr(not(test), allow(dead_code))]
    table_aliases: HashMap<String, String>,
    // Mapping from table name to fully qualified name (e.g., "customers" -> "main.customers")
    // This preserves database qualification from the original query
    #[cfg_attr(not(test), allow(dead_code))]
    qualified_table_names: HashMap<String, String>,
    // WHERE conditions for each table (accumulated from all occurrences)
    // Multiple conditions from UNION branches or duplicate references are stored as a vector
    #[cfg_attr(not(test), allow(dead_code))]
    table_conditions: HashMap<String, Vec<Option<ast::Expr>>>,
    // The view's column schema with table relationships
    pub column_schema: ViewColumnSchema,
    // Root page of the btree storing the materialized state (0 for unmaterialized)
    root_page: i64,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
unsafe impl Send for IncrementalView {}
unsafe impl Sync for IncrementalView {}
crate::assert::assert_send_sync!(IncrementalView);

impl IncrementalView {
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
        resolver: &crate::translate::emitter::Resolver,
    ) -> Result<ViewColumnSchema> {
        crate::util::validate_select_for_unsupported_features(select)?;
        // Views are maintained by compiled VDBE programs; shapes the codegen
        // cannot maintain yet are rejected at CREATE rather than silently
        // mis-maintained (and the rejection must happen HERE, at translate
        // time: failing later inside the CREATE program aborts the statement
        // after btree pages were allocated and leaves stale in-memory schema
        // entries behind, which corrupts the database once those pages are
        // reused). The supported set grows as operator codegen lands.
        let shape = crate::incremental::vdbe_maintenance::classify_view(select, schema, resolver)?;
        crate::incremental::vdbe_maintenance::validate_multiset_args(select, &shape, schema)?;
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
        _schema: &Schema,
        main_data_root: i64,
        _internal_state_root: i64,
        _internal_state_index_root: i64,
    ) -> Result<Self> {
        Ok(Self {
            name,
            select_stmt,
            referenced_tables,
            table_aliases,
            qualified_table_names,
            table_conditions,
            column_schema,
            root_page: main_data_root,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
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

    /// Generate SQL queries for populating the view from each source table.
    /// Retained for tests of populate-query generation.
    #[cfg(test)]
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::vec;
    use crate::schema::{
        BTreeCharacteristics, BTreeTable, ColDef, Column as SchemaColumn, Schema, Type,
    };
    use crate::sync::Arc;
    use turso_parser::ast;
    use turso_parser::parser::Parser;

    // Helper function to create a test schema with multiple tables
    fn create_test_schema() -> Schema {
        let mut schema = Schema::new();

        // Create customers table
        let columns = vec![
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
                    explicit_notnull: false,
                    unique: false,
                    hidden: false,
                    notnull_conflict_clause: None,
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        ];
        let customers_table = BTreeTable::new(
            2,
            "customers".to_string(),
            vec![("id".to_string(), ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        // Create orders table
        let columns = vec![
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
                    explicit_notnull: false,
                    unique: false,
                    hidden: false,
                    notnull_conflict_clause: None,
                },
            ),
            SchemaColumn::new(
                Some("customer_id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            ),
            SchemaColumn::new_default_integer(
                Some("total".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let orders_table = BTreeTable::new(
            3,
            "orders".to_string(),
            vec![("id".to_string(), ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        // Create products table
        let columns = vec![
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
                    explicit_notnull: false,
                    unique: false,
                    hidden: false,
                    notnull_conflict_clause: None,
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
            SchemaColumn::new(
                Some("price".to_string()),
                "REAL".to_string(),
                None,
                None,
                Type::Real,
                None,
                ColDef::default(),
            ),
        ];
        let products_table = BTreeTable::new(
            4,
            "products".to_string(),
            vec![("id".to_string(), ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        // Create logs table - without a rowid alias (no INTEGER PRIMARY KEY)
        let columns = vec![
            SchemaColumn::new(
                Some("message".to_string()),
                "TEXT".to_string(),
                None,
                None,
                Type::Text,
                None,
                ColDef::default(),
            ),
            SchemaColumn::new_default_integer(
                Some("level".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("timestamp".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        // logs has no primary key (no rowid alias) but does have an implicit rowid.
        let logs_table = BTreeTable::new(
            5,
            "logs".to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        );

        schema
            .add_btree_table(Arc::new(customers_table))
            .expect("Test setup: failed to add customers table");

        schema
            .add_btree_table(Arc::new(orders_table))
            .expect("Test setup: failed to add orders table");

        schema
            .add_btree_table(Arc::new(products_table))
            .expect("Test setup: failed to add products table");

        schema
            .add_btree_table(Arc::new(logs_table))
            .expect("Test setup: failed to add logs table");

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

    // Type alias for the complex return type of extract_all_tables
    type ExtractedTableInfo = (
        Vec<Arc<BTreeTable>>,
        HashMap<String, String>,
        HashMap<String, String>,
        HashMap<String, Vec<Option<ast::Expr>>>,
    );

    fn extract_all_tables(select: &ast::Select, schema: &Schema) -> Result<ExtractedTableInfo> {
        let mut referenced_tables = Vec::new();
        let mut table_aliases = HashMap::default();
        let mut qualified_table_names = HashMap::default();
        let mut table_conditions = HashMap::default();
        IncrementalView::extract_all_tables(
            select,
            schema,
            &mut referenced_tables,
            &mut table_aliases,
            &mut qualified_table_names,
            &mut table_conditions,
        )?;
        Ok((
            referenced_tables,
            table_aliases,
            qualified_table_names,
            table_conditions,
        ))
    }

    #[test]
    fn test_extract_single_table() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers");

        let (tables, _, _, _table_conditions) = extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "customers");
    }

    #[test]
    fn test_tables_from_union() {
        let schema = create_test_schema();
        let select = parse_select("SELECT name FROM customers union SELECT name from products");

        let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(table_conditions.contains_key("customers"));
        assert!(table_conditions.contains_key("products"));
    }

    #[test]
    fn test_extract_tables_from_inner_join() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id",
        );

        let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(table_conditions.contains_key("customers"));
        assert!(table_conditions.contains_key("orders"));
    }

    #[test]
    fn test_extract_tables_from_multiple_joins() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers
             INNER JOIN orders ON customers.id = orders.customer_id
             INNER JOIN products ON orders.id = products.id",
        );

        let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 3);
        assert!(table_conditions.contains_key("customers"));
        assert!(table_conditions.contains_key("orders"));
        assert!(table_conditions.contains_key("products"));
    }

    #[test]
    fn test_extract_tables_from_left_join() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers LEFT JOIN orders ON customers.id = orders.customer_id",
        );

        let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(table_conditions.contains_key("customers"));
        assert!(table_conditions.contains_key("orders"));
    }

    #[test]
    fn test_extract_tables_from_cross_join() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM customers CROSS JOIN orders");

        let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(table_conditions.contains_key("customers"));
        assert!(table_conditions.contains_key("orders"));
    }

    #[test]
    fn test_extract_tables_with_aliases() {
        let schema = create_test_schema();
        let select =
            parse_select("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id");

        let (tables, aliases, _, _table_conditions) = extract_all_tables(&select, &schema).unwrap();

        // Should still extract the actual table names, not aliases
        assert_eq!(tables.len(), 2);
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"orders"));

        // Check that aliases are correctly mapped
        assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
        assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
    }

    #[test]
    fn test_extract_tables_nonexistent_table_error() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM nonexistent");

        let result = extract_all_tables(&select, &schema).map(|(tables, _, _, _)| tables);

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

        let result = extract_all_tables(&select, &schema).map(|(tables, _, _, _)| tables);

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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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
        assert!(queries
            .iter()
            .any(|q| q == "SELECT * FROM customers WHERE id > 10"));
        assert!(queries
            .iter()
            .any(|q| q == "SELECT * FROM orders WHERE total > 100"));
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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
        // Check both queries exist (order doesn't matter)
        assert!(queries
            .contains(&"SELECT * FROM customers WHERE id > 10 AND name = 'John'".to_string()));
        assert!(queries
            .contains(&"SELECT * FROM orders WHERE total > 100 AND customer_id = 5".to_string()));
    }

    #[test]
    fn test_sql_for_populate_table_without_rowid_alias() {
        let schema = create_test_schema();
        let select = parse_select("SELECT * FROM logs WHERE level > 2");

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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
        assert!(queries.contains(&"SELECT * FROM customers WHERE id > 10".to_string()));
        assert!(queries.contains(&"SELECT *, rowid FROM logs WHERE level > 2".to_string()));
    }

    #[test]
    fn test_sql_for_populate_with_database_qualified_names() {
        // Test that database.table.column references are handled correctly
        // The table name in FROM should keep the database prefix,
        // but column names in WHERE should be unqualified
        let schema = create_test_schema();

        // Test with single table using database qualification
        let select = parse_select("SELECT * FROM main.customers WHERE main.customers.id > 10");

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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
        assert!(queries.contains(&"SELECT * FROM main.customers WHERE id > 10".to_string()));
        assert!(queries.contains(&"SELECT * FROM main.orders WHERE total > 100".to_string()));
    }

    #[test]
    fn test_where_extraction_for_three_tables_with_aliases() {
        // Test that WHERE clause extraction correctly separates conditions for 3+ tables
        // This addresses the concern about conditions "piling up" as joins increase
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers c
             JOIN orders o ON c.id = o.customer_id
             JOIN products p ON p.id = o.product_id
             WHERE c.id > 10 AND o.total > 100 AND p.price > 50",
        );

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        // Verify we extracted all three tables
        assert_eq!(tables.len(), 3);
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"products"));

        // Verify aliases are correctly mapped
        assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
        assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
        assert_eq!(aliases.get("p"), Some(&"products".to_string()));

        // Generate populate queries to verify each table gets its own conditions
        let queries = IncrementalView::generate_populate_queries(
            &select,
            &tables,
            &aliases,
            &qualified_names,
            &table_conditions,
        )
        .unwrap();

        assert_eq!(queries.len(), 3);

        // Verify the exact queries generated for each table
        // The order might vary, so check all possibilities
        let expected_queries = vec![
            "SELECT * FROM customers WHERE id > 10",
            "SELECT * FROM orders WHERE total > 100",
            "SELECT * FROM products WHERE price > 50",
        ];

        for expected in &expected_queries {
            assert!(
                queries.contains(&expected.to_string()),
                "Missing expected query: {expected}. Got: {queries:?}"
            );
        }
    }

    #[test]
    fn test_sql_for_populate_complex_expressions_not_included() {
        // Test that complex expressions (subqueries, CASE, string concat) are NOT included in populate queries
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM customers
             WHERE id > (SELECT MAX(customer_id) FROM orders)
               AND name || ' Customer' = 'John Customer'
               AND CASE WHEN id > 10 THEN 1 ELSE 0 END = 1
               AND EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)",
        );

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        let queries = IncrementalView::generate_populate_queries(
            &select,
            &tables,
            &aliases,
            &qualified_names,
            &table_conditions,
        )
        .unwrap();

        assert_eq!(queries.len(), 1);
        // Since customers table has an INTEGER PRIMARY KEY (id), we should get SELECT *
        // without rowid and without WHERE clause (all conditions are complex)
        assert_eq!(queries[0], "SELECT * FROM customers");
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();
        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
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
        assert!(queries.contains(&"SELECT * FROM customers".to_string()));
        assert!(queries.contains(&"SELECT * FROM orders WHERE total > 100".to_string()));
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

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
            qualified_names,
            table_conditions,
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
        assert!(queries.contains(&"SELECT * FROM main.customers WHERE id > 10".to_string()));
        assert!(queries.contains(&"SELECT * FROM main.orders".to_string()));
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

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

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
            qualified_names,
            table_conditions,
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
        assert!(queries.contains(&"SELECT * FROM main.customers WHERE id > 10".to_string()));
        assert!(queries.contains(&"SELECT * FROM orders WHERE total < 1000".to_string()));
    }

    #[test]
    fn test_extract_tables_with_simple_cte() {
        let schema = create_test_schema();
        let select = parse_select(
            "WITH customer_totals AS (
                SELECT c.id, c.name, SUM(o.total) as total_spent
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                GROUP BY c.id, c.name
            )
            SELECT * FROM customer_totals WHERE total_spent > 1000",
        );

        let (tables, aliases, _qualified_names, _table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        // Check that we found both tables from the CTE
        assert_eq!(tables.len(), 2);
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"orders"));

        // Check aliases from the CTE
        assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
        assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
    }

    #[test]
    fn test_extract_tables_with_multiple_ctes() {
        let schema = create_test_schema();
        let select = parse_select(
            "WITH
            high_value_customers AS (
                SELECT id, name
                FROM customers
                WHERE id IN (SELECT customer_id FROM orders WHERE total > 500)
            ),
            recent_orders AS (
                SELECT id, customer_id, total
                FROM orders
                WHERE id > 100
            )
            SELECT hvc.name, ro.total
            FROM high_value_customers hvc
            JOIN recent_orders ro ON hvc.id = ro.customer_id",
        );

        let (tables, _aliases, _qualified_names, _table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        // Check that we found both tables from both CTEs
        assert_eq!(tables.len(), 2);
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"orders"));
    }

    #[test]
    fn test_sql_for_populate_union_mixed_conditions() {
        // Test UNION where same table appears with and without WHERE clause
        // This should drop ALL conditions to ensure we get all rows
        let schema = create_test_schema();

        let select = parse_select(
            "SELECT * FROM customers WHERE id > 10
             UNION ALL
             SELECT * FROM customers",
        );

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        let view = IncrementalView::new(
            "union_view".to_string(),
            select.clone(),
            tables,
            aliases,
            qualified_names,
            table_conditions,
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1, // main_data_root
            2, // internal_state_root
            3, // internal_state_index_root
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 1);
        // When the same table appears with and without WHERE conditions in a UNION,
        // we must fetch ALL rows (no WHERE clause) because the conditions are incompatible
        assert_eq!(
            queries[0], "SELECT * FROM customers",
            "UNION with mixed conditions (some with WHERE, some without) should fetch ALL rows"
        );
    }

    #[test]
    fn test_extract_tables_with_nested_cte() {
        let schema = create_test_schema();
        let select = parse_select(
            "WITH RECURSIVE customer_hierarchy AS (
                SELECT id, name, 0 as level
                FROM customers
                WHERE id = 1
                UNION ALL
                SELECT c.id, c.name, ch.level + 1
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN customer_hierarchy ch ON o.customer_id = ch.id
                WHERE ch.level < 3
            )
            SELECT * FROM customer_hierarchy",
        );

        let (tables, _aliases, _qualified_names, _table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        // Check that we found the tables referenced in the recursive CTE
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

        // We're finding duplicates because "customers" appears twice in the recursive CTE
        // Let's deduplicate
        let unique_tables: HashSet<&str> = table_names.iter().cloned().collect();
        assert_eq!(unique_tables.len(), 2);
        assert!(unique_tables.contains("customers"));
        assert!(unique_tables.contains("orders"));
    }

    #[test]
    fn test_extract_tables_with_cte_and_main_query() {
        let schema = create_test_schema();
        let select = parse_select(
            "WITH customer_stats AS (
                SELECT customer_id, COUNT(*) as order_count
                FROM orders
                GROUP BY customer_id
            )
            SELECT c.name, cs.order_count, p.name as product_name
            FROM customers c
            JOIN customer_stats cs ON c.id = cs.customer_id
            JOIN products p ON p.id = 1",
        );

        let (tables, aliases, _qualified_names, _table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        // Check that we found tables from both the CTE and the main query
        assert_eq!(tables.len(), 3);
        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"orders"));
        assert!(table_names.contains(&"products"));

        // Check aliases from main query
        assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
        assert_eq!(aliases.get("p"), Some(&"products".to_string()));
    }

    #[test]
    fn test_sql_for_populate_simple_union() {
        let schema = create_test_schema();
        let select = parse_select(
            "SELECT * FROM orders WHERE total > 1000
             UNION ALL
             SELECT * FROM orders WHERE total < 100",
        );

        let (tables, aliases, qualified_names, table_conditions) =
            extract_all_tables(&select, &schema).unwrap();

        // Generate populate queries
        let queries = IncrementalView::generate_populate_queries(
            &select,
            &tables,
            &aliases,
            &qualified_names,
            &table_conditions,
        )
        .unwrap();

        // We should have deduplicated to a single table
        assert_eq!(tables.len(), 1, "Should have one unique table");
        assert_eq!(tables[0].name, "orders"); // Single table, order doesn't matter

        // Should have collected two conditions
        assert_eq!(table_conditions.get("orders").unwrap().len(), 2);

        // Should combine multiple conditions with OR
        assert_eq!(queries.len(), 1);
        // Conditions are combined with OR
        assert_eq!(
            queries[0],
            "SELECT * FROM orders WHERE (total > 1000) OR (total < 100)"
        );
    }

    #[test]
    fn test_sql_for_populate_with_union_and_filters() {
        let schema = create_test_schema();

        // Test UNION with different WHERE conditions on the same table
        let select = parse_select(
            "SELECT * FROM orders WHERE total > 1000
             UNION ALL
             SELECT * FROM orders WHERE total < 100",
        );

        let view = IncrementalView::from_stmt(
            ast::QualifiedName {
                db_name: None,
                name: ast::Name::exact("test_view".to_string()),
                alias: None,
            },
            select,
            &schema,
            1,
            2,
            3,
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        // We deduplicate tables, so we get 1 query for orders
        assert_eq!(queries.len(), 1);

        // Multiple conditions on the same table are combined with OR
        assert_eq!(
            queries[0],
            "SELECT * FROM orders WHERE (total > 1000) OR (total < 100)"
        );
    }

    #[test]
    fn test_sql_for_populate_with_union_mixed_tables() {
        let schema = create_test_schema();

        // Test UNION with different tables
        let select = parse_select(
            "SELECT id, name FROM customers WHERE id > 10
             UNION ALL
             SELECT customer_id as id, 'Order' as name FROM orders WHERE total > 500",
        );

        let view = IncrementalView::from_stmt(
            ast::QualifiedName {
                db_name: None,
                name: ast::Name::exact("test_view".to_string()),
                alias: None,
            },
            select,
            &schema,
            1,
            2,
            3,
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        assert_eq!(queries.len(), 2, "Should have one query per table");

        // Check that each table gets its appropriate WHERE clause
        let customers_query = queries
            .iter()
            .find(|q| q.contains("FROM customers"))
            .unwrap();
        let orders_query = queries.iter().find(|q| q.contains("FROM orders")).unwrap();

        assert!(customers_query.contains("WHERE id > 10"));
        assert!(orders_query.contains("WHERE total > 500"));
    }

    #[test]
    fn test_sql_for_populate_duplicate_tables_conflicting_filters() {
        // This tests what happens when we have duplicate table references with different filters
        // We need to manually construct a view to simulate what would happen with CTEs
        let schema = create_test_schema();

        // Get the orders table twice (simulating what would happen with CTEs)
        let orders_table = schema.get_btree_table("orders").unwrap();

        let referenced_tables = std::vec![orders_table.clone(), orders_table];

        // Create a SELECT that would have conflicting WHERE conditions
        let select = parse_select(
            "SELECT * FROM orders WHERE total > 1000", // This is just for the AST
        );

        let view = IncrementalView::new(
            "test_view".to_string(),
            select.clone(),
            referenced_tables,
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            extract_view_columns(&select, &schema).unwrap(),
            &schema,
            1,
            2,
            3,
        )
        .unwrap();

        let queries = view.sql_for_populate().unwrap();

        // With duplicates, we should get 2 identical queries
        assert_eq!(queries.len(), 2);

        // Both should be the same since they're from the same table reference
        assert_eq!(queries[0], queries[1]);
    }

    #[test]
    fn test_table_extraction_with_nested_ctes_complex_conditions() {
        let schema = create_test_schema();
        let select = parse_select(
            "WITH
            customer_orders AS (
                SELECT c.*, o.total
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                WHERE c.name LIKE 'A%' AND o.total > 100
            ),
            top_customers AS (
                SELECT * FROM customer_orders WHERE total > 500
            )
            SELECT * FROM top_customers",
        );

        // Test table extraction directly without creating a view
        let mut tables = Vec::new();
        let mut aliases = HashMap::default();
        let mut qualified_names = HashMap::default();
        let mut table_conditions = HashMap::default();

        IncrementalView::extract_all_tables(
            &select,
            &schema,
            &mut tables,
            &mut aliases,
            &mut qualified_names,
            &mut table_conditions,
        )
        .unwrap();

        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

        // Should have one reference to each table
        assert_eq!(table_names.len(), 2, "Should have 2 table references");
        assert!(table_names.contains(&"customers"));
        assert!(table_names.contains(&"orders"));

        // Check aliases
        assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
        assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
    }

    #[test]
    fn test_union_all_populate_queries() {
        // Test that UNION ALL generates correct populate queries
        let schema = create_test_schema();

        // Create a UNION ALL query that references the same table twice with different WHERE conditions
        let sql = "
            SELECT id, name FROM customers WHERE id < 5
            UNION ALL
            SELECT id, name FROM customers WHERE id > 10
        ";

        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd().unwrap();
        let select_stmt = match cmd.unwrap() {
            turso_parser::ast::Cmd::Stmt(ast::Stmt::Select(select)) => select,
            _ => panic!("Expected SELECT statement"),
        };

        // Extract tables and conditions
        let (tables, aliases, qualified_names, conditions) =
            extract_all_tables(&select_stmt, &schema).unwrap();

        // Generate populate queries
        let queries = IncrementalView::generate_populate_queries(
            &select_stmt,
            &tables,
            &aliases,
            &qualified_names,
            &conditions,
        )
        .unwrap();

        // Expected query - assuming customers table has INTEGER PRIMARY KEY
        // so we don't need to select rowid separately
        let expected = "SELECT * FROM customers WHERE (id < 5) OR (id > 10)";

        assert_eq!(
            queries.len(),
            1,
            "Should generate exactly 1 query for UNION ALL with same table"
        );
        assert_eq!(queries[0], expected, "Query should match expected format");
    }

    #[test]
    fn test_union_all_different_tables_populate_queries() {
        // Test UNION ALL with different tables
        let schema = create_test_schema();

        let sql = "
            SELECT id, name FROM customers WHERE id < 5
            UNION ALL
            SELECT id, product_name FROM orders WHERE amount > 100
        ";

        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd().unwrap();
        let select_stmt = match cmd.unwrap() {
            turso_parser::ast::Cmd::Stmt(ast::Stmt::Select(select)) => select,
            _ => panic!("Expected SELECT statement"),
        };

        // Extract tables and conditions
        let (tables, aliases, qualified_names, conditions) =
            extract_all_tables(&select_stmt, &schema).unwrap();

        // Generate populate queries
        let queries = IncrementalView::generate_populate_queries(
            &select_stmt,
            &tables,
            &aliases,
            &qualified_names,
            &conditions,
        )
        .unwrap();

        // Should generate separate queries for each table
        assert_eq!(
            queries.len(),
            2,
            "Should generate 2 queries for different tables"
        );

        // Check we have queries for both tables
        let has_customers = queries.iter().any(|q| q.contains("customers"));
        let has_orders = queries.iter().any(|q| q.contains("orders"));
        assert!(has_customers, "Should have a query for customers table");
        assert!(has_orders, "Should have a query for orders table");

        // Verify the customers query has its WHERE clause
        let customers_query = queries
            .iter()
            .find(|q| q.contains("customers"))
            .expect("Should have customers query");
        assert!(
            customers_query.contains("WHERE"),
            "Customers query should have WHERE clause"
        );
    }
}
