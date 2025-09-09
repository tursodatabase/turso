use crate::incremental::view::IncrementalView;

/// Simple view structure for non-materialized views
#[derive(Debug, Clone)]
pub struct View {
    pub name: String,
    pub sql: String,
    pub select_stmt: ast::Select,
    pub columns: Vec<Column>,
}

/// Type alias for regular views collection
pub type ViewsMap = HashMap<String, View>;

use crate::result::LimboResult;
use crate::storage::btree::BTreeCursor;
use crate::translate::collate::CollationSeq;
use crate::translate::plan::SelectPlan;
use crate::util::{
    module_args_from_sql, module_name_from_sql, type_from_name, IOExt, UnparsedFromSqlIndex,
};
use crate::{util::normalize_ident, Result};
use crate::{LimboError, MvCursor, Pager, RefValue, SymbolTable, VirtualTable};
use core::fmt;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::trace;
use turso_parser::ast::{self, ColumnDefinition, Expr, Literal, SortOrder, TableOptions};
use turso_parser::{
    ast::{Cmd, CreateTableBody, QualifiedName, ResultColumn, Stmt},
    parser::Parser,
};

const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_TABLE_NAME_ALT: &str = "sqlite_master";
pub const DBSP_TABLE_PREFIX: &str = "__turso_internal_dbsp_state_";

/// Check if a table name refers to a system table that should be protected from direct writes
pub fn is_system_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    normalized == SCHEMA_TABLE_NAME || normalized == SCHEMA_TABLE_NAME_ALT
}

#[derive(Debug)]
pub struct Schema {
    pub tables: HashMap<String, Arc<Table>>,

    /// Track which tables are actually materialized views
    pub materialized_view_names: HashSet<String>,
    /// Store original SQL for materialized views (for .schema command)
    pub materialized_view_sql: HashMap<String, String>,
    /// The incremental view objects (DBSP circuits)
    pub incremental_views: HashMap<String, Arc<Mutex<IncrementalView>>>,

    pub views: ViewsMap,

    /// table_name to list of indexes for the table
    pub indexes: HashMap<String, Vec<Arc<Index>>>,
    pub has_indexes: std::collections::HashSet<String>,
    pub indexes_enabled: bool,
    pub schema_version: u32,

    /// Mapping from table names to the materialized views that depend on them
    pub table_to_materialized_views: HashMap<String, Vec<String>>,
}

impl Schema {
    pub fn new(indexes_enabled: bool) -> Self {
        let mut tables: HashMap<String, Arc<Table>> = HashMap::new();
        let has_indexes = std::collections::HashSet::new();
        let indexes: HashMap<String, Vec<Arc<Index>>> = HashMap::new();
        #[allow(clippy::arc_with_non_send_sync)]
        tables.insert(
            SCHEMA_TABLE_NAME.to_string(),
            Arc::new(Table::BTree(sqlite_schema_table().into())),
        );
        for function in VirtualTable::builtin_functions() {
            tables.insert(
                function.name.to_owned(),
                Arc::new(Table::Virtual(Arc::new((*function).clone()))),
            );
        }
        let materialized_view_names = HashSet::new();
        let materialized_view_sql = HashMap::new();
        let incremental_views = HashMap::new();
        let views: ViewsMap = HashMap::new();
        let table_to_materialized_views: HashMap<String, Vec<String>> = HashMap::new();
        Self {
            tables,
            materialized_view_names,
            materialized_view_sql,
            incremental_views,
            views,
            indexes,
            has_indexes,
            indexes_enabled,
            schema_version: 0,
            table_to_materialized_views,
        }
    }

    pub fn is_unique_idx_name(&self, name: &str) -> bool {
        !self
            .indexes
            .iter()
            .any(|idx| idx.1.iter().any(|i| i.name == name))
    }
    pub fn add_materialized_view(&mut self, view: IncrementalView, table: Arc<Table>, sql: String) {
        let name = normalize_ident(view.name());

        // Add to tables (so it appears as a regular table)
        self.tables.insert(name.clone(), table);

        // Track that this is a materialized view
        self.materialized_view_names.insert(name.clone());
        self.materialized_view_sql.insert(name.clone(), sql);

        // Store the incremental view (DBSP circuit)
        self.incremental_views
            .insert(name, Arc::new(Mutex::new(view)));
    }

    pub fn get_materialized_view(&self, name: &str) -> Option<Arc<Mutex<IncrementalView>>> {
        let name = normalize_ident(name);
        self.incremental_views.get(&name).cloned()
    }

    pub fn is_materialized_view(&self, name: &str) -> bool {
        let name = normalize_ident(name);
        self.materialized_view_names.contains(&name)
    }

    pub fn remove_view(&mut self, name: &str) -> Result<()> {
        let name = normalize_ident(name);

        if self.views.contains_key(&name) {
            self.views.remove(&name);
            Ok(())
        } else if self.materialized_view_names.contains(&name) {
            // Remove from tables
            self.tables.remove(&name);

            // Remove from materialized view tracking
            self.materialized_view_names.remove(&name);
            self.materialized_view_sql.remove(&name);
            self.incremental_views.remove(&name);

            // Remove from table_to_materialized_views dependencies
            for views in self.table_to_materialized_views.values_mut() {
                views.retain(|v| v != &name);
            }

            Ok(())
        } else {
            Err(crate::LimboError::ParseError(format!(
                "no such view: {name}"
            )))
        }
    }

    /// Register that a materialized view depends on a table
    pub fn add_materialized_view_dependency(&mut self, table_name: &str, view_name: &str) {
        let table_name = normalize_ident(table_name);
        let view_name = normalize_ident(view_name);

        self.table_to_materialized_views
            .entry(table_name)
            .or_default()
            .push(view_name);
    }

    /// Get all materialized views that depend on a given table
    pub fn get_dependent_materialized_views(&self, table_name: &str) -> Vec<String> {
        let table_name = normalize_ident(table_name);
        self.table_to_materialized_views
            .get(&table_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Add a regular (non-materialized) view
    pub fn add_view(&mut self, view: View) {
        let name = normalize_ident(&view.name);
        self.views.insert(name, view);
    }

    /// Get a regular view by name
    pub fn get_view(&self, name: &str) -> Option<&View> {
        let name = normalize_ident(name);
        self.views.get(&name)
    }

    pub fn add_btree_table(&mut self, table: Arc<BTreeTable>) {
        let name = normalize_ident(&table.name);
        self.tables.insert(name, Table::BTree(table).into());
    }

    pub fn add_virtual_table(&mut self, table: Arc<VirtualTable>) {
        let name = normalize_ident(&table.name);
        self.tables.insert(name, Table::Virtual(table).into());
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        let name = normalize_ident(name);
        let name = if name.eq_ignore_ascii_case(SCHEMA_TABLE_NAME_ALT) {
            SCHEMA_TABLE_NAME
        } else {
            &name
        };
        self.tables.get(name).cloned()
    }

    pub fn remove_table(&mut self, table_name: &str) {
        let name = normalize_ident(table_name);
        self.tables.remove(&name);

        // If this was a materialized view, also clean up the metadata
        if self.materialized_view_names.remove(&name) {
            self.incremental_views.remove(&name);
            self.materialized_view_sql.remove(&name);
        }
    }

    pub fn get_btree_table(&self, name: &str) -> Option<Arc<BTreeTable>> {
        let name = normalize_ident(name);
        if let Some(table) = self.tables.get(&name) {
            table.btree()
        } else {
            None
        }
    }

    pub fn add_index(&mut self, index: Arc<Index>) {
        let table_name = normalize_ident(&index.table_name);
        self.indexes
            .entry(table_name)
            .or_default()
            .push(index.clone())
    }

    pub fn get_indices(&self, table_name: &str) -> &[Arc<Index>] {
        let name = normalize_ident(table_name);
        self.indexes
            .get(&name)
            .map_or_else(|| &[] as &[Arc<Index>], |v| v.as_slice())
    }

    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<&Arc<Index>> {
        let name = normalize_ident(table_name);
        self.indexes
            .get(&name)?
            .iter()
            .find(|index| index.name == index_name)
    }

    pub fn remove_indices_for_table(&mut self, table_name: &str) {
        let name = normalize_ident(table_name);
        self.indexes.remove(&name);
    }

    pub fn remove_index(&mut self, idx: &Index) {
        let name = normalize_ident(&idx.table_name);
        self.indexes
            .get_mut(&name)
            .expect("Must have the index")
            .retain_mut(|other_idx| other_idx.name != idx.name);
    }

    pub fn table_has_indexes(&self, table_name: &str) -> bool {
        let name = normalize_ident(table_name);
        self.has_indexes.contains(&name)
    }

    pub fn table_set_has_index(&mut self, table_name: &str) {
        self.has_indexes.insert(table_name.to_string());
    }

    pub fn indexes_enabled(&self) -> bool {
        self.indexes_enabled
    }

    /// Update [Schema] by scanning the first root page (sqlite_schema)
    pub fn make_from_btree(
        &mut self,
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        syms: &SymbolTable,
    ) -> Result<()> {
        let mut cursor = BTreeCursor::new_table(mv_cursor, pager.clone(), 1, 10);

        let mut from_sql_indexes = Vec::with_capacity(10);
        let mut automatic_indices: HashMap<String, Vec<(String, usize)>> =
            HashMap::with_capacity(10);

        // Store DBSP state table root pages: view_name -> dbsp_state_root_page
        let mut dbsp_state_roots: HashMap<String, usize> = HashMap::new();
        // Store materialized view info (SQL and root page) for later creation
        let mut materialized_view_info: HashMap<String, (String, usize)> = HashMap::new();

        if matches!(pager.begin_read_tx()?, LimboResult::Busy) {
            return Err(LimboError::Busy);
        }

        pager.io.block(|| cursor.rewind())?;

        loop {
            let Some(row) = pager.io.block(|| cursor.record())? else {
                break;
            };

            let mut record_cursor = cursor.record_cursor.borrow_mut();
            let ty_value = record_cursor.get_value(&row, 0)?;
            let RefValue::Text(ty) = ty_value else {
                return Err(LimboError::ConversionError("Expected text value".into()));
            };
            match ty.as_str() {
                "table" => {
                    let root_page_value = record_cursor.get_value(&row, 3)?;
                    let RefValue::Integer(root_page) = root_page_value else {
                        return Err(LimboError::ConversionError("Expected integer value".into()));
                    };
                    let sql_value = record_cursor.get_value(&row, 4)?;
                    let RefValue::Text(sql_text) = sql_value else {
                        return Err(LimboError::ConversionError("Expected text value".into()));
                    };
                    let sql = sql_text.as_str();
                    let create_virtual = "create virtual";
                    if root_page == 0
                        && sql[0..create_virtual.len()].eq_ignore_ascii_case(create_virtual)
                    {
                        let name_value = record_cursor.get_value(&row, 1)?;
                        let RefValue::Text(name_text) = name_value else {
                            return Err(LimboError::ConversionError("Expected text value".into()));
                        };
                        let name = name_text.as_str();

                        // a virtual table is found in the sqlite_schema, but it's no
                        // longer in the in-memory schema. We need to recreate it if
                        // the module is loaded in the symbol table.
                        let vtab = if let Some(vtab) = syms.vtabs.get(name) {
                            Arc::new((**vtab).clone())
                        } else {
                            let mod_name = module_name_from_sql(sql)?;
                            let vtab_rc = crate::VirtualTable::table(
                                Some(name),
                                mod_name,
                                module_args_from_sql(sql)?,
                                syms,
                            )?;
                            Arc::new((*vtab_rc).clone())
                        };
                        self.add_virtual_table(vtab);
                        continue;
                    }

                    let table = BTreeTable::from_sql(sql, root_page as usize)?;

                    // Check if this is a DBSP state table
                    if table.name.starts_with(DBSP_TABLE_PREFIX) {
                        // Extract the view name from _dbsp_state_<viewname>
                        let view_name = table
                            .name
                            .strip_prefix(DBSP_TABLE_PREFIX)
                            .unwrap()
                            .to_string();
                        dbsp_state_roots.insert(view_name, root_page as usize);
                    }

                    self.add_btree_table(Arc::new(table));
                }
                "index" => {
                    let root_page_value = record_cursor.get_value(&row, 3)?;
                    let RefValue::Integer(root_page) = root_page_value else {
                        return Err(LimboError::ConversionError("Expected integer value".into()));
                    };
                    match record_cursor.get_value(&row, 4) {
                        Ok(RefValue::Text(sql_text)) => {
                            let table_name_value = record_cursor.get_value(&row, 2)?;
                            let RefValue::Text(table_name_text) = table_name_value else {
                                return Err(LimboError::ConversionError(
                                    "Expected text value".into(),
                                ));
                            };

                            from_sql_indexes.push(UnparsedFromSqlIndex {
                                table_name: table_name_text.as_str().to_string(),
                                root_page: root_page as usize,
                                sql: sql_text.as_str().to_string(),
                            });
                        }
                        _ => {
                            let index_name_value = record_cursor.get_value(&row, 1)?;
                            let RefValue::Text(index_name_text) = index_name_value else {
                                return Err(LimboError::ConversionError(
                                    "Expected text value".into(),
                                ));
                            };

                            let table_name_value = record_cursor.get_value(&row, 2)?;
                            let RefValue::Text(table_name_text) = table_name_value else {
                                return Err(LimboError::ConversionError(
                                    "Expected text value".into(),
                                ));
                            };

                            match automatic_indices.entry(table_name_text.as_str().to_string()) {
                                Entry::Vacant(e) => {
                                    e.insert(vec![(
                                        index_name_text.as_str().to_string(),
                                        root_page as usize,
                                    )]);
                                }
                                Entry::Occupied(mut e) => {
                                    e.get_mut().push((
                                        index_name_text.as_str().to_string(),
                                        root_page as usize,
                                    ));
                                }
                            }
                        }
                    }
                }
                "view" => {
                    let name_value = record_cursor.get_value(&row, 1)?;
                    let RefValue::Text(name_text) = name_value else {
                        return Err(LimboError::ConversionError("Expected text value".into()));
                    };
                    let name = name_text.as_str();

                    // Get the root page (column 3) to determine if this is a materialized view
                    // Regular views have rootpage = 0, materialized views have rootpage != 0
                    let root_page_value = record_cursor.get_value(&row, 3)?;
                    let RefValue::Integer(root_page_int) = root_page_value else {
                        return Err(LimboError::ConversionError("Expected integer value".into()));
                    };
                    let root_page = root_page_int as usize;

                    let sql_value = record_cursor.get_value(&row, 4)?;
                    let RefValue::Text(sql_text) = sql_value else {
                        return Err(LimboError::ConversionError("Expected text value".into()));
                    };
                    let sql = sql_text.as_str();

                    // Parse the SQL to determine if it's a regular or materialized view
                    let mut parser = Parser::new(sql.as_bytes());
                    if let Ok(Some(Cmd::Stmt(stmt))) = parser.next_cmd() {
                        match stmt {
                            Stmt::CreateMaterializedView { .. } => {
                                // Store materialized view info for later creation
                                // We'll create the actual IncrementalView in a later pass
                                // when we have both the main root page and DBSP state root
                                let view_name = name.to_string();
                                materialized_view_info
                                    .insert(view_name, (sql.to_string(), root_page));
                            }
                            Stmt::CreateView {
                                view_name: _,
                                columns: column_names,
                                select,
                                ..
                            } => {
                                // Extract actual columns from the SELECT statement
                                let view_columns = crate::util::extract_view_columns(&select, self);

                                // If column names were provided in CREATE VIEW (col1, col2, ...),
                                // use them to rename the columns
                                let mut final_columns = view_columns;
                                for (i, indexed_col) in column_names.iter().enumerate() {
                                    if let Some(col) = final_columns.get_mut(i) {
                                        col.name = Some(indexed_col.col_name.to_string());
                                    }
                                }

                                // Create regular view
                                let view = View {
                                    name: name.to_string(),
                                    sql: sql.to_string(),
                                    select_stmt: select,
                                    columns: final_columns,
                                };
                                self.add_view(view);
                            }
                            _ => {}
                        }
                    }
                }

                _ => {}
            };
            drop(record_cursor);
            drop(row);

            pager.io.block(|| cursor.next())?;
        }

        pager.end_read_tx()?;

        for unparsed_sql_from_index in from_sql_indexes {
            if !self.indexes_enabled() {
                self.table_set_has_index(&unparsed_sql_from_index.table_name);
            } else {
                let table = self
                    .get_btree_table(&unparsed_sql_from_index.table_name)
                    .unwrap();
                let index = Index::from_sql(
                    &unparsed_sql_from_index.sql,
                    unparsed_sql_from_index.root_page,
                    table.as_ref(),
                )?;
                self.add_index(Arc::new(index));
            }
        }

        for automatic_index in automatic_indices {
            if !self.indexes_enabled() {
                self.table_set_has_index(&automatic_index.0);
            } else {
                let table = self.get_btree_table(&automatic_index.0).unwrap();
                let ret_index = Index::automatic_from_primary_key_and_unique(
                    table.as_ref(),
                    automatic_index.1,
                )?;
                for index in ret_index {
                    self.add_index(Arc::new(index));
                }
            }
        }

        // Third pass: Create materialized views now that we have both root pages
        for (view_name, (sql, main_root)) in materialized_view_info {
            // Look up the DBSP state root for this view - must exist for materialized views
            let dbsp_state_root = dbsp_state_roots.get(&view_name).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "Materialized view {view_name} is missing its DBSP state table"
                ))
            })?;

            // Create the IncrementalView with both root pages
            let incremental_view =
                IncrementalView::from_sql(&sql, self, main_root, *dbsp_state_root)?;
            let referenced_tables = incremental_view.get_referenced_table_names();

            // Create a BTreeTable for the materialized view
            let table = Arc::new(Table::BTree(Arc::new(BTreeTable {
                name: view_name.clone(),
                root_page: main_root,
                columns: incremental_view.columns.clone(),
                primary_key_columns: Vec::new(),
                has_rowid: true,
                is_strict: false,
                unique_sets: None,
            })));

            self.add_materialized_view(incremental_view, table, sql);

            // Register dependencies
            for table_name in referenced_tables {
                self.add_materialized_view_dependency(&table_name, &view_name);
            }
        }

        Ok(())
    }
}

impl Clone for Schema {
    /// Cloning a `Schema` requires deep cloning of all internal tables and indexes, even though they are wrapped in `Arc`.
    /// Simply copying the `Arc` pointers would result in multiple `Schema` instances sharing the same underlying tables and indexes,
    /// which could lead to panics or data races if any instance attempts to modify them.
    /// To ensure each `Schema` is independent and safe to modify, we clone the underlying data for all tables and indexes.
    fn clone(&self) -> Self {
        let tables = self
            .tables
            .iter()
            .map(|(name, table)| match table.deref() {
                Table::BTree(table) => {
                    let table = Arc::deref(table);
                    (
                        name.clone(),
                        Arc::new(Table::BTree(Arc::new(table.clone()))),
                    )
                }
                Table::Virtual(table) => {
                    let table = Arc::deref(table);
                    (
                        name.clone(),
                        Arc::new(Table::Virtual(Arc::new(table.clone()))),
                    )
                }
                Table::FromClauseSubquery(from_clause_subquery) => (
                    name.clone(),
                    Arc::new(Table::FromClauseSubquery(from_clause_subquery.clone())),
                ),
            })
            .collect();
        let indexes = self
            .indexes
            .iter()
            .map(|(name, indexes)| {
                let indexes = indexes
                    .iter()
                    .map(|index| Arc::new((**index).clone()))
                    .collect();
                (name.clone(), indexes)
            })
            .collect();
        let materialized_view_names = self.materialized_view_names.clone();
        let materialized_view_sql = self.materialized_view_sql.clone();
        let incremental_views = self
            .incremental_views
            .iter()
            .map(|(name, view)| (name.clone(), view.clone()))
            .collect();
        let views = self.views.clone();
        Self {
            tables,
            materialized_view_names,
            materialized_view_sql,
            incremental_views,
            views,
            indexes,
            has_indexes: self.has_indexes.clone(),
            indexes_enabled: self.indexes_enabled,
            schema_version: self.schema_version,
            table_to_materialized_views: self.table_to_materialized_views.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Table {
    BTree(Arc<BTreeTable>),
    Virtual(Arc<VirtualTable>),
    FromClauseSubquery(FromClauseSubquery),
}

impl Table {
    pub fn get_root_page(&self) -> usize {
        match self {
            Table::BTree(table) => table.root_page,
            Table::Virtual(_) => unimplemented!(),
            Table::FromClauseSubquery(_) => unimplemented!(),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Self::BTree(table) => &table.name,
            Self::Virtual(table) => &table.name,
            Self::FromClauseSubquery(from_clause_subquery) => &from_clause_subquery.name,
        }
    }

    pub fn get_column_at(&self, index: usize) -> Option<&Column> {
        match self {
            Self::BTree(table) => table.columns.get(index),
            Self::Virtual(table) => table.columns.get(index),
            Self::FromClauseSubquery(from_clause_subquery) => {
                from_clause_subquery.columns.get(index)
            }
        }
    }

    /// Returns the column position and column for a given column name.
    pub fn get_column_by_name(&self, name: &str) -> Option<(usize, &Column)> {
        let name = normalize_ident(name);
        match self {
            Self::BTree(table) => table.get_column(name.as_str()),
            Self::Virtual(table) => table
                .columns
                .iter()
                .enumerate()
                .find(|(_, col)| col.name.as_ref() == Some(&name)),
            Self::FromClauseSubquery(from_clause_subquery) => from_clause_subquery
                .columns
                .iter()
                .enumerate()
                .find(|(_, col)| col.name.as_ref() == Some(&name)),
        }
    }

    pub fn columns(&self) -> &Vec<Column> {
        match self {
            Self::BTree(table) => &table.columns,
            Self::Virtual(table) => &table.columns,
            Self::FromClauseSubquery(from_clause_subquery) => &from_clause_subquery.columns,
        }
    }

    pub fn btree(&self) -> Option<Arc<BTreeTable>> {
        match self {
            Self::BTree(table) => Some(table.clone()),
            Self::Virtual(_) => None,
            Self::FromClauseSubquery(_) => None,
        }
    }

    pub fn virtual_table(&self) -> Option<Arc<VirtualTable>> {
        match self {
            Self::Virtual(table) => Some(table.clone()),
            _ => None,
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BTree(a), Self::BTree(b)) => Arc::ptr_eq(a, b),
            (Self::Virtual(a), Self::Virtual(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BTreeTable {
    pub root_page: usize,
    pub name: String,
    pub primary_key_columns: Vec<(String, SortOrder)>,
    pub columns: Vec<Column>,
    pub has_rowid: bool,
    pub is_strict: bool,
    pub unique_sets: Option<Vec<Vec<(String, SortOrder)>>>,
}

impl BTreeTable {
    pub fn get_rowid_alias_column(&self) -> Option<(usize, &Column)> {
        if self.primary_key_columns.len() == 1 {
            let (idx, col) = self.get_column(&self.primary_key_columns[0].0)?;
            if self.column_is_rowid_alias(col) {
                return Some((idx, col));
            }
        }
        None
    }

    pub fn column_is_rowid_alias(&self, col: &Column) -> bool {
        col.is_rowid_alias
    }

    /// Returns the column position and column for a given column name.
    /// Returns None if the column name is not found.
    /// E.g. if table is CREATE TABLE t (a, b, c)
    /// then get_column("b") returns (1, &Column { .. })
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        let name = normalize_ident(name);

        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name.as_ref() == Some(&name))
    }

    pub fn from_sql(sql: &str, root_page: usize) -> Result<BTreeTable> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateTable { tbl_name, body, .. })) => {
                create_table(tbl_name, body, root_page)
            }
            _ => unreachable!("Expected CREATE TABLE statement"),
        }
    }

    /// Reconstruct the SQL for the table.
    /// FIXME: this makes us incompatible with SQLite since sqlite stores the user-provided SQL as is in
    /// `sqlite_schema.sql`
    /// For example, if a user creates a table like: `CREATE TABLE t              (x)`, we store it as
    /// `CREATE TABLE t (x)`, whereas sqlite stores it with the original extra whitespace.
    pub fn to_sql(&self) -> String {
        let mut sql = format!("CREATE TABLE {} (", self.name);
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }

            // we need to wrap the column name in square brackets if it contains special characters
            let column_name = column.name.as_ref().expect("column name is None");
            if identifier_contains_special_chars(column_name) {
                sql.push('[');
                sql.push_str(column_name);
                sql.push(']');
            } else {
                sql.push_str(column_name);
            }

            if !column.ty_str.is_empty() {
                sql.push(' ');
                sql.push_str(&column.ty_str);
            }

            if column.unique {
                sql.push_str(" UNIQUE");
            }

            if column.primary_key {
                sql.push_str(" PRIMARY KEY");
            }

            if let Some(default) = &column.default {
                sql.push_str(" DEFAULT ");
                sql.push_str(&default.to_string());
            }
        }
        sql.push(')');
        sql
    }

    pub fn column_collations(&self) -> Vec<Option<CollationSeq>> {
        self.columns.iter().map(|column| column.collation).collect()
    }
}

fn identifier_contains_special_chars(name: &str) -> bool {
    name.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PseudoCursorType {
    pub column_count: usize,
}

impl PseudoCursorType {
    pub fn new() -> Self {
        Self { column_count: 0 }
    }

    pub fn new_with_columns(columns: impl AsRef<[Column]>) -> Self {
        Self {
            column_count: columns.as_ref().len(),
        }
    }
}

/// A derived table from a FROM clause subquery.
#[derive(Debug, Clone)]
pub struct FromClauseSubquery {
    /// The name of the derived table; uses the alias if available.
    pub name: String,
    /// The query plan for the derived table.
    pub plan: Box<SelectPlan>,
    /// The columns of the derived table.
    pub columns: Vec<Column>,
    /// The start register for the result columns of the derived table;
    /// must be set before data is read from it.
    pub result_columns_start_reg: Option<usize>,
}

#[derive(Debug, Eq)]
struct UniqueColumnProps {
    column_name: String,
    order: SortOrder,
}

impl PartialEq for UniqueColumnProps {
    fn eq(&self, other: &Self) -> bool {
        self.column_name.eq(&other.column_name)
    }
}

impl PartialOrd for UniqueColumnProps {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UniqueColumnProps {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.column_name.cmp(&other.column_name)
    }
}

fn create_table(
    tbl_name: QualifiedName,
    body: CreateTableBody,
    root_page: usize,
) -> Result<BTreeTable> {
    let table_name = normalize_ident(tbl_name.name.as_str());
    trace!("Creating table {}", table_name);
    let mut has_rowid = true;
    let mut primary_key_columns = vec![];
    let mut cols = vec![];
    let is_strict: bool;
    // BtreeSet here to preserve order of inserted keys
    let mut unique_sets: Vec<BTreeSet<UniqueColumnProps>> = vec![];
    match body {
        CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        } => {
            is_strict = options.contains(TableOptions::STRICT);
            for c in constraints {
                if let ast::TableConstraint::PrimaryKey { columns, .. } = c.constraint {
                    for column in columns {
                        let col_name = match column.expr.as_ref() {
                            Expr::Id(id) => normalize_ident(id.as_str()),
                            Expr::Literal(Literal::String(value)) => {
                                value.trim_matches('\'').to_owned()
                            }
                            _ => {
                                todo!("Unsupported primary key expression");
                            }
                        };
                        primary_key_columns
                            .push((col_name, column.order.unwrap_or(SortOrder::Asc)));
                    }
                } else if let ast::TableConstraint::Unique {
                    columns,
                    conflict_clause,
                } = c.constraint
                {
                    if conflict_clause.is_some() {
                        unimplemented!("ON CONFLICT not implemented");
                    }
                    let unique_set = columns
                        .into_iter()
                        .map(|column| {
                            let column_name = match column.expr.as_ref() {
                                Expr::Id(id) => normalize_ident(id.as_str()),
                                _ => {
                                    todo!("Unsupported unique expression");
                                }
                            };
                            UniqueColumnProps {
                                column_name,
                                order: column.order.unwrap_or(SortOrder::Asc),
                            }
                        })
                        .collect();
                    unique_sets.push(unique_set);
                }
            }
            for ast::ColumnDefinition {
                col_name,
                col_type,
                constraints,
            } in &columns
            {
                let name = col_name.as_str().to_string();
                // Regular sqlite tables have an integer rowid that uniquely identifies a row.
                // Even if you create a table with a column e.g. 'id INT PRIMARY KEY', there will still
                // be a separate hidden rowid, and the 'id' column will have a separate index built for it.
                //
                // However:
                // A column defined as exactly INTEGER PRIMARY KEY is a rowid alias, meaning that the rowid
                // and the value of this column are the same.
                // https://www.sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
                let ty_str = col_type
                    .as_ref()
                    .cloned()
                    .map(|ast::Type { name, .. }| name.clone())
                    .unwrap_or_default();

                let mut typename_exactly_integer = false;
                let ty = match col_type {
                    Some(data_type) => {
                        let (ty, ei) = type_from_name(&data_type.name);
                        typename_exactly_integer = ei;
                        ty
                    }
                    None => Type::Null,
                };

                let mut default = None;
                let mut primary_key = false;
                let mut notnull = false;
                let mut order = SortOrder::Asc;
                let mut unique = false;
                let mut collation = None;
                for c_def in constraints {
                    match c_def.constraint {
                        ast::ColumnConstraint::PrimaryKey { order: o, .. } => {
                            primary_key = true;
                            if let Some(o) = o {
                                order = o;
                            }
                        }
                        ast::ColumnConstraint::NotNull { nullable, .. } => {
                            notnull = !nullable;
                        }
                        ast::ColumnConstraint::Default(ref expr) => default = Some(expr),
                        // TODO: for now we don't check Resolve type of unique
                        ast::ColumnConstraint::Unique(on_conflict) => {
                            if on_conflict.is_some() {
                                unimplemented!("ON CONFLICT not implemented");
                            }
                            unique = true;
                        }
                        ast::ColumnConstraint::Collate { ref collation_name } => {
                            collation = Some(CollationSeq::new(collation_name.as_str())?);
                        }
                        _ => {}
                    }
                }

                if primary_key {
                    primary_key_columns.push((name.clone(), order));
                } else if primary_key_columns
                    .iter()
                    .any(|(col_name, _)| col_name == &name)
                {
                    primary_key = true;
                }

                cols.push(Column {
                    name: Some(normalize_ident(&name)),
                    ty,
                    ty_str,
                    primary_key,
                    is_rowid_alias: typename_exactly_integer && primary_key,
                    notnull,
                    default: default.cloned(),
                    unique,
                    collation,
                    hidden: false,
                });
            }
            if options.contains(TableOptions::WITHOUT_ROWID) {
                has_rowid = false;
            }
        }
        CreateTableBody::AsSelect(_) => todo!(),
    };
    // flip is_rowid_alias back to false if the table has multiple primary keys
    // or if the table has no rowid
    if !has_rowid || primary_key_columns.len() > 1 {
        for col in cols.iter_mut() {
            col.is_rowid_alias = false;
        }
    }
    Ok(BTreeTable {
        root_page,
        name: table_name,
        has_rowid,
        primary_key_columns,
        columns: cols,
        is_strict,
        unique_sets: if unique_sets.is_empty() {
            None
        } else {
            // Sort first so that dedup operation removes all duplicates
            unique_sets.dedup();
            Some(
                unique_sets
                    .into_iter()
                    .map(|set| {
                        set.into_iter()
                            .map(|UniqueColumnProps { column_name, order }| (column_name, order))
                            .collect()
                    })
                    .collect(),
            )
        },
    })
}

pub fn _build_pseudo_table(columns: &[ResultColumn]) -> PseudoCursorType {
    let table = PseudoCursorType::new();
    for column in columns {
        match column {
            ResultColumn::Expr(expr, _as_name) => {
                todo!("unsupported expression {:?}", expr);
            }
            ResultColumn::Star => {
                todo!();
            }
            ResultColumn::TableStar(_) => {
                todo!();
            }
        }
    }
    table
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: Option<String>,
    pub ty: Type,
    // many sqlite operations like table_info retain the original string
    pub ty_str: String,
    pub primary_key: bool,
    pub is_rowid_alias: bool,
    pub notnull: bool,
    pub default: Option<Box<Expr>>,
    pub unique: bool,
    pub collation: Option<CollationSeq>,
    pub hidden: bool,
}

impl Column {
    pub fn affinity(&self) -> Affinity {
        affinity(&self.ty_str)
    }
}

// TODO: This might replace some of util::columns_from_create_table_body
impl From<&ColumnDefinition> for Column {
    fn from(value: &ColumnDefinition) -> Self {
        let name = value.col_name.as_str();

        let mut default = None;
        let mut notnull = false;
        let mut primary_key = false;
        let mut unique = false;
        let mut collation = None;

        for ast::NamedColumnConstraint { constraint, .. } in &value.constraints {
            match constraint {
                ast::ColumnConstraint::PrimaryKey { .. } => primary_key = true,
                ast::ColumnConstraint::NotNull { .. } => notnull = true,
                ast::ColumnConstraint::Unique(..) => unique = true,
                ast::ColumnConstraint::Default(expr) => {
                    default.replace(expr.clone());
                }
                ast::ColumnConstraint::Collate { collation_name } => {
                    collation.replace(
                        CollationSeq::new(collation_name.as_str())
                            .expect("collation should have been set correctly in create table"),
                    );
                }
                _ => {}
            };
        }

        let ty = match value.col_type {
            Some(ref data_type) => type_from_name(&data_type.name).0,
            None => Type::Null,
        };

        let ty_str = value
            .col_type
            .as_ref()
            .map(|t| t.name.to_string())
            .unwrap_or_default();

        let hidden = ty_str.contains("HIDDEN");

        Column {
            name: Some(normalize_ident(name)),
            ty,
            default,
            notnull,
            ty_str,
            primary_key,
            is_rowid_alias: primary_key && matches!(ty, Type::Integer),
            unique,
            collation,
            hidden,
        }
    }
}

/// 3.1. Determination Of Column Affinity
/// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
///
/// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
///
/// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
///
/// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
///
/// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
///
/// Otherwise, the affinity is NUMERIC.
///
/// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
pub fn affinity(datatype: &str) -> Affinity {
    // Note: callers of this function must ensure that the datatype is uppercase.
    // Rule 1: INT -> INTEGER affinity
    if datatype.contains("INT") {
        return Affinity::Integer;
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if datatype.contains("CHAR") || datatype.contains("CLOB") || datatype.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if datatype.contains("BLOB") || datatype.is_empty() || datatype.contains("ANY") {
        return Affinity::Blob;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if datatype.contains("REAL") || datatype.contains("FLOA") || datatype.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::Numeric
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Null,
    Text,
    Numeric,
    Integer,
    Real,
    Blob,
}

/// # SQLite Column Type Affinities
///
/// Each column in an SQLite 3 database is assigned one of the following type affinities:
///
/// - **TEXT**
/// - **NUMERIC**
/// - **INTEGER**
/// - **REAL**
/// - **BLOB**
///
/// > **Note:** Historically, the "BLOB" type affinity was called "NONE". However, this term was renamed to avoid confusion with "no affinity".
///
/// ## Affinity Descriptions
///
/// ### **TEXT**
/// - Stores data using the NULL, TEXT, or BLOB storage classes.
/// - Numerical data inserted into a column with TEXT affinity is converted into text form before being stored.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col TEXT);
///   INSERT INTO example (col) VALUES (123); -- Stored as '123' (text)
///   SELECT typeof(col) FROM example; -- Returns 'text'
///   ```
///
/// ### **NUMERIC**
/// - Can store values using all five storage classes.
/// - Text data is converted to INTEGER or REAL (in that order of preference) if it is a well-formed integer or real literal.
/// - If the text represents an integer too large for a 64-bit signed integer, it is converted to REAL.
/// - If the text is not a well-formed literal, it is stored as TEXT.
/// - Hexadecimal integer literals are stored as TEXT for historical compatibility.
/// - Floating-point values that can be exactly represented as integers are converted to integers.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col NUMERIC);
///   INSERT INTO example (col) VALUES ('3.0e+5'); -- Stored as 300000 (integer)
///   SELECT typeof(col) FROM example; -- Returns 'integer'
///   ```
///
/// ### **INTEGER**
/// - Behaves like NUMERIC affinity but differs in `CAST` expressions.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col INTEGER);
///   INSERT INTO example (col) VALUES (4.0); -- Stored as 4 (integer)
///   SELECT typeof(col) FROM example; -- Returns 'integer'
///   ```
///
/// ### **REAL**
/// - Similar to NUMERIC affinity but forces integer values into floating-point representation.
/// - **Optimization:** Small floating-point values with no fractional component may be stored as integers on disk to save space. This is invisible at the SQL level.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col REAL);
///   INSERT INTO example (col) VALUES (4); -- Stored as 4.0 (real)
///   SELECT typeof(col) FROM example; -- Returns 'real'
///   ```
///
/// ### **BLOB**
/// - Does not prefer any storage class.
/// - No coercion is performed between storage classes.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col BLOB);
///   INSERT INTO example (col) VALUES (x'1234'); -- Stored as a binary blob
///   SELECT typeof(col) FROM example; -- Returns 'blob'
///   ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

pub const SQLITE_AFF_NONE: char = 'A'; // Historically called NONE, but it's the same as BLOB
pub const SQLITE_AFF_TEXT: char = 'B';
pub const SQLITE_AFF_NUMERIC: char = 'C';
pub const SQLITE_AFF_INTEGER: char = 'D';
pub const SQLITE_AFF_REAL: char = 'E';

impl Affinity {
    /// This is meant to be used in opcodes like Eq, which state:
    ///
    /// "The SQLITE_AFF_MASK portion of P5 must be an affinity character - SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth.
    /// An attempt is made to coerce both inputs according to this affinity before the comparison is made.
    /// If the SQLITE_AFF_MASK is 0x00, then numeric affinity is used.
    /// Note that the affinity conversions are stored back into the input registers P1 and P3.
    /// So this opcode can cause persistent changes to registers P1 and P3.""
    pub fn aff_mask(&self) -> char {
        match self {
            Affinity::Integer => SQLITE_AFF_INTEGER,
            Affinity::Text => SQLITE_AFF_TEXT,
            Affinity::Blob => SQLITE_AFF_NONE,
            Affinity::Real => SQLITE_AFF_REAL,
            Affinity::Numeric => SQLITE_AFF_NUMERIC,
        }
    }

    pub fn from_char(char: char) -> Self {
        match char {
            SQLITE_AFF_INTEGER => Affinity::Integer,
            SQLITE_AFF_TEXT => Affinity::Text,
            SQLITE_AFF_NONE => Affinity::Blob,
            SQLITE_AFF_REAL => Affinity::Real,
            SQLITE_AFF_NUMERIC => Affinity::Numeric,
            _ => Affinity::Blob,
        }
    }

    pub fn as_char_code(&self) -> u8 {
        self.aff_mask() as u8
    }

    pub fn from_char_code(code: u8) -> Self {
        Self::from_char(code as char)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Affinity::Integer | Affinity::Real | Affinity::Numeric)
    }

    pub fn has_affinity(&self) -> bool {
        !matches!(self, Affinity::Blob)
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Null => "",
            Self::Text => "TEXT",
            Self::Numeric => "NUMERIC",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
        };
        write!(f, "{s}")
    }
}

pub fn sqlite_schema_table() -> BTreeTable {
    BTreeTable {
        root_page: 1,
        name: "sqlite_schema".to_string(),
        has_rowid: true,
        is_strict: false,
        primary_key_columns: vec![],
        columns: vec![
            Column {
                name: Some("type".to_string()),
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
            Column {
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
            Column {
                name: Some("tbl_name".to_string()),
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
            Column {
                name: Some("rootpage".to_string()),
                ty: Type::Integer,
                ty_str: "INT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
                unique: false,
                collation: None,
                hidden: false,
            },
            Column {
                name: Some("sql".to_string()),
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
        unique_sets: None,
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub root_page: usize,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub ephemeral: bool,
    /// Does the index have a rowid as the last column?
    /// This is the case for btree indexes (persistent or ephemeral) that
    /// have been created based on a table with a rowid.
    /// For example, WITHOUT ROWID tables (not supported in Limbo yet),
    /// and  SELECT DISTINCT ephemeral indexes will not have a rowid.
    pub has_rowid: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub order: SortOrder,
    /// the position of the column in the source table.
    /// for example:
    /// CREATE TABLE t (a,b,c)
    /// CREATE INDEX idx ON t(b)
    /// b.pos_in_table == 1
    pub pos_in_table: usize,
    pub collation: Option<CollationSeq>,
    pub default: Option<Box<Expr>>,
}

impl Index {
    pub fn from_sql(sql: &str, root_page: usize, table: &BTreeTable) -> Result<Index> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateIndex {
                idx_name,
                tbl_name,
                columns,
                unique,
                ..
            })) => {
                let index_name = normalize_ident(idx_name.name.as_str());
                let mut index_columns = Vec::with_capacity(columns.len());
                for col in columns.into_iter() {
                    let name = normalize_ident(&col.expr.to_string());
                    let Some((pos_in_table, _)) = table.get_column(&name) else {
                        return Err(crate::LimboError::InternalError(format!(
                            "Column {} is in index {} but not found in table {}",
                            name, index_name, table.name
                        )));
                    };
                    let (_, column) = table.get_column(&name).unwrap();
                    index_columns.push(IndexColumn {
                        name,
                        order: col.order.unwrap_or(SortOrder::Asc),
                        pos_in_table,
                        collation: column.collation,
                        default: column.default.clone(),
                    });
                }
                Ok(Index {
                    name: index_name,
                    table_name: normalize_ident(tbl_name.as_str()),
                    root_page,
                    columns: index_columns,
                    unique,
                    ephemeral: false,
                    has_rowid: table.has_rowid,
                })
            }
            _ => todo!("Expected create index statement"),
        }
    }

    /// The order of index returned should be kept the same
    ///
    /// If the order of the index returned changes, this is a breaking change
    ///
    /// In the future when we support Alter Column, we should revisit a way to make this less dependent on ordering
    pub fn automatic_from_primary_key_and_unique(
        table: &BTreeTable,
        auto_indices: Vec<(String, usize)>,
    ) -> Result<Vec<Index>> {
        assert!(!auto_indices.is_empty());

        let mut indices = Vec::with_capacity(auto_indices.len());

        // The number of auto_indices in create table should match in the number of indices we calculate in this function
        let mut auto_indices = auto_indices.into_iter();

        // TODO: see a better way to please Rust type system with iterators here
        // I wanted to just chain the iterator above but Rust type system get's messy with Iterators.
        // It would not allow me chain them even by using a core::iter::empty()
        // To circumvent this, I'm having to allocate a second Vec, and extend the other from it.
        let has_primary_key_index =
            table.get_rowid_alias_column().is_none() && !table.primary_key_columns.is_empty();
        if has_primary_key_index {
            let (index_name, root_page) = auto_indices.next().expect(
                "number of auto_indices in schema should be same number of indices calculated",
            );

            let primary_keys = table
                .primary_key_columns
                .iter()
                .map(|(col_name, order)| {
                    // Verify that each primary key column exists in the table
                    let Some((pos_in_table, _)) = table.get_column(col_name) else {
                        // This is clearly an invariant that should be maintained, so a panic seems more correct here
                        panic!(
                            "Column {} is in index {} but not found in table {}",
                            col_name, index_name, table.name
                        );
                    };

                    let (_, column) = table.get_column(col_name).unwrap();

                    IndexColumn {
                        name: normalize_ident(col_name),
                        order: *order,
                        pos_in_table,
                        collation: column.collation,
                        default: column.default.clone(),
                    }
                })
                .collect::<Vec<_>>();

            indices.push(Index {
                name: normalize_ident(index_name.as_str()),
                table_name: table.name.clone(),
                root_page,
                columns: primary_keys,
                unique: true,
                ephemeral: false,
                has_rowid: table.has_rowid,
            });
        }

        // Each unique col needs its own index
        let unique_indices = table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(pos_in_table, col)| {
                if col.unique {
                    // Unique columns in Table should always be named
                    let col_name = col.name.as_ref().unwrap();
                    if has_primary_key_index
                        && table.primary_key_columns.len() == 1
                        && &table.primary_key_columns.first().as_ref().unwrap().0 == col_name {
                            // skip unique columns that are satisfied with pk constraint
                            return None;
                    }
                    let (index_name, root_page) = auto_indices.next().expect("number of auto_indices in schema should be same number of indices calculated");
                    let (_, column) = table.get_column(col_name).unwrap();
                    Some(Index {
                        name: normalize_ident(index_name.as_str()),
                        table_name: table.name.clone(),
                        root_page,
                        columns: vec![IndexColumn {
                            name: normalize_ident(col_name),
                            order: SortOrder::Asc, // Default Sort Order
                            pos_in_table,
                            collation: column.collation,
                            default: column.default.clone(),
                        }],
                        unique: true,
                        ephemeral: false,
                        has_rowid: table.has_rowid,
                    })
                } else {
                    None
                }
            });

        indices.extend(unique_indices);

        if table.primary_key_columns.is_empty() && indices.is_empty() && table.unique_sets.is_none()
        {
            return Err(crate::LimboError::InternalError(
                "Cannot create automatic index for table without primary key or unique constraint"
                    .to_string(),
            ));
        }

        // Invariant: We should not create an automatic index on table with a single column as rowid_alias
        // and no Unique columns.
        // e.g CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);
        // If this happens, the caller incorrectly called this function
        if table.get_rowid_alias_column().is_some()
            && indices.is_empty()
            && table.unique_sets.is_none()
        {
            panic!("should not create an automatic index on table with a single column as rowid_alias and no UNIQUE columns");
        }

        if let Some(unique_sets) = table.unique_sets.as_ref() {
            let unique_set_indices = unique_sets
                .iter()
                .filter(|set| {
                    if has_primary_key_index
                        && table.primary_key_columns.len() == set.len()
                        && table
                            .primary_key_columns
                            .iter()
                            .all(|col| set.contains(col))
                    {
                        // skip unique columns that are satisfied with pk constraint
                        false
                    } else {
                        true
                    }
                })
                .map(|set| {
                    let (index_name, root_page) = auto_indices.next().expect(
                    "number of auto_indices in schema should be same number of indices calculated",
                );

                    let index_cols = set.iter().map(|(col_name, order)| {
                        let Some((pos_in_table, _)) = table.get_column(col_name) else {
                            // This is clearly an invariant that should be maintained, so a panic seems more correct here
                            panic!(
                                "Column {} is in index {} but not found in table {}",
                                col_name, index_name, table.name
                            );
                        };
                        let (_, column) = table.get_column(col_name).unwrap();
                        IndexColumn {
                            name: normalize_ident(col_name),
                            order: *order,
                            pos_in_table,
                            collation: column.collation,
                            default: column.default.clone(),
                        }
                    });
                    Index {
                        name: normalize_ident(index_name.as_str()),
                        table_name: table.name.clone(),
                        root_page,
                        columns: index_cols.collect(),
                        unique: true,
                        ephemeral: false,
                        has_rowid: table.has_rowid,
                    }
                });
            indices.extend(unique_set_indices);
        }

        if auto_indices.next().is_some() {
            panic!("number of auto_indices in schema should be same number of indices calculated");
        }

        Ok(indices)
    }

    /// Given a column position in the table, return the position in the index.
    /// Returns None if the column is not found in the index.
    /// For example, given:
    /// CREATE TABLE t (a, b, c)
    /// CREATE INDEX idx ON t(b)
    /// then column_table_pos_to_index_pos(1) returns Some(0)
    pub fn column_table_pos_to_index_pos(&self, table_pos: usize) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.pos_in_table == table_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LimboError;

    #[test]
    pub fn test_has_rowid_true() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(table.has_rowid, "has_rowid should be set to true");
        Ok(())
    }

    #[test]
    pub fn test_has_rowid_false() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(!table.has_rowid, "has_rowid should be set to false");
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_text() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a TEXT PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ has type different than INTEGER so can't be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            table.column_is_rowid_alias(column),
            "column 'a´ should be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition() -> Result<()>
    {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            table.column_is_rowid_alias(column),
            "column 'a´ should be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition_without_rowid(
    ) -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a)) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has no rowid"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_without_rowid() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has no rowid"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_inline_composite_primary_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has composite primary key"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_separate_composite_primary_key_definition() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !table.column_is_rowid_alias(column),
            "column 'a´ shouldn't be a rowid alias because table has composite primary key"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_inline_single() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT, c REAL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec![("a".to_string(), SortOrder::Asc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_inline_multiple() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY, c REAL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(column.primary_key, "column 'b' shouldn be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec![
                ("a".to_string(), SortOrder::Asc),
                ("b".to_string(), SortOrder::Asc)
            ],
            table.primary_key_columns,
            "primary key column names should be ['a', 'b']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_single() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a desc));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec![("a".to_string(), SortOrder::Desc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_multiple() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a, b desc));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(column.primary_key, "column 'b' shouldn be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec![
                ("a".to_string(), SortOrder::Asc),
                ("b".to_string(), SortOrder::Desc)
            ],
            table.primary_key_columns,
            "primary key column names should be ['a', 'b']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_single_quoted() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY('a'));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec![("a".to_string(), SortOrder::Asc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }
    #[test]
    pub fn test_primary_key_separate_single_doubly_quoted() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY("a"));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key, "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(!column.primary_key, "column 'b' shouldn't be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(!column.primary_key, "column 'c' shouldn't be a primary key");
        assert_eq!(
            vec![("a".to_string(), SortOrder::Asc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_default_value() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER DEFAULT 23);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        let default = column.default.clone().unwrap();
        assert_eq!(default.to_string(), "23");
        Ok(())
    }

    #[test]
    pub fn test_col_notnull() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER NOT NULL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.notnull);
        Ok(())
    }

    #[test]
    pub fn test_col_notnull_negative() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(!column.notnull);
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_integer() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a InTeGeR);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "InTeGeR");
        Ok(())
    }

    #[test]
    pub fn test_sqlite_schema() {
        let expected = r#"CREATE TABLE sqlite_schema (type TEXT, name TEXT, tbl_name TEXT, rootpage INT, sql TEXT)"#;
        let actual = sqlite_schema_table().to_sql();
        assert_eq!(expected, actual);
    }

    #[test]
    pub fn test_special_column_names() -> Result<()> {
        let tests = [
            ("foobar", "CREATE TABLE t (foobar TEXT)"),
            ("_table_name3", "CREATE TABLE t (_table_name3 TEXT)"),
            ("special name", "CREATE TABLE t ([special name] TEXT)"),
            ("foo&bar", "CREATE TABLE t ([foo&bar] TEXT)"),
            (" name", "CREATE TABLE t ([ name] TEXT)"),
        ];

        for (input_column_name, expected_sql) in tests {
            let sql = format!("CREATE TABLE t ([{input_column_name}] TEXT)");
            let actual = BTreeTable::from_sql(&sql, 0)?.to_sql();
            assert_eq!(expected_sql, actual);
        }

        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_automatic_index_single_column() {
        // Without composite primary keys, we should not have an automatic index on a primary key that is a rowid alias
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0).unwrap();
        let _index = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        )
        .unwrap();
    }

    #[test]
    fn test_automatic_index_composite_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut index = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        )?;

        assert!(index.len() == 1);
        let index = index.pop().unwrap();
        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));
        assert!(matches!(index.columns[1].order, SortOrder::Asc));
        Ok(())
    }

    #[test]
    fn test_automatic_index_no_primary_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let result = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        );

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LimboError::InternalError(msg) if msg.contains("without primary key")
        ));
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_automatic_index_nonexistent_column() {
        // Create a table with a primary key column that doesn't exist in the table
        let table = BTreeTable {
            root_page: 0,
            name: "t1".to_string(),
            has_rowid: true,
            is_strict: false,
            primary_key_columns: vec![("nonexistent".to_string(), SortOrder::Asc)],
            columns: vec![Column {
                name: Some("a".to_string()),
                ty: Type::Integer,
                ty_str: "INT".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
                unique: false,
                collation: None,
                hidden: false,
            }],
            unique_sets: None,
        };

        let _result = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        );
    }

    #[test]
    fn test_automatic_index_unique_column() -> Result<()> {
        let sql = r#"CREATE table t1 (x INTEGER, y INTEGER UNIQUE);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut index = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        )?;

        assert!(index.len() == 1);
        let index = index.pop().unwrap();

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "y");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));
        Ok(())
    }

    #[test]
    fn test_automatic_index_pkey_unique_column() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (x PRIMARY KEY, y UNIQUE);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let auto_indices = vec![
            ("sqlite_autoindex_t1_1".to_string(), 2),
            ("sqlite_autoindex_t1_2".to_string(), 3),
        ];
        let indices = Index::automatic_from_primary_key_and_unique(&table, auto_indices.clone())?;

        assert!(indices.len() == auto_indices.len());

        for (pos, index) in indices.iter().enumerate() {
            let (index_name, root_page) = &auto_indices[pos];
            assert_eq!(index.name, *index_name);
            assert_eq!(index.table_name, "t1");
            assert_eq!(index.root_page, *root_page);
            assert!(index.unique);
            assert_eq!(index.columns.len(), 1);
            if pos == 0 {
                assert_eq!(index.columns[0].name, "x");
            } else if pos == 1 {
                assert_eq!(index.columns[0].name, "y");
            }

            assert!(matches!(index.columns[0].order, SortOrder::Asc));
        }

        Ok(())
    }

    #[test]
    fn test_automatic_index_pkey_many_unique_columns() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a PRIMARY KEY, b UNIQUE, c, d, UNIQUE(c, d));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let auto_indices = vec![
            ("sqlite_autoindex_t1_1".to_string(), 2),
            ("sqlite_autoindex_t1_2".to_string(), 3),
            ("sqlite_autoindex_t1_2".to_string(), 4),
        ];
        let indices = Index::automatic_from_primary_key_and_unique(&table, auto_indices.clone())?;

        assert!(indices.len() == auto_indices.len());

        for (pos, index) in indices.iter().enumerate() {
            let (index_name, root_page) = &auto_indices[pos];
            assert_eq!(index.name, *index_name);
            assert_eq!(index.table_name, "t1");
            assert_eq!(index.root_page, *root_page);
            assert!(index.unique);

            if pos == 0 {
                assert_eq!(index.columns.len(), 1);
                assert_eq!(index.columns[0].name, "a");
            } else if pos == 1 {
                assert_eq!(index.columns.len(), 1);
                assert_eq!(index.columns[0].name, "b");
            } else if pos == 2 {
                assert_eq!(index.columns.len(), 2);
                assert_eq!(index.columns[0].name, "c");
                assert_eq!(index.columns[1].name, "d");
            }

            assert!(matches!(index.columns[0].order, SortOrder::Asc));
        }

        Ok(())
    }

    #[test]
    fn test_automatic_index_unique_set_dedup() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b), UNIQUE(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut index = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        )?;

        assert!(index.len() == 1);
        let index = index.pop().unwrap();

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[1].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_automatic_index_primary_key_is_unique() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a primary key unique);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut index = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        )?;

        assert!(index.len() == 1);
        let index = index.pop().unwrap();

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_automatic_index_primary_key_is_unique_and_composite() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a, b, PRIMARY KEY(a, b), UNIQUE(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut index = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![("sqlite_autoindex_t1_1".to_string(), 2)],
        )?;

        assert!(index.len() == 1);
        let index = index.pop().unwrap();

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_automatic_index_unique_and_a_pk() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a NUMERIC UNIQUE UNIQUE,  b TEXT PRIMARY KEY)"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut indexes = Index::automatic_from_primary_key_and_unique(
            &table,
            vec![
                ("sqlite_autoindex_t1_1".to_string(), 2),
                ("sqlite_autoindex_t1_2".to_string(), 3),
            ],
        )?;

        assert!(indexes.len() == 2);
        let index = indexes.pop().unwrap();
        assert_eq!(index.name, "sqlite_autoindex_t1_2");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 3);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        let index = indexes.pop().unwrap();
        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "b");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        Ok(())
    }
}
