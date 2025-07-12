use crate::result::LimboResult;
use crate::storage::btree::BTreeCursor;
use crate::translate::collate::CollationSeq;
use crate::translate::plan::SelectPlan;
use crate::types::CursorResult;
use crate::util::{module_args_from_sql, module_name_from_sql, UnparsedFromSqlIndex};
use crate::{util::normalize_ident, Result};
use crate::{LimboError, MvCursor, Pager, RefValue, SymbolTable, VirtualTable};
use core::fmt;
use fallible_iterator::FallibleIterator;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use tracing::trace;
use turso_sqlite3_parser::ast::{self, ColumnDefinition, Expr, Literal, SortOrder, TableOptions};
use turso_sqlite3_parser::{
    ast::{Cmd, CreateTableBody, QualifiedName, ResultColumn, Stmt},
    lexer::sql::Parser,
};

const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_TABLE_NAME_ALT: &str = "sqlite_master";

#[derive(Debug, Clone)]
pub struct Schema {
    pub tables: HashMap<String, Arc<Table>>,
    /// table_name to list of indexes for the table
    pub indexes: HashMap<String, Vec<Arc<Index>>>,
    pub has_indexes: std::collections::HashSet<String>,
    pub indexes_enabled: bool,
    pub schema_version: u32,
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
        Self {
            tables,
            indexes,
            has_indexes,
            indexes_enabled,
            schema_version: 0,
        }
    }

    pub fn is_unique_idx_name(&self, name: &str) -> bool {
        !self
            .indexes
            .iter()
            .any(|idx| idx.1.iter().any(|i| i.name == name))
    }

    pub fn add_btree_table(&mut self, table: Rc<BTreeTable>) {
        let name = normalize_ident(&table.name);
        self.tables.insert(name, Table::BTree(table).into());
    }

    pub fn add_virtual_table(&mut self, table: Rc<VirtualTable>) {
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
    }

    pub fn get_btree_table(&self, name: &str) -> Option<Rc<BTreeTable>> {
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
        self.has_indexes.contains(table_name)
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

        match pager.begin_read_tx()? {
            CursorResult::Ok(v) => {
                if matches!(v, LimboResult::Busy) {
                    return Err(LimboError::Busy);
                }
            }
            CursorResult::IO => pager.io.run_once()?,
        }

        match cursor.rewind()? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => pager.io.run_once()?,
        };

        loop {
            let Some(row) = (loop {
                match cursor.record()? {
                    CursorResult::Ok(v) => break v,
                    CursorResult::IO => pager.io.run_once()?,
                }
            }) else {
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
                            vtab.clone()
                        } else {
                            let mod_name = module_name_from_sql(sql)?;
                            crate::VirtualTable::table(
                                Some(name),
                                mod_name,
                                module_args_from_sql(sql)?,
                                syms,
                            )?
                        };
                        self.add_virtual_table(vtab);
                        continue;
                    }

                    let table = BTreeTable::from_sql(sql, root_page as usize)?;
                    self.add_btree_table(Rc::new(table));
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
                _ => {}
            };
            drop(record_cursor);
            drop(row);

            if matches!(cursor.next()?, CursorResult::IO) {
                pager.io.run_once()?;
            };
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

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum Table {
    BTree(Rc<BTreeTable>),
    Virtual(Rc<VirtualTable>),
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

    pub fn columns(&self) -> &Vec<Column> {
        match self {
            Self::BTree(table) => &table.columns,
            Self::Virtual(table) => &table.columns,
            Self::FromClauseSubquery(from_clause_subquery) => &from_clause_subquery.columns,
        }
    }

    pub fn btree(&self) -> Option<Rc<BTreeTable>> {
        match self {
            Self::BTree(table) => Some(table.clone()),
            Self::Virtual(_) => None,
            Self::FromClauseSubquery(_) => None,
        }
    }

    pub fn virtual_table(&self) -> Option<Rc<VirtualTable>> {
        match self {
            Self::Virtual(table) => Some(table.clone()),
            _ => None,
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BTree(a), Self::BTree(b)) => Rc::ptr_eq(a, b),
            (Self::Virtual(a), Self::Virtual(b)) => Rc::ptr_eq(a, b),
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
    /// E.g. if table is CREATE TABLE t(a, b, c)
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
        let cmd = parser.next()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateTable { tbl_name, body, .. })) => {
                create_table(tbl_name, *body, root_page)
            }
            _ => todo!("Expected CREATE TABLE statement"),
        }
    }

    pub fn to_sql(&self) -> String {
        let mut sql = format!("CREATE TABLE {} (", self.name);
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(column.name.as_ref().expect("column name is None"));

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
    let table_name = normalize_ident(&tbl_name.name.0);
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
            if let Some(constraints) = constraints {
                for c in constraints {
                    if let turso_sqlite3_parser::ast::TableConstraint::PrimaryKey {
                        columns, ..
                    } = c.constraint
                    {
                        for column in columns {
                            let col_name = match column.expr {
                                Expr::Id(id) => normalize_ident(&id.0),
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
                    } else if let turso_sqlite3_parser::ast::TableConstraint::Unique {
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
                                let column_name = match column.expr {
                                    Expr::Id(id) => normalize_ident(&id.0),
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
            }
            for (col_name, col_def) in columns {
                let name = col_name.0.to_string();
                // Regular sqlite tables have an integer rowid that uniquely identifies a row.
                // Even if you create a table with a column e.g. 'id INT PRIMARY KEY', there will still
                // be a separate hidden rowid, and the 'id' column will have a separate index built for it.
                //
                // However:
                // A column defined as exactly INTEGER PRIMARY KEY is a rowid alias, meaning that the rowid
                // and the value of this column are the same.
                // https://www.sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
                let ty_str = col_def
                    .col_type
                    .as_ref()
                    .map(|ast::Type { name, .. }| name.clone())
                    .unwrap_or_default();

                let mut typename_exactly_integer = false;
                let ty = match col_def.col_type {
                    Some(data_type) => 'ty: {
                        // https://www.sqlite.org/datatype3.html
                        let mut type_name = data_type.name;
                        type_name.make_ascii_uppercase();

                        if type_name.is_empty() {
                            break 'ty Type::Blob;
                        }

                        if type_name == "INTEGER" {
                            typename_exactly_integer = true;
                            break 'ty Type::Integer;
                        }

                        if let Some(ty) = type_name.as_bytes().windows(3).find_map(|s| match s {
                            b"INT" => Some(Type::Integer),
                            _ => None,
                        }) {
                            break 'ty ty;
                        }

                        if let Some(ty) = type_name.as_bytes().windows(4).find_map(|s| match s {
                            b"CHAR" | b"CLOB" | b"TEXT" => Some(Type::Text),
                            b"BLOB" => Some(Type::Blob),
                            b"REAL" | b"FLOA" | b"DOUB" => Some(Type::Real),
                            _ => None,
                        }) {
                            break 'ty ty;
                        }

                        Type::Numeric
                    }
                    None => Type::Null,
                };

                let mut default = None;
                let mut primary_key = false;
                let mut notnull = false;
                let mut order = SortOrder::Asc;
                let mut unique = false;
                let mut collation = None;
                for c_def in col_def.constraints {
                    match c_def.constraint {
                        turso_sqlite3_parser::ast::ColumnConstraint::PrimaryKey {
                            order: o,
                            ..
                        } => {
                            primary_key = true;
                            if let Some(o) = o {
                                order = o;
                            }
                        }
                        turso_sqlite3_parser::ast::ColumnConstraint::NotNull { .. } => {
                            notnull = true;
                        }
                        turso_sqlite3_parser::ast::ColumnConstraint::Default(expr) => {
                            default = Some(expr)
                        }
                        // TODO: for now we don't check Resolve type of unique
                        turso_sqlite3_parser::ast::ColumnConstraint::Unique(on_conflict) => {
                            if on_conflict.is_some() {
                                unimplemented!("ON CONFLICT not implemented");
                            }
                            unique = true;
                        }
                        turso_sqlite3_parser::ast::ColumnConstraint::Collate { collation_name } => {
                            collation = Some(CollationSeq::new(collation_name.0.as_str())?);
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
                    default,
                    unique,
                    collation,
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
    pub default: Option<Expr>,
    pub unique: bool,
    pub collation: Option<CollationSeq>,
}

impl Column {
    pub fn affinity(&self) -> Affinity {
        affinity(&self.ty_str)
    }
}

// TODO: This might replace some of util::columns_from_create_table_body
impl From<ColumnDefinition> for Column {
    fn from(value: ColumnDefinition) -> Self {
        let ast::Name(name) = value.col_name;

        let mut default = None;
        let mut notnull = false;
        let mut primary_key = false;
        let mut unique = false;
        let mut collation = None;

        for ast::NamedColumnConstraint { constraint, .. } in value.constraints {
            match constraint {
                ast::ColumnConstraint::PrimaryKey { .. } => primary_key = true,
                ast::ColumnConstraint::NotNull { .. } => notnull = true,
                ast::ColumnConstraint::Unique(..) => unique = true,
                ast::ColumnConstraint::Default(expr) => {
                    default.replace(expr);
                }
                ast::ColumnConstraint::Collate { collation_name } => {
                    collation.replace(
                        CollationSeq::new(&collation_name.0)
                            .expect("collation should have been set correctly in create table"),
                    );
                }
                _ => {}
            };
        }

        let ty = match value.col_type {
            Some(ref data_type) => {
                // https://www.sqlite.org/datatype3.html
                let type_name = data_type.name.clone().to_uppercase();

                if type_name.contains("INT") {
                    Type::Integer
                } else if type_name.contains("CHAR")
                    || type_name.contains("CLOB")
                    || type_name.contains("TEXT")
                {
                    Type::Text
                } else if type_name.contains("BLOB") || type_name.is_empty() {
                    Type::Blob
                } else if type_name.contains("REAL")
                    || type_name.contains("FLOA")
                    || type_name.contains("DOUB")
                {
                    Type::Real
                } else {
                    Type::Numeric
                }
            }
            None => Type::Null,
        };

        let ty_str = value
            .col_type
            .map(|t| t.name.to_string())
            .unwrap_or_default();

        Column {
            name: Some(name),
            ty,
            default,
            notnull,
            ty_str,
            primary_key,
            is_rowid_alias: primary_key && matches!(ty, Type::Integer),
            unique,
            collation,
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

    pub fn from_char(char: char) -> Result<Self> {
        match char {
            SQLITE_AFF_INTEGER => Ok(Affinity::Integer),
            SQLITE_AFF_TEXT => Ok(Affinity::Text),
            SQLITE_AFF_NONE => Ok(Affinity::Blob),
            SQLITE_AFF_REAL => Ok(Affinity::Real),
            SQLITE_AFF_NUMERIC => Ok(Affinity::Numeric),
            _ => Err(LimboError::InternalError(format!(
                "Invalid affinity character: {char}"
            ))),
        }
    }

    pub fn as_char_code(&self) -> u8 {
        self.aff_mask() as u8
    }

    pub fn from_char_code(code: u8) -> Result<Self, LimboError> {
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
            },
        ],
        unique_sets: None,
    }
}

#[allow(dead_code)]
#[derive(Debug)]
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
    /// CREATE TABLE t(a,b,c)
    /// CREATE INDEX idx ON t(b)
    /// b.pos_in_table == 1
    pub pos_in_table: usize,
    pub collation: Option<CollationSeq>,
    pub default: Option<Expr>,
}

impl Index {
    pub fn from_sql(sql: &str, root_page: usize, table: &BTreeTable) -> Result<Index> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateIndex {
                idx_name,
                tbl_name,
                columns,
                unique,
                ..
            })) => {
                let index_name = normalize_ident(&idx_name.name.0);
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
                    table_name: normalize_ident(&tbl_name.0),
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
    /// CREATE TABLE t(a, b, c)
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
