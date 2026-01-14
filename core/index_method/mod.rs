use std::{collections::HashMap, sync::Arc};

use turso_parser::ast;

use crate::{
    schema::IndexColumn,
    storage::btree::BTreeCursor,
    types::{IOResult, IndexInfo, KeyInfo},
    vdbe::Register,
    Connection, LimboError, Result, Value,
};

pub mod backing_btree;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
pub mod fts;
pub mod toy_vector_sparse_ivf;

pub const BACKING_BTREE_INDEX_METHOD_NAME: &str = "backing_btree";
pub const TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME: &str = "toy_vector_sparse_ivf";

/// index method "entry point" which can create attachment of the method to the table with given configuration
/// (this trait acts like a "factory")
pub trait IndexMethod: std::fmt::Debug + Send + Sync {
    /// create attachment of the index method to the specific table with specific method configuration
    fn attach(
        &self,
        configuration: &IndexMethodConfiguration,
    ) -> Result<Arc<dyn IndexMethodAttachment>>;
}

#[derive(Debug, Clone)]
pub struct IndexMethodConfiguration {
    /// table name for which index_method is defined
    pub table_name: String,
    /// index name
    pub index_name: String,
    /// columns c1, c2, c3, ... provided to the index method (e.g. create index t_idx on t using method (c1, c2, c3, ...))
    pub columns: Vec<IndexColumn>,
    /// optional parameters provided to the index method through WITH clause
    pub parameters: HashMap<String, Value>,
}

/// index method attached to the table with specific configuration
/// the attachment is capable of generating SELECT patterns where index can be used and also can create cursor for query execution
pub trait IndexMethodAttachment: std::fmt::Debug + Send + Sync {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a>;
    fn init(&self) -> Result<Box<dyn IndexMethodCursor>>;
}

#[derive(Debug)]
pub struct IndexMethodDefinition<'a> {
    /// index method name
    pub method_name: &'a str,
    /// index name
    pub index_name: &'a str,
    /// SELECT patterns where index method can be used
    /// the patterns can contain positional placeholder which will make planner to capture parameters from the original query and provide them to the index method
    /// (for example, pattern 'SELECT * FROM {table} LIMIT ?' will capture LIMIT parameter and provide its value from the query to the index method query_start(...) call)
    pub patterns: &'a [ast::Select],
    /// special marker which forces tursodb core to treat index method as backing btree - so it will allocate real btree on disk for that index method
    pub backing_btree: bool,
}

/// Cost estimate returned by custom index methods for optimizer integration.
/// This enables the optimizer to make cost-based decisions when choosing between
/// custom index methods and traditional BTree indexes.
#[derive(Debug, Clone, Copy)]
pub struct IndexMethodCostEstimate {
    /// Estimated CPU/IO cost (lower is better, comparable to optimizer Cost values)
    pub estimated_cost: f64,
    /// Estimated number of rows returned by the query
    pub estimated_rows: u64,
}

/// cursor opened for index method and capable of executing DML/DDL/DQL queries for the index method over fixed table
pub trait IndexMethodCursor {
    /// create necessary components for index method (usually, this is a bunch of btree-s)
    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    /// destroy components created in the create(...) call for index method
    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;

    /// open necessary components for reading the index
    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    /// open necessary components for writing the index
    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;

    /// handle insert action
    /// "values" argument contains registers with values for index columns followed by rowid Integer register
    /// (e.g. for "CREATE INDEX i ON t USING method (x, z)" insert(...) call will have 3 registers in values: [x, z, rowid])
    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    /// handle delete action
    /// "values" argument contains registers with values for index columns followed by rowid Integer register
    /// (e.g. for "CREATE INDEX i ON t USING method (x, z)" insert(...) call will have 3 registers in values: [x, z, rowid])
    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>>;

    /// initialize query to the index method
    /// first element of "values" slice is the Integer register which holds index of the chosen [IndexMethodDefinition::patterns] by query planner
    /// next arguments of the "values" slice are values from the original query expression captured by pattern
    ///
    /// For example, for 2 patterns ["SELECT * FROM {table} LIMIT ?", "SELECT * FROM {table} WHERE x = ?"], query_start(...) call can have following arguments:
    /// - [Integer(0), Integer(10)] - pattern "SELECT * FROM {table} LIMIT ?" was chosen with LIMIT parameter equals to 10
    /// - [Integer(1), Text("turso")] - pattern "SELECT * FROM {table} WHERE x = ?" was chosen with equality comparison equals to "turso"
    ///
    /// Returns false if query will produce no rows (similar to VFilter/Rewind op codes)
    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<bool>>;

    /// Moves cursor to the next response row
    /// Returns false if query exhausted all rows
    fn query_next(&mut self) -> Result<IOResult<bool>>;

    /// Return column with given idx (zero-based) from current row
    fn query_column(&mut self, idx: usize) -> Result<IOResult<Value>>;

    /// Return rowid of the original table row which corresponds to the current cursor row
    ///
    /// This method is used by tursodb core in order to "enrich" response from query pattern with additional fields from original table
    /// For example, consider pattern like this:
    ///
    /// > SELECT vector_distance_jaccard(embedding, ?) as d FROM table ORDER BY d LIMIT 10
    ///
    /// It can be used in more complex query:
    ///
    /// > SELECT name, comment, rating, vector_distance_jaccard(embedding, ?) as d FROM table ORDER BY d LIMIT 10
    ///
    /// In this case query planner will execute index method query first, and then
    /// enrich its result with name, comment, rating columns from original table accessing original row by its rowid
    /// returned from query_rowid(...) method
    fn query_rowid(&mut self) -> Result<IOResult<Option<i64>>>;

    /// Called before transaction commit to flush any pending writes.
    /// This ensures index method writes are persisted as part of the transaction.
    fn pre_commit(&mut self) -> Result<IOResult<()>> {
        Ok(IOResult::Done(()))
    }

    /// Optimize the index by merging segments or performing other maintenance.
    fn optimize(&mut self, _connection: &Arc<Connection>) -> Result<IOResult<()>> {
        Ok(IOResult::Done(()))
    }

    /// Estimate the cost of executing a query with the given pattern.
    ///
    /// This method enables the optimizer to make cost-based decisions when choosing
    /// between custom index methods and traditional BTree indexes.
    fn estimate_cost(
        &self,
        pattern_idx: usize,
        base_table_rows: f64,
    ) -> Option<IndexMethodCostEstimate> {
        let _ = (pattern_idx, base_table_rows);
        None
    }
}

/// helper method to open table BTree cursor in the index method implementation
pub(crate) fn open_table_cursor(connection: &Connection, table: &str) -> Result<BTreeCursor> {
    let pager = connection.pager.load().clone();
    let schema = connection.schema.read();
    let Some(table) = schema.get_table(table) else {
        return Err(LimboError::InternalError(format!(
            "table {table} not found",
        )));
    };
    let cursor = BTreeCursor::new_table(pager, table.get_root_page(), table.columns().len());
    Ok(cursor)
}

/// helper method to open index BTree cursor in the index method implementation
pub(crate) fn open_index_cursor(
    connection: &Connection,
    table: &str,
    index: &str,
    keys: Vec<KeyInfo>,
) -> Result<BTreeCursor> {
    let pager = connection.pager.load().clone();
    let schema = connection.schema.read();
    let Some(scratch) = schema.get_index(table, index) else {
        return Err(LimboError::InternalError(format!(
            "index {index} for table {table} not found",
        )));
    };
    let mut cursor = BTreeCursor::new(pager, scratch.root_page, keys.len());
    cursor.index_info = Some(Arc::new(IndexInfo {
        has_rowid: false,
        num_cols: keys.len(),
        key_info: keys,
    }));
    Ok(cursor)
}

/// helper method to parse select patterns for [IndexMethodAttachment::definition] call
pub(crate) fn parse_patterns(patterns: &[&str]) -> Result<Vec<ast::Select>> {
    let mut parsed = Vec::new();
    for pattern in patterns {
        let mut parser = turso_parser::parser::Parser::new(pattern.as_bytes());
        let Some(ast) = parser.next() else {
            return Err(LimboError::ParseError(format!(
                "unable to parse pattern statement: {pattern}",
            )));
        };
        let ast = ast?;
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) = ast else {
            return Err(LimboError::ParseError(format!(
                "only select patterns are allowed: {pattern}",
            )));
        };
        parsed.push(select);
    }
    Ok(parsed)
}
