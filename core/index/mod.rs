use std::{collections::HashMap, sync::Arc};

use turso_parser::ast;

use crate::{
    storage::btree::BTreeCursor,
    types::{IOResult, IndexInfo, KeyInfo},
    vdbe::Register,
    Connection, LimboError, Result, Value,
};

pub mod hidden_btree;
pub mod vector_sparse_ivf;

#[derive(Debug, Clone)]
pub struct IndexConfiguration {
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub parameters: HashMap<String, Value>,
}

pub trait IndexModule: std::fmt::Debug + Send + Sync {
    fn descriptor(&self, configuration: &IndexConfiguration) -> Result<Arc<dyn IndexDescriptor>>;
}

pub trait IndexDescriptor: std::fmt::Debug + Send + Sync {
    fn definition<'a>(&'a self) -> IndexDefinition<'a>;
    fn init(&self) -> Result<Box<dyn IndexCursor>>;
}

pub const HIDDEN_BTREE_MODULE_NAME: &str = "btree";
pub const VECTOR_SPARSE_IVF_MODULE_NAME: &str = "vector_sparse_ivf";

#[derive(Debug)]
pub struct IndexDefinition<'a> {
    pub module_name: &'a str,
    pub index_name: &'a str,
    pub patterns: &'a [ast::Select],
    pub hidden: bool,
}

pub trait IndexCursor {
    fn create(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    fn destroy(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;

    fn open_read(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;
    fn open_write(&mut self, connection: &Arc<Connection>) -> Result<IOResult<()>>;

    fn insert(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    fn delete(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    fn query_start(&mut self, values: &[Register]) -> Result<IOResult<()>>;
    fn query_next(&mut self) -> Result<IOResult<bool>>;
    fn query_column(&mut self, position: usize) -> Result<IOResult<&Value>>;

    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
}

pub(crate) fn open_btree_cursor(
    connection: &Connection,
    table: &str,
    index: &str,
    keys: Vec<KeyInfo>,
) -> Result<BTreeCursor> {
    let pager = connection.pager.read().clone();
    let schema = connection.schema.read();
    let Some(scratch) = schema.get_index(table, index) else {
        return Err(LimboError::InternalError(format!(
            "index {} for table {} not found",
            index, table
        )));
    };
    let mut cursor = BTreeCursor::new(pager, scratch.root_page, keys.len());
    cursor.index_info = Some(IndexInfo {
        has_rowid: false,
        num_cols: keys.len(),
        key_info: keys,
    });
    Ok(cursor)
}

pub(crate) fn parse_patterns(patterns: &[&str]) -> Result<Vec<ast::Select>> {
    let mut parsed = Vec::new();
    for pattern in patterns {
        let mut parser = turso_parser::parser::Parser::new(pattern.as_bytes());
        let Some(ast) = parser.next() else {
            return Err(LimboError::ParseError(format!(
                "unable to parse pattern statement: {}",
                pattern
            )));
        };
        let ast = ast?;
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) = ast else {
            return Err(LimboError::ParseError(format!(
                "only select patterns are allowed: {}",
                pattern
            )));
        };
        parsed.push(select);
    }
    Ok(parsed)
}
