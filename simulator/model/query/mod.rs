use std::{collections::HashSet, fmt::Display};

pub(crate) use create::Create;
pub(crate) use create_index::CreateIndex;
pub(crate) use delete::Delete;
pub(crate) use drop::Drop;
pub(crate) use insert::Insert;
pub(crate) use select::Select;
use serde::{Deserialize, Serialize};
use turso_sqlite3_parser::to_sql_string::ToSqlContext;
use update::Update;

use crate::model::{table::SimValue, SimulatorEnv};

pub mod create;
pub mod create_index;
pub mod delete;
pub mod drop;
pub mod insert;
pub mod predicate;
pub mod select;
pub mod update;

// This type represents the potential queries on the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    Create(Create),
    Select(Select),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Drop(Drop),
    CreateIndex(CreateIndex),
}

impl Query {
    pub fn dependencies(&self) -> HashSet<String> {
        match self {
            Query::Create(_) => HashSet::new(),
            Query::Select(Select { table, .. })
            | Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Update(Update { table, .. })
            | Query::Drop(Drop { table, .. }) => HashSet::from_iter([table.clone()]),
            Query::CreateIndex(CreateIndex { table_name, .. }) => {
                HashSet::from_iter([table_name.clone()])
            }
        }
    }
    pub fn uses(&self) -> Vec<String> {
        match self {
            Query::Create(Create { table }) => vec![table.name.clone()],
            Query::Select(Select { table, .. })
            | Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Update(Update { table, .. })
            | Query::Drop(Drop { table, .. }) => vec![table.clone()],
            Query::CreateIndex(CreateIndex { table_name, .. }) => vec![table_name.clone()],
        }
    }

    pub fn shadow<E: SimulatorEnv>(&self, env: &mut E) -> Vec<Vec<SimValue>> {
        match self {
            Query::Create(create) => create.shadow(env),
            Query::Insert(insert) => insert.shadow(env),
            Query::Delete(delete) => delete.shadow(env),
            Query::Select(select) => select.shadow(env),
            Query::Update(update) => update.shadow(env),
            Query::Drop(drop) => drop.shadow(env),
            Query::CreateIndex(create_index) => create_index.shadow(env),
        }
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(create) => write!(f, "{}", create),
            Self::Select(select) => write!(f, "{}", select),
            Self::Insert(insert) => write!(f, "{}", insert),
            Self::Delete(delete) => write!(f, "{}", delete),
            Self::Update(update) => write!(f, "{}", update),
            Self::Drop(drop) => write!(f, "{}", drop),
            Self::CreateIndex(create_index) => write!(f, "{}", create_index),
        }
    }
}

/// Used to print sql strings that already have all the context it needs
struct EmptyContext;

impl ToSqlContext for EmptyContext {
    fn get_column_name(
        &self,
        _table_id: turso_sqlite3_parser::ast::TableInternalId,
        _col_idx: usize,
    ) -> &str {
        unreachable!()
    }

    fn get_table_name(&self, _id: turso_sqlite3_parser::ast::TableInternalId) -> &str {
        unreachable!()
    }
}
