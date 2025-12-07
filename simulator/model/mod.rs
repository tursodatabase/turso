use std::fmt::Display;

use anyhow::Context;
use bitflags::bitflags;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use sql_generation::model::query::select::SelectTable;
use sql_generation::model::{
    query::{
        Create, CreateIndex, Delete, Drop, DropIndex, Insert, Select,
        alter_table::{AlterTable, AlterTableType},
        pragma::Pragma,
        select::{CompoundOperator, FromClause, ResultColumn, SelectInner},
        transaction::{Begin, Commit, Rollback},
        update::Update,
    },
    table::{Index, JoinTable, JoinType, SimValue, Table, TableContext},
};
use turso_parser::ast::Distinctness;

use crate::{generation::Shadow, runner::env::ShadowTablesMut};

pub mod interactions;
pub mod metrics;
pub mod property;

pub(crate) type ResultSet = turso_core::Result<Vec<Vec<SimValue>>>;

// This type represents the potential queries on the database.
#[derive(Debug, Clone, Serialize, Deserialize, strum::EnumDiscriminants)]
pub enum Query {
    Create(Create),
    Select(Select),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Drop(Drop),
    CreateIndex(CreateIndex),
    AlterTable(AlterTable),
    DropIndex(DropIndex),
    Begin(Begin),
    Commit(Commit),
    Rollback(Rollback),
    Pragma(Pragma),
    /// Placeholder query that still needs to be generated
    Placeholder,
}

impl Query {
    pub fn as_create(&self) -> &Create {
        match self {
            Self::Create(create) => create,
            _ => unreachable!(),
        }
    }

    pub fn unwrap_create(self) -> Create {
        match self {
            Self::Create(create) => create,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn unwrap_insert(self) -> Insert {
        match self {
            Self::Insert(insert) => insert,
            _ => unreachable!(),
        }
    }

    pub fn dependencies(&self) -> IndexSet<String> {
        match self {
            Query::Select(select) => select.dependencies(),
            Query::Create(_) => IndexSet::new(),
            Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Update(Update { table, .. })
            | Query::Drop(Drop { table, .. })
            | Query::CreateIndex(CreateIndex {
                index: Index {
                    table_name: table, ..
                },
            })
            | Query::AlterTable(AlterTable {
                table_name: table, ..
            })
            | Query::DropIndex(DropIndex {
                table_name: table, ..
            }) => IndexSet::from_iter([table.clone()]),
            Query::Begin(_)
            | Query::Commit(_)
            | Query::Rollback(_)
            | Query::Placeholder
            | Query::Pragma(_) => IndexSet::new(),
        }
    }
    pub fn uses(&self) -> Vec<String> {
        match self {
            Query::Create(Create { table }) => vec![table.name.clone()],
            Query::Select(select) => select.dependencies().into_iter().collect(),
            Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Update(Update { table, .. })
            | Query::Drop(Drop { table, .. })
            | Query::CreateIndex(CreateIndex {
                index: Index {
                    table_name: table, ..
                },
            })
            | Query::AlterTable(AlterTable {
                table_name: table, ..
            })
            | Query::DropIndex(DropIndex {
                table_name: table, ..
            }) => vec![table.clone()],
            Query::Begin(..) | Query::Commit(..) | Query::Rollback(..) => vec![],
            Query::Placeholder => vec![],
            Query::Pragma(_) => vec![],
        }
    }

    #[inline]
    pub fn is_transaction(&self) -> bool {
        matches!(
            self,
            Self::Begin(..) | Self::Commit(..) | Self::Rollback(..)
        )
    }

    #[inline]
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            Self::Create(..)
                | Self::CreateIndex(..)
                | Self::Drop(..)
                | Self::AlterTable(..)
                | Self::DropIndex(..)
        )
    }

    #[inline]
    pub fn is_dml(&self) -> bool {
        matches!(self, Self::Insert(..) | Self::Update(..) | Self::Delete(..))
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(create) => write!(f, "{create}"),
            Self::Select(select) => write!(f, "{select}"),
            Self::Insert(insert) => write!(f, "{insert}"),
            Self::Delete(delete) => write!(f, "{delete}"),
            Self::Update(update) => write!(f, "{update}"),
            Self::Drop(drop) => write!(f, "{drop}"),
            Self::CreateIndex(create_index) => write!(f, "{create_index}"),
            Self::AlterTable(alter_table) => write!(f, "{alter_table}"),
            Self::DropIndex(drop_index) => write!(f, "{drop_index}"),
            Self::Begin(begin) => write!(f, "{begin}"),
            Self::Commit(commit) => write!(f, "{commit}"),
            Self::Rollback(rollback) => write!(f, "{rollback}"),
            Self::Placeholder => Ok(()),
            Query::Pragma(pragma) => write!(f, "{pragma}"),
        }
    }
}

impl Shadow for Query {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, env: &mut ShadowTablesMut) -> Self::Result {
        match self {
            Query::Create(create) => create.shadow(env),
            Query::Insert(insert) => insert.shadow(env),
            Query::Delete(delete) => delete.shadow(env),
            Query::Select(select) => select.shadow(env),
            Query::Update(update) => update.shadow(env),
            Query::Drop(drop) => drop.shadow(env),
            Query::CreateIndex(create_index) => Ok(create_index.shadow(env)),
            Query::AlterTable(alter_table) => alter_table.shadow(env),
            Query::DropIndex(drop_index) => drop_index.shadow(env),
            Query::Begin(begin) => Ok(begin.shadow(env)),
            Query::Commit(commit) => Ok(commit.shadow(env)),
            Query::Rollback(rollback) => Ok(rollback.shadow(env)),
            Query::Placeholder => Ok(vec![]),
            Query::Pragma(Pragma::AutoVacuumMode(_)) => Ok(vec![]),
        }
    }
}

bitflags! {
    pub struct QueryCapabilities: u32 {
        const NONE = 0;
        const CREATE = 1 << 0;
        const SELECT = 1 << 1;
        const INSERT = 1 << 2;
        const DELETE = 1 << 3;
        const UPDATE = 1 << 4;
        const DROP = 1 << 5;
        const CREATE_INDEX = 1 << 6;
        const ALTER_TABLE = 1 << 7;
        const DROP_INDEX = 1 << 8;
    }
}

impl QueryCapabilities {
    // TODO: can be const fn in the future
    pub fn from_list_queries(queries: &[QueryDiscriminants]) -> Self {
        queries
            .iter()
            .fold(Self::empty(), |accum, q| accum.union(q.into()))
    }
}

impl From<&QueryDiscriminants> for QueryCapabilities {
    fn from(value: &QueryDiscriminants) -> Self {
        (*value).into()
    }
}

impl From<QueryDiscriminants> for QueryCapabilities {
    fn from(value: QueryDiscriminants) -> Self {
        match value {
            QueryDiscriminants::Create => Self::CREATE,
            QueryDiscriminants::Select => Self::SELECT,
            QueryDiscriminants::Insert => Self::INSERT,
            QueryDiscriminants::Delete => Self::DELETE,
            QueryDiscriminants::Update => Self::UPDATE,
            QueryDiscriminants::Drop => Self::DROP,
            QueryDiscriminants::CreateIndex => Self::CREATE_INDEX,
            QueryDiscriminants::AlterTable => Self::ALTER_TABLE,
            QueryDiscriminants::DropIndex => Self::DROP_INDEX,
            QueryDiscriminants::Begin
            | QueryDiscriminants::Commit
            | QueryDiscriminants::Rollback => {
                unreachable!("QueryCapabilities do not apply to transaction queries")
            }
            QueryDiscriminants::Placeholder => {
                unreachable!("QueryCapabilities do not apply to query Placeholder")
            }
            QueryDiscriminants::Pragma => QueryCapabilities::NONE,
        }
    }
}

impl QueryDiscriminants {
    pub const ALL_NO_TRANSACTION: &'_ [QueryDiscriminants] = &[
        QueryDiscriminants::Select,
        QueryDiscriminants::Create,
        QueryDiscriminants::Insert,
        QueryDiscriminants::Update,
        QueryDiscriminants::Delete,
        QueryDiscriminants::Drop,
        QueryDiscriminants::CreateIndex,
        QueryDiscriminants::AlterTable,
        QueryDiscriminants::DropIndex,
        QueryDiscriminants::Pragma,
    ];
}

impl Shadow for Create {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        if !tables.iter().any(|t| t.name == self.table.name) {
            tables.push(self.table.clone());
            Ok(vec![])
        } else {
            Err(anyhow::anyhow!(
                "Table {} already exists. CREATE TABLE statement ignored.",
                self.table.name
            ))
        }
    }
}

impl Shadow for CreateIndex {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, env: &mut ShadowTablesMut) -> Vec<Vec<SimValue>> {
        env.iter_mut()
            .find(|t| t.name == self.table_name)
            .unwrap()
            .indexes
            .push(self.index.clone());
        vec![]
    }
}

impl Shadow for Delete {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        let table = tables.iter_mut().find(|t| t.name == self.table);

        if let Some(table) = table {
            // If the table exists, we can delete from it
            let t2 = table.clone();
            table.rows.retain_mut(|r| !self.predicate.test(r, &t2));
        } else {
            // If the table does not exist, we return an error
            return Err(anyhow::anyhow!(
                "Table {} does not exist. DELETE statement ignored.",
                self.table
            ));
        }

        Ok(vec![])
    }
}

impl Shadow for Drop {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        tracing::info!("dropping {:?}", self);
        if !tables.iter().any(|t| t.name == self.table) {
            // If the table does not exist, we return an error
            return Err(anyhow::anyhow!(
                "Table {} does not exist. DROP statement ignored.",
                self.table
            ));
        }

        tables.retain(|t| t.name != self.table);

        // NOTE: We do NOT remove foreign keys in other tables that reference the dropped table.
        // While the FK is logically "dead" (parent doesn't exist), SQLite still stores the FK
        // constraint in the child table's schema. If we remove the FK from our shadow, we might
        // generate an ALTER TABLE DROP COLUMN that drops a column referenced by the FK, which
        // would cause "malformed database schema" errors in SQLite.
        // The FK constraint will remain in the schema until the child table is dropped or
        // the referencing columns are renamed/dropped (which SQLite will then complain about).

        Ok(vec![])
    }
}

impl Shadow for Insert {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    //FIXME this doesn't handle type affinity
    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        match self {
            Insert::Values { table, values } => {
                if let Some(t) = tables.iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(values.clone());
                } else {
                    return Err(anyhow::anyhow!(
                        "Table {} does not exist. INSERT statement ignored.",
                        table
                    ));
                }
            }
            Insert::Select { table, select } => {
                let rows = select.shadow(tables)?;
                if let Some(t) = tables.iter_mut().find(|t| &t.name == table) {
                    t.rows.extend(rows);
                } else {
                    return Err(anyhow::anyhow!(
                        "Table {} does not exist. INSERT statement ignored.",
                        table
                    ));
                }
            }
        }

        Ok(vec![])
    }
}

impl Shadow for FromClause {
    type Result = anyhow::Result<JoinTable>;
    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        let mut join_table = match &self.table {
            SelectTable::Table(table) => {
                let first_table = tables
                    .iter()
                    .find(|t| t.name == *table)
                    .context("Table not found")?;
                JoinTable {
                    tables: vec![first_table.clone()],
                    rows: first_table.rows.clone(),
                }
            }
            SelectTable::Select(select) => {
                let select_dependencies = select.dependencies();
                let result_tables = tables
                    .iter()
                    .filter(|shadow_table| select_dependencies.contains(shadow_table.name.as_str()))
                    .cloned()
                    .collect();
                let rows = select.shadow(tables)?;
                JoinTable {
                    tables: result_tables,
                    rows,
                }
            }
        };

        for join in &self.joins {
            let joined_table = tables
                .iter()
                .find(|t| t.name == join.table)
                .context("Joined table not found")?;

            join_table.tables.push(joined_table.clone());

            match join.join_type {
                JoinType::Inner => {
                    let prev_rows = std::mem::take(&mut join_table.rows);
                    let mut new_rows = Vec::new();
                    for row1 in prev_rows.into_iter() {
                        for row2 in joined_table.rows.iter() {
                            let combined_row =
                                row1.iter().chain(row2.iter()).cloned().collect::<Vec<_>>();
                            if join.on.test(&combined_row, &join_table) {
                                new_rows.push(combined_row);
                            }
                        }
                    }
                    join_table.rows = new_rows;
                }
                _ => todo!(),
            }
        }
        Ok(join_table)
    }
}

impl Shadow for SelectInner {
    type Result = anyhow::Result<JoinTable>;

    fn shadow(&self, env: &mut ShadowTablesMut) -> Self::Result {
        if let Some(from) = &self.from {
            let mut join_table = from.shadow(env)?;
            let col_count = join_table.columns().count();
            for row in &mut join_table.rows {
                assert_eq!(
                    row.len(),
                    col_count,
                    "Row length does not match column length after join"
                );
            }
            let join_clone = join_table.clone();

            join_table
                .rows
                .retain(|row| self.where_clause.test(row, &join_clone));

            if self.distinctness == Distinctness::Distinct {
                join_table.rows.sort_unstable();
                join_table.rows.dedup();
            }

            Ok(join_table)
        } else {
            assert!(
                self.columns
                    .iter()
                    .all(|col| matches!(col, ResultColumn::Expr(_)))
            );

            // If `WHERE` is false, just return an empty table
            if !self.where_clause.test(&[], &Table::anonymous(vec![])) {
                return Ok(JoinTable {
                    tables: Vec::new(),
                    rows: Vec::new(),
                });
            }

            // Compute the results of the column expressions and make a row
            let mut row = Vec::new();
            for col in &self.columns {
                match col {
                    ResultColumn::Expr(expr) => {
                        let value = expr.eval(&[], &Table::anonymous(vec![]));
                        if let Some(value) = value {
                            row.push(value);
                        } else {
                            return Err(anyhow::anyhow!(
                                "Failed to evaluate expression in free select ({})",
                                expr.0
                            ));
                        }
                    }
                    _ => unreachable!("Only expressions are allowed in free selects"),
                }
            }

            Ok(JoinTable {
                tables: Vec::new(),
                rows: vec![row],
            })
        }
    }
}

impl Shadow for Select {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, env: &mut ShadowTablesMut) -> Self::Result {
        let first_result = self.body.select.shadow(env)?;

        let mut rows = first_result.rows;

        for compound in self.body.compounds.iter() {
            let compound_results = compound.select.shadow(env)?;

            match compound.operator {
                CompoundOperator::Union => {
                    // Union means we need to combine the results, removing duplicates
                    let mut new_rows = compound_results.rows;
                    new_rows.extend(rows.clone());
                    new_rows.sort_unstable();
                    new_rows.dedup();
                    rows = new_rows;
                }
                CompoundOperator::UnionAll => {
                    // Union all means we just concatenate the results
                    rows.extend(compound_results.rows.into_iter());
                }
            }
        }

        Ok(rows)
    }
}

impl Shadow for Begin {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        // FIXME: currently the snapshot is taken eagerly
        // this is wrong for Deffered transactions
        tables.create_snapshot();
        vec![]
    }
}

impl Shadow for Commit {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        tables.apply_snapshot();
        vec![]
    }
}

impl Shadow for Rollback {
    type Result = Vec<Vec<SimValue>>;
    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        tables.delete_snapshot();
        vec![]
    }
}

impl Shadow for Update {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        let table = tables.iter_mut().find(|t| t.name == self.table);

        let table = if let Some(table) = table {
            table
        } else {
            return Err(anyhow::anyhow!(
                "Table {} does not exist. UPDATE statement ignored.",
                self.table
            ));
        };

        let t2 = table.clone();
        for row in table
            .rows
            .iter_mut()
            .filter(|r| self.predicate.test(r, &t2))
        {
            for (column, set_value) in &self.set_values {
                if let Some((idx, _)) = table
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| &c.name == column)
                {
                    row[idx] = set_value.clone();
                }
            }
        }

        Ok(vec![])
    }
}

impl Shadow for AlterTable {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut<'_>) -> Self::Result {
        // For RenameTo, we need to update foreign keys in other tables that reference this table
        if let AlterTableType::RenameTo { new_name } = &self.alter_table_type {
            let old_name = &self.table_name;
            for table in tables.iter_mut() {
                for fk in table.foreign_keys.iter_mut() {
                    if &fk.parent_table == old_name {
                        fk.parent_table = new_name.clone();
                    }
                }
            }
        }

        let table = tables
            .iter_mut()
            .find(|t| t.name == self.table_name)
            .ok_or_else(|| anyhow::anyhow!("Table {} does not exist", self.table_name))?;

        match &self.alter_table_type {
            AlterTableType::RenameTo { new_name } => {
                table.name = new_name.clone();
            }
            AlterTableType::AddColumn { column } => {
                table.columns.push(column.clone());
                table.rows.iter_mut().for_each(|row| {
                    row.push(SimValue(turso_core::Value::Null));
                });
            }
            AlterTableType::AlterColumn { old, new } => {
                let col = table.columns.iter_mut().find(|c| c.name == *old).unwrap();
                *col = new.clone();
                table.indexes.iter_mut().for_each(|index| {
                    index.columns.iter_mut().for_each(|(col_name, _)| {
                        if col_name == old {
                            *col_name = new.name.clone();
                        }
                    });
                });
                table.foreign_keys.iter_mut().for_each(|fk| {
                    fk.child_columns.iter_mut().for_each(|col_name| {
                        if col_name == old {
                            *col_name = new.name.clone();
                        }
                    });
                });
            }
            AlterTableType::RenameColumn { old, new } => {
                let col = table.columns.iter_mut().find(|c| c.name == *old).unwrap();
                col.name = new.clone();
                table.indexes.iter_mut().for_each(|index| {
                    index.columns.iter_mut().for_each(|(col_name, _)| {
                        if col_name == old {
                            *col_name = new.clone();
                        }
                    });
                });
                table.foreign_keys.iter_mut().for_each(|fk| {
                    fk.child_columns.iter_mut().for_each(|col_name| {
                        if col_name == old {
                            *col_name = new.clone();
                        }
                    });
                });
            }
            AlterTableType::DropColumn { column_name } => {
                let col_idx = table
                    .columns
                    .iter()
                    .position(|c| c.name == *column_name)
                    .unwrap();
                table.columns.remove(col_idx);
                table.rows.iter_mut().for_each(|row| {
                    row.remove(col_idx);
                });
                // Remove foreign keys that reference the dropped column
                table.foreign_keys.retain(|fk| {
                    !fk.child_columns.contains(column_name)
                });
            }
        };
        Ok(vec![])
    }
}

impl Shadow for DropIndex {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut<'_>) -> Self::Result {
        let table = tables
            .iter_mut()
            .find(|t| t.name == self.table_name)
            .ok_or_else(|| anyhow::anyhow!("Table {} does not exist", self.table_name))?;

        table
            .indexes
            .retain(|index| index.index_name != self.index_name);
        Ok(vec![])
    }
}
