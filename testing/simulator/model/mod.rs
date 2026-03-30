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
        update::{SetValue, Update},
    },
    table::{Column, ColumnType, Index, JoinTable, JoinType, SimValue, Table, TableContext},
};
use turso_core::Value;
use turso_core::turso_assert_eq;
use turso_parser::ast::Distinctness;

use crate::runner::env::TransactionMode;
use crate::{generation::Shadow, runner::env::ShadowTablesMut};
use std::collections::{HashMap, HashSet};

fn integer_pk_index(columns: &[Column]) -> Option<usize> {
    columns
        .iter()
        .position(|c| matches!(c.column_type, ColumnType::Integer) && c.is_primary_key())
}

fn ensure_row_width(table_name: &str, columns: &[Column], row: &[SimValue]) -> anyhow::Result<()> {
    if row.len() != columns.len() {
        return Err(anyhow::anyhow!(
            "INSERT VALUES row width {} does not match table '{}' column width {}",
            row.len(),
            table_name,
            columns.len()
        ));
    }
    Ok(())
}

fn check_unique_row(
    table_name: &str,
    columns: &[Column],
    rows: &[Vec<SimValue>],
    candidate_row: &[SimValue],
    except_row: Option<usize>,
) -> anyhow::Result<()> {
    ensure_row_width(table_name, columns, candidate_row)?;

    for (col_idx, col) in columns.iter().enumerate() {
        if !col.has_unique_or_pk() {
            continue;
        }

        if candidate_row[col_idx].0 == turso_core::Value::Null {
            continue;
        }

        let conflict = rows
            .iter()
            .enumerate()
            .filter(|(i, _)| except_row != Some(*i))
            .any(|(_, r)| r[col_idx] == candidate_row[col_idx]);
        if conflict {
            return Err(anyhow::anyhow!(
                "UNIQUE constraint violation: column '{}' in table '{}'",
                col.name,
                table_name
            ));
        }
    }
    Ok(())
}

fn check_unique_batch(
    table_name: &str,
    columns: &[Column],
    existing_rows: &[Vec<SimValue>],
    new_rows: &[Vec<SimValue>],
) -> anyhow::Result<()> {
    for (row_idx, row) in new_rows.iter().enumerate() {
        ensure_row_width(table_name, columns, row)?;

        for (col_idx, col) in columns.iter().enumerate() {
            if !col.has_unique_or_pk() {
                continue;
            }
            if row[col_idx].0 == Value::Null {
                continue;
            }
            let duplicate = existing_rows
                .iter()
                .chain(new_rows[..row_idx].iter())
                .any(|existing| existing[col_idx] == row[col_idx]);
            if duplicate {
                return Err(anyhow::anyhow!(
                    "UNIQUE constraint violation: column '{}' in table '{}'",
                    col.name,
                    table_name
                ));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct RowidAllocator {
    used: HashSet<i64>,
    next: i64,
}

impl RowidAllocator {
    fn new(existing_rows: &[Vec<SimValue>], pk_idx: usize) -> Self {
        let mut used = HashSet::new();
        let mut max_rowid = 0i64;
        for r in existing_rows {
            assert!(
                pk_idx < r.len(),
                "row width invariant violated: pk_idx={}, row_len={}",
                pk_idx,
                r.len()
            );
            if let Some(i) = r[pk_idx].0.as_int() {
                used.insert(i);
                max_rowid = max_rowid.max(i);
            }
        }
        let next = if max_rowid > 0 {
            max_rowid.saturating_add(1)
        } else {
            1
        };
        Self { used, next }
    }

    fn observe(&mut self, v: i64) {
        self.used.insert(v);
        if v >= self.next {
            self.next = v.saturating_add(1);
        }
    }

    fn alloc(&mut self) -> i64 {
        let mut tries = 0u32;
        while self.used.contains(&self.next) {
            self.next = self.next.saturating_add(1);
            tries += 1;
            assert!(
                tries < 1_000_000,
                "rowid allocator failed to find a free rowid"
            );
        }
        let v = self.next;
        self.used.insert(v);
        self.next = self.next.saturating_add(1);
        v
    }
}

fn normalize_integer_pk_row(
    table_name: &str,
    columns: &[Column],
    pk_idx: usize,
    alloc: &mut RowidAllocator,
    row: &[SimValue],
) -> anyhow::Result<Vec<SimValue>> {
    ensure_row_width(table_name, columns, row)?;
    let mut out = row.to_vec();
    if out[pk_idx].0 == Value::Null {
        let new_id = alloc.alloc();
        out[pk_idx] = SimValue(Value::from_i64(new_id));
    } else if let Some(i) = out[pk_idx].0.as_int() {
        alloc.observe(i);
    } else {
        return Err(anyhow::anyhow!(
            "datatype mismatch: table '{}' INTEGER PRIMARY KEY column '{}' must be an integer",
            table_name,
            columns[pk_idx].name
        ));
    }
    Ok(out)
}

fn validate_integer_pk_row_non_null(
    table_name: &str,
    columns: &[Column],
    pk_idx: usize,
    row: &[SimValue],
) -> anyhow::Result<()> {
    ensure_row_width(table_name, columns, row)?;
    if row[pk_idx].0.as_int().is_some() {
        Ok(())
    } else if row[pk_idx].0 == Value::Null {
        Err(anyhow::anyhow!(
            "datatype mismatch: table '{}' INTEGER PRIMARY KEY column '{}' cannot be NULL",
            table_name,
            columns[pk_idx].name
        ))
    } else {
        Err(anyhow::anyhow!(
            "datatype mismatch: table '{}' INTEGER PRIMARY KEY column '{}' must be an integer",
            table_name,
            columns[pk_idx].name
        ))
    }
}

fn prepare_insert_rows(
    table_name: &str,
    columns: &[Column],
    existing_rows: &[Vec<SimValue>],
    input_rows: &[Vec<SimValue>],
) -> anyhow::Result<Vec<Vec<SimValue>>> {
    let mut new_rows = input_rows.to_vec();

    if let Some(pk_idx) = integer_pk_index(columns) {
        let mut alloc = RowidAllocator::new(existing_rows, pk_idx);
        new_rows = new_rows
            .iter()
            .map(|r| normalize_integer_pk_row(table_name, columns, pk_idx, &mut alloc, r))
            .collect::<anyhow::Result<Vec<_>>>()?;
    } else {
        for r in &new_rows {
            ensure_row_width(table_name, columns, r)?;
        }
    }

    check_unique_batch(table_name, columns, existing_rows, &new_rows)?;
    Ok(new_rows)
}

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

    #[inline]
    pub fn is_write(&self) -> bool {
        self.is_ddl() || self.is_dml()
    }

    #[inline]
    pub fn is_select(&self) -> bool {
        matches!(self, Self::Select(_))
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
        // First check if we are not in a deferred transaction, if we are create a snapshot
        env.upgrade_transaction(self);

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
            // Record the operation BEFORE applying it to current_tables
            tables.record_create_table(self.table.clone());
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
        // Record the operation BEFORE applying it to current_tables
        env.record_create_index(self.table_name.clone(), self.index.clone());
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
        // First pass: find deleted rows and collect them
        let deleted_rows = {
            let table = tables.iter().find(|t| t.name == self.table);
            if let Some(table) = table {
                let t2 = table.clone();
                table
                    .rows
                    .iter()
                    .filter(|r| self.predicate.test(r, &t2))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                return Err(anyhow::anyhow!(
                    "Table {} does not exist. DELETE statement ignored.",
                    self.table
                ));
            }
        };

        // Record deleted rows for transaction tracking
        for row in &deleted_rows {
            tables.record_delete(self.table.clone(), row.clone());
        }

        // Second pass: actually remove the rows
        if let Some(table) = tables.iter_mut().find(|t| t.name == self.table) {
            let t2 = table.clone();
            table.rows.retain_mut(|r| !self.predicate.test(r, &t2));
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

        // Record the drop for transaction tracking
        tables.record_drop_table(self.table.clone());

        tables.retain(|t| t.name != self.table);

        Ok(vec![])
    }
}

impl Shadow for Insert {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    //FIXME this doesn't handle type affinity
    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        match self {
            Insert::Select { table, select } => {
                let table_name = table.clone();
                let raw_rows = select.shadow(tables)?;

                let table_pos = tables
                    .iter()
                    .position(|t| t.name == table_name)
                    .ok_or_else(|| anyhow::anyhow!("Table {} does not exist", table_name))?;

                let columns = tables[table_pos].columns.clone();
                let rows =
                    prepare_insert_rows(&table_name, &columns, &tables[table_pos].rows, &raw_rows)?;

                for row in &rows {
                    tables.record_insert(table_name.clone(), row.clone());
                }
                tables[table_pos].rows.extend(rows);
                Ok(vec![])
            }
            Insert::Values {
                table,
                values,
                on_conflict,
            } => {
                let table_name = table.clone();

                let table_pos = tables
                    .iter()
                    .position(|t| t.name == table_name)
                    .ok_or_else(|| anyhow::anyhow!("Table {} does not exist", table_name))?;

                let columns = tables[table_pos].columns.clone();

                match on_conflict {
                    None => {
                        let new_rows = prepare_insert_rows(
                            &table_name,
                            &columns,
                            &tables[table_pos].rows,
                            values,
                        )?;

                        for row in &new_rows {
                            tables.record_insert(table_name.clone(), row.clone());
                        }
                        tables[table_pos].rows.extend(new_rows);
                        Ok(vec![])
                    }
                    Some(on_conflict) => {
                        let target_col_idx = columns
                            .iter()
                            .position(|c| c.name == on_conflict.target_column)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "ON CONFLICT target column '{}' does not exist in table '{}'",
                                    on_conflict.target_column,
                                    table_name
                                )
                            })?;
                        if !columns[target_col_idx].has_unique_or_pk() {
                            return Err(anyhow::anyhow!(
                                "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"
                            ));
                        }

                        let assignments = &on_conflict.assignments;
                        if assignments.is_empty() {
                            return Err(anyhow::anyhow!(
                                "ON CONFLICT DO UPDATE must have at least one assignment"
                            ));
                        }

                        let col_idx_map: HashMap<String, usize> = columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (c.name.clone(), i))
                            .collect();

                        let pk_idx_opt = integer_pk_index(&columns);
                        let mut staged_rows = tables[table_pos].rows.clone();
                        let mut alloc_opt =
                            pk_idx_opt.map(|pk_idx| RowidAllocator::new(&staged_rows, pk_idx));

                        enum StagedOp {
                            Insert(Vec<SimValue>),
                            Update {
                                old_row: Vec<SimValue>,
                                new_row: Vec<SimValue>,
                            },
                        }
                        let mut staged_ops: Vec<StagedOp> = Vec::new();

                        for raw_row in values.iter() {
                            ensure_row_width(&table_name, &columns, raw_row)?;

                            let excluded_row = if let (Some(pk_idx), Some(alloc)) =
                                (pk_idx_opt, alloc_opt.as_mut())
                            {
                                normalize_integer_pk_row(
                                    &table_name,
                                    &columns,
                                    pk_idx,
                                    alloc,
                                    raw_row,
                                )?
                            } else {
                                raw_row.clone()
                            };

                            let target_val = &excluded_row[target_col_idx];

                            let conflict_idx = if target_val.0 == turso_core::Value::Null {
                                None
                            } else {
                                let mut found = None;
                                for (i, r) in staged_rows.iter().enumerate() {
                                    if r[target_col_idx] == *target_val {
                                        if found.is_some() {
                                            return Err(anyhow::anyhow!(
                                                "UNIQUE constraint invariant violated: multiple rows match ON CONFLICT target column '{}' in table '{}'",
                                                on_conflict.target_column,
                                                table_name
                                            ));
                                        }
                                        found = Some(i);
                                    }
                                }
                                found
                            };

                            match conflict_idx {
                                None => {
                                    check_unique_row(
                                        &table_name,
                                        &columns,
                                        &staged_rows,
                                        &excluded_row,
                                        None,
                                    )?;
                                    staged_rows.push(excluded_row.clone());
                                    staged_ops.push(StagedOp::Insert(excluded_row));
                                }
                                Some(conflict_idx) => {
                                    let old_row = staged_rows[conflict_idx].clone();
                                    let mut new_row = old_row.clone();

                                    for a in assignments {
                                        let dst_idx = *col_idx_map.get(&a.column).ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "ON CONFLICT assignment column '{}' does not exist in table '{}'",
                                                a.column,
                                                table_name
                                            )
                                        })?;
                                        let src_idx = *col_idx_map.get(&a.excluded_column).ok_or_else(
                                            || {
                                                anyhow::anyhow!(
                                                    "excluded column '{}' does not exist in table '{}'",
                                                    a.excluded_column,
                                                    table_name
                                                )
                                            },
                                        )?;
                                        new_row[dst_idx] = excluded_row[src_idx].clone();
                                    }

                                    if let (Some(pk_idx), Some(alloc)) =
                                        (pk_idx_opt, alloc_opt.as_mut())
                                    {
                                        validate_integer_pk_row_non_null(
                                            &table_name,
                                            &columns,
                                            pk_idx,
                                            &new_row,
                                        )?;
                                        if let Some(i) = new_row[pk_idx].0.as_int() {
                                            alloc.observe(i);
                                        }
                                    }

                                    check_unique_row(
                                        &table_name,
                                        &columns,
                                        &staged_rows,
                                        &new_row,
                                        Some(conflict_idx),
                                    )?;

                                    staged_rows[conflict_idx].clone_from(&new_row);
                                    staged_ops.push(StagedOp::Update { old_row, new_row });
                                }
                            }
                        }

                        tables[table_pos].rows = staged_rows;
                        for op in staged_ops {
                            match op {
                                StagedOp::Insert(row) => {
                                    tables.record_insert(table_name.clone(), row);
                                }
                                StagedOp::Update {
                                    old_row, new_row, ..
                                } => {
                                    tables.record_update(table_name.clone(), old_row, new_row);
                                }
                            }
                        }
                        Ok(vec![])
                    }
                }
            }
        }
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
        match self {
            Begin::Deferred => {
                tables.create_deferred_snapshot();
            }
            Begin::Immediate => {
                tables.create_snapshot(TransactionMode::Write);
            }
            Begin::Concurrent => {
                tables.create_snapshot(TransactionMode::Concurrent);
            }
        };

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
        // First pass: find rows to update and compute old/new values
        let (updates, columns) = {
            let table = tables.iter().find(|t| t.name == self.table);
            let table = if let Some(table) = table {
                table
            } else {
                return Err(anyhow::anyhow!(
                    "Table {} does not exist. UPDATE statement ignored.",
                    self.table
                ));
            };

            let t2 = table.clone();
            let columns = table.columns.clone();

            let updates: Vec<(usize, Vec<SimValue>, Vec<SimValue>)> = table
                .rows
                .iter()
                .enumerate()
                .filter(|(_, r)| self.predicate.test(r, &t2))
                .map(|(row_idx, old_row)| {
                    let mut new_row = old_row.clone();
                    for (column, set_value) in &self.set_values {
                        if let Some((idx, _)) =
                            columns.iter().enumerate().find(|(_, c)| &c.name == column)
                        {
                            match set_value {
                                SetValue::Simple(v) => {
                                    new_row[idx] = v.clone();
                                }
                                SetValue::CaseWhen {
                                    condition,
                                    then_value,
                                    else_column,
                                } => {
                                    #[cfg(debug_assertions)]
                                    turso_assert_eq!(else_column, column);
                                    if condition.test(old_row, &t2) {
                                        new_row[idx] = then_value.clone();
                                    }
                                }
                            }
                        }
                    }
                    (row_idx, old_row.clone(), new_row)
                })
                .collect();

            (updates, columns)
        };

        if let Some(pk_idx) = integer_pk_index(&columns) {
            for (_, _, new_row) in &updates {
                validate_integer_pk_row_non_null(&self.table, &columns, pk_idx, new_row)?;
            }
        }

        let updated_row_indices: std::collections::HashSet<usize> =
            updates.iter().map(|(idx, _, _)| *idx).collect();

        if let Some(table) = tables.iter().find(|t| t.name == self.table) {
            for (col_idx, col) in columns.iter().enumerate() {
                if !col.has_unique_or_pk() {
                    continue;
                }
                let new_values: Vec<_> = updates
                    .iter()
                    .map(|(_, _, new)| &new[col_idx])
                    .filter(|v| v.0 != turso_core::Value::Null)
                    .collect();
                // check duplicates within batch
                for (i, v) in new_values.iter().enumerate() {
                    if new_values[..i].contains(v) {
                        return Err(anyhow::anyhow!(
                            "UNIQUE constraint: duplicate '{}' in table '{}'",
                            col.name,
                            self.table
                        ));
                    }
                }
                // check against existing rows not being updated
                for v in &new_values {
                    let conflicts = table
                        .rows
                        .iter()
                        .enumerate()
                        .filter(|(row_idx, _)| !updated_row_indices.contains(row_idx))
                        .any(|(_, r)| &r[col_idx] == *v);
                    if conflicts {
                        return Err(anyhow::anyhow!(
                            "UNIQUE constraint: '{}' already exists in '{}'",
                            col.name,
                            self.table
                        ));
                    }
                }
            }
        }

        // Record the operations for transaction tracking
        for (_, old_row, new_row) in &updates {
            tables.record_update(self.table.clone(), old_row.clone(), new_row.clone());
        }

        // Second pass: apply the updates
        if let Some(table) = tables.iter_mut().find(|t| t.name == self.table) {
            for (row_idx, _, new_row) in &updates {
                table.rows[*row_idx].clone_from(new_row);
            }
        }

        Ok(vec![])
    }
}

impl Shadow for AlterTable {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut<'_>) -> Self::Result {
        // Record operations BEFORE applying them to current_tables.
        // This ensures the operation is recorded with the correct state (e.g., column index
        // before any changes happen).
        // For attached databases, table names are qualified (e.g., "aux1.foo").
        // Extract the database prefix so we can preserve it during renames.
        let db_prefix = self.table_name.split_once('.').map(|(p, _)| p);

        match &self.alter_table_type {
            AlterTableType::RenameTo { new_name } => {
                let qualified_new = match db_prefix {
                    Some(prefix) => format!("{prefix}.{new_name}"),
                    None => new_name.clone(),
                };
                tables.record_rename_table(self.table_name.clone(), qualified_new);
            }
            AlterTableType::AddColumn { column } => {
                tables.record_add_column(self.table_name.clone(), column.clone());
            }
            AlterTableType::DropColumn { column_name } => {
                // Find column index before applying the change
                let col_idx = tables
                    .iter()
                    .find(|t| t.name == self.table_name)
                    .and_then(|t| t.columns.iter().position(|c| c.name == *column_name))
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Column {} does not exist in table {}",
                            column_name,
                            self.table_name
                        )
                    })?;
                tables.record_drop_column(self.table_name.clone(), col_idx);
            }
            AlterTableType::RenameColumn { old, new } => {
                tables.record_rename_column(self.table_name.clone(), old.clone(), new.clone());
            }
            AlterTableType::AlterColumn { old, new } => {
                tables.record_alter_column(self.table_name.clone(), old.clone(), new.clone());
            }
        }

        // Now apply the change to current_tables
        let table = tables
            .iter_mut()
            .find(|t| t.name == self.table_name)
            .ok_or_else(|| anyhow::anyhow!("Table {} does not exist", self.table_name))?;

        match &self.alter_table_type {
            AlterTableType::RenameTo { new_name } => {
                // Preserve the database prefix (e.g., "aux1.") for attached tables
                table.name = match db_prefix {
                    Some(prefix) => format!("{prefix}.{new_name}"),
                    None => new_name.clone(),
                };
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
                            col_name.clone_from(&new.name);
                        }
                    });
                });
            }
            AlterTableType::RenameColumn { old, new } => {
                let col = table.columns.iter_mut().find(|c| c.name == *old).unwrap();
                col.name.clone_from(new);
                table.indexes.iter_mut().for_each(|index| {
                    index.columns.iter_mut().for_each(|(col_name, _)| {
                        if col_name == old {
                            col_name.clone_from(new);
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
            }
        };
        Ok(vec![])
    }
}

impl Shadow for DropIndex {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut<'_>) -> Self::Result {
        // Record the operation BEFORE applying it to current_tables
        tables.record_drop_index(self.table_name.clone(), self.index_name.clone());

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
