use std::fmt::Display;

use anyhow::Context;
use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sql_generation::model::{
    query::{
        Create, CreateIndex, Delete, Drop, Insert, Select,
        select::{CompoundOperator, FromClause, ResultColumn, SelectInner},
        transaction::{Begin, Commit, Rollback},
        update::Update,
    },
    table::{JoinTable, JoinType, SimValue, Table, TableContext},
};
use turso_parser::ast::Distinctness;

use crate::{generation::Shadow, runner::env::ShadowTablesMut};

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
    Begin(Begin),
    Commit(Commit),
    Rollback(Rollback),
}

impl Query {
    pub fn dependencies(&self) -> IndexSet<String> {
        match self {
            Query::Select(select) => select.dependencies(),
            Query::Create(_) => IndexSet::new(),
            Query::Insert(Insert::Select { table, .. })
            | Query::Insert(Insert::Values { table, .. })
            | Query::Delete(Delete { table, .. })
            | Query::Update(Update { table, .. })
            | Query::Drop(Drop { table, .. }) => IndexSet::from_iter([table.clone()]),
            Query::CreateIndex(CreateIndex { table_name, .. }) => {
                IndexSet::from_iter([table_name.clone()])
            }
            Query::Begin(_) | Query::Commit(_) | Query::Rollback(_) => IndexSet::new(),
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
            | Query::Drop(Drop { table, .. }) => vec![table.clone()],
            Query::CreateIndex(CreateIndex { table_name, .. }) => vec![table_name.clone()],
            Query::Begin(..) | Query::Commit(..) | Query::Rollback(..) => vec![],
        }
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
            Self::Begin(begin) => write!(f, "{begin}"),
            Self::Commit(commit) => write!(f, "{commit}"),
            Self::Rollback(rollback) => write!(f, "{rollback}"),
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
            Query::Begin(begin) => Ok(begin.shadow(env)),
            Query::Commit(commit) => Ok(commit.shadow(env)),
            Query::Rollback(rollback) => Ok(rollback.shadow(env)),
        }
    }
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
            .push(self.index_name.clone());
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
        if !tables.iter().any(|t| t.name == self.table) {
            // If the table does not exist, we return an error
            return Err(anyhow::anyhow!(
                "Table {} does not exist. DROP statement ignored.",
                self.table
            ));
        }

        tables.retain(|t| t.name != self.table);

        Ok(vec![])
    }
}

impl Shadow for Insert {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

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
        let first_table = tables
            .iter()
            .find(|t| t.name == self.table)
            .context("Table not found")?;

        let mut join_table = JoinTable {
            tables: vec![first_table.clone()],
            rows: Vec::new(),
        };

        for join in &self.joins {
            let joined_table = tables
                .iter()
                .find(|t| t.name == join.table)
                .context("Joined table not found")?;

            join_table.tables.push(joined_table.clone());

            match join.join_type {
                JoinType::Inner => {
                    // Implement inner join logic
                    let join_rows = joined_table
                        .rows
                        .iter()
                        .filter(|row| join.on.test(row, joined_table))
                        .cloned()
                        .collect::<Vec<_>>();
                    // take a cartesian product of the rows
                    let all_row_pairs = join_table
                        .rows
                        .clone()
                        .into_iter()
                        .cartesian_product(join_rows.iter());

                    for (row1, row2) in all_row_pairs {
                        let row = row1.iter().chain(row2.iter()).cloned().collect::<Vec<_>>();

                        let is_in = join.on.test(&row, &join_table);

                        if is_in {
                            join_table.rows.push(row);
                        }
                    }
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
