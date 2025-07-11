use std::{collections::HashSet, fmt::Display};

use anyhow::Context;
pub use ast::Distinctness;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use turso_sqlite3_parser::{ast, to_sql_string::ToSqlString};

use crate::{
    generation::Shadow,
    model::{
        query::EmptyContext,
        table::{SimValue, Table},
    },
    runner::env::SimulatorTables,
};

use super::predicate::Predicate;

/// `SELECT` or `RETURNING` result column
// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResultColumn {
    /// expression
    Expr(Predicate),
    /// `*`
    Star,
    /// column name
    Column(String),
}

impl Display for ResultColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultColumn::Expr(expr) => write!(f, "({expr})"),
            ResultColumn::Star => write!(f, "*"),
            ResultColumn::Column(name) => write!(f, "{name}"),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Select {
    pub(crate) body: SelectBody,
    pub(crate) limit: Option<usize>,
}

impl Select {
    pub fn simple(table: String, where_clause: Predicate) -> Self {
        Self::single(
            table,
            vec![ResultColumn::Star],
            where_clause,
            None,
            Distinctness::All,
        )
    }

    pub fn single(
        table: String,
        result_columns: Vec<ResultColumn>,
        where_clause: Predicate,
        limit: Option<usize>,
        distinct: Distinctness,
    ) -> Self {
        Select {
            body: SelectBody {
                select: Box::new(SelectInner {
                    distinctness: distinct,
                    columns: result_columns,
                    from: FromClause {
                        table,
                        joins: Vec::new(),
                    },
                    where_clause,
                }),
                compounds: Vec::new(),
            },
            limit,
        }
    }

    pub fn compound(left: Select, right: Select, operator: CompoundOperator) -> Self {
        let mut body = left.body;
        body.compounds.push(CompoundSelect {
            operator,
            select: Box::new(right.body.select.as_ref().clone()),
        });
        Select {
            body,
            limit: left.limit.or(right.limit),
        }
    }

    pub(crate) fn dependencies(&self) -> HashSet<String> {
        let mut tables = HashSet::new();
        tables.insert(self.body.select.from.table.clone());

        tables.extend(self.body.select.from.dependencies());

        for compound in &self.body.compounds {
            tables.extend(compound.select.from.dependencies().into_iter());
        }

        tables
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SelectBody {
    /// first select
    pub select: Box<SelectInner>,
    /// compounds
    pub compounds: Vec<CompoundSelect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SelectInner {
    /// `DISTINCT`
    pub distinctness: Distinctness,
    /// columns
    pub columns: Vec<ResultColumn>,
    /// `FROM` clause
    pub from: FromClause,
    /// `WHERE` clause
    pub where_clause: Predicate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CompoundOperator {
    /// `UNION`
    Union,
    /// `UNION ALL`
    UnionAll,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompoundSelect {
    /// operator
    pub operator: CompoundOperator,
    /// select
    pub select: Box<SelectInner>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FromClause {
    /// table
    pub table: String,
    /// `JOIN`ed tables
    pub joins: Vec<JoinedTable>,
}

impl FromClause {
    fn to_sql_ast(&self) -> ast::FromClause {
        ast::FromClause {
            select: Some(Box::new(ast::SelectTable::Table(
                ast::QualifiedName::single(ast::Name(self.table.clone())),
                None,
                None,
            ))),
            joins: if self.joins.is_empty() {
                None
            } else {
                Some(
                    self.joins
                        .iter()
                        .map(|join| ast::JoinedSelectTable {
                            operator: match join.join_type {
                                JoinType::Inner => {
                                    ast::JoinOperator::TypedJoin(Some(ast::JoinType::INNER))
                                }
                                JoinType::Left => {
                                    ast::JoinOperator::TypedJoin(Some(ast::JoinType::LEFT))
                                }
                                JoinType::Right => {
                                    ast::JoinOperator::TypedJoin(Some(ast::JoinType::RIGHT))
                                }
                                JoinType::Full => {
                                    ast::JoinOperator::TypedJoin(Some(ast::JoinType::OUTER))
                                }
                                JoinType::Cross => {
                                    ast::JoinOperator::TypedJoin(Some(ast::JoinType::CROSS))
                                }
                            },
                            table: ast::SelectTable::Table(
                                ast::QualifiedName::single(ast::Name(join.table.clone())),
                                None,
                                None,
                            ),
                            constraint: Some(ast::JoinConstraint::On(join.on.0.clone())),
                        })
                        .collect(),
                )
            },
            op: None, // FIXME: this is a temporary fix, we should remove this field
        }
    }

    pub(crate) fn dependencies(&self) -> Vec<String> {
        let mut deps = vec![self.table.clone()];
        for join in &self.joins {
            deps.push(join.table.clone());
        }
        deps
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinedTable {
    /// table name
    pub table: String,
    /// `JOIN` type
    pub join_type: JoinType,
    /// `ON` clause
    pub on: Predicate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinTable {
    pub tables: Vec<Table>,
    pub rows: Vec<Vec<SimValue>>,
}

impl JoinTable {
    pub(crate) fn into_table(self) -> Table {
        let t = Table {
            name: "".to_string(),
            columns: self
                .tables
                .iter()
                .flat_map(|t| {
                    t.columns.iter().map(|c| {
                        let mut c = c.clone();
                        c.name = format!("{}.{}", t.name, c.name);
                        c
                    })
                })
                .collect(),
            rows: self.rows,
        };
        for row in &t.rows {
            assert_eq!(
                row.len(),
                t.columns.len(),
                "Row length does not match column length after join"
            );
        }
        t
    }
}

impl Shadow for FromClause {
    type Result = anyhow::Result<JoinTable>;
    fn shadow(&self, env: &mut SimulatorTables) -> Self::Result {
        let tables = &mut env.tables;

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

                        let as_table = join_table.clone().into_table();
                        let is_in = join.on.test(&row, &as_table);

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

    fn shadow(&self, env: &mut SimulatorTables) -> Self::Result {
        let mut join_table = self.from.shadow(env)?;
        let as_table = join_table.clone().into_table();
        for row in &mut join_table.rows {
            assert_eq!(
                row.len(),
                as_table.columns.len(),
                "Row length does not match column length after join"
            );
        }

        join_table
            .rows
            .retain(|row| self.where_clause.test(row, &as_table));

        if self.distinctness == Distinctness::Distinct {
            join_table.rows.sort_unstable();
            join_table.rows.dedup();
        }

        Ok(join_table)
    }
}

impl Shadow for Select {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, env: &mut SimulatorTables) -> Self::Result {
        let first_result = self.body.select.shadow(env)?;

        let mut rows = first_result.into_table().rows;

        for compound in self.body.compounds.iter() {
            let compound_results = compound.select.shadow(env)?;

            match compound.operator {
                CompoundOperator::Union => {
                    // Union means we need to combine the results, removing duplicates
                    let mut new_rows = compound_results.into_table().rows;
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

impl Select {
    pub fn to_sql_ast(&self) -> ast::Select {
        ast::Select {
            with: None,
            body: ast::SelectBody {
                select: Box::new(ast::OneSelect::Select(Box::new(ast::SelectInner {
                    distinctness: if self.body.select.distinctness == Distinctness::Distinct {
                        Some(ast::Distinctness::Distinct)
                    } else {
                        None
                    },
                    columns: self
                        .body
                        .select
                        .columns
                        .iter()
                        .map(|col| match col {
                            ResultColumn::Expr(expr) => {
                                ast::ResultColumn::Expr(expr.0.clone(), None)
                            }
                            ResultColumn::Star => ast::ResultColumn::Star,
                            ResultColumn::Column(name) => {
                                ast::ResultColumn::Expr(ast::Expr::Id(ast::Id(name.clone())), None)
                            }
                        })
                        .collect(),
                    from: Some(self.body.select.from.to_sql_ast()),
                    where_clause: Some(self.body.select.where_clause.0.clone()),
                    group_by: None,
                    window_clause: None,
                }))),
                compounds: Some(
                    self.body
                        .compounds
                        .iter()
                        .map(|compound| ast::CompoundSelect {
                            operator: match compound.operator {
                                CompoundOperator::Union => ast::CompoundOperator::Union,
                                CompoundOperator::UnionAll => ast::CompoundOperator::UnionAll,
                            },
                            select: Box::new(ast::OneSelect::Select(Box::new(ast::SelectInner {
                                distinctness: Some(compound.select.distinctness),
                                columns: compound
                                    .select
                                    .columns
                                    .iter()
                                    .map(|col| match col {
                                        ResultColumn::Expr(expr) => {
                                            ast::ResultColumn::Expr(expr.0.clone(), None)
                                        }
                                        ResultColumn::Star => ast::ResultColumn::Star,
                                        ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                            ast::Expr::Id(ast::Id(name.clone())),
                                            None,
                                        ),
                                    })
                                    .collect(),
                                from: Some(compound.select.from.to_sql_ast()),
                                where_clause: Some(compound.select.where_clause.0.clone()),
                                group_by: None,
                                window_clause: None,
                            }))),
                        })
                        .collect(),
                ),
            },
            order_by: None,
            limit: self.limit.map(|l| {
                Box::new(ast::Limit {
                    expr: ast::Expr::Literal(ast::Literal::Numeric(l.to_string())),
                    offset: None,
                })
            }),
        }
    }
}
impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql_ast().to_sql_string(&EmptyContext {}))
    }
}

#[cfg(test)]
mod select_tests {

    #[test]
    fn test_select_display() {}
}
