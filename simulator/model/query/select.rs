use std::{collections::HashSet, fmt::Display};

use anyhow::Context;
pub use ast::Distinctness;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use turso_sqlite3_parser::ast::{self, fmt::ToTokens, SortOrder};

use crate::{
    generation::Shadow,
    model::{
        query::EmptyContext,
        table::{JoinTable, JoinType, JoinedTable, SimValue, Table, TableContext},
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

    pub fn expr(expr: Predicate) -> Self {
        Select {
            body: SelectBody {
                select: Box::new(SelectInner {
                    distinctness: Distinctness::All,
                    columns: vec![ResultColumn::Expr(expr)],
                    from: None,
                    where_clause: Predicate::true_(),
                    order_by: None,
                }),
                compounds: Vec::new(),
            },
            limit: None,
        }
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
                    from: Some(FromClause {
                        table,
                        joins: Vec::new(),
                    }),
                    where_clause,
                    order_by: None,
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
        if self.body.select.from.is_none() {
            return HashSet::new();
        }
        let from = self.body.select.from.as_ref().unwrap();
        let mut tables = HashSet::new();
        tables.insert(from.table.clone());

        tables.extend(from.dependencies());

        for compound in &self.body.compounds {
            tables.extend(
                compound
                    .select
                    .from
                    .as_ref()
                    .map(|f| f.dependencies())
                    .unwrap_or(vec![])
                    .into_iter(),
            );
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
pub struct OrderBy {
    pub columns: Vec<(String, SortOrder)>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SelectInner {
    /// `DISTINCT`
    pub distinctness: Distinctness,
    /// columns
    pub columns: Vec<ResultColumn>,
    /// `FROM` clause
    pub from: Option<FromClause>,
    /// `WHERE` clause
    pub where_clause: Predicate,
    /// `ORDER BY` clause
    pub order_by: Option<OrderBy>,
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
                ast::QualifiedName::single(ast::Name::from_str(&self.table)),
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
                                ast::QualifiedName::single(ast::Name::from_str(&join.table)),
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

    fn shadow(&self, env: &mut SimulatorTables) -> Self::Result {
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
            assert!(self
                .columns
                .iter()
                .all(|col| matches!(col, ResultColumn::Expr(_))));

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
                                expr.0.format_with_context(&EmptyContext {}).unwrap()
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

    fn shadow(&self, env: &mut SimulatorTables) -> Self::Result {
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
                            ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                ast::Expr::Id(ast::Name::Ident(name.clone())),
                                None,
                            ),
                        })
                        .collect(),
                    from: self.body.select.from.as_ref().map(|f| f.to_sql_ast()),
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
                                            ast::Expr::Id(ast::Name::Ident(name.clone())),
                                            None,
                                        ),
                                    })
                                    .collect(),
                                from: compound.select.from.as_ref().map(|f| f.to_sql_ast()),
                                where_clause: Some(compound.select.where_clause.0.clone()),
                                group_by: None,
                                window_clause: None,
                            }))),
                        })
                        .collect(),
                ),
            },
            order_by: self.body.select.order_by.as_ref().map(|o| {
                o.columns
                    .iter()
                    .map(|(name, order)| ast::SortedColumn {
                        expr: ast::Expr::Id(ast::Name::Ident(name.clone())),
                        order: match order {
                            SortOrder::Asc => Some(ast::SortOrder::Asc),
                            SortOrder::Desc => Some(ast::SortOrder::Desc),
                        },
                        nulls: None,
                    })
                    .collect()
            }),
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
        self.to_sql_ast().to_fmt_with_context(f, &EmptyContext {})
    }
}

#[cfg(test)]
mod select_tests {

    #[test]
    fn test_select_display() {}
}
