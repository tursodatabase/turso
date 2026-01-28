use std::fmt::Display;

pub use ast::Distinctness;
use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use turso_parser::ast::{
    self,
    fmt::{BlankContext, ToTokens},
    SortOrder,
};

use crate::model::table::{JoinTable, JoinType, JoinedTable, Table};

use super::predicate::Predicate;

/// `SELECT` or `RETURNING` result column
// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
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
pub struct Select {
    pub body: SelectBody,
    pub limit: Option<usize>,
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
                        table: SelectTable::Table(table),
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

    pub fn dependencies(&self) -> IndexSet<String> {
        if self.body.select.from.is_none() {
            return IndexSet::new();
        }
        let from = self.body.select.from.as_ref().unwrap();
        let mut tables = IndexSet::new();

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
    pub table: SelectTable,
    /// `JOIN`ed tables
    pub joins: Vec<JoinedTable>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SelectTable {
    Table(String),
    Select(Select),
}

impl FromClause {
    fn to_sql_ast(&self) -> ast::FromClause {
        ast::FromClause {
            select: Box::new(match &self.table {
                SelectTable::Table(table) => ast::SelectTable::Table(
                    ast::QualifiedName::single(ast::Name::from_string(table)),
                    None,
                    None,
                ),
                SelectTable::Select(select) => ast::SelectTable::Select(select.to_sql_ast(), None),
            }),
            joins: self
                .joins
                .iter()
                .map(|join| ast::JoinedSelectTable {
                    operator: match join.join_type {
                        JoinType::Inner => ast::JoinOperator::TypedJoin(Some(ast::JoinType::INNER)),
                        JoinType::Left => ast::JoinOperator::TypedJoin(Some(ast::JoinType::LEFT)),
                        JoinType::Right => ast::JoinOperator::TypedJoin(Some(ast::JoinType::RIGHT)),
                        JoinType::Full => ast::JoinOperator::TypedJoin(Some(ast::JoinType::OUTER)),
                        JoinType::Cross => ast::JoinOperator::TypedJoin(Some(ast::JoinType::CROSS)),
                    },
                    table: Box::new(ast::SelectTable::Table(
                        ast::QualifiedName::single(ast::Name::from_string(&join.table)),
                        None,
                        None,
                    )),
                    constraint: Some(ast::JoinConstraint::On(Box::new(join.on.0.clone()))),
                })
                .collect(),
        }
    }

    pub fn dependencies(&self) -> Vec<String> {
        let mut deps = self.table.dependencies();
        for join in &self.joins {
            deps.push(join.table.clone());
        }
        deps
    }

    pub fn into_join_table(&self, tables: &[Table]) -> JoinTable {
        let self_table = if let SelectTable::Table(table) = &self.table {
            table.clone()
        } else {
            unimplemented!("into_join_table is only implemented for Table");
        };

        let first_table = tables
            .iter()
            .find(|t| t.name == self_table)
            .expect("Table not found");

        let mut join_table = JoinTable {
            tables: vec![first_table.clone()],
            rows: first_table.rows.clone(),
        };

        for join in &self.joins {
            let joined_table = tables
                .iter()
                .find(|t| t.name == join.table)
                .expect("Joined table not found");

            join_table.tables.push(joined_table.clone());

            match join.join_type {
                JoinType::Inner => {
                    // Implement inner join logic
                    // take a cartesian product of the rows
                    let all_row_pairs = join_table
                        .rows
                        .clone()
                        .into_iter()
                        .cartesian_product(joined_table.rows.iter());

                    let mut new_rows = Vec::new();
                    for (row1, row2) in all_row_pairs {
                        let row = row1.iter().chain(row2.iter()).cloned().collect::<Vec<_>>();

                        let is_in = join.on.test(&row, &join_table);

                        if is_in {
                            new_rows.push(row);
                        }
                    }
                    join_table.rows = new_rows;
                }
                _ => todo!(),
            }
        }
        join_table
    }
}

impl Select {
    pub fn to_sql_ast(&self) -> ast::Select {
        ast::Select {
            with: None,
            body: ast::SelectBody {
                select: ast::OneSelect::Select {
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
                                ast::ResultColumn::Expr(expr.0.clone().into_boxed(), None)
                            }
                            ResultColumn::Star => ast::ResultColumn::Star,
                            ResultColumn::Column(name) => {
                                let expr = if let Some((table, col)) = name.split_once('.') {
                                    ast::Expr::Qualified(
                                        ast::Name::exact(table.to_string()),
                                        ast::Name::exact(col.to_string()),
                                    )
                                } else {
                                    ast::Expr::Id(ast::Name::exact(name.clone()))
                                };
                                ast::ResultColumn::Expr(expr.into_boxed(), None)
                            }
                        })
                        .collect(),
                    from: self.body.select.from.as_ref().map(|f| f.to_sql_ast()),
                    where_clause: Some(self.body.select.where_clause.0.clone().into_boxed()),
                    group_by: None,
                    window_clause: Vec::new(),
                },
                compounds: self
                    .body
                    .compounds
                    .iter()
                    .map(|compound| ast::CompoundSelect {
                        operator: match compound.operator {
                            CompoundOperator::Union => ast::CompoundOperator::Union,
                            CompoundOperator::UnionAll => ast::CompoundOperator::UnionAll,
                        },
                        select: ast::OneSelect::Select {
                            distinctness: Some(compound.select.distinctness),
                            columns: compound
                                .select
                                .columns
                                .iter()
                                .map(|col| match col {
                                    ResultColumn::Expr(expr) => {
                                        ast::ResultColumn::Expr(expr.0.clone().into_boxed(), None)
                                    }
                                    ResultColumn::Star => ast::ResultColumn::Star,
                                    ResultColumn::Column(name) => {
                                        let expr = if let Some((table, col)) = name.split_once('.')
                                        {
                                            ast::Expr::Qualified(
                                                ast::Name::exact(table.to_string()),
                                                ast::Name::exact(col.to_string()),
                                            )
                                        } else {
                                            ast::Expr::Id(ast::Name::exact(name.clone()))
                                        };
                                        ast::ResultColumn::Expr(expr.into_boxed(), None)
                                    }
                                })
                                .collect(),
                            from: compound.select.from.as_ref().map(|f| f.to_sql_ast()),
                            where_clause: Some(compound.select.where_clause.0.clone().into_boxed()),
                            group_by: None,
                            window_clause: Vec::new(),
                        },
                    })
                    .collect(),
            },
            order_by: self
                .body
                .select
                .order_by
                .as_ref()
                .map(|o| {
                    o.columns
                        .iter()
                        .map(|(name, order)| ast::SortedColumn {
                            expr: ast::Expr::Id(ast::Name::exact(name.clone())).into_boxed(),
                            order: match order {
                                SortOrder::Asc => Some(ast::SortOrder::Asc),
                                SortOrder::Desc => Some(ast::SortOrder::Desc),
                            },
                            nulls: None,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            limit: self.limit.map(|l| ast::Limit {
                expr: ast::Expr::Literal(ast::Literal::Numeric(l.to_string())).into_boxed(),
                offset: None,
            }),
        }
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_sql_ast().displayer(&BlankContext).fmt(f)
    }
}

impl SelectTable {
    pub fn dependencies(&self) -> Vec<String> {
        match self {
            SelectTable::Table(table) => vec![table.to_owned()],
            SelectTable::Select(select) => {
                if let Some(from) = &select.body.select.from {
                    let mut dependencies = from.table.dependencies();
                    dependencies.extend(
                        from.joins
                            .iter()
                            .map(|joined_table| joined_table.table.clone()),
                    );
                    dependencies
                } else {
                    vec![]
                }
            }
        }
    }
}
