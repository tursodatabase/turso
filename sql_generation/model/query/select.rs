use core::panic;
use std::fmt::Display;

pub use ast::Distinctness;
use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use turso_parser::ast::{
    self,
    fmt::{BlankContext, ToTokens},
    OneSelect, SortOrder,
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
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
}

impl From<turso_parser::ast::Select> for Select {
    fn from(select: ast::Select) -> Self {
        Select {
            body: SelectBody {
                select: Box::new(SelectInner::from(select.body.select)),
                compounds: select
                    .body
                    .compounds
                    .into_iter()
                    .map(|compound| CompoundSelect {
                        operator: match compound.operator {
                            ast::CompoundOperator::Union => CompoundOperator::Union,
                            ast::CompoundOperator::UnionAll => CompoundOperator::UnionAll,
                            ast::CompoundOperator::Except => todo!(),
                            ast::CompoundOperator::Intersect => todo!(),
                        },
                        select: Box::new(SelectInner::from(compound.select)),
                    })
                    .collect(),
            },
            order_by: if select.order_by.is_empty() {
                None
            } else {
                Some(OrderBy {
                    columns: select
                        .order_by
                        .into_iter()
                        .map(|col| {
                            (
                                match *col.expr {
                                    ast::Expr::Id(name) => name,
                                    _ => panic!("Only column names are supported in ORDER BY"),
                                }
                                .to_string(),
                                match col.order {
                                    Some(ast::SortOrder::Asc) => SortOrder::Asc,
                                    Some(ast::SortOrder::Desc) => SortOrder::Desc,
                                    None => SortOrder::Asc,
                                },
                            )
                        })
                        .collect(),
                })
            },
            limit: select.limit.map(|l| match *l.expr {
                ast::Expr::Literal(ast::Literal::Numeric(n)) => n.parse::<usize>().unwrap(),
                _ => panic!("Only numeric literals are supported in LIMIT"),
            }),
        }
    }
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
                }),
                compounds: Vec::new(),
            },
            order_by: None,
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
                }),
                compounds: Vec::new(),
            },
            order_by: None,
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
            order_by: left.order_by.or(right.order_by),
            limit: left.limit.or(right.limit),
        }
    }

    pub fn dependencies(&self) -> IndexSet<String> {
        if self.body.select.from.is_none() {
            return IndexSet::new();
        }
        let from = self.body.select.from.as_ref().unwrap();
        let mut tables = IndexSet::new();
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
}

fn as_table(select: &ast::SelectTable) -> &ast::QualifiedName {
    match select {
        ast::SelectTable::Table(name, _, _) => name,
        _ => panic!("Expected table in SELECT clause"),
    }
}

impl From<OneSelect> for SelectInner {
    fn from(select: OneSelect) -> Self {
        match select {
            OneSelect::Select {
                distinctness,
                columns,
                from,
                where_clause,
                group_by: _,
                window_clause: _,
            } => SelectInner {
                distinctness: if distinctness.is_some() {
                    Distinctness::Distinct
                } else {
                    Distinctness::All
                },
                columns: columns
                    .into_iter()
                    .map(|col| match col {
                        ast::ResultColumn::Expr(expr, alias) => {
                            if let Some(alias) = alias {
                                panic!("Aliases in SELECT columns are not supported: {}", alias);
                            }
                            ResultColumn::Expr(Predicate(*expr))
                        }
                        ast::ResultColumn::Star => ResultColumn::Star,
                        ast::ResultColumn::TableStar(name) => {
                            panic!("Table stars in SELECT columns are not supported: {}", name);
                        }
                    })
                    .collect(),
                from: from.map(|f| FromClause {
                    table: as_table(&f.select).to_string(),
                    joins: f
                        .joins
                        .into_iter()
                        .map(|join| JoinedTable {
                            table: as_table(&join.table).to_string(),
                            join_type: match join.operator {
                                ast::JoinOperator::TypedJoin(Some(ast::JoinType::INNER)) => {
                                    JoinType::Inner
                                }
                                ast::JoinOperator::TypedJoin(Some(ast::JoinType::LEFT)) => {
                                    JoinType::Left
                                }
                                ast::JoinOperator::TypedJoin(Some(ast::JoinType::RIGHT)) => {
                                    JoinType::Right
                                }
                                ast::JoinOperator::TypedJoin(Some(ast::JoinType::OUTER)) => {
                                    JoinType::Full
                                }
                                ast::JoinOperator::TypedJoin(Some(ast::JoinType::CROSS)) => {
                                    JoinType::Cross
                                }
                                _ => panic!("Unsupported join type"),
                            },
                            on: Predicate(match join.constraint {
                                Some(ast::JoinConstraint::On(expr)) => *expr,
                                _ => panic!("Expected ON constraint in JOIN"),
                            }),
                        })
                        .collect(),
                }),
                where_clause: where_clause.map_or(Predicate::true_(), |wc| Predicate(*wc)),
            },
            OneSelect::Values(_) => panic!(
                "Conversion from OneSelect to SelectInner is only valid for OneSelect::Select"
            ),
        }
    }
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
            select: Box::new(ast::SelectTable::Table(
                ast::QualifiedName::single(ast::Name::from_string(&self.table)),
                None,
                None,
            )),
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
        let mut deps = vec![self.table.clone()];
        for join in &self.joins {
            deps.push(join.table.clone());
        }
        deps
    }

    pub fn into_join_table(&self, tables: &[Table]) -> JoinTable {
        let first_table = tables
            .iter()
            .find(|t| t.name == self.table)
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
                            ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                ast::Expr::Id(ast::Name::exact(name.clone())).into_boxed(),
                                None,
                            ),
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
                                    ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                        ast::Expr::Id(ast::Name::exact(name.clone())).into_boxed(),
                                        None,
                                    ),
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

#[cfg(test)]
mod select_tests {

    #[test]
    fn test_select_display() {}
}
