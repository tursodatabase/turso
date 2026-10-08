use std::fmt::Display;

use anyhow::Context;
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use sql_generation::model::{
    query::{predicate::Predicate, select::table_qualified_name},
    table::SimValue,
};
use turso_parser::ast::{
    self,
    fmt::{BlankContext, ToTokens},
};

use crate::{generation::Shadow, runner::env::ShadowTablesMut};

const LATERAL_ROWS_ALIAS: &str = "q";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateralSelect {
    pub table: String,
    pub table_alias: String,
    pub columns: Vec<String>,
    pub joins: Vec<LateralJoin>,
    pub where_clause: Predicate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateralJoin {
    pub join_type: LateralJoinType,
    pub form: LateralForm,
    pub alias: String,
    pub table: String,
    pub table_alias: String,
    pub distinct: bool,
    pub columns: Vec<LateralColumn>,
    pub correlation: Comparison,
    pub filter_operator: ast::Operator,
    pub filter: Predicate,
    pub order_by: Vec<(usize, ast::SortOrder)>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LateralJoinType {
    Comma,
    Cross,
    Inner { on: Predicate },
    Left { on: Predicate },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LateralForm {
    Lateral,
    JsonEach,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateralColumn {
    pub source: ColumnRef,
    pub quoted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comparison {
    pub left: ColumnRef,
    pub operator: ast::Operator,
    pub right: ColumnRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnRef {
    Table { alias: String, column: String },
    Lateral { join: usize, column: usize },
}

impl Display for LateralSelect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_sql_ast().displayer(&BlankContext).fmt(f)
    }
}

impl Shadow for LateralSelect {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;

    fn shadow(&self, tables: &mut ShadowTablesMut) -> Self::Result {
        for table in self.dependencies() {
            tables
                .iter()
                .find(|t| t.name == table)
                .with_context(|| format!("table {table} not found"))?;
        }
        Ok(vec![])
    }
}

impl LateralSelect {
    pub fn dependencies(&self) -> IndexSet<String> {
        std::iter::once(&self.table)
            .chain(self.joins.iter().map(|join| &join.table))
            .cloned()
            .collect()
    }

    pub fn with_json_each_joins(&self, joins: &[usize]) -> LateralSelect {
        let mut select = self.clone();
        for &join in joins {
            select.joins[join].form = LateralForm::JsonEach;
        }
        select
    }

    fn to_sql_ast(&self) -> ast::Select {
        let table_columns = self.columns.iter().map(|column| ColumnRef::Table {
            alias: self.table_alias.clone(),
            column: column.clone(),
        });
        let lateral_columns = self.joins.iter().enumerate().flat_map(|(join, lateral)| {
            (0..lateral.columns.len()).map(move |column| ColumnRef::Lateral { join, column })
        });
        let columns = table_columns
            .chain(lateral_columns)
            .map(|column| ast::ResultColumn::Expr(Box::new(self.column_ref_expr(&column)), None))
            .collect();
        let from = ast::FromClause {
            select: Box::new(ast::SelectTable::Table(
                table_qualified_name(&self.table),
                Some(alias(&self.table_alias)),
                None,
            )),
            joins: self
                .joins
                .iter()
                .map(|join| self.joined_table(join))
                .collect(),
        };
        select_ast(columns, from, Some(self.where_clause.0.clone()), false)
    }

    fn joined_table(&self, join: &LateralJoin) -> ast::JoinedSelectTable {
        let (operator, constraint) = match &join.join_type {
            LateralJoinType::Comma => (ast::JoinOperator::Comma, None),
            LateralJoinType::Cross => (
                ast::JoinOperator::TypedJoin(Some(ast::JoinType::INNER | ast::JoinType::CROSS)),
                None,
            ),
            LateralJoinType::Inner { on } => (
                ast::JoinOperator::TypedJoin(Some(ast::JoinType::INNER)),
                Some(ast::JoinConstraint::On(Box::new(on.0.clone()))),
            ),
            LateralJoinType::Left { on } => (
                ast::JoinOperator::TypedJoin(Some(ast::JoinType::LEFT | ast::JoinType::OUTER)),
                Some(ast::JoinConstraint::On(Box::new(on.0.clone()))),
            ),
        };
        let table = match join.form {
            LateralForm::Lateral => ast::SelectTable::Select {
                select: self.lateral_subquery(join),
                alias: Some(alias(&join.alias)),
                lateral: true,
            },
            LateralForm::JsonEach => ast::SelectTable::TableCall(
                ast::QualifiedName::single(ast::Name::exact("json_each".to_string())),
                vec![Box::new(ast::Expr::Subquery(Box::new(
                    self.json_group_array_subquery(join),
                )))],
                Some(alias(&join.alias)),
            ),
        };
        ast::JoinedSelectTable {
            operator,
            table: Box::new(table),
            constraint,
        }
    }

    fn json_group_array_subquery(&self, join: &LateralJoin) -> ast::Select {
        if join.distinct || !join.order_by.is_empty() || join.limit.is_some() {
            return self.json_group_array_over_lateral_subquery(join);
        }
        let values = join
            .columns
            .iter()
            .map(|column| self.lateral_column_expr(column))
            .collect();
        let column = ast::ResultColumn::Expr(Box::new(json_group_array(values)), None);
        self.subquery(join, false, vec![column])
    }

    fn json_group_array_over_lateral_subquery(&self, join: &LateralJoin) -> ast::Select {
        let values = (0..join.columns.len())
            .map(|column| {
                ast::Expr::Qualified(
                    ast::Name::exact(LATERAL_ROWS_ALIAS.to_string()),
                    ast::Name::exact(lateral_column_name(column)),
                )
            })
            .collect();
        let column = ast::ResultColumn::Expr(Box::new(json_group_array(values)), None);
        let from = ast::FromClause {
            select: Box::new(ast::SelectTable::Select {
                select: self.lateral_subquery(join),
                alias: Some(alias(LATERAL_ROWS_ALIAS)),
                lateral: false,
            }),
            joins: Vec::new(),
        };
        select_ast(vec![column], from, None, false)
    }

    fn lateral_subquery(&self, join: &LateralJoin) -> ast::Select {
        let columns = join
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| {
                ast::ResultColumn::Expr(
                    Box::new(self.lateral_column_expr(column)),
                    Some(alias(&lateral_column_name(position))),
                )
            })
            .collect();
        let mut select = self.subquery(join, join.distinct, columns);
        select.order_by = join
            .order_by
            .iter()
            .map(|(column, order)| ast::SortedColumn {
                expr: Box::new(ast::Expr::Id(ast::Name::exact(lateral_column_name(
                    *column,
                )))),
                order: Some(*order),
                nulls: None,
            })
            .collect();
        select.limit = join.limit.map(|limit| ast::Limit {
            expr: Box::new(ast::Expr::Literal(ast::Literal::Numeric(limit.to_string()))),
            offset: None,
        });
        select
    }

    fn subquery(
        &self,
        join: &LateralJoin,
        distinct: bool,
        columns: Vec<ast::ResultColumn>,
    ) -> ast::Select {
        let from = ast::FromClause {
            select: Box::new(ast::SelectTable::Table(
                table_qualified_name(&join.table),
                Some(alias(&join.table_alias)),
                None,
            )),
            joins: Vec::new(),
        };
        let correlation = ast::Expr::Binary(
            Box::new(self.column_ref_expr(&join.correlation.left)),
            join.correlation.operator,
            Box::new(self.column_ref_expr(&join.correlation.right)),
        );
        let where_clause = ast::Expr::Binary(
            Box::new(ast::Expr::Parenthesized(vec![Box::new(correlation)])),
            join.filter_operator,
            Box::new(join.filter.0.clone()),
        );
        select_ast(columns, from, Some(where_clause), distinct)
    }

    fn lateral_column_expr(&self, column: &LateralColumn) -> ast::Expr {
        let value = self.column_ref_expr(&column.source);
        if column.quoted {
            function_call("quote", vec![value])
        } else {
            value
        }
    }

    fn column_ref_expr(&self, column: &ColumnRef) -> ast::Expr {
        match column {
            ColumnRef::Table { alias, column } => ast::Expr::Qualified(
                ast::Name::from_string(alias),
                ast::Name::from_string(column),
            ),
            ColumnRef::Lateral { join, column } => {
                let lateral = &self.joins[*join];
                let alias = ast::Name::from_string(&lateral.alias);
                match lateral.form {
                    LateralForm::Lateral => {
                        ast::Expr::Qualified(alias, ast::Name::exact(lateral_column_name(*column)))
                    }
                    LateralForm::JsonEach if lateral.columns.len() == 1 => {
                        ast::Expr::Qualified(alias, ast::Name::exact("value".to_string()))
                    }
                    LateralForm::JsonEach => {
                        let value =
                            ast::Expr::Qualified(alias, ast::Name::exact("value".to_string()));
                        let position =
                            ast::Expr::Literal(ast::Literal::Numeric(column.to_string()));
                        ast::Expr::Parenthesized(vec![Box::new(ast::Expr::Binary(
                            Box::new(value),
                            ast::Operator::ArrowRightShift,
                            Box::new(position),
                        ))])
                    }
                }
            }
        }
    }
}

fn json_group_array(mut values: Vec<ast::Expr>) -> ast::Expr {
    let item = if values.len() == 1 {
        values.pop().unwrap()
    } else {
        function_call("json_array", values)
    };
    function_call("json_group_array", vec![item])
}

fn lateral_column_name(position: usize) -> String {
    format!("c{position}")
}

fn alias(name: &str) -> ast::As {
    ast::As::As(ast::Name::from_string(name))
}

fn select_ast(
    columns: Vec<ast::ResultColumn>,
    from: ast::FromClause,
    where_clause: Option<ast::Expr>,
    distinct: bool,
) -> ast::Select {
    ast::Select {
        with: None,
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: distinct.then_some(ast::Distinctness::Distinct),
                columns,
                from: Some(from),
                where_clause: where_clause.map(Box::new),
                group_by: None,
                window_clause: Vec::new(),
            },
            compounds: Vec::new(),
        },
        order_by: Vec::new(),
        limit: None,
    }
}

fn function_call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::FunctionCall {
        name: ast::Name::exact(name.to_string()),
        distinctness: None,
        args: args.into_iter().map(Box::new).collect(),
        order_by: Vec::new(),
        within_group: Vec::new(),
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    }
}
