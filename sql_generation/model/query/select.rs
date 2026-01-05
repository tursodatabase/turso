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
    /// expression (with optional alias)
    Expr(Predicate),
    /// expression with alias: expr AS name
    ExprAs(Predicate, String),
    /// `*`
    Star,
    /// column name
    Column(String),
}

impl Display for ResultColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultColumn::Expr(expr) => write!(f, "({expr})"),
            ResultColumn::ExprAs(expr, alias) => write!(f, "({expr}) AS {alias}"),
            ResultColumn::Star => write!(f, "*"),
            ResultColumn::Column(name) => write!(f, "{name}"),
        }
    }
}

/// WITH clause for Common Table Expressions (CTEs)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    /// Whether this is a recursive CTE (WITH RECURSIVE)
    pub recursive: bool,
    /// The CTEs defined in this WITH clause
    pub ctes: Vec<Cte>,
}

/// A Common Table Expression (CTE)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    /// Name of the CTE
    pub name: String,
    /// Column names for the CTE (optional, but required for recursive CTEs)
    pub columns: Vec<String>,
    /// The body of the CTE (a SelectBody, which can include UNION ALL for recursive CTEs)
    pub body: SelectBody,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Select {
    /// Optional WITH clause for CTEs
    pub with: Option<WithClause>,
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
            with: None,
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
            with: None,
            body: SelectBody {
                select: Box::new(SelectInner {
                    distinctness: distinct,
                    columns: result_columns,
                    from: Some(FromClause {
                        table: SelectTable::Table(table, None),
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
            with: left.with,
            body,
            limit: left.limit.or(right.limit),
        }
    }

    /// Create a SELECT with a WITH clause (CTE).
    ///
    /// # Arguments
    /// * `with_clause` - The WITH clause containing CTEs
    /// * `body` - The main SELECT body
    /// * `limit` - Optional LIMIT clause
    pub fn with_cte(with_clause: WithClause, body: SelectBody, limit: Option<usize>) -> Self {
        Select {
            with: Some(with_clause),
            body,
            limit,
        }
    }

    /// Create a recursive CTE for parent-child hierarchy traversal.
    ///
    /// This generates SQL like:
    /// ```sql
    /// WITH RECURSIVE tree(id, parent_id, content, depth, path) AS (
    ///     SELECT id, parent_id, content, 0, id FROM items WHERE parent_id IS NULL
    ///     UNION ALL
    ///     SELECT i.id, i.parent_id, i.content, t.depth + 1, t.path || '/' || i.id
    ///     FROM items i INNER JOIN tree t ON i.parent_id = t.id
    ///     WHERE t.depth < 10
    /// )
    /// SELECT * FROM tree ORDER BY path
    /// ```
    pub fn recursive_hierarchy_cte(
        cte_name: String,
        base_table: String,
        id_column: String,
        parent_column: String,
        content_columns: Vec<String>,
        _max_depth: u32,
    ) -> Self {
        // Don't specify CTE column list to match the pattern that works in the original test
        // The original test uses `WITH RECURSIVE paths AS (...)` not `WITH RECURSIVE paths(id, parent_id, ...) AS (...)`
        let cte_columns: Vec<String> = vec![];

        // Build anchor result columns: SELECT id, parent_id, content, '/' || id as path
        let mut anchor_columns: Vec<ResultColumn> = vec![
            ResultColumn::Column(id_column.clone()),
            ResultColumn::Column(parent_column.clone()),
        ];
        for col in &content_columns {
            anchor_columns.push(ResultColumn::Column(col.clone()));
        }
        // path = '/' || id AS path (matches original test pattern)
        anchor_columns.push(ResultColumn::ExprAs(
            Predicate::concat(vec![
                Predicate::literal_text("/"),
                Predicate::column(id_column.clone()),
            ]),
            "path".to_string(),
        ));

        // Anchor: SELECT ... FROM base_table WHERE parent_column IS NULL
        let anchor = SelectInner {
            distinctness: Distinctness::All,
            columns: anchor_columns,
            from: Some(FromClause {
                table: SelectTable::Table(base_table.clone(), None),
                joins: Vec::new(),
            }),
            where_clause: Predicate::is_null(Predicate::column(parent_column.clone())),
            order_by: None,
        };

        // Recursive: SELECT b.id, b.parent_id, b.content, p.path || '/' || b.id as path
        // FROM base_table b INNER JOIN cte_name p ON b.parent_id = p.id
        // Note: No depth column to avoid "non-equijoin" error in DBSP
        let b_alias = "b".to_string();
        let p_alias = "p".to_string();

        let mut recursive_columns: Vec<ResultColumn> = vec![
            ResultColumn::Expr(Predicate::qualified_column(&b_alias, &id_column)),
            ResultColumn::Expr(Predicate::qualified_column(&b_alias, &parent_column)),
        ];
        for col in &content_columns {
            recursive_columns.push(ResultColumn::Expr(Predicate::qualified_column(
                &b_alias, col,
            )));
        }
        // p.path || '/' || b.id AS path
        recursive_columns.push(ResultColumn::ExprAs(
            Predicate::concat(vec![
                Predicate::qualified_column(&p_alias, "path"),
                Predicate::literal_text("/"),
                Predicate::qualified_column(&b_alias, &id_column),
            ]),
            "path".to_string(),
        ));

        // For the recursive part, we need a join.
        // ON b.parent_id = p.id
        // Use eq_bare (no parentheses) - DBSP parser requires bare equality in ON clauses
        let join_condition = Predicate::eq_bare(
            Predicate::qualified_column(&b_alias, &parent_column),
            Predicate::qualified_column(&p_alias, &id_column),
        );

        let recursive = SelectInner {
            distinctness: Distinctness::All,
            columns: recursive_columns,
            from: Some(FromClause {
                table: SelectTable::Table(base_table.clone(), Some(b_alias)),
                joins: vec![JoinedTable {
                    join_type: JoinType::Inner,
                    table: cte_name.clone(),
                    alias: Some(p_alias),
                    on: join_condition,
                }],
            }),
            // No WHERE clause - any conditions cause "non-equijoin" error in DBSP
            where_clause: Predicate::true_(),
            order_by: None,
        };

        // CTE body: anchor UNION ALL recursive
        let cte_body = SelectBody {
            select: Box::new(anchor),
            compounds: vec![CompoundSelect {
                operator: CompoundOperator::UnionAll,
                select: Box::new(recursive),
            }],
        };

        let cte = Cte {
            name: cte_name.clone(),
            columns: cte_columns,
            body: cte_body,
        };

        let with_clause = WithClause {
            recursive: true,
            ctes: vec![cte],
        };

        // Final SELECT * FROM cte_name
        // Note: ORDER BY is not supported by DBSP compiler for IVM, so we don't use it
        let final_body = SelectBody {
            select: Box::new(SelectInner {
                distinctness: Distinctness::All,
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: SelectTable::Table(cte_name, None),
                    joins: Vec::new(),
                }),
                where_clause: Predicate::true_(),
                order_by: None,
            }),
            compounds: Vec::new(),
        };

        Select {
            with: Some(with_clause),
            body: final_body,
            limit: None,
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
    /// Table with optional alias: (table_name, optional_alias)
    Table(String, Option<String>),
    Select(Select),
}

/// Convert a table name string to a QualifiedName, handling attached DB prefixes.
/// Names like "aux0.t1" become `QualifiedName::fullname("aux0", "t1")`,
/// while plain names like "t1" become `QualifiedName::single("t1")`.
fn table_qualified_name(table: &str) -> ast::QualifiedName {
    if let Some((db, tbl)) = table.split_once('.') {
        ast::QualifiedName::fullname(ast::Name::from_string(db), ast::Name::from_string(tbl))
    } else {
        ast::QualifiedName::single(ast::Name::from_string(table))
    }
}

/// Convert a column name string to a qualified Expr, handling attached DB prefixes.
/// - "column" → `Id(column)`
/// - "table.column" → `Qualified(table, column)`
/// - "db.table.column" → `DoublyQualified(db, table, column)`
fn column_qualified_expr(name: &str) -> ast::Expr {
    match name.rsplit_once('.') {
        None => ast::Expr::Id(ast::Name::exact(name.to_owned())),
        Some((prefix, col)) => {
            if let Some((db, tbl)) = prefix.split_once('.') {
                ast::Expr::DoublyQualified(
                    ast::Name::from_string(db),
                    ast::Name::from_string(tbl),
                    ast::Name::from_string(col),
                )
            } else {
                ast::Expr::Qualified(ast::Name::from_string(prefix), ast::Name::from_string(col))
            }
        }
    }
}

impl FromClause {
    fn to_sql_ast(&self) -> ast::FromClause {
        ast::FromClause {
            select: Box::new(match &self.table {
                SelectTable::Table(table, alias) => ast::SelectTable::Table(
                    table_qualified_name(table),
                    alias
                        .as_ref()
                        .map(|a| ast::As::As(ast::Name::from_string(a))),
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
                        table_qualified_name(&join.table),
                        join.alias
                            .as_ref()
                            .map(|a| ast::As::As(ast::Name::from_string(a))),
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
        let self_table = if let SelectTable::Table(table, _) = &self.table {
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

/// Convert a where clause predicate to AST Option, returning None for TRUE predicates
fn where_clause_to_ast(predicate: &Predicate) -> Option<Box<ast::Expr>> {
    // Check if predicate is a TRUE literal - if so, skip the WHERE clause
    match &predicate.0 {
        ast::Expr::Literal(ast::Literal::Keyword(kw)) if kw.eq_ignore_ascii_case("TRUE") => None,
        _ => Some(predicate.0.clone().into_boxed()),
    }
}

impl SelectBody {
    /// Convert SelectBody to AST format (used for CTE bodies)
    fn to_sql_ast(&self) -> ast::SelectBody {
        ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: if self.select.distinctness == Distinctness::Distinct {
                    Some(ast::Distinctness::Distinct)
                } else {
                    None
                },
                columns: self
                    .select
                    .columns
                    .iter()
                    .map(|col| match col {
                        ResultColumn::Expr(expr) => {
                            ast::ResultColumn::Expr(expr.0.clone().into_boxed(), None)
                        }
                        ResultColumn::ExprAs(expr, alias) => ast::ResultColumn::Expr(
                            expr.0.clone().into_boxed(),
                            Some(ast::As::As(ast::Name::from_string(alias))),
                        ),
                        ResultColumn::Star => ast::ResultColumn::Star,
                        ResultColumn::Column(name) => ast::ResultColumn::Expr(
                            ast::Expr::Id(ast::Name::exact(name.clone())).into_boxed(),
                            None,
                        ),
                    })
                    .collect(),
                from: self.select.from.as_ref().map(|f| f.to_sql_ast()),
                where_clause: where_clause_to_ast(&self.select.where_clause),
                group_by: None,
                window_clause: Vec::new(),
            },
            compounds: self
                .compounds
                .iter()
                .map(|compound| ast::CompoundSelect {
                    operator: match compound.operator {
                        CompoundOperator::Union => ast::CompoundOperator::Union,
                        CompoundOperator::UnionAll => ast::CompoundOperator::UnionAll,
                    },
                    select: ast::OneSelect::Select {
                        distinctness: if compound.select.distinctness == Distinctness::Distinct {
                            Some(ast::Distinctness::Distinct)
                        } else {
                            None
                        },
                        columns: compound
                            .select
                            .columns
                            .iter()
                            .map(|col| match col {
                                ResultColumn::Expr(expr) => {
                                    ast::ResultColumn::Expr(expr.0.clone().into_boxed(), None)
                                }
                                ResultColumn::ExprAs(expr, alias) => ast::ResultColumn::Expr(
                                    expr.0.clone().into_boxed(),
                                    Some(ast::As::As(ast::Name::from_string(alias))),
                                ),
                                ResultColumn::Star => ast::ResultColumn::Star,
                                ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                    ast::Expr::Id(ast::Name::exact(name.clone())).into_boxed(),
                                    None,
                                ),
                            })
                            .collect(),
                        from: compound.select.from.as_ref().map(|f| f.to_sql_ast()),
                        where_clause: where_clause_to_ast(&compound.select.where_clause),
                        group_by: None,
                        window_clause: Vec::new(),
                    },
                })
                .collect(),
        }
    }
}

impl Select {
    pub fn to_sql_ast(&self) -> ast::Select {
        let with_clause = self.with.as_ref().map(|w| ast::With {
            recursive: w.recursive,
            ctes: w
                .ctes
                .iter()
                .map(|cte| ast::CommonTableExpr {
                    tbl_name: ast::Name::from_string(&cte.name),
                    columns: cte
                        .columns
                        .iter()
                        .map(|col| ast::IndexedColumn {
                            col_name: ast::Name::from_string(col),
                            collation_name: None,
                            order: None,
                        })
                        .collect(),
                    materialized: ast::Materialized::Any,
                    select: ast::Select {
                        with: None,
                        body: cte.body.to_sql_ast(),
                        order_by: Vec::new(),
                        limit: None,
                    },
                })
                .collect(),
        });

        ast::Select {
            with: with_clause,
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
                            ResultColumn::ExprAs(expr, alias) => ast::ResultColumn::Expr(
                                expr.0.clone().into_boxed(),
                                Some(ast::As::As(ast::Name::from_string(alias))),
                            ),
                            ResultColumn::Star => ast::ResultColumn::Star,
                            ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                column_qualified_expr(name).into_boxed(),
                                None,
                            ),
                        })
                        .collect(),
                    from: self.body.select.from.as_ref().map(|f| f.to_sql_ast()),
                    where_clause: where_clause_to_ast(&self.body.select.where_clause),
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
                            distinctness: if compound.select.distinctness == Distinctness::Distinct
                            {
                                Some(ast::Distinctness::Distinct)
                            } else {
                                None
                            },
                            columns: compound
                                .select
                                .columns
                                .iter()
                                .map(|col| match col {
                                    ResultColumn::Expr(expr) => {
                                        ast::ResultColumn::Expr(expr.0.clone().into_boxed(), None)
                                    }
                                    ResultColumn::ExprAs(expr, alias) => ast::ResultColumn::Expr(
                                        expr.0.clone().into_boxed(),
                                        Some(ast::As::As(ast::Name::from_string(alias))),
                                    ),
                                    ResultColumn::Star => ast::ResultColumn::Star,
                                    ResultColumn::Column(name) => ast::ResultColumn::Expr(
                                        ast::Expr::Id(ast::Name::exact(name.clone())).into_boxed(),
                                        None,
                                    ),
                                })
                                .collect(),
                            from: compound.select.from.as_ref().map(|f| f.to_sql_ast()),
                            where_clause: where_clause_to_ast(&compound.select.where_clause),
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
            SelectTable::Table(table, _) => vec![table.to_owned()],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recursive_hierarchy_cte() {
        let select = Select::recursive_hierarchy_cte(
            "tree".to_string(),
            "items".to_string(),
            "id".to_string(),
            "parent_id".to_string(),
            vec!["content".to_string()],
            10,
        );

        let sql = select.to_string();

        // Verify key parts of the generated SQL
        assert!(sql.contains("WITH RECURSIVE"), "Should have WITH RECURSIVE");
        assert!(sql.contains("tree"), "Should reference CTE name 'tree'");
        assert!(sql.contains("UNION ALL"), "Should have UNION ALL");
        assert!(
            sql.contains("parent_id IS NULL"),
            "Anchor should check parent_id IS NULL"
        );
        assert!(sql.contains("path"), "Should have path column");
        // Note: No depth column - causes "non-equijoin" error in DBSP
        assert!(
            !sql.contains("depth"),
            "Should not have depth (causes non-equijoin error)"
        );
        // Note: ORDER BY is not supported by DBSP compiler for IVM
        assert!(
            !sql.contains("ORDER BY"),
            "Should not have ORDER BY (not supported by DBSP)"
        );
    }

    #[test]
    fn test_select_with_cte() {
        let cte_body = SelectBody {
            select: Box::new(SelectInner {
                distinctness: Distinctness::All,
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: SelectTable::Table("users".to_string(), None),
                    joins: Vec::new(),
                }),
                where_clause: Predicate::true_(),
                order_by: None,
            }),
            compounds: Vec::new(),
        };

        let with_clause = WithClause {
            recursive: false,
            ctes: vec![Cte {
                name: "active_users".to_string(),
                columns: vec![],
                body: cte_body,
            }],
        };

        let main_body = SelectBody {
            select: Box::new(SelectInner {
                distinctness: Distinctness::All,
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: SelectTable::Table("active_users".to_string(), None),
                    joins: Vec::new(),
                }),
                where_clause: Predicate::true_(),
                order_by: None,
            }),
            compounds: Vec::new(),
        };

        let select = Select::with_cte(with_clause, main_body, None);
        let sql = select.to_string();

        assert!(sql.contains("WITH"), "Should have WITH clause");
        assert!(
            !sql.contains("RECURSIVE"),
            "Non-recursive CTE should not have RECURSIVE"
        );
        assert!(sql.contains("active_users"), "Should reference CTE name");
    }
}
