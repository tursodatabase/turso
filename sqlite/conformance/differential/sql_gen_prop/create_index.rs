//! CREATE INDEX statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::create_table::identifier_excluding;
use crate::expression::{Expression, ExpressionContext, ExpressionProfile, expression};
use crate::function::builtin_functions;
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};
use crate::select::OrderDirection;

// =============================================================================
// CREATE INDEX PROFILE
// =============================================================================

/// Profile for controlling CREATE INDEX statement generation.
#[derive(Debug, Clone)]
pub struct CreateIndexProfile {
    /// Maximum number of columns in an index.
    pub max_columns: usize,
    /// Probability (0–100) that an index column is an expression instead of a plain column.
    pub expression_probability: u32,
}

impl Default for CreateIndexProfile {
    fn default() -> Self {
        Self {
            max_columns: 4,
            expression_probability: 20,
        }
    }
}

impl CreateIndexProfile {
    /// Builder method to set max columns.
    pub fn with_max_columns(mut self, count: usize) -> Self {
        self.max_columns = count;
        self
    }

    /// Builder method to set expression probability.
    pub fn with_expression_probability(mut self, pct: u32) -> Self {
        self.expression_probability = pct;
        self
    }
}

/// What kind of entry an index column holds.
#[derive(Debug, Clone)]
pub enum IndexColumnKind {
    /// A plain column reference.
    Column(String),
    /// An arbitrary expression (CAST, binary ops, function calls, etc.).
    Expression(Expression),
}

/// A column or expression in an index definition.
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub kind: IndexColumnKind,
    pub direction: Option<OrderDirection>,
}

impl IndexColumn {
    /// Returns true if this index column is an expression (not a plain column).
    pub fn is_expression(&self) -> bool {
        matches!(self.kind, IndexColumnKind::Expression(_))
    }
}

impl fmt::Display for IndexColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            IndexColumnKind::Column(name) => write!(f, "{name}")?,
            // SQLite requires parentheses around expressions in index column lists.
            IndexColumnKind::Expression(expr) => write!(f, "({expr})")?,
        }
        if let Some(dir) = &self.direction {
            write!(f, " {dir}")?;
        }
        Ok(())
    }
}

/// A CREATE INDEX statement.
#[derive(Debug, Clone)]
// NOTE: SQLite's grammar does NOT accept TEMP / TEMPORARY on
// CREATE INDEX. Temp indexes come from either the `temp.` name
// qualifier or from indexing a temp table — there is no
// `temporary` field here. The oracle would otherwise score
// "both errored" as a pass and the fuzzer would silently burn
// its statement budget.
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE ")?;
        if self.unique {
            write!(f, "UNIQUE ")?;
        }

        write!(f, "INDEX ")?;

        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }

        write!(f, "{} ON {} (", self.index_name, self.table_name)?;

        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{})", cols.join(", "))
    }
}

/// Generate an index column with optional direction from a plain column name.
pub fn index_column(col_name: String) -> impl Strategy<Value = IndexColumn> {
    proptest::option::of(prop_oneof![
        Just(OrderDirection::Asc),
        Just(OrderDirection::Desc),
    ])
    .prop_map(move |direction| IndexColumn {
        kind: IndexColumnKind::Column(col_name.clone()),
        direction,
    })
}

/// Generate an expression-based index column with optional direction.
/// The generated expression is guaranteed to reference at least one column,
/// since pure-literal expressions (e.g. `(NULL)`) are useless in indexes
/// and can confuse schema introspection.
fn expression_index_column(ctx: ExpressionContext) -> BoxedStrategy<IndexColumn> {
    (
        expression(&ctx).prop_filter("expression must reference a column", |e| {
            e.contains_column_ref()
        }),
        proptest::option::of(prop_oneof![
            Just(OrderDirection::Asc),
            Just(OrderDirection::Desc),
        ]),
    )
        .prop_map(|(expr, direction)| IndexColumn {
            kind: IndexColumnKind::Expression(expr),
            direction,
        })
        .boxed()
}

/// Build an ExpressionContext suitable for index expressions:
/// depth-limited, no aggregates, no subqueries.
fn index_expression_context(table: &TableRef, schema: &Schema) -> ExpressionContext {
    let functions = builtin_functions();
    let expr_profile = ExpressionProfile::default().with_subqueries_disabled();
    ExpressionContext::new(functions, schema.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(1)
        .with_aggregates(false)
        .with_profile(expr_profile)
}

/// Generate a single index column strategy that is either a plain column or an expression,
/// weighted by `expression_probability`.
fn index_column_strategy(
    col_name: String,
    expr_ctx: ExpressionContext,
    expression_probability: u32,
) -> BoxedStrategy<IndexColumn> {
    let plain_weight = 100u32.saturating_sub(expression_probability);
    let expr_weight = expression_probability;

    if expr_weight == 0 {
        return index_column(col_name).boxed();
    }
    if plain_weight == 0 {
        return expression_index_column(expr_ctx);
    }

    prop_oneof![
        plain_weight => index_column(col_name).boxed(),
        expr_weight => expression_index_column(expr_ctx),
    ]
    .boxed()
}

/// Generate a CREATE INDEX statement for a table with profile.
pub fn create_index_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateIndexStatement> {
    let index_database = table.database.clone();
    let table_name = table.unqualified_name().to_string();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let existing_indexes = schema.index_names_in_database(index_database.as_deref());

    let index_profile = profile.create_index_profile();
    let max_columns = index_profile.max_columns;
    let expression_probability = index_profile.expression_probability;
    let expr_ctx = index_expression_context(table, schema);

    if col_names.is_empty() {
        let index_name = match index_database.as_deref() {
            Some(db) => format!("{db}.idx_empty"),
            None => "idx_empty".to_string(),
        };
        return Just(CreateIndexStatement {
            index_name,
            table_name,
            columns: vec![],
            unique: false,
            if_not_exists: true,
        })
        .boxed();
    }

    (
        identifier_excluding(existing_indexes),
        any::<bool>(), // unique
        any::<bool>(), // if_not_exists
        proptest::sample::subsequence(col_names.clone(), 1..=col_names.len().min(max_columns)),
    )
        .prop_flat_map(
            move |(index_suffix, unique, if_not_exists, selected_cols)| {
                let index_database = index_database.clone();
                let table_name = table_name.clone();
                let expr_ctx = expr_ctx.clone();

                let col_strategies: Vec<_> = selected_cols
                    .into_iter()
                    .map(|name| {
                        index_column_strategy(name, expr_ctx.clone(), expression_probability)
                    })
                    .collect();

                col_strategies.prop_map(move |columns| {
                    let has_expression = columns.iter().any(|c| c.is_expression());
                    let index_name = format!("idx_{table_name}_{index_suffix}");
                    let qualified_index_name = match index_database.as_deref() {
                        Some(db) => format!("{db}.{index_name}"),
                        None => index_name,
                    };

                    CreateIndexStatement {
                        index_name: qualified_index_name,
                        table_name: table_name.clone(),
                        columns,
                        unique: if has_expression { false } else { unique },
                        if_not_exists,
                    }
                })
            },
        )
        .boxed()
}

/// Generate a CREATE INDEX statement for any table with profile.
pub fn create_index(
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateIndexStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let index_profile = profile.create_index_profile();
    let max_columns = index_profile.max_columns;
    let expression_probability = index_profile.expression_probability;

    let existing_indexes_by_database: Vec<(Option<String>, std::collections::HashSet<String>)> =
        std::iter::once(None)
            .chain(schema.attached_databases.iter().cloned().map(Some))
            .map(|db| {
                let existing = schema.index_names_in_database(db.as_deref());
                (db, existing)
            })
            .collect();
    let tables = (*schema.tables).clone();
    let schema_clone = schema.clone();

    proptest::sample::select(tables)
        .prop_flat_map(move |table| {
            let index_database = table.database.clone();
            let table_name = table.unqualified_name().to_string();
            let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
            let existing = existing_indexes_by_database
                .iter()
                .find(|(db, _)| *db == index_database)
                .map(|(_, names)| names.clone())
                .unwrap_or_default();
            let expr_ctx = index_expression_context(&table, &schema_clone);

            if col_names.is_empty() {
                let index_name = match index_database.as_deref() {
                    Some(db) => format!("{db}.idx_empty"),
                    None => "idx_empty".to_string(),
                };
                return Just(CreateIndexStatement {
                    index_name,
                    table_name,
                    columns: vec![],
                    unique: false,
                    if_not_exists: true,
                })
                .boxed();
            }

            (
                identifier_excluding(existing),
                any::<bool>(), // unique
                any::<bool>(), // if_not_exists
                proptest::sample::subsequence(
                    col_names.clone(),
                    1..=col_names.len().min(max_columns),
                ),
            )
                .prop_flat_map(
                    move |(index_suffix, unique, if_not_exists, selected_cols)| {
                        let index_database = index_database.clone();
                        let table_name = table_name.clone();
                        let expr_ctx = expr_ctx.clone();

                        let col_strategies: Vec<_> = selected_cols
                            .into_iter()
                            .map(|name| {
                                index_column_strategy(
                                    name,
                                    expr_ctx.clone(),
                                    expression_probability,
                                )
                            })
                            .collect();

                        col_strategies.prop_map(move |columns| {
                            let has_expression = columns.iter().any(|c| c.is_expression());
                            let index_name = format!("idx_{table_name}_{index_suffix}");
                            let qualified_index_name = match index_database.as_deref() {
                                Some(db) => format!("{db}.{index_name}"),
                                None => index_name,
                            };

                            CreateIndexStatement {
                                index_name: qualified_index_name,
                                table_name: table_name.clone(),
                                columns,
                                unique: if has_expression { false } else { unique },
                                if_not_exists,
                            }
                        })
                    },
                )
                .boxed()
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::SqlValue;

    #[test]
    fn test_create_index_display() {
        let stmt = CreateIndexStatement {
            index_name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec![IndexColumn {
                kind: IndexColumnKind::Column("email".to_string()),
                direction: Some(OrderDirection::Asc),
            }],
            unique: true,
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE UNIQUE INDEX idx_users_email ON users (email ASC)"
        );
    }

    #[test]
    fn test_create_index_multiple_columns() {
        let stmt = CreateIndexStatement {
            index_name: "idx_composite".to_string(),
            table_name: "orders".to_string(),
            columns: vec![
                IndexColumn {
                    kind: IndexColumnKind::Column("user_id".to_string()),
                    direction: None,
                },
                IndexColumn {
                    kind: IndexColumnKind::Column("created_at".to_string()),
                    direction: Some(OrderDirection::Desc),
                },
            ],
            unique: false,
            if_not_exists: true,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX IF NOT EXISTS idx_composite ON orders (user_id, created_at DESC)"
        );
    }

    #[test]
    fn test_create_index_with_temp_schema_name() {
        let stmt = CreateIndexStatement {
            index_name: "temp.idx_temp_users_email".to_string(),
            table_name: "users".to_string(),
            columns: vec![IndexColumn {
                kind: IndexColumnKind::Column("email".to_string()),
                direction: None,
            }],
            unique: false,
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX temp.idx_temp_users_email ON users (email)"
        );
    }

    #[test]
    fn test_create_index_expression_column_display() {
        let stmt = CreateIndexStatement {
            index_name: "idx_cast".to_string(),
            table_name: "t1".to_string(),
            columns: vec![IndexColumn {
                kind: IndexColumnKind::Expression(Expression::Cast {
                    expr: Box::new(Expression::Column("c1".to_string())),
                    target_type: crate::schema::DataType::Text,
                }),
                direction: None,
            }],
            unique: false,
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX idx_cast ON t1 ((CAST(c1 AS TEXT)))"
        );
    }

    #[test]
    fn test_create_index_mixed_columns_display() {
        let stmt = CreateIndexStatement {
            index_name: "idx_mixed".to_string(),
            table_name: "t1".to_string(),
            columns: vec![
                IndexColumn {
                    kind: IndexColumnKind::Column("c1".to_string()),
                    direction: Some(OrderDirection::Asc),
                },
                IndexColumn {
                    kind: IndexColumnKind::Expression(Expression::Value(SqlValue::Integer(1))),
                    direction: Some(OrderDirection::Desc),
                },
            ],
            unique: false,
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE INDEX idx_mixed ON t1 (c1 ASC, (1) DESC)"
        );
    }

    #[test]
    fn test_expression_column_is_expression() {
        let plain = IndexColumn {
            kind: IndexColumnKind::Column("c1".to_string()),
            direction: None,
        };
        assert!(!plain.is_expression());

        let expr = IndexColumn {
            kind: IndexColumnKind::Expression(Expression::Column("c1".to_string())),
            direction: None,
        };
        assert!(expr.is_expression());
    }
}
