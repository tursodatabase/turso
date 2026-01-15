//! WHERE clause conditions and generation strategies.
//!
//! This module provides strategies for generating WHERE clause expressions.
//! The main types are now in the expression module; this module provides
//! generation strategies.

use proptest::prelude::*;
use std::fmt;

use std::rc::Rc;

use crate::StatementProfile;
use crate::expression::{
    BinaryOperator, Expression, ExpressionContext, ExpressionKind, ExpressionProfile,
};
use crate::function::{FunctionRegistry, builtin_functions};
use crate::schema::{ColumnDef, DataType, Schema, TableRef};
use crate::select::SelectStatement;
use crate::value::SqlValue;

/// Quantifier for comparison subqueries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryQuantifier {
    /// ANY - condition is true if comparison is true for at least one row.
    Any,
    /// ALL - condition is true if comparison is true for all rows.
    All,
}

impl fmt::Display for SubqueryQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubqueryQuantifier::Any => write!(f, "ANY"),
            SubqueryQuantifier::All => write!(f, "ALL"),
        }
    }
}

/// ORDER BY direction.
#[derive(Debug, Clone, Copy)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// An ORDER BY clause item.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    /// The expression to order by (can be a column, function call, etc.).
    pub expr: Expression,
    pub direction: OrderDirection,
}

impl OrderByItem {
    /// Create an ORDER BY item for a column name (convenience method).
    pub fn column(name: impl Into<String>, direction: OrderDirection) -> Self {
        Self {
            expr: Expression::Column(name.into()),
            direction,
        }
    }
}

impl fmt::Display for OrderByItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.expr, self.direction)
    }
}

/// Generate a comparison operator (BinaryOperator for comparisons).
pub fn comparison_op() -> impl Strategy<Value = BinaryOperator> {
    prop_oneof![
        Just(BinaryOperator::Eq),
        Just(BinaryOperator::Ne),
        Just(BinaryOperator::Lt),
        Just(BinaryOperator::Le),
        Just(BinaryOperator::Gt),
        Just(BinaryOperator::Ge),
    ]
}

/// Generate a logical operator (BinaryOperator for And/Or).
pub fn logical_op() -> impl Strategy<Value = BinaryOperator> {
    prop_oneof![Just(BinaryOperator::And), Just(BinaryOperator::Or),]
}

/// Generate an order direction.
pub fn order_direction() -> impl Strategy<Value = OrderDirection> {
    prop_oneof![Just(OrderDirection::Asc), Just(OrderDirection::Desc),]
}

/// Generate a condition using expressions (supports function calls).
pub fn expression_condition(
    column: &ColumnDef,
    ctx: &ExpressionContext,
) -> BoxedStrategy<Expression> {
    let col_expr = Expression::Column(column.name.clone());
    let data_type = column.data_type;
    let ctx_clone = ctx.clone();

    let comparison = (
        comparison_op(),
        crate::expression::expression_for_type(Some(&data_type), &ctx_clone),
    )
        .prop_map(move |(op, right)| Expression::binary(col_expr.clone(), op, right));

    if column.nullable {
        let col_expr2 = Expression::Column(column.name.clone());
        let col_expr3 = Expression::Column(column.name.clone());
        prop_oneof![
            comparison,
            Just(Expression::is_null(col_expr2)),
            Just(Expression::is_not_null(col_expr3)),
        ]
        .boxed()
    } else {
        comparison.boxed()
    }
}

/// Generate a condition tree for a table (recursive, bounded depth).
///
/// Uses expression-based conditions with builtin functions.
/// When the profile enables subqueries, subquery conditions will be included
/// using tables from the schema.
pub fn condition_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<Expression> {
    let functions = builtin_functions();
    let expr_profile = &profile.generation.expression.base;
    condition_for_table_internal(
        table,
        schema,
        &functions,
        profile,
        expr_profile.condition_max_depth,
    )
}

/// Internal recursive implementation of condition_for_table.
fn condition_for_table_internal(
    table: &TableRef,
    schema: &Schema,
    functions: &FunctionRegistry,
    profile: &StatementProfile,
    depth: u32,
) -> BoxedStrategy<Expression> {
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        return Just(Expression::binary(
            Expression::Value(SqlValue::Integer(1)),
            BinaryOperator::Eq,
            Expression::Value(SqlValue::Integer(1)),
        ))
        .boxed();
    }

    let expr_profile = &profile.generation.expression.base;
    let ctx = ExpressionContext::new(functions.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(expr_profile.condition_expression_max_depth)
        .with_aggregates(false);

    // Create leaf strategies from all filterable columns
    let expr_leaves = filterable.iter().map(|c| expression_condition(c, &ctx));
    let expr_leaf = proptest::strategy::Union::new(expr_leaves).boxed();

    // Build leaf strategy including subqueries if enabled
    let leaf_strategy: BoxedStrategy<Expression> =
        if expr_profile.any_subquery_enabled() && !schema.tables.is_empty() {
            let subquery_total = expr_profile.total_subquery_weight();
            let simple_weight = expr_profile.simple_condition_weight;

            if subquery_total > 0 {
                let sq_tables = schema.tables.clone();
                let profile_clone = profile.clone();
                let outer_table = table.clone();
                proptest::strategy::Union::new_weighted(vec![
                    (simple_weight, expr_leaf),
                    (
                        subquery_total,
                        subquery_condition_for_tables(&outer_table, &sq_tables, &profile_clone),
                    ),
                ])
                .boxed()
            } else {
                expr_leaf
            }
        } else {
            expr_leaf
        };

    if depth == 0 {
        return leaf_strategy;
    }

    // Recursive case
    let table_clone = table.clone();
    let schema_clone = schema.clone();
    let functions_clone = functions.clone();
    let profile_clone = profile.clone();
    leaf_strategy
        .prop_flat_map(move |left| {
            let table_inner = table_clone.clone();
            let schema_inner = schema_clone.clone();
            let funcs_inner = functions_clone.clone();
            let profile_inner = profile_clone.clone();
            condition_for_table_internal(
                &table_inner,
                &schema_inner,
                &funcs_inner,
                &profile_inner,
                depth - 1,
            )
            .prop_map(move |right| Expression::and(left.clone(), right))
        })
        .boxed()
}

/// Generate a subquery condition given available tables.
fn subquery_condition_for_tables(
    outer_table: &TableRef,
    tables: &[TableRef],
    profile: &StatementProfile,
) -> BoxedStrategy<Expression> {
    let expr_profile = &profile.generation.expression.base;
    if tables.is_empty() || !expr_profile.any_subquery_enabled() {
        return Just(Expression::binary(
            Expression::Value(SqlValue::Integer(1)),
            BinaryOperator::Eq,
            Expression::Value(SqlValue::Integer(1)),
        ))
        .boxed();
    }

    // Build weighted strategies for enabled subquery types
    let mut weighted_strategies: Vec<(u32, BoxedStrategy<Expression>)> = vec![];

    if expr_profile.exists_weight > 0 {
        let tables_vec = tables.to_vec();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            expr_profile.exists_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| exists_condition(&table, &profile_clone, false))
                .boxed(),
        ));
    }

    if expr_profile.not_exists_weight > 0 {
        let tables_vec = tables.to_vec();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            expr_profile.not_exists_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| exists_condition(&table, &profile_clone, true))
                .boxed(),
        ));
    }

    if expr_profile.in_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            expr_profile.in_subquery_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| {
                    in_subquery_condition(&outer, &table, &profile_clone, false)
                })
                .boxed(),
        ));
    }

    if expr_profile.not_in_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            expr_profile.not_in_subquery_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| {
                    in_subquery_condition(&outer, &table, &profile_clone, true)
                })
                .boxed(),
        ));
    }

    if expr_profile.any_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            expr_profile.any_subquery_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| {
                    comparison_subquery_condition(
                        &outer,
                        &table,
                        &profile_clone,
                        SubqueryQuantifier::Any,
                    )
                })
                .boxed(),
        ));
    }

    if expr_profile.all_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            expr_profile.all_subquery_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| {
                    comparison_subquery_condition(
                        &outer,
                        &table,
                        &profile_clone,
                        SubqueryQuantifier::All,
                    )
                })
                .boxed(),
        ));
    }

    if weighted_strategies.is_empty() {
        return Just(Expression::binary(
            Expression::Value(SqlValue::Integer(1)),
            BinaryOperator::Eq,
            Expression::Value(SqlValue::Integer(1)),
        ))
        .boxed();
    }

    proptest::strategy::Union::new_weighted(weighted_strategies).boxed()
}

/// Generate an optional WHERE clause for a table with profile.
///
/// When the profile enables subqueries, subquery conditions will be included
/// using tables from the schema.
pub fn optional_where_clause(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<Option<Expression>> {
    let table_clone = table.clone();
    let schema_clone = schema.clone();
    prop_oneof![
        Just(None),
        condition_for_table(&table_clone, &schema_clone, profile).prop_map(Some),
    ]
    .boxed()
}

/// Generate ORDER BY items for a table with profile.
///
/// Uses expression-based ORDER BY with builtin functions.
/// When `order_by_allow_integer_positions` is false, literal values are excluded
/// to prevent integer position references like `ORDER BY 1, 2`.
pub fn order_by_for_table(
    table: &TableRef,
    profile: &StatementProfile,
) -> BoxedStrategy<Vec<OrderByItem>> {
    let expr_profile = &profile.generation.expression.base;
    let max_items = expr_profile.max_order_by_items;
    let expr_max_depth = expr_profile.condition_expression_max_depth;
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        return Just(vec![]).boxed();
    }

    let functions = builtin_functions();
    let mut ctx = ExpressionContext::new(functions)
        .with_columns(table.columns.clone())
        .with_max_depth(expr_max_depth)
        .with_aggregates(false);

    // When integer positions are not allowed, disable value expressions entirely
    // to prevent generating integer literals that would be interpreted as column positions
    if !expr_profile.order_by_allow_integer_positions {
        ctx.profile = ctx.profile.with_weight(ExpressionKind::Value, 0);
    }

    proptest::collection::vec(
        (crate::expression::expression(&ctx), order_direction()),
        0..=max_items,
    )
    .prop_map(|items| {
        items
            .into_iter()
            .map(|(expr, direction)| OrderByItem { expr, direction })
            .collect()
    })
    .boxed()
}

// =============================================================================
// SUBQUERY GENERATION STRATEGIES
// =============================================================================

/// Generate a SelectStatement for use in subqueries.
///
/// The generated subquery will be simple (no nested subqueries in WHERE clause)
/// to prevent infinite recursion.
pub fn subquery_select_for_table(
    table: &TableRef,
    profile: &StatementProfile,
    target_type: Option<DataType>,
) -> BoxedStrategy<SelectStatement> {
    let table_name = table.name.clone();
    let columns = table.columns.clone();
    let table_clone = table.clone();
    let expr_profile = &profile.generation.expression.base;
    let limit_max = expr_profile.subquery_limit_max;

    // Determine what columns to select based on target type
    let columns_strategy: BoxedStrategy<Vec<Expression>> = match target_type {
        Some(data_type) => {
            // For IN/ANY/ALL subqueries, select a single column of the right type
            let matching_cols: Vec<String> = columns
                .iter()
                .filter(|c| c.data_type == data_type)
                .map(|c| c.name.clone())
                .collect();
            if matching_cols.is_empty() {
                // No matching columns, use SELECT *
                Just(vec![]).boxed()
            } else {
                proptest::sample::select(matching_cols)
                    .prop_map(|col| vec![Expression::Column(col)])
                    .boxed()
            }
        }
        None => {
            // For EXISTS, we can select anything (or *)
            prop_oneof![
                Just(vec![]),                                        // SELECT *
                Just(vec![Expression::Value(SqlValue::Integer(1))]), // SELECT 1
            ]
            .boxed()
        }
    };

    // Generate optional simple WHERE clause (no subqueries to avoid infinite recursion)
    let mut simple_profile = StatementProfile::default();
    simple_profile.generation.expression.base =
        ExpressionProfile::simple().with_condition_max_depth(1);
    // Create a minimal schema with just this table for the simple condition
    let simple_schema = Schema {
        tables: Rc::new(vec![table_clone.clone()]),
        indexes: Rc::new(vec![]),
        views: Rc::new(vec![]),
        triggers: Rc::new(vec![]),
    };
    let where_strategy = prop_oneof![
        3 => Just(None),
        2 => condition_for_table(&table_clone, &simple_schema, &simple_profile).prop_map(Some),
    ];

    // Generate optional LIMIT
    let limit_strategy = prop_oneof![
        3 => Just(None),
        1 => (1..=limit_max).prop_map(Some),
    ];

    (columns_strategy, where_strategy, limit_strategy)
        .prop_map(move |(columns, where_clause, limit)| SelectStatement {
            table: table_name.clone(),
            columns,
            where_clause,
            order_by: vec![],
            limit,
            offset: None,
        })
        .boxed()
}

/// Generate an EXISTS or NOT EXISTS condition.
pub fn exists_condition(
    subquery_table: &TableRef,
    profile: &StatementProfile,
    negated: bool,
) -> BoxedStrategy<Expression> {
    subquery_select_for_table(subquery_table, profile, None)
        .prop_map(move |subquery| {
            if negated {
                Expression::not_exists(subquery)
            } else {
                Expression::exists(subquery)
            }
        })
        .boxed()
}

/// Generate an IN or NOT IN subquery condition.
pub fn in_subquery_condition(
    outer_table: &TableRef,
    subquery_table: &TableRef,
    profile: &StatementProfile,
    negated: bool,
) -> BoxedStrategy<Expression> {
    let filterable: Vec<ColumnDef> = outer_table.filterable_columns().cloned().collect();

    if filterable.is_empty() {
        // Fallback to a tautology EXISTS
        return exists_condition(subquery_table, profile, false);
    }

    // Select a column from the outer table
    let profile_clone = profile.clone();
    let subquery_table_clone = subquery_table.clone();

    proptest::sample::select(filterable)
        .prop_flat_map(move |column| {
            let col_name = column.name.clone();
            let data_type = column.data_type;
            let profile_inner = profile_clone.clone();
            let sq_table = subquery_table_clone.clone();

            subquery_select_for_table(&sq_table, &profile_inner, Some(data_type)).prop_map(
                move |subquery| {
                    let expr = Expression::Column(col_name.clone());
                    if negated {
                        Expression::not_in_subquery(expr, subquery)
                    } else {
                        Expression::in_subquery(expr, subquery)
                    }
                },
            )
        })
        .boxed()
}

/// Generate a comparison subquery condition (ANY or ALL).
pub fn comparison_subquery_condition(
    outer_table: &TableRef,
    subquery_table: &TableRef,
    profile: &StatementProfile,
    quantifier: SubqueryQuantifier,
) -> BoxedStrategy<Expression> {
    let filterable: Vec<ColumnDef> = outer_table.filterable_columns().cloned().collect();

    if filterable.is_empty() {
        // Fallback to EXISTS
        return exists_condition(subquery_table, profile, false);
    }

    let profile_clone = profile.clone();
    let subquery_table_clone = subquery_table.clone();

    proptest::sample::select(filterable)
        .prop_flat_map(move |column| {
            let col_name = column.name.clone();
            let data_type = column.data_type;
            let profile_inner = profile_clone.clone();
            let sq_table = subquery_table_clone.clone();

            (
                comparison_op(),
                subquery_select_for_table(&sq_table, &profile_inner, Some(data_type)),
            )
                .prop_map(move |(op, subquery)| {
                    let left = Expression::Column(col_name.clone());
                    match quantifier {
                        SubqueryQuantifier::Any => Expression::any_subquery(left, op, subquery),
                        SubqueryQuantifier::All => Expression::all_subquery(left, op, subquery),
                    }
                })
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::SqlValue;

    #[test]
    fn test_expression_condition_display() {
        // Simple comparison
        let cond = Expression::binary(
            Expression::Column("age".to_string()),
            BinaryOperator::Gt,
            Expression::Value(SqlValue::Integer(18)),
        );
        assert_eq!(cond.to_string(), "\"age\" > 18");

        // IS NULL
        let null_cond = Expression::is_null(Expression::Column("email".to_string()));
        assert_eq!(null_cond.to_string(), "\"email\" IS NULL");

        // AND condition
        let and_cond = Expression::and(cond.clone(), null_cond);
        assert_eq!(and_cond.to_string(), "\"age\" > 18 AND \"email\" IS NULL");
    }

    #[test]
    fn test_expression_with_function_display() {
        let cond = Expression::binary(
            Expression::function_call("UPPER", vec![Expression::Column("name".to_string())]),
            BinaryOperator::Eq,
            Expression::Value(SqlValue::Text("ALICE".to_string())),
        );
        assert_eq!(cond.to_string(), "UPPER(\"name\") = 'ALICE'");
    }

    #[test]
    fn test_order_by_with_expression() {
        let item = OrderByItem {
            expr: Expression::function_call("LENGTH", vec![Expression::Column("name".to_string())]),
            direction: OrderDirection::Desc,
        };
        assert_eq!(item.to_string(), "LENGTH(\"name\") DESC");
    }

    // =========================================================================
    // SUBQUERY CONDITION TESTS
    // =========================================================================

    /// Helper to create a simple SelectStatement for tests.
    fn test_select(table: &str) -> SelectStatement {
        SelectStatement {
            table: table.to_string(),
            columns: vec![],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }
    }

    #[test]
    fn test_select_statement_in_subquery_display() {
        let subquery = test_select("users");
        assert_eq!(subquery.to_string(), "SELECT * FROM \"users\"");

        let subquery_with_cols = SelectStatement {
            table: "users".to_string(),
            columns: vec![Expression::Column("id".to_string())],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        assert_eq!(
            subquery_with_cols.to_string(),
            "SELECT \"id\" FROM \"users\""
        );

        let subquery_with_where = SelectStatement {
            table: "users".to_string(),
            columns: vec![],
            where_clause: Some(Expression::binary(
                Expression::Column("active".to_string()),
                BinaryOperator::Eq,
                Expression::Value(SqlValue::Integer(1)),
            )),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        assert_eq!(
            subquery_with_where.to_string(),
            "SELECT * FROM \"users\" WHERE \"active\" = 1"
        );

        let subquery_with_limit = SelectStatement {
            table: "users".to_string(),
            columns: vec![],
            where_clause: None,
            order_by: vec![],
            limit: Some(10),
            offset: None,
        };
        assert_eq!(
            subquery_with_limit.to_string(),
            "SELECT * FROM \"users\" LIMIT 10"
        );

        let subquery_full = SelectStatement {
            table: "users".to_string(),
            columns: vec![Expression::Column("id".to_string())],
            where_clause: Some(Expression::binary(
                Expression::Column("active".to_string()),
                BinaryOperator::Eq,
                Expression::Value(SqlValue::Integer(1)),
            )),
            order_by: vec![],
            limit: Some(100),
            offset: None,
        };
        assert_eq!(
            subquery_full.to_string(),
            "SELECT \"id\" FROM \"users\" WHERE \"active\" = 1 LIMIT 100"
        );
    }

    #[test]
    fn test_exists_condition_display() {
        let subquery = test_select("orders");
        let cond = Expression::exists(subquery);
        assert_eq!(cond.to_string(), "EXISTS (SELECT * FROM \"orders\")");
    }

    #[test]
    fn test_not_exists_condition_display() {
        let subquery = SelectStatement {
            table: "orders".to_string(),
            columns: vec![],
            where_clause: Some(Expression::binary(
                Expression::Column("user_id".to_string()),
                BinaryOperator::Eq,
                Expression::Value(SqlValue::Integer(1)),
            )),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let cond = Expression::not_exists(subquery);
        assert_eq!(
            cond.to_string(),
            "NOT EXISTS (SELECT * FROM \"orders\" WHERE \"user_id\" = 1)"
        );
    }

    #[test]
    fn test_in_subquery_condition_display() {
        let subquery = SelectStatement {
            table: "departments".to_string(),
            columns: vec![Expression::Column("id".to_string())],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let cond = Expression::in_subquery(Expression::Column("dept_id".to_string()), subquery);
        assert_eq!(
            cond.to_string(),
            "\"dept_id\" IN (SELECT \"id\" FROM \"departments\")"
        );
    }

    #[test]
    fn test_not_in_subquery_condition_display() {
        let subquery = SelectStatement {
            table: "blacklist".to_string(),
            columns: vec![Expression::Column("user_id".to_string())],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let cond = Expression::not_in_subquery(Expression::Column("id".to_string()), subquery);
        assert_eq!(
            cond.to_string(),
            "\"id\" NOT IN (SELECT \"user_id\" FROM \"blacklist\")"
        );
    }

    #[test]
    fn test_any_subquery_condition_display() {
        let subquery = SelectStatement {
            table: "salaries".to_string(),
            columns: vec![Expression::Column("amount".to_string())],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let cond = Expression::any_subquery(
            Expression::Column("salary".to_string()),
            BinaryOperator::Gt,
            subquery,
        );
        assert_eq!(
            cond.to_string(),
            "\"salary\" > ANY (SELECT \"amount\" FROM \"salaries\")"
        );
    }

    #[test]
    fn test_all_subquery_condition_display() {
        let subquery = SelectStatement {
            table: "scores".to_string(),
            columns: vec![Expression::Column("value".to_string())],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let cond = Expression::all_subquery(
            Expression::Column("score".to_string()),
            BinaryOperator::Ge,
            subquery,
        );
        assert_eq!(
            cond.to_string(),
            "\"score\" >= ALL (SELECT \"value\" FROM \"scores\")"
        );
    }

    #[test]
    fn test_subquery_weight_methods() {
        let profile = ExpressionProfile::default();
        // exists(5) + not_exists(3) + in(8) + not_in(4) + any(3) + all(3) = 26
        assert_eq!(profile.total_subquery_weight(), 26);
        assert!(profile.any_subquery_enabled());

        let simple = ExpressionProfile::simple();
        assert_eq!(simple.total_subquery_weight(), 0);
        assert!(!simple.any_subquery_enabled());
    }

    #[test]
    fn test_expression_profile_condition_settings() {
        let profile = ExpressionProfile::complex();
        assert!(profile.any_subquery_enabled());
        assert_eq!(profile.simple_condition_weight, 60);

        let simple = ExpressionProfile::simple();
        assert!(!simple.any_subquery_enabled());
        assert_eq!(simple.simple_condition_weight, 100);
    }

    #[test]
    fn test_order_by_integer_positions_config() {
        // Default allows integer positions
        let profile = ExpressionProfile::default();
        assert!(profile.order_by_allow_integer_positions);

        // Simple profile disables them to avoid column position reference issues
        let simple = ExpressionProfile::simple();
        assert!(!simple.order_by_allow_integer_positions);

        // Can be disabled via builder
        let profile = ExpressionProfile::default().with_order_by_integer_positions(false);
        assert!(!profile.order_by_allow_integer_positions);
    }

    #[test]
    fn test_order_by_no_integers() {
        use crate::schema::{DataType, Table};
        use proptest::strategy::Strategy;
        use proptest::test_runner::TestRunner;

        let table: TableRef = Table::new(
            "test",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("name", DataType::Text),
            ],
        )
        .into();

        // Simple profile has integer positions disabled
        let mut profile = StatementProfile::default();
        profile.generation.expression.base = ExpressionProfile::simple();
        let strategy = order_by_for_table(&table, &profile);

        let mut runner = TestRunner::default();
        for _ in 0..50 {
            let items = strategy.new_tree(&mut runner).unwrap().current();
            for item in &items {
                let expr_str = item.expr.to_string();
                // Should not be a bare integer (but integers in expressions are fine)
                assert!(
                    !expr_str.chars().all(|c| c.is_ascii_digit()),
                    "Found bare integer literal in ORDER BY: {expr_str}"
                );
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn generated_subquery_select_is_valid(
            subquery in {
                use crate::schema::{Table, DataType};
                let table: TableRef = Table::new(
                    "test_table",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                        ColumnDef::new("value", DataType::Integer),
                    ],
                ).into();
                let profile = StatementProfile::default();
                subquery_select_for_table(&table, &profile, None)
            }
        ) {
            let sql = subquery.to_string();
            proptest::prop_assert!(sql.starts_with("SELECT"));
            proptest::prop_assert!(sql.contains("FROM \"test_table\""));
        }
    }

    proptest::proptest! {
        #[test]
        fn generated_condition_with_subqueries_is_valid(
            cond in {
                use crate::schema::{Table, DataType, SchemaBuilder};
                let users_table: TableRef = Table::new(
                    "users",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                        ColumnDef::new("age", DataType::Integer),
                    ],
                ).into();
                let orders_table = Table::new(
                    "orders",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("user_id", DataType::Integer),
                        ColumnDef::new("amount", DataType::Integer),
                    ],
                );
                let schema = SchemaBuilder::new()
                    .add_table((*users_table).clone())
                    .add_table(orders_table)
                    .build();
                let mut profile = StatementProfile::default();
                profile.generation.expression.base = ExpressionProfile::complex();
                condition_for_table(&users_table, &schema, &profile)
            }
        ) {
            let sql = cond.to_string();
            proptest::prop_assert!(!sql.is_empty());
        }
    }
}
