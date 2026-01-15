//! SELECT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;
use std::ops::RangeInclusive;

use crate::expression::{
    BinaryOperator, Expression, ExpressionContext, ExpressionKind, ExpressionProfile,
};
use crate::function::builtin_functions;
use crate::profile::StatementProfile;
use crate::schema::{ColumnDef, DataType, Schema, TableRef};
use crate::value::SqlValue;

// =============================================================================
// ORDER BY TYPES
// =============================================================================

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

/// Generate an order direction.
pub fn order_direction() -> impl Strategy<Value = OrderDirection> {
    prop_oneof![Just(OrderDirection::Asc), Just(OrderDirection::Desc),]
}

// =============================================================================
// SELECT STATEMENT PROFILE
// =============================================================================

/// Profile for controlling SELECT statement generation.
#[derive(Debug, Clone)]
pub struct SelectProfile {
    /// Maximum depth for expressions in SELECT list.
    pub expression_max_depth: u32,
    /// Whether to allow aggregate functions.
    pub allow_aggregates: bool,
    /// Weight for SELECT * (empty column list).
    pub select_star_weight: u32,
    /// Weight for expression list in SELECT.
    pub expression_list_weight: u32,
    /// Weight for column subsequence in SELECT.
    pub column_list_weight: u32,
    /// Range for number of expressions in SELECT list.
    pub expression_count_range: RangeInclusive<usize>,
    /// Range for LIMIT clause values.
    pub limit_range: RangeInclusive<u32>,
    /// Range for OFFSET clause values.
    pub offset_range: RangeInclusive<u32>,
    /// Expression profile for SELECT expressions.
    pub expression_profile: ExpressionProfile,
}

impl Default for SelectProfile {
    fn default() -> Self {
        Self {
            expression_max_depth: 2,
            allow_aggregates: true,
            select_star_weight: 3,
            expression_list_weight: 7,
            column_list_weight: 5,
            expression_count_range: 1..=5,
            limit_range: 1..=1000,
            offset_range: 0..=100,
            expression_profile: ExpressionProfile::default(),
        }
    }
}

impl SelectProfile {
    /// Create a profile for simple SELECT queries.
    pub fn simple() -> Self {
        Self {
            expression_max_depth: 1,
            allow_aggregates: false,
            select_star_weight: 5,
            expression_list_weight: 3,
            column_list_weight: 7,
            expression_count_range: 1..=3,
            limit_range: 1..=100,
            offset_range: 0..=10,
            expression_profile: ExpressionProfile::simple(),
        }
    }

    /// Create a profile for complex SELECT queries.
    pub fn complex() -> Self {
        Self {
            expression_max_depth: 4,
            allow_aggregates: true,
            select_star_weight: 1,
            expression_list_weight: 10,
            column_list_weight: 3,
            expression_count_range: 1..=10,
            limit_range: 1..=10000,
            offset_range: 0..=1000,
            expression_profile: ExpressionProfile::function_heavy(),
        }
    }

    /// Builder method to set expression max depth.
    pub fn with_expression_max_depth(mut self, depth: u32) -> Self {
        self.expression_max_depth = depth;
        self
    }

    /// Builder method to set whether aggregates are allowed.
    pub fn with_aggregates(mut self, allow: bool) -> Self {
        self.allow_aggregates = allow;
        self
    }

    /// Builder method to set SELECT * weight.
    pub fn with_select_star_weight(mut self, weight: u32) -> Self {
        self.select_star_weight = weight;
        self
    }

    /// Builder method to set expression list weight.
    pub fn with_expression_list_weight(mut self, weight: u32) -> Self {
        self.expression_list_weight = weight;
        self
    }

    /// Builder method to set column list weight.
    pub fn with_column_list_weight(mut self, weight: u32) -> Self {
        self.column_list_weight = weight;
        self
    }

    /// Builder method to set expression count range.
    pub fn with_expression_count_range(mut self, range: RangeInclusive<usize>) -> Self {
        self.expression_count_range = range;
        self
    }

    /// Builder method to set LIMIT range.
    pub fn with_limit_range(mut self, range: RangeInclusive<u32>) -> Self {
        self.limit_range = range;
        self
    }

    /// Builder method to set OFFSET range.
    pub fn with_offset_range(mut self, range: RangeInclusive<u32>) -> Self {
        self.offset_range = range;
        self
    }

    /// Builder method to set expression profile.
    pub fn with_expression_profile(mut self, profile: ExpressionProfile) -> Self {
        self.expression_profile = profile;
        self
    }
}

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub table: String,
    /// The columns/expressions in the SELECT list. Empty means SELECT *.
    pub columns: Vec<Expression>,
    pub where_clause: Option<Expression>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl SelectStatement {
    /// Returns true if this SELECT has a LIMIT clause without an ORDER BY.
    ///
    /// Queries with LIMIT but no ORDER BY may return different rows between
    /// database implementations since the order is undefined.
    pub fn has_unordered_limit(&self) -> bool {
        self.limit.is_some() && self.order_by.is_empty()
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;

        if self.columns.is_empty() {
            write!(f, "*")?;
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            write!(f, "{}", cols.join(", "))?;
        }

        write!(f, " FROM \"{}\"", self.table)?;

        if let Some(cond) = &self.where_clause {
            write!(f, " WHERE {cond}")?;
        }

        if !self.order_by.is_empty() {
            let orders: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            write!(f, " ORDER BY {}", orders.join(", "))?;
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }

        Ok(())
    }
}

/// Generate a SELECT statement for a table with profile.
pub fn select_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<SelectStatement> {
    let table_name = table.name.clone();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let functions = builtin_functions();

    // Extract profile values from the SelectProfile
    let select_profile = profile.select_profile();
    let expression_max_depth = select_profile.expression_max_depth;
    let allow_aggregates = select_profile.allow_aggregates;
    let select_star_weight = select_profile.select_star_weight;
    let expression_list_weight = select_profile.expression_list_weight;
    let column_list_weight = select_profile.column_list_weight;
    let expression_count_range = select_profile.expression_count_range.clone();
    let limit_range = select_profile.limit_range.clone();
    let offset_range = select_profile.offset_range.clone();

    // Build expression context for generating expressions
    let ctx = ExpressionContext::new(functions, schema.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(expression_max_depth)
        .with_aggregates(allow_aggregates);

    // Generate either SELECT * or a list of expressions
    let columns_strategy = proptest::strategy::Union::new_weighted(vec![
        (
            select_star_weight,
            Just(vec![]).boxed(), // SELECT *
        ),
        (
            expression_list_weight,
            proptest::collection::vec(crate::expression::expression(&ctx), expression_count_range)
                .boxed(),
        ),
        (
            column_list_weight,
            proptest::sample::subsequence(col_names.clone(), 1..=col_names.len())
                .prop_map(|cols| cols.into_iter().map(Expression::Column).collect::<Vec<_>>())
                .boxed(),
        ),
    ]);

    (
        columns_strategy,
        optional_where_clause(table, schema, profile),
        order_by_for_table(table, profile),
        proptest::option::of(limit_range),
        proptest::option::of(offset_range),
    )
        .prop_map(
            move |(columns, where_clause, order_by, limit, offset)| SelectStatement {
                table: table_name.clone(),
                columns,
                where_clause,
                order_by,
                limit,
                offset: if limit.is_some() { offset } else { None },
            },
        )
        .boxed()
}

// =============================================================================
// WHERE CLAUSE AND CONDITION GENERATION
// =============================================================================

/// Generate a comparison operator.
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

/// Generate a condition expression for a single column.
///
/// This creates a comparison like `col = value` or `col IS NULL`.
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
    functions: &crate::function::FunctionRegistry,
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
    let ctx = ExpressionContext::new(functions.clone(), schema.clone())
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
                    comparison_subquery_condition(&outer, &table, &profile_clone, false)
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
                    comparison_subquery_condition(&outer, &table, &profile_clone, true)
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
    let mut ctx = ExpressionContext::new(functions, Schema::default())
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
    use std::rc::Rc;

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
    is_all: bool,
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
                    if is_all {
                        Expression::all_subquery(left, op, subquery)
                    } else {
                        Expression::any_subquery(left, op, subquery)
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
    fn test_select_display() {
        let stmt = SelectStatement {
            table: "users".to_string(),
            columns: vec![
                Expression::Column("id".to_string()),
                Expression::Column("name".to_string()),
            ],
            where_clause: Some(Expression::binary(
                Expression::Column("age".to_string()),
                BinaryOperator::Ge,
                Expression::Value(SqlValue::Integer(21)),
            )),
            order_by: vec![OrderByItem::column("name", OrderDirection::Asc)],
            limit: Some(10),
            offset: Some(5),
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"age\" >= 21 ORDER BY \"name\" ASC LIMIT 10 OFFSET 5"
        );
    }

    #[test]
    fn test_select_with_function() {
        let stmt = SelectStatement {
            table: "users".to_string(),
            columns: vec![
                Expression::Column("id".to_string()),
                Expression::function_call("UPPER", vec![Expression::Column("name".to_string())]),
            ],
            where_clause: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "SELECT \"id\", UPPER(\"name\") FROM \"users\"");
    }

    proptest::proptest! {
        #[test]
        fn generated_select_is_valid(
            stmt in {
                let table = crate::schema::Table::new(
                    "test",
                    vec![
                        crate::schema::ColumnDef::new("id", crate::schema::DataType::Integer).primary_key(),
                        crate::schema::ColumnDef::new("name", crate::schema::DataType::Text),
                        crate::schema::ColumnDef::new("age", crate::schema::DataType::Integer),
                    ],
                );
                let schema = crate::schema::SchemaBuilder::new().add_table(table.clone()).build();
                let table_ref: crate::schema::TableRef = table.into();
                select_for_table(&table_ref, &schema, &StatementProfile::default())
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("SELECT"));
            proptest::prop_assert!(sql.contains("FROM \"test\""));
        }
    }

    #[test]
    fn test_select_generates_functions() {
        use proptest::strategy::Strategy;
        use proptest::test_runner::TestRunner;

        let table = crate::schema::Table::new(
            "test",
            vec![
                crate::schema::ColumnDef::new("id", crate::schema::DataType::Integer).primary_key(),
                crate::schema::ColumnDef::new("name", crate::schema::DataType::Text),
                crate::schema::ColumnDef::new("age", crate::schema::DataType::Integer),
            ],
        );
        let schema = crate::schema::SchemaBuilder::new()
            .add_table(table.clone())
            .build();
        let table_ref: crate::schema::TableRef = table.into();
        let strategy = select_for_table(&table_ref, &schema, &StatementProfile::default());

        let mut runner = TestRunner::default();
        let mut found_function = false;

        for _ in 0..50 {
            let stmt = strategy.new_tree(&mut runner).unwrap().current();
            let sql = stmt.to_string();
            // Check if any column expression contains a function (has parentheses that aren't just quotes)
            if sql.contains("(") && !sql.starts_with("SELECT *") {
                found_function = true;
                println!("Found function in: {sql}");
                break;
            }
        }

        assert!(
            found_function,
            "Expected to generate at least one SELECT with function calls in 50 attempts"
        );
    }
}
