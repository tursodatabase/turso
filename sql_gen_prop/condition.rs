//! WHERE clause conditions and generation strategies.

use proptest::prelude::*;
use std::fmt;

use std::rc::Rc;

use crate::expression::{Expression, ExpressionContext};
use crate::function::{FunctionRegistry, builtin_functions};
use crate::schema::{ColumnDef, DataType, Schema, TableRef};
use crate::select::SelectStatement;
use crate::value::{SqlValue, value_for_type};

// =============================================================================
// SUBQUERY PROFILE (defined first because ConditionProfile depends on it)
// =============================================================================

/// Profile for controlling subquery generation in conditions.
#[derive(Debug, Clone)]
pub struct SubqueryProfile {
    /// Whether to generate EXISTS subqueries.
    pub enable_exists: bool,
    /// Whether to generate NOT EXISTS subqueries.
    pub enable_not_exists: bool,
    /// Whether to generate IN subqueries.
    pub enable_in_subquery: bool,
    /// Whether to generate NOT IN subqueries.
    pub enable_not_in_subquery: bool,
    /// Whether to generate comparison subqueries (ANY/ALL).
    pub enable_comparison_subquery: bool,
    /// Weight for EXISTS conditions relative to other leaf conditions.
    pub exists_weight: u32,
    /// Weight for NOT EXISTS conditions.
    pub not_exists_weight: u32,
    /// Weight for IN subquery conditions.
    pub in_subquery_weight: u32,
    /// Weight for NOT IN subquery conditions.
    pub not_in_subquery_weight: u32,
    /// Weight for ANY comparison subquery conditions.
    pub any_subquery_weight: u32,
    /// Weight for ALL comparison subquery conditions.
    pub all_subquery_weight: u32,
    /// Maximum LIMIT value for subqueries.
    pub subquery_limit_max: u32,
}

impl Default for SubqueryProfile {
    fn default() -> Self {
        Self {
            enable_exists: true,
            enable_not_exists: true,
            enable_in_subquery: true,
            enable_not_in_subquery: true,
            enable_comparison_subquery: true,
            exists_weight: 5,
            not_exists_weight: 3,
            in_subquery_weight: 8,
            not_in_subquery_weight: 4,
            any_subquery_weight: 3,
            all_subquery_weight: 3,
            subquery_limit_max: 100,
        }
    }
}

impl SubqueryProfile {
    /// Create a profile with no subqueries enabled.
    pub fn disabled() -> Self {
        Self {
            enable_exists: false,
            enable_not_exists: false,
            enable_in_subquery: false,
            enable_not_in_subquery: false,
            enable_comparison_subquery: false,
            exists_weight: 0,
            not_exists_weight: 0,
            in_subquery_weight: 0,
            not_in_subquery_weight: 0,
            any_subquery_weight: 0,
            all_subquery_weight: 0,
            subquery_limit_max: 100,
        }
    }

    /// Create a profile that only enables EXISTS subqueries.
    pub fn exists_only() -> Self {
        Self {
            enable_exists: true,
            enable_not_exists: true,
            enable_in_subquery: false,
            enable_not_in_subquery: false,
            enable_comparison_subquery: false,
            exists_weight: 5,
            not_exists_weight: 3,
            in_subquery_weight: 0,
            not_in_subquery_weight: 0,
            any_subquery_weight: 0,
            all_subquery_weight: 0,
            subquery_limit_max: 100,
        }
    }

    /// Create a profile that only enables IN subqueries.
    pub fn in_only() -> Self {
        Self {
            enable_exists: false,
            enable_not_exists: false,
            enable_in_subquery: true,
            enable_not_in_subquery: true,
            enable_comparison_subquery: false,
            exists_weight: 0,
            not_exists_weight: 0,
            in_subquery_weight: 8,
            not_in_subquery_weight: 4,
            any_subquery_weight: 0,
            all_subquery_weight: 0,
            subquery_limit_max: 100,
        }
    }

    /// Builder method to enable/disable EXISTS.
    pub fn with_exists(mut self, enable: bool) -> Self {
        self.enable_exists = enable;
        self
    }

    /// Builder method to enable/disable NOT EXISTS.
    pub fn with_not_exists(mut self, enable: bool) -> Self {
        self.enable_not_exists = enable;
        self
    }

    /// Builder method to enable/disable IN subquery.
    pub fn with_in_subquery(mut self, enable: bool) -> Self {
        self.enable_in_subquery = enable;
        self
    }

    /// Builder method to enable/disable NOT IN subquery.
    pub fn with_not_in_subquery(mut self, enable: bool) -> Self {
        self.enable_not_in_subquery = enable;
        self
    }

    /// Builder method to enable/disable comparison subqueries.
    pub fn with_comparison_subquery(mut self, enable: bool) -> Self {
        self.enable_comparison_subquery = enable;
        self
    }

    /// Returns the total weight for all enabled subquery condition types.
    pub fn total_weight(&self) -> u32 {
        let mut weight = 0;
        if self.enable_exists {
            weight += self.exists_weight;
        }
        if self.enable_not_exists {
            weight += self.not_exists_weight;
        }
        if self.enable_in_subquery {
            weight += self.in_subquery_weight;
        }
        if self.enable_not_in_subquery {
            weight += self.not_in_subquery_weight;
        }
        if self.enable_comparison_subquery {
            weight += self.any_subquery_weight + self.all_subquery_weight;
        }
        weight
    }

    /// Returns true if any subquery conditions are enabled.
    pub fn any_enabled(&self) -> bool {
        self.enable_exists
            || self.enable_not_exists
            || self.enable_in_subquery
            || self.enable_not_in_subquery
            || self.enable_comparison_subquery
    }
}

// =============================================================================
// CONDITION GENERATION PROFILE
// =============================================================================

/// Profile for controlling WHERE clause and condition generation.
#[derive(Debug, Clone)]
pub struct ConditionProfile {
    /// Maximum depth for condition trees (AND/OR nesting).
    pub max_depth: u32,
    /// Maximum number of ORDER BY items.
    pub max_order_by_items: usize,
    /// Maximum depth for expressions within conditions.
    pub expression_max_depth: u32,
    /// Profile for subquery conditions.
    pub subquery_profile: SubqueryProfile,
    /// Weight for simple (non-subquery) conditions relative to subquery conditions.
    /// Higher values make subquery conditions less likely.
    pub simple_condition_weight: u32,
}

impl Default for ConditionProfile {
    fn default() -> Self {
        Self::new()
    }
}

impl ConditionProfile {
    /// Create a new default profile.
    pub fn new() -> Self {
        Self {
            max_depth: 2,
            max_order_by_items: 3,
            expression_max_depth: 1,
            subquery_profile: SubqueryProfile::default(),
            simple_condition_weight: 80,
        }
    }

    /// Create a profile for simple conditions (no nesting, no subqueries).
    pub fn simple() -> Self {
        Self {
            max_depth: 0,
            max_order_by_items: 1,
            expression_max_depth: 0,
            subquery_profile: SubqueryProfile::disabled(),
            simple_condition_weight: 100,
        }
    }

    /// Create a profile for complex conditions (with subqueries).
    pub fn complex() -> Self {
        Self {
            max_depth: 4,
            max_order_by_items: 5,
            expression_max_depth: 2,
            subquery_profile: SubqueryProfile::default(),
            simple_condition_weight: 60,
        }
    }

    /// Builder method to set the max depth.
    pub fn with_max_depth(mut self, depth: u32) -> Self {
        self.max_depth = depth;
        self
    }

    /// Builder method to set the max ORDER BY items.
    pub fn with_max_order_by_items(mut self, count: usize) -> Self {
        self.max_order_by_items = count;
        self
    }

    /// Builder method to set the expression max depth.
    pub fn with_expression_max_depth(mut self, depth: u32) -> Self {
        self.expression_max_depth = depth;
        self
    }

    /// Builder method to set the subquery profile.
    pub fn with_subquery_profile(mut self, profile: SubqueryProfile) -> Self {
        self.subquery_profile = profile;
        self
    }

    /// Builder method to set the simple condition weight.
    pub fn with_simple_condition_weight(mut self, weight: u32) -> Self {
        self.simple_condition_weight = weight;
        self
    }

    /// Create a profile with no subqueries.
    pub fn no_subqueries() -> Self {
        Self {
            subquery_profile: SubqueryProfile::disabled(),
            simple_condition_weight: 100,
            ..Self::new()
        }
    }
}

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

// =============================================================================
// COMPARISON AND LOGICAL OPERATORS
// =============================================================================

/// Comparison operators for WHERE clauses.
#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Eq => write!(f, "="),
            ComparisonOp::Ne => write!(f, "!="),
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::Le => write!(f, "<="),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::Ge => write!(f, ">="),
        }
    }
}

/// Logical operators for combining conditions.
#[derive(Debug, Clone, Copy)]
pub enum LogicalOp {
    And,
    Or,
}

impl fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOp::And => write!(f, "AND"),
            LogicalOp::Or => write!(f, "OR"),
        }
    }
}

// =============================================================================
// CONDITION ENUM
// =============================================================================

/// A WHERE clause condition.
#[derive(Debug, Clone)]
pub enum Condition {
    /// A comparison between two expressions (e.g., `col = value` or `UPPER(name) = 'ALICE'`).
    Comparison {
        left: Expression,
        op: ComparisonOp,
        right: Expression,
    },
    /// Legacy comparison with column name and SqlValue (for backward compatibility).
    SimpleComparison {
        column: String,
        op: ComparisonOp,
        value: SqlValue,
    },
    /// Check if an expression is NULL.
    IsNull { expr: Expression },
    /// Check if an expression is NOT NULL.
    IsNotNull { expr: Expression },
    /// Legacy IS NULL with column name (for backward compatibility).
    SimpleIsNull { column: String },
    /// Legacy IS NOT NULL with column name (for backward compatibility).
    SimpleIsNotNull { column: String },
    /// Logical AND of two conditions.
    And(Box<Condition>, Box<Condition>),
    /// Logical OR of two conditions.
    Or(Box<Condition>, Box<Condition>),

    // =========================================================================
    // SUBQUERY CONDITIONS
    // =========================================================================
    /// EXISTS subquery condition (e.g., `EXISTS (SELECT ...)`).
    Exists { subquery: Box<SelectStatement> },
    /// NOT EXISTS subquery condition (e.g., `NOT EXISTS (SELECT ...)`).
    NotExists { subquery: Box<SelectStatement> },
    /// IN subquery condition (e.g., `expr IN (SELECT ...)`).
    InSubquery {
        expr: Expression,
        subquery: Box<SelectStatement>,
    },
    /// NOT IN subquery condition (e.g., `expr NOT IN (SELECT ...)`).
    NotInSubquery {
        expr: Expression,
        subquery: Box<SelectStatement>,
    },
    /// Comparison with ANY subquery (e.g., `expr op ANY (SELECT ...)`).
    AnySubquery {
        left: Expression,
        op: ComparisonOp,
        subquery: Box<SelectStatement>,
    },
    /// Comparison with ALL subquery (e.g., `expr op ALL (SELECT ...)`).
    AllSubquery {
        left: Expression,
        op: ComparisonOp,
        subquery: Box<SelectStatement>,
    },
}

impl Condition {
    /// Create a comparison condition between two expressions.
    pub fn comparison(left: Expression, op: ComparisonOp, right: Expression) -> Self {
        Condition::Comparison { left, op, right }
    }

    /// Create an IS NULL condition on an expression.
    pub fn is_null(expr: Expression) -> Self {
        Condition::IsNull { expr }
    }

    /// Create an IS NOT NULL condition on an expression.
    pub fn is_not_null(expr: Expression) -> Self {
        Condition::IsNotNull { expr }
    }

    /// Create an EXISTS subquery condition.
    pub fn exists(subquery: SelectStatement) -> Self {
        Condition::Exists {
            subquery: Box::new(subquery),
        }
    }

    /// Create a NOT EXISTS subquery condition.
    pub fn not_exists(subquery: SelectStatement) -> Self {
        Condition::NotExists {
            subquery: Box::new(subquery),
        }
    }

    /// Create an IN subquery condition.
    pub fn in_subquery(expr: Expression, subquery: SelectStatement) -> Self {
        Condition::InSubquery {
            expr,
            subquery: Box::new(subquery),
        }
    }

    /// Create a NOT IN subquery condition.
    pub fn not_in_subquery(expr: Expression, subquery: SelectStatement) -> Self {
        Condition::NotInSubquery {
            expr,
            subquery: Box::new(subquery),
        }
    }

    /// Create a comparison with ANY subquery condition.
    pub fn any_subquery(left: Expression, op: ComparisonOp, subquery: SelectStatement) -> Self {
        Condition::AnySubquery {
            left,
            op,
            subquery: Box::new(subquery),
        }
    }

    /// Create a comparison with ALL subquery condition.
    pub fn all_subquery(left: Expression, op: ComparisonOp, subquery: SelectStatement) -> Self {
        Condition::AllSubquery {
            left,
            op,
            subquery: Box::new(subquery),
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Condition::Comparison { left, op, right } => {
                write!(f, "{left} {op} {right}")
            }
            Condition::SimpleComparison { column, op, value } => {
                write!(f, "\"{column}\" {op} {value}")
            }
            Condition::IsNull { expr } => write!(f, "{expr} IS NULL"),
            Condition::IsNotNull { expr } => write!(f, "{expr} IS NOT NULL"),
            Condition::SimpleIsNull { column } => write!(f, "\"{column}\" IS NULL"),
            Condition::SimpleIsNotNull { column } => write!(f, "\"{column}\" IS NOT NULL"),
            Condition::And(left, right) => write!(f, "({left} AND {right})"),
            Condition::Or(left, right) => write!(f, "({left} OR {right})"),
            // Subquery conditions
            Condition::Exists { subquery } => write!(f, "EXISTS ({subquery})"),
            Condition::NotExists { subquery } => write!(f, "NOT EXISTS ({subquery})"),
            Condition::InSubquery { expr, subquery } => write!(f, "{expr} IN ({subquery})"),
            Condition::NotInSubquery { expr, subquery } => {
                write!(f, "{expr} NOT IN ({subquery})")
            }
            Condition::AnySubquery { left, op, subquery } => {
                write!(f, "{left} {op} ANY ({subquery})")
            }
            Condition::AllSubquery { left, op, subquery } => {
                write!(f, "{left} {op} ALL ({subquery})")
            }
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

/// Generate a comparison operator.
pub fn comparison_op() -> impl Strategy<Value = ComparisonOp> {
    prop_oneof![
        Just(ComparisonOp::Eq),
        Just(ComparisonOp::Ne),
        Just(ComparisonOp::Lt),
        Just(ComparisonOp::Le),
        Just(ComparisonOp::Gt),
        Just(ComparisonOp::Ge),
    ]
}

/// Generate a logical operator.
pub fn logical_op() -> impl Strategy<Value = LogicalOp> {
    prop_oneof![Just(LogicalOp::And), Just(LogicalOp::Or),]
}

/// Generate an order direction.
pub fn order_direction() -> impl Strategy<Value = OrderDirection> {
    prop_oneof![Just(OrderDirection::Asc), Just(OrderDirection::Desc),]
}

/// Generate a simple condition for a column (using legacy SimpleComparison).
pub fn simple_condition(column: &ColumnDef) -> BoxedStrategy<Condition> {
    let col_name = column.name.clone();
    let col_name2 = column.name.clone();
    let col_name3 = column.name.clone();
    let value_profile = crate::value::ValueProfile::default();

    let comparison = (
        comparison_op(),
        value_for_type(&column.data_type, false, &value_profile),
    )
        .prop_map(move |(op, value)| Condition::SimpleComparison {
            column: col_name.clone(),
            op,
            value,
        });

    if column.nullable {
        prop_oneof![
            comparison,
            Just(Condition::SimpleIsNull {
                column: col_name2.clone()
            }),
            Just(Condition::SimpleIsNotNull {
                column: col_name3.clone()
            }),
        ]
        .boxed()
    } else {
        comparison.boxed()
    }
}

/// Generate a condition using expressions (supports function calls).
pub fn expression_condition(
    column: &ColumnDef,
    ctx: &ExpressionContext,
) -> BoxedStrategy<Condition> {
    let col_expr = Expression::Column(column.name.clone());
    let data_type = column.data_type;
    let ctx_clone = ctx.clone();

    let comparison = (
        comparison_op(),
        crate::expression::expression_for_type(Some(&data_type), &ctx_clone),
    )
        .prop_map(move |(op, right)| Condition::Comparison {
            left: col_expr.clone(),
            op,
            right,
        });

    if column.nullable {
        let col_expr2 = Expression::Column(column.name.clone());
        let col_expr3 = Expression::Column(column.name.clone());
        prop_oneof![
            comparison,
            Just(Condition::IsNull { expr: col_expr2 }),
            Just(Condition::IsNotNull { expr: col_expr3 }),
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
    profile: &ConditionProfile,
) -> BoxedStrategy<Condition> {
    let functions = builtin_functions();
    condition_for_table_internal(table, schema, &functions, profile, profile.max_depth)
}

/// Internal recursive implementation of condition_for_table.
fn condition_for_table_internal(
    table: &TableRef,
    schema: &Schema,
    functions: &FunctionRegistry,
    profile: &ConditionProfile,
    depth: u32,
) -> BoxedStrategy<Condition> {
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        return Just(Condition::Comparison {
            left: Expression::Value(SqlValue::Integer(1)),
            op: ComparisonOp::Eq,
            right: Expression::Value(SqlValue::Integer(1)),
        })
        .boxed();
    }

    let ctx = ExpressionContext::new(functions.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(profile.expression_max_depth)
        .with_aggregates(false);

    // Create leaf strategies from all filterable columns
    let expr_leaves: Vec<BoxedStrategy<Condition>> = filterable
        .iter()
        .map(|c| expression_condition(c, &ctx))
        .collect();
    let expr_leaf = proptest::strategy::Union::new(expr_leaves).boxed();

    // Build leaf strategy including subqueries if enabled
    let subquery_profile = &profile.subquery_profile;
    let leaf_strategy: BoxedStrategy<Condition> =
        if subquery_profile.any_enabled() && !schema.tables.is_empty() {
            let subquery_total = subquery_profile.total_weight();
            let simple_weight = profile.simple_condition_weight;

            if subquery_total > 0 {
                let sq_tables = schema.tables.clone();
                let sq_profile = subquery_profile.clone();
                let outer_table = table.clone();
                proptest::strategy::Union::new_weighted(vec![
                    (simple_weight, expr_leaf),
                    (
                        subquery_total,
                        subquery_condition_for_tables(&outer_table, &sq_tables, &sq_profile),
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
            .prop_map(move |right| Condition::And(Box::new(left.clone()), Box::new(right)))
        })
        .boxed()
}

/// Generate a subquery condition given available tables.
fn subquery_condition_for_tables(
    outer_table: &TableRef,
    tables: &[TableRef],
    profile: &SubqueryProfile,
) -> BoxedStrategy<Condition> {
    if tables.is_empty() || !profile.any_enabled() {
        return Just(Condition::Comparison {
            left: Expression::Value(SqlValue::Integer(1)),
            op: ComparisonOp::Eq,
            right: Expression::Value(SqlValue::Integer(1)),
        })
        .boxed();
    }

    // Build weighted strategies for enabled subquery types
    let mut weighted_strategies: Vec<(u32, BoxedStrategy<Condition>)> = vec![];

    if profile.enable_exists && profile.exists_weight > 0 {
        let tables_vec = tables.to_vec();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            profile.exists_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| exists_condition(&table, &profile_clone, false))
                .boxed(),
        ));
    }

    if profile.enable_not_exists && profile.not_exists_weight > 0 {
        let tables_vec = tables.to_vec();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            profile.not_exists_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| exists_condition(&table, &profile_clone, true))
                .boxed(),
        ));
    }

    if profile.enable_in_subquery && profile.in_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            profile.in_subquery_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| {
                    in_subquery_condition(&outer, &table, &profile_clone, false)
                })
                .boxed(),
        ));
    }

    if profile.enable_not_in_subquery && profile.not_in_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            profile.not_in_subquery_weight,
            proptest::sample::select(tables_vec)
                .prop_flat_map(move |table| {
                    in_subquery_condition(&outer, &table, &profile_clone, true)
                })
                .boxed(),
        ));
    }

    if profile.enable_comparison_subquery && profile.any_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            profile.any_subquery_weight,
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

    if profile.enable_comparison_subquery && profile.all_subquery_weight > 0 {
        let tables_vec = tables.to_vec();
        let outer = outer_table.clone();
        let profile_clone = profile.clone();
        weighted_strategies.push((
            profile.all_subquery_weight,
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
        return Just(Condition::Comparison {
            left: Expression::Value(SqlValue::Integer(1)),
            op: ComparisonOp::Eq,
            right: Expression::Value(SqlValue::Integer(1)),
        })
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
    profile: &ConditionProfile,
) -> BoxedStrategy<Option<Condition>> {
    let table_clone = table.clone();
    let schema_clone = schema.clone();
    let profile_clone = profile.clone();
    prop_oneof![
        Just(None),
        condition_for_table(&table_clone, &schema_clone, &profile_clone).prop_map(Some),
    ]
    .boxed()
}

/// Generate ORDER BY items for a table with profile.
///
/// Uses expression-based ORDER BY with builtin functions.
pub fn order_by_for_table(
    table: &TableRef,
    profile: &ConditionProfile,
) -> BoxedStrategy<Vec<OrderByItem>> {
    let max_items = profile.max_order_by_items;
    let expr_max_depth = profile.expression_max_depth;
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        return Just(vec![]).boxed();
    }

    let functions = builtin_functions();
    let ctx = ExpressionContext::new(functions)
        .with_columns(table.columns.clone())
        .with_max_depth(expr_max_depth)
        .with_aggregates(false);

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
    profile: &SubqueryProfile,
    target_type: Option<DataType>,
) -> BoxedStrategy<SelectStatement> {
    let table_name = table.name.clone();
    let columns = table.columns.clone();
    let table_clone = table.clone();
    let limit_max = profile.subquery_limit_max;

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
    let simple_profile = ConditionProfile::simple().with_max_depth(1);
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
    profile: &SubqueryProfile,
    negated: bool,
) -> BoxedStrategy<Condition> {
    subquery_select_for_table(subquery_table, profile, None)
        .prop_map(move |subquery| {
            if negated {
                Condition::not_exists(subquery)
            } else {
                Condition::exists(subquery)
            }
        })
        .boxed()
}

/// Generate an IN or NOT IN subquery condition.
pub fn in_subquery_condition(
    outer_table: &TableRef,
    subquery_table: &TableRef,
    profile: &SubqueryProfile,
    negated: bool,
) -> BoxedStrategy<Condition> {
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
                        Condition::not_in_subquery(expr, subquery)
                    } else {
                        Condition::in_subquery(expr, subquery)
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
    profile: &SubqueryProfile,
    quantifier: SubqueryQuantifier,
) -> BoxedStrategy<Condition> {
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
                        SubqueryQuantifier::Any => Condition::any_subquery(left, op, subquery),
                        SubqueryQuantifier::All => Condition::all_subquery(left, op, subquery),
                    }
                })
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_condition_display() {
        let cond = Condition::SimpleComparison {
            column: "age".to_string(),
            op: ComparisonOp::Gt,
            value: SqlValue::Integer(18),
        };
        assert_eq!(cond.to_string(), "\"age\" > 18");

        let null_cond = Condition::SimpleIsNull {
            column: "email".to_string(),
        };
        assert_eq!(null_cond.to_string(), "\"email\" IS NULL");

        let and_cond = Condition::And(Box::new(cond.clone()), Box::new(null_cond));
        assert_eq!(and_cond.to_string(), "(\"age\" > 18 AND \"email\" IS NULL)");
    }

    #[test]
    fn test_expression_condition_display() {
        let cond = Condition::Comparison {
            left: Expression::function_call("UPPER", vec![Expression::Column("name".to_string())]),
            op: ComparisonOp::Eq,
            right: Expression::Value(SqlValue::Text("ALICE".to_string())),
        };
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
            where_clause: Some(Condition::SimpleComparison {
                column: "active".to_string(),
                op: ComparisonOp::Eq,
                value: SqlValue::Integer(1),
            }),
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
            where_clause: Some(Condition::SimpleComparison {
                column: "active".to_string(),
                op: ComparisonOp::Eq,
                value: SqlValue::Integer(1),
            }),
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
        let cond = Condition::exists(subquery);
        assert_eq!(cond.to_string(), "EXISTS (SELECT * FROM \"orders\")");
    }

    #[test]
    fn test_not_exists_condition_display() {
        let subquery = SelectStatement {
            table: "orders".to_string(),
            columns: vec![],
            where_clause: Some(Condition::SimpleComparison {
                column: "user_id".to_string(),
                op: ComparisonOp::Eq,
                value: SqlValue::Integer(1),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let cond = Condition::not_exists(subquery);
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
        let cond = Condition::in_subquery(Expression::Column("dept_id".to_string()), subquery);
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
        let cond = Condition::not_in_subquery(Expression::Column("id".to_string()), subquery);
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
        let cond = Condition::any_subquery(
            Expression::Column("salary".to_string()),
            ComparisonOp::Gt,
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
        let cond = Condition::all_subquery(
            Expression::Column("score".to_string()),
            ComparisonOp::Ge,
            subquery,
        );
        assert_eq!(
            cond.to_string(),
            "\"score\" >= ALL (SELECT \"value\" FROM \"scores\")"
        );
    }

    #[test]
    fn test_subquery_profile_total_weight() {
        let profile = SubqueryProfile::default();
        // exists(5) + not_exists(3) + in(8) + not_in(4) + any(3) + all(3) = 26
        assert_eq!(profile.total_weight(), 26);

        let disabled = SubqueryProfile::disabled();
        assert_eq!(disabled.total_weight(), 0);

        let exists_only = SubqueryProfile::exists_only();
        // exists(5) + not_exists(3) = 8
        assert_eq!(exists_only.total_weight(), 8);

        let in_only = SubqueryProfile::in_only();
        // in(8) + not_in(4) = 12
        assert_eq!(in_only.total_weight(), 12);
    }

    #[test]
    fn test_subquery_profile_any_enabled() {
        let profile = SubqueryProfile::default();
        assert!(profile.any_enabled());

        let disabled = SubqueryProfile::disabled();
        assert!(!disabled.any_enabled());
    }

    #[test]
    fn test_condition_profile_with_subqueries() {
        let profile = ConditionProfile::complex();
        assert!(profile.subquery_profile.any_enabled());
        assert_eq!(profile.simple_condition_weight, 60);

        let simple = ConditionProfile::simple();
        assert!(!simple.subquery_profile.any_enabled());
        assert_eq!(simple.simple_condition_weight, 100);

        let no_subqueries = ConditionProfile::no_subqueries();
        assert!(!no_subqueries.subquery_profile.any_enabled());
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
                let profile = SubqueryProfile::default();
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
                let profile = ConditionProfile::complex();
                condition_for_table(&users_table, &schema, &profile)
            }
        ) {
            let sql = cond.to_string();
            proptest::prop_assert!(!sql.is_empty());
        }
    }
}
