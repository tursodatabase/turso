//! WHERE clause conditions and generation strategies.

use proptest::prelude::*;
use std::fmt;

use crate::expression::{Expression, ExpressionContext};
use crate::function::FunctionRegistry;
use crate::schema::{ColumnDef, Table};
use crate::value::{SqlValue, value_for_type};

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
    IsNull {
        expr: Expression,
    },
    /// Check if an expression is NOT NULL.
    IsNotNull {
        expr: Expression,
    },
    /// Legacy IS NULL with column name (for backward compatibility).
    SimpleIsNull {
        column: String,
    },
    /// Legacy IS NOT NULL with column name (for backward compatibility).
    SimpleIsNotNull {
        column: String,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
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

    let comparison =
        (comparison_op(), value_for_type(&column.data_type, false)).prop_map(move |(op, value)| {
            Condition::SimpleComparison {
                column: col_name.clone(),
                op,
                value,
            }
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
        crate::expression::expression_for_type(Some(&data_type), &ctx_clone, 1),
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
pub fn condition_for_table(table: &Table, max_depth: u32) -> BoxedStrategy<Condition> {
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        // No filterable columns: generate a tautology
        return Just(Condition::SimpleComparison {
            column: "1".to_string(),
            op: ComparisonOp::Eq,
            value: SqlValue::Integer(1),
        })
        .boxed();
    }

    // Create leaf strategies from all filterable columns
    let leaves = filterable.iter().map(|c| simple_condition(c));

    let leaf = proptest::strategy::Union::new(leaves).boxed();

    if max_depth == 0 {
        return leaf;
    }

    // Recursive case
    let table_clone = table.clone();
    leaf.prop_flat_map(move |left| {
        let table_inner = table_clone.clone();
        condition_for_table(&table_inner, max_depth - 1)
            .prop_map(move |right| Condition::And(Box::new(left.clone()), Box::new(right)))
    })
    .boxed()
}

/// Generate a condition tree for a table with expression support.
pub fn condition_for_table_with_expressions(
    table: &Table,
    functions: &FunctionRegistry,
    max_depth: u32,
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
        .with_max_depth(1)
        .with_aggregates(false);

    // Create leaf strategies from all filterable columns
    let leaves = filterable.iter().map(|c| expression_condition(c, &ctx));

    let leaf = proptest::strategy::Union::new(leaves).boxed();

    if max_depth == 0 {
        return leaf;
    }

    // Recursive case
    let table_clone = table.clone();
    let functions_clone = functions.clone();
    leaf.prop_flat_map(move |left| {
        let table_inner = table_clone.clone();
        let funcs_inner = functions_clone.clone();
        condition_for_table_with_expressions(&table_inner, &funcs_inner, max_depth - 1)
            .prop_map(move |right| Condition::And(Box::new(left.clone()), Box::new(right)))
    })
    .boxed()
}

/// Generate an optional WHERE clause for a table.
pub fn optional_where_clause(table: &Table) -> BoxedStrategy<Option<Condition>> {
    let table_clone = table.clone();
    prop_oneof![
        Just(None),
        condition_for_table(&table_clone, 2).prop_map(Some),
    ]
    .boxed()
}

/// Generate an optional WHERE clause for a table with expression support.
pub fn optional_where_clause_with_expressions(
    table: &Table,
    functions: &FunctionRegistry,
) -> BoxedStrategy<Option<Condition>> {
    let table_clone = table.clone();
    let functions_clone = functions.clone();
    prop_oneof![
        Just(None),
        condition_for_table_with_expressions(&table_clone, &functions_clone, 2).prop_map(Some),
    ]
    .boxed()
}

/// Generate ORDER BY items for a table (simple column references).
pub fn order_by_for_table(table: &Table) -> BoxedStrategy<Vec<OrderByItem>> {
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        return Just(vec![]).boxed();
    }

    let col_names: Vec<String> = filterable.iter().map(|c| c.name.clone()).collect();

    proptest::collection::vec(
        (proptest::sample::select(col_names), order_direction()),
        0..=3,
    )
    .prop_map(|items| {
        // Deduplicate columns in ORDER BY
        let mut seen = std::collections::HashSet::new();
        items
            .into_iter()
            .filter_map(|(column, direction)| {
                if seen.insert(column.clone()) {
                    Some(OrderByItem {
                        expr: Expression::Column(column),
                        direction,
                    })
                } else {
                    None
                }
            })
            .collect()
    })
    .boxed()
}

/// Generate ORDER BY items for a table with expression support.
pub fn order_by_for_table_with_expressions(
    table: &Table,
    functions: &FunctionRegistry,
) -> BoxedStrategy<Vec<OrderByItem>> {
    let filterable = table.filterable_columns().collect::<Vec<_>>();
    if filterable.is_empty() {
        return Just(vec![]).boxed();
    }

    let ctx = ExpressionContext::new(functions.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(1)
        .with_aggregates(false);

    proptest::collection::vec(
        (crate::expression::expression(&ctx), order_direction()),
        0..=3,
    )
    .prop_map(|items| {
        items
            .into_iter()
            .map(|(expr, direction)| OrderByItem { expr, direction })
            .collect()
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
}
