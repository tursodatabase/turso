//! SQL expressions for use in SELECT columns, WHERE clauses, and function arguments.
//!
//! This module provides a general `Expression` type that can represent:
//! - Literal values
//! - Column references
//! - Function calls
//! - Binary operations
//!
//! Expressions are composable: function arguments can themselves be expressions,
//! allowing nested function calls like `UPPER(SUBSTR(name, 1, 3))`.

use proptest::prelude::*;
use std::fmt;

use crate::function::{FunctionDef, FunctionRegistry};
use crate::schema::{ColumnDef, DataType, Table};
use crate::value::{SqlValue, value_for_type};

/// A SQL expression that can appear in SELECT lists, WHERE clauses, etc.
#[derive(Debug, Clone)]
pub enum Expression {
    /// A literal value (integer, text, etc.).
    Value(SqlValue),
    /// A column reference.
    Column(String),
    /// A function call with arguments.
    FunctionCall { name: String, args: Vec<Expression> },
    /// A binary operation (e.g., `a + b`, `a || b`).
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// A unary operation (e.g., `-a`, `NOT a`).
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },
    /// A parenthesized expression.
    Parenthesized(Box<Expression>),
    /// A CASE expression.
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    /// A CAST expression.
    Cast {
        expr: Box<Expression>,
        target_type: DataType,
    },
    /// A subquery expression (for scalar subqueries).
    Subquery(String),
}

/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Concat,
    // Comparison (when used in expressions, not conditions)
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Add => write!(f, "+"),
            BinaryOperator::Sub => write!(f, "-"),
            BinaryOperator::Mul => write!(f, "*"),
            BinaryOperator::Div => write!(f, "/"),
            BinaryOperator::Mod => write!(f, "%"),
            BinaryOperator::Concat => write!(f, "||"),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::Ne => write!(f, "!="),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::Le => write!(f, "<="),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Ge => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
        }
    }
}

/// Unary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Neg,
    Not,
    BitNot,
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Neg => write!(f, "-"),
            UnaryOperator::Not => write!(f, "NOT "),
            UnaryOperator::BitNot => write!(f, "~"),
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Value(v) => write!(f, "{v}"),
            Expression::Column(name) => write!(f, "\"{name}\""),
            Expression::FunctionCall { name, args } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
            Expression::BinaryOp { left, op, right } => {
                write!(f, "{left} {op} {right}")
            }
            Expression::UnaryOp { op, operand } => {
                write!(f, "{op}{operand}")
            }
            Expression::Parenthesized(expr) => write!(f, "({expr})"),
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op}")?;
                }
                for (when_expr, then_expr) in when_clauses {
                    write!(f, " WHEN {when_expr} THEN {then_expr}")?;
                }
                if let Some(else_expr) = else_clause {
                    write!(f, " ELSE {else_expr}")?;
                }
                write!(f, " END")
            }
            Expression::Cast { expr, target_type } => {
                write!(f, "CAST({expr} AS {target_type})")
            }
            Expression::Subquery(sql) => write!(f, "({sql})"),
        }
    }
}

impl Expression {
    /// Create a literal value expression.
    pub fn value(v: SqlValue) -> Self {
        Expression::Value(v)
    }

    /// Create a column reference expression.
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(name.into())
    }

    /// Create a function call expression.
    pub fn function_call(name: impl Into<String>, args: Vec<Expression>) -> Self {
        Expression::FunctionCall {
            name: name.into(),
            args,
        }
    }

    /// Create a binary operation expression.
    pub fn binary(left: Expression, op: BinaryOperator, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Create a unary operation expression.
    pub fn unary(op: UnaryOperator, operand: Expression) -> Self {
        Expression::UnaryOp {
            op,
            operand: Box::new(operand),
        }
    }

    /// Wrap an expression in parentheses.
    pub fn parenthesized(expr: Expression) -> Self {
        Expression::Parenthesized(Box::new(expr))
    }

    /// Create a CAST expression.
    pub fn cast(expr: Expression, target_type: DataType) -> Self {
        Expression::Cast {
            expr: Box::new(expr),
            target_type,
        }
    }
}

/// Context for generating expressions.
///
/// This context owns its data to allow use in proptest strategies.
#[derive(Debug, Clone)]
pub struct ExpressionContext {
    /// Available columns (if in a table context).
    pub columns: Vec<ColumnDef>,
    /// The function registry to use for generating function calls.
    pub functions: FunctionRegistry,
    /// Maximum nesting depth for recursive expressions.
    pub max_depth: u32,
    /// Whether aggregate functions are allowed in this context.
    pub allow_aggregates: bool,
}

impl ExpressionContext {
    /// Create a new context for expression generation.
    pub fn new(functions: FunctionRegistry) -> Self {
        Self {
            columns: Vec::new(),
            functions,
            max_depth: 3,
            allow_aggregates: false,
        }
    }

    /// Set the available columns.
    pub fn with_columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.columns = columns;
        self
    }

    /// Set the maximum nesting depth.
    pub fn with_max_depth(mut self, depth: u32) -> Self {
        self.max_depth = depth;
        self
    }

    /// Allow aggregate functions.
    pub fn with_aggregates(mut self, allow: bool) -> Self {
        self.allow_aggregates = allow;
        self
    }

    /// Create a child context with reduced depth.
    fn child_context(&self, depth: u32) -> Self {
        Self {
            columns: self.columns.clone(),
            functions: self.functions.clone(),
            max_depth: depth,
            allow_aggregates: self.allow_aggregates,
        }
    }
}

/// Generate a simple value expression.
pub fn value_expression(data_type: &DataType) -> BoxedStrategy<Expression> {
    value_for_type(data_type, true)
        .prop_map(Expression::Value)
        .boxed()
}

/// Generate a column reference expression from a list of columns.
pub fn column_expression(columns: Vec<ColumnDef>) -> BoxedStrategy<Expression> {
    if columns.is_empty() {
        return Just(Expression::Value(SqlValue::Null)).boxed();
    }

    let col_names: Vec<String> = columns.into_iter().map(|c| c.name).collect();
    proptest::sample::select(col_names)
        .prop_map(Expression::Column)
        .boxed()
}

/// Generate a function call expression for a specific function.
pub fn function_call_expression(
    func: FunctionDef,
    ctx: ExpressionContext,
    depth: u32,
) -> BoxedStrategy<Expression> {
    let name = func.name.to_string();
    let min_args = func.min_args;
    let max_args = func.max_args;

    if min_args == 0 && max_args == 0 {
        // No-arg function
        return Just(Expression::function_call(name, vec![])).boxed();
    }

    let num_args = if min_args == max_args {
        Just(min_args).boxed()
    } else {
        (min_args..=max_args).boxed()
    };

    let func_clone = func.clone();
    num_args
        .prop_flat_map(move |n| {
            let arg_strategies: Vec<BoxedStrategy<Expression>> = (0..n)
                .map(|i| {
                    let arg_type = func_clone.expected_type_at(i).cloned();
                    let child_ctx = ctx.child_context(depth.saturating_sub(1));
                    expression_for_type_inner(arg_type, child_ctx, depth.saturating_sub(1))
                })
                .collect();

            arg_strategies
        })
        .prop_map(move |args| Expression::function_call(name.clone(), args))
        .boxed()
}

/// Internal recursive expression generator.
fn expression_for_type_inner(
    target_type: Option<DataType>,
    ctx: ExpressionContext,
    depth: u32,
) -> BoxedStrategy<Expression> {
    let mut strategies: Vec<BoxedStrategy<Expression>> = Vec::new();

    // Always include literal values
    let data_type = target_type.unwrap_or(DataType::Integer);
    strategies.push(value_expression(&data_type));

    // Include column references if available and type-compatible
    let matching_cols: Vec<ColumnDef> = if let Some(ref t) = target_type {
        ctx.columns
            .iter()
            .filter(|c| &c.data_type == t)
            .cloned()
            .collect()
    } else {
        ctx.columns.clone()
    };

    if !matching_cols.is_empty() {
        strategies.push(column_expression(matching_cols));
    }

    // Include function calls if we have depth remaining
    if depth > 0 {
        let compatible_funcs: Vec<FunctionDef> = ctx
            .functions
            .functions_returning(target_type.as_ref())
            .filter(|f| ctx.allow_aggregates || !f.is_aggregate)
            .cloned()
            .collect();

        if !compatible_funcs.is_empty() {
            let ctx_clone = ctx.clone();
            strategies.push(
                proptest::sample::select(compatible_funcs)
                    .prop_flat_map(move |func| {
                        function_call_expression(func, ctx_clone.clone(), depth - 1)
                    })
                    .boxed(),
            );
        }
    }

    if strategies.is_empty() {
        Just(Expression::Value(SqlValue::Null)).boxed()
    } else {
        proptest::strategy::Union::new(strategies).boxed()
    }
}

/// Generate an expression that produces a value of the given type.
pub fn expression_for_type(
    target_type: Option<&DataType>,
    ctx: &ExpressionContext,
    depth: u32,
) -> BoxedStrategy<Expression> {
    expression_for_type_inner(target_type.cloned(), ctx.clone(), depth)
}

/// Generate an arbitrary expression with bounded depth.
pub fn expression(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    expression_for_type(None, ctx, ctx.max_depth)
}

/// Generate an expression suitable for use in a SELECT column list for a table.
pub fn select_expression_for_table(
    table: &Table,
    functions: &FunctionRegistry,
) -> BoxedStrategy<Expression> {
    let ctx = ExpressionContext::new(functions.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(2)
        .with_aggregates(true);

    expression(&ctx)
}

/// Generate an expression suitable for use in a WHERE clause.
pub fn where_expression_for_table(
    table: &Table,
    functions: &FunctionRegistry,
) -> BoxedStrategy<Expression> {
    let ctx = ExpressionContext::new(functions.clone())
        .with_columns(table.columns.clone())
        .with_max_depth(2)
        .with_aggregates(false);

    expression(&ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::builtin_functions;

    #[test]
    fn test_expression_display() {
        let expr = Expression::Value(SqlValue::Integer(42));
        assert_eq!(expr.to_string(), "42");

        let expr = Expression::Column("name".to_string());
        assert_eq!(expr.to_string(), "\"name\"");

        let expr = Expression::function_call("UPPER", vec![Expression::Column("name".to_string())]);
        assert_eq!(expr.to_string(), "UPPER(\"name\")");

        let expr = Expression::function_call(
            "COALESCE",
            vec![
                Expression::Column("name".to_string()),
                Expression::Value(SqlValue::Text("default".to_string())),
            ],
        );
        assert_eq!(expr.to_string(), "COALESCE(\"name\", 'default')");
    }

    #[test]
    fn test_nested_function_display() {
        let expr = Expression::function_call(
            "UPPER",
            vec![Expression::function_call(
                "SUBSTR",
                vec![
                    Expression::Column("name".to_string()),
                    Expression::Value(SqlValue::Integer(1)),
                    Expression::Value(SqlValue::Integer(3)),
                ],
            )],
        );
        assert_eq!(expr.to_string(), "UPPER(SUBSTR(\"name\", 1, 3))");
    }

    #[test]
    fn test_case_expression_display() {
        let expr = Expression::Case {
            operand: None,
            when_clauses: vec![(
                Expression::binary(
                    Expression::Column("age".to_string()),
                    BinaryOperator::Lt,
                    Expression::Value(SqlValue::Integer(18)),
                ),
                Expression::Value(SqlValue::Text("minor".to_string())),
            )],
            else_clause: Some(Box::new(Expression::Value(SqlValue::Text(
                "adult".to_string(),
            )))),
        };
        assert_eq!(
            expr.to_string(),
            "CASE WHEN \"age\" < 18 THEN 'minor' ELSE 'adult' END"
        );
    }

    proptest::proptest! {
        #[test]
        fn generated_expression_is_valid(
            expr in {
                let registry = builtin_functions();
                let ctx = ExpressionContext::new(registry).with_max_depth(2);
                expression(&ctx)
            }
        ) {
            let sql = expr.to_string();
            proptest::prop_assert!(!sql.is_empty());
        }
    }

    #[test]
    fn test_functions_are_generated() {
        use proptest::strategy::Strategy;
        use proptest::test_runner::TestRunner;

        let registry = builtin_functions();
        let ctx = ExpressionContext::new(registry).with_max_depth(3);
        let strategy = expression(&ctx);

        let mut runner = TestRunner::default();
        let mut found_function = false;

        // Generate 100 expressions and check if we get at least one function call
        for _ in 0..100 {
            let expr = strategy.new_tree(&mut runner).unwrap().current();
            if matches!(expr, Expression::FunctionCall { .. }) {
                found_function = true;
                break;
            }
        }

        assert!(
            found_function,
            "Expected to generate at least one function call in 100 attempts"
        );
    }
}
