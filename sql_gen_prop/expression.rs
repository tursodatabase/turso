//! SQL expressions for use in SELECT columns, WHERE clauses, and function arguments.
//!
//! This module provides a general `Expression` type that can represent:
//! - Literal values
//! - Column references
//! - Function calls
//! - Binary operations
//! - Unary operations
//! - CASE expressions
//! - CAST expressions
//!
//! Expressions are composable: function arguments can themselves be expressions,
//! allowing nested function calls like `UPPER(SUBSTR(name, 1, 3))`.

use proptest::prelude::*;
use std::fmt;
use strum::IntoEnumIterator;

use crate::function::{FunctionCategory, FunctionDef, FunctionProfile, FunctionRegistry};
use crate::generator::SqlGeneratorKind;
use crate::schema::{ColumnDef, DataType};
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

/// The kind of expression for generation control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum ExpressionKind {
    /// A literal value.
    Value,
    /// A column reference.
    Column,
    /// A function call.
    FunctionCall,
    /// A binary operation.
    BinaryOp,
    /// A unary operation.
    UnaryOp,
    /// A CASE expression.
    Case,
    /// A CAST expression.
    Cast,
}

impl fmt::Display for ExpressionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionKind::Value => write!(f, "Value"),
            ExpressionKind::Column => write!(f, "Column"),
            ExpressionKind::FunctionCall => write!(f, "FunctionCall"),
            ExpressionKind::BinaryOp => write!(f, "BinaryOp"),
            ExpressionKind::UnaryOp => write!(f, "UnaryOp"),
            ExpressionKind::Case => write!(f, "Case"),
            ExpressionKind::Cast => write!(f, "Cast"),
        }
    }
}

/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter)]
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

impl BinaryOperator {
    /// Returns operators suitable for numeric types.
    pub fn numeric_operators() -> Vec<BinaryOperator> {
        vec![
            BinaryOperator::Add,
            BinaryOperator::Sub,
            BinaryOperator::Mul,
            BinaryOperator::Div,
            BinaryOperator::Mod,
        ]
    }

    /// Returns operators suitable for text types.
    pub fn text_operators() -> Vec<BinaryOperator> {
        vec![BinaryOperator::Concat]
    }

    /// Returns comparison operators.
    pub fn comparison_operators() -> Vec<BinaryOperator> {
        vec![
            BinaryOperator::Eq,
            BinaryOperator::Ne,
            BinaryOperator::Lt,
            BinaryOperator::Le,
            BinaryOperator::Gt,
            BinaryOperator::Ge,
        ]
    }

    /// Returns logical operators.
    pub fn logical_operators() -> Vec<BinaryOperator> {
        vec![BinaryOperator::And, BinaryOperator::Or]
    }

    /// Returns operators suitable for the given data type.
    pub fn operators_for_type(data_type: &DataType) -> Vec<BinaryOperator> {
        match data_type {
            DataType::Integer | DataType::Real => Self::numeric_operators(),
            DataType::Text => Self::text_operators(),
            DataType::Blob | DataType::Null => vec![],
        }
    }
}

/// Unary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter)]
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

impl UnaryOperator {
    /// Returns operators suitable for the given data type.
    pub fn operators_for_type(data_type: &DataType) -> Vec<UnaryOperator> {
        match data_type {
            DataType::Integer => vec![UnaryOperator::Neg, UnaryOperator::BitNot],
            DataType::Real => vec![UnaryOperator::Neg],
            DataType::Text | DataType::Blob | DataType::Null => vec![],
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

/// Profile for controlling expression generation weights.
#[derive(Debug, Clone)]
pub struct ExpressionProfile {
    /// Weight for literal value expressions.
    pub value_weight: u32,
    /// Weight for column reference expressions.
    pub column_weight: u32,
    /// Weight for function call expressions.
    pub function_call_weight: u32,
    /// Weight for binary operation expressions.
    pub binary_op_weight: u32,
    /// Weight for unary operation expressions.
    pub unary_op_weight: u32,
    /// Weight for CASE expressions.
    pub case_weight: u32,
    /// Weight for CAST expressions.
    pub cast_weight: u32,
    /// Profile for controlling function category weights.
    pub function_profile: FunctionProfile,
}

impl Default for ExpressionProfile {
    fn default() -> Self {
        Self {
            value_weight: 30,
            column_weight: 30,
            function_call_weight: 20,
            binary_op_weight: 10,
            unary_op_weight: 5,
            case_weight: 3,
            cast_weight: 2,
            function_profile: FunctionProfile::default(),
        }
    }
}

impl ExpressionProfile {
    /// Create a profile with all expression kinds equally weighted.
    pub fn uniform() -> Self {
        Self {
            value_weight: 15,
            column_weight: 15,
            function_call_weight: 15,
            binary_op_weight: 15,
            unary_op_weight: 14,
            case_weight: 13,
            cast_weight: 13,
            function_profile: FunctionProfile::default(),
        }
    }

    /// Create a profile with no function calls.
    pub fn no_functions() -> Self {
        Self {
            value_weight: 40,
            column_weight: 40,
            function_call_weight: 0,
            binary_op_weight: 10,
            unary_op_weight: 5,
            case_weight: 3,
            cast_weight: 2,
            function_profile: FunctionProfile::default(),
        }
    }

    /// Create a profile that heavily favors function calls.
    pub fn function_heavy() -> Self {
        Self {
            value_weight: 10,
            column_weight: 10,
            function_call_weight: 60,
            binary_op_weight: 8,
            unary_op_weight: 5,
            case_weight: 4,
            cast_weight: 3,
            function_profile: FunctionProfile::default(),
        }
    }

    /// Create a profile for simple expressions (values and columns only).
    pub fn simple() -> Self {
        Self {
            value_weight: 50,
            column_weight: 50,
            function_call_weight: 0,
            binary_op_weight: 0,
            unary_op_weight: 0,
            case_weight: 0,
            cast_weight: 0,
            function_profile: FunctionProfile::default(),
        }
    }

    /// Builder method to set the weight for an expression kind.
    pub fn with_weight(mut self, kind: ExpressionKind, weight: u32) -> Self {
        match kind {
            ExpressionKind::Value => self.value_weight = weight,
            ExpressionKind::Column => self.column_weight = weight,
            ExpressionKind::FunctionCall => self.function_call_weight = weight,
            ExpressionKind::BinaryOp => self.binary_op_weight = weight,
            ExpressionKind::UnaryOp => self.unary_op_weight = weight,
            ExpressionKind::Case => self.case_weight = weight,
            ExpressionKind::Cast => self.cast_weight = weight,
        }
        self
    }

    /// Builder method to set the function profile.
    pub fn with_function_profile(mut self, profile: FunctionProfile) -> Self {
        self.function_profile = profile;
        self
    }

    /// Get the weight for an expression kind.
    pub fn weight_for(&self, kind: ExpressionKind) -> u32 {
        match kind {
            ExpressionKind::Value => self.value_weight,
            ExpressionKind::Column => self.column_weight,
            ExpressionKind::FunctionCall => self.function_call_weight,
            ExpressionKind::BinaryOp => self.binary_op_weight,
            ExpressionKind::UnaryOp => self.unary_op_weight,
            ExpressionKind::Case => self.case_weight,
            ExpressionKind::Cast => self.cast_weight,
        }
    }

    /// Returns an iterator over (kind, weight) pairs for all enabled expression kinds.
    pub fn enabled_kinds(&self) -> impl Iterator<Item = (ExpressionKind, u32)> + '_ {
        ExpressionKind::iter()
            .map(|kind| (kind, self.weight_for(kind)))
            .filter(|(_, weight)| *weight > 0)
    }
}

// =============================================================================
// EXTENDED EXPRESSION PROFILE
// =============================================================================

/// Extended expression profile with additional configuration.
#[derive(Debug, Clone)]
pub struct ExtendedExpressionProfile {
    /// Base expression profile.
    pub base: ExpressionProfile,
    /// Range for number of CASE WHEN clauses.
    pub case_when_clause_range: std::ops::RangeInclusive<usize>,
    /// Default max depth for expressions.
    pub default_max_depth: u32,
}

impl Default for ExtendedExpressionProfile {
    fn default() -> Self {
        Self {
            base: ExpressionProfile::default(),
            case_when_clause_range: 1..=3,
            default_max_depth: 3,
        }
    }
}

impl ExtendedExpressionProfile {
    /// Create a simple expression profile.
    pub fn simple() -> Self {
        Self {
            base: ExpressionProfile::simple(),
            case_when_clause_range: 1..=1,
            default_max_depth: 1,
        }
    }

    /// Create a complex expression profile.
    pub fn complex() -> Self {
        Self {
            base: ExpressionProfile::function_heavy(),
            case_when_clause_range: 1..=5,
            default_max_depth: 5,
        }
    }

    /// Builder method to set base profile.
    pub fn with_base(mut self, base: ExpressionProfile) -> Self {
        self.base = base;
        self
    }

    /// Builder method to set CASE WHEN clause range.
    pub fn with_case_when_clause_range(mut self, range: std::ops::RangeInclusive<usize>) -> Self {
        self.case_when_clause_range = range;
        self
    }

    /// Builder method to set default max depth.
    pub fn with_default_max_depth(mut self, depth: u32) -> Self {
        self.default_max_depth = depth;
        self
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
    /// The target data type for expressions (if type-constrained).
    pub target_type: Option<DataType>,
    /// The expression generation profile.
    pub profile: ExpressionProfile,
    /// Range for number of CASE WHEN clauses.
    pub case_when_clause_range: std::ops::RangeInclusive<usize>,
}

impl ExpressionContext {
    /// Create a new context for expression generation.
    pub fn new(functions: FunctionRegistry) -> Self {
        Self {
            columns: Vec::new(),
            functions,
            max_depth: 3,
            allow_aggregates: false,
            target_type: None,
            profile: ExpressionProfile::default(),
            case_when_clause_range: 1..=3,
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

    /// Set the target data type.
    pub fn with_target_type(mut self, data_type: Option<DataType>) -> Self {
        self.target_type = data_type;
        self
    }

    /// Set the expression profile.
    pub fn with_profile(mut self, profile: ExpressionProfile) -> Self {
        self.profile = profile;
        self
    }

    /// Set the CASE WHEN clause range.
    pub fn with_case_when_clause_range(
        mut self,
        range: std::ops::RangeInclusive<usize>,
    ) -> Self {
        self.case_when_clause_range = range;
        self
    }

    /// Create a child context with reduced depth.
    fn child_context(&self, depth: u32) -> Self {
        Self {
            columns: self.columns.clone(),
            functions: self.functions.clone(),
            max_depth: depth,
            allow_aggregates: self.allow_aggregates,
            target_type: self.target_type,
            profile: self.profile.clone(),
            case_when_clause_range: self.case_when_clause_range.clone(),
        }
    }
}

impl SqlGeneratorKind for ExpressionKind {
    type Context<'a> = ExpressionContext;
    type Output = Expression;
    type Profile = ExpressionProfile;

    fn available(&self, ctx: &Self::Context<'_>) -> bool {
        match self {
            ExpressionKind::Value => true,
            ExpressionKind::Column => {
                if ctx.columns.is_empty() {
                    return false;
                }
                match &ctx.target_type {
                    Some(t) => ctx.columns.iter().any(|c| &c.data_type == t),
                    None => true,
                }
            }
            ExpressionKind::FunctionCall => {
                if ctx.max_depth == 0 {
                    return false;
                }
                ctx.functions
                    .functions_returning(ctx.target_type.as_ref())
                    .any(|f| ctx.allow_aggregates || !f.is_aggregate)
            }
            ExpressionKind::BinaryOp => {
                if ctx.max_depth == 0 {
                    return false;
                }
                // Binary ops need a type that supports them
                !matches!(
                    &ctx.target_type,
                    Some(DataType::Blob) | Some(DataType::Null)
                )
            }
            ExpressionKind::UnaryOp => {
                if ctx.max_depth == 0 {
                    return false;
                }
                // Unary ops need numeric types
                match &ctx.target_type {
                    Some(DataType::Integer) | Some(DataType::Real) => true,
                    Some(_) => false,
                    None => true,
                }
            }
            ExpressionKind::Case => ctx.max_depth > 0,
            ExpressionKind::Cast => ctx.max_depth > 0,
        }
    }

    fn supported(&self) -> bool {
        true
    }

    fn strategy<'a>(
        &self,
        ctx: &Self::Context<'a>,
        _profile: &Self::Profile,
    ) -> BoxedStrategy<Self::Output> {
        match self {
            ExpressionKind::Value => value_expression_strategy(ctx),
            ExpressionKind::Column => column_expression_strategy(ctx),
            ExpressionKind::FunctionCall => function_call_expression_strategy(ctx),
            ExpressionKind::BinaryOp => binary_op_expression_strategy(ctx),
            ExpressionKind::UnaryOp => unary_op_expression_strategy(ctx),
            ExpressionKind::Case => case_expression_strategy(ctx),
            ExpressionKind::Cast => cast_expression_strategy(ctx),
        }
    }
}

/// Generate a literal value expression.
fn value_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let data_type = ctx.target_type.unwrap_or(DataType::Integer);
    let value_profile = crate::value::ValueProfile::default();
    value_for_type(&data_type, true, &value_profile)
        .prop_map(Expression::Value)
        .boxed()
}

/// Generate a column reference expression.
fn column_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let col_names: Vec<String> = ctx
        .columns
        .iter()
        .filter(|c| ctx.target_type.as_ref().is_none_or(|t| &c.data_type == t))
        .map(|c| c.name.clone())
        .collect();

    if col_names.is_empty() {
        return Just(Expression::Value(SqlValue::Null)).boxed();
    }

    proptest::sample::select(col_names)
        .prop_map(Expression::Column)
        .boxed()
}

/// Generate a function call expression.
fn function_call_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let profile = &ctx.profile.function_profile;
    let depth = ctx.max_depth.saturating_sub(1);

    let weighted_strategies: Vec<(u32, BoxedStrategy<Expression>)> = profile
        .enabled_operations()
        .filter(|(cat, _)| {
            ctx.allow_aggregates
                || !matches!(cat, FunctionCategory::Aggregate | FunctionCategory::Window)
        })
        .filter_map(|(category, weight)| {
            let funcs: Vec<FunctionDef> = ctx
                .functions
                .in_category(category)
                .filter(|f| {
                    ctx.target_type
                        .as_ref()
                        .is_none_or(|t| f.return_type.as_ref().is_none_or(|rt| rt == t))
                })
                .filter(|f| ctx.allow_aggregates || !f.is_aggregate)
                .cloned()
                .collect();

            if funcs.is_empty() {
                return None;
            }

            let ctx_clone = ctx.clone();
            let strategy = proptest::sample::select(funcs)
                .prop_flat_map(move |func| function_call_for_def(func, ctx_clone.clone(), depth))
                .boxed();

            Some((weight, strategy))
        })
        .collect();

    if weighted_strategies.is_empty() {
        Just(Expression::Value(SqlValue::Null)).boxed()
    } else {
        proptest::strategy::Union::new_weighted(weighted_strategies).boxed()
    }
}

/// Generate a function call for a specific function definition.
fn function_call_for_def(
    func: FunctionDef,
    ctx: ExpressionContext,
    depth: u32,
) -> BoxedStrategy<Expression> {
    let name = func.name.to_string();

    if func.min_args == 0 && func.max_args == 0 {
        return Just(Expression::function_call(name, vec![])).boxed();
    }

    let int_arg_max = func.int_arg_max;
    (func.min_args..=func.max_args)
        .prop_flat_map(move |n| {
            (0..n)
                .map(|i| {
                    let arg_type = func.expected_type_at(i).cloned();
                    // Use bounded integers for functions with int_arg_max
                    if let (Some(max), Some(DataType::Integer)) = (int_arg_max, arg_type.as_ref()) {
                        (0..=max)
                            .prop_map(|v| Expression::Value(SqlValue::Integer(v)))
                            .boxed()
                    } else {
                        expression(
                            &ctx.child_context(depth.saturating_sub(1))
                                .with_target_type(arg_type),
                        )
                    }
                })
                .collect::<Vec<_>>()
        })
        .prop_map(move |args| Expression::function_call(name.clone(), args))
        .boxed()
}

/// Generate a binary operation expression.
fn binary_op_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let data_type = ctx.target_type.unwrap_or(DataType::Integer);
    let operators = BinaryOperator::operators_for_type(&data_type);

    if operators.is_empty() {
        return Just(Expression::Value(SqlValue::Null)).boxed();
    }

    let child_ctx = ctx
        .child_context(ctx.max_depth.saturating_sub(1))
        .with_target_type(Some(data_type));

    (
        expression(&child_ctx),
        proptest::sample::select(operators),
        expression(&child_ctx),
    )
        .prop_map(|(left, op, right)| Expression::binary(left, op, right))
        .boxed()
}

/// Generate a unary operation expression.
fn unary_op_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let data_type = ctx.target_type.unwrap_or(DataType::Integer);
    let operators = UnaryOperator::operators_for_type(&data_type);

    if operators.is_empty() {
        return Just(Expression::Value(SqlValue::Null)).boxed();
    }

    let child_ctx = ctx
        .child_context(ctx.max_depth.saturating_sub(1))
        .with_target_type(Some(data_type));

    (proptest::sample::select(operators), expression(&child_ctx))
        .prop_map(|(op, operand)| Expression::unary(op, operand))
        .boxed()
}

/// Generate a CASE expression.
fn case_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let child_ctx = ctx.child_context(ctx.max_depth.saturating_sub(1));

    // Create contexts for condition and then expressions
    let cond_ctx = child_ctx.clone().with_target_type(Some(DataType::Integer));
    let then_ctx = child_ctx.clone();

    // Use the context's case_when_clause_range
    let when_clause_range = ctx.case_when_clause_range.clone();
    let when_clause_strategy = (expression(&cond_ctx), expression(&then_ctx));

    (
        proptest::collection::vec(when_clause_strategy, when_clause_range),
        proptest::option::of(expression(&child_ctx)),
    )
        .prop_map(|(when_clauses, else_clause)| Expression::Case {
            operand: None,
            when_clauses,
            else_clause: else_clause.map(Box::new),
        })
        .boxed()
}

/// Generate a CAST expression.
fn cast_expression_strategy(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let target_type = ctx.target_type.unwrap_or(DataType::Integer);
    let child_ctx = ctx
        .child_context(ctx.max_depth.saturating_sub(1))
        .with_target_type(None); // Source can be any type

    expression(&child_ctx)
        .prop_map(move |expr| Expression::cast(expr, target_type))
        .boxed()
}

/// Generate an expression using the context's profile.
pub fn expression(ctx: &ExpressionContext) -> BoxedStrategy<Expression> {
    let weighted_strategies: Vec<(u32, BoxedStrategy<Expression>)> = ctx
        .profile
        .enabled_kinds()
        .filter(|(kind, _)| kind.available(ctx))
        .map(|(kind, weight)| (weight, kind.strategy(ctx, &ctx.profile)))
        .collect();

    if weighted_strategies.is_empty() {
        Just(Expression::Value(SqlValue::Null)).boxed()
    } else {
        proptest::strategy::Union::new_weighted(weighted_strategies).boxed()
    }
}

/// Generate an expression for a specific type.
pub fn expression_for_type(
    target_type: Option<&DataType>,
    ctx: &ExpressionContext,
) -> BoxedStrategy<Expression> {
    let child_ctx = ctx
        .child_context(ctx.max_depth)
        .with_target_type(target_type.cloned());
    expression(&child_ctx)
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
    fn test_binary_op_display() {
        let expr = Expression::binary(
            Expression::Value(SqlValue::Integer(1)),
            BinaryOperator::Add,
            Expression::Value(SqlValue::Integer(2)),
        );
        assert_eq!(expr.to_string(), "1 + 2");

        let expr = Expression::binary(
            Expression::Column("a".to_string()),
            BinaryOperator::Concat,
            Expression::Column("b".to_string()),
        );
        assert_eq!(expr.to_string(), "\"a\" || \"b\"");
    }

    #[test]
    fn test_unary_op_display() {
        let expr = Expression::unary(UnaryOperator::Neg, Expression::Value(SqlValue::Integer(5)));
        assert_eq!(expr.to_string(), "-5");

        let expr = Expression::unary(UnaryOperator::Not, Expression::Value(SqlValue::Integer(1)));
        assert_eq!(expr.to_string(), "NOT 1");
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

    #[test]
    fn test_cast_expression_display() {
        let expr = Expression::cast(Expression::Column("value".to_string()), DataType::Integer);
        assert_eq!(expr.to_string(), "CAST(\"value\" AS INTEGER)");
    }

    #[test]
    fn test_expression_profile_default() {
        let profile = ExpressionProfile::default();
        assert!(profile.value_weight > 0);
        assert!(profile.column_weight > 0);
        assert!(profile.function_call_weight > 0);
        assert!(profile.binary_op_weight > 0);
        assert!(profile.unary_op_weight > 0);
        assert!(profile.case_weight > 0);
        assert!(profile.cast_weight > 0);
    }

    #[test]
    fn test_expression_profile_simple() {
        let profile = ExpressionProfile::simple();
        assert!(profile.value_weight > 0);
        assert!(profile.column_weight > 0);
        assert_eq!(profile.function_call_weight, 0);
        assert_eq!(profile.binary_op_weight, 0);
        assert_eq!(profile.unary_op_weight, 0);
        assert_eq!(profile.case_weight, 0);
        assert_eq!(profile.cast_weight, 0);
    }

    #[test]
    fn test_expression_kind_available() {
        let registry = builtin_functions();
        let ctx = ExpressionContext::new(registry)
            .with_max_depth(2)
            .with_columns(vec![ColumnDef::new("id", DataType::Integer)]);

        assert!(ExpressionKind::Value.available(&ctx));
        assert!(ExpressionKind::Column.available(&ctx));
        assert!(ExpressionKind::FunctionCall.available(&ctx));
        assert!(ExpressionKind::BinaryOp.available(&ctx));
        assert!(ExpressionKind::UnaryOp.available(&ctx));
        assert!(ExpressionKind::Case.available(&ctx));
        assert!(ExpressionKind::Cast.available(&ctx));

        // With no columns, Column is not available
        let ctx_no_cols = ExpressionContext::new(builtin_functions()).with_max_depth(2);
        assert!(!ExpressionKind::Column.available(&ctx_no_cols));

        // With depth 0, recursive expressions are not available
        let ctx_no_depth = ExpressionContext::new(builtin_functions())
            .with_max_depth(0)
            .with_columns(vec![ColumnDef::new("id", DataType::Integer)]);
        assert!(!ExpressionKind::FunctionCall.available(&ctx_no_depth));
        assert!(!ExpressionKind::BinaryOp.available(&ctx_no_depth));
        assert!(!ExpressionKind::UnaryOp.available(&ctx_no_depth));
        assert!(!ExpressionKind::Case.available(&ctx_no_depth));
        assert!(!ExpressionKind::Cast.available(&ctx_no_depth));
    }

    #[test]
    fn test_binary_operators_for_type() {
        let int_ops = BinaryOperator::operators_for_type(&DataType::Integer);
        assert!(int_ops.contains(&BinaryOperator::Add));
        assert!(int_ops.contains(&BinaryOperator::Sub));

        let text_ops = BinaryOperator::operators_for_type(&DataType::Text);
        assert!(text_ops.contains(&BinaryOperator::Concat));
        assert!(!text_ops.contains(&BinaryOperator::Add));

        let blob_ops = BinaryOperator::operators_for_type(&DataType::Blob);
        assert!(blob_ops.is_empty());
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

    #[test]
    fn test_binary_ops_are_generated() {
        use proptest::strategy::Strategy;
        use proptest::test_runner::TestRunner;

        let registry = builtin_functions();
        let profile = ExpressionProfile::default().with_weight(ExpressionKind::BinaryOp, 50);
        let ctx = ExpressionContext::new(registry)
            .with_max_depth(3)
            .with_profile(profile);
        let strategy = expression(&ctx);

        let mut runner = TestRunner::default();
        let mut found_binary = false;

        for _ in 0..100 {
            let expr = strategy.new_tree(&mut runner).unwrap().current();
            if matches!(expr, Expression::BinaryOp { .. }) {
                found_binary = true;
                break;
            }
        }

        assert!(
            found_binary,
            "Expected to generate at least one binary operation in 100 attempts"
        );
    }

    #[test]
    fn test_simple_profile_only_values_and_columns() {
        use proptest::strategy::Strategy;
        use proptest::test_runner::TestRunner;

        let registry = builtin_functions();
        let profile = ExpressionProfile::simple();
        let ctx = ExpressionContext::new(registry)
            .with_max_depth(3)
            .with_columns(vec![ColumnDef::new("id", DataType::Integer)])
            .with_profile(profile);
        let strategy = expression(&ctx);

        let mut runner = TestRunner::default();

        for _ in 0..50 {
            let expr = strategy.new_tree(&mut runner).unwrap().current();
            assert!(
                matches!(expr, Expression::Value(_) | Expression::Column(_)),
                "Expected only Value or Column with simple profile, got: {expr}"
            );
        }
    }
}
