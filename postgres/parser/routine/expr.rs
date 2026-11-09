//! The checks that PostgreSQL's transformExpr() does on the expressions of a
//! statement that tursopg does not run, and the types of simple
//! expressions.

use super::{error, strings, RoutineCatalog};
use crate::builtins::{self, FunctionKind};
use crate::routine::types::builtin_type;
use crate::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_GROUPING_ERROR, SQLSTATE_UNDEFINED_COLUMN,
    SQLSTATE_UNDEFINED_TABLE, SQLSTATE_WINDOWING_ERROR,
};
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, a_const, AExprKind, BoolExprType, SqlValueFunctionOp};

/// SQLSTATE 42P02: a `$n` parameter that the statement does not have.
pub(super) const SQLSTATE_UNDEFINED_PARAMETER: &str = "42P02";

/// Where an expression is, for the errors of PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExpressionKind {
    CallArgument,
    /// The DEFAULT of a function parameter.
    Default,
}

impl ExpressionKind {
    /// The words for the place in the singular, as in "cannot use subquery
    /// in CALL argument".
    fn singular(self) -> &'static str {
        match self {
            Self::CallArgument => "CALL argument",
            Self::Default => "DEFAULT expression",
        }
    }

    /// The words for the place in the plural, as in "aggregate functions
    /// are not allowed in CALL arguments".
    fn plural(self) -> &'static str {
        match self {
            Self::CallArgument => "CALL arguments",
            Self::Default => "DEFAULT expressions",
        }
    }
}

/// Checks expressions that have no table to read: a column reference, a
/// `$n` parameter, a subquery, an aggregate, a window function and a
/// set-returning function are errors. Then the other errors come from
/// preparing a SELECT of the expressions.
pub(super) fn check_expressions(
    expressions: &[&pb::Node],
    kind: ExpressionKind,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    if expressions.is_empty() {
        return Ok(());
    }
    for expression in expressions {
        if let Some(node) = &expression.node {
            check_expression(node, kind)?;
        }
    }
    let sql = select_of(expressions)?;
    super::prepare(catalog, &sql).map(|_| ())
}

/// The value of an argument of a function call, without the name of named
/// notation.
pub(super) fn named_argument_value(argument: &pb::Node) -> &pb::Node {
    match &argument.node {
        Some(Node::NamedArgExpr(named)) => named.arg.as_deref().unwrap_or(argument),
        _ => argument,
    }
}

/// The SQL text of `SELECT <expressions>`.
pub(super) fn select_of(expressions: &[&pb::Node]) -> Result<String, ParseError> {
    let target_list = expressions
        .iter()
        .map(|expression| pb::Node {
            node: Some(Node::ResTarget(Box::new(pb::ResTarget {
                val: Some(Box::new((*expression).clone())),
                ..Default::default()
            }))),
        })
        .collect();
    let select = pb::SelectStmt {
        target_list,
        limit_option: pb::LimitOption::Default.into(),
        op: pb::SetOperation::SetopNone.into(),
        ..Default::default()
    };
    Node::SelectStmt(Box::new(select))
        .deparse()
        .map_err(|error| ParseError::ParseError(error.to_string()))
}

/// The first error of an expression, in the order that PostgreSQL finds it:
/// the arguments of a function before the function.
fn check_expression(node: &Node, kind: ExpressionKind) -> Result<(), ParseError> {
    let check = |child: &Option<Box<pb::Node>>| match child.as_deref().and_then(|n| n.node.as_ref())
    {
        Some(node) => check_expression(node, kind),
        None => Ok(()),
    };
    let check_all = |children: &[pb::Node]| {
        children
            .iter()
            .filter_map(|child| child.node.as_ref())
            .try_for_each(|node| check_expression(node, kind))
    };
    match node {
        Node::ColumnRef(column) => Err(missing_column(column)),
        Node::ParamRef(param) => Err(missing_parameter(param.number)),
        Node::SubLink(_) => Err(error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("cannot use subquery in {}", kind.singular()),
        )),
        Node::FuncCall(call) => {
            check_all(&call.args)?;
            let names = strings(&call.funcname);
            let name = names.last().copied().unwrap_or_default();
            if call.over.is_some() {
                return Err(error(
                    SQLSTATE_WINDOWING_ERROR,
                    format!("window functions are not allowed in {}", kind.plural()),
                ));
            }
            let candidates = builtins::functions(name);
            let arity = call.args.len();
            let is_aggregate = call.agg_star
                || call.agg_distinct
                || !call.agg_order.is_empty()
                || call.agg_filter.is_some()
                || candidates.iter().any(|function| {
                    function.kind == FunctionKind::Aggregate && function.parameters.len() == arity
                });
            if is_aggregate {
                return Err(error(
                    SQLSTATE_GROUPING_ERROR,
                    format!("aggregate functions are not allowed in {}", kind.plural()),
                ));
            }
            let returns_set = candidates
                .iter()
                .any(|function| function.returns_set && function.parameters.len() == arity);
            if returns_set {
                return Err(error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "set-returning functions are not allowed in {}",
                        kind.plural()
                    ),
                ));
            }
            Ok(())
        }
        Node::AExpr(expr) => {
            check(&expr.lexpr)?;
            check(&expr.rexpr)
        }
        Node::BoolExpr(expr) => check_all(&expr.args),
        Node::TypeCast(cast) => check(&cast.arg),
        Node::NullTest(test) => check(&test.arg),
        Node::BooleanTest(test) => check(&test.arg),
        Node::CollateClause(collate) => check(&collate.arg),
        Node::NamedArgExpr(named) => check(&named.arg),
        Node::AIndirection(indirection) => check(&indirection.arg),
        Node::CoalesceExpr(expr) => check_all(&expr.args),
        Node::MinMaxExpr(expr) => check_all(&expr.args),
        Node::RowExpr(expr) => check_all(&expr.args),
        Node::AArrayExpr(expr) => check_all(&expr.elements),
        Node::CaseExpr(case) => {
            check(&case.arg)?;
            for when in &case.args {
                if let Some(Node::CaseWhen(when)) = &when.node {
                    check(&when.expr)?;
                    check(&when.result)?;
                }
            }
            check(&case.defresult)
        }
        Node::List(list) => check_all(&list.items),
        _ => Ok(()),
    }
}

/// The error for a column reference in an expression without a table, as
/// PostgreSQL's errorMissingColumn() and errorMissingRTE() give it.
fn missing_column(column: &pb::ColumnRef) -> ParseError {
    let names = strings(&column.fields);
    // A star, as in `t.*`, is a field that is not a String.
    let table = match names.as_slice() {
        _ if column.fields.len() == 1 => None,
        [.., table] if names.len() < column.fields.len() => Some(*table),
        [.., table, _] => Some(*table),
        _ => None,
    };
    match (table, names.as_slice()) {
        (Some(table), _) => error(
            SQLSTATE_UNDEFINED_TABLE,
            format!("missing FROM-clause entry for table \"{table}\""),
        ),
        (None, [name]) => error(
            SQLSTATE_UNDEFINED_COLUMN,
            format!("column \"{name}\" does not exist"),
        ),
        _ => error(SQLSTATE_UNDEFINED_COLUMN, "column does not exist"),
    }
}

pub(super) fn missing_parameter(number: i32) -> ParseError {
    error(
        SQLSTATE_UNDEFINED_PARAMETER,
        format!("there is no parameter ${number}"),
    )
}

/// The type of a simple expression by its name in pg_type, such as `int4`,
/// or `unknown` for a string literal. None when the type is not known
/// without the catalog.
pub(crate) fn expression_type(node: &pb::Node) -> Option<&'static str> {
    Some(match node.node.as_ref()? {
        Node::AConst(constant) => match &constant.val {
            None => "unknown",
            Some(a_const::Val::Ival(_)) => "int4",
            Some(a_const::Val::Fval(value)) => {
                let is_integer = !value.fval.contains(['.', 'e', 'E']);
                if is_integer && value.fval.parse::<i64>().is_ok() {
                    "int8"
                } else {
                    "numeric"
                }
            }
            Some(a_const::Val::Sval(_)) => "unknown",
            Some(a_const::Val::Boolval(_)) => "bool",
            Some(a_const::Val::Bsval(_)) => "bit",
        },
        Node::TypeCast(cast) => builtin_type(cast.type_name.as_ref()?)?.builtin_name()?,
        Node::BoolExpr(expr) if expr.boolop() != BoolExprType::Undefined => "bool",
        Node::NullTest(_) | Node::BooleanTest(_) => "bool",
        Node::AExpr(expr) => match expr.kind() {
            AExprKind::AexprOp => {
                let operator = strings(&expr.name);
                match operator.last().copied() {
                    Some("=" | "<>" | "!=" | "<" | ">" | "<=" | ">=") => "bool",
                    _ => return None,
                }
            }
            AExprKind::AexprLike
            | AExprKind::AexprIlike
            | AExprKind::AexprSimilar
            | AExprKind::AexprBetween
            | AExprKind::AexprNotBetween
            | AExprKind::AexprBetweenSym
            | AExprKind::AexprNotBetweenSym
            | AExprKind::AexprIn
            | AExprKind::AexprDistinct
            | AExprKind::AexprNotDistinct
            | AExprKind::AexprOpAny
            | AExprKind::AexprOpAll => "bool",
            _ => return None,
        },
        Node::SubLink(link) if link.sub_link_type() == pb::SubLinkType::ExistsSublink => "bool",
        Node::FuncCall(call) => function_result_type(call)?,
        Node::SqlvalueFunction(function) => match function.op() {
            SqlValueFunctionOp::SvfopCurrentDate => "date",
            SqlValueFunctionOp::SvfopCurrentTime | SqlValueFunctionOp::SvfopCurrentTimeN => {
                "timetz"
            }
            SqlValueFunctionOp::SvfopCurrentTimestamp
            | SqlValueFunctionOp::SvfopCurrentTimestampN => "timestamptz",
            SqlValueFunctionOp::SvfopLocaltime | SqlValueFunctionOp::SvfopLocaltimeN => "time",
            SqlValueFunctionOp::SvfopLocaltimestamp | SqlValueFunctionOp::SvfopLocaltimestampN => {
                "timestamp"
            }
            SqlValueFunctionOp::SvfopCurrentRole
            | SqlValueFunctionOp::SvfopCurrentUser
            | SqlValueFunctionOp::SvfopUser
            | SqlValueFunctionOp::SvfopSessionUser
            | SqlValueFunctionOp::SvfopCurrentCatalog
            | SqlValueFunctionOp::SvfopCurrentSchema => "name",
            SqlValueFunctionOp::SqlvalueFunctionOpUndefined => return None,
        },
        _ => return None,
    })
}

/// The result type of a call of a built-in function, when each function
/// with the name and number of arguments has the same result type.
fn function_result_type(call: &pb::FuncCall) -> Option<&'static str> {
    let names = strings(&call.funcname);
    let name = match names.as_slice() {
        [name] | ["pg_catalog", name] => *name,
        _ => return None,
    };
    let arity = call.args.len();
    let mut result = None;
    for function in builtins::functions(name) {
        if function.parameters.len() != arity && !function.parameters.iter().any(|p| p.variadic) {
            continue;
        }
        if builtins::is_polymorphic(function.result_type) || function.result_type == "record" {
            return None;
        }
        match result {
            None => result = Some(function.result_type),
            Some(other) if other == function.result_type => {}
            Some(_) => return None,
        }
    }
    result
}
