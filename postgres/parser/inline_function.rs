//! The expression that a call of an SQL function stands for.
//!
//! PostgreSQL replaces a call of an SQL function with the expression of the
//! function's body when the body is a plain `SELECT` of one expression
//! (`inline_function` in `clauses.c`). tursopg runs every call this way, so
//! it can only call the functions that PostgreSQL inlines.

use crate::create_function::{
    display_signature_type_name, return_type_mismatch_message, CalledFunction,
};
use crate::sqlstate::SQLSTATE_INVALID_FUNCTION_DEFINITION;
use crate::translator::PostgreSQLTranslator;
use crate::ParseError;
use pg_query::protobuf::{node::Node, ParamRef, SelectStmt, SetOperation, TypeCast, TypeName};
use turso_parser::ast;

/// The expressions that a call of an SQL function is made of.
pub struct InlinedFunction {
    /// The expression of the body, cast to the result type. Parameter `$n`
    /// is `ast::Expr::Variable`.
    pub result: ast::Expr,
    /// For each parameter, `$n` cast to the parameter's type. An argument
    /// takes the place of the `ast::Expr::Variable` in it.
    pub parameters: Vec<ast::Expr>,
    /// The functions that the body calls.
    pub called_functions: Vec<CalledFunction>,
}

/// The expressions of a function with the parameters `parameter_types`, the
/// result `return_type` and the body `body`, or None when PostgreSQL does
/// not inline the function. The types are named as a signature names them,
/// such as `int4`.
pub fn inlined_function(
    parameter_types: &[String],
    return_type: &str,
    body: &str,
) -> Result<Option<InlinedFunction>, ParseError> {
    let parsed = crate::parse(body)?;
    // CREATE FUNCTION refuses such a body when it checks the body, which it
    // does not do when check_function_bodies is off.
    if returns_several_columns(&parsed) {
        return Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_INVALID_FUNCTION_DEFINITION,
            message: return_type_mismatch_message(&display_signature_type_name(return_type)),
        });
    }
    let Some(expression) = inlinable_expression(&parsed) else {
        return Ok(None);
    };
    let translator = PostgreSQLTranslator::new();
    let result = translator.translate_expr(&type_cast(expression.clone(), return_type))?;
    let called_functions = translator.called_functions();
    let parameters = parameter_types
        .iter()
        .zip(1..)
        .map(|(parameter_type, number)| {
            let parameter = Node::ParamRef(ParamRef {
                number,
                location: -1,
            });
            translator.translate_expr(&type_cast(
                pg_query::protobuf::Node {
                    node: Some(parameter),
                },
                parameter_type,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(InlinedFunction {
        result,
        parameters,
        called_functions,
    }))
}

/// Whether the query of the body returns more than one column. A `*` counts
/// as one column, because the tables are not known here.
fn returns_several_columns(parsed: &pg_query::ParseResult) -> bool {
    let [statement] = parsed.protobuf.stmts.as_slice() else {
        return false;
    };
    let Some(Node::SelectStmt(select)) = statement
        .stmt
        .as_ref()
        .and_then(|statement| statement.node.as_ref())
    else {
        return false;
    };
    // The first query of a set operation has the columns.
    let mut select = select.as_ref();
    while let Some(first) = &select.larg {
        select = first;
    }
    select.target_list.len() > 1
}

/// The expression of a body that is a plain `SELECT` of one expression:
/// without a table, a condition, a grouping, an order, a limit or a set
/// operation. PostgreSQL also does not inline an expression with a subquery
/// or with a call of an aggregate or window function. The caller checks
/// that on the translated expression, where it knows the functions.
fn inlinable_expression(parsed: &pg_query::ParseResult) -> Option<&pg_query::protobuf::Node> {
    let [statement] = parsed.protobuf.stmts.as_slice() else {
        return None;
    };
    let Some(Node::SelectStmt(select)) = &statement.stmt.as_ref()?.node else {
        return None;
    };
    if !is_plain_select(select) {
        return None;
    }
    let [target] = select.target_list.as_slice() else {
        return None;
    };
    let Some(Node::ResTarget(target)) = &target.node else {
        return None;
    };
    target.val.as_deref()
}

fn is_plain_select(select: &SelectStmt) -> bool {
    select.op() == SetOperation::SetopNone
        && select.with_clause.is_none()
        && select.into_clause.is_none()
        && select.from_clause.is_empty()
        && select.where_clause.is_none()
        && select.group_clause.is_empty()
        && select.having_clause.is_none()
        && select.window_clause.is_empty()
        && select.distinct_clause.is_empty()
        && select.sort_clause.is_empty()
        && select.limit_offset.is_none()
        && select.limit_count.is_none()
        && select.locking_clause.is_empty()
        && select.values_lists.is_empty()
}

/// `expression` cast to the type that a signature names `type_name`.
fn type_cast(expression: pg_query::protobuf::Node, type_name: &str) -> pg_query::protobuf::Node {
    let name = |name: &str| pg_query::protobuf::Node {
        node: Some(Node::String(pg_query::protobuf::String {
            sval: name.to_string(),
        })),
    };
    pg_query::protobuf::Node {
        node: Some(Node::TypeCast(Box::new(TypeCast {
            arg: Some(Box::new(expression)),
            type_name: Some(TypeName {
                names: vec![name("pg_catalog"), name(type_name)],
                typemod: -1,
                location: -1,
                ..Default::default()
            }),
            location: -1,
        }))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inlined(body: &str) -> Option<InlinedFunction> {
        inlined_function(&["int4".to_string()], "int4", body).unwrap()
    }

    #[test]
    fn a_plain_select_of_one_expression_is_inlined() {
        let function = inlined("SELECT $1 + 1").unwrap();
        assert_eq!(function.result.to_string(), "CAST (?1 + 1 AS INTEGER)");
        assert_eq!(function.parameters.len(), 1);
        assert_eq!(function.parameters[0].to_string(), "CAST (?1 AS INTEGER)");
    }

    #[test]
    fn a_body_that_postgres_does_not_inline_has_no_expression() {
        for body in [
            "SELECT a FROM t",
            "SELECT 1 WHERE true",
            "SELECT 1 LIMIT 1",
            "SELECT 1 OFFSET 0",
            "SELECT 1 ORDER BY 1",
            "SELECT DISTINCT 1",
            "SELECT 1 GROUP BY 1",
            "SELECT 1 HAVING true",
            "SELECT 1 UNION SELECT 2",
            "VALUES (1)",
            "WITH one AS (SELECT 1) SELECT 1",
            "SELECT 1 FOR UPDATE",
        ] {
            assert!(inlined(body).is_none(), "{body}");
        }
    }

    #[test]
    fn a_body_that_returns_several_columns_is_refused() {
        for body in [
            "SELECT 1, 2",
            "SELECT a, b FROM t",
            "SELECT 1, 2 UNION SELECT 3, 4",
        ] {
            let Err(ParseError::Sqlstate { sqlstate, message }) =
                inlined_function(&["int4".to_string()], "int4", body)
            else {
                panic!("{body} is not refused");
            };
            assert_eq!(sqlstate, "42P13", "{body}");
            assert_eq!(
                message, "return type mismatch in function declared to return integer",
                "{body}"
            );
        }
    }
}
