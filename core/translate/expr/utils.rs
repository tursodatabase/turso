use crate::Result;
use turso_parser::ast;

/// Remove SQL literal quotes and collapse escaped single quotes.
pub fn sanitize_string(input: &str) -> String {
    let inner = &input[1..input.len() - 1];
    if inner.contains("''") {
        inner.replace("''", "'")
    } else {
        inner.to_string()
    }
}

/// Recursively unwrap a single expression in parentheses.
pub fn unwrap_parens(mut expression: &ast::Expr) -> Result<&ast::Expr> {
    while let ast::Expr::Parenthesized(expressions) = expression {
        if expressions.len() != 1 {
            break;
        }
        expression = &expressions[0];
    }
    Ok(expression)
}
