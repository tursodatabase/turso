use crate::schema::Type;
use crate::vdbe::{builder::ProgramBuilder, insn::Insn};
use crate::Result;
use turso_parser::ast;

pub fn maybe_apply_affinity(col_type: Type, target_register: usize, program: &mut ProgramBuilder) {
    if col_type == Type::Real {
        program.emit_insn(Insn::RealAffinity {
            register: target_register,
        })
    }
}

/// Sanitizes a string literal by removing single quote at front and back
/// and escaping double single quotes
pub fn sanitize_string(input: &str) -> String {
    let inner = &input[1..input.len() - 1];

    // Fast path, avoid replacing.
    if !inner.contains("''") {
        return inner.to_string();
    }

    inner.replace("''", "'")
}

/// Recursively unwrap parentheses from an expression
/// e.g. (((t.x > 5))) -> t.x > 5
pub fn unwrap_parens(expr: &ast::Expr) -> Result<&ast::Expr> {
    match expr {
        ast::Expr::Column { .. } => Ok(expr),
        ast::Expr::Parenthesized(exprs) => match exprs.len() {
            1 => unwrap_parens(exprs.first().unwrap()),
            _ => Ok(expr), // If the expression is e.g. (x, y), as used in e.g. (x, y) IN (SELECT ...), return as is.
        },
        _ => Ok(expr),
    }
}
