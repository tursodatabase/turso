//! SQL frontend slice: AST expressions -> [`Compiler`] values.
//!
//! `compile_value_expr` builds a *description* of the work to evaluate an
//! expression in value position; nothing touches a builder until the
//! description is run. Returns `Ok(None)` when the expression is not yet
//! representable, in which case the caller stays on the eager path — the
//! gradual-migration escape hatch.
//!
//! Coverage today: literals, parenthesization, unary `+`/`-`/`~`/NOT
//! (including the eager path's compile-time folds for signed and
//! bit-inverted numeric literals), and arithmetic/bitwise/concat binary
//! operators — over literal-only operands. Column reads, comparisons,
//! functions, and short-circuit operators are future slices (see
//! docs/internals/composable-compiler-ir.md).

use turso_parser::ast::{self, UnaryOperator};

use crate::alloc::TursoIteratorExt;
use crate::translate::expr::sanitize_string;
use crate::util::parse_numeric_literal;
use crate::{Numeric, Result, Value, ValueBlob};

use super::combine::{self, Compiler};
use super::ir::{BinOp, UnaryOp, ValueId};

/// Try to describe `expr` (in value position) as a composable compiler.
///
/// The compile-time folds intentionally mirror the eager path in
/// `core/translate/expr/translator.rs` so both paths produce identical
/// runtime values (e.g. `-9223372036854775808` folds to `i64::MIN`
/// instead of overflowing through a real).
pub(crate) fn compile_value_expr<'a>(expr: &'a ast::Expr) -> Result<Option<Compiler<'a, ValueId>>> {
    Ok(match expr {
        ast::Expr::Literal(literal) => compile_literal(literal)?,
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            compile_value_expr(exprs[0].as_ref())?
        }
        ast::Expr::Unary(op, operand) => match (op, operand.as_ref()) {
            (UnaryOperator::Positive, operand) => compile_value_expr(operand)?,
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                // Parse the sign together with the literal so i64::MIN
                // folds to an integer instead of overflowing to a real.
                let negated = format!("-{value}");
                Some(compile_numeric(&negated)?)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                let folded = match parse_numeric_literal(value)? {
                    Value::Numeric(Numeric::Integer(int_value)) => combine::int(!int_value),
                    Value::Numeric(Numeric::Float(real_value)) => {
                        combine::int(!(f64::from(real_value) as i64))
                    }
                    _ => unreachable!(),
                };
                Some(folded)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                Some(combine::null())
            }
            (UnaryOperator::Negative, operand) => {
                compile_value_expr(operand)?.map(|operand| {
                    // The eager path negates by subtracting from zero.
                    operand.map_with(|builder, value| {
                        let zero = builder.int(0);
                        Ok(builder.binary(BinOp::Subtract, zero, value))
                    })
                })
            }
            (UnaryOperator::BitwiseNot, operand) => compile_value_expr(operand)?.map(|operand| {
                operand.map_with(|builder, value| Ok(builder.unary(UnaryOp::BitNot, value)))
            }),
            (UnaryOperator::Not, operand) => compile_value_expr(operand)?.map(|operand| {
                operand.map_with(|builder, value| Ok(builder.unary(UnaryOp::Not, value)))
            }),
        },
        ast::Expr::Binary(lhs, op, rhs) => {
            let Some(op) = value_binop(op) else {
                return Ok(None);
            };
            // Array operands of `||` emit ArrayConcat, not Concat — but
            // arrays only arise from columns and vector functions, which
            // are not representable here, so the operand compilation
            // below already refuses them.
            let Some(lhs) = compile_value_expr(lhs.as_ref())? else {
                return Ok(None);
            };
            let Some(rhs) = compile_value_expr(rhs.as_ref())? else {
                return Ok(None);
            };
            Some(
                lhs.then(rhs)
                    .map_with(move |builder, (lhs, rhs)| Ok(builder.binary(op, lhs, rhs))),
            )
        }
        _ => None,
    })
}

fn compile_literal(literal: &ast::Literal) -> Result<Option<Compiler<'static, ValueId>>> {
    Ok(match literal {
        ast::Literal::Numeric(value) => Some(compile_numeric(value)?),
        ast::Literal::String(value) => Some(combine::text(sanitize_string(value))),
        ast::Literal::Blob(value) => {
            let bytes: ValueBlob = ast::blob_literal_hex(value)
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    // The parser has already validated the hex string.
                    let hex_byte = std::str::from_utf8(pair).unwrap();
                    u8::from_str_radix(hex_byte, 16).unwrap()
                })
                .try_collect()?;
            Some(combine::blob(bytes))
        }
        ast::Literal::Null => Some(combine::null()),
        ast::Literal::True => Some(combine::int(1)),
        ast::Literal::False => Some(combine::int(0)),
        // Keyword is rejected by the eager path; the CURRENT_* literals
        // are evaluated at compile time there. Leave both to it.
        ast::Literal::Keyword(_)
        | ast::Literal::CurrentDate
        | ast::Literal::CurrentTime
        | ast::Literal::CurrentTimestamp => None,
    })
}

fn compile_numeric(value: &str) -> Result<Compiler<'static, ValueId>> {
    Ok(match parse_numeric_literal(value)? {
        Value::Numeric(Numeric::Integer(int_value)) => combine::int(int_value),
        Value::Numeric(Numeric::Float(real_value)) => combine::real(f64::from(real_value)),
        _ => unreachable!(),
    })
}

const fn value_binop(op: &ast::Operator) -> Option<BinOp> {
    Some(match op {
        ast::Operator::Add => BinOp::Add,
        ast::Operator::Subtract => BinOp::Subtract,
        ast::Operator::Multiply => BinOp::Multiply,
        ast::Operator::Divide => BinOp::Divide,
        ast::Operator::Modulus => BinOp::Remainder,
        ast::Operator::BitwiseAnd => BinOp::BitAnd,
        ast::Operator::BitwiseOr => BinOp::BitOr,
        ast::Operator::LeftShift => BinOp::ShiftLeft,
        ast::Operator::RightShift => BinOp::ShiftRight,
        ast::Operator::Concat => BinOp::Concat,
        _ => return None,
    })
}
