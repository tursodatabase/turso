use turso_parser::ast::{self, UnaryOperator};

use crate::alloc::TursoIteratorExt;
use crate::translate::expr::sanitize_string;
use crate::util::parse_numeric_literal;
use crate::{Numeric, Result, Value, ValueBlob};

use super::arena::{BinOp, ExprArena, UnaryOp, ValId};

/// Best-effort AST → IR builder.
///
/// Returns `Ok(None)` when the expression (or any subexpression) is not
/// yet representable in the IR, in which case the caller falls back to the
/// eager translation path. Coverage grows as the migration proceeds
/// (docs/internals/declarative-bytecode-compiler.md); today it is the
/// literal-only value subset: literals, parenthesization, unary +/-/~/NOT,
/// and arithmetic/bitwise/concat binary operators.
///
/// The compile-time folds (negative and bit-inverted numeric literals)
/// intentionally mirror the eager path in
/// `core/translate/expr/translator.rs` so the two paths produce the same
/// runtime values.
pub(crate) fn try_build_value(arena: &mut ExprArena, expr: &ast::Expr) -> Result<Option<ValId>> {
    Ok(match expr {
        ast::Expr::Literal(literal) => build_literal(arena, literal)?,
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            try_build_value(arena, exprs[0].as_ref())?
        }
        ast::Expr::Unary(op, operand) => match (op, operand.as_ref()) {
            (UnaryOperator::Positive, operand) => try_build_value(arena, operand)?,
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                // Parse the sign together with the literal so i64::MIN
                // folds to an integer instead of overflowing to a real.
                let negated = format!("-{value}");
                Some(build_numeric(arena, &negated)?)
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                match parse_numeric_literal(value)? {
                    Value::Numeric(Numeric::Integer(int_value)) => Some(arena.int(!int_value)),
                    Value::Numeric(Numeric::Float(real_value)) => {
                        Some(arena.int(!(f64::from(real_value) as i64)))
                    }
                    _ => unreachable!(),
                }
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                Some(arena.null())
            }
            (UnaryOperator::Negative, operand) => try_build_value(arena, operand)?.map(|val| {
                let zero = arena.int(0);
                arena.binary(BinOp::Subtract, zero, val)
            }),
            (UnaryOperator::BitwiseNot, operand) => {
                try_build_value(arena, operand)?.map(|val| arena.unary(UnaryOp::BitNot, val))
            }
            (UnaryOperator::Not, operand) => {
                try_build_value(arena, operand)?.map(|val| arena.unary(UnaryOp::Not, val))
            }
        },
        ast::Expr::Binary(lhs, op, rhs) => {
            let Some(op) = ir_binop(op) else {
                return Ok(None);
            };
            let Some(lhs) = try_build_value(arena, lhs.as_ref())? else {
                return Ok(None);
            };
            let Some(rhs) = try_build_value(arena, rhs.as_ref())? else {
                return Ok(None);
            };
            Some(arena.binary(op, lhs, rhs))
        }
        _ => None,
    })
}

fn build_literal(arena: &mut ExprArena, literal: &ast::Literal) -> Result<Option<ValId>> {
    Ok(match literal {
        ast::Literal::Numeric(value) => Some(build_numeric(arena, value)?),
        ast::Literal::String(value) => Some(arena.text(sanitize_string(value))),
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
            Some(arena.blob(bytes))
        }
        ast::Literal::Null => Some(arena.null()),
        ast::Literal::True => Some(arena.int(1)),
        ast::Literal::False => Some(arena.int(0)),
        // Keyword is rejected by the eager path; the CURRENT_* literals
        // are evaluated at compile time there. Leave both to it.
        ast::Literal::Keyword(_)
        | ast::Literal::CurrentDate
        | ast::Literal::CurrentTime
        | ast::Literal::CurrentTimestamp => None,
    })
}

fn build_numeric(arena: &mut ExprArena, value: &str) -> Result<ValId> {
    Ok(match parse_numeric_literal(value)? {
        Value::Numeric(Numeric::Integer(int_value)) => arena.int(int_value),
        Value::Numeric(Numeric::Float(real_value)) => arena.real(f64::from(real_value)),
        _ => unreachable!(),
    })
}

const fn ir_binop(op: &ast::Operator) -> Option<BinOp> {
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

#[cfg(test)]
mod tests {
    use super::super::Lowerer;
    use super::*;
    use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
    use crate::vdbe::insn::Insn;

    fn expr(sql: &str) -> ast::Expr {
        let stmt = format!("SELECT {sql}");
        let ast::Cmd::Stmt(ast::Stmt::Select(select)) =
            turso_parser::parser::Parser::new(stmt.as_bytes())
                .next_cmd()
                .unwrap()
                .unwrap()
        else {
            panic!("expected SELECT statement for {sql:?}");
        };
        let ast::OneSelect::Select { columns, .. } = select.body.select else {
            panic!("expected simple SELECT for {sql:?}");
        };
        let ast::ResultColumn::Expr(expr, _) = columns.into_iter().next().unwrap() else {
            panic!("expected expression column for {sql:?}");
        };
        *expr
    }

    fn lower(sql: &str) -> Option<Vec<Insn>> {
        let mut arena = ExprArena::new();
        let val = try_build_value(&mut arena, &expr(sql)).unwrap()?;
        let mut program = ProgramBuilder::new(
            QueryMode::Normal,
            None,
            ProgramBuilderOpts {
                num_cursors: 0,
                approx_num_insns: 8,
                approx_num_labels: 0,
            },
        );
        let dest = program.alloc_register();
        Lowerer::new(&arena)
            .lower_into(&mut program, val, dest)
            .unwrap();
        Some(program.insns.into_iter().map(|(insn, _)| insn).collect())
    }

    #[test]
    fn literals_build_and_lower() {
        assert!(matches!(
            lower("42").unwrap()[..],
            [Insn::Integer { value: 42, .. }]
        ));
        assert!(matches!(lower("1.5").unwrap()[..], [Insn::Real { .. }]));
        assert!(matches!(lower("NULL").unwrap()[..], [Insn::Null { .. }]));
        assert!(matches!(
            lower("TRUE").unwrap()[..],
            [Insn::Integer { value: 1, .. }]
        ));
        assert!(matches!(
            lower("FALSE").unwrap()[..],
            [Insn::Integer { value: 0, .. }]
        ));
        let insns = lower("'it''s'").unwrap();
        let [Insn::String8 { value, .. }] = &insns[..] else {
            panic!("expected String8, got {insns:?}");
        };
        assert_eq!(value, "it's");
        let insns = lower("x'CAFE'").unwrap();
        let [Insn::Blob { value, .. }] = &insns[..] else {
            panic!("expected Blob, got {insns:?}");
        };
        assert_eq!(value.as_slice(), &[0xCA, 0xFE]);
    }

    #[test]
    fn unary_literal_folds_match_eager_path() {
        assert!(matches!(
            lower("-5").unwrap()[..],
            [Insn::Integer { value: -5, .. }]
        ));
        // i64::MIN parses with its sign, not as an overflowing positive.
        assert!(matches!(
            lower("-9223372036854775808").unwrap()[..],
            [Insn::Integer {
                value: i64::MIN,
                ..
            }]
        ));
        assert!(matches!(
            lower("~5").unwrap()[..],
            [Insn::Integer { value: -6, .. }]
        ));
        assert!(matches!(lower("~NULL").unwrap()[..], [Insn::Null { .. }]));
        assert!(matches!(
            lower("+7").unwrap()[..],
            [Insn::Integer { value: 7, .. }]
        ));
    }

    #[test]
    fn binary_trees_lower_bottom_up() {
        let insns = lower("(1 + 2) * 3").unwrap();
        assert!(matches!(
            insns[..],
            [
                Insn::Integer { value: 1, .. },
                Insn::Integer { value: 2, .. },
                Insn::Add { .. },
                Insn::Integer { value: 3, .. },
                Insn::Multiply { .. },
            ]
        ));
        let insns = lower("'a' || 'b'").unwrap();
        assert!(matches!(
            insns[..],
            [
                Insn::String8 { .. },
                Insn::String8 { .. },
                Insn::Concat { .. },
            ]
        ));
    }

    #[test]
    fn negation_of_non_numeric_subtracts_from_zero() {
        let insns = lower("-'a'").unwrap();
        assert!(matches!(
            insns[..],
            [
                Insn::Integer { value: 0, .. },
                Insn::String8 { .. },
                Insn::Subtract { .. },
            ]
        ));
    }

    #[test]
    fn unsupported_shapes_fall_back() {
        assert!(lower("x + 1").is_none());
        assert!(lower("1 = 2").is_none());
        assert!(lower("abs(-1)").is_none());
        assert!(lower("CURRENT_TIMESTAMP").is_none());
        assert!(lower("1 AND 2").is_none());
    }
}
