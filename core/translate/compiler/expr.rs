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
//! bit-inverted numeric literals), arithmetic/bitwise/concat binary
//! operators, and column/rowid reads as opaque leaves. Comparisons,
//! functions, and short-circuit operators are future slices (see
//! docs/internals/composable-compiler-ir.md).

use turso_parser::ast::{self, UnaryOperator};

use crate::alloc::TursoIteratorExt;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::Resolver;
use crate::translate::expr::sanitize_string;
use crate::translate::plan::TableReferences;
use crate::util::parse_numeric_literal;
use crate::{Numeric, Result, Value, ValueBlob};

use super::combine::{self, Compiler};
use super::ir::{BinOp, UnaryOp, ValueId};

/// Static context the frontend needs to admit effectful leaves. Column
/// and rowid reads are only representable when the table reference
/// resolves, because the frontend must compute the expression's collation
/// context at description time.
///
/// `resolver` must be supplied by any caller whose expressions can
/// reference columns: it is how the frontend detects custom-typed
/// columns, whose overloaded operators and decode logic must stay on the
/// eager path. Only column-free contexts (unit tests) may omit it.
pub(crate) struct BuildCtx<'a> {
    pub referenced_tables: Option<&'a TableReferences>,
    pub resolver: Option<&'a Resolver<'a>>,
}

impl BuildCtx<'_> {
    /// Context for expressions that cannot contain column references
    /// (literal-only positions, unit tests).
    #[allow(dead_code)] // lib callers always have tables; tests use this
    pub const NO_TABLES: BuildCtx<'static> = BuildCtx {
        referenced_tables: None,
        resolver: None,
    };
}

/// A successfully described expression plus the collation context its
/// evaluation leaves behind. The eager path threads collation through
/// `ProgramBuilder` state as a side effect of emission order; this
/// frontend computes the same final context statically (same merge rules
/// as `binary_expr_shared`) so the integration hook can restore it after
/// emission.
pub(crate) struct Built<'a> {
    pub compiler: Compiler<'a, ValueId>,
    pub collation: Option<(CollationSeq, bool)>,
}

impl<'a> Built<'a> {
    fn plain(compiler: Compiler<'a, ValueId>) -> Self {
        Self {
            compiler,
            collation: None,
        }
    }
}

/// Try to describe `expr` (in value position) as a composable compiler.
///
/// The compile-time folds intentionally mirror the eager path in
/// `core/translate/expr/translator.rs` so both paths produce identical
/// runtime values (e.g. `-9223372036854775808` folds to `i64::MIN`
/// instead of overflowing through a real).
///
/// Callers must NOT use this while the resolver's expression→register
/// cache is enabled or expression indexes are in play for the referenced
/// tables: the frontend decomposes trees without consulting either, and
/// re-reading columns in those contexts is incorrect, not just different
/// (cursors may not be positioned on the source row). The integration
/// hook in `translate_expr` gates on both.
pub(crate) fn compile_value_expr<'a>(
    expr: &'a ast::Expr,
    ctx: &BuildCtx<'_>,
) -> Result<Option<Built<'a>>> {
    Ok(match expr {
        ast::Expr::Literal(literal) => compile_literal(literal)?,
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            compile_value_expr(exprs[0].as_ref(), ctx)?
        }
        ast::Expr::Unary(op, operand) => match (op, operand.as_ref()) {
            (UnaryOperator::Positive, operand) => compile_value_expr(operand, ctx)?,
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                // Parse the sign together with the literal so i64::MIN
                // folds to an integer instead of overflowing to a real.
                let negated = format!("-{value}");
                Some(Built::plain(compile_numeric(&negated)?))
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                let folded = match parse_numeric_literal(value)? {
                    Value::Numeric(Numeric::Integer(int_value)) => combine::int(!int_value),
                    Value::Numeric(Numeric::Float(real_value)) => {
                        combine::int(!(f64::from(real_value) as i64))
                    }
                    _ => unreachable!(),
                };
                Some(Built::plain(folded))
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                Some(Built::plain(combine::null()))
            }
            (UnaryOperator::Negative, operand) => {
                compile_value_expr(operand, ctx)?.map(|operand| Built {
                    // The eager path negates by subtracting from zero,
                    // and its unary arm does no collation bookkeeping, so
                    // the operand's context survives.
                    compiler: operand.compiler.map_with(|builder, value| {
                        let zero = builder.int(0);
                        Ok(builder.binary(BinOp::Subtract, zero, value))
                    }),
                    collation: operand.collation,
                })
            }
            (UnaryOperator::BitwiseNot, operand) => {
                compile_value_expr(operand, ctx)?.map(|operand| Built {
                    compiler: operand
                        .compiler
                        .map_with(|builder, value| Ok(builder.unary(UnaryOp::BitNot, value))),
                    collation: operand.collation,
                })
            }
            (UnaryOperator::Not, operand) => {
                compile_value_expr(operand, ctx)?.map(|operand| Built {
                    compiler: operand
                        .compiler
                        .map_with(|builder, value| Ok(builder.unary(UnaryOp::Not, value))),
                    collation: operand.collation,
                })
            }
        },
        ast::Expr::Binary(lhs, op, rhs) => {
            let Some(op) = value_binop(op) else {
                return Ok(None);
            };
            // Array concatenation emits ArrayConcat, not Concat; leave
            // any array-typed operand to the eager path.
            if matches!(op, BinOp::Concat)
                && (crate::translate::expr::expr_is_array(lhs, ctx.referenced_tables)
                    || crate::translate::expr::expr_is_array(rhs, ctx.referenced_tables))
            {
                return Ok(None);
            }
            let Some(lhs) = compile_value_expr(lhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            let Some(rhs) = compile_value_expr(rhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            Some(Built {
                compiler: lhs
                    .compiler
                    .then(rhs.compiler)
                    .map_with(move |builder, (lhs, rhs)| Ok(builder.binary(op, lhs, rhs))),
                collation: merge_collation(lhs.collation, rhs.collation),
            })
        }
        ast::Expr::Column {
            table: table_ref_id,
            column,
            ..
        } => {
            // SELF_TABLE placeholders (generated columns) re-resolve
            // through scoped context; leave them to the eager path.
            if table_ref_id.is_self_table() {
                return Ok(None);
            }
            let Some(tables) = ctx.referenced_tables else {
                return Ok(None);
            };
            let Some((_, table)) = tables.find_table_by_internal_id(*table_ref_id) else {
                return Ok(None);
            };
            let Some(table_column) = table.get_column_at(*column) else {
                return Ok(None);
            };
            // Custom-typed columns stay on the eager path: their
            // operators can be overloaded per type, which this frontend
            // does not model. Without a resolver we cannot rule that
            // out, so refuse then too.
            let Some(resolver) = ctx.resolver else {
                return Ok(None);
            };
            if resolver
                .schema()
                .get_type_def(&table_column.ty_str, table.is_strict())
                .is_some()
            {
                return Ok(None);
            }
            // Matches the eager Column arm: a column read always
            // establishes its column's collation.
            let collation = Some((table_column.collation(), false));
            Some(Built {
                compiler: Compiler::build_with(move |builder| Ok(builder.leaf(expr))),
                collation,
            })
        }
        ast::Expr::RowId {
            table: table_ref_id,
            ..
        } => {
            let Some(tables) = ctx.referenced_tables else {
                return Ok(None);
            };
            if tables.find_table_by_internal_id(*table_ref_id).is_none() {
                return Ok(None);
            }
            // The eager RowId arm does no collation bookkeeping.
            Some(Built::plain(Compiler::build_with(move |builder| {
                Ok(builder.leaf(expr))
            })))
        }
        _ => None,
    })
}

/// Collation merge rules for binary operators, mirroring
/// `binary_expr_shared` in `core/translate/expr/binary.rs`: an explicit
/// COLLATE wins (left precedence), then column collations (left
/// precedence), otherwise none.
fn merge_collation(
    left: Option<(CollationSeq, bool)>,
    right: Option<(CollationSeq, bool)>,
) -> Option<(CollationSeq, bool)> {
    match (left, right) {
        (Some((collation, true)), _) => Some((collation, true)),
        (_, Some((collation, true))) => Some((collation, true)),
        (Some((collation, from_collate)), None) => Some((collation, from_collate)),
        (None, Some((collation, from_collate))) => Some((collation, from_collate)),
        (Some((collation, from_collate)), Some((_, false))) => Some((collation, from_collate)),
        (None, None) => None,
    }
}

fn compile_literal(literal: &ast::Literal) -> Result<Option<Built<'static>>> {
    Ok(match literal {
        ast::Literal::Numeric(value) => Some(Built::plain(compile_numeric(value)?)),
        ast::Literal::String(value) => Some(Built::plain(combine::text(sanitize_string(value)))),
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
            Some(Built::plain(combine::blob(bytes)))
        }
        ast::Literal::Null => Some(Built::plain(combine::null())),
        ast::Literal::True => Some(Built::plain(combine::int(1))),
        ast::Literal::False => Some(Built::plain(combine::int(0))),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collation_merges_with_left_precedence() {
        // Explicit COLLATE wins over column collation, right-side
        // explicit beats left-side implicit.
        assert_eq!(
            merge_collation(
                Some((CollationSeq::NoCase, false)),
                Some((CollationSeq::Rtrim, true))
            ),
            Some((CollationSeq::Rtrim, true))
        );
        // Two column collations: left precedence.
        assert_eq!(
            merge_collation(
                Some((CollationSeq::NoCase, false)),
                Some((CollationSeq::Rtrim, false))
            ),
            Some((CollationSeq::NoCase, false))
        );
        // One side collated, the other not: the collated side wins.
        assert_eq!(
            merge_collation(None, Some((CollationSeq::Rtrim, false))),
            Some((CollationSeq::Rtrim, false))
        );
        assert_eq!(merge_collation(None, None), None);
    }
}
