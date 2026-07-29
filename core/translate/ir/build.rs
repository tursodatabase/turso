use turso_parser::ast::{self, UnaryOperator};

use crate::alloc::TursoIteratorExt;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::Resolver;
use crate::translate::expr::sanitize_string;
use crate::translate::plan::TableReferences;
use crate::util::parse_numeric_literal;
use crate::{Numeric, Result, Value, ValueBlob};

use super::arena::{BinOp, ExprArena, UnaryOp, ValId};

/// Static context the builder needs to admit effectful leaves. Column and
/// rowid reads are only representable when the table reference resolves,
/// because the builder must compute the expression's collation context at
/// build time.
///
/// `resolver` must be supplied by any caller whose expressions can
/// reference columns: it is how the builder detects custom-typed columns,
/// whose overloaded operators and decode logic must stay on the eager
/// path. Only column-free contexts (unit tests) may omit it.
pub(crate) struct BuildCtx<'a> {
    pub referenced_tables: Option<&'a TableReferences>,
    pub resolver: Option<&'a Resolver<'a>>,
}

impl BuildCtx<'_> {
    /// Context for expressions that cannot contain column references
    /// (literal-only positions, unit tests).
    pub const NO_TABLES: BuildCtx<'static> = BuildCtx {
        referenced_tables: None,
        resolver: None,
    };
}

/// A successfully built value plus the collation context translating it
/// leaves behind. The eager path threads collation through
/// `ProgramBuilder` state as a side effect of emission order; the IR
/// computes the same final context statically (same merge rules as
/// `binary_expr_shared`) so the integration hook can restore it after
/// lowering.
pub(crate) struct Built {
    pub val: ValId,
    pub collation: Option<(CollationSeq, bool)>,
}

impl Built {
    fn plain(val: ValId) -> Self {
        Self {
            val,
            collation: None,
        }
    }
}

/// Best-effort AST → IR builder.
///
/// Returns `Ok(None)` when the expression (or any subexpression) is not
/// yet representable in the IR, in which case the caller falls back to the
/// eager translation path. Coverage grows as the migration proceeds
/// (docs/internals/declarative-bytecode-compiler.md); today: literals,
/// parenthesization, unary +/-/~/NOT, arithmetic/bitwise/concat binary
/// operators, and column/rowid reads as opaque leaves.
///
/// The compile-time folds (negative and bit-inverted numeric literals)
/// intentionally mirror the eager path in
/// `core/translate/expr/translator.rs` so the two paths produce the same
/// runtime values.
///
/// Callers must NOT use this while the resolver's expression→register
/// cache is enabled or expression indexes are in play for the referenced
/// tables: the builder decomposes trees without consulting either, and
/// re-reading columns in those contexts is incorrect, not just different
/// (cursors may not be positioned on the source row). The integration
/// hook in `translate_expr` gates on both.
pub(crate) fn try_build_value(
    arena: &mut ExprArena,
    expr: &ast::Expr,
    ctx: &BuildCtx<'_>,
) -> Result<Option<Built>> {
    Ok(match expr {
        ast::Expr::Literal(literal) => build_literal(arena, literal)?,
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            try_build_value(arena, exprs[0].as_ref(), ctx)?
        }
        ast::Expr::Unary(op, operand) => match (op, operand.as_ref()) {
            (UnaryOperator::Positive, operand) => try_build_value(arena, operand, ctx)?,
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                // Parse the sign together with the literal so i64::MIN
                // folds to an integer instead of overflowing to a real.
                let negated = format!("-{value}");
                Some(Built::plain(build_numeric(arena, &negated)?))
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(value))) => {
                let folded = match parse_numeric_literal(value)? {
                    Value::Numeric(Numeric::Integer(int_value)) => arena.int(!int_value),
                    Value::Numeric(Numeric::Float(real_value)) => {
                        arena.int(!(f64::from(real_value) as i64))
                    }
                    _ => unreachable!(),
                };
                Some(Built::plain(folded))
            }
            (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                Some(Built::plain(arena.null()))
            }
            (UnaryOperator::Negative, operand) => {
                try_build_value(arena, operand, ctx)?.map(|operand| {
                    let zero = arena.int(0);
                    Built {
                        val: arena.binary(BinOp::Subtract, zero, operand.val),
                        // The eager unary path does no collation
                        // bookkeeping, so the operand's context survives.
                        collation: operand.collation,
                    }
                })
            }
            (UnaryOperator::BitwiseNot, operand) => {
                try_build_value(arena, operand, ctx)?.map(|operand| Built {
                    val: arena.unary(UnaryOp::BitNot, operand.val),
                    collation: operand.collation,
                })
            }
            (UnaryOperator::Not, operand) => {
                try_build_value(arena, operand, ctx)?.map(|operand| Built {
                    val: arena.unary(UnaryOp::Not, operand.val),
                    collation: operand.collation,
                })
            }
        },
        ast::Expr::Binary(lhs, op, rhs) => {
            let Some(op) = ir_binop(op) else {
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
            let Some(lhs) = try_build_value(arena, lhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            let Some(rhs) = try_build_value(arena, rhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            Some(Built {
                val: arena.binary(op, lhs.val, rhs.val),
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
            // operators can be overloaded per type, which the builder
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
                val: arena.opaque(expr),
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
            Some(Built::plain(arena.opaque(expr)))
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

fn build_literal(arena: &mut ExprArena, literal: &ast::Literal) -> Result<Option<Built>> {
    Ok(match literal {
        ast::Literal::Numeric(value) => Some(Built::plain(build_numeric(arena, value)?)),
        ast::Literal::String(value) => Some(Built::plain({
            let text = sanitize_string(value);
            arena.text(text)
        })),
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
            Some(Built::plain(arena.blob(bytes)))
        }
        ast::Literal::Null => Some(Built::plain(arena.null())),
        ast::Literal::True => Some(Built::plain(arena.int(1))),
        ast::Literal::False => Some(Built::plain(arena.int(0))),
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
        let built = try_build_value(&mut arena, &expr(sql), &BuildCtx::NO_TABLES).unwrap()?;
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
            .lower_into(&mut program, built.val, dest)
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
        // Column references need referenced tables; NO_TABLES refuses.
        assert!(lower("x + 1").is_none());
        assert!(lower("1 = 2").is_none());
        assert!(lower("abs(-1)").is_none());
        assert!(lower("CURRENT_TIMESTAMP").is_none());
        assert!(lower("1 AND 2").is_none());
    }

    #[test]
    fn collation_merges_left_precedence() {
        let mut arena = ExprArena::new();
        // Literal-only trees carry no collation.
        let built = try_build_value(&mut arena, &expr("1 || 'a'"), &BuildCtx::NO_TABLES)
            .unwrap()
            .unwrap();
        assert!(built.collation.is_none());

        // Explicit-COLLATE wins over column collation, left precedence.
        assert_eq!(
            merge_collation(
                Some((CollationSeq::NoCase, false)),
                Some((CollationSeq::Rtrim, true))
            ),
            Some((CollationSeq::Rtrim, true))
        );
        assert_eq!(
            merge_collation(
                Some((CollationSeq::NoCase, false)),
                Some((CollationSeq::Rtrim, false))
            ),
            Some((CollationSeq::NoCase, false))
        );
        assert_eq!(
            merge_collation(None, Some((CollationSeq::Rtrim, false))),
            Some((CollationSeq::Rtrim, false))
        );
        assert_eq!(merge_collation(None, None), None);
    }
}
