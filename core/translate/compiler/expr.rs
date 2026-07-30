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
//! operators, column/rowid reads as opaque leaves, and scalar function
//! calls on the generic `Insn::Function` path (see
//! [`scalar_call_is_generic`]). Comparisons and short-circuit operators
//! are future slices (see docs/internals/composable-compiler-ir.md).

use turso_parser::ast::{self, UnaryOperator};

use crate::alloc::TursoIteratorExt;
use crate::function::{Func, FuncCtx, ScalarFunc};
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::Resolver;
use crate::translate::expr::sanitize_string;
use crate::translate::optimizer::Optimizable;
use crate::translate::plan::TableReferences;
use crate::util::{exprs_are_equivalent, parse_numeric_literal};
use crate::{Numeric, Result, Value, ValueBlob};

use super::combine::{self, Compiler, Predicate};
use super::ir::{BinOp, CmpOp, JumpTarget, UnaryOp, ValueId};

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

/// What evaluating an expression does to the ambient collation context,
/// assuming a clean incoming state (the eager path resets before each
/// binary operand, so subtree contributions are always clean-state).
///
/// The distinction between `Untouched` and `Sets(None)` is real eager
/// behavior: a literal leaves the caller's state alone, while a
/// non-equivalent binary always overwrites it (possibly with `None`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CollationEffect {
    /// Evaluation does not touch the collation state.
    Untouched,
    /// Evaluation overwrites the collation state with this context.
    Sets(Option<(CollationSeq, bool)>),
}

impl CollationEffect {
    /// The collation context this expression contributes to an enclosing
    /// binary operator's merge, i.e. its post-state from a clean start.
    fn contribution(self) -> Option<(CollationSeq, bool)> {
        match self {
            CollationEffect::Untouched => None,
            CollationEffect::Sets(collation) => collation,
        }
    }

    /// Apply this effect to the integration boundary: restore the state
    /// the eager path would have left behind.
    pub fn apply(self, program: &mut crate::vdbe::builder::ProgramBuilder) {
        match self {
            CollationEffect::Untouched => {}
            CollationEffect::Sets(collation) => program.set_collation(collation),
        }
    }
}

/// A successfully described expression plus its collation effect. The
/// eager path threads collation through `ProgramBuilder` state as a side
/// effect of emission order; this frontend computes the same final state
/// statically (same merge rules as `binary_expr_shared`) so the
/// integration hook can restore it after emission.
pub(crate) struct Built<'a> {
    pub compiler: Compiler<'a, ValueId>,
    pub effect: CollationEffect,
}

impl<'a> Built<'a> {
    fn plain(compiler: Compiler<'a, ValueId>) -> Self {
        Self {
            compiler,
            effect: CollationEffect::Untouched,
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
                    effect: operand.effect,
                })
            }
            (UnaryOperator::BitwiseNot, operand) => {
                compile_value_expr(operand, ctx)?.map(|operand| Built {
                    compiler: operand
                        .compiler
                        .map_with(|builder, value| Ok(builder.unary(UnaryOp::BitNot, value))),
                    effect: operand.effect,
                })
            }
            (UnaryOperator::Not, operand) => {
                compile_value_expr(operand, ctx)?.map(|operand| Built {
                    compiler: operand
                        .compiler
                        .map_with(|builder, value| Ok(builder.unary(UnaryOp::Not, value))),
                    effect: operand.effect,
                })
            }
        },
        ast::Expr::Binary(lhs_expr, op, rhs_expr) => {
            enum Kind {
                Value(BinOp),
                Cmp(CmpOp),
            }
            let kind = if let Some(op) = value_binop(op) {
                Kind::Value(op)
            } else if let Some(op) = cmp_binop(op) {
                Kind::Cmp(op)
            } else {
                return Ok(None);
            };
            // Array operands change instruction selection (ArrayConcat,
            // array_cmp flags); leave both shapes to the eager path.
            if matches!(kind, Kind::Value(BinOp::Concat) | Kind::Cmp(_))
                && (crate::translate::expr::expr_is_array(lhs_expr, ctx.referenced_tables)
                    || crate::translate::expr::expr_is_array(rhs_expr, ctx.referenced_tables))
            {
                return Ok(None);
            }
            let Some(lhs) = compile_value_expr(lhs_expr.as_ref(), ctx)? else {
                return Ok(None);
            };
            let Some(rhs) = compile_value_expr(rhs_expr.as_ref(), ctx)? else {
                return Ok(None);
            };
            match kind {
                Kind::Value(op) => {
                    // The eager equivalent-operand branch translates the
                    // shared operand once and never writes the collation
                    // state; the general branch always overwrites it with
                    // the merge result.
                    let effect = if exprs_are_equivalent(lhs_expr, rhs_expr) {
                        lhs.effect
                    } else {
                        CollationEffect::Sets(merge_collation(
                            lhs.effect.contribution(),
                            rhs.effect.contribution(),
                        ))
                    };
                    Some(Built {
                        compiler: lhs
                            .compiler
                            .then(rhs.compiler)
                            .map_with(move |builder, (lhs, rhs)| Ok(builder.binary(op, lhs, rhs))),
                        effect,
                    })
                }
                Kind::Cmp(op) => {
                    // Affinity and collation are payloads of the
                    // comparison, resolved here at description time the
                    // same way the eager path resolves them at emission
                    // time (`comparison_affinity` + the operand collation
                    // merge). Comparisons consume the collation context:
                    // the eager path resets it after emitting.
                    let affinity = crate::translate::expr::comparison_affinity(
                        lhs_expr,
                        rhs_expr,
                        ctx.referenced_tables,
                        ctx.resolver,
                    );
                    let collation =
                        merge_collation(lhs.effect.contribution(), rhs.effect.contribution())
                            .map(|(collation, _)| collation);
                    Some(Built {
                        compiler: lhs.compiler.then(rhs.compiler).map_with(
                            move |builder, (lhs, rhs)| {
                                Ok(builder.compare(op, Some(affinity), collation, lhs, rhs))
                            },
                        ),
                        effect: CollationEffect::Sets(None),
                    })
                }
            }
        }
        ast::Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        } => {
            // Only plain scalar calls; anything with aggregate/window
            // shape stays eager.
            if distinctness.is_some()
                || !order_by.is_empty()
                || !within_group.is_empty()
                || filter_over.filter_clause.is_some()
                || filter_over.over_clause.is_some()
            {
                return Ok(None);
            }
            let Some(resolver) = ctx.resolver else {
                return Ok(None);
            };
            // Unknown functions fall back so the eager path raises its
            // usual "no such function" error.
            let Some(func) = resolver.resolve_function(name.as_str(), args.len())? else {
                return Ok(None);
            };
            let Func::Scalar(scalar) = &func else {
                return Ok(None);
            };
            if !scalar_call_is_generic(scalar, args.len()) {
                return Ok(None);
            }
            let mut arg_builds = Vec::with_capacity(args.len());
            for arg in args {
                let Some(built) = compile_value_expr(arg, ctx)? else {
                    return Ok(None);
                };
                arg_builds.push(built);
            }
            // Eager translation evaluates the arguments in order with no
            // collation resets between them: the post-state is the last
            // argument that sets one.
            let effect = arg_builds
                .iter()
                .fold(CollationEffect::Untouched, |acc, arg| match arg.effect {
                    CollationEffect::Sets(collation) => CollationEffect::Sets(collation),
                    CollationEffect::Untouched => acc,
                });
            let constant = expr.is_constant(resolver);
            let func_ctx = FuncCtx {
                func,
                arg_count: args.len(),
            };
            let arg_compilers: Vec<Compiler<'a, ValueId>> =
                arg_builds.into_iter().map(|built| built.compiler).collect();
            Some(Built {
                compiler: Compiler::build_with(move |builder| {
                    let mut values = Vec::with_capacity(arg_compilers.len());
                    for arg in arg_compilers {
                        values.push(arg.run(builder)?);
                    }
                    Ok(builder.call(func_ctx, constant, values))
                }),
                effect,
            })
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            // Both CASE forms compile to a chain of per-arm blocks
            // joining in one block whose parameter carries the result —
            // the block-parameter replacement for the eager path's
            // shared target register (which forces its RegisterReuse
            // constant-hoisting deopt; IR arms use fresh registers, so
            // constant THEN values hoist safely).
            let base = match base {
                Some(base_expr) => {
                    let Some(built) = compile_value_expr(base_expr.as_ref(), ctx)? else {
                        return Ok(None);
                    };
                    Some(built)
                }
                None => None,
            };
            // Collation is compile-time state threaded through eager
            // emission order (base, then when/then per pair, then else)
            // with no resets: the running state at each base-form
            // comparison is that comparison's collation payload.
            let mut running = match &base {
                Some(built) => built.effect,
                None => CollationEffect::Untouched,
            };
            let mut pairs = Vec::with_capacity(when_then_pairs.len());
            for (when_expr, then_expr) in when_then_pairs {
                let Some(when) = compile_value_expr(when_expr.as_ref(), ctx)? else {
                    return Ok(None);
                };
                if let CollationEffect::Sets(collation) = when.effect {
                    running = CollationEffect::Sets(collation);
                }
                let payload = running.contribution().map(|(collation, _)| collation);
                let Some(then) = compile_value_expr(then_expr.as_ref(), ctx)? else {
                    return Ok(None);
                };
                if let CollationEffect::Sets(collation) = then.effect {
                    running = CollationEffect::Sets(collation);
                }
                pairs.push((when.compiler, then.compiler, payload));
            }
            let else_compiler = match else_expr {
                Some(else_expr) => {
                    let Some(built) = compile_value_expr(else_expr.as_ref(), ctx)? else {
                        return Ok(None);
                    };
                    if let CollationEffect::Sets(collation) = built.effect {
                        running = CollationEffect::Sets(collation);
                    }
                    Some(built.compiler)
                }
                None => None,
            };
            let base_compiler = base.map(|built| built.compiler);
            Some(Built {
                compiler: Compiler::build_with(move |builder| {
                    let base_value = match base_compiler {
                        Some(compiler) => Some(compiler.run(builder)?),
                        None => None,
                    };
                    // Precreate the arm blocks and the join last, so
                    // emission order matches the eager layout: each WHEN
                    // falls into its THEN, the final ELSE falls into the
                    // join.
                    let arm_blocks: Vec<(super::ir::BlockId, super::ir::BlockId)> = pairs
                        .iter()
                        .map(|_| (builder.create_block(), builder.create_block()))
                        .collect();
                    let join = builder.create_block();
                    let result = builder.add_block_param(join);
                    for ((when, then, payload), &(then_block, next_block)) in
                        pairs.into_iter().zip(&arm_blocks)
                    {
                        let when_value = when.run(builder)?;
                        match base_value {
                            // Base form: `Ne base, when -> next` with
                            // jump_if_null and no affinity conversion —
                            // a NULL comparison is an untrue WHEN.
                            Some(base_value) => builder.cmp_branch(
                                CmpOp::Eq,
                                None,
                                payload,
                                base_value,
                                when_value,
                                JumpTarget::new(then_block, Vec::new()),
                                JumpTarget::new(next_block, Vec::new()),
                                JumpTarget::new(next_block, Vec::new()),
                            ),
                            // Searched form: `IfNot when -> next`, NULL
                            // untrue.
                            None => builder.branch(
                                when_value,
                                JumpTarget::new(then_block, Vec::new()),
                                JumpTarget::new(next_block, Vec::new()),
                                JumpTarget::new(next_block, Vec::new()),
                            ),
                        }
                        builder.switch_to(then_block);
                        let then_value = then.run(builder)?;
                        builder.jump(join, vec![then_value]);
                        builder.switch_to(next_block);
                    }
                    // ELSE (or NULL) in the final fallthrough block.
                    let else_value = match else_compiler {
                        Some(compiler) => compiler.run(builder)?,
                        None => builder.null(),
                    };
                    builder.jump(join, vec![else_value]);
                    builder.switch_to(join);
                    Ok(result)
                }),
                effect: running,
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
            let effect = CollationEffect::Sets(Some((table_column.collation(), false)));
            Some(Built {
                compiler: Compiler::build_with(move |builder| Ok(builder.leaf(expr))),
                effect,
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

/// A successfully described condition plus its collation effect (the
/// effect of the *last-emitted* terminal, since collation is
/// compile-time state threaded in emission order).
pub(crate) struct CondBuilt<'a> {
    pub predicate: Predicate<'a>,
    pub effect: CollationEffect,
}

/// Try to describe `expr` in condition position: control flow that
/// leaves for a true/false/NULL continuation instead of materializing a
/// boolean. AND/OR compose predicates the way the eager path threads
/// `ConditionMetadata` labels; comparison terminals branch directly;
/// anything value-representable becomes a truthiness test (NULL treated
/// as false, as `emit_cond_jump` does). Everything else — IS/IS NOT,
/// BETWEEN, IN, LIKE, CASE, subqueries — falls back to the eager path.
pub(crate) fn compile_condition_expr<'a>(
    expr: &'a ast::Expr,
    ctx: &BuildCtx<'_>,
) -> Result<Option<CondBuilt<'a>>> {
    Ok(match expr {
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            compile_condition_expr(exprs[0].as_ref(), ctx)?
        }
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            let Some(lhs) = compile_condition_expr(lhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            let Some(rhs) = compile_condition_expr(rhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            Some(CondBuilt {
                predicate: lhs.predicate.and(rhs.predicate),
                effect: rhs.effect,
            })
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let Some(lhs) = compile_condition_expr(lhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            let Some(rhs) = compile_condition_expr(rhs.as_ref(), ctx)? else {
                return Ok(None);
            };
            Some(CondBuilt {
                predicate: lhs.predicate.or(rhs.predicate),
                effect: rhs.effect,
            })
        }
        ast::Expr::Binary(lhs_expr, op, rhs_expr) if cmp_binop(op).is_some() => {
            let op = cmp_binop(op).expect("guarded by match arm");
            // Array comparisons need array_cmp flags; leave them eager.
            if crate::translate::expr::expr_is_array(lhs_expr, ctx.referenced_tables)
                || crate::translate::expr::expr_is_array(rhs_expr, ctx.referenced_tables)
            {
                return Ok(None);
            }
            let Some(lhs) = compile_value_expr(lhs_expr.as_ref(), ctx)? else {
                return Ok(None);
            };
            let Some(rhs) = compile_value_expr(rhs_expr.as_ref(), ctx)? else {
                return Ok(None);
            };
            // Same payload capture as value-position comparisons.
            let affinity = crate::translate::expr::comparison_affinity(
                lhs_expr,
                rhs_expr,
                ctx.referenced_tables,
                ctx.resolver,
            );
            let collation = merge_collation(lhs.effect.contribution(), rhs.effect.contribution())
                .map(|(collation, _)| collation);
            let operands = lhs.compiler.then(rhs.compiler);
            Some(CondBuilt {
                predicate: Predicate::build_with(move |builder, targets| {
                    let (lhs, rhs) = operands.run(builder)?;
                    builder.cmp_branch(
                        op,
                        Some(affinity),
                        collation,
                        lhs,
                        rhs,
                        JumpTarget::new(targets.if_true, Vec::new()),
                        JumpTarget::new(targets.if_false, Vec::new()),
                        JumpTarget::new(targets.if_null, Vec::new()),
                    );
                    Ok(())
                }),
                effect: CollationEffect::Sets(None),
            })
        }
        // Any value-representable expression is a truthiness terminal in
        // condition position, exactly the set the eager path routes
        // through translate_expr + emit_cond_jump (or the non-comparison
        // BinaryEmitMode::Condition tail).
        _ => compile_value_expr(expr, ctx)?.map(|built| CondBuilt {
            predicate: Predicate::from_bool(built.compiler),
            effect: built.effect,
        }),
    })
}

/// Scalar functions whose eager translation is exactly the generic shape
/// this frontend mirrors: arguments translated in order into a contiguous
/// register block, then one `Insn::Function` — no extra instructions,
/// compile-time evaluation, or collation bookkeeping. The arity gates
/// match the eager arms' checks, so unsupported arities fall back and
/// fail with identical errors.
fn scalar_call_is_generic(func: &ScalarFunc, arg_count: usize) -> bool {
    match func {
        ScalarFunc::Abs
        | ScalarFunc::Lower
        | ScalarFunc::Upper
        | ScalarFunc::Length
        | ScalarFunc::OctetLength
        | ScalarFunc::Typeof
        | ScalarFunc::Unicode
        | ScalarFunc::Unistr
        | ScalarFunc::UnistrQuote
        | ScalarFunc::Quote
        | ScalarFunc::RandomBlob
        | ScalarFunc::Sign
        | ScalarFunc::Soundex
        | ScalarFunc::ZeroBlob => arg_count == 1,
        ScalarFunc::Trim
        | ScalarFunc::LTrim
        | ScalarFunc::RTrim
        | ScalarFunc::Round
        | ScalarFunc::Unhex => arg_count <= 2,
        ScalarFunc::Nullif | ScalarFunc::Instr => arg_count == 2,
        ScalarFunc::Min | ScalarFunc::Max | ScalarFunc::Concat => arg_count >= 1,
        ScalarFunc::Char | ScalarFunc::Printf => true,
        _ => false,
    }
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

/// The six ordinary comparisons. IS/IS NOT (null-equality semantics) and
/// LIKE/GLOB (function calls in disguise) stay on the eager path.
const fn cmp_binop(op: &ast::Operator) -> Option<CmpOp> {
    Some(match op {
        ast::Operator::Equals => CmpOp::Eq,
        ast::Operator::NotEquals => CmpOp::Ne,
        ast::Operator::Less => CmpOp::Lt,
        ast::Operator::LessEquals => CmpOp::Le,
        ast::Operator::Greater => CmpOp::Gt,
        ast::Operator::GreaterEquals => CmpOp::Ge,
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
