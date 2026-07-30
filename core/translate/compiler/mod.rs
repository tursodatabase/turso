//! Composable compiler: translation as descriptions, not mutations.
//!
//! This module is the beginning of the migration of `core/translate/`
//! away from eager `ProgramBuilder::emit_insn` mutation and toward a
//! pipeline with three separated layers
//! (docs/internals/composable-compiler-ir.md is the source of truth):
//!
//! 1. **SQL frontend** ([`expr`]) — resolves SQLite semantics and returns
//!    [`combine::Compiler`] values: composable *descriptions* of work.
//!    Constructing or combining them emits nothing.
//! 2. **Compiler IR** ([`ir`], [`verify`]) — running a description builds
//!    SSA control-flow IR: symbolic values, explicit basic blocks, block
//!    parameters instead of manually copied branch results. The IR is
//!    verified before any bytecode exists.
//! 3. **VDBE backend** ([`emit`]) — allocates physical registers and
//!    labels, and turns verified IR into `Insn`s in a `ProgramBuilder`.
//!
//! Integration is gradual: frontends try to describe an expression
//! ([`compile_value_expr`] returns `None` for unsupported shapes) and
//! fall back to the eager path. The eager fallback shrinks as coverage
//! grows; it is an escape hatch, not an architecture. Opaque leaves
//! (column and rowid reads) bridge the other direction: the IR owns the
//! tree, and emission delegates each leaf back to eager translation.

// The combinator and IR authoring surfaces are ahead of their lib-code
// callers by design: branches, block parameters, and external inputs are
// exercised by unit tests until the translation slices that need them
// land. Drop these allows as integration catches up.
#[allow(dead_code)]
pub(crate) mod combine;
pub(crate) mod emit;
pub(crate) mod expr;
#[allow(dead_code)]
pub(crate) mod ir;
pub(crate) mod verify;

pub(crate) use emit::LeafEmitter;
pub(crate) use expr::{compile_condition_expr, compile_value_expr, BuildCtx};

use crate::translate::expr::ConditionMetadata;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::BranchOffset;
use crate::Result;

/// Run a described value through build → verify → emit, leaving the
/// result in `dest`. `leaf_emitter` is required when the description
/// contains opaque leaves (column/rowid reads).
pub(crate) fn emit_value(
    program: &mut ProgramBuilder,
    compiler: combine::Compiler<'_, ir::ValueId>,
    dest: usize,
    leaf_emitter: Option<&mut LeafEmitter<'_>>,
) -> Result<()> {
    let mut builder = ir::FuncBuilder::new();
    let value = compiler.run(&mut builder)?;
    builder.ret(value);
    let func = builder.finish();
    match leaf_emitter {
        Some(leaf_emitter) => emit::emit_function_with_leaves(program, &func, dest, leaf_emitter),
        None => emit::emit_function(program, &func, dest),
    }
}

/// Run a described predicate as a condition island honoring the eager
/// [`ConditionMetadata`] contract: one side jumps to its metadata label,
/// the other falls through to the code emitted after this call.
///
/// The NULL continuation is honored when it coincides with the jumped-to
/// side (the eager `jump_if_null` flag selection); otherwise NULL takes
/// the fallthrough side, exactly as eager comparison terminals behave.
pub(crate) fn emit_condition(
    program: &mut ProgramBuilder,
    predicate: combine::Predicate<'_>,
    metadata: &ConditionMetadata,
    leaf_emitter: Option<&mut LeafEmitter<'_>>,
) -> Result<()> {
    let fallthrough = program.allocate_label();
    let (true_label, false_label) = if metadata.jump_if_condition_is_true {
        (metadata.jump_target_when_true, fallthrough)
    } else {
        (fallthrough, metadata.jump_target_when_false)
    };
    let null_label = if metadata.jump_if_condition_is_true {
        if metadata.jump_target_when_null == metadata.jump_target_when_true {
            true_label
        } else {
            false_label
        }
    } else if metadata.jump_target_when_null == metadata.jump_target_when_false {
        false_label
    } else {
        true_label
    };

    let mut builder = ir::FuncBuilder::new();
    // One exit block per distinct label, so terminals can detect NULL
    // joining the true or false side by block equality.
    let mut exit_labels: Vec<BranchOffset> = Vec::new();
    let mut exit_blocks: Vec<ir::BlockId> = Vec::new();
    let mut block_for = |builder: &mut ir::FuncBuilder, label: BranchOffset| -> ir::BlockId {
        if let Some(position) = exit_labels.iter().position(|&l| l == label) {
            return exit_blocks[position];
        }
        let exit = builder.declare_exit();
        exit_labels.push(label);
        let block = builder.exit_block(exit);
        exit_blocks.push(block);
        block
    };
    let if_true = block_for(&mut builder, true_label);
    let if_false = block_for(&mut builder, false_label);
    let if_null = block_for(&mut builder, null_label);
    predicate.run(
        &mut builder,
        combine::CondTargets {
            if_true,
            if_false,
            if_null,
        },
    )?;
    let func = builder.finish();
    emit::emit_condition_function(
        program,
        &func,
        &exit_labels,
        Some(fallthrough),
        leaf_emitter,
    )?;
    program.preassign_label_to_next_insn(fallthrough);
    Ok(())
}

#[cfg(test)]
mod tests {
    use turso_parser::ast;

    use super::combine::{self, Compiler, Predicate};
    use super::ir::{BinOp, FuncBuilder, JumpTarget, UnaryOp};
    use super::verify::{verify, VerifyError};
    use super::*;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};
    use crate::vdbe::insn::Insn;

    fn test_program() -> ProgramBuilder {
        ProgramBuilder::new(
            QueryMode::Normal,
            None,
            ProgramBuilderOpts {
                num_cursors: 0,
                approx_num_insns: 8,
                approx_num_labels: 0,
            },
        )
    }

    fn parse_expr(sql: &str) -> ast::Expr {
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

    /// Run the full pipeline for a column-free SQL expression;
    /// `None` = fell back.
    fn pipeline(sql: &str) -> Option<Vec<Insn>> {
        let expr = parse_expr(sql);
        let mut program = test_program();
        let dest = program.alloc_register();
        let built = compile_value_expr(&expr, &BuildCtx::NO_TABLES).unwrap()?;
        emit_value(&mut program, built.compiler, dest, None).unwrap();
        Some(program.insns.into_iter().map(|(insn, _)| insn).collect())
    }

    #[test]
    fn literals_compile_through_the_pipeline() {
        assert!(matches!(
            pipeline("42").unwrap()[..],
            [Insn::Integer { value: 42, .. }]
        ));
        assert!(matches!(pipeline("1.5").unwrap()[..], [Insn::Real { .. }]));
        assert!(matches!(pipeline("NULL").unwrap()[..], [Insn::Null { .. }]));
        assert!(matches!(
            pipeline("TRUE").unwrap()[..],
            [Insn::Integer { value: 1, .. }]
        ));
        assert!(matches!(
            pipeline("FALSE").unwrap()[..],
            [Insn::Integer { value: 0, .. }]
        ));
        let insns = pipeline("'it''s'").unwrap();
        let [Insn::String8 { value, .. }] = &insns[..] else {
            panic!("expected String8, got {insns:?}");
        };
        assert_eq!(value, "it's");
        let insns = pipeline("x'CAFE'").unwrap();
        let [Insn::Blob { value, .. }] = &insns[..] else {
            panic!("expected Blob, got {insns:?}");
        };
        assert_eq!(value.as_slice(), &[0xCA, 0xFE]);
    }

    #[test]
    fn literal_folds_match_the_eager_path() {
        assert!(matches!(
            pipeline("-5").unwrap()[..],
            [Insn::Integer { value: -5, .. }]
        ));
        // i64::MIN parses with its sign, not as an overflowing positive.
        assert!(matches!(
            pipeline("-9223372036854775808").unwrap()[..],
            [Insn::Integer {
                value: i64::MIN,
                ..
            }]
        ));
        assert!(matches!(
            pipeline("~5").unwrap()[..],
            [Insn::Integer { value: -6, .. }]
        ));
        assert!(matches!(
            pipeline("~NULL").unwrap()[..],
            [Insn::Null { .. }]
        ));
        assert!(matches!(
            pipeline("+7").unwrap()[..],
            [Insn::Integer { value: 7, .. }]
        ));
    }

    #[test]
    fn binary_trees_emit_operands_before_operators() {
        let insns = pipeline("(1 + 2) * 3").unwrap();
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
        let insns = pipeline("'a' || 'b'").unwrap();
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
        let insns = pipeline("-'a'").unwrap();
        assert!(matches!(
            insns[..],
            [
                Insn::String8 { .. },
                Insn::Integer { value: 0, .. },
                Insn::Subtract { .. },
            ]
        ));
    }

    #[test]
    fn identical_constants_are_interned() {
        // 1 + 1: both operands are the same SSA value, loaded once.
        let insns = pipeline("1 + 1").unwrap();
        let [Insn::Integer { value: 1, dest }, Insn::Add { lhs, rhs, .. }] = insns[..] else {
            panic!("expected Integer + Add, got {insns:?}");
        };
        assert_eq!(lhs, dest);
        assert_eq!(rhs, dest);
    }

    #[test]
    fn root_lands_in_the_requested_destination() {
        let expr = parse_expr("1 + 2");
        let mut program = test_program();
        let dest = program.alloc_register();
        let built = compile_value_expr(&expr, &BuildCtx::NO_TABLES)
            .unwrap()
            .unwrap();
        emit_value(&mut program, built.compiler, dest, None).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let Insn::Add { dest: add_dest, .. } = insns[insns.len() - 1] else {
            panic!("expected trailing Add, got {insns:?}");
        };
        assert_eq!(*add_dest, dest);
    }

    #[test]
    fn unsupported_shapes_fall_back_to_eager() {
        assert!(pipeline("x + 1").is_none());
        // IS/IS NOT carry null-equality semantics; not in the IR yet.
        assert!(pipeline("1 IS 2").is_none());
        // Function calls need a resolver; NO_TABLES has none.
        assert!(pipeline("abs(-1)").is_none());
        assert!(pipeline("CURRENT_TIMESTAMP").is_none());
        assert!(pipeline("1 AND 2").is_none());
        assert!(pipeline("CAST(1 AS TEXT)").is_none());
    }

    #[test]
    fn comparisons_expand_to_the_zero_or_null_idiom() {
        let insns = pipeline("1 < 2").unwrap();
        let [Insn::Integer {
            value: 1,
            dest: lhs_reg,
        }, Insn::Integer {
            value: 2,
            dest: rhs_reg,
        }, Insn::Integer {
            value: 1,
            dest: result,
        }, Insn::Lt { lhs, rhs, .. }, Insn::ZeroOrNull {
            rg1,
            rg2,
            dest: zdest,
        }] = insns[..]
        else {
            panic!("expected comparison idiom, got {insns:?}");
        };
        assert_eq!((lhs, rhs), (lhs_reg, rhs_reg));
        assert_eq!((rg1, rg2), (lhs_reg, rhs_reg));
        assert_eq!(zdest, result);
        // The whole comparison is constant and sits in one span.
        assert!(matches!(
            pipeline("1 = 2").unwrap()[..],
            [.., Insn::ZeroOrNull { .. }]
        ));
        assert!(matches!(
            pipeline("'a' >= 'b'").unwrap()[3],
            Insn::Ge { .. }
        ));
    }

    #[test]
    fn branch_joins_through_a_block_parameter() {
        // Describe: if 1 then 2 else 3 — no bytecode exists until emit.
        let description = combine::int(1).branch(combine::int(2), combine::int(3));
        let mut builder = FuncBuilder::new();
        let result = description.run(&mut builder).unwrap();
        builder.ret(result);
        let func = builder.finish();
        verify(&func).unwrap();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();

        // Constants intern in the entry block; the branch tests the
        // condition once (IfNot to the false arm, true arm falls
        // through) and each arm copies its value into the join's block
        // parameter, which is bound to `dest`.
        let [Insn::Integer {
            value: 1,
            dest: cond,
        }, Insn::Integer { value: 2, dest: t }, Insn::Integer { value: 3, dest: f }, Insn::IfNot { reg, .. }, Insn::Copy {
            src_reg: true_src,
            dst_reg: true_dst,
            ..
        }, Insn::Goto { .. }, Insn::Copy {
            src_reg: false_src,
            dst_reg: false_dst,
            ..
        }] = insns[..]
        else {
            panic!("unexpected shape: {insns:?}");
        };
        assert_eq!(reg, cond);
        assert_eq!((true_src, true_dst), (t, &dest));
        assert_eq!((false_src, false_dst), (f, &dest));
    }

    #[test]
    fn branch3_gives_null_its_own_arm() {
        let description = combine::null().branch3(
            combine::int(1),
            combine::int(2),
            combine::text("unknown".to_string()),
        );
        let mut builder = FuncBuilder::new();
        let result = description.run(&mut builder).unwrap();
        builder.ret(result);
        let func = builder.finish();
        verify(&func).unwrap();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        // Cond + two int consts + text const, If/IfNot dispatch, three
        // arm copies with their jumps to the join.
        assert!(
            insns.iter().any(|insn| matches!(insn, Insn::IfNot { .. })),
            "three-way branch needs IfNot for the false edge: {insns:?}"
        );
        assert_eq!(
            insns
                .iter()
                .filter(|insn| matches!(insn, Insn::Copy { dst_reg, .. } if *dst_reg == dest))
                .count(),
            3,
            "each arm must copy its result into the join parameter: {insns:?}"
        );
    }

    #[test]
    fn map_and_then_compose_without_emitting() {
        let description = combine::int(20)
            .map_with(|builder, v| {
                let two = builder.int(2);
                Ok(builder.binary(BinOp::Multiply, v, two))
            })
            .then(combine::int(2))
            .map_with(|builder, (product, two)| Ok(builder.binary(BinOp::Add, product, two)));
        let mut builder = FuncBuilder::new();
        let result = description.run(&mut builder).unwrap();
        builder.ret(result);
        let func = builder.finish();
        verify(&func).unwrap();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        // 20 and 2 load once each (2 is interned across both uses).
        assert!(matches!(
            insns[..],
            [
                Insn::Integer { value: 20, .. },
                Insn::Integer { value: 2, .. },
                Insn::Multiply { .. },
                Insn::Add { .. },
            ]
        ));
    }

    #[test]
    fn external_values_bind_without_emitting_a_load() {
        let description = combine::external(7).map_with(|builder, v| {
            let one = builder.int(1);
            Ok(builder.binary(BinOp::Add, v, one))
        });
        let mut builder = FuncBuilder::new();
        let result = description.run(&mut builder).unwrap();
        builder.ret(result);
        let func = builder.finish();

        let mut program = test_program();
        // Simulate the eager code owning registers 0..=7.
        let dest = program.alloc_registers(8);
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let [Insn::Integer { value: 1, .. }, Insn::Add { lhs, .. }] = insns[..] else {
            panic!("expected Integer + Add, got {insns:?}");
        };
        assert_eq!(*lhs, 7, "external input reads its register directly");
    }

    #[test]
    fn loop_carried_swap_stages_through_temporaries() {
        // header(p, q) jumping to itself with (q, p) requires parallel
        // copies: naive sequential Copy would clobber p before q reads
        // it. (Never executed — this is a shape test for the emitter.)
        let mut builder = FuncBuilder::new();
        let a = builder.int(1);
        let b = builder.int(2);
        let header = builder.create_block();
        let p = builder.add_block_param(header);
        let q = builder.add_block_param(header);
        builder.jump(header, vec![a, b]);
        builder.switch_to(header);
        builder.jump(header, vec![q, p]);
        // Loops never reach a Ret; give the verifier a reachable exit by
        // construction instead: none needed — Ret is not required, but
        // emission needs a `dest`, so hand it an unrelated one.
        let func = builder.finish();
        verify(&func).unwrap();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        // The self-edge must copy q and p into temporaries before
        // writing either parameter register.
        let copies: Vec<(usize, usize)> = insns
            .iter()
            .filter_map(|insn| match insn {
                Insn::Copy {
                    src_reg, dst_reg, ..
                } => Some((*src_reg, *dst_reg)),
                _ => None,
            })
            .collect();
        // Edge from entry: 2 copies. Self-edge: 2 stages + 2 writes.
        assert_eq!(copies.len(), 6, "copies: {copies:?}");
        let goto_count = insns
            .iter()
            .filter(|insn| matches!(insn, Insn::Goto { .. }))
            .count();
        assert_eq!(goto_count, 1, "self-edge jumps back to the header");
    }

    #[test]
    fn verifier_rejects_arity_mismatch() {
        let mut builder = FuncBuilder::new();
        let one = builder.int(1);
        let target = builder.create_block();
        builder.jump(target, vec![one]); // target has no params
        builder.switch_to(target);
        builder.ret(one);
        let err = verify(&builder.finish()).unwrap_err();
        assert!(matches!(err, VerifyError::ArityMismatch { .. }), "{err}");
    }

    #[test]
    fn verifier_rejects_use_not_dominated_by_def() {
        let mut builder = FuncBuilder::new();
        let cond = builder.int(1);
        let true_block = builder.create_block();
        let false_block = builder.create_block();
        builder.branch(
            cond,
            JumpTarget::new(true_block, Vec::new()),
            JumpTarget::new(false_block, Vec::new()),
            JumpTarget::new(false_block, Vec::new()),
        );
        builder.switch_to(true_block);
        let only_in_true = builder.unary(UnaryOp::Not, cond);
        builder.ret(only_in_true);
        builder.switch_to(false_block);
        // Uses a value defined only on the true path.
        let bogus = builder.unary(UnaryOp::Not, only_in_true);
        builder.ret(bogus);
        let err = verify(&builder.finish()).unwrap_err();
        assert!(
            matches!(err, VerifyError::UseNotDominatedByDef { .. }),
            "{err}"
        );
    }

    #[test]
    fn verifier_rejects_missing_terminator() {
        let mut builder = FuncBuilder::new();
        let target = builder.create_block();
        builder.jump(target, Vec::new());
        // `target` is reachable but never terminated.
        let err = verify(&builder.finish()).unwrap_err();
        assert!(
            matches!(err, VerifyError::MissingTerminator { .. }),
            "{err}"
        );
    }

    #[test]
    fn verifier_rejects_jump_to_entry() {
        let mut builder = FuncBuilder::new();
        let target = builder.create_block();
        builder.jump(target, Vec::new());
        builder.switch_to(target);
        builder.jump(super::ir::BlockId::ENTRY, Vec::new());
        let err = verify(&builder.finish()).unwrap_err();
        assert!(matches!(err, VerifyError::JumpToEntry { .. }), "{err}");
    }

    #[test]
    fn emission_is_iterative_over_deep_chains() {
        // 50k chained adds built directly against the builder: emission
        // and verification must not recurse over value chains.
        let mut builder = FuncBuilder::new();
        let mut acc = builder.int(0);
        for i in 1..=50_000i64 {
            let leaf = builder.int(i);
            acc = builder.binary(BinOp::Add, acc, leaf);
        }
        builder.ret(acc);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        // 50_001 Integer loads + 50_000 Adds.
        assert_eq!(program.insns.len(), 100_001);
    }

    #[test]
    fn emission_is_deterministic() {
        let run = || {
            let mut program = test_program();
            let dest = program.alloc_register();
            let expr = parse_expr("('a' || 'b') || 3");
            let built = compile_value_expr(&expr, &BuildCtx::NO_TABLES)
                .unwrap()
                .unwrap();
            emit_value(&mut program, built.compiler, dest, None).unwrap();
            program
                .insns
                .iter()
                .map(|(insn, _)| format!("{insn:?}"))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    /// Stub leaf emitter: materializes every leaf as `Integer 7`.
    fn stub_leaf_emitter() -> impl FnMut(&mut ProgramBuilder, &ast::Expr, usize) -> Result<()> {
        |program: &mut ProgramBuilder, _leaf: &ast::Expr, dest: usize| {
            program.emit_insn(Insn::Integer { value: 7, dest });
            Ok(())
        }
    }

    #[test]
    fn leaves_delegate_to_the_emitter_and_dedup() {
        // x + x: structurally equal leaves share one value, so the leaf
        // is materialized once and both operands read its register.
        let leaf_expr = parse_expr("x");
        let mut builder = FuncBuilder::new();
        let lhs = builder.leaf(&leaf_expr);
        let rhs = builder.leaf(&leaf_expr);
        assert_eq!(lhs, rhs);
        let sum = builder.binary(BinOp::Add, lhs, rhs);
        builder.ret(sum);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        let mut leaf_emitter = stub_leaf_emitter();
        emit::emit_function_with_leaves(&mut program, &func, dest, &mut leaf_emitter).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let [Insn::Integer {
            value: 7,
            dest: leaf_reg,
        }, Insn::Add { lhs, rhs, .. }] = insns[..]
        else {
            panic!("expected one leaf load + Add, got {insns:?}");
        };
        assert_eq!(lhs, leaf_reg);
        assert_eq!(rhs, leaf_reg);
    }

    #[test]
    fn leaf_without_emitter_is_an_error() {
        let leaf_expr = parse_expr("x");
        let mut builder = FuncBuilder::new();
        let leaf = builder.leaf(&leaf_expr);
        builder.ret(leaf);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        let err = emit::emit_function(&mut program, &func, dest);
        assert!(matches!(err, Err(crate::LimboError::InternalError(_))));
    }

    #[test]
    fn constant_subtrees_of_mixed_trees_emit_in_spans() {
        // (1 + 2) + x: the constant subtree emits inside a constant span
        // so it hoists into the prologue; the leaf does not.
        let mut builder = FuncBuilder::new();
        let one = builder.int(1);
        let two = builder.int(2);
        let sum = builder.binary(BinOp::Add, one, two);
        let leaf_expr = parse_expr("x");
        let leaf = builder.leaf(&leaf_expr);
        let root = builder.binary(BinOp::Add, sum, leaf);
        builder.ret(root);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        let mut leaf_emitter = stub_leaf_emitter();
        emit::emit_function_with_leaves(&mut program, &func, dest, &mut leaf_emitter).unwrap();
        // One span covering exactly the constant run: Integer 1,
        // Integer 2, Add — instructions 0..=2.
        assert_eq!(program.constant_spans, vec![(0, 2)]);
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        assert!(matches!(
            insns[..],
            [
                Insn::Integer { value: 1, .. },
                Insn::Integer { value: 2, .. },
                Insn::Add { .. },
                Insn::Integer { value: 7, .. },
                Insn::Add { .. },
            ]
        ));
    }

    fn abs_ctx() -> crate::function::FuncCtx {
        crate::function::FuncCtx {
            func: crate::function::Func::Scalar(crate::function::ScalarFunc::Abs),
            arg_count: 1,
        }
    }

    #[test]
    fn call_args_steer_into_the_pack() {
        // abs(5): the single-use argument's definition writes directly
        // into the pack slot — no Copy, exactly the eager shape.
        let mut builder = FuncBuilder::new();
        let five = builder.int(5);
        let call = builder.call(abs_ctx(), true, vec![five]);
        builder.ret(call);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let [Insn::Integer {
            value: 5,
            dest: arg_reg,
        }, Insn::Function {
            start_reg,
            dest: fdest,
            ..
        }] = insns[..]
        else {
            panic!("expected Integer + Function, got {insns:?}");
        };
        assert_eq!(arg_reg, start_reg, "argument lands in the pack slot");
        assert_eq!(*fdest, dest);
        // The whole call is constant: one span covering both insns.
        assert_eq!(program.constant_spans, vec![(0, 1)]);
    }

    #[test]
    fn shared_call_args_are_copied_into_slots() {
        // f(v, v) where v is one interned constant: the shared value
        // keeps its register and is copied into both pack slots.
        let mut builder = FuncBuilder::new();
        let seven = builder.int(7);
        let ctx = crate::function::FuncCtx {
            func: crate::function::Func::Scalar(crate::function::ScalarFunc::Instr),
            arg_count: 2,
        };
        let call = builder.call(ctx, true, vec![seven, seven]);
        builder.ret(call);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let [Insn::Integer { dest: v, .. }, Insn::Copy {
            src_reg: s1,
            dst_reg: d1,
            ..
        }, Insn::Copy {
            src_reg: s2,
            dst_reg: d2,
            ..
        }, Insn::Function { start_reg, .. }] = insns[..]
        else {
            panic!("expected Integer + 2 Copies + Function, got {insns:?}");
        };
        assert_eq!((s1, s2), (v, v));
        assert_eq!(*d1, *start_reg);
        assert_eq!(*d2, *start_reg + 1);
    }

    #[test]
    fn nested_calls_chain_through_pack_slots() {
        // outer(inner(x)): the inner call's single-use result is steered
        // into the outer call's pack slot — no intermediate Copy.
        let mut builder = FuncBuilder::new();
        let x = builder.int(3);
        let inner = builder.call(abs_ctx(), true, vec![x]);
        let outer = builder.call(abs_ctx(), true, vec![inner]);
        builder.ret(outer);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let [Insn::Integer { .. }, Insn::Function {
            dest: inner_dest, ..
        }, Insn::Function {
            start_reg: outer_pack,
            dest: outer_dest,
            ..
        }] = insns[..]
        else {
            panic!("expected Integer + 2 Functions, got {insns:?}");
        };
        assert_eq!(inner_dest, outer_pack);
        assert_eq!(*outer_dest, dest);
    }

    #[test]
    fn non_constant_calls_do_not_join_spans() {
        // A call marked non-constant (e.g. randomblob) must not emit
        // inside a constant span even when its argument is constant.
        let mut builder = FuncBuilder::new();
        let n = builder.int(8);
        let call = builder.call(abs_ctx(), false, vec![n]);
        builder.ret(call);
        let func = builder.finish();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        // Only the Integer argument is in a span; the Function is not.
        assert_eq!(program.constant_spans, vec![(0, 0)]);
    }

    #[test]
    fn leaf_dedup_respects_dominance() {
        // A leaf read placed in a non-entry block must not be reused
        // from a sibling block it does not dominate (the partial-index
        // `(a>10 AND b>10) OR (a<2 AND b<2)` shape). Entry-block reads
        // are reusable from anywhere.
        let leaf_expr = parse_expr("b");
        let mut builder = FuncBuilder::new();
        let entry_read = builder.leaf(&leaf_expr);
        let sibling_a = builder.create_block();
        let sibling_b = builder.create_block();
        builder.branch(
            entry_read,
            JumpTarget::new(sibling_a, Vec::new()),
            JumpTarget::new(sibling_b, Vec::new()),
            JumpTarget::new(sibling_b, Vec::new()),
        );
        builder.switch_to(sibling_a);
        // Entry read dominates: reused.
        assert_eq!(builder.leaf(&leaf_expr), entry_read);
        let other_expr = parse_expr("c");
        let read_in_a = builder.leaf(&other_expr);
        builder.ret(read_in_a);
        builder.switch_to(sibling_b);
        // sibling_a does not dominate sibling_b: fresh read.
        let read_in_b = builder.leaf(&other_expr);
        assert_ne!(read_in_a, read_in_b);
        builder.ret(read_in_b);
        verify(&builder.finish()).unwrap();
    }

    #[test]
    fn condition_predicates_emit_branching_islands() {
        use crate::translate::expr::ConditionMetadata;
        // external(3) AND external(4), standard WHERE contract: jump to
        // `false_label` when false/null, fall through when true.
        let mut program = test_program();
        let false_label = program.allocate_label();
        let metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: program.allocate_label(),
            jump_target_when_false: false_label,
            jump_target_when_null: false_label,
        };
        let predicate = Predicate::from_bool(combine::external(3))
            .and(Predicate::from_bool(combine::external(4)));
        emit_condition(&mut program, predicate, &metadata, None).unwrap();
        program.preassign_label_to_next_insn(metadata.jump_target_when_true);
        program.preassign_label_to_next_insn(false_label);
        program.resolve_labels().unwrap();

        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        // Both truthiness terminals take the eager IfNot shape: jump to
        // the false exit (NULL included), fall through on true — the
        // left into the right, the right out of the island.
        let [Insn::IfNot {
            reg: 3,
            jump_if_null: true,
            ..
        }, Insn::IfNot {
            reg: 4,
            jump_if_null: true,
            ..
        }] = insns[..]
        else {
            panic!("expected two IfNot terminals, got {insns:?}");
        };
    }

    #[test]
    fn comparison_conditions_branch_without_materializing() {
        use super::ir::CmpOp;
        use crate::translate::expr::ConditionMetadata;
        use crate::vdbe::affinity::Affinity;
        // external(1) < external(2) as a WHERE terminal: one comparison
        // jump, no Integer/ZeroOrNull boolean materialization.
        let mut program = test_program();
        let false_label = program.allocate_label();
        let metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: program.allocate_label(),
            jump_target_when_false: false_label,
            jump_target_when_null: false_label,
        };
        let predicate = Predicate::build_with(|builder, targets| {
            let lhs = builder.external(1);
            let rhs = builder.external(2);
            builder.cmp_branch(
                CmpOp::Lt,
                Some(Affinity::Numeric),
                None,
                lhs,
                rhs,
                JumpTarget::new(targets.if_true, Vec::new()),
                JumpTarget::new(targets.if_false, Vec::new()),
                JumpTarget::new(targets.if_null, Vec::new()),
            );
            Ok(())
        });
        emit_condition(&mut program, predicate, &metadata, None).unwrap();
        program.preassign_label_to_next_insn(metadata.jump_target_when_true);
        program.preassign_label_to_next_insn(false_label);
        program.resolve_labels().unwrap();

        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        // The eager shape: one negated comparison jumping to the false
        // exit (NULL included via jump_if_null), true falls through out
        // of the island. No boolean is materialized.
        let [Insn::Ge { lhs: 1, rhs: 2, .. }] = insns[..] else {
            panic!("expected a single negated comparison, got {insns:?}");
        };
    }

    #[test]
    fn searched_case_joins_arms_through_a_block_parameter() {
        // CASE WHEN 1 THEN 'a' ELSE 'b' END: constants intern in the
        // entry, IfNot picks the arm, each arm copies its result into
        // the join parameter (bound to dest), the else arm falls into
        // the join.
        let insns = pipeline("CASE WHEN 1 THEN 'a' ELSE 'b' END").unwrap();
        let [Insn::Integer { value: 1, .. }, Insn::String8 { dest: a, .. }, Insn::String8 { dest: b, .. }, Insn::IfNot {
            jump_if_null: true, ..
        }, Insn::Copy {
            src_reg: then_src,
            dst_reg: then_dst,
            ..
        }, Insn::Goto { .. }, Insn::Copy {
            src_reg: else_src,
            dst_reg: else_dst,
            ..
        }] = insns[..]
        else {
            panic!("unexpected searched CASE shape: {insns:?}");
        };
        assert_eq!(then_src, a);
        assert_eq!(else_src, b);
        assert_eq!(then_dst, else_dst);
    }

    #[test]
    fn base_case_compares_with_ne_and_no_boolean() {
        // CASE 2 WHEN 1 THEN 10 ELSE 30 END: the eager shape — one Ne
        // jump per WHEN (NULL untrue via jump_if_null), no materialized
        // comparison result.
        let insns = pipeline("CASE 2 WHEN 1 THEN 10 ELSE 30 END").unwrap();
        assert!(
            insns.iter().any(|insn| matches!(insn, Insn::Ne { .. })),
            "{insns:?}"
        );
        assert!(
            !insns
                .iter()
                .any(|insn| matches!(insn, Insn::ZeroOrNull { .. } | Insn::Eq { .. })),
            "base CASE must branch on Ne, not materialize equality: {insns:?}"
        );
        // Missing ELSE means ELSE NULL.
        let insns = pipeline("CASE WHEN 0 THEN 1 END").unwrap();
        assert!(
            insns.iter().any(|insn| matches!(insn, Insn::Null { .. })),
            "{insns:?}"
        );
    }

    #[test]
    fn null_tests_produce_boolean_values() {
        // NULL ISNULL: the eager assume-true idiom, result never NULL.
        let insns = pipeline("NULL ISNULL").unwrap();
        let [Insn::Null { .. }, Insn::Integer { value: 1, dest: d1 }, Insn::IsNull { .. }, Insn::Integer { value: 0, dest: d0 }] =
            insns[..]
        else {
            panic!("unexpected NullTest shape: {insns:?}");
        };
        assert_eq!(d1, d0);
        assert!(matches!(
            pipeline("5 NOTNULL").unwrap()[2],
            Insn::NotNull { .. }
        ));
    }

    #[test]
    fn null_branches_chain_coalesce_style() {
        // coalesce(v1, 5): keep v1 unless NULL, else 5 — the join block
        // parameter carries whichever side won.
        let mut builder = FuncBuilder::new();
        let first = builder.null();
        let fallback_block = builder.create_block();
        let join = builder.create_block();
        let result = builder.add_block_param(join);
        builder.null_branch(
            first,
            JumpTarget::new(fallback_block, Vec::new()),
            JumpTarget::new(join, vec![first]),
        );
        builder.switch_to(fallback_block);
        let five = builder.int(5);
        builder.jump(join, vec![five]);
        builder.switch_to(join);
        builder.ret(result);
        let func = builder.finish();
        verify(&func).unwrap();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        // Argless-side preference: IsNull jumps to the fallback arm, the
        // non-null side copies into the join parameter inline. The
        // constant 5 interns into the entry block.
        let [Insn::Null { dest: v, .. }, Insn::Integer {
            value: 5,
            dest: fb_src,
        }, Insn::IsNull { reg, .. }, Insn::Copy {
            src_reg: keep_src,
            dst_reg: keep_dst,
            ..
        }, Insn::Goto { .. }, Insn::Copy {
            src_reg: fb_copy_src,
            dst_reg: fb_dst,
            ..
        }] = insns[..]
        else {
            panic!("unexpected coalesce shape: {insns:?}");
        };
        assert_eq!(reg, v);
        assert_eq!(keep_src, v);
        assert_eq!(fb_copy_src, fb_src);
        assert_eq!(keep_dst, fb_dst);
        assert_eq!(*keep_dst, dest);
    }

    #[test]
    fn is_null_conditions_use_nullness_jumps() {
        use crate::translate::expr::ConditionMetadata;
        // WHERE external(3) IS NULL, standard contract: matches the
        // eager single NotNull -> false shape.
        let mut program = test_program();
        let false_label = program.allocate_label();
        let metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: program.allocate_label(),
            jump_target_when_false: false_label,
            jump_target_when_null: false_label,
        };
        let predicate = Predicate::build_with(|builder, targets| {
            let value = builder.external(3);
            builder.null_branch(
                value,
                JumpTarget::new(targets.if_true, Vec::new()),
                JumpTarget::new(targets.if_false, Vec::new()),
            );
            Ok(())
        });
        emit_condition(&mut program, predicate, &metadata, None).unwrap();
        program.preassign_label_to_next_insn(metadata.jump_target_when_true);
        program.preassign_label_to_next_insn(false_label);
        program.resolve_labels().unwrap();
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let [Insn::NotNull { reg: 3, .. }] = insns[..] else {
            panic!("expected a single NotNull jump, got {insns:?}");
        };
    }

    #[test]
    fn pure_and_map_pass_values_through() {
        let description = Compiler::pure(41).map(|v| v + 1);
        let mut builder = FuncBuilder::new();
        assert_eq!(description.run(&mut builder).unwrap(), 42);
    }

    #[test]
    fn and_then_builds_compilers_recursively() {
        // Recursively assemble a right-leaning chain of adds from data,
        // demonstrating that continuations can construct sub-compilers.
        fn chain(values: Vec<i64>) -> Compiler<'static, ValueIdAlias> {
            let mut iter = values.into_iter();
            let first = iter.next().expect("non-empty");
            iter.fold(combine::int(first), |acc, next| {
                acc.and_then(move |lhs| {
                    combine::int(next)
                        .map_with(move |builder, rhs| Ok(builder.binary(BinOp::Add, lhs, rhs)))
                })
            })
        }
        type ValueIdAlias = super::ir::ValueId;

        let mut builder = FuncBuilder::new();
        let result = chain(vec![1, 2, 3, 4]).run(&mut builder).unwrap();
        builder.ret(result);
        let func = builder.finish();
        verify(&func).unwrap();

        let mut program = test_program();
        let dest = program.alloc_register();
        emit::emit_function(&mut program, &func, dest).unwrap();
        let adds = program
            .insns
            .iter()
            .filter(|(insn, _)| matches!(insn, Insn::Add { .. }))
            .count();
        assert_eq!(adds, 3);
    }
}
