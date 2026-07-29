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
//! Integration is gradual: frontends try to describe an expression and
//! fall back to the eager path when a construct is not yet representable
//! ([`try_emit_value_expr`] returns `false`). The eager fallback shrinks
//! as coverage grows; it is an escape hatch, not an architecture.

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

use turso_parser::ast;

use crate::vdbe::builder::ProgramBuilder;
use crate::Result;

/// Full pipeline for an expression in value position: describe → build
/// IR → verify → emit, leaving the result in `dest`.
///
/// Returns `Ok(false)` without emitting anything when the expression is
/// not yet representable, in which case the caller must use the eager
/// path.
pub(crate) fn try_emit_value_expr(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    dest: usize,
) -> Result<bool> {
    let Some(compiler) = expr::compile_value_expr(expr)? else {
        return Ok(false);
    };
    let mut builder = ir::FuncBuilder::new();
    let value = compiler.run(&mut builder)?;
    builder.ret(value);
    let func = builder.finish();
    emit::emit_function(program, &func, dest)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::combine::{self, Compiler};
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

    /// Run the full pipeline for a SQL expression; `None` = fell back.
    fn pipeline(sql: &str) -> Option<Vec<Insn>> {
        let expr = parse_expr(sql);
        let mut program = test_program();
        let dest = program.alloc_register();
        let emitted = try_emit_value_expr(&mut program, &expr, dest).unwrap();
        emitted.then(|| program.insns.into_iter().map(|(insn, _)| insn).collect())
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
        assert!(try_emit_value_expr(&mut program, &expr, dest).unwrap());
        let insns: Vec<_> = program.insns.iter().map(|(insn, _)| insn).collect();
        let Insn::Add { dest: add_dest, .. } = insns[insns.len() - 1] else {
            panic!("expected trailing Add, got {insns:?}");
        };
        assert_eq!(*add_dest, dest);
    }

    #[test]
    fn unsupported_shapes_fall_back_to_eager() {
        assert!(pipeline("x + 1").is_none());
        assert!(pipeline("1 = 2").is_none());
        assert!(pipeline("abs(-1)").is_none());
        assert!(pipeline("CURRENT_TIMESTAMP").is_none());
        assert!(pipeline("1 AND 2").is_none());
        assert!(pipeline("CAST(1 AS TEXT)").is_none());
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

        // Constants intern in the entry block; the branch selects an arm
        // and each arm copies its value into the join's block parameter,
        // which is bound to `dest`.
        let [Insn::Integer {
            value: 1,
            dest: cond,
        }, Insn::Integer { value: 2, dest: t }, Insn::Integer { value: 3, dest: f }, Insn::If { reg, .. }, Insn::Goto { .. }, Insn::Copy {
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
            assert!(try_emit_value_expr(&mut program, &expr, dest).unwrap());
            program
                .insns
                .iter()
                .map(|(insn, _)| format!("{insn:?}"))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
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
