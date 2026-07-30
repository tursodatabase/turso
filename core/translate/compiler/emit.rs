//! Backend: emit verified IR into a [`ProgramBuilder`].
//!
//! This is where symbolic things become physical: values get registers,
//! blocks get labels, edges become jumps, and block-parameter binding
//! becomes register copies on the incoming edge. Nothing upstream of this
//! file knows about any of those resources.
//!
//! Emission is deterministic: blocks are emitted in creation order
//! (entry first, unreachable blocks skipped), instructions in block
//! order, and registers are allocated monotonically at definition sites.

use std::collections::HashSet;

use turso_parser::ast;

use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::{CmpInsFlags, Insn};
use crate::vdbe::BranchOffset;
use crate::{LimboError, Result};

use super::ir::{
    BinOp, BlockId, CmpOp, Const, Function, Inst, JumpTarget, ScanDirection, Terminator, UnaryOp,
};
use super::verify::verify;

/// Callback that emits an opaque leaf ([`Inst::Leaf`]) by materializing
/// the given AST expression into `dest`. In production this delegates to
/// the eager `translate_expr`, which keeps cursor/index/collation
/// resolution in one place while the IR owns the tree structure around
/// it.
pub type LeafEmitter<'e> = dyn FnMut(&mut ProgramBuilder, &ast::Expr, usize) -> Result<()> + 'e;

/// Emit `func` into `program`, leaving the function's result in `dest`.
/// Functions containing [`Inst::Leaf`] must use
/// [`emit_function_with_leaves`].
///
/// The IR is verified first; malformed IR is an internal error and never
/// reaches bytecode. `dest` must already be allocated by the caller (the
/// usual pre-allocated `target_register` of the eager translation paths).
pub fn emit_function(program: &mut ProgramBuilder, func: &Function, dest: usize) -> Result<()> {
    emit_function_bound(program, func, dest, &[], None)
}

/// [`emit_function`] with a leaf emitter for functions whose values
/// include opaque leaves.
pub fn emit_function_with_leaves(
    program: &mut ProgramBuilder,
    func: &Function,
    dest: usize,
    leaf_emitter: &mut LeafEmitter<'_>,
) -> Result<()> {
    emit_function_bound(program, func, dest, &[], Some(leaf_emitter))
}

/// The general value-producing entry point: emit `func` into `program`
/// leaving its `Ret` result in `dest`, with `cursors[i]` the physical
/// VDBE cursor id bound to the function's `CursorId(i)`.
pub fn emit_function_bound(
    program: &mut ProgramBuilder,
    func: &Function,
    dest: usize,
    cursors: &[usize],
    leaf_emitter: Option<&mut LeafEmitter<'_>>,
) -> Result<()> {
    verify(func)
        .map_err(|e| LimboError::InternalError(format!("compiler IR failed verification: {e}")))?;
    bind_cursors(func, cursors)?;
    // Explicit reborrow: `Option<&mut dyn ...>` is invariant, so shorten
    // the emitter borrow to this call's lifetime by hand.
    match leaf_emitter {
        Some(leaf_emitter) => Emitter::new(
            program,
            func,
            Some(dest),
            &[],
            cursors,
            None,
            Some(&mut *leaf_emitter),
        )
        .emit(),
        None => Emitter::new(program, func, Some(dest), &[], cursors, None, None).emit(),
    }
}

/// Emit a condition island: a function whose control flow leaves through
/// declared exits rather than a `Ret` value. `exit_labels[i]` is the
/// label bound to `ExitId(i)`; empty exit blocks are bypassed entirely
/// (jumps go straight to the bound label). `cursors[i]` is the physical
/// VDBE cursor id bound to the function's `CursorId(i)`. `row_dest`, if
/// given, is the caller's pre-allocated result-column pack: every
/// `EmitRow` writes those registers instead of a freshly allocated pack,
/// matching eager result-column register numbering.
#[allow(clippy::too_many_arguments)]
pub fn emit_condition_function(
    program: &mut ProgramBuilder,
    func: &Function,
    exit_labels: &[BranchOffset],
    cursors: &[usize],
    row_dest: Option<usize>,
    fallthrough_label: Option<BranchOffset>,
    leaf_emitter: Option<&mut LeafEmitter<'_>>,
) -> Result<()> {
    verify(func)
        .map_err(|e| LimboError::InternalError(format!("compiler IR failed verification: {e}")))?;
    if exit_labels.len() != func.num_exits() {
        return Err(LimboError::InternalError(format!(
            "compiler IR: {} exit labels bound but {} exits declared",
            exit_labels.len(),
            func.num_exits()
        )));
    }
    bind_cursors(func, cursors)?;
    // Explicit reborrow: `Option<&mut dyn ...>` is invariant, so shorten
    // the emitter borrow to this call's lifetime by hand.
    match leaf_emitter {
        Some(leaf_emitter) => {
            let mut emitter = Emitter::new(
                program,
                func,
                None,
                exit_labels,
                cursors,
                row_dest,
                Some(&mut *leaf_emitter),
            );
            emitter.fallthrough_label = fallthrough_label;
            emitter.emit()
        }
        None => {
            let mut emitter =
                Emitter::new(program, func, None, exit_labels, cursors, row_dest, None);
            emitter.fallthrough_label = fallthrough_label;
            emitter.emit()
        }
    }
}

/// Check that exactly one physical cursor id was supplied per declared
/// symbolic cursor.
fn bind_cursors(func: &Function, cursors: &[usize]) -> Result<()> {
    if cursors.len() != func.num_cursors() {
        return Err(LimboError::InternalError(format!(
            "compiler IR: {} cursor ids bound but {} cursors declared",
            cursors.len(),
            func.num_cursors()
        )));
    }
    Ok(())
}

struct Emitter<'a> {
    program: &'a mut ProgramBuilder,
    func: &'a Function,
    /// Destination register for `Ret` values; `None` for condition
    /// islands, whose control flow leaves through exits instead.
    dest: Option<usize>,
    /// Labels bound to declared exits, indexed by `ExitId`.
    exit_labels: &'a [BranchOffset],
    /// Physical VDBE cursor ids bound to declared symbolic cursors,
    /// indexed by `CursorId`.
    cursors: &'a [usize],
    /// Physical register per value, assigned at definition.
    regs: Vec<Option<usize>>,
    /// Emission order: entry first, reachable blocks only.
    order: Vec<BlockId>,
    /// Label per block, allocated upfront for every reachable block so
    /// back-edges can reference blocks that were already emitted.
    labels: Vec<Option<BranchOffset>>,
    /// Label for the single exit point, if any `Ret` needed a jump.
    exit_label: Option<BranchOffset>,
    /// Emits [`Inst::Leaf`] values; absent when the function has none.
    leaf_emitter: Option<&'a mut LeafEmitter<'a>>,
    /// Whether each value is transitively constant. Runs of constant
    /// instructions emit inside constant spans so they stay eligible for
    /// hoisting into the program prologue, matching what nested eager
    /// translation does for constant subtrees of mixed expressions.
    is_const: Vec<bool>,
    /// Contiguous register pack per call site (indexed by `CallId`):
    /// `Insn::Function` requires its arguments in adjacent registers.
    call_packs: Vec<usize>,
    /// Contiguous register pack per `EmitRow` site (indexed by the
    /// instruction's own value id): `Insn::ResultRow` requires adjacent
    /// registers.
    row_packs: Vec<Option<usize>>,
    /// The label the caller binds to the first instruction after this
    /// island. Jumps to an exit bound to this label from the island's
    /// last emitted block are pure fallthrough and are elided, and
    /// branch directions prefer falling into it — matching the eager
    /// jump-on-the-opposite-condition shape.
    fallthrough_label: Option<BranchOffset>,
}

impl<'a> Emitter<'a> {
    #[allow(clippy::too_many_arguments)] // internal constructor behind the pub entry points
    fn new(
        program: &'a mut ProgramBuilder,
        func: &'a Function,
        dest: Option<usize>,
        exit_labels: &'a [BranchOffset],
        cursors: &'a [usize],
        row_dest: Option<usize>,
        leaf_emitter: Option<&'a mut LeafEmitter<'a>>,
    ) -> Self {
        // Emission order: creation order restricted to reachable blocks.
        // Creation order keeps combinator-generated CFGs readable (arms
        // appear where they were described) and is trivially
        // deterministic. Bypassable exit blocks (empty, no params) are
        // excluded: every reference to them jumps straight to the bound
        // external label, so they would be dead code.
        let mut reachable = vec![false; func.blocks.len()];
        let mut stack = vec![BlockId::ENTRY];
        reachable[BlockId::ENTRY.index()] = true;
        while let Some(block) = stack.pop() {
            if let Some(terminator) = &func.block(block).terminator {
                for target in terminator.targets() {
                    if !reachable[target.block.index()] {
                        reachable[target.block.index()] = true;
                        stack.push(target.block);
                    }
                }
            }
        }
        let order: Vec<BlockId> = reachable
            .iter()
            .enumerate()
            .filter(|(_, &reachable)| reachable)
            .map(|(index, _)| BlockId::from_index(index))
            .filter(|&block| Self::bypass_exit(func, exit_labels, block).is_none())
            .collect();

        // Transitive constness per value. Operand values are always
        // created before their users, so one pass in id order suffices.
        let mut inst_of: Vec<Option<&Inst>> = vec![None; func.num_values()];
        for block in &func.blocks {
            for (value, inst) in &block.insts {
                inst_of[value.index()] = Some(inst);
            }
        }
        let mut is_const = vec![false; func.num_values()];
        for id in 0..func.num_values() {
            is_const[id] = match inst_of[id] {
                Some(Inst::Const(_)) => true,
                Some(Inst::Unary { operand, .. })
                | Some(Inst::NullTest { operand, .. })
                | Some(Inst::Cast { operand, .. })
                | Some(Inst::Truth { operand, .. }) => is_const[operand.index()],
                Some(Inst::Binary { lhs, rhs, .. }) | Some(Inst::Compare { lhs, rhs, .. }) => {
                    is_const[lhs.index()] && is_const[rhs.index()]
                }
                // Never constant: hoisting a call into the prologue would
                // evaluate it unconditionally, and calls can throw (LIKE
                // with a malformed ESCAPE, abs(i64::MIN)) — a call guarded
                // by a CASE arm or AND short-circuit must not run early.
                // The eager path draws the same line: it marks leaf
                // constants hoistable, never Insn::Function.
                Some(Inst::Call { .. }) => false,
                // External inputs, leaves, and block parameters read
                // state the prologue cannot see.
                Some(Inst::External { .. })
                | Some(Inst::Leaf(_))
                | Some(Inst::EmitRow { .. })
                | None => false,
            };
        }

        // Use counts drive call-argument placement below: a value used
        // exactly once (by the call) can have its defining instruction
        // write directly into the pack slot, eliminating the copy.
        let mut use_count = vec![0usize; func.num_values()];
        let mut count = |value: &super::ir::ValueId| use_count[value.index()] += 1;
        for &block_id in &order {
            let block = func.block(block_id);
            for (_, inst) in &block.insts {
                match inst {
                    Inst::Const(_) | Inst::External { .. } | Inst::Leaf(_) => {}
                    Inst::Unary { operand, .. }
                    | Inst::NullTest { operand, .. }
                    | Inst::Cast { operand, .. }
                    | Inst::Truth { operand, .. } => count(operand),
                    Inst::Binary { lhs, rhs, .. } | Inst::Compare { lhs, rhs, .. } => {
                        count(lhs);
                        count(rhs);
                    }
                    Inst::Call { args, .. } => args.iter().for_each(&mut count),
                    Inst::EmitRow { values } => values.iter().for_each(&mut count),
                }
            }
            if let Some(terminator) = &block.terminator {
                match terminator {
                    Terminator::Jump(_) | Terminator::Exit(_) => {}
                    Terminator::Branch { cond, .. } => count(cond),
                    Terminator::CmpBranch { lhs, rhs, .. } => {
                        count(lhs);
                        count(rhs);
                    }
                    Terminator::NullBranch { value, .. } => count(value),
                    Terminator::Ret { value } => count(value),
                    Terminator::ScanStart { .. }
                    | Terminator::ScanAdvance { .. }
                    | Terminator::DecrJumpZero { .. }
                    | Terminator::IfPos { .. } => {}
                }
                for target in terminator.targets() {
                    target.args.iter().for_each(&mut count);
                }
            }
        }

        // Allocate one contiguous register pack per call site, in
        // creation order (deterministic), and steer single-use argument
        // definitions straight into their pack slots. Shared arguments
        // (interned constants, deduped leaves used elsewhere) keep their
        // own register and are copied into the slot at the call site.
        let mut regs: Vec<Option<usize>> = vec![None; func.num_values()];
        let mut call_packs = vec![0usize; func.num_calls()];
        let mut row_packs: Vec<Option<usize>> = vec![None; func.num_values()];
        let steer = |program: &mut ProgramBuilder,
                     regs: &mut Vec<Option<usize>>,
                     args: &[super::ir::ValueId],
                     preassigned: Option<usize>| {
            let pack = preassigned.unwrap_or_else(|| program.alloc_registers(args.len()));
            for (slot, arg) in args.iter().enumerate() {
                let bindable = matches!(
                    inst_of[arg.index()],
                    Some(
                        Inst::Const(_)
                            | Inst::Unary { .. }
                            | Inst::Binary { .. }
                            | Inst::Compare { .. }
                            | Inst::NullTest { .. }
                            | Inst::Cast { .. }
                            | Inst::Truth { .. }
                            | Inst::Call { .. }
                            | Inst::Leaf(_)
                    )
                );
                if use_count[arg.index()] == 1 && regs[arg.index()].is_none() && bindable {
                    regs[arg.index()] = Some(pack + slot);
                }
            }
            pack
        };
        for &block_id in &order {
            for (value, inst) in &func.block(block_id).insts {
                match inst {
                    Inst::Call { call, args } => {
                        call_packs[call.index()] = steer(program, &mut regs, args, None);
                    }
                    // All EmitRow sites share `row_dest` when the caller
                    // supplies one — matching eager emission, where every
                    // result row writes the same pre-allocated result
                    // column registers.
                    Inst::EmitRow { values } => {
                        row_packs[value.index()] =
                            Some(steer(program, &mut regs, values, row_dest));
                    }
                    _ => {}
                }
            }
        }

        Self {
            program,
            func,
            dest,
            exit_labels,
            cursors,
            regs,
            labels: vec![None; func.blocks.len()],
            order,
            exit_label: None,
            leaf_emitter,
            is_const,
            call_packs,
            row_packs,
            fallthrough_label: None,
        }
    }

    /// Whether `block` is an exit bound to the island's fallthrough
    /// label: control arriving there from the island's last emitted
    /// block needs no jump at all.
    fn exits_to_fallthrough(&self, block: BlockId) -> bool {
        self.fallthrough_label.is_some()
            && Self::bypass_exit(self.func, self.exit_labels, block) == self.fallthrough_label
    }

    /// The external label a jump to `block` should use instead, when
    /// `block` is an empty parameterless exit block. Bypassing avoids a
    /// chain of `Goto`s through trivial exit trampolines.
    fn bypass_exit(
        func: &Function,
        exit_labels: &[BranchOffset],
        block: BlockId,
    ) -> Option<BranchOffset> {
        let block = func.block(block);
        if !block.insts.is_empty() || !block.params.is_empty() {
            return None;
        }
        match block.terminator {
            Some(Terminator::Exit(exit)) => exit_labels.get(exit.index()).copied(),
            _ => None,
        }
    }

    fn emit(mut self) -> Result<()> {
        for i in 0..self.order.len() {
            let label = self.program.allocate_label();
            self.labels[self.order[i].index()] = Some(label);
        }
        for i in 0..self.order.len() {
            let block_id = self.order[i];
            let next = self.order.get(i + 1).copied();
            self.emit_block(block_id, next)?;
        }
        if let Some(exit) = self.exit_label {
            self.program.preassign_label_to_next_insn(exit);
        }
        Ok(())
    }

    fn emit_block(&mut self, block_id: BlockId, next: Option<BlockId>) -> Result<()> {
        let label = self.labels[block_id.index()].expect("labels are allocated upfront");
        self.program.preassign_label_to_next_insn(label);
        let block = self.func.block(block_id);
        // Block parameters are given registers when first referenced
        // (either by an incoming edge emitted earlier, or here).
        for &param in &block.params {
            let _ = self.reg_of(param);
        }
        // A maximal run of constant instructions emits inside its own
        // constant span so it stays eligible for hoisting into the
        // program prologue, matching what nested eager translation does
        // for constant subtrees of mixed expressions. If a span is
        // already open (e.g. the whole expression is constant and the
        // caller opened one), the outer span covers us.
        let mut open_span: Option<usize> = None;
        for (value, inst) in &block.insts {
            let value = *value;
            if self.is_const[value.index()] {
                if open_span.is_none() && !self.program.constant_span_is_open() {
                    open_span = Some(self.program.constant_span_start());
                }
            } else if let Some(span) = open_span.take() {
                self.program.constant_span_end(span);
            }
            match inst {
                Inst::External { reg } => {
                    // Bind, no code. The value simply *is* that register.
                    if self.regs[value.index()].is_none() {
                        self.regs[value.index()] = Some(*reg);
                    }
                }
                Inst::Leaf(leaf) => {
                    let dest = self.reg_of(value);
                    let expr = self.func.leaf_expr(*leaf);
                    let emitter = self.leaf_emitter.as_mut().ok_or_else(|| {
                        LimboError::InternalError(
                            "compiler IR: leaf value without a leaf emitter".to_string(),
                        )
                    })?;
                    emitter(self.program, expr, dest)?;
                }
                Inst::Compare { cmp, lhs, rhs } => {
                    let lhs = self.reg_of(*lhs);
                    let rhs = self.reg_of(*rhs);
                    let dest = self.reg_of(value);
                    let data = self.func.cmp_data(*cmp);
                    let flags = match data.affinity {
                        Some(affinity) => CmpInsFlags::default().with_affinity(affinity),
                        None => CmpInsFlags::default(),
                    };
                    let collation = data.collation;
                    let if_true_label = self.program.allocate_label();
                    let jump = match data.op {
                        CmpOp::Eq => Insn::Eq {
                            lhs,
                            rhs,
                            target_pc: if_true_label,
                            flags,
                            collation,
                        },
                        CmpOp::Ne => Insn::Ne {
                            lhs,
                            rhs,
                            target_pc: if_true_label,
                            flags,
                            collation,
                        },
                        CmpOp::Lt => Insn::Lt {
                            lhs,
                            rhs,
                            target_pc: if_true_label,
                            flags,
                            collation,
                        },
                        CmpOp::Le => Insn::Le {
                            lhs,
                            rhs,
                            target_pc: if_true_label,
                            flags,
                            collation,
                        },
                        CmpOp::Gt => Insn::Gt {
                            lhs,
                            rhs,
                            target_pc: if_true_label,
                            flags,
                            collation,
                        },
                        CmpOp::Ge => Insn::Ge {
                            lhs,
                            rhs,
                            target_pc: if_true_label,
                            flags,
                            collation,
                        },
                    };
                    // The eager wrap_eval_jump_expr_zero_or_null idiom:
                    // assume true, jump past the correction when the
                    // comparison holds, otherwise 0 — or NULL when either
                    // operand is NULL.
                    self.program.emit_insn(Insn::Integer { value: 1, dest });
                    self.program.emit_insn(jump);
                    self.program.emit_insn(Insn::ZeroOrNull {
                        rg1: lhs,
                        rg2: rhs,
                        dest,
                    });
                    self.program.preassign_label_to_next_insn(if_true_label);
                }
                Inst::NullTest { operand, negated } => {
                    // The eager assume-true idiom: 1, then correct to 0
                    // when the test fails. The result is never NULL.
                    let reg = self.reg_of(*operand);
                    let dest = self.reg_of(value);
                    let label = self.program.allocate_label();
                    self.program.emit_insn(Insn::Integer { value: 1, dest });
                    let jump = if *negated {
                        Insn::NotNull {
                            reg,
                            target_pc: label,
                        }
                    } else {
                        Insn::IsNull {
                            reg,
                            target_pc: label,
                        }
                    };
                    self.program.emit_insn(jump);
                    self.program.emit_insn(Insn::Integer { value: 0, dest });
                    self.program.preassign_label_to_next_insn(label);
                }
                Inst::EmitRow { values } => {
                    let pack = self.row_packs[value.index()]
                        .expect("row packs are allocated upfront for every EmitRow");
                    for (slot, &row_value) in values.iter().enumerate() {
                        let src = self.reg_of(row_value);
                        if src != pack + slot {
                            self.program.emit_insn(Insn::Copy {
                                src_reg: src,
                                dst_reg: pack + slot,
                                extra_amount: 0,
                            });
                        }
                    }
                    self.program.emit_insn(Insn::ResultRow {
                        start_reg: pack,
                        count: values.len(),
                    });
                }
                Inst::Call { call, args } => {
                    let pack = self.call_packs[call.index()];
                    // Arguments whose definitions were steered into their
                    // pack slots are already in place; everything else is
                    // copied in.
                    for (slot, &arg) in args.iter().enumerate() {
                        let src = self.reg_of(arg);
                        if src != pack + slot {
                            self.program.emit_insn(Insn::Copy {
                                src_reg: src,
                                dst_reg: pack + slot,
                                extra_amount: 0,
                            });
                        }
                    }
                    let dest = self.reg_of(value);
                    self.program.emit_insn(Insn::Function {
                        constant_mask: self.func.call_data(*call).constant_mask,
                        start_reg: pack,
                        dest,
                        func: self.func.call_data(*call).func.clone(),
                    });
                }
                Inst::Const(constant) => {
                    let dest = self.reg_of(value);
                    let insn = match constant {
                        Const::Null => Insn::Null {
                            dest,
                            dest_end: None,
                        },
                        Const::Int(v) => Insn::Integer { value: *v, dest },
                        Const::Real(v) => Insn::Real {
                            value: v.value(),
                            dest,
                        },
                        Const::Text(v) => Insn::String8 {
                            value: v.clone(),
                            dest,
                        },
                        Const::Blob(v) => Insn::Blob {
                            value: v.clone(),
                            dest,
                        },
                    };
                    self.program.emit_insn(insn);
                }
                Inst::Unary { op, operand } => {
                    let reg = self.reg_of(*operand);
                    let dest = self.reg_of(value);
                    let insn = match op {
                        UnaryOp::Not => Insn::Not { reg, dest },
                        UnaryOp::BitNot => Insn::BitNot { reg, dest },
                    };
                    self.program.emit_insn(insn);
                }
                Inst::Binary { op, lhs, rhs } => {
                    let lhs = self.reg_of(*lhs);
                    let rhs = self.reg_of(*rhs);
                    let dest = self.reg_of(value);
                    let insn = match op {
                        BinOp::Add => Insn::Add { lhs, rhs, dest },
                        BinOp::Subtract => Insn::Subtract { lhs, rhs, dest },
                        BinOp::Multiply => Insn::Multiply { lhs, rhs, dest },
                        BinOp::Divide => Insn::Divide { lhs, rhs, dest },
                        BinOp::Remainder => Insn::Remainder { lhs, rhs, dest },
                        BinOp::BitAnd => Insn::BitAnd { lhs, rhs, dest },
                        BinOp::BitOr => Insn::BitOr { lhs, rhs, dest },
                        BinOp::ShiftLeft => Insn::ShiftLeft { lhs, rhs, dest },
                        BinOp::ShiftRight => Insn::ShiftRight { lhs, rhs, dest },
                        BinOp::Concat => Insn::Concat { lhs, rhs, dest },
                        BinOp::And => Insn::And { lhs, rhs, dest },
                        BinOp::Or => Insn::Or { lhs, rhs, dest },
                    };
                    self.program.emit_insn(insn);
                }
                Inst::Cast { cast, operand } => {
                    // Insn::Cast mutates in place; the operand may be
                    // shared (interned constants), so cast a copy in
                    // this value's own register.
                    let src = self.reg_of(*operand);
                    let dest = self.reg_of(value);
                    self.program.emit_insn(Insn::Copy {
                        src_reg: src,
                        dst_reg: dest,
                        extra_amount: 0,
                    });
                    self.program.emit_insn(Insn::Cast {
                        reg: dest,
                        affinity: self.func.cast_affinity(*cast),
                    });
                }
                Inst::Truth {
                    operand,
                    null_value,
                    invert,
                } => {
                    let reg = self.reg_of(*operand);
                    let dest = self.reg_of(value);
                    self.program.emit_insn(Insn::IsTrue {
                        reg,
                        dest,
                        null_value: *null_value,
                        invert: *invert,
                    });
                }
            }
        }
        // Close the trailing constant run before control flow: jumps,
        // copies, and later blocks are not constant work.
        if let Some(span) = open_span.take() {
            self.program.constant_span_end(span);
        }

        let terminator = block
            .terminator
            .as_ref()
            .expect("verified functions have terminators on reachable blocks");
        match terminator {
            Terminator::Jump(target) => {
                self.emit_edge(target);
                self.emit_goto_unless_next(target.block, next, true);
            }
            Terminator::Ret { value } => {
                let dest = self.dest.ok_or_else(|| {
                    LimboError::InternalError(
                        "compiler IR: Ret in a function emitted without a destination".to_string(),
                    )
                })?;
                let reg = self.reg_of(*value);
                if reg != dest {
                    self.program.emit_insn(Insn::Copy {
                        src_reg: reg,
                        dst_reg: dest,
                        extra_amount: 0,
                    });
                }
                if next.is_some() {
                    let exit = *self
                        .exit_label
                        .get_or_insert_with(|| self.program.allocate_label());
                    self.program.emit_insn(Insn::Goto { target_pc: exit });
                }
            }
            Terminator::Exit(exit) => {
                let label = self.exit_labels.get(exit.index()).copied().ok_or_else(|| {
                    LimboError::InternalError(
                        "compiler IR: Exit terminator without a bound exit label".to_string(),
                    )
                })?;
                self.program.emit_insn(Insn::Goto { target_pc: label });
            }
            Terminator::CmpBranch {
                cmp,
                lhs,
                rhs,
                if_true,
                if_false,
                if_null,
            } => {
                let lhs = self.reg_of(*lhs);
                let rhs = self.reg_of(*rhs);
                let data = self.func.cmp_data(*cmp);
                let base_flags = match data.affinity {
                    Some(affinity) => CmpInsFlags::default().with_affinity(affinity),
                    None => CmpInsFlags::default(),
                };
                let collation = data.collation;
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                // Pick the jump direction so the other side falls
                // through, mirroring the eager opposite-op selection
                // (`WHERE x = 1` jumps on `Ne`). NULL routing rides the
                // jump_if_null flag: set it when the NULL target is the
                // jumped-to side; otherwise NULL falls through.
                let true_falls_through = next == Some(if_true.block)
                    || (next.is_none() && self.exits_to_fallthrough(if_true.block));
                let (op, jump_target, fall_target, null_jumps) = if true_falls_through {
                    (data.op.negated(), if_false, if_true, if_null == if_false)
                } else {
                    (data.op, if_true, if_false, if_null == if_true)
                };
                let flags = if null_jumps {
                    base_flags.jump_if_null()
                } else {
                    base_flags
                };
                let target_pc = self.edge_entry_pc(jump_target, &mut trampolines);
                let insn = match op {
                    CmpOp::Eq => Insn::Eq {
                        lhs,
                        rhs,
                        target_pc,
                        flags,
                        collation,
                    },
                    CmpOp::Ne => Insn::Ne {
                        lhs,
                        rhs,
                        target_pc,
                        flags,
                        collation,
                    },
                    CmpOp::Lt => Insn::Lt {
                        lhs,
                        rhs,
                        target_pc,
                        flags,
                        collation,
                    },
                    CmpOp::Le => Insn::Le {
                        lhs,
                        rhs,
                        target_pc,
                        flags,
                        collation,
                    },
                    CmpOp::Gt => Insn::Gt {
                        lhs,
                        rhs,
                        target_pc,
                        flags,
                        collation,
                    },
                    CmpOp::Ge => Insn::Ge {
                        lhs,
                        rhs,
                        target_pc,
                        flags,
                        collation,
                    },
                };
                self.program.emit_insn(insn);
                // The not-jumped side continues here.
                self.emit_edge(fall_target);
                self.emit_goto_unless_next(fall_target.block, next, trampolines.is_empty());
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
            Terminator::NullBranch {
                value,
                if_null,
                if_not_null,
            } => {
                let reg = self.reg_of(*value);
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                let null_falls = next == Some(if_null.block)
                    || (next.is_none() && self.exits_to_fallthrough(if_null.block));
                let not_null_falls = next == Some(if_not_null.block)
                    || (next.is_none() && self.exits_to_fallthrough(if_not_null.block));
                // Direction: honor natural fallthrough when the jumped
                // side is argless (a trampoline would defeat the point);
                // otherwise prefer jumping to an argless side so the
                // arg-carrying edge gets inline copies instead of a
                // trampoline.
                let (jump_on_null, jump_target, fall_target) =
                    if not_null_falls && if_null.args.is_empty() {
                        (true, if_null, if_not_null)
                    } else if null_falls && if_not_null.args.is_empty() {
                        (false, if_not_null, if_null)
                    } else if if_null.args.is_empty() {
                        (true, if_null, if_not_null)
                    } else if if_not_null.args.is_empty() {
                        (false, if_not_null, if_null)
                    } else {
                        (true, if_null, if_not_null)
                    };
                let target_pc = self.edge_entry_pc(jump_target, &mut trampolines);
                let jump = if jump_on_null {
                    Insn::IsNull { reg, target_pc }
                } else {
                    Insn::NotNull { reg, target_pc }
                };
                self.program.emit_insn(jump);
                self.emit_edge(fall_target);
                self.emit_goto_unless_next(fall_target.block, next, trampolines.is_empty());
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
            Terminator::ScanStart {
                cursor,
                direction,
                if_empty,
                if_rows,
            } => {
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                let pc_if_empty = self.edge_entry_pc(if_empty, &mut trampolines);
                let cursor_id = self.cursors[cursor.index()];
                self.program.emit_insn(match direction {
                    ScanDirection::Forward => Insn::Rewind {
                        cursor_id,
                        pc_if_empty,
                    },
                    ScanDirection::Backward => Insn::Last {
                        cursor_id,
                        pc_if_empty,
                    },
                });
                self.emit_edge(if_rows);
                self.emit_goto_unless_next(if_rows.block, next, trampolines.is_empty());
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
            Terminator::ScanAdvance {
                cursor,
                direction,
                if_more,
                if_done,
            } => {
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                let pc_back_edge = self.edge_entry_pc(if_more, &mut trampolines);
                let cursor_id = self.cursors[cursor.index()];
                self.program.emit_insn(match direction {
                    ScanDirection::Forward => Insn::Next {
                        cursor_id,
                        pc_if_next: pc_back_edge,
                    },
                    ScanDirection::Backward => Insn::Prev {
                        cursor_id,
                        pc_if_prev: pc_back_edge,
                    },
                });
                self.emit_edge(if_done);
                self.emit_goto_unless_next(if_done.block, next, trampolines.is_empty());
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
            Terminator::DecrJumpZero {
                counter_reg,
                if_zero,
                if_more,
            } => {
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                let target_pc = self.edge_entry_pc(if_zero, &mut trampolines);
                self.program.emit_insn(Insn::DecrJumpZero {
                    reg: *counter_reg,
                    target_pc,
                });
                self.emit_edge(if_more);
                self.emit_goto_unless_next(if_more.block, next, trampolines.is_empty());
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
            Terminator::IfPos {
                counter_reg,
                if_pos,
                if_rest,
            } => {
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                let target_pc = self.edge_entry_pc(if_pos, &mut trampolines);
                self.program.emit_insn(Insn::IfPos {
                    reg: *counter_reg,
                    target_pc,
                    decrement_by: 1,
                });
                self.emit_edge(if_rest);
                self.emit_goto_unless_next(if_rest.block, next, trampolines.is_empty());
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
            Terminator::Branch {
                cond,
                if_true,
                if_false,
                if_null,
            } => {
                let cond = self.reg_of(*cond);
                // Truthiness with the true side falling through (into
                // the next block, or out of the island): a single IfNot
                // with NULL jumping false — the eager emit_cond_jump
                // shape.
                let true_falls_through = next == Some(if_true.block)
                    || (next.is_none() && self.exits_to_fallthrough(if_true.block));
                if if_false == if_null
                    && if_true.args.is_empty()
                    && if_false.args.is_empty()
                    && true_falls_through
                {
                    let false_pc = self.jump_target_pc(if_false.block);
                    self.program.emit_insn(Insn::IfNot {
                        reg: cond,
                        target_pc: false_pc,
                        jump_if_null: true,
                    });
                    return Ok(());
                }
                // Truthy first. Arg-carrying edges need their copies to
                // happen on the edge, so they go through a local
                // trampoline; bare edges jump straight to the target.
                let mut trampolines: Vec<(BranchOffset, JumpTarget)> = Vec::new();
                let true_pc = self.edge_entry_pc(if_true, &mut trampolines);
                self.program.emit_insn(Insn::If {
                    reg: cond,
                    target_pc: true_pc,
                    jump_if_null: false,
                });
                if if_false == if_null {
                    // False and NULL share the edge: falsy or NULL both
                    // fall through here.
                    self.emit_edge(if_false);
                    self.emit_goto_unless_next(if_false.block, next, trampolines.is_empty());
                } else {
                    let false_pc = self.edge_entry_pc(if_false, &mut trampolines);
                    self.program.emit_insn(Insn::IfNot {
                        reg: cond,
                        target_pc: false_pc,
                        jump_if_null: false,
                    });
                    // Neither truthy nor falsy: NULL falls through.
                    self.emit_edge(if_null);
                    self.emit_goto_unless_next(if_null.block, next, trampolines.is_empty());
                }
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    let pc = self.jump_target_pc(target.block);
                    self.program.emit_insn(Insn::Goto { target_pc: pc });
                }
            }
        }
        Ok(())
    }

    /// Where a conditional jump should land for `target`: directly at the
    /// target block when the edge carries no arguments, otherwise at a
    /// trampoline that performs the edge's copies first.
    fn edge_entry_pc(
        &mut self,
        target: &JumpTarget,
        trampolines: &mut Vec<(BranchOffset, JumpTarget)>,
    ) -> BranchOffset {
        if target.args.is_empty() {
            self.jump_target_pc(target.block)
        } else {
            let label = self.program.allocate_label();
            trampolines.push((label, target.clone()));
            label
        }
    }

    /// Where a jump to `block` should land: the bound external label when
    /// `block` is a bypassable exit block, its own label otherwise.
    fn jump_target_pc(&self, block: BlockId) -> BranchOffset {
        Self::bypass_exit(self.func, self.exit_labels, block)
            .unwrap_or_else(|| self.block_label(block))
    }

    /// Copy edge arguments into the target's block-parameter registers.
    /// The copies of one edge are conceptually parallel; when a source
    /// register is also a destination (possible once loops carry values),
    /// all sources are staged through fresh temporaries first.
    fn emit_edge(&mut self, target: &JumpTarget) {
        let params = &self.func.block(target.block).params;
        debug_assert_eq!(params.len(), target.args.len());
        let pairs: Vec<(usize, usize)> = target
            .args
            .iter()
            .zip(params.iter())
            .map(|(&arg, &param)| (self.reg_of(arg), self.reg_of(param)))
            .filter(|(src, dst)| src != dst)
            .collect();
        let dests: HashSet<usize> = pairs.iter().map(|&(_, dst)| dst).collect();
        let overlaps = pairs.iter().any(|&(src, _)| dests.contains(&src));
        if overlaps {
            let staged: Vec<(usize, usize)> = pairs
                .iter()
                .map(|&(src, dst)| {
                    let temp = self.program.alloc_register();
                    self.program.emit_insn(Insn::Copy {
                        src_reg: src,
                        dst_reg: temp,
                        extra_amount: 0,
                    });
                    (temp, dst)
                })
                .collect();
            for (temp, dst) in staged {
                self.program.emit_insn(Insn::Copy {
                    src_reg: temp,
                    dst_reg: dst,
                    extra_amount: 0,
                });
            }
        } else {
            for (src, dst) in pairs {
                self.program.emit_insn(Insn::Copy {
                    src_reg: src,
                    dst_reg: dst,
                    extra_amount: 0,
                });
            }
        }
    }

    /// Emit the goto ending a block's straight-line path to `target`,
    /// elided (when `may_elide`) if control would arrive there anyway:
    /// the target is the next emitted block, or this is the island's
    /// last emitted code and the target exits to the label bound right
    /// after the island. `may_elide` must be false when more code (edge
    /// trampolines) follows within the same block.
    fn emit_goto_unless_next(&mut self, target: BlockId, next: Option<BlockId>, may_elide: bool) {
        if may_elide {
            if next == Some(target) {
                return;
            }
            if next.is_none() && self.exits_to_fallthrough(target) {
                return;
            }
        }
        let label = self.jump_target_pc(target);
        self.program.emit_insn(Insn::Goto { target_pc: label });
    }

    fn block_label(&self, block: BlockId) -> BranchOffset {
        self.labels[block.index()].expect("labels are allocated upfront for reachable blocks")
    }

    fn reg_of(&mut self, value: super::ir::ValueId) -> usize {
        if let Some(reg) = self.regs[value.index()] {
            return reg;
        }
        // The function result is steered into `dest` by binding the
        // first Ret value's register to it; everything else gets a fresh
        // register. External values never reach here: they are bound
        // when their defining pseudo-instruction is visited, which
        // dominates (hence precedes in emission) every use.
        let reg = match self.dest {
            Some(dest) if self.is_ret_value(value) && !self.dest_taken() => dest,
            _ => self.program.alloc_register(),
        };
        self.regs[value.index()] = Some(reg);
        reg
    }

    fn is_ret_value(&self, value: super::ir::ValueId) -> bool {
        self.order.iter().any(|&block| {
            matches!(
                self.func.block(block).terminator,
                Some(Terminator::Ret { value: v }) if v == value
            )
        })
    }

    fn dest_taken(&self) -> bool {
        self.dest
            .is_some_and(|dest| self.regs.iter().flatten().any(|&reg| reg == dest))
    }
}
