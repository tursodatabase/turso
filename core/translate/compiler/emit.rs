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

use super::ir::{BinOp, BlockId, CmpOp, Const, Function, Inst, JumpTarget, Terminator, UnaryOp};
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
    verify(func)
        .map_err(|e| LimboError::InternalError(format!("compiler IR failed verification: {e}")))?;
    Emitter::new(program, func, dest, None).emit()
}

/// [`emit_function`] with a leaf emitter for functions whose values
/// include opaque leaves.
pub fn emit_function_with_leaves(
    program: &mut ProgramBuilder,
    func: &Function,
    dest: usize,
    leaf_emitter: &mut LeafEmitter<'_>,
) -> Result<()> {
    verify(func)
        .map_err(|e| LimboError::InternalError(format!("compiler IR failed verification: {e}")))?;
    Emitter::new(program, func, dest, Some(leaf_emitter)).emit()
}

struct Emitter<'a> {
    program: &'a mut ProgramBuilder,
    func: &'a Function,
    dest: usize,
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
}

impl<'a> Emitter<'a> {
    fn new(
        program: &'a mut ProgramBuilder,
        func: &'a Function,
        dest: usize,
        leaf_emitter: Option<&'a mut LeafEmitter<'a>>,
    ) -> Self {
        // Emission order: creation order restricted to reachable blocks.
        // Creation order keeps combinator-generated CFGs readable (arms
        // appear where they were described) and is trivially
        // deterministic.
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
                Some(Inst::Unary { operand, .. }) => is_const[operand.index()],
                Some(Inst::Binary { lhs, rhs, .. }) | Some(Inst::Compare { lhs, rhs, .. }) => {
                    is_const[lhs.index()] && is_const[rhs.index()]
                }
                // Constant only when the frontend proved the whole call
                // constant (deterministic function); the argument check
                // guards against hoisting a call whose inputs are not in
                // the same constant run.
                Some(Inst::Call { call, args }) => {
                    func.call_data(*call).constant && args.iter().all(|arg| is_const[arg.index()])
                }
                // External inputs, leaves, and block parameters read
                // state the prologue cannot see.
                Some(Inst::External { .. }) | Some(Inst::Leaf(_)) | None => false,
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
                    Inst::Unary { operand, .. } => count(operand),
                    Inst::Binary { lhs, rhs, .. } | Inst::Compare { lhs, rhs, .. } => {
                        count(lhs);
                        count(rhs);
                    }
                    Inst::Call { args, .. } => args.iter().for_each(&mut count),
                }
            }
            if let Some(terminator) = &block.terminator {
                match terminator {
                    Terminator::Jump(_) => {}
                    Terminator::Branch { cond, .. } => count(cond),
                    Terminator::Ret { value } => count(value),
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
        for &block_id in &order {
            for (_, inst) in &func.block(block_id).insts {
                let Inst::Call { call, args } = inst else {
                    continue;
                };
                let pack = program.alloc_registers(args.len());
                call_packs[call.index()] = pack;
                for (slot, arg) in args.iter().enumerate() {
                    let bindable = matches!(
                        inst_of[arg.index()],
                        Some(
                            Inst::Const(_)
                                | Inst::Unary { .. }
                                | Inst::Binary { .. }
                                | Inst::Compare { .. }
                                | Inst::Call { .. }
                                | Inst::Leaf(_)
                        )
                    );
                    if use_count[arg.index()] == 1 && regs[arg.index()].is_none() && bindable {
                        regs[arg.index()] = Some(pack + slot);
                    }
                }
            }
        }

        Self {
            program,
            func,
            dest,
            regs,
            labels: vec![None; func.blocks.len()],
            order,
            exit_label: None,
            leaf_emitter,
            is_const,
            call_packs,
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
                    let flags = CmpInsFlags::default().with_affinity(data.affinity);
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
                        constant_mask: 0,
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
                    };
                    self.program.emit_insn(insn);
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
                self.emit_goto_unless_next(target.block, next);
            }
            Terminator::Ret { value } => {
                let reg = self.reg_of(*value);
                if reg != self.dest {
                    self.program.emit_insn(Insn::Copy {
                        src_reg: reg,
                        dst_reg: self.dest,
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
            Terminator::Branch {
                cond,
                if_true,
                if_false,
                if_null,
            } => {
                let cond = self.reg_of(*cond);
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
                    // Fallthrough elision is only safe when no trampoline
                    // code will be emitted between here and the next
                    // block.
                    let fallthrough = if trampolines.is_empty() { next } else { None };
                    self.emit_goto_unless_next(if_false.block, fallthrough);
                } else {
                    let false_pc = self.edge_entry_pc(if_false, &mut trampolines);
                    self.program.emit_insn(Insn::IfNot {
                        reg: cond,
                        target_pc: false_pc,
                        jump_if_null: false,
                    });
                    // Neither truthy nor falsy: NULL falls through.
                    self.emit_edge(if_null);
                    let fallthrough = if trampolines.is_empty() { next } else { None };
                    self.emit_goto_unless_next(if_null.block, fallthrough);
                }
                for (label, target) in trampolines {
                    self.program.preassign_label_to_next_insn(label);
                    self.emit_edge(&target);
                    self.program.emit_insn(Insn::Goto {
                        target_pc: self.block_label(target.block),
                    });
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
            self.block_label(target.block)
        } else {
            let label = self.program.allocate_label();
            trampolines.push((label, target.clone()));
            label
        }
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

    fn emit_goto_unless_next(&mut self, target: BlockId, next: Option<BlockId>) {
        if next == Some(target) {
            // Fallthrough: the target is emitted immediately after. Its
            // label may still be referenced by other edges; that label is
            // preassigned when the block is emitted.
            return;
        }
        let label = self.block_label(target);
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
        let reg = if self.is_ret_value(value) && !self.dest_taken() {
            self.dest
        } else {
            self.program.alloc_register()
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
        self.regs.iter().flatten().any(|&reg| reg == self.dest)
    }
}
