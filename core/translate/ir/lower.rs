use std::collections::HashMap;

use turso_parser::ast;

use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{LimboError, Result};

use super::arena::{BinOp, ExprArena, Node, SlotId, UnaryOp, ValId};

/// Callback that lowers an opaque leaf ([`Node::Opaque`]) by emitting the
/// given AST expression into `dest`. In production this delegates to the
/// eager `translate_expr`, which keeps cursor/index/collation resolution
/// in one place while the IR owns the tree structure around it.
pub type OpaqueEmitter<'e> = dyn FnMut(&mut ProgramBuilder, &ast::Expr, usize) -> Result<()> + 'e;

/// Materializes values from an [`ExprArena`] into a [`ProgramBuilder`].
///
/// One `Lowerer` instance is one *region*: it memoizes node → register, so
/// every distinct value is computed at most once and shared uses read the
/// same register. The memo is only sound while no effectful instruction
/// (cursor movement, slot write, control transfer) invalidates a computed
/// value, so frontends must drop the lowerer and create a fresh one at
/// every such boundary.
pub struct Lowerer<'a> {
    arena: &'a ExprArena,
    /// Node → register holding its value, for the current region.
    lowered: HashMap<ValId, usize>,
    /// Frontend-provided register bindings for mutable slots.
    slots: HashMap<SlotId, usize>,
    /// Lowers [`Node::Opaque`] leaves; absent when the graph has none.
    opaque_emitter: Option<&'a mut OpaqueEmitter<'a>>,
}

/// Work item for the explicit post-order walk. `Visit` schedules a node's
/// operands; `Emit` runs once its operands are all lowered; `EndSpan`
/// closes the constant span opened for a maximal constant subtree.
enum Task {
    Visit(ValId),
    Emit(ValId),
    EndSpan(usize),
}

impl<'a> Lowerer<'a> {
    pub fn new(arena: &'a ExprArena) -> Self {
        Self {
            arena,
            lowered: HashMap::new(),
            slots: HashMap::new(),
            opaque_emitter: None,
        }
    }

    /// A lowerer that can materialize [`Node::Opaque`] leaves through the
    /// given callback.
    pub fn with_opaque_emitter(arena: &'a ExprArena, emitter: &'a mut OpaqueEmitter<'a>) -> Self {
        Self {
            arena,
            lowered: HashMap::new(),
            slots: HashMap::new(),
            opaque_emitter: Some(emitter),
        }
    }

    /// Bind a mutable slot to the register that backs it. Must be called
    /// before lowering any value that reads the slot.
    pub fn bind_slot(&mut self, slot: SlotId, reg: usize) {
        self.slots.insert(slot, reg);
    }

    /// Lower `val`, returning the register holding its result. Registers
    /// are chosen by the lowerer; callers must use the returned register.
    #[must_use = "the returned register is where the value is stored"]
    pub fn lower(&mut self, program: &mut ProgramBuilder, val: ValId) -> Result<usize> {
        let mut stack = vec![Task::Visit(val)];
        while let Some(task) = stack.pop() {
            match task {
                Task::Visit(id) => {
                    if self.lowered.contains_key(&id) {
                        continue;
                    }
                    // A maximal constant subtree in a mixed tree emits
                    // inside its own constant span so it stays eligible
                    // for hoisting into the program prologue, matching
                    // what nested eager translation does for constant
                    // operands. If a span is already open (e.g. the whole
                    // expression is constant), the parent span covers us.
                    let span = if !program.constant_span_is_open() && self.arena.is_constant(id) {
                        Some(program.constant_span_start())
                    } else {
                        None
                    };
                    match self.arena.node(id) {
                        Node::ConstNull
                        | Node::ConstInt(_)
                        | Node::ConstReal(_)
                        | Node::ConstText(_)
                        | Node::ConstBlob(_)
                        | Node::Slot(_)
                        | Node::Opaque(_) => {
                            self.emit_node(program, id, None)?;
                            if let Some(span) = span {
                                program.constant_span_end(span);
                            }
                        }
                        Node::Unary(_, operand) => {
                            let operand = *operand;
                            if let Some(span) = span {
                                stack.push(Task::EndSpan(span));
                            }
                            stack.push(Task::Emit(id));
                            stack.push(Task::Visit(operand));
                        }
                        Node::Binary(_, lhs, rhs) => {
                            let (lhs, rhs) = (*lhs, *rhs);
                            if let Some(span) = span {
                                stack.push(Task::EndSpan(span));
                            }
                            stack.push(Task::Emit(id));
                            // Popped in reverse: lhs is visited (and thus
                            // emitted) before rhs, keeping lowering order
                            // deterministic and source-ordered.
                            stack.push(Task::Visit(rhs));
                            stack.push(Task::Visit(lhs));
                        }
                    }
                }
                Task::Emit(id) => {
                    if self.lowered.contains_key(&id) {
                        continue;
                    }
                    self.emit_node(program, id, None)?;
                }
                Task::EndSpan(span) => {
                    program.constant_span_end(span);
                }
            }
        }
        Ok(*self
            .lowered
            .get(&val)
            .expect("root value must be lowered by the walk"))
    }

    /// Lower `val` into a caller-specified destination register.
    ///
    /// Exists for integration with translation paths that still
    /// pre-allocate target registers; the value's operands are placed in
    /// lowerer-chosen registers, and only the root lands in `dest`. If the
    /// value was already computed in this region, a `Copy` is emitted.
    /// The caller must not clobber `dest` for the lifetime of the region,
    /// since shared uses of `val` will read it.
    pub fn lower_into(
        &mut self,
        program: &mut ProgramBuilder,
        val: ValId,
        dest: usize,
    ) -> Result<()> {
        if let Some(&reg) = self.lowered.get(&val) {
            if reg != dest {
                program.emit_insn(Insn::Copy {
                    src_reg: reg,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            }
            return Ok(());
        }
        // Lower operands first (memoized), then emit the root directly
        // into `dest` so the common case costs no extra Copy.
        match self.arena.node(val) {
            Node::ConstNull
            | Node::ConstInt(_)
            | Node::ConstReal(_)
            | Node::ConstText(_)
            | Node::ConstBlob(_)
            | Node::Slot(_)
            | Node::Opaque(_) => {}
            Node::Unary(_, operand) => {
                let operand = *operand;
                let _ = self.lower(program, operand)?;
            }
            Node::Binary(_, lhs, rhs) => {
                let (lhs, rhs) = (*lhs, *rhs);
                let _ = self.lower(program, lhs)?;
                let _ = self.lower(program, rhs)?;
            }
        }
        self.emit_node(program, val, Some(dest))?;
        Ok(())
    }

    /// Emit the instruction for a single node whose operands are already
    /// lowered, recording the register that now holds its value. With
    /// `dest: None` a fresh register is allocated.
    fn emit_node(
        &mut self,
        program: &mut ProgramBuilder,
        id: ValId,
        dest: Option<usize>,
    ) -> Result<()> {
        let reg = match self.arena.node(id) {
            Node::Opaque(opaque) => {
                let arena = self.arena;
                let expr = arena.opaque_expr(*opaque);
                let emitter = self.opaque_emitter.as_mut().ok_or_else(|| {
                    LimboError::InternalError(
                        "IR lowering: opaque leaf without an opaque emitter".to_string(),
                    )
                })?;
                let dest = dest.unwrap_or_else(|| program.alloc_register());
                emitter(program, expr, dest)?;
                dest
            }
            Node::Slot(slot) => {
                let slot_reg = *self.slots.get(slot).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "IR lowering: slot {slot:?} read before bind_slot"
                    ))
                })?;
                match dest {
                    None => slot_reg,
                    Some(dest) if dest != slot_reg => {
                        program.emit_insn(Insn::Copy {
                            src_reg: slot_reg,
                            dst_reg: dest,
                            extra_amount: 0,
                        });
                        dest
                    }
                    Some(dest) => dest,
                }
            }
            node => {
                let dest = dest.unwrap_or_else(|| program.alloc_register());
                let insn = match node {
                    Node::ConstNull => Insn::Null {
                        dest,
                        dest_end: None,
                    },
                    Node::ConstInt(value) => Insn::Integer {
                        value: *value,
                        dest,
                    },
                    Node::ConstReal(value) => Insn::Real {
                        value: value.value(),
                        dest,
                    },
                    Node::ConstText(value) => Insn::String8 {
                        value: value.clone(),
                        dest,
                    },
                    Node::ConstBlob(value) => Insn::Blob {
                        value: value.clone(),
                        dest,
                    },
                    Node::Unary(op, operand) => {
                        let reg = self.operand_reg(*operand);
                        match op {
                            UnaryOp::Not => Insn::Not { reg, dest },
                            UnaryOp::BitNot => Insn::BitNot { reg, dest },
                        }
                    }
                    Node::Binary(op, lhs, rhs) => {
                        let lhs = self.operand_reg(*lhs);
                        let rhs = self.operand_reg(*rhs);
                        match op {
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
                        }
                    }
                    Node::Slot(_) | Node::Opaque(_) => unreachable!("handled above"),
                };
                program.emit_insn(insn);
                dest
            }
        };
        self.lowered.insert(id, reg);
        Ok(())
    }

    /// Register of an already-lowered operand. Operands are guaranteed
    /// lowered by the post-order walk; a miss is a walk bug.
    fn operand_reg(&self, id: ValId) -> usize {
        *self
            .lowered
            .get(&id)
            .expect("operand must be lowered before its user")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};

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

    fn insns(program: &ProgramBuilder) -> Vec<&Insn> {
        program.insns.iter().map(|(insn, _)| insn).collect()
    }

    #[test]
    fn lowers_arithmetic_tree_in_source_order() {
        let mut arena = ExprArena::new();
        let one = arena.int(1);
        let two = arena.int(2);
        let three = arena.int(3);
        let sum = arena.binary(BinOp::Add, one, two);
        let product = arena.binary(BinOp::Multiply, sum, three);

        let mut program = test_program();
        let result = Lowerer::new(&arena).lower(&mut program, product).unwrap();

        let insns = insns(&program);
        assert_eq!(insns.len(), 5);
        let Insn::Integer { value: 1, dest: r1 } = insns[0] else {
            panic!("expected Integer 1, got {:?}", insns[0]);
        };
        let Insn::Integer { value: 2, dest: r2 } = insns[1] else {
            panic!("expected Integer 2, got {:?}", insns[1]);
        };
        let Insn::Add {
            lhs,
            rhs,
            dest: rsum,
        } = insns[2]
        else {
            panic!("expected Add, got {:?}", insns[2]);
        };
        assert_eq!((lhs, rhs), (r1, r2));
        let Insn::Integer { value: 3, dest: r3 } = insns[3] else {
            panic!("expected Integer 3, got {:?}", insns[3]);
        };
        let Insn::Multiply { lhs, rhs, dest } = insns[4] else {
            panic!("expected Multiply, got {:?}", insns[4]);
        };
        assert_eq!((lhs, rhs), (rsum, r3));
        assert_eq!(dest, &result);
    }

    #[test]
    fn shared_subexpressions_are_computed_once() {
        let mut arena = ExprArena::new();
        let one = arena.int(1);
        let two = arena.int(2);
        let sum = arena.binary(BinOp::Add, one, two);
        // (1 + 2) + (1 + 2): both operands intern to the same node.
        let total = arena.binary(BinOp::Add, sum, sum);

        let mut program = test_program();
        let result = Lowerer::new(&arena).lower(&mut program, total).unwrap();

        let insns = insns(&program);
        // Integer 1, Integer 2, inner Add, outer Add — the inner sum is
        // emitted exactly once and both outer operands read its register.
        assert_eq!(insns.len(), 4);
        let Insn::Add {
            lhs,
            rhs,
            dest: inner,
        } = insns[2]
        else {
            panic!("expected inner Add, got {:?}", insns[2]);
        };
        assert_ne!(lhs, rhs);
        let Insn::Add { lhs, rhs, dest } = insns[3] else {
            panic!("expected outer Add, got {:?}", insns[3]);
        };
        assert_eq!(lhs, inner);
        assert_eq!(rhs, inner);
        assert_eq!(dest, &result);
    }

    #[test]
    fn slot_reads_use_the_bound_register() {
        let mut arena = ExprArena::new();
        let slot = arena.declare_slot();
        let counter = arena.slot(slot);
        let one = arena.int(1);
        let next = arena.binary(BinOp::Add, counter, one);

        let mut program = test_program();
        let slot_reg = program.alloc_register();
        let mut lowerer = Lowerer::new(&arena);
        lowerer.bind_slot(slot, slot_reg);
        let result = lowerer.lower(&mut program, next).unwrap();

        let insns = insns(&program);
        // No instruction is emitted for the slot read itself.
        assert_eq!(insns.len(), 2);
        let Insn::Add { lhs, rhs: _, dest } = insns[1] else {
            panic!("expected Add, got {:?}", insns[1]);
        };
        assert_eq!(lhs, &slot_reg);
        assert_eq!(dest, &result);
    }

    #[test]
    fn unbound_slot_is_an_error() {
        let mut arena = ExprArena::new();
        let slot = arena.declare_slot();
        let read = arena.slot(slot);

        let mut program = test_program();
        let err = Lowerer::new(&arena).lower(&mut program, read);
        assert!(matches!(err, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn lower_into_emits_directly_into_dest() {
        let mut arena = ExprArena::new();
        let one = arena.int(1);
        let two = arena.int(2);
        let sum = arena.binary(BinOp::Add, one, two);

        let mut program = test_program();
        let dest = program.alloc_register();
        Lowerer::new(&arena)
            .lower_into(&mut program, sum, dest)
            .unwrap();

        let insns = insns(&program);
        // Operands go to fresh registers, the root lands in dest, no Copy.
        assert_eq!(insns.len(), 3);
        let Insn::Add { dest: add_dest, .. } = insns[2] else {
            panic!("expected Add, got {:?}", insns[2]);
        };
        assert_eq!(add_dest, &dest);
    }

    #[test]
    fn lower_into_copies_already_computed_values() {
        let mut arena = ExprArena::new();
        let forty_two = arena.int(42);

        let mut program = test_program();
        let mut lowerer = Lowerer::new(&arena);
        let first = lowerer.lower(&mut program, forty_two).unwrap();
        let dest = program.alloc_register();
        lowerer.lower_into(&mut program, forty_two, dest).unwrap();

        let insns = insns(&program);
        assert_eq!(insns.len(), 2);
        let Insn::Copy {
            src_reg, dst_reg, ..
        } = insns[1]
        else {
            panic!("expected Copy, got {:?}", insns[1]);
        };
        assert_eq!((src_reg, dst_reg), (&first, &dest));
    }

    #[test]
    fn unary_ops_lower() {
        let mut arena = ExprArena::new();
        let one = arena.int(1);
        let not = arena.unary(UnaryOp::Not, one);
        let bitnot = arena.unary(UnaryOp::BitNot, not);

        let mut program = test_program();
        let result = Lowerer::new(&arena).lower(&mut program, bitnot).unwrap();

        let insns = insns(&program);
        assert_eq!(insns.len(), 3);
        let Insn::Not {
            reg: _,
            dest: not_dest,
        } = insns[1]
        else {
            panic!("expected Not, got {:?}", insns[1]);
        };
        let Insn::BitNot { reg, dest } = insns[2] else {
            panic!("expected BitNot, got {:?}", insns[2]);
        };
        assert_eq!(reg, not_dest);
        assert_eq!(dest, &result);
    }

    #[test]
    fn deep_trees_lower_without_recursion() {
        let mut arena = ExprArena::new();
        let mut acc = arena.int(0);
        for i in 1..=50_000i64 {
            let leaf = arena.int(i);
            acc = arena.binary(BinOp::Add, acc, leaf);
        }

        let mut program = test_program();
        let result = Lowerer::new(&arena).lower(&mut program, acc);
        assert!(result.is_ok());
        // 50_001 Integer loads + 50_000 Adds.
        assert_eq!(program.insns.len(), 100_001);
    }

    #[test]
    fn lowering_is_deterministic() {
        let build = || {
            let mut arena = ExprArena::new();
            let a = arena.text("a");
            let b = arena.text("b");
            let ab = arena.binary(BinOp::Concat, a, b);
            let three = arena.int(3);
            let root = arena.binary(BinOp::Concat, ab, three);
            (arena, root)
        };
        let run = || {
            let (arena, root) = build();
            let mut program = test_program();
            Lowerer::new(&arena).lower(&mut program, root).unwrap();
            program
                .insns
                .iter()
                .map(|(insn, _)| format!("{insn:?}"))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }
}
