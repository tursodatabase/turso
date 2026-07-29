//! Declarative IR for bytecode compilation.
//!
//! This module is the frontend of the ongoing migration away from eager
//! `ProgramBuilder::emit_insn` calls: translation builds *values* (handles
//! into an [`ExprArena`]) that compose like `Iterator` combinators, and a
//! separate lowering pass ([`Lowerer`]) materializes instructions into a
//! [`crate::vdbe::builder::ProgramBuilder`], assigning registers as an
//! emission detail rather than a frontend responsibility.
//!
//! See `docs/internals/declarative-bytecode-compiler.md` for the full
//! design and the phased migration plan. The invariants that matter when
//! extending this module:
//!
//! - **Value vs Slot.** [`ValId`] nodes are immutable values: interned
//!   (hash-consed), safe to share and compute once. A [`SlotId`] is an
//!   explicitly declared mutable register cell (aggregate accumulators,
//!   coroutine yield slots, ...) whose register binding the frontend
//!   controls via [`Lowerer::bind_slot`]. Never model a mutable register
//!   as a plain value.
//! - **Region purity.** Values are only pure between effectful statements
//!   (cursor movement, slot writes, control transfer). One [`Lowerer`]
//!   instance is one region: it memoizes node → register, so a fresh
//!   `Lowerer` must be created after any effect that could invalidate a
//!   previously computed value.
//! - **No recursion.** Expression trees can be arbitrarily deep; arena
//!   walks use explicit stacks.
//! - **Determinism.** Lowering visits operands in source order and
//!   allocates registers monotonically; identical input graphs must
//!   produce identical instruction sequences.

mod arena;
mod build;
mod lower;

pub(crate) use arena::{BinOp, ExprArena, Node, OpaqueId, SlotId, UnaryOp, ValId};
pub(crate) use build::{try_build_value, BuildCtx, Built};
pub(crate) use lower::{Lowerer, OpaqueEmitter};
