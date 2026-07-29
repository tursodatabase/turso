//! Composable compiler values.
//!
//! A [`Compiler<T>`] is a *description* of compilation work: constructing
//! one emits nothing. Combinators chain descriptions the way parser
//! combinators chain parsers; only [`Compiler::run`] materializes the
//! description into SSA IR (and even then no bytecode exists until the
//! verified IR is emitted by the backend).
//!
//! ```ignore
//! condition
//!     .branch(compile_then(), compile_else())
//!     .map(transform_result)
//!     .then(compile_next_step())
//! ```
//!
//! The payoff is locality: a compiler function is understood from what it
//! consumes and returns. It does not coordinate register numbers, labels,
//! or instruction positions with distant code — those are invented by the
//! backend after the whole computation is described.

use crate::Result;

use super::ir::{FuncBuilder, JumpTarget, ValueId};

/// The deferred build step at the heart of a [`Compiler`].
type BuildFn<'a, T> = Box<dyn FnOnce(&mut FuncBuilder) -> Result<T> + 'a>;

/// A deferred compilation step producing a `T` (usually a [`ValueId`])
/// when run against a [`FuncBuilder`].
pub struct Compiler<'a, T> {
    build: BuildFn<'a, T>,
}

impl<'a, T: 'a> Compiler<'a, T> {
    /// Wrap a raw build step. This is the escape hatch for constructing
    /// primitives; prefer the typed constructors and combinators.
    pub fn build_with(f: impl FnOnce(&mut FuncBuilder) -> Result<T> + 'a) -> Self {
        Self { build: Box::new(f) }
    }

    /// A compiler that does no work and yields `value`.
    pub fn pure(value: T) -> Self {
        Self::build_with(move |_| Ok(value))
    }

    /// Materialize the description into IR being built by `builder`.
    pub fn run(self, builder: &mut FuncBuilder) -> Result<T> {
        (self.build)(builder)
    }

    /// Transform the result without touching the IR.
    pub fn map<U: 'a>(self, f: impl FnOnce(T) -> U + 'a) -> Compiler<'a, U> {
        Compiler::build_with(move |builder| Ok(f(self.run(builder)?)))
    }

    /// Transform the result with access to the builder — the primitive
    /// for appending instructions that consume earlier results.
    pub fn map_with<U: 'a>(
        self,
        f: impl FnOnce(&mut FuncBuilder, T) -> Result<U> + 'a,
    ) -> Compiler<'a, U> {
        Compiler::build_with(move |builder| {
            let value = self.run(builder)?;
            f(builder, value)
        })
    }

    /// Sequence two compilers, yielding both results.
    pub fn then<U: 'a>(self, next: Compiler<'a, U>) -> Compiler<'a, (T, U)> {
        Compiler::build_with(move |builder| {
            let first = self.run(builder)?;
            let second = next.run(builder)?;
            Ok((first, second))
        })
    }

    /// Monadic sequencing: the next compiler is chosen from this one's
    /// result. This is what makes recursive compiler construction work —
    /// the continuation can itself assemble arbitrary sub-compilers.
    pub fn and_then<U: 'a>(self, f: impl FnOnce(T) -> Compiler<'a, U> + 'a) -> Compiler<'a, U> {
        Compiler::build_with(move |builder| {
            let value = self.run(builder)?;
            f(value).run(builder)
        })
    }
}

impl<'a> Compiler<'a, ValueId> {
    /// Three-valued conditional: `self` is a SQL boolean; truthy runs
    /// `when_true`, falsy runs `when_false`, NULL runs `when_null`. Each
    /// arm produces a value; control joins in a fresh block whose single
    /// block parameter carries the chosen result — no register is shared
    /// by convention between the arms.
    pub fn branch3(
        self,
        when_true: Compiler<'a, ValueId>,
        when_false: Compiler<'a, ValueId>,
        when_null: Compiler<'a, ValueId>,
    ) -> Compiler<'a, ValueId> {
        Compiler::build_with(move |builder| {
            let cond = self.run(builder)?;
            let true_block = builder.create_block();
            let false_block = builder.create_block();
            let null_block = builder.create_block();
            let join = builder.create_block();
            let result = builder.add_block_param(join);
            builder.branch(
                cond,
                JumpTarget::new(true_block, Vec::new()),
                JumpTarget::new(false_block, Vec::new()),
                JumpTarget::new(null_block, Vec::new()),
            );
            // Each arm may create blocks of its own; the jump to the join
            // is emitted from whatever block the arm ends in.
            builder.switch_to(true_block);
            let true_value = when_true.run(builder)?;
            builder.jump(join, vec![true_value]);
            builder.switch_to(false_block);
            let false_value = when_false.run(builder)?;
            builder.jump(join, vec![false_value]);
            builder.switch_to(null_block);
            let null_value = when_null.run(builder)?;
            builder.jump(join, vec![null_value]);
            builder.switch_to(join);
            Ok(result)
        })
    }

    /// Two-armed conditional; NULL takes the false arm (the common SQL
    /// value-position behavior, e.g. CASE with no matching WHEN). The
    /// false arm's *computation* is shared: the NULL edge jumps to the
    /// same block, it is not duplicated.
    pub fn branch(
        self,
        when_true: Compiler<'a, ValueId>,
        when_false: Compiler<'a, ValueId>,
    ) -> Compiler<'a, ValueId> {
        Compiler::build_with(move |builder| {
            let cond = self.run(builder)?;
            let true_block = builder.create_block();
            let false_block = builder.create_block();
            let join = builder.create_block();
            let result = builder.add_block_param(join);
            builder.branch(
                cond,
                JumpTarget::new(true_block, Vec::new()),
                JumpTarget::new(false_block, Vec::new()),
                JumpTarget::new(false_block, Vec::new()),
            );
            builder.switch_to(true_block);
            let true_value = when_true.run(builder)?;
            builder.jump(join, vec![true_value]);
            builder.switch_to(false_block);
            let false_value = when_false.run(builder)?;
            builder.jump(join, vec![false_value]);
            builder.switch_to(join);
            Ok(result)
        })
    }
}

/// A value imported from a physical register owned by surrounding eager
/// code. See [`super::ir::Inst::External`].
pub fn external(reg: usize) -> Compiler<'static, ValueId> {
    Compiler::build_with(move |builder| Ok(builder.external(reg)))
}

pub fn null() -> Compiler<'static, ValueId> {
    Compiler::build_with(|builder| Ok(builder.null()))
}

pub fn int(value: i64) -> Compiler<'static, ValueId> {
    Compiler::build_with(move |builder| Ok(builder.int(value)))
}

pub fn real(value: f64) -> Compiler<'static, ValueId> {
    Compiler::build_with(move |builder| Ok(builder.real(value)))
}

pub fn text(value: String) -> Compiler<'static, ValueId> {
    Compiler::build_with(move |builder| Ok(builder.text(value)))
}

pub fn blob(value: crate::ValueBlob) -> Compiler<'static, ValueId> {
    Compiler::build_with(move |builder| Ok(builder.blob(value)))
}
