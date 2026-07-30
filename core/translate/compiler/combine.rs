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

use super::ir::{BlockId, FuncBuilder, JumpTarget, ValueId};

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

/// The three continuations of a predicate: where control goes when the
/// condition is true, false, or NULL. Composition rewires these blocks
/// the way the eager path threads `ConditionMetadata` labels — but
/// symbolically, with the backend inventing every physical label.
#[derive(Debug, Clone, Copy)]
pub struct CondTargets {
    pub if_true: BlockId,
    pub if_false: BlockId,
    pub if_null: BlockId,
}

/// The deferred build step at the heart of a [`Predicate`].
type PredicateFn<'a> = Box<dyn FnOnce(&mut FuncBuilder, CondTargets) -> Result<()> + 'a>;

/// A deferred predicate: when run, it appends control flow to the
/// function under construction that leaves for one of the three targets.
/// The block the builder is positioned in when `run` is called is the
/// predicate's entry; every path out of the predicate ends in a
/// terminator (there is no fallthrough).
pub struct Predicate<'a> {
    build: PredicateFn<'a>,
}

impl<'a> Predicate<'a> {
    pub fn build_with(f: impl FnOnce(&mut FuncBuilder, CondTargets) -> Result<()> + 'a) -> Self {
        Self { build: Box::new(f) }
    }

    pub fn run(self, builder: &mut FuncBuilder, targets: CondTargets) -> Result<()> {
        (self.build)(builder, targets)
    }

    /// Logical AND: the left predicate's true edge continues into the
    /// right predicate; false short-circuits to the outer target. A NULL
    /// left side short-circuits only when the outer NULL and false
    /// continuations coincide; otherwise it continues into the right
    /// predicate — exactly the eager behavior, where the left terminal's
    /// jump_if_null flag is set only when the NULL label equals the
    /// false label it jumps to.
    pub fn and(self, rhs: Predicate<'a>) -> Predicate<'a> {
        Predicate::build_with(move |builder, targets| {
            let mid = builder.create_block();
            let lhs_null = if targets.if_null == targets.if_false {
                targets.if_false
            } else {
                mid
            };
            self.run(
                builder,
                CondTargets {
                    if_true: mid,
                    if_null: lhs_null,
                    ..targets
                },
            )?;
            builder.switch_to(mid);
            rhs.run(builder, targets)
        })
    }

    /// Logical OR: the left predicate's false AND NULL edges continue
    /// into the right predicate (a NULL left side must still evaluate the
    /// right side); true short-circuits to the outer target. Mirrors the
    /// eager OR label threading.
    pub fn or(self, rhs: Predicate<'a>) -> Predicate<'a> {
        Predicate::build_with(move |builder, targets| {
            let mid = builder.create_block();
            self.run(
                builder,
                CondTargets {
                    if_false: mid,
                    if_null: mid,
                    ..targets
                },
            )?;
            builder.switch_to(mid);
            rhs.run(builder, targets)
        })
    }

    /// A truthiness test over a computed value: truthy takes the true
    /// edge, falsy and NULL take the false edge. NULL joining false —
    /// regardless of the NULL target — matches the eager
    /// `emit_cond_jump` in both of its jump directions.
    pub fn from_bool(value: Compiler<'a, ValueId>) -> Predicate<'a> {
        Predicate::build_with(move |builder, targets| {
            let cond = value.run(builder)?;
            builder.branch(
                cond,
                JumpTarget::new(targets.if_true, Vec::new()),
                JumpTarget::new(targets.if_false, Vec::new()),
                JumpTarget::new(targets.if_false, Vec::new()),
            );
            Ok(())
        })
    }
}

/// Describe a full scan over an externally-opened cursor: rewind, run
/// `body` once per row, advance. The eager Rewind/Next loop shape,
/// composed instead of hand-sequenced.
///
/// `body` runs with the builder positioned in the loop-body block and
/// must leave its final block unterminated (the loop wires it to the
/// latch). The builder is left positioned in the loop's continuation
/// block. Loop-carried values (block parameters on the body) come with
/// the aggregate work; today's bodies are effect-only.
pub fn scan_loop<'a>(
    cursor: usize,
    body: impl FnOnce(&mut FuncBuilder) -> Result<()> + 'a,
) -> Compiler<'a, ()> {
    Compiler::build_with(move |builder| {
        let body_block = builder.create_block();
        let latch = builder.create_block();
        let done = builder.create_block();
        builder.rewind(
            cursor,
            JumpTarget::new(done, Vec::new()),
            JumpTarget::new(body_block, Vec::new()),
        );
        builder.switch_to(body_block);
        body(builder)?;
        builder.jump(latch, Vec::new());
        builder.switch_to(latch);
        builder.next_row(
            cursor,
            JumpTarget::new(body_block, Vec::new()),
            JumpTarget::new(done, Vec::new()),
        );
        builder.switch_to(done);
        Ok(())
    })
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
