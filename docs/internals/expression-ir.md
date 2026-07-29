# Declarative expression IR

`core/translate/expr/ir.rs` introduces a value IR between the AST and bytecode
emission for expression translation. This document explains why it exists, how
it preserves bytecode compatibility, and the longer-term compiler architecture
it is the first step of.

## Why

Historically, expression translation was eager: emission order *was* program
order, and every decision had to be made at the moment an instruction was
appended to the `ProgramBuilder`. That coupling is the root cause of several
mechanisms in `core/translate/`:

- Callers must pre-allocate target registers before knowing what an expression
  compiles to, forcing `Copy` instructions and the expression-register cache.
- Constant hoisting operates on the emitted instruction stream ("constant
  spans"), with deopt escape hatches (`NoConstantOptReason`) for cases where a
  parent guessed wrong mid-emission.
- Phase ordering (init/open/body/close in `main_loop/`) is choreographed by
  call order and "did someone already emit X?" flags.
- Registers are allocated monotonically and never freed; there is no liveness.

## What the IR is

`ExprIr` is an arena (`Vec<VNode>`, `VId` indices) of `VExpr` nodes. Building
the arena is **pure**: no registers are allocated and no instructions are
emitted, so nodes can be constructed, chained and inspected freely — the
"return a description, emit later" model, analogous to Rust's lazy `Iterator`
adaptors. A separate deterministic lowering pass walks the tree and emits
instructions into the `ProgramBuilder`.

Two properties keep this safe to land incrementally:

1. **Bytecode identity.** For the supported subset, lowering mirrors the eager
   path exactly — same instruction order, same register allocation pattern,
   same collation/constant-span state transitions — validated by the
   equivalence tests in `ir.rs`, which compile a statement corpus with the IR
   path toggled on and off and assert identical `EXPLAIN` listings. Identical
   bytecode means execution behavior and performance cannot regress.
2. **Strangler pattern.** Unsupported subexpression shapes become `Opaque`
   leaves that lower by delegating to the eager `translate_expr`. Coverage
   grows node kind by node kind without a flag day. Supported today: literals,
   parenthesized single expressions, unary operators, scalar binary operators
   (including the shared-operand form), `IS [NOT] NULL`, and both `CASE`
   forms.

Each node carries a back-pointer to its AST node. That is what lets the
lowering pass replicate the per-node translation preamble (constant
classification, expression-register cache, expression-index satisfaction — see
`open_expr_constant_span`, `try_emit_cached_expr_reg`,
`try_emit_expression_index_lookup` in `translator.rs`) and share the existing
emission helpers (`emit_literal`, `emit_binary_insn`, collation merging), so
there is a single source of truth for emission semantics.

## The ladder

The IR is rung one of a deliberate progression from "eager SQL translator" to
a conventional compiler pipeline. Each rung pays for itself and none requires
the next:

1. **Value IR + deterministic lowering** (this change). The traversal is
   IR-driven; emission is a pass. New passes can now be written against the
   arena instead of against the emitted instruction stream.
2. **IR-level passes.** Hash-consing the arena gives common-subexpression
   elimination (subsuming the expression-register cache); constant-ness as a
   node property turns hoisting into scheduling (subsuming constant spans and
   their deopt reasons); late register assignment with liveness enables
   register reuse. Each of these deliberately changes bytecode, so each must
   land behind differential validation (conformance suite, differential
   fuzzer, `EXPLAIN` diffing) rather than the identity test.
3. **Operator-level lowering and block-argument SSA.** Plan operators become
   produce/consume compositions emitting into a CFG of basic blocks with block
   parameters (Cranelift-style, no phi nodes). Loop-carried state — aggregate
   accumulators, LEFT JOIN match flags, coroutine yields — becomes block
   parameters instead of hand-managed registers. VDBE-specific constraints to
   design in from the start: contiguous-register groups (function arguments,
   `MakeRecord`, `ResultRow`) as tuple pseudo-values the register allocator
   must place contiguously; cursors as pinned stateful effects outside SSA;
   deterministic, source-ordered lowering so bytecode remains comparable with
   SQLite's `EXPLAIN` output.

## Invariants for contributors

- Decomposed lowering must stay instruction-identical to the eager path until
  rung 2 lands. If you add a node kind, extend the equivalence corpus in
  `ir.rs` and keep the eager arm as the single semantic reference (share its
  emission helpers; do not fork emission logic).
- If a shape cannot be mirrored exactly, leave it `Opaque` — an `Opaque` leaf
  is always correct.
- `ExprIr::build` must stay cheap: it runs on every composite expression
  during prepare. Anything super-linear in expression size belongs in an
  explicit opt-in pass, not in build.
- Benchmarks guarding prepare-time overhead:
  `cargo bench --bench expr_prepare_benchmark` and
  `cargo bench --bench prepare_params_benchmark`.
