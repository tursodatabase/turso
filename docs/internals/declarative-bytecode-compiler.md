# Declarative Bytecode Compiler (IR) — Design & Plan of Attack

Status: **Phase 1 in progress** (see checklist at the bottom — keep it updated).
Branch: `claude/declarative-bytecode-compiler-3xpd9p`.

This document is the source of truth for the ongoing migration of
`core/translate/` from eager bytecode emission to a declarative,
compiler-style pipeline. It is written so that work can continue from a
clean context: read this document, check the phase checklist, continue.

## Motivation

Today the whole of `core/translate/` (~52k LOC) is built around one
primitive: `ProgramBuilder::emit_insn` appends an instruction immediately.
That eagerness entangles three concerns a compiler normally separates:

1. **What to compute** — the recursion in `translate_expr`
   (`core/translate/expr/translator.rs`)
2. **Where the result lives** — callers pre-allocate `target_register`
   and thread it down every call
3. **Where control goes** — `ConditionMetadata { jump_target_when_true /
   _false / _null }` labels threaded *into* expression translation

Several of the gnarliest mechanisms in the codebase exist only to
compensate for eager emission:

- **Constant spans** (`constant_span_start` / `constant_span_end_all` /
  `constant_spans_invalidate_after` in `core/vdbe/builder.rs`) are a shadow
  data structure over the instruction stream so constants can be hoisted
  *after the fact*. `translate_expr_no_constant_opt` and its
  `NoConstantOptReason` enum (`RegisterReuse`, `CustomTypeEncode`,
  `InListEphemeral`) catalog the ways retroactive hoisting goes wrong.
- **Register-reuse hazards** — because callers pick destination registers
  and emission is immediate, two computations can silently stomp each
  other (the `COALESCE(1, t.x, NULL)` bug class documented in
  `translator.rs`).
- **`expr_to_reg_cache`** in `Resolver` is hand-rolled CSE, maintained
  manually because there is no IR to run CSE on.

The goal: translation *returns values* (handles into an arena), composition
builds a graph — like `Iterator` combinators build a description — and a
final lowering pass materializes `Vec<Insn>`, doing register allocation,
label creation, and hoisting as emission details.

## Decisions already made

- **SQLite bytecode parity is NOT a goal.** Different-but-correct register
  numbering and instruction order are acceptable. Correctness is validated
  by *results* (sqltest conformance corpus, `scripts/diff.sh`, differential
  fuzzer), not by EXPLAIN diffs. EXPLAIN comparison remains a debugging
  aid only. (Explicit user decision, 2026-07-29.)
- **`ProgramBuilder` stays as the backend assembler.** Lowering targets it
  (`emit_insn`, `alloc_register`, label allocation/resolution). The IR is a
  new frontend layer, not a replacement of the assembler.
- **Incremental migration with an escape hatch.** Old eager code and new
  declarative code must be embeddable in each other during the transition.
- **`Value` vs `Slot` split from day one.** See below — this is the
  decision that is expensive to retrofit.
- **Migration order: expressions → conditions → projections → main loop.**

## Architecture: three layers, adopted bottom-up

### Layer 1 — Value IR (expressions)

Arena-based dataflow representation in `core/translate/ir/`. Nothing is
emitted at build time; nodes are data:

- `ValId` — index into `ExprArena`; the handle that composes.
- Node kinds (grow over time): constants (`Null`, `Int`, `Real`, `Text`,
  `Blob`), unary ops, binary ops (arithmetic, bitwise, concat, and later
  comparisons with affinity + collation baked into the node), function
  calls, column reads, slot reads.
- **Hash-consing**: pure nodes are interned, so identical subexpressions
  share a `ValId`. This is CSE by construction and replaces
  `expr_to_reg_cache` over time.
- Building `a + b` is: build `a` → `ValId`, build `b` → `ValId`,
  `arena.binary(Add, a, b)` → `ValId`. No `ProgramBuilder`, no
  `target_register`.

Lowering (`ir/lower.rs`): deterministic post-order walk with an explicit
stack (no recursion — expression trees can be deep), per-lowering
memoization of node → register so shared nodes are computed once,
registers assigned by the lowering pass via `ProgramBuilder`.

What this buys at lowering time:

- **Constant hoisting becomes structural**: a node whose transitive inputs
  are all constants can be hoisted, by construction. The constant-span
  machinery and its deopt reasons eventually disappear.
- **CSE replaces the expr-register cache.**
- **Register clobbering becomes unrepresentable**: nobody picks
  destination registers mid-tree; the allocator does.

### The `Value` / `Slot` split (critical)

VDBE registers are mutable and parts of the system depend on it:
aggregate accumulators, coroutine yield slots, in-place custom-type encode
(the double-encoding hazard documented in `translator.rs`), IN-list
ephemeral loading. Pure SSA cannot express these. Therefore the IR has two
distinct notions:

- **`Value`** — immutable, allocator-owned, freely CSE-able / hoistable.
- **`Slot`** (`SlotId`) — an explicitly declared mutable cell with a
  program-defined register binding. Slot *reads* are values, but they are
  only coherent within a region (below); slots are never hoisted, and a
  slot's register is bound explicitly by the frontend
  (`Lowerer::bind_slot`).

Most historical bugs come from conflating the two; the type system makes
the distinction mandatory.

### Effect regions (purity rule)

`Column { cursor, idx }` and slot reads are not globally pure — cursor
position and slot contents change over time. Rather than full effect-token
dependency edges, we use a pragmatic invariant:

> Values are pure *within a region* — the code between two effectful
> statements (cursor movement, slot write, control transfer). Lowering may
> memoize/CSE only within a region, plus a whole-program constant region
> for hoisting.

Concretely: one `Lowerer` instance = one region. Frontends must create a
fresh `Lowerer` (or call an explicit region-reset) after any effectful
statement. This is exactly the invariant the constant-span code tries to
enforce today, but structurally guaranteed.

### Layer 2 — Structured control (statements, not labels)

Control flow becomes structure:

- `Stmt::If { cond, then_, else_ }`, `Stmt::Loop { body }` with structured
  `Break`/`Continue`, `Stmt::Coroutine { .. }`.
- Three-valued logic is first-class: `Branch3 { val, when_true,
  when_false, when_null }` replaces `ConditionMetadata`. The true/false/
  null distinction is real SQL semantics and stays; the *labels* become
  something lowering invents and resolves internally.
- **Escape hatch**: `Stmt::Raw(callback: FnOnce(&mut ProgramBuilder, ..))`
  so old eager code embeds inside new declarative code and vice versa.
  This is the single most important migration feature.

`allocate_label` / `preassign_label_to_next_insn` / `resolve_labels`
become private to the backend once migration completes.

### Layer 3 — Row-stream operators (the Iterator, literally)

`core/translate/plan.rs` (`SelectPlan` / `Operation`) is already a
declarative operator description; the problem is `emitter/` and
`main_loop/` walk it while eagerly interleaving open/seek/condition/body/
close emission. The declarative version makes each operator a node with
lowering hooks — `open()`, `row()` (produces `ValId`s for the current
row's columns), `close()` — and compiling a pipeline nests them:
`Project(Filter(Scan(t)))` lowers to the same Rewind/Next loop emitted
today, but composed rather than hand-sequenced. Join reordering, pushdown,
and subquery decorrelation become tree rewrites. Biggest payoff, biggest
lift — goes last.

## Known hard constraints

1. **Contiguous register ranges**: `ResultRow`, `MakeRecord`, `Function`,
   `IdxInsert` need contiguous registers. The allocator needs a "group"
   concept — allocate N adjacent registers, members pinned relative to
   each other (like register pairs on old ISAs). Naive linear-scan is out.
2. **Determinism**: lowering must be fully deterministic (visit in source
   order, allocate monotonically) so compiled programs are stable across
   runs and diffs between compiler versions are reviewable.
3. **Affinity and collation** ride along on comparison/concat nodes — they
   are part of the operation, not context. `CmpInsFlags`, collation
   resolution (`core/translate/collate.rs`), and `comparison_affinity`
   must be captured into node payloads when comparisons are added.
4. **Deep trees**: no unbounded recursion in lowering; explicit stacks.
   (Frontend `translate_expr` uses `#[turso_macros::trace_stack]`; the IR
   walker avoids the issue entirely.)

## Validation strategy

- Unit tests in `core/translate/ir/` assert exact emitted `Insn` sequences
  for small graphs (the `insns` field on `ProgramBuilder` is public).
- Every integration step must keep the full existing suites green:
  `cargo test`, `make test`,
  `make -C sqlite/conformance run-rust ARGS='--snapshot-filter __never__'`.
- Results-level differential validation: `scripts/diff.sh "SQL"` and the
  differential fuzzer (see `differential-fuzzer` agent skill) after each
  integration phase.
- Bytecode churn in EXPLAIN-based snapshots is acceptable; re-baseline
  deliberately and review the diff for pathologies (dead code, register
  explosions), not for equality.

## Plan of attack (keep statuses updated)

### Phase 1 — Value IR skeleton + first integration

- [x] 1a. `core/translate/ir/` module: `ExprArena`, `ValId`, `SlotId`,
      const/unary/binary nodes, hash-consing, iterative lowering to
      `ProgramBuilder` with node→register memoization, `lower_into` for
      caller-specified destination registers, unit tests.
      (Landed in this phase's first commit.)
- [x] 1b. First real caller: `translate_expr`'s `Expr::Binary` value arm
      tries `ir::try_build_value` (literal-only trees over
      arithmetic/bitwise/concat ops, incl. parenthesization and unary
      folds mirroring the eager path) and lowers via `Lowerer::lower_into`
      before falling back to `binary_expr_shared`. Constant hoisting rides
      the existing constant-span mechanism at the integration boundary
      until Layer 2 owns hoisting; collation post-state mirrors the eager
      path. Standalone `Expr::Literal` deliberately stays on the
      `emit_literal` fast path — a per-literal arena buys nothing until
      arenas are shared across a statement. Validated: core unit tests,
      conformance corpus, EXPLAIN shows IR-lowered constants hoisting
      correctly out of scan loops.
- [x] 1c. Column and rowid reads join the IR as **opaque leaves**
      (`Node::Opaque`): the arena stores the leaf AST expression and
      lowering delegates it back to eager `translate_expr` via
      `Lowerer::with_opaque_emitter`, so cursor/index/covering/virtual/
      custom-type resolution stays in one place while the IR owns the
      tree around it. Structurally equal leaves dedup (repeated column
      reads share a register per region). `BuildCtx` carries
      referenced_tables + resolver; the builder computes the collation
      post-state statically (same merge rules as `binary_expr_shared`)
      and the hook restores it after lowering. Maximal constant subtrees
      of mixed trees are wrapped in constant spans during lowering so
      hoisting still works (verified via EXPLAIN). Safety gates — the IR
      path refuses: expression→register cache enabled (GROUP BY-style
      contexts where re-reading columns is wrong), expression indexes in
      play, custom-typed columns (operator overloads), array operands of
      `||` (ArrayConcat), SELF_TABLE placeholders. Function calls did NOT
      make this slice — they need contiguous argument blocks, moved to 1d.
- [ ] 1d. Function calls as nodes. Needs the contiguous-register-group
      concept: `Insn::Function` takes its args in adjacent registers, so
      lowering a FnCall must allocate a block and `lower_into` each arg
      slot (mirroring the eager shape). Start with pure scalar functions
      on the generic `Function` path; leave specialized emission
      (datetime folds, JSON fast paths, aggregates) to the eager path.
- [ ] 1e. Comparisons with affinity + collation payloads
      (`CmpInsFlags`, `comparison_affinity`), value-position only
      (`Insn::Eq/Ne/Lt/…` value mode / `Bool` results).

### Phase 2 — Structured control

- [ ] 2a. `Block` / `Stmt` structures with `Stmt::Raw` escape hatch;
      lowering creates and resolves labels internally.
- [ ] 2b. `Branch3` replaces `ConditionMetadata` for
      `translate_condition_expr` callers, starting with WHERE-clause
      terminals in `main_loop/conditions.rs`.
- [ ] 2c. Short-circuit expressions (AND/OR/CASE/COALESCE/IN/BETWEEN) as
      structured nodes; delete `translate_expr_no_constant_opt` deopts
      that become unrepresentable.
- [ ] 2d. Constant hoisting moves into IR lowering; delete constant-span
      machinery from `ProgramBuilder`.

### Phase 3 — Register allocator

- [ ] 3a. Group-aware allocator (contiguous ranges) with liveness from the
      IR; replace monotonic `alloc_register` for IR-lowered code.
- [ ] 3b. Slot lifetime audit: aggregates, coroutines, custom-type encode
      paths expressed as explicit slots.

### Phase 4 — Row-stream operators

- [ ] 4a. Operator trait (`open`/`row`/`close` lowering hooks) over
      `SelectPlan`; port simple `SELECT ... FROM t WHERE ...` pipeline.
- [ ] 4b. Joins, aggregation, ORDER BY/sorters, coroutine-based subqueries.
- [ ] 4c. Delete `main_loop/` eager sequencing once operator coverage is
      complete.

## Continuation notes for a fresh context

- Read this file, then `core/translate/ir/` (module docs are load-bearing).
- The IR is only as integrated as the checklist says — do not assume more.
- When adding node kinds: keep `Node` `Eq + Hash` (interning), keep
  lowering iterative, keep the `Value`/`Slot` distinction, capture
  affinity/collation in node payloads rather than reading ambient state at
  lowering time.
- Never break `cargo clippy --workspace --all-features --all-targets --
  --deny=warnings` or `cargo fmt`.
