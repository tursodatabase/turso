# Composable Compiler IR — Design & Plan of Attack

Status: **Phase 1 in progress** (see checklist at the bottom — keep it updated).
Branch: `claude/composable-compiler-ir-4zkm3h`.
Code: `core/translate/compiler/`.

This document is the source of truth for the migration of
`core/translate/` from eager bytecode emission to a composable,
verify-then-emit pipeline. It is written so work can continue from a clean
context: read this file, read `core/translate/compiler/mod.rs`, check the
checklist, continue.

Related earlier explorations (unmerged, superseded by this branch):
`claude/declarative-bytecode-compiler-3xpd9p` (arena value-DAG IR with
opaque column leaves; its design doc catalogs SQLite-specific hazards
worth rereading) and `claude/declarative-bytecode-compiler-es1ta5`.

## Goal

Translation code returns composable **descriptions** of work instead of
immediately mutating `ProgramBuilder`:

```text
compile_expr(expr)   -> Compiler<Value>
compile_filter(expr) -> Compiler<Predicate>
compile_select(plan) -> Compiler<RowStream>
```

Descriptions chain with `map` / `map_with` / `then` / `and_then` /
`branch` (later `loop_over`). Constructing an expression like

```text
condition
    .branch(compile_then(), compile_else())
    .map(transform_result)
    .then(compile_next_step())
```

emits nothing. Only after the complete operation is described do we:

1. Build symbolic SSA IR (run the description).
2. Verify it (and later: optimize/transform it).
3. Allocate registers, cursors, labels, temporaries.
4. Emit the final VDBE program.

```text
SQL AST/plan -> Compiler values -> SSA CFG IR -> verify/optimize
             -> register/cursor/label allocation -> VDBE program
```

The success criterion: a compiler function is understood locally from what
it consumes and returns. It never coordinates register numbers, labels, or
instruction positions with distant code. VDBE remains the target; eager
`ProgramBuilder` mutation stops being the programming model.

## Architecture (three layers)

### SQL frontend — `compiler/expr.rs` (grows per construct)

Resolves SQLite semantics (names, affinity, collation, NULL behavior) and
returns `Option<Compiler<ValueId>>`. `None` means "not representable yet";
the caller stays on the eager path. This is the gradual-migration escape
hatch, and it is how coverage grows one construct at a time without ever
breaking the suite.

### Compiler IR — `compiler/ir.rs`, `compiler/verify.rs`, `compiler/combine.rs`

- `Compiler<'a, T>` (`combine.rs`): a boxed `FnOnce(&mut FuncBuilder) ->
  Result<T>`. Combinators: `pure`, `map`, `map_with`, `then`, `and_then`,
  `branch`, `branch3`. Constructing/combining runs nothing.
- SSA CFG (`ir.rs`): `Function` = blocks + value definition sites.
  Blocks have **parameters** (SSA-with-block-arguments, as in Cranelift/
  MLIR); edges (`JumpTarget`) carry arguments. Terminators: `Jump`,
  three-valued `Branch` (truthy/falsy/NULL — replaces the eager
  `ConditionMetadata` label triple), `Ret`.
- Values are immutable and symbolic. Constants and `External` inputs
  intern into the entry block: identical leaves share a definition
  (CSE-by-construction for leaves) and dominate all uses.
- `Inst::External { reg }` imports a value living in a register owned by
  surrounding eager code — the bridge that lets IR islands consume eager
  results.
- Verifier (`verify.rs`): entry has no params; no edges into entry (loop
  headers must be their own blocks); every reachable block terminated;
  edge arity matches target params; every use dominated by its def
  (dominator tree via Cooper–Harvey–Kennedy on RPO). Malformed IR fails
  *before* bytecode exists.

### VDBE backend — `compiler/emit.rs`

`emit_function(program, func, dest)`: verifies, then emits. Owns all
physical resources: fresh register per value (`Ret` values steered into
`dest`), label per block (allocated upfront so back-edges work), block-
parameter binding as edge copies (staged through temporaries when an
edge's copies overlap, e.g. loop-carried swaps), `If`/`IfNot`/`Goto`
encoding of three-valued branches with per-edge trampolines when
argument copies must happen on a conditional edge, fallthrough elision.
Emission is iterative (no recursion over values) and deterministic
(creation order, monotonic allocation).

## Integration points (today)

`translate_expr`'s `Expr::Binary` value arm tries
`compiler::try_emit_value_expr` before `binary_expr_shared`. Coverage:
literal-only trees over arithmetic/bitwise/concat operators, incl.
parenthesization and the eager path's unary folds (`-9223372036854775808`
-> `i64::MIN`, `~5` folded, `+x` transparent). Collation post-state
mirrors the eager path (cleared unless operands are equivalent); constant
hoisting rides the existing constant-span mechanism at the integration
boundary (the whole tree is constant, so the span opened by
`translate_expr` covers it — verified via EXPLAIN).

## Decisions already made

- **SQLite bytecode parity is NOT a goal.** Different-but-correct register
  numbering and instruction order are acceptable. Correctness is validated
  by results (conformance corpus, differential fuzzer), not EXPLAIN
  equality. (Explicit user decision, 2026-07-29, prior branch.)
- **`ProgramBuilder` stays as the backend assembler.** `emit.rs` targets
  it; the IR is a new frontend, not a new assembler.
- **Incremental migration with an escape hatch.** Eager and composable
  code must embed in each other throughout the transition
  (`External` inputs today; a `Raw`-style escape hatch when statements
  join the IR).
- **Verification is mandatory**, not debug-only, until the pipeline is
  mature.
- **Loop headers are explicit blocks**; no edges back into the entry.

## Known hard constraints (from the prior exploration; still ahead)

1. **Contiguous register ranges**: `ResultRow`, `MakeRecord`, `Function`,
   `IdxInsert` need adjacent registers -> the allocator needs register
   *packs* (groups pinned relative to each other).
2. **Mutable cells**: aggregate accumulators, coroutine yield slots,
   in-place custom-type encode. Pure SSA can't express them; they need an
   explicit `Slot` notion distinct from values, never CSE'd or hoisted.
3. **Effect regions**: column reads are pure only between cursor
   movements. When columns join the IR, either effect-token edges or a
   region discipline must bound interning/reuse of column reads.
4. **Affinity/collation ride on nodes**, not ambient state: comparisons
   must capture `CmpInsFlags` + collation into instruction payloads.
5. **Deep trees**: emission/verification are iterative. Building
   descriptions recurses over the AST exactly like today's
   `translate_expr` (`#[trace_stack]`-guarded); keep it that way until the
   frontend is iterative too.

## Validation

- Unit tests in `core/translate/compiler/mod.rs` assert emitted `Insn`
  shapes, verifier rejections, interning, edge-copy staging, determinism.
- Every integration step keeps `cargo test -p turso_core`, the
  conformance corpus (`make -C sqlite/conformance run-rust
  ARGS='--snapshot-filter __never__'`), clippy `--deny=warnings`, and
  `cargo fmt` green.
- Results-level differential validation (`scripts/diff.sh`, differential
  fuzzer) after each phase.

## Plan of attack (keep statuses updated)

### Phase 1 — Foundation + first integration

- [x] 1a. `core/translate/compiler/`: `Compiler<T>` combinators (`pure`,
      `map`, `map_with`, `then`, `and_then`, `branch`, `branch3`,
      external inputs), SSA CFG with block parameters, verifier
      (terminators, arity, entry rules, dominance-based def-before-use),
      backend emission (registers, labels, edge copies with parallel-copy
      staging, three-valued branch encoding, trampolines, fallthrough
      elision, `Ret`-into-dest). Unit-tested end to end, including a
      loop-shaped CFG.
- [x] 1b. First integration: `translate_expr` `Expr::Binary` value arm
      routes literal-only arithmetic/bitwise/concat trees through
      describe -> build -> verify -> emit, eager fallback otherwise.
      Collation post-state mirrored; constant hoisting verified via
      EXPLAIN; conformance corpus green.
- [x] 1c. Column and rowid reads as IR leaves (`Inst::Leaf` +
      `emit_function_with_leaves`): the leaf's AST expression is emitted
      by delegating back to eager `translate_expr` at the leaf's
      destination register, so cursor/index/covering/virtual/custom-type
      resolution stays in one place while the IR owns the tree.
      Structurally equal leaves dedup (`x + x` reads the column once).
      The frontend computes the collation post-state statically (same
      merge rules as `binary_expr_shared`) and the integration hook
      restores it after emission. Maximal constant runs of mixed trees
      emit inside constant spans during emission (`is_const` transitive
      flags), preserving hoisting into the prologue — verified via
      EXPLAIN. Safety gates — the IR path refuses: expression→register
      cache enabled (GROUP BY-style contexts where re-reading columns is
      wrong), expression indexes in play, custom-typed columns (operator
      overloads), array operands of `||` (ArrayConcat), SELF_TABLE
      placeholders. Region rule: one IR island = one region; no cursor
      movement inside.
- [x] 1d. Function calls as instructions (`Inst::Call` + per-call-site
      contiguous register packs). `FuncCtx` is not `Eq`/`Hash`, so call
      payloads live in a side table and calls are never interned. The
      emitter counts uses and steers single-use argument definitions
      directly into their pack slots (constants, tree results, leaves via
      `translate_expr(arg → slot)`, nested call results), so the common
      shape emits zero copies — identical to eager; shared values are
      copied in at the call site. Constness rides a frontend-set flag
      (`Expr::is_constant`, i.e. deterministic + constant args) ANDed
      with IR-level argument constness, so `abs(-5)` still hoists into
      the prologue (verified via EXPLAIN). Frontend coverage is the
      allowlist in `scalar_call_is_generic`: scalar functions whose eager
      arm is exactly args-into-contiguous-registers + one
      `Insn::Function` (abs/lower/upper/length/…, trim family, round,
      nullif, instr, scalar min/max, concat, char, printf), with arity
      gates matching the eager checks so violations fall back to
      identical errors. Also this slice: `Built.collation` replaced by
      `CollationEffect` (Untouched vs Sets), modeling the eager
      distinction between "leaves ambient collation alone" (literals,
      equivalent-operand binaries over neutral operands) and "overwrites
      it, possibly with None" (columns, non-equivalent binaries, calls
      fold their args' effects in order); the integration hook just
      applies the effect.
- [x] 1e. Comparisons in value position (`Inst::Compare` + `CmpData`
      side table — `Affinity` is not `Eq`/`Hash`). Affinity
      (`comparison_affinity`) and collation (operand contribution merge)
      are captured into the payload at description time, never read from
      ambient state at emission. The emitter expands each comparison to
      the eager `wrap_eval_jump_expr_zero_or_null` idiom (assume-true,
      conditional jump, `ZeroOrNull`) with backend-invented labels, and
      constant comparisons hoist (the span machinery already supports
      internal jumps). Comparisons set the collation effect to
      `Sets(None)` (the eager post-comparison reset). Gates: IS/IS NOT
      (null-equality), LIKE/GLOB, and array-typed operands stay eager.
      Coverage: `=`, `<>`, `<`, `<=`, `>`, `>=`.

### Phase 2 — Conditions and short-circuit control

- [x] 2a. Condition position: `Predicate` combinators (`and`, `or`,
      `from_bool`, comparison terminals) with `CondTargets` block
      triples replacing `ConditionMetadata` label threading. New IR:
      `Terminator::Exit(ExitId)` (symbolic external continuations, bound
      to labels at emission; empty exit blocks are bypassed so jumps go
      straight to the external label) and `Terminator::CmpBranch`
      (three-way comparison branch; the verifier requires the NULL
      target to coincide with true or false, since VDBE encodes NULL
      routing as `jump_if_null`). The integration hook at the top of
      `translate_condition_expr` covers AND/OR trees over comparison and
      truthiness terminals; IS/IS NOT, BETWEEN, IN, LIKE, CASE, and
      subqueries fall back. Emission is shape-identical to eager for
      WHERE terminals: a fallthrough-label hint drives opposite-op
      selection (`WHERE x=1` emits `Ne -> false` with `jump_if_null`)
      and the single-`IfNot` truthiness form. AND rewires the left NULL
      edge to continue into the right side unless NULL and false labels
      coincide; OR routes left false AND NULL into the right side —
      both exactly the eager flag semantics. Lesson recorded: leaf dedup
      across blocks must be dominance-safe (entry-or-current-block rule)
      — the verifier caught the sibling-branch reuse on the partial-index
      corpus before it became corrupt bytecode.
- [x] 2b-1. CASE (both forms) in value position: per-arm then/next
      blocks chaining into one join whose block parameter carries the
      result — the first production use of block parameters for values.
      Base form emits the eager `Ne base,when -> next` shape
      (`jump_if_null`, NO affinity conversion — `CmpData.affinity`
      became `Option<Affinity>` because flags-without-affinity is not
      `Some(Blob)`); searched form emits `IfNot when -> next`. The join
      is created after the arm blocks so emission order matches the
      eager layout (each WHEN falls into its THEN, ELSE falls into the
      join). Collation payloads per base comparison come from the
      running compile-time state folded in eager emission order (base,
      when/then per pair, else). Improvement over eager: arms use fresh
      registers, so the RegisterReuse constant-hoisting deopt does not
      apply — constant THEN values hoist (whole constant CASEs hoist
      wholesale, span machinery handles the internal control flow). The
      integration hook is shared (`try_compiler_value_expr`) and now
      covers both the Binary and Case arms of `translate_expr`.
- [x] 2b-2. Nullness in the IR: `Terminator::NullBranch` (two-way
      NULL/not-NULL branch; direction selection honors fallthrough and
      prefers jumping to the argless side so arg-carrying edges get
      inline copies instead of trampolines) and `Inst::NullTest`
      (value-position IS [NOT] NULL, the eager assume-true idiom, result
      never NULL). Coverage: COALESCE (>=2 args) and IFNULL (2 args) as
      nullness-branch chains joining in a block parameter — control
      flow, not calls; IS NULL / IS NOT NULL / ISNULL / NOTNULL in value
      position; and condition terminals in both AST spellings (postfix
      and `Binary(e, Is/IsNot, NULL)`), emitting the exact eager
      single-jump shapes (`NotNull -> false` / `IsNull -> true`).
      Root-level integration hooks added to the FunctionCall, IsNull,
      and NotNull arms of `translate_expr` (previously only Binary/Case
      roots reached the pipeline).
- [ ] 2b-3. Remaining short-circuit value forms: AND/OR in value
      position, BETWEEN, IN-list, LIKE/GLOB, IS TRUE/FALSE, CAST.
      Parameter/variable references (`Expr::Variable`) as leaves.
- [ ] 2c. Constant hoisting as an IR transform (move constant-only
      instructions to the entry block) so IR islands stop depending on
      the constant-span machinery; then start deleting
      `translate_expr_no_constant_opt` deopts that become
      unrepresentable.

### Phase 3 — Loops, effects, and resources

- [ ] 3a. `loop_over` combinator: explicit loop headers with loop-carried
      block parameters (the emitter's parallel-copy staging already
      handles the swap hazard).
- [ ] 3b. Effectful instructions: cursor open/seek/step, row reads as
      effects with region boundaries; symbolic cursors.
- [ ] 3c. Register packs (contiguous groups) + `Slot` (explicit mutable
      cells) in IR and emitter; liveness-based reuse instead of
      fresh-register-per-value.

### Phase 4 — Row streams

- [ ] 4a. `Compiler<RowStream>`: scan/filter/map over a table -> the
      Rewind/Next loop, composed (`scan(t).filter(p).map(proj)
      .consume(result_rows)`).
- [ ] 4b. Joins, aggregates, sorters, subqueries as stream operators;
      `emitter/` + `main_loop/` sequencing becomes tree lowering.
- [ ] 4c. Shrink and remove the eager-emission fallback per construct.

## Continuation notes for a fresh context

- Read this file, then `core/translate/compiler/mod.rs` (module docs are
  load-bearing), then the checklist above. The IR is only as integrated
  as the checklist says — do not assume more.
- The `#[allow(dead_code)]` on `combine`/`ir` in `compiler/mod.rs` exists
  because the authoring surface (branches, block params, externals) is
  test-only until Phase 2; drop the allows as integration catches up.
- When adding instructions: keep `Inst` `Eq + Hash` only if the new kind
  is safe to intern (effectful instructions must NOT be interned — gate
  `intern_in_entry` on purity when they arrive), keep emission iterative,
  capture affinity/collation in payloads rather than ambient state.
- Never break `cargo clippy --workspace --all-features --all-targets --
  --deny=warnings` or `cargo fmt`.
