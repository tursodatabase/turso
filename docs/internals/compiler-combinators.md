# Compiler combinators

The VDBE compiler is moving from direct instruction emission toward typed,
deferred compilation. A compilation routine should describe work and return a
symbolic result. Physical VDBE registers, cursor numbers, labels, and instruction
offsets are assigned only when the description is lowered into a program.

The authoring model follows parser combinators: primitive compilers are combined
into larger compilers with operations such as sequencing, output mapping, and
dependent sequencing. Constructing a compiler must not mutate `ProgramBuilder`.
Running the outermost compiler interprets the description into an intermediate
representation.

SQL parameters are symbolic values in that representation. Their stable parser
identity is retained without allocating a register or changing the statement's
bind table. Lowering registers each named or indexed slot with `ProgramBuilder`
and emits the corresponding VDBE `Variable` instruction into the allocated SSA
result register.

## Invariants

- A compiler has one typed output.
- Combining compilers preserves the execution order of their effects.
- Symbolic values have exactly one definition.
- Every value must dominate its use; loop-carried and merged values enter blocks
  through explicit block parameters.
- An owned cursor's open effect must dominate every operation that uses it.
- A once-only cursor initializer may satisfy resource dominance across its
  re-entry edge, but it never makes values defined by the initializer available
  as SSA outputs.
- Intermediate representations contain no physical VDBE locations.
- Invalid intermediate representations are rejected before VDBE emission.
- VDBE lowering is deterministic.
- Optimizations must preserve SQL evaluation frequency, error ordering,
  volatility, and three-valued boolean semantics.

## Optimization boundary

Lowering consumes optimized, re-verified IR rather than the graph produced
directly by compiler combinators. The initial pass folds branches whose
condition is a direct constant and removes blocks that become unreachable. It
uses the same numeric coercion as VDBE `IfNot`, including treating `NULL` as
false. It intentionally does not evaluate comparisons, arithmetic, integer
coercions, or other operations whose errors and SQL semantics need dedicated
analysis.

After simplifying control flow, a backwards liveness pass removes recursively
unused pure value instructions. Constants, symbolic inputs, parameters,
arithmetic, and logical operations are removable; integer coercions and column
reads remain ordered roots because they can expose datatype, storage, or
corruption behavior. Effect instructions and terminator control operands are
always live. Edge arguments instead become live through the destination block
parameters that consume them.

Block-parameter liveness therefore follows every incoming edge. When a join
position is unused, optimization removes that parameter and the corresponding
argument from every predecessor before recursively removing newly dead value
producers. Cursor rewind and advance terminators share one argument vector
between their row and exit targets, so the optimizer couples matching positions
in those target signatures: a position survives on both targets if either one
uses it. Removing dead join state before lowering avoids allocating its
registers and emitting its edge copies while preserving valid SSA arity.

Optimization runs before registers, cursors, labels, or instruction offsets are
assigned. Surviving value and resource identifiers remain stable arena indices,
so removed definitions may leave unused slots; the verifier still rejects every
used undefined value or unopened cursor and rechecks dominance on the rewritten
graph. The allocator represents those holes explicitly and assigns no physical
register or owned cursor to them. Statement parameter declarations and external
input arity are interface metadata rather than runtime value instructions. A
parameter occurring only in removed code therefore still creates its bind slot,
while its VDBE `Variable` instruction is not emitted.

Register allocation computes backwards liveness over the optimized control-flow
graph and colors an SSA interference graph. Values whose lifetimes do not
overlap can share one physical register, including values in mutually exclusive
branch arms and preheader values whose storage is dead before a loop-body
temporary is defined. Block parameters are simultaneous definitions at block
entry. The paired parameter packs targeted by cursor rewind and advance also
remain distinct because the current VDBE lowering materializes both packs before
choosing the successor. External input registers and the caller-provided result
register stay fixed; comparison scratch registers and contiguous result-row
packs are allocated separately from the SSA color pool.

Block arguments are parallel assignments, not an ordered series of copies.
Lowering emits every acyclic move only after its destination is no longer needed
as a source. A cyclic transfer, such as swapping two loop-carried values, saves
one source in a reusable temporary register before completing the remaining
moves. This makes register coalescing and reuse independent of edge-copy order.

## Migration plan

1. Introduce the deferred compiler interface and a straight-line scalar IR.
2. Route a narrow scalar expression path through the IR and lower it to the
   existing VDBE instruction set.
3. Add basic blocks, block parameters, and explicit terminators. Replace label
   threading in conditional expression compilation with three-way branches.
4. Represent registers, register packs, cursors, sorters, hash tables, rowsets,
   and coroutine state symbolically.
5. Express SELECT loop construction as composable producers and consumers that
   build control-flow IR instead of mutating `ProgramBuilder`.
6. Lower block parameters to edge copies, allocate physical registers and
   cursors, lay out blocks, and resolve instruction offsets.
7. Move aggregates, sorting, joins, subqueries, DML, triggers, and foreign-key
   programs across incrementally, retaining the existing emitter as a fallback
   until each path has equivalent coverage.

The current IR supports straight-line scalar operations, conditional diamonds,
explicit loops with block parameters, and effectful cursor folds and row
production. A production SELECT path uses these pieces for a forward scan of
one B-tree table. Its frontend first resolves an owned scalar description, then
compiles that description against each symbolic row. Columns, literals,
addition, three-valued `AND` and `OR`, searched `CASE`, parentheses, collation
wrappers, and ordinary comparisons can be nested and shared by both result
expressions and predicates.
It composes `scan_table`, row-stream operators, scalar projection, and
`result_row`, then builds and verifies the complete IR before touching
`ProgramBuilder`.

The row stream has an associated symbolic item type and composes chainable
`filter` and `map` operators before a terminal `for_each`. A filter compiles its
predicate in the row block and emits an SSA branch around the downstream
consumer; false and NULL rows proceed directly to cursor advance. A map runs a
deferred compiler and changes the item type, so SELECT projection is expressed
as `filter(...).map(...).for_each(result_row)` rather than being embedded in the
terminal consumer.
Short-circuiting consumers use an SSA `try_fold` protocol: each accepted item
returns a state pack and a symbolic continuation value. A false continuation
branches directly to the loop exit, while a true continuation reaches the
cursor-advance block. `take` layers a remaining-row value onto that state pack,
so nested stream operators preserve their own loop state and the final accepted
row exits without moving the cursor again. Its count is itself a deferred value:
an SSA integer-coercion operation implements SQLite's `MustBeInt` behavior, a
zero count branches around the source pipeline, and a negative count never
reaches zero. `skip` uses the same deferred count contract, carries its
remaining offset through loop block parameters, and invokes its downstream
consumer only after that value is no longer positive. This places filtering
before offset accounting and projection after it. Nesting `skip` inside `take`
also preserves SQLite's evaluation order: a zero limit exits before the offset
expression is evaluated. The production table-scan path uses these operators
for row-independent `LIMIT` and `OFFSET` expressions supported by the scalar IR,
including parameters and searched `CASE`; column-dependent and otherwise
unsupported counts retain the eager emitter.
`has_rows` is the scalar terminal built on the same protocol. It carries a
false value from the empty edge, replaces it with true on the first row, and
returns a false continuation so both paths join as one SSA boolean without
advancing past that row. This is the compiler-level operation used for
declarative `EXISTS`. `first_or` generalizes that terminal to a stream of SSA
values: it carries a caller-provided empty-stream value, replaces it with the
first produced value, and joins the two paths as one result. Scalar subqueries
use `first_or(NULL)`, which represents SQLite's empty-result and first-row
semantics without assigning a result register or emitting a subroutine.

The authoring surface remains generically typed, but stream consumption erases
the concrete compiler and row-callback types at every adapter boundary. This is
the practical escape hatch used by parser-combinator libraries for large
recursive descriptions: it bounds Rust monomorphization and generated symbol
size without assigning registers, emitting VDBE instructions, or losing the
typed item and result contracts visible to compiler authors.
The production path preserves predicate and projection source order. Comparison
affinity and collation are resolved by the SQL frontend before IR construction.
The resulting terminator has separate true, false, and NULL successors; a
comparison used as a value joins `1`, `0`, or `NULL` through a block parameter.
An ordered compiler combinator collects independently composed scalar values
into a symbolic `ValuePack`; contiguous result registers are still chosen only
during lowering. Searched `CASE` builds nested SSA diamonds, so only the chosen
result arm executes and every arm joins as one value. Base-expression `CASE`
still falls back until its per-`WHEN` affinity and collation rules are resolved.
The path also falls back when SQL semantics still live in the eager frontend:
other expression forms, `IS` comparisons, joins, aggregates, most subquery
forms, generated values, arrays, and custom-type decoding.
`EXPLAIN QUERY PLAN` also remains on the eager path until the IR models
explain-tree effects.
Ordinary column lowering does reuse the VDBE backend's logical-column helper, so
rowid aliases, logical-to-physical column mapping, and ALTER-added defaults keep
their established behavior. This narrow bridge establishes that a complete
query loop can cross the deferred compiler boundary while unsupported plans
continue through the existing emitter.

`loop_while` builds a preheader, header, body, and exit. Its initial value and
each backedge value flow into the same header parameter, so the loop state has
one SSA definition even though it changes at runtime. The Rust closures used to
describe the condition and body run once while building IR; repetition exists
only in the resulting control-flow graph and lowered VDBE program.

`fold_cursor` describes an iterator-like fold over an already-open symbolic
cursor. It emits a cursor `Rewind` terminator, a row block whose parameter is the
accumulator, a cursor `Next` backedge, and an exit parameter containing either
the initial value for an empty cursor or the final accumulated value. `RowStream`
provides the producer/consumer form used by SELECT compilation: `scan_table`
returns a symbolic stream and `for_each` adds a row block without exposing a
dummy loop-carried value to the SQL frontend. Both operations build CFG edges;
neither iterates while the compiler description is constructed.

Column reads remain ordered instructions inside the row block. The IR stores a
symbolic cursor identifier. External cursors are declared as input resources and
receive their physical binding at lowering. IR-owned table cursors instead carry
their `CursorType` as resource metadata; lowering allocates the physical cursor,
while an ordered `open_read` effect determines where `OpenRead` executes. The
effect also carries its database schema cookie, so lowering registers the read
transaction for that database before the cursor opens. The verifier treats that
effect as the cursor's definition, rejecting a read, rewind, or advance unless
the open dominates it on every control-flow path. Only after those checks does
lowering emit `OpenRead`, `Rewind`, `Column`, and `Next` instructions.

Ephemeral indexes separate symbolic resource declaration from runtime
initialization. Declaration returns an unopened typed handle; the open compiler
consumes that handle and returns the cursor accepted by stream and index
operations. The existing combined open operation is built from those two
primitives. This distinction lets structured control-flow regions own the open
effect without selecting a physical cursor early, while the verifier continues
to reject every path that uses a declared but unopened resource.

`initialize_cursor_once` places that open and an arbitrary effectful producer
behind an IR `Once` terminator. First entry follows the initializer edge;
subsequent entries in the same statement execution jump directly to the ready
edge and reuse the populated cursor. Lowering emits VDBE `Once` plus an explicit
jump to the initializer, so correctness does not depend on physical block
layout. Verification keeps two dominance views: ordinary values use the full
CFG, while cursor-open checks omit only the stateful re-entry edge. A value
defined inside the initializer therefore still cannot escape it. Nested
uncorrelated `IN` materialization uses this region to compose its producer and
consumer as one deferred program while preserving once-per-statement behavior.
Composition recursively descends through fully declarative `IN` chains before
unwinding their cursor bindings. At the top level, the resulting producers are
ordinary ordered stages in one IR graph and need no `Once` guard at all. If an
unsupported ancestor leaves the region behind a nested eager boundary, the
guarded initializer preserves the standalone subquery's re-entry semantics.

Row production is an effect over a symbolic `ValuePack`, not an eagerly chosen
register range. Each pack member remains an ordinary SSA value and must dominate
the `result_row` effect. During lowering, the backend allocates one consecutive
register range, copies the member values into that range in order, and emits the
VDBE `ResultRow`. This lets producer combinators return values independently of
the contiguous layout required by row consumers.

Supported expression fragments are assembled recursively. Boxing erases the
concrete Rust type of heterogeneous combinator trees while preserving deferred
execution; unsupported fragments return to the established expression emitter.

Values produced outside a region enter through symbolic input slots. Physical
register bindings are supplied only to the lowering boundary, allowing legacy
expression producers to feed declarative regions without placing register
numbers in the IR. Input collection walks supported scalar expression trees, so
columns, parameters, and legacy subexpressions can participate in one deferred
region while preserving their source evaluation order. Collection is
transactional: if a subtree cannot be represented safely, speculative inputs
are discarded before that subtree is treated as one external value.
An input slot can also be scoped to another compiler output before IR is built.
In that case every use resolves directly to the producer's `ValueId`, no
external input remains at lowering, and ordinary dominance verification covers
the composed value. Simple uncorrelated `EXISTS` children compile to `has_rows`;
simple uncorrelated single-column scalar children compile to `first_or(NULL)`.
Their values can be bound into supported outer expression trees, and both scans
then lower as one graph. Scalar `LIMIT` and `OFFSET` compose through the same
stream operators before the first-row terminal. Correlated, aggregate,
distinct, ordered, multi-column, and other unsupported subquery shapes retain
the eager subquery emitter.

Conditional branches only admit branch bodies that are fully represented in
the IR. An unsupported branch body sends the whole conditional back to the
legacy emitter instead of evaluating that body as an eager external input.
Branch outputs implement an SSA-join contract, so the same combinator can merge
a scalar value, a loop-state pack, or a state-plus-continuation loop step through
block parameters.
Likewise, SQL-specific operator implementations remain atomic external inputs;
the surrounding expression may be deferred without bypassing custom-type
dispatch.
