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

## Invariants

- A compiler has one typed output.
- Combining compilers preserves the execution order of their effects.
- Symbolic values have exactly one definition.
- An operation may only use values defined before it in the same straight-line
  region. Control-flow regions will extend this rule with block dominance.
- Intermediate representations contain no physical VDBE locations.
- Invalid intermediate representations are rejected before VDBE emission.
- VDBE lowering is deterministic.
- Optimizations must preserve SQL evaluation frequency, error ordering,
  volatility, and three-valued boolean semantics.

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

The current IR supports straight-line scalar operations plus conditional
diamonds with block parameters. This establishes the combinator contract,
SSA joins, and the separation between symbolic values and VDBE resources
without prematurely choosing the representation of effectful database
resources or loops.

Supported expression fragments are assembled recursively. Boxing erases the
concrete Rust type of heterogeneous combinator trees while preserving deferred
execution; unsupported fragments return to the established expression emitter.

Values produced outside a region enter through symbolic input slots. Physical
register bindings are supplied only to the lowering boundary, allowing legacy
expression producers to feed declarative regions without placing register
numbers in the IR.
