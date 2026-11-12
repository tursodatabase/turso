# HIR migration plan

Status: preparation in progress. Checkpoint 1 is complete. Checkpoints 5–7 are not authorized for this execution.

Move SQL binding and resolution into a semantic analyzer that produces HIR,
while preserving the existing planning and execution algorithms. Start by
separating the data those algorithms consume from parser expressions and
name-resolution state. Then introduce the analyzer against those prepared
interfaces and remove the old binding path by statement family.

This plan concerns SQL compilation in `core/translate`, not the language driver
bindings in `bindings/`.

## Reference implementation

The reference is [PR #10579, SQL Expression Binding/Resolution rewrite](https://github.com/tursodatabase/turso/pull/10579),
examined at commit `1ab5d728b9d86e64362b29ed8136525ceffcc875`. Its
[migration notes](https://github.com/pedrocarlo/turso/blob/1ab5d728b9d86e64362b29ed8136525ceffcc875/docs/hir-binding-migration.md)
describe both intermediate extractions and the eventual replacement.

The starting code for this plan is main at `65154dad`, examined on
2026-11-05. The draft supplies implementation ideas and test cases; its
completion claims do not establish that the checkpoints below have passed on
current main. Intentional behavior fixes from the draft should remain separate
from behavior-preserving preparation changes.

## Current coupling

| Current type or function | Responsibilities combined today | Required separation |
| --- | --- | --- |
| `TableReferences` and `JoinedTable` in [plan.rs](../../core/translate/plan.rs) | Visible names, source metadata, access operations, column usage and index requirements | Semantic source descriptions and physical source requirements |
| `OuterQueryReference` in [plan.rs](../../core/translate/plan.rs) | Outer visibility, CTE definitions, lexical depth, rowid usage and correlation tracking | Lexical scopes, a CTE environment and resolved dependencies |
| `Resolver` in [emitter/mod.rs](../../core/translate/emitter/mod.rs) | Catalog access, semantic state, expression caches and register metadata | Analysis context and emission state |
| `FromClauseSubquery` in [schema.rs](../../core/schema.rs) | Output shape, nested plan, result registers and materialized cursor | Query output metadata and physical query execution |
| `prepare_one_select_plan` in [select.rs](../../core/translate/select.rs) | Source construction, binding, output metadata, aggregate ownership, subquery preparation and planning | Analysis followed by plan construction |

Table identity and outer dependencies remain necessary with HIR. Their current
combined representations become unnecessary. Putting the existing methods behind
a larger resolver interface would preserve the same coupling.

## Target ownership and interfaces

```text
parser AST + catalog context
             |
       semantic analysis
             |
         resolved HIR
             |
   planning and optimization
             |
      bytecode emission
```

| Information | Owner |
| --- | --- |
| Name lookup, aliases, lexical scopes, CTE visibility and SQL diagnostics | Semantic analysis |
| Source, query, expression and output identities; resolved functions, types, affinity and collation | HIR |
| Access paths, join order, covering requirements, derived predicates and evaluation scheduling | Planner |
| Cursors, registers, materialization and cached values | Emission |

The intended interface is small. These signatures describe the responsibilities,
not final Rust types:

```text
analyze(syntax_root, semantic_context) -> HirDocument
plan(document, planning_context)      -> QueryPlan
emit(plan, emission_context)          -> Program
```

Analysis must not require `ProgramBuilder`, `Resolver`, chosen access paths or
runtime locations. Scopes and individual name lookups stay inside the semantic
module. The planner receives resolved sources and expressions rather than
calling binding again.

### Identities and dependencies

- A source identity denotes one occurrence. A self-join has two source identities
  even when both refer to the same catalog table.
- A resolved column identifies its source and column position. Rowid remains
  explicit; it is not a fictitious ordinary column.
- An outer reference is a reference to a source or output owned by an ancestor.
  Lexical visibility is checked during analysis; scheduling is decided later.
- Direct captures and transitive dependencies are distinct. A grandchild reading
  a grandparent can impose a scheduling dependency on the intermediate query.
- A CTE definition, a reference to it, and shared materialization are distinct
  concepts. CTE definitions are bound lazily in their definition-time environment.
- An output identity does not by itself authorize evaluating an expression once
  and reusing the result. Alias substitution and volatile evaluation must preserve
  their SQL behavior.

### Metadata and runtime values

Reuse the existing [CatalogContext](../../core/translate/catalog.rs) and
[expression metadata interfaces](../../core/translate/expr_context.rs) where
they already provide useful separation. The semantic subset should describe
source columns and query output positions without requiring registers.

Analysis and planning must use consistent catalog facts, including namespaces,
search order, functions and collations. Captured catalog handles do not replace
schema-version checks and statement reprepare behavior. Dialect differences
remain methods on `Dialect`; PostgreSQL-specific implementation stays in
`postgres/`.

Runtime source bindings map resolved identities to cursors or register rows.
This lets ordinary scans, materialized results and OLD/NEW/EXCLUDED values share
expression lowering without rewriting semantic columns into registers.

### Keep planner preparation outside analysis

The reference draft's [analyzer](https://github.com/pedrocarlo/turso/blob/1ab5d728b9d86e64362b29ed8136525ceffcc875/core/translate/semantic/analyze.rs#L104)
calls `prepare_planning_terms_with_reads`, and its
[HirDocument](https://github.com/pedrocarlo/turso/blob/1ab5d728b9d86e64362b29ed8136525ceffcc875/core/translate/semantic/hir/mod.rs#L623)
contains `PlanningTerms`, including optimizer-derived predicates.

Move that preparation to planner entry. Keep intrinsic resolved facts and
dependencies available from HIR, but let planning own its additional terms and
caches. If optimization transforms HIR, define that transformation's ownership
and validation rules separately from the analyzer's output contract.

## Ordered implementation checkpoints

Each checkpoint should leave one working production path. The first three
extract existing algorithms and data without introducing another binder.

### 1. Make seek data independent of parser expressions

Extract `SeekDef<E>`, `SeekKey<E>`, range constraints and their iterators from
the draft. Keep current callers using `ast::Expr` through defaults or aliases.
Key width, prefix iteration and affinity iteration treat expressions as opaque
values. Keep AST-specific `IS`, `=` and NULL classification in the legacy adapter
or pass the resulting fact explicitly.

Start in [plan.rs](../../core/translate/plan.rs); the draft's
[generic seek definitions](https://github.com/pedrocarlo/turso/blob/1ab5d728b9d86e64362b29ed8136525ceffcc875/core/translate/plan.rs#L1893)
provide a concrete model.

Completion: existing seek, composite-index, NULL and affinity cases preserve
results and access paths. A HIR expression ID can occupy the same structures
without changing their algorithms.

### 2. Separate ordering proofs from semantic lookup

Extract order targets parameterized by source and expression representation.
Supply resolved direction, collation, NULL order, rowid alias and expression-index
matching facts to the existing ordering algorithm.

Keep both access-path scoring and final sort elimination using the same proof.
Preserve `AnyEquality` versus `ConstantEquality`: an equality depending on an
earlier loop does not establish global ordering across all iterations.

Completion: reverse scans, expression indexes, NULL placement, rowid suffixes
and MIN/MAX retain their behavior. The ordering proof no longer resolves SQL
names or reaches into `TableReferences` for semantic decisions.

### 3. Separate physical source requirements

Give B-tree candidate selection a small source interface containing the table,
namespace, index hints, covering requirements, right-row preservation and order
consumption facts. Keep `JoinedTable` as the current adapter.

Extract only the shared data required by these callers. The draft's complete
`PlannedSource<I,H,E,J>` is a reference, not a requirement to add all four
parameters immediately. Reuse current candidate iteration and cost calculations.

Completion: candidate costs, chosen indexes, covering decisions and join choices
remain unchanged. HIR sources can use the algorithm without fabricating a
`JoinedTable`.

### 4. Narrow resolver dependencies and separate usage tracking

Change semantic helpers to accept catalog or expression metadata instead of
all of `Resolver`. Separate derived-query output shape from its physical plan.
Keep expression caches and register metadata with emission.

Introduce one interface for column and rowid dependencies. During transition,
the legacy adapter applies usage updates at the same points as today. Preserve
usage multiplicities required by expression-index coverage; a dependency set
alone cannot replace those counts.

Avoid a mutable scope copy synchronized with `TableReferences`. Borrow current
facts through temporary adapters; after cutover, semantic sources become
authoritative and physical planning consumes them.

Completion: semantic metadata queries work without registers, cursors or nested
physical plans. Dependency reporting and planner usage accounting have separate
contracts.

### 5. Introduce the analyzer and execute a complete HIR query subset

Reuse the draft's analyzer, temporary scopes, lazy CTE environment and typed
identities, adapted to current main. Start with complete simple SELECT roots:
literals, parameters, ordinary columns, scalar calls, filtering, ordering and
limits. Connect them to shared planning and expression lowering in tests early.

Carry resolved function, type, collation and comparison decisions into consumers.
Do not convert HIR back to parser AST or call `prepare_select_plan` on reconstructed
syntax. That would repeat semantic analysis and lose the isolation.

Completion: analyze, plan and execute work together for the selected subset;
output metadata and errors agree with the intended behavior; analysis requires
no `ProgramBuilder`. Validate completed HIR documents in tests and debug builds.

### 6. Complete SELECT coverage and switch production

Extend the same path through derived queries, nested correlation, joins, CTEs,
recursive CTEs, compounds, aggregates and windows. Resolve query output metadata
before assigning runtime destinations. Keep aggregate and window ownership
explicit through transformations.

Include source-read programs: generated columns, stored defaults, custom-type
decoding and expressions needed for index planning. SELECT already depends on
these; deferring all stored-expression work until DML would leave hidden binding
inside emission.

Keep production on the existing SELECT path until the replacement covers the
family. Temporary old/new comparisons belong in tests. An analysis or lowering
error must never trigger a retry through the old binder.

Completion: SELECT uses one analyzer, preserves the required compatibility and
planning behavior, and its old binding path can be deleted.

### 7. Migrate remaining consumers and remove compatibility code

Migrate DML, RETURNING, trigger programs, foreign-key actions, schema-expression
callers and materialized-view maintenance. Model OLD, NEW and EXCLUDED as logical
sources with explicit visibility; assign runtime rows during lowering.

Use positional logical inputs for stored expressions and instantiate them for
the required source. Preserve existing database and schema-loading behavior,
including intentionally unresolved stored syntax where loading permits it.

Delete mini-binders and runtime AST rewrites as their callers move. Remove
semantic responsibilities from `Resolver` and `TableReferences`, then delete
the compatibility types when no consumer needs them.

Completion: migrated paths neither resolve raw names after analysis nor mutate
parser expressions into execution locations. Remaining DDL catalog operations
have explicit interfaces and do not recreate expression binding.

## Validation and performance

Preparation changes preserve current behavior. When an intentional fix is needed,
identify it separately and demonstrate its regression test failing before the
fix and passing afterward. Update SQL reference documentation for user-visible
changes.

| Area | What must remain correct |
| --- | --- |
| Resolution | Alias precedence, ambiguity, qualification, rowid shadowing, DQS and dialect rules |
| Query outputs | Width, names, declared types, affinity and collation |
| Correlation | Nested captures, outer rowid reads, aggregate ownership and evaluation timing |
| CTEs | Lazy validation, lexical shadowing, occurrence identity and materialization behavior |
| Joins | USING/NATURAL merged values, parenthesized groups, lateral visibility and NULL extension |
| Expression evaluation | Volatility, alias evaluation counts, short-circuit behavior and validation timing |
| Access planning | Covering requirements, expression/partial indexes, seek affinity and ordering proofs |
| Writes | Constraints, OLD/NEW/EXCLUDED, RETURNING, triggers and foreign-key actions |

Extend the narrowest existing suites, including
[nested scope depth](../../sqlite/conformance/sqlite-sqltests/nested-subquery-scope-depth.sqltest),
[outer USING references](../../sqlite/conformance/sqlite-sqltests/using-dedup-outer-ref.sqltest),
[outer-column aggregates](../../sqlite/conformance/sqlite-sqltests/aggregate-of-outer-column.sqltest),
[correlated expression-index reads](../../sqlite/conformance/sqlite-sqltests/expr-index-correlated-subquery.sqltest),
[CTEs](../../sqlite/conformance/sqlite-sqltests/cte.sqltest) and
[recursive CTEs](../../sqlite/conformance/sqlite-sqltests/recursive-cte.sqltest).
Use Rust integration coverage when SQL tests cannot express metadata or lifecycle
requirements. Exercise PostgreSQL conformance for dialect-sensitive changes.

Compare results, preparation errors, metadata and representative EXPLAIN plans.
Normalize incidental IDs only; do not normalize away different indexes, join
methods or sort operations. A bulk snapshot regeneration is not evidence that
planning behavior was preserved.

Measure preparation with the existing
[prepare benchmarks](../../benches/prepare_benchmark.rs) and
[parameter benchmarks](../../benches/prepare_params_benchmark.rs) before and after
the relevant changes, using the same build configuration. Include nested scopes
and repeated CTE references to catch repeated cloning or lookup work. Compare
representative execution workloads when planning or lowering changes.

## Proposed decisions and open questions

Proposed decisions:

- Prepare physical consumers first, then introduce the draft analyzer.
- Reuse the draft's scope and CTE model rather than building a temporary binder.
- Keep one production semantic path per migrated statement family.
- Keep runtime locations outside HIR and planner preparation outside analysis.
- Require downstream consumers to use resolved facts rather than lookup names.

Questions to resolve while implementing the corresponding checkpoint:

1. Which source facts can be borrowed directly, and which require an owned
   description for derived outputs? Prefer the smallest interface with actual
   callers over a generic replacement for all of `TableReferences`.
2. How should planner-generated expressions be owned after moving `PlanningTerms`
   out of analysis: a separate arena, or a planner-owned transformed document?
3. Which output references substitute an expression and which read an already
   evaluated value? Specify this before adopting register mappings for aliases.
4. Which dependency summaries are stored once, and which physical usage counts
   must be recomputed after optimization?
5. What prepare-time and execution regression thresholds are acceptable before
   each production cutover? Establish baselines before setting the thresholds.

## Progress

- [x] 1. Generic seek data
- [ ] 2. Ordering proofs over resolved inputs
- [ ] 3. Physical source requirements
- [ ] 4. Narrow contexts and explicit dependency reporting
- [ ] 5. Executable HIR SELECT subset in tests
- [ ] 6. Complete SELECT coverage and production cutover
- [ ] 7. Remaining consumers migrated and compatibility code removed

The first implementation PR is checkpoint 1. It changes seek data and its
current callers without changing SQL binding or selecting a different plan.
