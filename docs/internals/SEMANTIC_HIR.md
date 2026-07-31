# Semantic HIR architecture

Status: implemented for the statement roots accepted by `semantic::analyze`
and for incremental materialized views.

## Overview

Semantic HIR is the single name-resolution and SQL-meaning boundary for
`SELECT`, `INSERT`, `UPDATE`, `DELETE`, trigger commands, and trigger `WHEN`
predicates. It borrows parser syntax, resolves it once, and returns an owned
`HirDocument`. Consumers lower that document into their own representations
without consulting parser syntax or resolving SQL names again.

Stored schema expressions use the separate, schema-owned `SchemaExpr` boundary.
When a supported statement needs one of those expressions, semantic analysis
instantiates it as ordinary HIR and keeps the bound program in the same
document. DDL and control statements that have no semantic HIR root continue
to use narrow schema-only adapters.

```text
SQL text
   |
   v
parser AST (borrowed syntax)
   |
   v
semantic::analyze + SemanticContext
   |
   v
HirDocument (owned and closed)
   |                              |
   v                              v
PlanIdentityMap + PlanExpr        LogicalPlan + LogicalExpr
   |                              |
   v                              v
VDBE planning and emission        DBSP compilation
```

The VDBE and DBSP plans stay separate. They share resolved SQL meaning, not one
physical plan or one runtime representation.

## Analysis boundary

`semantic::analyze` accepts an `AnalyzeInput` containing one of:

- a supported statement;
- a trigger command plus its trigger environment; or
- a trigger predicate plus its trigger environment.

The input AST is immutable. Analysis never rewrites it and never allocates a
register, cursor, label, or VDBE offset. The returned document owns every query,
source, CTE, output, expression tree, bound schema program, and resolved catalog
handle needed by its root.

`SemanticContext` is a read-only snapshot of the facts needed to understand
SQL:

- main, temp, and attached schemas;
- function, collation, and custom-type lookup;
- dialect and double-quoted-string settings;
- DML policy; and
- optional trigger database and `NEW`/`OLD` visibility.

It contains no `ProgramBuilder`, execution-time register map, cursor state, or
mutable expression cache. A `CatalogSnapshot` token is copied into the HIR, and
resolved catalog values carry stable `CatalogObjectId` values. Consumers use
those resolved objects; they do not search a live catalog by SQL name.

The parser AST can still be retained for SQL rendering or storage. Once
analysis succeeds, it is not a second source of meaning for a HIR consumer.

## The closed HIR document

`HirDocument` owns one `HirRoot` and document-local arenas for queries, sources,
CTEs, and bound schema programs. A root is a query, insert, update, delete, or
trigger predicate. Trigger commands produce ordinary DML roots with an
explicit `TriggerEnvironment`.

### Typed identities and graph ownership

Semantic relationships use typed identities:

- `QueryId` identifies a query or subquery;
- `QueryBlockId` identifies a block within a query;
- `SourceId` identifies one row-source occurrence;
- `CteId` identifies one reachable CTE definition;
- `SchemaProgramId` identifies one bound stored-expression body;
- `OutputId` identifies an existing output value; and
- `CatalogObjectId` identifies a resolved catalog object in the captured
  snapshot.

An expression subquery points to a `QueryId` in the same document. A column is
a `ColumnRef` containing a `SourceId` and column position. An output alias,
ordinal, or compound-ordering reference that denotes an existing result uses
`OutputId` instead of copying the expression.

This makes ownership explicit. Correlation is represented by a column whose
`SourceId` belongs to an outer query; there is no second depth counter or
captured-column side list to keep synchronized.

### Queries, sources, and CTEs

A `Query` owns its blocks, compound arms, final output list, ordering, limit,
and reachable CTEs. A `QueryBlock` owns its FROM tree, output expressions,
filter, grouping, windows, values rows, and distinctness.

Every `Source` has an owner, a `SourceId`, a known column shape, affinity and
collation facts, rowid availability, and a resolved source kind. Source kinds
cover tables, table-valued functions, CTEs, derived queries, recursive inputs,
schema-expression inputs, and the `EXCLUDED`, `NEW`, and `OLD` pseudo-sources.

Table sources also have three column-aligned program lists: generated
expressions, read-time defaults for short records, and custom-type column
programs. Generated and default entries distinguish an absent expression from
one that exists but is not needed by this statement. Analysis starts with that
catalog shape and materializes entries only for required columns. Dependencies
found while materializing one entry feed the same required-column worklist, so
the finished document includes the transitive programs needed by the root and
does not bind every stored expression on the table.

`USING` and `NATURAL` joins store the resolved input columns and the visible
merged-value rule. Star expansion, alias precedence, ordinal resolution,
compound-output validation, and output naming finish during analysis.

Only reachable CTE bodies enter the document. Recursive CTEs store seed and
recursive arms explicitly, plus the occurrence-local recursive-input sources.
Their column facts are solved to a fixed point after every arm is bound. A
cycle that can keep increasing array rank is widened to an explicit unbounded
rank instead of freezing the first iteration's answer. Each round refreshes
nested expression subqueries, derived sources, reachable CTE sources,
table-function arguments, VALUES columns, and compound outputs before the
parent expression reads them. Type-dependent custom operators are rebound
once, after the facts stabilize, so a temporary round cannot create a schema
program or a false error.

Recursive queue ordering stores a resolved output position and any explicit
collation instead of recovering them from a child plan. UNION comparison
collations are also frozen separately from outward seed-column collation,
using compound left precedence across the seed and recursive arms. Physical
planning then creates one canonical recursive result-column set from those
final semantic columns; the seed and recursive plans are execution inputs,
not alternate metadata sources.

### Expressions

`hir::Expr` contains resolved meaning:

- literals and parameters;
- source columns, merged columns, rowids, and output references;
- resolved scalar, aggregate, and window calls;
- operators, casts, collations, CASE, arrays, subscripts, and field access;
- scalar, `EXISTS`, and `IN` subqueries linked by `QueryId`; and
- `RAISE`, with trigger visibility and Turso's outside-trigger `ABORT` extension
  fixed by the expression policy.

Raw identifiers, unresolved aliases, `DEFAULT`, star expressions, register
references, and optional binding sidecars cannot be represented in HIR.
`NEW`, `OLD`, and `EXCLUDED` are source identities rather than special runtime
expression variants.

`ExprPolicy` holds clause-specific name and feature rules. It also captures
SQLite's narrow double-quoted-string compatibility rule for an optimized
single-row `INSERT ... VALUES`: a top-level quoted identifier may become a
string, while the same identifier nested inside another expression remains an
unresolved column. This policy is decided during analysis and is not inferred
again by the planner.

### Bound schema programs

A `BoundSchemaProgram` is an ordinary HIR expression bound against a synthetic
`SourceKind::SchemaExpression` source in the same document. Its
`SchemaProgramId` is stable for the life of the document. A `BoundSchemaCall`
contains that ID and owned HIR expressions for the call's type arguments; the
runtime value occupies the program's first synthetic input.

Analysis specializes type parameters or the domain value before binding the
program. It reuses a body when the resolved type object, program kind, input
facts, and expression profile match. Calls still keep their own actual
arguments. Explicit binding state rejects a recursive program instead of
leaving an unfinished arena entry.

These calls freeze the custom-type choices needed by supported statement
roots:

- each required source column carries its encode chain, reverse decode chain,
  array storage facts, and whether a scalar encoder must run for `NULL`;
- each cast target carries its encode calls, optional domain checks, and an
  explicit decision about whether ordinary SQLite affinity still applies;
- domain `NOT NULL` and CHECK failures carry their final error descriptions;
  and
- a custom binary operator carries the resolved function and, when one literal
  operand needs conversion, the original operand plus its optional encoder
  call.

The HIR does not retain a physical column or a custom-type name for later
rediscovery of those decisions.

### Type, affinity, and collation facts

SQLite is dynamically typed, so HIR records facts without inventing certainty.
`TypeFact` has four independent pieces:

- `storage: Option<Type>` records a known storage class when one is known;
- `declared: Option<DeclaredType>` records a resolved declared or custom type;
- `array_dimensions: u32` records array rank even for literals and computed
  arrays that have no declared SQL type; and
- `array_rank_unbounded: bool` records that recursion or a dynamic array
  operation can produce values deeper than any finite rank known at prepare
  time.

A dynamic expression has no storage or declaration and has rank zero. Source
columns, query outputs, merged columns, parameters, cast targets, function
results, and custom field operations carry the relevant facts. Affinity is
separate from storage type:
`affinity` plus `has_affinity` distinguishes a real BLOB affinity from an
expression that has no affinity. Collation is also explicit.

These facts are part of the semantic contract and are copied into consumer
representations. Consumers may derive physical policy from them, but may not
re-run type-name or custom-type resolution.

## Closed consumers

HIR is immutable input. Each consumer creates owned identities and expressions
suited to its runtime. Neither consumer stores a movable HIR expression with a
document-local identity and then discards the document.

### VDBE planning

Before expression lowering, `ProgramBuilder::allocate_plan_identities` asks
`PlanIdentityMap::allocate_document` to build a complete semantic-to-plan
mapping through the builder-owned `PlanIdentityAllocator`:

- `PlanSourceId` for sources;
- `PlanOutputId` for outputs;
- `PlanSubqueryId` for queries; and
- `PlanCteId` for CTEs.

Each HIR document gets its own mapping, but every document lowered into one
program draws source, output, subquery, and CTE identities from the same
allocator stream. This includes trigger subprograms and prevents identities
from different documents from colliding. Pre-allocation also makes forward
output and subquery references independent of traversal order. Missing
mappings are internal lowering errors, never prompts for name or catalog
lookup.

`lower_hir_expr` converts `hir::Expr` to `PlanExpr`. `PlanExpr` preserves
resolved functions, collations, custom-type operations, type facts, affinity,
and typed plan identities. It cannot represent unresolved column references,
stars, defaults, parser subqueries, registers, cursors, or labels.

`PlanIdentityMap` lowers each `SchemaProgramId` at most once into a shared
`PlanBoundSchemaProgram`. Each call lowers only its owned arguments and points
at that shared body. The program's synthetic source receives a normal
`PlanSourceId`, so emission supplies runtime values through the same typed row
binding mechanism as other planned expressions.

Planning lowers one source's generated, default, and column-codec entries into
an `Arc<SourceReadPrograms>`. The same allocation is carried by its
`JoinedTable`, every correlated `OuterQueryReference`, and any encoded
`RuntimeRowBinding` for that source. A correlated, trigger, or foreign-key read
therefore uses the exact programs selected for the original source instead of
looking at the live schema again.

`HirPlanContext` borrows the document and identity map while building a plan.
Its `ProgramBuilder` reference is the construction target after semantic
analysis; it is not part of semantic analysis. The context does not carry a
`Resolver` or `Connection` for rediscovering meaning.

Runtime locations are a later concern. `PlanRuntimeBindings` maps
`PlanSourceId`, `PlanOutputId`, and `PlanSubqueryId` values to row, register, or
subquery runtime operands during emission. Materialized-CTE bookkeeping is
keyed directly by `PlanCteId`. Runtime registers therefore never flow backward
into HIR or `PlanExpr`.

Output ownership remains explicit while lowering nested queries. References
to an output owned by the current query block may expand to that block's
definition. A reference owned by an enclosing block stays a
`PlanExpr::Output` and gives the child plan a `PlanOuterOutputReference` with
the frozen output facts and source dependencies needed for scheduling. The
owning SELECT assigns each output a stable register and refreshes the required
value immediately before the child runs. Grouped and ungrouped aggregate
aliases are refreshed only after their aggregate values have been finalized.
This lets nested `WHERE`, `HAVING`, and `ORDER BY` queries read enclosing
aliases without copying an aggregate expression into the wrong query block.

Normal `SELECT`, DML, trigger, and foreign-key emission consumes these planned
expressions, schema programs, and read-program allocations. It does not run
semantic analysis again and does not resolve a custom type by name.

Array records remain canonical BLOB values through coroutines, ephemeral
tables, DISTINCT, ordering, recursive queues, row-value subqueries, and
buffered DML `RETURNING`. The shared result emitter copies and presents them
only immediately before an API-facing `ResultRow`, using the frozen `TypeFact`
and custom element programs. Internal consumers therefore never receive a
display string in place of the value selected by SQL.

Trigger preparation is still semantic orchestration: each trigger command and
`WHEN` predicate enters through its own `AnalyzeInput`, produces its own
`HirDocument`, and receives fresh plan identities. This happens while the
statement and trigger subprograms are prepared. It is not a schema-only
adapter, per-row analysis, or post-HIR name resolution.

### Incremental planning

`LogicalPlanBuilder` borrows a `HirDocument` and produces an owned
`LogicalPlan`. It may reject a resolved construct that the DBSP implementation
does not support, but it does not parse SQL, consult a schema, or resolve a
name.

Logical columns use `LogicalColumnId`:

- `Source(ColumnRef)` for source values;
- `Output(OutputId)` for existing query outputs; and
- `Synthetic(usize)` for compiler-created values.

`LogicalSchema::find_column_id` is the lookup boundary. Column and table names
remain only as display and output metadata. CTE plans are keyed by `CteId`, and
table scans retain the exact `SourceId` and resolved table handle.

Incremental lowering finalizes HIR type facts into concrete DBSP `Type` values.
It rejects dynamic, array, custom, or otherwise unsupported values rather than
guessing. `LogicalExpr` retains exact resolved functions, collations, cast
targets, aggregate argument types, and aggregate result types. The DBSP
compiler resolves row positions only through typed logical IDs.

## Incremental materialized-view lifecycle

`IncrementalViewTemplate` is the immutable, root-independent product of
incremental analysis. It privately owns:

- the complete `LogicalPlan`;
- the resolved base-table handles needed to feed the circuit; and
- the final materialized-view column schema.

The template does not retain parser syntax or a `HirDocument`. The HIR is
borrowed only while producing the owned logical plan.

CREATE and reload use the same semantic path:

1. During `CREATE MATERIALIZED VIEW` preparation, `analyze_select` runs
   semantic analysis and builds the template. The prepared program owns the
   template.
2. Execution allocates the materialized data root and the two DBSP state roots.
   `IncrementalView::from_template` supplies those roots and compiles the
   circuit.
3. Schema reload parses the stored CREATE statement after the main schema is
   available, analyzes it with `SemanticContext::for_main_schema_object`, and
   builds the same template shape before instantiating the circuit.
4. Initial population scans each resolved base table once. Filtering,
   projection, joining, grouping, and aggregation happen in the circuit, not
   in parser-expression helpers.

Materialized views persist source SQL rather than HIR because catalog snapshots
and source identities are process-local. A semantic or incremental-compilation
failure aborts view creation or schema finalization; no partly resolved view is
installed.

## Stored schema expressions

Stored expressions have a different lifetime from statements and are owned by
`core::schema_expr`.

`SchemaExpr::resolve` applies a `SchemaExprProfile` for defaults, CHECK
constraints, generated columns, index keys, partial-index predicates, domains,
or type transforms. It produces one of two explicit states:

- `ValidSchemaExpr`, whose column meaning is positional through `SelfColumn`,
  `SelfRowId`, `DomainValue`, or a typed parameter; or
- `UnresolvedSchemaExpr`, which deliberately retains parser syntax and an
  optional error for lenient schema loading and later repair.

Valid expressions own resolved functions, collations, casts, custom-type
operations, and field positions. They support dependency discovery, rendering,
rename, and positional remapping without using a fake table identity.
Compilation requires the valid form.

`core::translate::semantic::schema_expr` instantiates a valid stored expression
for a concrete semantic source. This keeps the schema module independent of
HIR while giving query and DML consumers the same closed expression contract.

For HIR-backed reads and writes, semantic analysis fills only the required
generated, default, and column-codec slots. Planning places them in the
source's shared `SourceReadPrograms`, and emission consumes those `PlanExpr`
and schema-call values.

For HIR-backed DML, analysis also instantiates the target table's CHECK
constraints when they are enabled. Each planned check owns its HIR expression
and final name or rendered description. The emitter evaluates those planned
checks; it does not reopen the stored CHECK expression.

Some schema operations deliberately have no statement HIR source. Their
adapters are explicit and named for that boundary:

- ALTER TABLE row validation uses `compute_virtual_columns_from_schema` and
  `emit_check_constraints_from_schema`;
- DROP TABLE foreign-key cleanup uses
  `emit_columns_and_dependencies_from_schema`, the schema parent-key path, and
  `decode_fk_key_registers_from_schema`; and
- schema-only custom transforms and domains use
  `emit_schema_type_transform` and `emit_schema_domain_constraints`.

These helpers may instantiate and immediately lower valid `SchemaExpr` values
or consult the catalog facts supplied by the DDL operation. They are not the
normal SELECT, DML, trigger, or foreign-key emission path. In particular,
ordinary foreign-key DML uses the target's frozen `SourceReadPrograms`; the
DROP TABLE path is the explicit exception because it has no DML HIR target.

## Architectural invariants

After `semantic::analyze` succeeds:

- all table, column, alias, database, CTE, function, collation, type, and field
  meaning required by the root is resolved;
- every semantic identity belongs to the returned document and every column
  position is valid for its source;
- every referenced schema program belongs to the returned document, and each
  call owns the HIR arguments needed to invoke it;
- output arity, names, star expansion, compounds, and reachable CTE shape are
  final;
- catalog meaning is held by resolved snapshot objects rather than names;
- no runtime register, cursor, label, or VDBE offset exists in HIR;
- HIR consumers lower through typed identity maps and do no semantic lookup;
- VDBE planning preserves `TypeFact`, affinity, and collation in `PlanExpr` and
  plan output facts;
- required generated, default, custom-type, cast, domain, literal-encoding,
  and DML CHECK behavior is frozen before normal emission;
- incremental planning produces concrete DBSP types or returns an unsupported
  feature error; and
- materialized-view templates are complete before storage-root instantiation
  or initial population.

Column-use masks, plan runtime bindings, and storage roots are derived after
analysis. They are plan or execution state, not detached semantic side data.

## Removed binding surface

Paths that enter through semantic analysis no longer have:

- `core/translate/bind.rs`;
- `BindContext`, `BindPhase`, `BindScope`, or public bind traits;
- `BoundSelect`, `BoundSubquery`, `BoundInsert`, `BoundUpdate`,
  `BoundDelete`, or paired `Bound*` sidecars;
- a `ProgramBuilder` dependency in semantic analysis;
- trigger binding that rewrites `NEW` or `OLD` into registers;
- a parser-to-logical-plan binder;
- `bind_logical_source` or `bind_logical_column`; or
- name-based logical column and CTE lookup.

Names still exist for diagnostics, rendering, and output metadata. They are not
semantic identity.

## Parser and direct-translation boundary

The parser crate represents syntax only. Parser expressions do not contain
resolved columns or rowids, internal table identities, resolved custom-type
calls, subquery-result slots, or runtime registers. Those concepts belong to
HIR, `PlanExpr`, or runtime bindings.

DDL and control statements with no HIR query root may still use parser syntax
or the explicit schema-only adapters described above. This does not permit a
translator or optimizer to write resolved or runtime state into the AST.
Stored `UnresolvedSchemaExpr` also retains parser syntax intentionally, but its
unresolved state is explicit and cannot be compiled as a valid schema
expression.

New work on SELECT, DML, and trigger expressions enters through
`semantic::analyze`; it must not invent a parallel resolution path.

## File map

| Area | Main files |
| --- | --- |
| Analysis boundary | `core/translate/semantic/mod.rs`, `core/translate/semantic/context.rs` |
| Query and DML analysis | `core/translate/semantic/query.rs`, `cte.rs`, `dml.rs`, `trigger.rs`, `expr.rs`, `scope.rs` |
| HIR | `core/translate/semantic/hir/{mod,expr,query,root,schema_program}.rs` |
| VDBE expressions and identities | `core/translate/plan_expr.rs`, `core/vdbe/builder.rs` |
| VDBE HIR lowering and source programs | `core/translate/planner.rs`, `core/translate/plan.rs` |
| Stored expressions and bound programs | `core/schema_expr/{mod,resolve,rewrite,render}.rs`, `core/translate/semantic/{schema_expr,schema_program}.rs` |
| Schema-only adapters | `core/translate/emitter/{mod,gencol}.rs`, `core/translate/expr/plan.rs`, `core/translate/fkeys.rs` |
| Incremental HIR lowering | `core/translate/logical.rs`, `core/incremental/compiler.rs`, `core/incremental/expr_compiler.rs` |
| Incremental template lifecycle | `core/incremental/view.rs`, `core/translate/view.rs`, `core/schema.rs` |

## Maintenance rule

Add new SQL meaning at the semantic boundary and represent it in HIR. Lower it
independently into `PlanExpr` and, when supported, `LogicalExpr`. A consumer may
choose a physical strategy or reject an unsupported feature, but it must not
resolve a raw SQL name, inspect parser syntax for meaning, or write runtime
state back into HIR.
