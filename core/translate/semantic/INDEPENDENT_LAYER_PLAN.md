# Independent semantic HIR plan

Status: design and verification plan only. The extracted semantic code remains
dead code as described in `EXTRACTION.md`.

## Goal

Make semantic analysis a module that can be compiled, changed, benchmarked, and
tested without calling the current prepare path. The module should eventually
replace the old binding code as one unit, but it must prove that it is complete
before any replacement begins.

The module's main interface is deliberately small:

```text
parser AST + immutable semantic context
                  |
                  v
       semantic::analyze(...)
                  |
                  v
        closed HirDocument
```

`HirDocument` is closed when every SQL name, scope decision, catalog object,
type-dependent operation, stored expression, and correlation needed by a later
stage is represented without consulting parser syntax or doing a live catalog
name lookup.

## Non-goals

- Do not call this analyzer from production prepare yet.
- Do not change or remove `core/translate/bind.rs` yet.
- Do not add a VDBE, DBSP, optimizer, or planner lowerer yet.
- Do not keep two implementations in sync through shared binding helpers. The
  independent layer needs its own tests and simple external oracles.
- Do not split this into a separate crate before the module interface is stable.
- Do not optimize every `clone()`. Cheap ID and `Arc` clones are useful.

## How to compile it without integrating it

The first implementation step should add only a test compile seam. Compile the
semantic module for its unit/property tests under `cfg(test)` or a private test
feature. Do not make the production prepare path call it.

Keep the Hegel tests beside the module so they can use crate-private parser,
schema, function, and HIR types:

```text
core/translate/semantic/
  tests/
    mod.rs
    model.rs
    generators.rs
    reference_scope.rs
    canonical.rs
    properties.rs
    regressions.rs
```

This is preferable to a standalone test crate with copied table/function
stubs. Tests using fake catalog objects could pass while the real replacement
fails.

Add `hegeltest` as a development-only dependency of the owning crate. The
workspace already resolves version `0.28.2`, whose Rust import name is `hegel`.
Pin the chosen version because the package describes its API as beta.

## Changes to make inside the layer

The sections below are ordered. Items marked **required** should be completed
before the old binder is replaced. Choices explicitly left to benchmarks should
be implemented only after the prepare measurements show that they matter.

### 1. Add an independent HIR validator — required

Add a validator that accepts only `&HirDocument`. It must not receive parser
syntax, `SemanticContext`, `Analyzer`, or a live catalog. Run it after every
successful Hegel case and fuzz input.

The validator should live with HIR, not in a planner. That makes document
closure part of the HIR interface.

It should validate at least:

- every `QueryId`, `QueryBlockId`, `SourceId`, `CteId`, `OutputId`,
  `SchemaProgramId`, and index-method pattern identity resolves;
- each arena entry contains the identity implied by its index;
- query blocks, outputs, sources, CTEs, and root outputs have the right owner;
- every `ColumnRef` is within the target source's column range;
- every `OutputId` is within the target owner's output range;
- subquery, derived-source, and CTE edges resolve;
- compound arms have the required output width;
- source column metadata vectors have exactly the same width as `columns`;
- required generated/default/type programs are no longer `NotRequired` when
  analysis finishes;
- schema-program calls refer to finished programs and their input sources have
  `SourceKind::SchemaExpression`;
- no schema-program binding cycle or reserved-but-unfilled arena slot survives;
- all resolved catalog objects carry the document snapshot and expected
  database identity;
- all roots and arena objects are reachable, unless an explicitly documented
  metadata table permits otherwise; and
- an expression can be walked and understood using only the document.

Keep this validator independent from analyzer helpers. Reusing the same helper
for construction and validation would let one bug teach both sides the same
wrong rule.

The current document IDs are plain arena indexes. A validator cannot tell that
`SourceId(0)` was copied from another document if this document also has source
zero. Do not claim that it can. By default, keep IDs compact and expose
document-bound accessors to consumers so a borrow of one document is needed to
resolve them. If hard runtime cross-document detection is required, adding a
document nonce to every ID is an explicit memory/performance tradeoff that must
be measured first.

The validator should be available in debug/test/fuzz builds. Whether it runs in
release prepare can be decided later.

### 2. Record lexical query relationships — required

HIR currently represents an outer column as a `SourceId`, but a query does not
explicitly record its lexical parent or the outer sources it captured. A later
stage can reconstruct that information by repeatedly walking expressions, but
then lexical validity is harder to validate and lowering repeats semantic work.

Record on each query:

- its lexical parent query/block, if any;
- the distinct outer `SourceId`s it uses; and
- preferably the exact outer columns it uses.

Populate this during expression analysis. The validator should reject a
correlated reference whose source is not owned by a lexical ancestor.

This information is semantic context, not planner state. It belongs in HIR.

### 3. Make the semantic context a plain snapshot — required

Separate two things that are currently stored together:

1. shared immutable catalog facts; and
2. small per-analysis options such as trigger visibility and DML policy.

The shared snapshot should contain the database search order, schemas, symbol
table/function resolver, dialect, DQS setting, custom-type setting, and a real
catalog/settings generation identity. Trigger and nested-DML options should be
small values layered on top without cloning all schema maps.

Build this snapshot outside the analyzer. Hegel tests must be able to build one
directly from a small generated catalog without constructing a connection,
opening a database, or taking runtime locks.

Do not let this module import execution/compiler policy just to make a semantic
decision. In particular:

- materialized-view compatibility should arrive as a catalog fact or policy;
- sequence backing-table identity should arrive through a shared schema helper;
- SQL affinity should live in a shared semantic/schema type, not under VDBE;
  and
- collation identity should live in a shared catalog type, not behind a
  translation-stage dependency.

The first testable version may remain a private module in `turso_core`. These
dependency directions should still be cleaned up before considering a separate
crate.

### 4. Define snapshot meaning precisely — required

The current monotonically allocated snapshot serial prevents objects from two
analysis runs being mixed, but it is not itself proof of a schema change. Define
whether the identity means:

- one analysis run; or
- one immutable catalog/settings generation reusable by several analyses.

For future prepare caching, prefer a generation identity that changes when a
binding-relevant schema, function table, collation table, dialect setting, DQS
setting, or custom-type setting changes. Per-analysis ownership can remain a
separate debug identity if useful.

Physical planning must receive the same generation and reject incompatible
metadata. It must not silently fall back to a live name lookup.

### 5. Separate semantic graph facts from planner candidates — required design

A table source currently contains both SQL meaning and eagerly instantiated
index candidates. Adding an otherwise unused index can therefore change and
slow down the semantic document even though expression binding and result
metadata are unchanged.

Keep these concepts visibly separate, even if they remain fields of one returned
package at first:

- semantic graph: sources, columns, expressions, aliases, joins, CTEs, output
  facts, DML targets, and explicit `INDEXED BY`/`NOT INDEXED` constraints;
- snapshot planning facts: expression-index programs, partial-index
  predicates, index-method patterns, and other candidates discovered from the
  captured catalog.

The planner facts must still use resolved object identities and the same
snapshot. Separation must not reintroduce name lookup.

This gives tests a stable semantic projection: adding a non-hinted index may
change the candidate set, but it must not change bindings, outputs, types, or
the statement root.

Whether read-side index programs are bound eagerly or on demand is a benchmark
decision. DML generally needs all affected indexes and should remain closed.

### 6. Analyze an expression once — required for prepare cost

Expression analysis currently performs recursive analysis and then walks each
completed subtree again to collect required source columns. Output construction
then walks expressions again for type, affinity, affinity presence, and
collation.

Return an internal analyzed value containing the expression and its computed
facts:

```text
AnalyzedExpr
  expression
  type fact
  affinity and has-affinity
  collation
  source-column dependencies
  aggregate/window/subquery facts needed by the caller
```

Children should contribute these facts while the expression is built. Do not
walk every subtree again at every parent. This removes quadratic behavior for
deep expression chains and gives one place to state fact-composition rules.

Do not put `Arc` around every expression node as the first fix. Per-node
allocation and reference counting may cost more than the copies being removed.

### 7. Make scopes persistent and keep namespace indexes — required

Nested queries should share an immutable outer scope using `Arc<Scope>` rather
than deep-cloning all outer sources, columns, outputs, windows, and CTE
bindings.

Within one scope, keep ordered vectors where SQL order matters, such as star
expansion, and build lookup maps for:

- normalized unqualified column name to unique/ambiguous resolution;
- table/alias qualifier to source identities;
- database plus table qualifier to source identities; and
- output alias to unique/ambiguous output resolution.

Scope entries should carry IDs and compact facts. They should not own copies of
complete output expressions unless SQL alias substitution requires an owned
expression at that exact point.

Use one normalized identifier representation per name. Repeatedly allocating a
lowercase `String` during lookup should not be part of the hot path.

### 8. Remove accidental heavyweight cloning — required

After the analyzer can mutate arena entries safely, remove `Clone` from
heavyweight HIR types where it is not part of the intended interface. In
particular, avoid cloning a whole `HirDocument`, `Query`, `Source`, `Cte`,
`QueryBlock`, or output vector to satisfy a borrow.

Use:

- borrowed output slices or a lightweight output-facts view;
- field splitting where two arena fields need independent borrows; and
- take, mutate, and restore helpers for reserved `Option<T>` arena entries.

Cheap clones of typed IDs, `Arc` catalog handles, and small policies should
remain.

The recursive CTE refresh path is especially important: mutate the query/CTE
in place and return compact output facts instead of cloning the complete query
on every fixed-point round.

### 9. Use a work queue for required source programs — required

Track required source columns with a membership set plus a deterministic queue.
Enqueue a column only on the first insertion. Materializing one column may
enqueue its transitive dependencies.

Every unique required column should be materialized at most once. Avoid scanning
the whole pending set to select one item for each iteration.

### 10. Compute CTE dependencies once — required for large WITH clauses

Collect references to visible CTE names in one syntax traversal per CTE body,
rather than scanning the same SELECT once for every visible CTE. Keep source
order separately where shadowing rules depend on it.

Borrow parser CTE bodies for the duration of analysis instead of deep-cloning
them. The returned HIR must remain owned; transient analyzer state does not need
to be.

### 11. Keep stored schema expressions as a separate positional form — required

`ValidSchemaExpr` is a useful seam. Keep its important rules:

- table columns and type inputs are positional after resolution;
- valid and preserved-unresolved expressions are different states;
- each expression profile explicitly controls columns, rowid, subqueries,
  non-deterministic functions, and current-time literals; and
- instantiation into statement HIR uses the same catalog generation as the
  owning source.

Avoid specializing a complete stored-expression tree before checking whether
the same schema program is already cached. Prefer a cache key based on the
definition, program kind, source identity, and input type facts. If measured
cost remains high, represent type-parameter substitution as an input mapping
instead of rewriting the tree.

### 12. Add semantic error categories — worth doing

Property and differential tests should compare stable error categories, not
complete text. Keep the human message and optional syntax location, but expose
categories such as:

- unsupported statement;
- unknown database/table/column/function/type/collation;
- ambiguous name;
- invalid aggregate/window/subquery placement;
- invalid CTE recursion;
- invalid stored expression; and
- schema generation mismatch.

Generated user input should not produce `InternalError`. Reserve that category
for a broken context or HIR invariant.

## Hegel test model

Generate a small model first and render it into parser AST or SQL. Do not start
with arbitrary SQL strings for property tests; most bytes only test parser
rejection and shrink poorly.

The model should contain:

- one to four databases with an explicit search order;
- tables with bounded columns, rowid modes, hidden columns, declared types,
  generated/default expressions, checks, and optional indexes;
- built-in and generated function/type/collation definitions;
- query sources, aliases, joins, CTEs, subqueries, compounds, outputs, grouping,
  windows, ordering, and limits;
- INSERT, UPDATE, DELETE, RETURNING, UPSERT, and trigger pseudo-sources in
  separate generators; and
- expressions chosen from the exact sources and outputs visible at the point
  where they are generated.

Use deferred recursive generators with separate depth and total-node budgets.
Keep names short and drawn from a collision-prone set so ambiguity and
shadowing occur often. Record the generated catalog, SQL, settings, and random
seed in Hegel notes so a failure can be replayed before shrinking.

Maintain two top-level generator families:

- valid-by-construction inputs, which should usually produce HIR; and
- deliberately invalid inputs, each carrying the expected semantic error
  category.

## Hegel properties

### Document closure and structural validity — P0

1. **No panic:** every generated AST/context pair returns HIR or a semantic
   error. It never panics, aborts, loops forever, or indexes outside an arena.
2. **Success validates:** every successful analysis passes the independent HIR
   validator.
3. **No internal error for user input:** a well-formed generated context plus
   arbitrary user SQL never returns `InternalError`.
4. **All references resolve locally:** replacing an ID with an out-of-range ID
   or an ID whose encoded owner disagrees with its arena location makes
   validation fail. Same-index IDs from different documents are intentionally
   outside this property unless IDs gain a document nonce.
5. **Snapshot consistency:** every resolved catalog handle and schema program
   in one document has the document's catalog generation.
6. **No unfinished state:** successful analysis contains no reserved arena
   holes, `NotRequired` required programs, unresolved parser names, or
   in-progress schema-program bindings.
7. **Reachability:** every HIR node is reachable from the root or from an
   explicitly documented snapshot-planning table.

### Determinism — P0

8. **Same input, same HIR:** analyzing the same AST and semantic context twice
   produces the same canonical semantic projection after erasing only
   per-analysis debug identity.
9. **Catalog insertion order does not matter:** rebuilding the same catalog in
   a different map insertion order gives the same result. Keep attached search
   order fixed for this property.
10. **Error category is deterministic:** invalid input produces the same error
    category and relevant object name on replay.

The canonical projection must be implemented separately from the analyzer and
must not clone the complete document merely to erase unstable values.

### Name binding and scope — P0

Build a small, deliberately boring reference resolver for generated scope
models. It should implement only lookup precedence and ambiguity, not expression
analysis. Compare production scope resolution with it.

11. **Unqualified lookup agrees with the reference resolver.**
12. **Qualified lookup agrees with the reference resolver.**
13. **Ambiguity is never resolved by source insertion accident.**
14. **Output-alias precedence is clause-specific:** SELECT, GROUP BY, HAVING,
    compound ORDER BY, and ordinary ORDER BY follow their documented rules.
15. **Star expansion order is stable:** database/table stars and merged
    `USING`/`NATURAL` columns appear exactly once in SQL order.
16. **Outer lookup is lexical:** a correlated expression may see permitted
    ancestors and may not see siblings, descendants, or pruned trigger/DML
    scopes.
17. **Correlation summary is exact:** the query's recorded outer sources and
    columns equal those found by an independent walk of its expressions.
18. **Rowid rules hold:** rowid aliases, WITHOUT ROWID tables, hidden columns,
    and explicit columns named `rowid`, `_rowid_`, or `oid` resolve correctly.
19. **CTE shadowing holds:** the nearest visible CTE wins and a WITH clause's
    visibility rules do not depend on hash-map order.
20. **Unused CTEs stay unused:** an unreachable CTE body does not add queries,
    sources, or schema programs to the document. Compatibility with SQLite
    should be checked for cases where an unused body contains an invalid name.

### Metamorphic binding properties — P0/P1

21. **Alpha rename:** consistently renaming a generated table alias, CTE name,
    or output alias preserves the canonical semantic graph modulo display
    names.
22. **Identifier case:** consistently changing ASCII case of non-conflicting
    generated identifiers preserves binding according to SQLite identifier
    rules.
23. **Irrelevant catalog extension:** adding an object with an unrelated name
    and function arity does not change the semantic projection.
24. **Unused index isolation:** adding a non-hinted index may change only the
    snapshot planner-candidate projection, never expression bindings, outputs,
    types, or the root.
25. **Equivalent qualification:** adding a database/table qualifier that names
    the object already selected by unqualified lookup preserves binding.
26. **Source reorder sensitivity is intentional:** reordering independent FROM
    sources preserves bindings only when all references remain unambiguous; it
    must still update star order and join ownership correctly.

### Expression facts — P0

27. **Dependency summary is exact:** dependencies returned during analysis equal
    an independent walk of the finished expression.
28. **Facts match outputs:** an output's type, affinity, has-affinity, and
    collation match the facts computed for its expression under SQLite's
    left-precedence rules.
29. **Parameter identity is stable:** repeated named parameters share the
    required parameter identity; numbered and anonymous parameters follow
    parser numbering rules without zero or duplicate accidental slots.
30. **Clause policies hold:** aggregate, window, subquery, and `RAISE`
    expressions are accepted only in allowed contexts.
31. **Function identity is closed:** overload selection depends on normalized
    name and argument count, and the chosen function handle survives without a
    later resolver call.
32. **Collation precedence is stable:** explicit COLLATE and left-side
    precedence produce the same resolved collation after harmless expression
    rewrites that do not change precedence.

### Type facts and recursive CTEs — P0

33. **Type merge is idempotent:** merging a fact with itself returns the same
    fact.
34. **Order-independent set merge:** operations documented as merging a set of
    possible values are invariant under permutation.
35. **Widening only loses certainty:** a recursive round never changes an
    unknown fact back into an unsupported stronger claim.
36. **Fixed point terminates:** every bounded generated recursive CTE finishes
    within the documented round bound.
37. **Fixed point is idempotent:** running stabilization again on stable facts
    makes no change.
38. **Widths stay aligned:** seed, recursive arms, outward CTE columns,
    comparison collations, and recursive ORDER BY outputs agree on width.
39. **Recursive references are occurrence-local:** every syntactic recursive
    reference has its own source identity but points to the same enclosing CTE.

### Stored expressions and schema programs — P0

40. **Resolve/render/parse/resolve round trip:** a valid stored expression keeps
    the same positional column references, functions, collations, and type
    inputs after rendering and reparsing.
41. **Column rename preserves position:** renaming a referenced column changes
    display syntax but not the resolved positional dependency.
42. **Column permutation remaps positions:** applying a known column permutation
    updates every positional reference by the same permutation.
43. **Profiles enforce their rules:** defaults, checks, generated columns,
    index keys, partial-index predicates, domain checks, and type transforms
    accept exactly their allowed source features.
44. **Strict and preserve modes differ only on failure:** successful resolution
    produces the same valid form in both modes; failure is returned in strict
    mode and kept explicitly unresolved in preserve mode.
45. **Program memoization:** the same definition, kind, source, and input facts
    yield one `SchemaProgramId`.
46. **Cache keys do not alias:** changing any meaning-bearing key component
    cannot return a program with the old meaning.
47. **Cycle rejection:** recursive generated/default/type program dependencies
    return a semantic error and leave no in-progress slot in a successful
    document.
48. **Instantiation is snapshot-local:** a stored expression resolved in one
    catalog generation cannot be instantiated with a source from another.

### DML and triggers — P1

49. **Target identity is exact:** INSERT, UPDATE, and DELETE roots contain the
    table selected under database search and trigger restrictions.
50. **Required row image is closed:** every old/new/target column required by
    assignments, checks, indexes, RETURNING, triggers, or custom codecs is
    materialized exactly once.
51. **Assignment order is not lookup order:** all update right-hand sides see
    the correct pre-update row regardless of assignment list order.
52. **UPSERT scope is correct:** target columns, `excluded`, source query
    columns, and RETURNING names follow their distinct visibility rules.
53. **Trigger pseudo-sources are explicit:** `NEW` and `OLD` exist only when
    allowed for the trigger event, and their source owners and table handles
    match the trigger input.
54. **Trigger database restriction holds:** generated cross-database references
    are rejected or accepted according to temp/non-temp trigger rules.
55. **Inherited conflict policy is captured:** changing the live caller after
    analysis cannot change the conflict choice stored in the root.

### Context state-machine properties — P1

Use Hegel's state-machine support to generate a sequence of catalog snapshots
and prepare operations.

56. **Old documents are immutable:** after adding/dropping/renaming a table,
    index, function, collation, type, or attached database, an already returned
    document keeps its original handles and canonical projection.
57. **New analysis sees the new generation:** a new context reflects the model
    mutation and carries a different generation when binding-relevant state
    changed.
58. **Cross-generation planning fails:** planner metadata from one generation
    cannot be paired with a document from another.
59. **Settings are binding inputs:** DQS, dialect, custom types, and symbol-table
    changes either change the generation or are captured explicitly in the
    document's resolved choices.
60. **No post-binding lookup:** after `analyze` returns, poison the context's
    name resolvers. HIR validation and every independent semantic projection
    must still work. When a lowerer exists, the same poison check becomes a
    required lowerer property.

## Complexity properties and prepare benchmarks

Do not assert wall-clock time in Hegel tests. Add test-only analysis counters
and assert work bounds on generated non-recursive inputs:

- completed expression subtrees are not rescanned for dependencies;
- each syntax expression node is semantically analyzed once per documented
  context;
- creating a nested scope copies no source columns or output expressions;
- reading query output facts performs no deep expression clone;
- each unique required source column is materialized at most once;
- one CTE body is not rescanned once per visible CTE; and
- recursive CTE rounds stay within their explicit bound.

Keep timing and allocation measurements in prepare-only benchmarks:

- `SELECT 1` for context fixed cost;
- deep and balanced expressions;
- wide tables and output lists;
- deep correlated subqueries;
- many joins, including `USING` and `NATURAL`;
- long compound queries;
- many reachable and unreachable CTEs;
- wide recursive CTEs requiring widening;
- tables with many indexes, expression indexes, generated columns, defaults,
  checks, and custom-type programs; and
- wide DML with RETURNING and triggers.

Record wall time, allocation count/bytes, syntax nodes, HIR nodes, scope entries
copied, expression fact walks, recursive rounds, and schema/index programs
instantiated. Use these measurements before choosing expression interning,
`Arc<Expr>`, or lazy read-index binding.

## Fuzz targets

Hegel provides structured generation and shrinking. Keep separate long-running
fuzz targets for mutation coverage:

1. Raw SQL bytes: parse, analyze supported roots, validate every success.
2. Structured catalog plus AST: analyze and validate, with strict size bounds.
3. Recursive CTEs: fixed-point termination, width agreement, and validation.
4. Stored expressions: profile resolution, render/reparse, rename, permutation,
   specialization, and schema-program binding.
5. Context sequences: catalog/settings mutation and cross-generation misuse.
6. SQLite differential prepare: compare success/error category and result
   column count/names for the shared supported subset.

Seed the raw parser target with real statements; otherwise most inputs stop at
syntax rejection. Give every target its own corpus and resource bounds so a
recursive-query timeout does not hide stored-expression coverage.

Every minimized semantic failure should become a deterministic Hegel/Rust
regression. SQL compatibility failures should also become `.sqltest` cases when
the test format can express them.

## Replacement gate

Do not replace the old binder merely because the new module compiles. Replacement
is ready only when all of the following are true:

- every supported statement kind has a documented generator and deterministic
  regression coverage;
- every successful Hegel and fuzz case passes the independent HIR validator;
- generated invalid cases return stable semantic categories rather than panic
  or `InternalError`;
- the SQLite differential subset agrees on prepare acceptance and result-column
  metadata, with intentional differences listed;
- stored-expression and recursive-CTE properties are clean across saved seeds;
- context state-machine properties prove snapshot isolation;
- prepare benchmarks meet an agreed allocation/time budget and show no
  expression-depth or scope-depth blow-up;
- a temporary comparison harness can lower old binding and HIR for the same
  corpus and compare externally visible plans/behavior without sharing binding
  helpers; and
- the HIR lowerer succeeds with live parser/catalog name lookup disabled.

Only after that gate should production prepare select HIR, first behind a
temporary switch, then by deleting the old binding path. The goal is deletion,
not permanent dual binding.
