# HIR binding migration

## Goal

Replace the planner-era semantic binder with an analyzer that produces closed,
validated HIR directly. Preserve SQLite binding rules, diagnostics, and test
coverage while replacing old data structures.

Do not make `BindScope` own a `HirDocument`. One `Analyzer` owns the document
arenas. Temporary scopes contain document-local IDs and small lookup facts.
They do not contain planner nodes, mutable AST, runtime registers, or cloned
documents.

## Design mapping

| Old binding concept | HIR design |
|---|---|
| `BindContext` | `Analyzer` owning document arenas |
| `BindScope` | Temporary name-resolution structure |
| `ScopeTable` / `TableInternalId` | `ScopeSource` / `SourceId` |
| Bound alias expression | `OutputId` plus resolved expression facts |
| `BoundSelect` / `BoundSubquery` | `Query`, `QueryBlock`, and `Source` |
| `BindTracking` | Required-column worklist; calculate captures afterward |
| Cloned CTE AST | Lazy CTE state with reserved `CteId` |
| `allow_unbound` | Register sources before resolving expressions |
| `right_join_swapped` | Preserve RIGHT/FULL joins in HIR |
| Trigger registers | NEW/OLD/EXCLUDED pseudo-sources |
| `ProgramBuilder` labels/registers | Physical lowering only |

## Migration order

1. Port `SemanticContext`, analyzer arenas, HIR scope, and expression analysis.
2. Move SELECT vertically: sources -> expressions -> joins -> outputs -> compounds.
3. Add lazy CTE handling and correlation calculation.
4. Move INSERT, UPDATE, DELETE, and pseudo-sources.
5. Move triggers and stored schema expressions.
6. Validate every completed document.
7. Switch the production entry point after coverage is complete.
8. Delete `Bound*`, `TableReferences` conversion, and planner-era binding code.

## Current status

Core SELECT and top-level DML binding are present. The user-visible SELECT
coverage audit is complete; deferred migration work remains below.

### Remaining binding coverage audit

This audit compared every remaining semantic catch-all with parser AST shapes,
the PR #8111 binder, the current planner and expression translator, and existing
conformance tests. The temporary copy of the PR binder was removed after the
audit completed. A generic `unsupported SELECT statement` error is not evidence
that the old path rejects the same SQL.

No audited user-visible SELECT binding gaps remain.

Deferred migration surfaces:

| Surface | Why deferred |
|---|---|
| Production entry point and lowering | Statement and whole-trigger-program inputs produce validated HIR from an owned multi-database semantic catalog. Preparation can capture that catalog from the same schema inputs used to create the legacy resolver, but does not call the semantic entry point yet. Switching execution to HIR remains a later step; physical lowering is explicitly out of scope for now. |

Preserved binding restrictions, not HIR gaps:

| Restriction | Matching old-path evidence |
|---|---|
| RIGHT JOIN after an earlier join | Same diagnostic in planner and archived binder. |
| Aggregate or window functions in a recursive CTE arm | Same planner rejection. |
| DISTINCT windows, aggregate argument ORDER BY for windows, unsupported ordered-set forms, and moving-start `array_agg` windows | Same planner restrictions and diagnostics. |
| UPDATE or DELETE of WITHOUT ROWID tables | Same statement-entrypoint rejection. |
| Subqueries in UPSERT conflict targets and DO UPDATE expressions | Same archived-binder and expression-translator rejection. |
| UPSERT or non-VALUES sources for virtual-table INSERT | Current virtual-table INSERT emitter only accepts VALUES or DEFAULT VALUES. |

Internal or already-normalized parser nodes are not new SQL coverage:

- Non-SELF `Expr::Column` and `Expr::RowId`, `Expr::Register`, and
  `Expr::SubqueryResult` are products of old binding, planning, or emission.
- `Expr::Default` is consumed by INSERT/UPDATE destination binding before
  ordinary expression analysis.
- `Func::AlterTable` is created by ALTER TABLE translation, not by a SELECT
  function call.

Completed:

- SELECT B-tree sources freeze every resolved ordinary, expression, and
  partial index in catalog order. Expression keys and predicates close over
  the exact `SourceId`; complete coverage lets HIR planning avoid rebinding
  schema AST when matching indexes.
- Analyzer-owned HIR arenas and mandatory document validation.
- Trigger `WHEN` predicates bind directly against event-shaped, qualified-only
  NEW/OLD pseudo-sources. Column types and rowid identity stay in HIR; invalid
  event-row references keep their existing diagnostics without rewriting AST
  nodes into positional variables.
- SELECT analysis receives one expression-policy bundle for all clauses,
  table-function arguments, CTEs, and nested queries. Trigger policy is applied
  when the bundle creates each clause policy, so callers do not consult
  semantic context again or lose NEW/OLD and RAISE rules in nested SELECTs.
- Trigger SELECT commands become ordinary query HIR carrying one trigger
  environment. NEW/OLD remain reserved qualified namespaces across every
  clause, CTE, and nested query, including LIMIT; no positional-variable AST
  rewrite is needed.
- Trigger INSERT commands become ordinary INSERT HIR carrying one trigger
  environment. Their target is resolved directly in the trigger database;
  VALUES, source queries, and UPSERT expressions share trigger expression
  policies, and outer conflict policy wins without cloning or rewriting AST.
- Trigger UPDATE commands become ordinary UPDATE HIR carrying one trigger
  environment. Inner changed-row sources remain distinct from outer trigger
  NEW/OLD sources; SET, FROM, derived queries, and WHERE share trigger policies,
  and the target plus conflict policy are resolved without AST rewriting.
- Trigger DELETE commands become ordinary DELETE HIR carrying one trigger
  environment. The inner target remains distinct from the outer OLD row;
  predicates and nested queries keep trigger policies, and the target resolves
  directly in the trigger database without AST rewriting.
- Trigger roots own their environment once and distinguish predicates from
  SELECT, INSERT, UPDATE, and DELETE commands with enums. Ordinary query and
  DML nodes no longer carry optional trigger-only state; validation receives
  trigger-command context from the root shape.
- Whole trigger programs bind their optional WHEN predicate and ordered command
  list in one analyzer. Every command reuses one NEW/OLD environment, statement
  conflict policy reaches INSERT and UPDATE without AST rewriting, and HIR
  validation visits the complete program.
- Semantic context owns one normalized database-name map, database-ID map, and
  explicit unqualified search path across main, temp, and attached schemas.
  Qualified tables and indexes keep their database identity; DML metadata uses
  the target database, and documents record sorted schema-version snapshots.
- A neutral semantic catalog captures `Arc` references to main, temp, attached,
  and connection-local staged schemas from the same inputs used to create the
  legacy resolver. It also captures the exact unqualified lookup order without
  holding catalog locks; the resolver neither creates nor owns this catalog.
- A standalone semantic entry point consumes that owned catalog and
  produces a validated statement or whole-trigger-program `HirDocument`.
  Trigger targets resolve inside the catalog from database ID and table name,
  so callers cannot inject an unrelated catalog table. Analysis borrows only
  catalog schemas, symbols, dialect, and semantic options; it imports no
  resolver or emitter types, and live resolver and locks are not needed.
- The defensive semantic path for parser-rejected `INSERT INTO t(a) DEFAULT
  VALUES` preserves the parser's `0 values for N columns` diagnostic.
- HIR scope with source, output-alias, database-qualified, outer-scope, rowid,
  USING/NATURAL merged-column, star-expansion, and collation rules.
- One ordinary table source and basic result outputs, including stars.
- Immutable AST-to-HIR binding for names, literals, unary and binary operators,
  null tests, parentheses, `COLLATE`, and SQL parameters.
- `LIKE`, `GLOB`, `REGEXP`, and `MATCH` expressions carry resolved function
  identity and argument count. Only `LIKE` accepts `ESCAPE`; row-valued left
  sides remain limited to `MATCH`.
- Iterative expression analysis, including deep-expression coverage.
- `BETWEEN`, list `IN`, `CASE`, and built-in `CAST` expressions.
- Row operands in binary comparisons, `BETWEEN`, and list `IN`. Iterative
  expression frames flatten only supported row positions; HIR keeps one
  comparison component per element with resolved affinity and collation, while
  width mismatches and scalar-only positions retain their existing errors.
- Multi-column UPDATE subqueries bind as one row-subquery assignment. Query
  output width is validated against target width, outputs receive destination
  types, correlated captures stay document-owned, and duplicate targets retain
  last-assignment-wins behavior through shared scalar output references.
- Row subqueries bind as single HIR operands in binary comparisons and
  `BETWEEN`. Iterative frames carry query identity plus output facts; width,
  affinity, and collation checks consume those facts without flattening query
  evaluation into separate scalar subqueries.
- Catalog-resolved custom `CAST` targets that need no stored program.
- Simple custom `CAST` encoders bound against document-owned synthetic inputs.
- Domain `NOT NULL` and `CHECK` rules frozen into custom `CAST` targets.
- Scalar function calls with resolved catalog identity, including calls inside
  custom `CAST` encoders.
- Plain aggregate calls with stable query-block identities and frozen result
  type facts. `COUNT(*)`, `DISTINCT`, argument `ORDER BY`, aggregate `FILTER`,
  and ordered-set forms are explicit in HIR; nested aggregates are rejected
  during analysis.
- Inline window calls with stable query-block identities, resolved partition
  and ordering expressions, and frozen effective frames. Aggregate window
  filters and built-in frame coercion keep their existing rules.
- Named and inherited windows resolve during analysis. Functions reference
  block-owned `WindowId` values; parser window names and inheritance links do
  not enter HIR.
- `RAISE(ABORT, ...)` expressions, including built-in custom-type encoders such
  as `VARCHAR`.
- Custom-type read functions. `union_tag`, `union_extract`, and
  `struct_extract` carry resolved types and stable member indexes in HIR.
- Direct and nested struct/union dot access resolves database and table names
  first, then freezes member kind, index, container type, and result type in
  HIR. Function and dot syntax share one member resolver.
- Sequence writes. `nextval` and `setval` carry the resolved sequence,
  backing table, and optional `sqlite_sequence` table. `currval` stays an
  ordinary scalar call because it only reads connection state.
- Basic joins. Comma, plain, inner, and cross joins preserve source order and
  bind `ON` expressions against the complete FROM scope.
- Table-valued functions become `SourceKind::TableFunction` sources with a
  resolved catalog table and bound HIR arguments. Arguments use the complete
  FROM scope, including forward references, while aggregate/window calls and
  excess hidden-column arguments keep their binding-time errors.
- `USING` and `NATURAL` joins resolve both column expressions once, freeze
  comparison rules in HIR, and expose one merged unqualified column while
  keeping qualified columns available.
- Left, right, and full joins remain in lexical source order. Merged columns
  select the left value, right value, or a fact-aware coalesced value according
  to the preserved join kind; no planner-era right-join swap enters HIR.
- Compound SELECT arms become ordered query blocks with separate source scopes,
  aggregate identities, and window identities. `UNION`, `UNION ALL`, `EXCEPT`,
  and `INTERSECT` are preserved in HIR, while outward output identity stays with
  the first arm and width errors keep the existing diagnostic.
- Ordinary CTEs bind lazily from immutable AST references. Referenced CTEs own
  HIR queries and sources, forward sibling references and nested shadowing keep
  their existing rules, and unused invalid definitions never enter the closed
  document. Document-local `CteId` values are allocated on first use so the HIR
  arena contains no unreachable definitions.
- Derived `FROM` subqueries own child queries with lexical parents and become
  `SourceKind::Derived` sources. Derived sources and CTEs share the same
  first-arm output-to-column fact builder. Derived sources remain
  non-correlated at this checkpoint.
- Scalar and `EXISTS` expression subqueries own child queries with lexical
  parents. Outer names resolve through nested HIR scopes, scalar results keep
  the selected output facts, and each query derives its exact sorted capture
  set from completed HIR instead of mutable binding sidecars.
- Scalar and row-value `IN` query expressions keep the left expressions,
  negation, child query, and one frozen comparison rule per column in HIR.
  Correlated right sides capture outer sources, and row-width errors keep the
  existing diagnostic.
- `IN table` and `IN table(arguments)` normalize to the same membership-query
  HIR. The generated query selects visible source columns from a resolved
  table, CTE, or table function; arguments and CTE bodies keep exact captures.
- WHERE expressions become query-block filters. Source columns win over result
  aliases, aliases remain stable `OutputId` references, correlated subqueries
  keep their captures, and aggregate or window calls retain their existing
  errors.
- GROUP BY keys keep resolved expressions, type facts, and collations. Source
  columns win over aliases, positive integer terms become `OutputId` references,
  and grouping expressions cannot capture an enclosing query. HAVING prefers
  aliases, allows aggregates with block-local identities, and rejects windows.
- Query-level ORDER BY terms keep resolved expressions, direction, null order,
  type facts, and collations. Ordinary queries prefer output aliases, preserve
  output ordinals, correlated subqueries, and ORDER-BY-only function identities.
  Compound queries resolve ordinals and names from any arm to the first arm's
  stable `OutputId` values and keep their restricted-expression diagnostics.
- Query-level LIMIT and OFFSET expressions bind in an empty scalar scope. They
  keep parameters, scalar functions, DQS literals, and independent child
  queries while rejecting query names, correlation, aggregates, and windows.
  Both OFFSET spellings share the same normalized HIR shape.
- VALUES rows bind as block-owned HIR expressions. Generated `columnN` outputs
  keep first-row type, affinity, and collation facts; row expressions remain
  the only runtime values. VALUES subqueries keep outer captures, scalar DQS
  rules, aggregate/window identities, and compound-arm ordering.
- Recursive CTE seeds and recursive arms bind as separate HIR queries.
  Self-references become occurrence-local `RecursiveInput` sources, UNION
  operators and comparison collations stay explicit, and recursive
  aggregate/window and structure errors remain binding-time errors. Queue
  ORDER BY terms resolve to output positions with explicit collations, while
  LIMIT/OFFSET expressions use the same empty scalar scope as ordinary limits.
  Recursive-reference counting follows nested CTE scopes: shadowing definitions
  hide outer names, unused definitions contribute nothing, and referenced
  definitions contribute their body's recursive references. CTE queries nested
  in correlated subqueries inherit the enclosing query scope; recursive seeds
  and arms record exact outer-source captures while same-query sibling sources
  remain invisible.
- Built-in array table columns. `SourceColumn::type_fact` carries array rank and
  element type without adding semantic storage metadata beside the column.
- `ARRAY[...]`, `array(...)`, bracket subscripts, and `array_element(...)`
  normalize to dedicated array/subscript HIR. Constructors derive rank from
  their elements; typed column subscripts retain element type facts.
- Array utility calls keep resolved function identity and derive exact result
  categories. Mutators preserve or deepen array facts, concatenation merges
  ranks, shape-preserving calls retain declarations, and scalar utilities have
  fixed integer or text results.
- Custom binary operators freeze their two-argument function identity,
  direct-or-derived swap/negate behavior, and any literal encoder call. Same
  declared custom types and compatible literals use the operator; different
  types and incompatible literals keep normal SQL behavior.
- `INDEXED BY` resolves to a snapshot-bound index identity on the source, while
  `NOT INDEXED` remains an explicit source hint. Missing and wrong-table indexes
  fail during analysis.
- Custom-type table columns keep their resolved type chain and declaration
  parameters in HIR. Encode programs follow leaf-to-base order, decode programs
  reverse it, and custom arrays reuse element `TypeFact` without separate array
  storage metadata.
- Referenced virtual generated columns and short-record defaults become
  source-owned HIR expressions. A final required-column worklist follows
  generated-column dependencies transitively, while unused stored expressions
  remain `NotRequired`.
- Basic `INSERT ... VALUES` and `DEFAULT VALUES` statements become INSERT HIR.
  Target columns, duplicate-column write selection, rowid aliases, defaults,
  generated expressions, and row-width diagnostics are resolved during
  analysis.
- Scalar, EXISTS, and IN queries inside direct INSERT VALUES are root-owned
  queries with no INSERT-target scope or query parent. Their own FROM sources
  resolve normally, captures stay exact, and destination types remain attached
  to the containing VALUES expression.
- INSERT targets freeze every CHECK expression plus every ordinary,
  expression, and partial index in catalog order. Stable catalog identities
  and `IndexCoverage::Complete` make the required write metadata explicit.
- AUTOINCREMENT INSERT targets carry one `ResolvedAutoincrement` object with
  the resolved `sqlite_sequence` table and an optional MVCC sequence operation.
  The grouped shape cannot represent an allocator without its sequence table.
- INSERT trigger identities are grouped by the write that can fire them.
  Ordinary INSERT triggers keep catalog order; UPSERT UPDATE triggers include
  ordinary UPDATE triggers and matching `UPDATE OF` triggers. Trigger
  data-changing command programs and temp-schema catalog inputs remain later
  checkpoints.
- INSERT foreign keys freeze outgoing and incoming constraint identities,
  resolved child and parent positions, rowid or UNIQUE-index parent lookup,
  and the exact child scan source. Incoming generated child keys close their
  stored expressions against that scan source. Enforcement remains outside
  semantic analysis.
- INSERT target metadata is split by storage kind. B-tree targets own defaults,
  AUTOINCREMENT, triggers, and foreign keys; virtual targets cannot represent
  any of those fields and retain the existing VALUES or DEFAULT VALUES source
  rule.
- Plain `INSERT ... SELECT` sources retain their query identity and complete
  SELECT HIR. Destination width is checked during analysis, and nested query
  captures remain attached to the source query tree.
- Top-level INSERT WITH clauses use one lazy CTE scope across query sources,
  inline VALUES, UPSERT expressions, and RETURNING. Referenced ordinary and
  recursive CTEs enter the closed document, unused invalid definitions remain
  unbound, and failed analysis always removes the statement scope.
- Statement-level INSERT conflict resolution remains an exact parser enum in
  HIR, distinguishing ROLLBACK, ABORT, FAIL, IGNORE, REPLACE, and no override.
- Direct INSERT values and defaults carry their destination type through the
  iterative expression frames. `union_value` freezes the destination union and
  tag in HIR, and its value argument receives the selected variant type,
  including for nested unions.
- INSERT-SELECT passes destination types by output position into every SELECT
  and VALUES compound arm. Destination-aware functions therefore resolve the
  same way in direct VALUES and query sources without changing standalone
  SELECT rules.
- Simple INSERT VALUES sources stay as inline rows. Compound VALUES and VALUES
  with query-level decorations use query HIR, preserving compound blocks,
  ORDER BY/LIMIT, width checks, and destination types in every arm.
- Scalar INSERT RETURNING expressions bind against the target source and become
  root-owned HIR outputs. Unqualified and qualified stars, aliases, type facts,
  affinities, collations, and generated-column dependencies are resolved during
  analysis.
- RETURNING scalar, EXISTS, and IN subqueries are root-owned queries with no
  query parent and exact captures of the INSERT target. Query-owned expressions
  keep their lexical parent. INSERT-level WITH scopes remain alive through
  RETURNING so lazy CTE definitions keep one statement-local identity.
- Catch-all `ON CONFLICT DO NOTHING` clauses become ordered INSERT HIR without
  creating an EXCLUDED source.
- Targeted `DO NOTHING` clauses freeze bound conflict expressions, explicit
  collations, sort order, partial-index predicates, and the exact PRIMARY KEY
  or UNIQUE index match. Chained clauses retain parser order. `DO UPDATE`
  actions bind assignments and predicates against the current target row plus
  one root-owned, qualified-only EXCLUDED pseudo-source. Row assignments,
  duplicate targets, generated-column checks, destination types, and existing
  subquery restrictions keep their binding rules.
- Basic B-tree UPDATE statements become UPDATE HIR with distinct OLD table and
  NEW pseudo-source identities. SET and WHERE expressions read OLD values;
  generated columns, defaults, CHECK constraints, and complete index metadata
  close separately against both row identities. Row assignments, duplicate
  targets, rowid aliases, SET DEFAULT, array-setter composition, destination
  types, and correlated expression subqueries keep their binding rules. WITHOUT
  ROWID targets retain their existing unsupported diagnostic.
- UPDATE trigger identities retain catalog order and include ordinary UPDATE
  triggers plus only the `UPDATE OF` triggers whose named columns are assigned.
  Trigger data-changing command programs remain a later checkpoint.
- UPDATE foreign keys freeze outgoing constraints against the NEW row source
  and incoming constraints against separate child-table scan sources. Parent
  identities, column positions, UNIQUE indexes, and generated child keys use
  the same closed metadata as INSERT. Enforcement remains outside semantic
  analysis.
- UPDATE RETURNING expressions bind against the NEW row identity. Stars,
  output facts, aliases, collations, generated columns, and root-owned
  subqueries share INSERT's DML RETURNING path. The base table name remains
  visible when the UPDATE target has an alias; that alias is not visible.
- SELECT, INSERT, recursive CTE bodies, and UPDATE use one scoped CTE helper,
  which removes the statement scope on success or error and checks that nested
  analysis did not leak another scope. UPDATE WITH definitions remain lazy,
  reach SET, WHERE, and RETURNING subqueries, support recursive bodies, and do
  not shadow the catalog UPDATE target.
- UPDATE FROM reuses the SELECT source analyzer with root ownership. JOIN
  constraints see only FROM sources; SET and WHERE see the target and FROM at
  one resolution level; RETURNING still sees only the NEW target row. CTE and
  derived sources keep their resolved identities, and every root expression
  read participates in stored-column metadata closure.
- UPDATE target metadata is split by storage kind. B-tree targets own defaults,
  triggers, foreign keys, CHECKs, and complete index metadata; virtual targets
  cannot represent those fields. Virtual SET, WHERE, FROM, RETURNING, aliases,
  and rowid expressions use the ordinary UPDATE binding rules.
- Basic B-tree DELETE statements become DELETE HIR with one resolved target
  source. WHERE expressions use the target scope, including aliases, rowid,
  and correlated subqueries. Generated-column reads and every ordinary,
  expression, and partial index close against that target; `INDEXED BY` keeps
  its resolved catalog identity. Target metadata is split between B-tree and
  virtual variants from the start. WITHOUT ROWID targets keep their existing
  unsupported diagnostic.
- DELETE trigger identities retain catalog order and include only DELETE
  triggers for the target database. Trigger data-changing command programs
  remain a later checkpoint.
- DELETE foreign keys freeze outgoing constraints against the deleted target
  row and incoming constraints against separate child-table scan sources.
  Parent identities, column positions, UNIQUE indexes, and generated child keys
  use the same closed metadata as INSERT and UPDATE. Enforcement remains
  outside semantic analysis.
- DELETE RETURNING expressions bind against the deleted target row. Stars,
  target aliases, rowid, output facts, collations, generated columns, and
  root-owned subqueries retain the same resolved HIR rules as other DML
  RETURNING clauses.
- DELETE uses the shared scoped CTE helper across WHERE and RETURNING. Ordinary
  and recursive definitions bind lazily, unused invalid definitions remain
  absent, and a CTE named like the target does not shadow the catalog DELETE
  destination. The statement scope is removed on success or error.
- Virtual-table DELETE targets use the virtual metadata variant and cannot
  carry B-tree indexes, CHECKs, triggers, or foreign keys. WHERE, RETURNING,
  aliases, rowid, and WITH subqueries retain the ordinary DELETE binding rules.

The completed standalone SELECT expression checkpoints cover scalar
expressions, row and row-subquery operands in comparisons and `BETWEEN`, list
and query `IN`, and `MATCH`. `union_value` is resolved only in
destination-aware DML expressions.

The supported SELECT path now reaches ordinary non-recursive CTEs, derived
`FROM` sources, and parenthesized FROM groups, plus correlated scalar, `EXISTS`,
and `IN` query expressions. FROM groups preserve their nested joins, merged
column order, aliases, unaliased inner qualifiers, CTE reachability, and source
ownership without inventing a query. WHERE filters, GROUP BY keys, HAVING
predicates, query-level ORDER BY terms, and LIMIT/OFFSET and VALUES rows are
also bound. Recursive CTE core identity, arms, queue ORDER BY, LIMIT/OFFSET,
and nested CTE identity are bound. Correlation with enclosing queries is also
bound for ordinary and recursive CTE queries.

Scalar calls now deliberately normalize production-only syntax before entering
HIR. `DISTINCT` is discarded after its arguments are bound, and a `FILTER`
expression is bound but does not change scalar evaluation. Supported nullary
`function(*)` calls become zero-argument calls, and JSON object star calls
expand to name/value arguments from the visible source columns. Argument
`ORDER BY`, scalar `OVER`, unsupported star syntax, and wrong arity retain
their existing diagnostics. Aggregate and window metadata are unchanged.

Custom CAST targets now use ordinary affinity CAST rules when their supplied
parameter count does not match the resolved custom type. Correctly
parameterized custom array targets retain their frozen custom type chain,
schema programs, and array dimensions in HIR.

Stored custom-type programs now treat parameter disagreement inside a resolved
type chain as a catalog invariant error. User-visible CAST arity fallback is
handled before schema-program binding, so this internal path no longer reports
unsupported SELECT.

## Working rules

- Keep existing binding rules and diagnostics.
- Keep parser AST immutable; expression binding returns HIR.
- Successful analysis must produce a closed document that passes
  `HirDocument::validate()`.
- Prefer narrow vertical checkpoints over mechanically translating old binder
  modules.
- Before each code checkpoint, show the proposed shape and obtain approval.
- Test and commit each approved checkpoint separately with `jj`.
- Tests should assert resolved HIR and document validity, not AST mutation or
  `TableReferences` sidecars.
