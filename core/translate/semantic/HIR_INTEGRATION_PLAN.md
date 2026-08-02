# HIR integration plan

## Goal

`semantic::analyze` is the only module that turns parser syntax into resolved
SQL meaning. Physical planning and bytecode emission consume the resulting
`HirDocument`; they do not inspect parser names or perform live catalog
resolution.

The parser AST remains syntax-only. HIR is the only resolved expression
language. Runtime locations such as cursors, registers, result slots, and
subprograms are mapped from `SourceId`, `OutputId`, and `QueryId` by physical
planning and emission; they are never written into parser or HIR expressions.

Prepare performance is measured after the replacement works and passes its
correctness gates. Clone and lookup optimizations are out of scope unless they
are required for correctness or to keep the verification suite viable.

## Required dependency direction

```text
parser AST + SemanticContext
    -> semantic::analyze
    -> closed HirDocument
    -> physical plan carrying hir::Expr
    -> VDBE emission with scoped runtime bindings
```

The HIR path never receives `Resolver`. Any physical catalog facts are read by
resolved identity from the same immutable snapshot used by analysis. DDL and
control statements use a separate catalog context because their live catalog
operations are not SQL binding.

## Migration checklist

### 1. Compile and test the real semantic module

- [x] Include the complete `translate::semantic` module instead of compiling
      selected files through the property-test shim.
- [x] Promote stored `SchemaExpr` support out of `cfg(test)`.
- [x] Reject bound or runtime parser expressions as invalid analyzer input.
- [x] Run existing scope, HIR, CTE, DML, trigger, and stored-expression
      properties through the real module tree.
- [x] Add properties at the `semantic::analyze` interface.

### 2. Close correctness-critical HIR facts

- [x] Use one meaningful catalog snapshot for semantic analysis and physical
      planning, including per-database schema versions.
- [x] Record lexical query parents and exact captured source dependencies.
- [x] Validate that resolved catalog objects belong to the document snapshot.
- [x] Validate that every required generated, default, CHECK, index, and type
      program is present.
- [x] Capture the resolved trigger and foreign-key facts needed by DML
      compilation so lowering does not search schemas by name.

### 3. Plan and emit HIR directly

- [x] Physical plans carry `hir::Expr`; do not add a duplicate `PlanExpr`.
- [x] Map `SourceId` to physical sources and cursors in scoped runtime state.
- [x] Map `OutputId` to projection/result locations in scoped runtime state.
- [x] Map `QueryId` to subquery plans and destinations in scoped runtime state.
- [ ] Express index-method coverage, window inputs, unnesting, and consumed
      predicates as physical metadata instead of synthetic expressions.
- [x] Prove with a poisoned catalog that planning and emission cannot resolve
      tables, functions, collations, or types by name.

### 4. Switch complete roots

- [ ] SELECT, compound queries, subqueries, CTEs, recursive CTEs, aggregates,
      and windows.
  - [x] Basic SELECT/VALUES scans, inner joins, derived sources, ordinary CTEs,
        correlated scalar/EXISTS/IN subqueries, UNION ALL, DISTINCT, ORDER BY,
        and LIMIT/OFFSET emit directly from HIR.
  - [x] Aggregate and window calls have stable block-local HIR identities;
        physical plans borrow the original calls and runtime bindings map the
        identities to registers.
  - [x] Ungrouped `sum`, `total`, `avg`, `count(expr)`, and `count(*)` emit
        directly from HIR, including WHERE and aggregate FILTER inputs.
  - [x] Grouped core aggregates emit from frozen HIR grouping type/collation
        facts and preserve source and aggregate identities through sorter
        materialization.
  - [ ] Ordered, DISTINCT, comparison-based, custom, and external aggregates.
  - [x] Binary `UNION`, `INTERSECT`, and `EXCEPT` use temporary set indexes
        whose equality collations come from the left HIR outputs.
  - [x] Mixed and multi-arm duplicate-removing compounds preserve SQLite's
        left-to-right set boundaries, including trailing UNION ALL duplicates.
  - [x] LEFT JOIN preserves the separate HIR join constraint and WHERE phases
        and null-extends the right SourceId only when no join match exists.
  - [ ] Window execution, RIGHT/FULL OUTER JOIN, and the remaining compound
        combinations.
  - [x] Recursive CTE seeds and arms feed a frozen-HIR priority queue, bind
        occurrence-local recursive inputs, and apply UNION seen-row equality.
  - [x] Table-function arguments and hidden-column inputs emit from frozen HIR.
  - [x] Two-source RIGHT and FULL joins preserve the original HIR sides and
        evaluate WHERE only after unmatched rows are null-extended.
- [ ] UPDATE and DELETE, including FROM, RETURNING, triggers, and foreign keys.
  - [x] A catalog-free root dispatcher emits simple rowid B-tree DELETE directly
        from closed HIR and rejects every unimplemented write obligation before
        opening the write cursor.
  - [x] Simple rowid B-tree UPDATE uses a stable HIR rowid set, evaluates
        assignments against OLD, and rebuilds the complete NEW row through the
        shared row-image layer.
  - [x] Ordinary, expression, partial, and UNIQUE secondary indexes preserve
        SQLite's prepare-before-write mutation order through one shared HIR
        index layer.
  - [x] RETURNING reads OLD before DELETE and the complete written NEW row
        after UPDATE, using the same HIR runtime bindings as the write.
  - [x] UPDATE FROM evaluates predicates and assignments while target and FROM
        SourceIds are live, then materializes one stable value row per target
        rowid before the write phase.
  - [x] UPDATE child-key changes remove deferred OLD violations and validate
        the complete NEW row through frozen parent table/index identities.
  - [ ] Triggers and foreign keys.
- [ ] INSERT, VALUES, INSERT SELECT, UPSERT, excluded, defaults, and RETURNING.
  - [x] VALUES and DEFAULT VALUES build supplied fields, frozen defaults, and
        generated columns through the shared row-image layer for simple rowid
        B-tree targets.
  - [x] CHECK and UNIQUE constraints run against the complete frozen-HIR row
        before any mutation.
  - [x] Explicit rowid and INTEGER PRIMARY KEY inputs share one generated or
        validated table-key path with pre-write uniqueness checks.
  - [x] Default NOT NULL enforcement and STRICT built-in type checks run on the
        complete NEW row before any mutation.
  - [x] INSERT SELECT materializes direct HIR query output before opening the
        target write loop, including safe self-inserts.
  - [x] RETURNING projects the written NEW HIR row after INSERT.
  - [x] ABORT, FAIL, ROLLBACK, and IGNORE route NOT NULL, CHECK, rowid, and
        UNIQUE failures before mutation using the statement or index policy.
  - [x] UPSERT DO NOTHING routes resolved rowid, UNIQUE-index, and catch-all
        conflict targets around the complete row write.
  - [x] UPSERT DO UPDATE keeps the conflicting target and completed excluded
        row as separate SourceId bindings, rebuilds generated/constraint/index
        programs under ABORT semantics, and returns the written NEW row.
  - [x] INSERT OR REPLACE deletes all OLD index entries and the conflicting
        row before the NEW write for rowid and UNIQUE conflicts; NOT NULL uses
        its frozen default and falls back to ABORT when the default is NULL.
  - [x] AUTOINCREMENT freezes the resolved sqlite_sequence table in HIR and
        maintains its high-water mark without a catalog lookup during emission.
  - [x] INSERT probes frozen rowid or UNIQUE parent identities for child keys,
        accepts NEW self-references, and repairs deferred counters when a new
        parent key satisfies existing child rows.
- [x] Trigger commands and predicates with explicit OLD/NEW runtime bindings.
- [ ] Generated columns, defaults, CHECK constraints, expression and partial
      indexes, and custom-type schema programs.
  - [x] Generated/default reads and generated/default DML row construction use
        only frozen HIR expressions, including logical-to-physical record
        mapping for virtual columns.
  - [x] CHECK constraints plus ordinary, expression, partial, and UNIQUE index
        programs execute only from frozen HIR metadata.

One syntax root uses exactly one semantic implementation. Falling back after a
semantic error is forbidden because it would hide analyzer defects.

### 5. Delete the old representation

- [ ] Remove every production use of `bind_and_rewrite_expr` and
      `BindingBehavior`, then delete `expr/binding.rs`.
- [ ] Remove optimizer, index, integrity-check, stored-expression, DML, UPSERT,
      RETURNING, and trigger mini-binders.
- [ ] Remove semantic lookup methods and fields from `Resolver`; keep physical
      emission state in an emission context and DDL catalog work in a DDL
      context.
- [ ] Remove parser `Expr::Column`, `Expr::RowId`, `Expr::SubqueryResult`,
      `FieldAccess.resolved`, `FieldAccessResolution`, `SELF_TABLE`, and
      binding-owned `TableInternalId` uses.
- [ ] Move incremental compilation's `Expr::Register` into its own IR, then
      remove the runtime variant from the parser AST.
- [ ] Remove AST mutation that replaces trigger, UPSERT, window, index-method,
      or subquery expressions with execution locations.

## Correctness gates

- Every successful analysis validates as a closed HIR document.
- Analysis does not mutate parser input.
- Equivalent syntax and catalog snapshots produce equivalent HIR meaning.
- Invalid SQL returns a SQL error, never a panic or unexplained internal error.
- Every valid generated HIR document can be physically planned and emitted.
- Planning and emission work when all name-resolution operations are poisoned.
- Temporary old/new comparison checks resolved facts, errors, and execution
  results; it is never enabled in normal prepare.
- SQLite differential coverage includes aliases, rowid, joins, correlation,
  CTEs, DML, UPSERT, triggers, and stored expressions.
- Raw SQL, structured catalog/AST, trigger-runtime, and stored-expression
  fuzzers run without panics or invalid HIR.

## Mechanical deletion gates

Before declaring the migration complete, these searches must find no
production use in the migrated paths:

```text
rg "bind_and_rewrite_expr|BindingBehavior" core
rg "Expr::Column|Expr::RowId|Expr::SubqueryResult" core sqlite/parser
rg "SELF_TABLE|FieldAccessResolution" core sqlite/parser
```
