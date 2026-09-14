# Logical Plan and Optimization

Implement a bound relational logical plan for Turso, integrate it into query compilation, implement general query unnesting, and introduce a declarative language for optimizer transformations. Complete the implementation, integration, tests, and benchmarks across the milestones below.

The finished system must preserve SQL behavior and be at least as fast as the current system on the existing workloads. Improved execution of difficult subqueries must not hide regressions in ordinary query preparation.

## References and Existing Code

Read both papers in the project root:

- `Neumann-Unnesting-1.pdf`: *Unnesting Arbitrary Queries*, Thomas Neumann and Alfons Kemper.
- `Neumann-Unnesting-2.pdf`: *A Formalization of Top-Down Unnesting*, Thomas Neumann.

Use the papers to establish operator semantics and rewrite preconditions. Consult their algorithm references where necessary, and explain any adaptations needed for Turso's SQL semantics.

Inventory the existing implementation before designing its replacement:

- `core/translate/plan.rs`, `select.rs`, `planner.rs`, and `subquery.rs`: binding, query representation, and compilation.
- `core/translate/optimizer/`: logical transformations, unnesting, costing, access paths, and join ordering.
- `core/translate/logical.rs` and `core/incremental/`: the logical plan used by incremental views and its consumers.
- `testing/differential-oracle/`: query generation, SQLite comparisons, and forced-versus-disabled unnesting checks.
- `core/benches/prepare_benchmark.rs`, the other prepare benchmarks, and the CodSpeed workflow.
- The existing `EXPLAIN QUERY PLAN FORMAT=JSON` interface and its tests.

Use [CockroachDB's optimizer](https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt) and [Optgen](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/optgen/lang/doc.go) as references for declarative rules and code generation. Validate each rule's applicability to Turso independently.

## Architecture and Scope

Implement this compilation boundary:

```text
SQL AST -> binding -> logical plan -> logical rewrites -> physical planning -> VDBE
```

The logical plan must be the representation through which migrated queries are optimized and compiled. Every operator produced by an enabled rewrite must have an executable lowering path.

Define the representation and its invariants before expanding the rule set:

- Bind names to stable relation and column identities. Represent scalar expressions and references to outer scopes explicitly.
- Preserve name resolution, result column names and metadata, and parameter slot behavior through binding and rewriting.
- Define each operator's inputs, outputs, and SQL semantics, including duplicate preservation and ordering requirements.
- Derive the properties needed to validate rewrites: output columns, outer references, nullability, uniqueness, affinity, collation, and relevant function behavior, including nondeterminism and possible errors.
- Represent dependent joins and the subquery forms needed for decorrelation explicitly.
- Support shared CTE and unnesting inputs through explicit references. Define recursive CTE behavior without requiring recursive expansion of the plan.
- Keep registers, cursors, bytecode labels, and `ProgramBuilder` state out of logical rewriting. Assign execution resources during lowering and emission.
- Validate plan invariants after construction and rewrite passes in tests and appropriate debug configurations.

Write a short design document explaining how transformed relational trees reach the existing join optimizer and bytecode emitter. Identify required extensions for joins of subplans, aggregates, shared inputs, and subquery results. Preserve and reuse the existing physical planner where practical; justify any replacement with correctness and performance evidence.

Decide explicitly whether the logical plan for incremental views will be extended, replaced, or kept separate. Describe the migration of its consumers or the reason for retaining a separate representation.

Maintain a scope matrix covering currently supported SELECT forms, SELECTs and subqueries used by DML, RETURNING, views, CTEs, virtual tables, and the supported SQL dialects. Record how each entry is bound, represented, optimized, lowered, and tested. The first milestone may cover a bounded subset; full integration must preserve existing supported behavior and account explicitly for every remaining legacy path.

## General Query Unnesting

Implement the general unnesting approach described in the papers, including deeply nested and non-equality correlations. Use the later formalization to guide correctness and avoid repeatedly processing nested query fragments where the top-down algorithm avoids that work.

Define a coverage matrix across subquery forms, expression positions, and relational operators. Include:

- `EXISTS`, `NOT EXISTS`, `IN`, `NOT IN`, scalar subqueries, and supported row-value subqueries.
- References to immediate and more distant outer scopes, multiple referenced columns, and nested dependent joins.
- Equality, inequality, disjunction, and other supported correlation predicates.
- Projection, filtering, aggregation, HAVING, DISTINCT, inner and outer joins, semi-joins and anti-joins, and set operations.
- Empty inputs, duplicate rows, NULL correlation values, and groups with no matching input.
- ORDER BY, LIMIT/OFFSET, window functions, CTEs, and supported subquery positions within larger expressions.

Preserve the distinct set of outer bindings used by the algorithm while retaining the query's duplicate rows. Implement the required NULL-aware binding comparisons. Preserve aggregate results for empty inputs and scalar-subquery behavior: under SQLite semantics, a scalar subquery returns its first row, or NULL when there are no rows. Preserve required ordering when determining that row.

Treat affinity, collation, nondeterministic functions, short-circuit evaluation, and expressions that can fail as rewrite correctness requirements. Do not assume that evaluating an expression earlier, more often, or for additional rows is valid. Keep dialect-specific behavior explicit.

For each matrix entry, record the applicable rules, preconditions, tests, and implementation status. Distinguish temporary implementation gaps from semantic cases that require dependent evaluation. An unimplemented entry remains outstanding work; a conservative fallback must not count as successful decorrelation.

Separate the ability to decorrelate a query from the physical choice of how to execute it. Preserve cost-based selection of indexed correlated execution when it is cheaper. Test both the decorrelated form and the form selected automatically.

## Declarative Optimization Rules

First validate the logical representation with existing Turso transformations and a representative decorrelation case. Then implement a small DSL based on the requirements demonstrated by those rules, and use it for the broader rule migration.

The DSL must support typed operator patterns, named bindings, replacement construction, semantic preconditions, and Rust functions for checks or construction that are better expressed in Rust. Generate executable Rust code as part of the build. Provide useful diagnostics for invalid rule definitions.

Distinguish normalization rules from rules that generate equivalent alternatives for costing. Define traversal order, rule priorities, pass boundaries, termination behavior, and limits on rewrite work and plan growth. Preserve a valid executable plan when an optimization budget is exhausted, and expose that outcome in diagnostics.

Create a named inventory of Turso's current heuristic transformations and migrate applicable logical transformations into declarative rule files. Record the reason for each transformation that remains procedural. Keep costing, access-path selection, and bytecode emission in their appropriate components.

Create a finite, named inventory of Cockroach-derived rule candidates before implementing them. For each candidate, record its source, purpose, required properties, SQLite or dialect-specific preconditions, tests, and benchmark. State which candidates are included and why the others are inapplicable or deferred. Implement the included inventory and update it as evidence changes; make the final scope explicit.

## Plan Inspection and Debugging

Extend the existing EXPLAIN JSON interface to expose logical plans before and after rewriting, as well as the selected physical plan. Define the syntax and output schema explicitly.

The logical output must include structured operators, expressions, inputs, output columns, outer references, and references to shared subplans. Use deterministic identifiers and a versioned format suitable for tests and tools.

Add opt-in rule tracing or counters that identify applied rules, remaining dependencies, fallback reasons, and exhausted optimization budgets. Keep serialization and tracing work off the ordinary prepare path when inspection is disabled.

## Correctness and Differential Testing

Extend the narrowest existing test harness that can express each requirement. Follow the repository's AGENTS.md and testing instructions.

- Add SQL result coverage to the existing SQLite `.sqltest` corpus. Use Turso-specific tests for inspection syntax and other behavior SQLite does not expose.
- Assert relevant logical structure before and after rewriting and the selected execution plan. Combine focused structural assertions with selected deterministic snapshots.
- Add targeted cases where each rule applies and where its preconditions prevent it from applying. Test interactions between rules and nested query scopes.
- Extend the existing differential fuzzer and query generator with bounded but configurable nesting and operator combinations.
- Compare the original query against SQLite, the existing Turso compilation path while it remains available, and the new path. Retain forced and disabled transformation modes for ongoing checks where both forms are executable.
- Generate independently constructed joined equivalents for additional comparisons. Validate those equivalents against SQLite; they must not be the only source of expected results.
- Compare row multiplicities, NULLs, relevant value types, and ordered results where SQL guarantees order. Handle nondeterministic results, ties, and unordered LIMIT explicitly so they do not produce false mismatches.
- Check success versus failure and relevant error behavior. Keep deterministic cases that detect invalid changes to expression evaluation.
- Preserve reproducible seeds, shrinking, and minimized regressions. Report rule and operator coverage, successful rewrites, and skipped or fallback cases separately. Verify that transformation comparisons exercise different plans.

Run the appropriate conformance, integration, snapshot, formatting, and lint checks for the changed components. Complete broader required checks as the new path becomes the default.

## Performance

Before implementation, record the baseline commit and establish a reproducible measurement protocol. Specify the compiler, features, build profile, hardware, data sets, statistics, benchmark commands, and a Linux environment for Callgrind. Measure baseline repeatability and define per-benchmark acceptance criteria before making changes.

Use Callgrind together with the existing prepare CodSpeed benchmarks and native wall-time measurements. For existing prepare workloads, target no increase in measured prepare instruction counts and no reproducible native wall-time regression beyond established measurement uncertainty. Keep any numerical tolerances fixed throughout the comparison.

Measure:

- Complete preparation, including parsing, binding, logical rewriting, physical planning, and emission.
- Focused rewrite costs where useful for explaining a regression.
- Query execution using already-prepared statements, with setup outside the measured interval and results fully consumed.
- Common point lookups, parameterized queries, joins, writes, and the existing prepare corpus.
- Scaling with nesting depth, join count, expression size, projection width, and shared subplan references.
- Execution across outer cardinalities, distinct correlation values, duplicate rates, NULLs, index availability, selectivity, and data distributions.
- Cases where decorrelation helps and cases where indexed correlated execution should remain cheaper.

Compare both automatic plan selection and forced executable alternatives. Include representative queries from the papers, existing unnesting cases, and newly supported query classes.

Retain raw measurements and report before/after results per workload, including the largest regressions. Aggregate gains do not compensate for slower ordinary prepares. Investigate and resolve reproducible regressions; report any remaining failures as unfinished work.

## Design and Version Control

Use focused modules with explicit interfaces for binding, logical operators and properties, transformations, rule generation, physical lowering, and inspection. Reuse existing abstractions where they fit and justify new ones through actual requirements.

Produce atomic commits containing small, reviewable units with their relevant validation. Keep the branch buildable and testable between milestones. Rebase when needed to maintain a coherent commit sequence.

Do not use worktrees. You may check out other branches or commits for baseline measurements, but perform all implementation work on `logical-plan-codex`. Preserve existing user changes when switching revisions. Follow repository instructions, including the restriction on release builds.

Temporary fallback to existing compilation is allowed during migration. Track its use, define when each fallback will be removed, and remove obsolete implementations after their replacements satisfy the correctness and performance criteria.

## Milestones and Completion Criteria

1. **Architecture and baseline:** Produce the design, scope and rule inventories, operator contracts, migration sequence, and reproducible baseline measurements. Resolve routine design choices and continue into implementation.
2. **Executable logical plan:** Compile a representative query subset through binding, the new logical representation, at least one meaningful transformation, physical lowering, and execution. Include a correlated case, shared-input handling, JSON inspection, result checks, and baseline performance comparisons.
3. **Integration and existing transformations:** Expand representation and lowering across the scope matrix, preserve dialect behavior and incremental-view consumers, and migrate existing transformations with parity tests. Account for every remaining legacy path.
4. **General unnesting:** Implement the claimed query classes and their executable lowering, demonstrate the required removal of logical dependencies, and complete differential coverage and execution benchmarks. Report justified semantic exceptions separately from implementation gaps.
5. **DSL and rule expansion:** Generate and use declarative rules for the applicable Turso transformations, implement the selected Cockroach-derived inventory, and validate rule interactions and prepare costs. Begin DSL development after milestone 2 establishes concrete requirements; use it during later migration where practical.
6. **Final validation and cleanup:** Complete the required checks, compare final performance with the original baseline, remove completed migration fallbacks, and provide a concise report of implementation scope, tests, fuzz coverage, measurements, and any outstanding limitations.

Each implementation milestone must produce executable, reviewable changes and evidence for its completion criteria. The final deliverables are the integrated logical plan, machine-readable inspection, general unnesting, the declarative optimizer rules, reproducible correctness and performance validation, and the atomic commit sequence.
