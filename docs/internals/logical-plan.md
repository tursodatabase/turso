# Logical plan: design and build plan

Status: proposal, September 2026.
Tracks [issue #1615](https://github.com/tursodatabase/turso/issues/1615).

## Summary

Turso compiles a `SELECT` from the AST straight into `SelectPlan`.
`SelectPlan` is one flat query block, and the optimizer writes its physical
decisions into the same struct. There is no tree of relational operators
between the AST and the physical plan. As a result, query rewrites are spread
over three layers, and some standard rewrites do not exist at all.

This document proposes a logical plan: a tree of operators (`Scan`, `Filter`,
`Project`, `Join`, `Aggregate`, `Sort`, `Limit`, `SetOp`, and more) built from
the bound AST. Rules rewrite the tree. A lowering step turns the tree into
today's `SelectPlan`. The join optimizer and the bytecode emitter do not
change.

The strategy is additive. The new stage is built next to the current path,
behind a connection setting. When it reaches parity on the full test corpus,
it becomes the default and the old path is deleted. Only then do rewrites move
into rules. This is the strategy that issue #1615 asks for.

## 1. The pipeline today

`translate_select` in `core/translate/select.rs` runs three steps:

1. `prepare_select_plan` builds a `SelectPlan` from the AST.
2. `optimize_plan` in `core/translate/optimizer/mod.rs` rewrites the plan
   and chooses the table order and the access methods.
3. `emit_program` in `core/translate/emitter/mod.rs` writes bytecode.

### 1.1 The binder and the planner are one function

`prepare_one_select_plan` in `select.rs` is about 650 lines. It does all of
this in one pass:

- It resolves names in place in the AST. `Expr::Id` becomes
  `Expr::Column { table: TableInternalId, column }`.
- It expands `*` and `t.*`.
- It collects aggregate calls and window calls into side lists.
- It splits `WHERE` and `ON` at `AND` into `WhereTerm` values. An `ON` term
  of an outer join records the right table in `from_outer_join`.
- It plans FROM subqueries, CTEs, and views as `Table::FromClauseSubquery`,
  each with its own `Plan` inside.
- It plans scalar, `IN`, and `EXISTS` subqueries into
  `NonFromClauseSubquery` and replaces the expression with
  `Expr::SubqueryResult { subquery_id }`.
- It rewrites a query with more than one window into nested subqueries
  (`core/translate/window.rs`).
- It validates column counts, misuse of aggregates, `HAVING` without
  aggregates, and more.

### 1.2 `SelectPlan` is one query block

`SelectPlan` in `core/translate/plan.rs` has 20 fields: `table_references`,
`join_order`, `result_columns`, `where_clause`, `group_by`, `order_by`,
`aggregates`, `limit`, `offset`, `distinctness`, `values`, `window`,
`non_from_clause_subqueries`, and fields that the optimizer fills later
(`estimated_cost`, `estimated_output_rows`, `simple_aggregate`, and the
`Operation` of each table). Seven files build a `SelectPlan` by hand.

Nested queries are attached to the block. `Table::FromClauseSubquery` holds a
`Box<Plan>`, and `NonFromClauseSubquery` holds another. So the shape is a tree
of blocks, but the block itself is flat. A rewrite that changes the shape must
edit many fields at once.

### 1.3 Rewrites live in three layers

| Layer | Rewrite | Location |
|---|---|---|
| AST | `BETWEEN` to two comparisons | `planner.rs`, `rewrite_between_exprs` |
| AST | View expansion, trigger subprogram rewrites | `planner.rs`, `trigger_exec.rs` |
| `SelectPlan` | Split windows into nested subqueries | `window.rs` |
| `SelectPlan` | Constant `WHERE` terms | `optimizer/mod.rs`, `eliminate_constant_conditions` |
| `SelectPlan` | Lift shared `AND` terms out of `OR` | `optimizer/lift_common_subexpressions.rs` |
| `SelectPlan` | `LEFT JOIN` to inner join | `optimizer/mod.rs`, `where_term_is_null_rejecting_for_table` |
| `SelectPlan` | Subquery unnesting to semi, anti, and group joins | `optimizer/unnest.rs` |
| `SelectPlan` | `MATCH` to an FTS index method | `optimizer/mod.rs`, `transform_match_to_fts_match` |
| After join search | Sort elimination, simple aggregates | `optimizer/order.rs`, `detect_simple_aggregate` |

The unnesting rewrite needs a cost comparison between two forms of the query.
Because there is no tree, it clones the whole `SelectPlan`, plans both copies,
and keeps one (`optimize_select_plan_with_cache`).

### 1.4 Planning depends on the bytecode builder

The planner takes `&mut ProgramBuilder`. It uses the builder for:

- `TableInternalId` values from `program.table_reference_counter`.
- Result registers for subqueries (`alloc_register`) and cursors for
  ephemeral indexes (`alloc_cursor_id`), in `core/translate/subquery.rs`.
- CTE bookkeeping (`alloc_cte_id`, `push_cte_being_defined`,
  `get_materialized_cte`).

`optimize_plan` compares the reserved resources before and after its
rewrites, and releases the ones that a removed subquery held. A plan cannot be
built or tested without a `ProgramBuilder`.

### 1.5 A second logical plan already exists

`core/translate/logical.rs` (4,200 lines) defines a DataFusion-style
`LogicalPlan` and `LogicalExpr`, with its own builder and its own name
resolution by string. Only the DBSP compiler for materialized views uses it
(`core/incremental/`). It does not support windows or table-valued functions.
It came from [PR #2786](https://github.com/tursodatabase/turso/pull/2786).
Its column references are names, not `TableInternalId` values, so the join
optimizer and the emitter cannot consume it.

## 2. What this costs today

### 2.1 Missing rewrites

The plans that follow come from this branch (`target/debug/tursodb`) and
from SQLite 3.45.1:

```sql
CREATE TABLE t1(a, b);
CREATE INDEX i1 ON t1(a);
CREATE VIEW v1 AS SELECT a, b FROM t1;
```

| Query | Turso | SQLite |
|---|---|---|
| `SELECT * FROM (SELECT a, b FROM t1) s WHERE s.a = 5` | `SCAN s` over `SCAN t1` | `SEARCH t1 USING INDEX i1 (a=?)` |
| `WITH c AS (SELECT a, b FROM t1) SELECT * FROM c WHERE a = 5` | `SCAN c` over `SCAN t1` | `SEARCH t1 USING INDEX i1 (a=?)` |
| `SELECT * FROM v1 WHERE a = 5` | `SCAN v1` over `SCAN t1` | `SEARCH t1 USING INDEX i1 (a=?)` |

SQLite merges the subquery into the outer query (the flattener in
`select.c`) and pushes the filter down to the table. Turso has neither
rewrite. Both are simple to state as tree rules and hard to state on
`SelectPlan`. ORMs and views produce this query shape often.

### 2.2 Every SQL feature edits the same function

A new clause or a new subquery position changes `prepare_one_select_plan`,
`SelectPlan`, the optimizer, and the emitter together. There is no layer where
a change is local.

### 2.3 No plan-level tests

A rewrite cannot be tested without a database and a `ProgramBuilder`. The
tests in `optimizer/mod.rs` build a `Resolver` from an empty schema and test
single helper functions, not plans.

### 2.4 Two binders can disagree

A materialized view is bound by `logical.rs`. The same `SELECT` run directly
is bound by `select.rs`. Each difference between them is a bug.

## 3. Goals and non-goals

Goals:

- A tree of relational operators for `SELECT`, later for DML.
- Rewrites as rules, each with plan-level tests.
- Planning that does not need a `ProgramBuilder`.
- One binder for the VDBE path and the DBSP path.
- No increase in statement preparation time.
- No behavior change without a test.

Non-goals for this plan:

- Replacing the join optimizer (`optimizer/join.rs`) or the emitter.
- A memo-based (Cascades) optimizer.
- A new expression type that replaces `ast::Expr`. See decision D2.

## 4. Design

### 4.1 Position in the pipeline

```text
ast::Select
   |  bind                    core/translate/logical/build.rs
   v
LogicalPlan  --rules-->  LogicalPlan     core/translate/logical/rules/
   |  lower                   core/translate/logical/lower.rs
   v
Plan / SelectPlan        physical plan, unchanged
   |  optimize_plan           join order, access methods, sort elimination
   |  emit_program            bytecode
   v
Program
```

`SelectPlan` stays. It becomes the physical plan and the only input of the
emitter. Nothing below the lowering changes in phases 0 to 3.

### 4.2 Nodes

This is a sketch. The field lists are not final.

```rust
pub enum LogicalPlan {
    Scan(Scan),                 // base table or virtual table, owns one TableInternalId
    OneRow,                     // SELECT without FROM
    Values(Values),             // VALUES (...), (...)
    Filter(Filter),             // conjuncts: Vec<Expr>
    Project(Project),           // Vec<ResultSetColumn>
    Aggregate(Aggregate),       // group_by: Vec<Expr>, aggregates: Vec<plan::Aggregate>
    Window(Window),             // one plan::Window
    Join(Join),                 // left, right, kind, on: Vec<Expr>, using
    Sort(Sort),                 // keys with direction and NULLS order
    Limit(Limit),               // limit, offset
    Distinct(Distinct),
    SetOp(SetOp),               // UNION, UNION ALL, INTERSECT, EXCEPT
    DerivedTable(DerivedTable), // input, TableInternalId, columns, CTE metadata
    RecursiveCte(RecursiveCte),
}

pub enum JoinKind { Inner, Cross, Left, Right, Full, Semi, Anti }
```

Each node gives `inputs()`, `inputs_mut()`, and `output_columns()`. The tree
owns its children with `Box`. A rule takes the tree by value and gives it
back, so a rule never clones a subtree that it does not change.

`HAVING` is a `Filter` above an `Aggregate`. `Semi` and `Anti` exist for the
unnesting rule. `Right` exists so that the tree keeps the shape the user
wrote. The lowering swaps it into a `Left` join, as the binder does today.

### 4.3 Decisions

**D1. Column references keep `Expr::Column { table: TableInternalId, column }`.**
Ids are unique in one statement. A node can move anywhere in the tree, and no
expression changes. This is the model of PostgreSQL (`Var` with `varno`), and
Turso already uses it. Position-based references (Calcite `RexInputRef`)
need renumbering after every rewrite.

**D2. Expressions stay bound `ast::Expr` in phases 0 to 3.**
The expression compiler (`expr/translator.rs`, 3,145 lines), the constraint
extraction, the cost model, the order analysis, and the emitter all consume
`ast::Expr`. A new expression type is a separate project (the second half of
issue #1615). The tree touches expressions only through a few helpers: walk,
referenced tables, equivalence, and rewrite. That keeps the later swap local.

**D3. Scalar, `IN`, and `EXISTS` subqueries stay expressions.**
The expression refers to a side plan by id (`Expr::SubqueryResult`), as today.
The node that owns the expression owns the list of subqueries. The unnesting
rule turns a subquery into a `Join` with kind `Semi`, `Anti`, or `Left`, which
is what `unnest.rs` does on `SelectPlan` today.

**D4. `DerivedTable` is the scope boundary.**
It is `Table::FromClauseSubquery` today: a FROM subquery, a CTE reference, or a
view. The lowering also uses it for a subtree that does not fit one query
block. So every tree can be lowered.

**D5. Ids come from the planner, not from the bytecode builder.**
The builder takes `&mut TableRefIdCounter`, not `&mut ProgramBuilder`.
Registers and cursors are allocated in the lowering or in the emitter, never
in the builder or in a rule. A rule can then create and delete nodes without a
resource leak.

**D6. Rules are not cost-based.**
A rule applies when its result is always at least as good. Cost-based choices
stay in `optimize_plan`. When two forms need a cost comparison, as with
unnesting, the lowering gives both forms to the physical planner, which picks
one, as today.

### 4.4 Bind

`build_select(select, scope, resolver, ids) -> Result<LogicalPlan>` reuses
`bind_and_rewrite_expr`, `select_star`, `resolve_window_and_aggregate_functions`,
`parse_limit`, and the validation functions of `select.rs`. Most of it is
moved code: the same steps in the same order, but each step adds a node
instead of filling a `SelectPlan` field. Semantic errors ("misuse of
aggregate", "no such column") stay in the binder, so error messages do not
change.

### 4.5 Rules

```rust
pub trait Rule {
    fn name(&self) -> &'static str;
    fn apply(&self, plan: LogicalPlan, ctx: &mut RuleContext) -> Result<Transformed>;
}

pub enum Transformed {
    Changed(LogicalPlan),
    Same(LogicalPlan),
}
```

- The rule list is fixed and ordered. Each round applies every rule bottom-up
  once. A round that changed the plan starts another round. The number of
  rounds has a limit, and a plan that reaches the limit fails an assertion.
- `RuleContext` gives the schema, the resolver, and the id counter.
- A rule is one file under `core/translate/logical/rules/`, with its tests in
  the same file.
- Traversal is iterative with an explicit stack, like `walk_expr` in
  `expr/walk.rs`. SQLite permits compound selects with 500 arms
  (`SQLITE_MAX_COMPOUND_SELECT`), and recursion over such a chain can overflow
  a small stack.

### 4.6 Lower

`lower(plan, query_destination, ...) -> Result<Plan>` extracts query blocks.
From the root down, it maps each node to a field:

| Node | `SelectPlan` field |
|---|---|
| `Limit` | `limit`, `offset` |
| `Sort` | `order_by` |
| `Distinct` | `distinctness` |
| `Project` | `result_columns` |
| `Filter` above `Aggregate` | `group_by.having` |
| `Aggregate` | `group_by`, `aggregates` |
| `Window` | `window` |
| `Filter` | `where_clause` terms |
| `Join` tree | `table_references`, `JoinInfo`, `from_outer_join` on `ON` terms of outer joins, `join_order` in written order |
| `Scan` | `JoinedTable` with the default `Operation` |
| `DerivedTable` | `Table::FromClauseSubquery` with a lowered inner `Plan` |
| `SetOp` chain | `Plan::CompoundSelect` |
| `RecursiveCte` | `Plan::RecursiveCte` |

A node in a position that the block cannot hold closes the current block. For
example, a `Filter` above a `Project` above a `Limit` turns the subtree below
the `Filter` into a `DerivedTable`. This is what `window.rs` does by hand
today.

For a query that no rule changed, the lowering output must equal today's
`SelectPlan`. That is the parity requirement of phase 1.

### 4.7 Text form

`Display` for `LogicalPlan` prints an indented tree:

```text
Project [s.a, s.b]
  Filter [s.a = 5]
    DerivedTable s
      Project [t1.a, t1.b]
        Scan t1
```

The text form is the unit of plan tests. `optimize_plan` also writes it with
`tracing::debug!`, next to the existing `plan_sql` output. `EXPLAIN QUERY PLAN`
keeps its meaning: the physical plan. A `FORMAT=LOGICAL` option can follow the
`FORMAT=JSON` precedent later, if the team wants the tree in SQL.

## 5. Phases

Each phase merges to `main` in small PRs. There is no long-lived branch,
because `core/translate` changes many times a week.

### Phase 0: skeleton (one PR, no behavior change)

- Move `core/translate/logical.rs` to `core/incremental/logical_plan.rs`. It
  is private to DBSP until phase 4.
- Add `core/translate/logical/` with `mod.rs` (nodes), `display.rs`, and
  `walk.rs`.
- Add unit tests for display and traversal on hand-built trees.

### Phase 1: parity for a simple `SELECT`

- Add `build.rs` and `lower.rs` for one `SELECT` block over base tables:
  joins, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `DISTINCT`, and
  `OneRow`.
- A query with a feature that the builder does not accept yet uses the old
  path. The builder returns `Unsupported`, and a counter in tests shows the
  coverage.
- Add a `PlannerMode { Legacy, Logical }` setting on `Connection`, default
  `Legacy`, in the style of `SubqueryUnnestingMode`. The sqltest runner and
  `make -C sqlite/conformance run-rust` accept `LOGICAL_PLAN=1`, in the style
  of `MVCC=1`.
- Add a parity test in `tests/integration/`. For every statement of the
  sqltest corpus, it prepares the statement in both modes and compares the
  `Display` text of the physical `Plan` and the `EXPLAIN QUERY PLAN` output.
  The differences are the bug list of phase 1.
- Extend `bench_prepare_query` in `core/benches/benchmark.rs` with a join
  of five tables, a correlated subquery, a window, a CTE, a compound select,
  and an `INSERT ... SELECT`. Do this before the first builder PR, so that
  CodSpeed shows the cost of every later PR.

Done when: the parity test passes for every accepted statement, and CI runs
the sqltest job with `LOGICAL_PLAN=1`.

### Phase 2: full `SELECT` coverage, then the switch

In this order, one PR each:

1. FROM subqueries, CTEs with their materialization metadata, and views.
2. Scalar, `IN`, and `EXISTS` subqueries, correlated and not correlated.
3. Compound selects and `VALUES`.
4. Windows. The split into nested subqueries becomes tree construction.
5. Recursive CTEs.
6. Virtual tables, table-valued functions, `INDEXED BY`, attached databases.
7. `INSERT ... SELECT`, `CREATE TABLE AS`, triggers, and every other caller
   of `prepare_select_plan`.

Then: make `Logical` the default, keep `Legacy` in CI for one release, and
delete the old path. After that, `prepare_one_select_plan` is gone, and only
`lower.rs` constructs a `SelectPlan`.

Done when: `make test`, both sqltest jobs, the simulator, and the differential
fuzzer pass in `Logical` mode, and the prepare benchmark shows no regression.

### Phase 3: rules

This phase starts after the old path is deleted, so that no rewrite is written
twice. Port the existing rewrites first, then add new ones:

1. Constant `WHERE` terms (`eliminate_constant_conditions`).
2. `BETWEEN` and other expression normalization (`rewrite_between_exprs`).
3. Lift shared `AND` terms out of `OR`.
4. `LEFT JOIN` to inner join when a later filter rejects NULL rows.
5. Window split.
6. Subquery unnesting (`unnest.rs`, the largest one).
7. New: push a `Filter` into a `DerivedTable` (section 2.1).
8. New: merge a simple `DerivedTable` into its parent block (the SQLite
   flattener, with its conditions).
9. New: remove unused columns of a `DerivedTable`.

Each rule PR has plan text tests for the rule, `.sqltest` coverage for the
result, and a differential fuzzer run.

### Phase 4: one binder

The DBSP compiler consumes the shared tree, and
`core/incremental/logical_plan.rs` is deleted. Materialized views get the
binder and the semantics of the main path.

### Phase 5: DML

Add `Delete`, `Update`, and `Insert` root nodes over a read subtree. The
lowering produces today's `DeletePlan` and `UpdatePlan`. Unification of the
DML emitters is a later plan.

Later, in separate plans: an expression type that replaces `ast::Expr` (issue
#1615, second half), and rule hooks for extensions
([issue #2523](https://github.com/tursodatabase/turso/issues/2523)), where the
`Rule` trait is the natural entry point.

## 6. Tests

| Level | Harness | What it checks |
|---|---|---|
| Plan text | Unit tests next to `build.rs`, `lower.rs`, and each rule, with a `Resolver` from an empty schema as in `optimizer/mod.rs` | One tree or one rule at a time, without a database |
| Parity | `tests/integration/`, phases 1 and 2 only | Both modes give the same physical plan text and the same EQP |
| SQL results | `sqlite/conformance/sqlite-sqltests/` | Behavior, in both modes during phases 1 and 2 |
| Differential | `testing/differential-oracle/` and `scripts/diff.sh` | The same rows as SQLite |
| Concurrency | `testing/simulator/` | No change in transaction behavior |
| Prepare time | CodSpeed, `bench_prepare_query` | No regression per PR |
| Stack | Tests with the largest compound and nesting sizes that the old path accepts | No stack overflow |

## 7. Risks

- **Parity with SQLite quirks.** Bare columns in aggregates, `ORDER BY rowid`
  truncation, aggregates moved to the enclosing query, the `RIGHT JOIN` swap,
  `USING` de-duplication, and `SELECT` inside trigger bodies. Mitigation: the
  binder is moved, not rewritten. The parity test runs on the full corpus. The
  old path stays until the corpus passes.
- **Prepare time.** One more tree per statement. Mitigation: the tree is
  consumed, not cloned, and the lowering moves values. The benchmark blocks a
  regression. The old path clones the whole plan for the unnesting comparison,
  so the new path can also be faster.
- **Two paths for a while.** Mitigation: phase 2 ends with the deletion. The
  mode setting is the only branch point. No rule PR before the deletion.
- **Merge conflicts.** Mitigation: small PRs to `main`, each behind the mode
  setting.

## 8. Open questions

1. **Aggregate outputs.** Today a `Project` expression contains the aggregate
   call itself, and the emitter matches it against `plan.aggregates`. Keep
   this until the expression type changes, or refer to aggregate outputs by
   index now? Proposed: keep it.
2. **Subquery lists per node or per block?** Proposed: per node. A rule that
   moves a `Filter` then moves its subqueries with it.
3. **CTE definitions.** A `With { ctes, body }` node, or one `DerivedTable`
   per reference with the shared CTE id, as today? Proposed: per reference.
   The materialization decision is per statement, and
   `FromClauseSubqueryCteMetadata` already models it.
4. **`EXPLAIN` of the logical tree.** `tracing` only, or `FORMAT=LOGICAL`?
   Proposed: `tracing` and tests first.

## 9. References

- [Issue #1615](https://github.com/tursodatabase/turso/issues/1615):
  transform the SQL AST into separate logical plan data structures.
- [PR #2786](https://github.com/tursodatabase/turso/pull/2786): first
  implementation of a logical plan, for DBSP.
- [Issue #2523](https://github.com/tursodatabase/turso/issues/2523): expose
  the planner to extensions for logical plan rewriting.
- DataFusion `LogicalPlan` and `OptimizerRule`:
  <https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html>
- CockroachDB normalization rules:
  <https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/norm/rules>
- SQLite subquery flattening: <https://sqlite.org/optoverview.html#flattening>
- T. Neumann and A. Kemper, "Unnesting Arbitrary Queries", BTW 2015.
