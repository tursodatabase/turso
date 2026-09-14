# Generated logical rules

`core/translate/relational/rules/logical.rules` declares the operators, Rust
helpers, and ten rules used by the current relational adapter. `core/build.rs`
compiles this file into Rust in Cargo's output directory. Preparing a query does
not parse rules, interpret patterns, or generate code.

The first executable adapter, correlated filter lowering, shared CTE references,
JSON inspection and result tests established the requirements for this compiler.
The remaining prepare regression is tracked separately in
[the measurement report](logical-plan-performance.md); introducing the DSL does
not close that performance requirement.

## Language

```text
operator Filter(input: Relation, predicates: Scalars);
predicate Empty(Scalars) = empty_predicates;

rule EliminateSelect normalize 10
    (Filter $input $predicates)
    when (Empty $predicates)
    => $input;
```

Operator declarations specify named Rust enum fields and their types. Supported
field types are `Relation`, `Scalars`, `Outputs`, `JoinKind`, `TableId`, and `Bool`.
`Bool` is also the result of a predicate. Rust compilation checks these declarations
against the actual operator fields and helper signatures.

Patterns contain operators and `$bindings`. Nested operators match boxed inputs.
Each pattern binding has one inferred type and appears once in the pattern.
Preconditions call declared predicates with borrowed bindings; several `when`
clauses form a left-to-right conjunction. They must leave the plan unchanged.
Predicates also receive the immutable `LogicalPlan`. Constructors may update its
binding catalog when introducing a fresh subquery input. Helpers return `Result<T>`.

Replacements construct operators, consume bindings, or call declared Rust
constructors. For example, merging filters uses a constructor that appends the
outer predicates after the inner predicates. The generator first checks a pattern
and its guards by reference, then moves its fields into the replacement. It does
not clone the matched tree. An unused binding is dropped. A replacement cannot
use a binding twice, so it cannot silently duplicate a subtree or an expression
with observable evaluation behavior.

The compiler rejects duplicate declarations and bindings, unknown operators and
helpers, incorrect arities and types, unbound replacement names, constructor calls
in guards, repeated ownership, invalid phases and undeclared operator growth. Rule
errors include the source path, line and column. Comments begin with `#`.

Rust constructors are trusted implementations of the declared operation. The
constructors concatenate predicates, move filters, or form an independent joined
input behind a subquery boundary. They must obey the rule's declared growth bound,
preserve unused inputs only as allowed by the rule, and return a valid relation.
Adding a constructor requires guard, invariant, result and growth tests.

An exploration rule may declare `grow N` after its priority. The default is zero;
normalization rules cannot declare growth. The compiler checks explicit replacement
operators against this bound. Rust constructors are responsible for their own
bound, which tests check against the resulting tree. The generated dispatcher
reserves the allowance before consuming the matched input. `PullDependentFilterOverJoin`
declares `grow 2`: it replaces a dependent filter with a semi/anti join over a
subquery and projection, retaining local predicates inside that subquery.

## Scheduling and limits

Rules have a phase and numeric priority; lower numbers run first. Equal priorities
retain declaration order. Normalization and exploration have separate generated
entry points. `PullDependentFilter` is an exploration rule: the existing optimizer
still costs the correlated and decorrelated forms, and forced/disabled modes
remain available. Normalization rules do not choose access paths.

The driver first runs normalization without exploration. For SELECTs that bind
to the logical representation, lowering applies those changes to the original
query before creating a rewritten alternative. Both forms therefore use the
same normalized predicates for costing. The exploration pass processes a
dependent join's left input first, tries to remove the dependency, and then
processes its right input. It does not revisit the left
subtree after replacing the dependent join. If rewriting the right input changes
it, the driver retries the enclosing dependency once without traversing that
fragment again. Normalization runs after processing
children, in priority order until no rule applies. Filter pushdown also normalizes
the filter's new location; it does not walk the unchanged input subtree again.

Exploration normalizes new or changed subtrees. An unchanged subtree has already
finished initial normalization, so it does not repeat those rule checks. A changed
shared producer makes later producers and the root eligible for normalization
again, even if their reference nodes did not change. The driver still visits
operators in the same order and charges those visits to the shared work budget.

Shared producers are processed once per pass, in dependency order, before the
root. References read the rewritten producer without expanding it into the consumer.
Producer and consumer visits, rule applications and growth share one budget
across both passes. Lowering rebuilds each producer once per query form, then
assigns that SELECT plan to its references while preserving the CTE's
materialization metadata. Existing
physical planning still chooses access paths and schedules the materialization.
The producer retains the identity of the reference that supplied its bound body.
Resource collection uses that reference even when it visits the outer FROM
clause before a consumer inside EXISTS.

Together the two passes permit at most 4096 visited nodes, 4096 rule applications
and 4096 added operators. Growth is charged using each rule's upper bound, without refunding later
removals. Filter and identity elimination reduce the tree's size;
filter merging removes an operator; projection pushdown reduces the number of
projections below that filter. `PullLeftFilter` moves a filter from a semi/anti
join's left input to above that join; no normalization moves it back. Budget
exhaustion leaves the last valid executable tree in place and sets the inspection
flag. Construction and each completed pass
are validated in debug builds; inspection validates all emitted logical trees.

`MergeSelectInnerJoin` requires local filter references. This keeps correlation
filters available to the dependent-filter rules during the first pass. A pure
derived input remains eligible after normalization removes an identity
projection; its underlying scan, filter or join must still be reorderable.

`DeduplicateSelectFilters` runs after the other filter normalizations. It preserves
the first occurrence of each identical predicate and removes later occurrences
without changing the order of the remaining predicates. An application strictly
reduces the number of predicates and adds no operators. Exact comparison includes
the expression tree, bound references, affinity, collation and nullability. The
rule requires every predicate to be deterministic and unable to fail; collation
callbacks and generated expressions with possible errors prevent the rewrite.
It does not reorder comparison operands or infer expression equivalence from SQL
text. Candidate groups use an expression hash, followed by exact scalar comparison;
hash collisions cannot remove different expressions. A direct comparison handles
two predicates without allocating a map. Join predicates remain outside this
rule's scope.

`after.rewrites.applied_rules` contains a counter for every generated rule.
`pull_dependent_filter` counts both dependent-filter rules for the first inspection
schema's consumers. `added_nodes` reports the charged growth. The `logical_optimizer`
trace target reports each applied rule.
Serialization runs only for `FORMAT=JSON_LOGICAL`; ordinary prepares do not build
JSON. Each bound phase also reports `dependent_joins`, counting the root and each
shared producer once. Each `dependent_join` node has `unnesting_rules` entries for
the two dependent-filter rules, with `rule`, `applicable` and, when false,
`decline_reason`. These use the same checks as the generated rules. The phase's
`dependency_declines` object groups those remaining failures by rule and reason.

Scalar joins contribute to the remaining dependency count and expose two inputs,
the nullable `result_column`, the `subquery` identity, `row_selection: "first"`
and `empty_result: "null"`. Their right query preserves the required ordering.
The driver normalizes both inputs, but no generated decorrelation rule targets
the scalar join itself yet. Its presence does not count as removed dependence.

These are checks on the displayed tree, not a history of rewrite attempts.
An applicable dependency can remain because the work budget was exhausted. The opt-in
`logical_optimizer` trace records actual failed rule checks separately; debug
events report remaining dependencies after rewriting and binding fallback
reasons. Remaining-dependency traversal runs only for inspection or enabled
tracing.

Each bound phase also reports `normalization_declines`, grouped by generated rule
name and the first failed DSL precondition. For example, `Pure` prevents removing
an expression that can fail, while `Duplicates` reports that no identical
predicates remain. Only rules whose operator patterns match the displayed node
contribute a check. Preconditions run in their declared order and stop at the
first failure. The root and each shared producer contribute once, regardless of
the number of shared references. The generated inspection function uses the same
patterns and Rust predicates as rewriting. It runs during JSON serialization and
adds no precondition calls to ordinary preparation.

| Decline code | Failed requirement |
|---|---|
| `right_input_shape` | The input must match the single-binding or filtered-join shape implemented by this rule. |
| `predicate_effects` | Moving a predicate must preserve errors, nondeterminism and collation callbacks. |
| `right_input_dependency` | The right subplan must be independent before its filter is pulled. |
| `missing_left_columns` | This lowering requires a left binding with columns. |
| `unavailable_columns` | Every referenced column must belong to the inner input or be supplied by the left input. |
| `anti_predicate_placement` | The physical anti join must be able to retain the predicate on its inner input. |
| `left_input_evaluation` / `right_input_evaluation` | Reordering the input must preserve its evaluation; effects, dependent inputs and ordering boundaries can prevent this. |
| `missing_correlation_column` | A wrapped joined input must project an inner column used by a correlation predicate. |

## Current coverage

| Rule | Guard and test coverage | Compilation coverage |
|---|---|---|
| EliminateSelect | Empty versus nonempty filter | Generated and unit tested; binding already omits empty filters |
| MergeSelects | Pure predicates; failure boundary retained; interaction with projection pushdown | Generated and unit tested; nested logical filter production is still limited |
| DeduplicateSelectFilters | Exact bound expressions and comparison properties; pure predicates; preserve operand and predicate order | SQL NULL, duplicate-row, type, collation and error cases; JSON positive/negative checks; forced/disabled EXISTS and NOT EXISTS; prepare scaling with 1, 8, 32 and 64 repeated or distinct filters |
| EliminateProject | Same identities, order, names, collation and other metadata; no effects or aliases | Generated and unit tested; SQL binding usually assigns fresh output identities |
| PushSelectIntoProject | Pure passthrough expressions; explicit column substitution; volatile/error negative cases | Generated and unit tested; derived-input migration remains outstanding |
| MergeSelectInnerJoin | Inner join only; local pure predicates and reorderable inputs; semi/anti and correlation negative cases | Executed through SQL with a dependent filter; JSON and duplicate-preserving result tests |
| PullDependentFilter | Available outer bindings, one independent B-tree/shared/derived right input, effect guards, anti predicate placement | Existing SQL corpus, shared CTE inputs on both sides, JSON, forced/disabled oracle and instruction measurements |
| PullDependentFilterOverJoin | Independent inner/semi/anti join, pure inputs and predicates, available outer columns, projected correlation columns, anti predicate placement | Joined and nested input SQL/JSON and forced/disabled tests; column mapping, effect and growth-exhaustion tests |
| PullLeftFilter | Semi/anti join only, pure filter and join predicates, reorderable inputs | Unit and nested SQL/JSON cases; failures/volatility and inner-join negative cases; remaining valid parent after growth exhaustion |
| UnnestMembership | Independent pure right projection/VALUES input, or a projected filter or output using available left columns over an independent input; pure comparisons and movable left input | Scalar and row IN/NOT IN filters, NULL/empty inputs, duplicates, affinity/collation, inequalities, disjunction, computed outer results, effect/order declines, arity and growth checks |

`UnnestMembership` replaces a membership filter with a semi/anti join, charging
at most one added node. A pure projection over one table scan can join that
table directly. Its result expressions become the comparison operands, keeping
the original bound IN collation. For NOT IN, every comparison and filter must
reference the inner scan so the anti loop evaluates it in the correct place.
Constant outputs or filters retain a FROM subquery. Joined, shared and VALUES
inputs also retain their executable subquery mapping. The rule reserves one
growth unit even when direct scan lowering removes operators.
IN requires every component comparison to be true. NOT IN rejects a left row
whenever some right row has no false component: each component predicate is
equality OR left-NULL OR right-NULL. A NULL check is omitted when that operand's
bound metadata proves it cannot be NULL. This preserves unknown row comparisons
and accepts every left row when the right input is empty.
Only deterministic expressions that cannot fail may be evaluated by these joins.
When a correlated projected filter retains a subquery, local predicates stay
inside the right input. The projection retains membership result columns first
and appends raw columns needed by the correlation predicates. Those predicates
then use the projected columns in the semi/anti join. The right projection must use fresh output
identities; computed outputs cannot substitute for raw correlation columns.
Equality, inequality, IS and disjunction keep their bound expression semantics.

For a projection over one independent table scan, output expressions may also
use columns available from the left input. The rule binds those references to
the resulting join, whether or not the scan has a filter. IN may project only
an outer value. NOT IN still requires every comparison output and filter to
reference the inner scan, so an outer-only result remains dependent.

For an outer-dependent projection over a joined, shared or derived input, the
rule keeps local filters inside that input and projects the raw inner columns
needed by the comparison expressions and correlated filters. Those expressions use the fresh
subquery columns and available left columns in the semi/anti join. The new
subquery boundary uses the existing one-node growth allowance. No computed
outer value is evaluated inside the independent input.

Result and forced/disabled checks cover joined inputs, MATERIALIZED CTEs and
derived VALUES. Each forced comparison verifies that the physical plans differ,
as well as checking the rows against SQLite.

NOT IN still requires each comparison output to reference an inner column.
A wrapped input also needs at least one raw inner column used by a projected
expression or correlated predicate. Outer-only outputs without such a predicate
remain an implementation gap; an IN projection over a direct scan does not need
that wrapper.

Remaining dependent inputs, aggregates, ordering, limits and effectful expressions
remain explicit membership operators. An outer-only NOT IN correlation also remains dependent
because the current anti loop cannot place that predicate correctly. These are tracked implementation
or evaluation constraints, not completed general decorrelation. Lowering
preserves the original IN evaluation for the retained membership operators.
Membership inspection reports its comparison expressions, negation, NULL semantics,
applicability and remaining decline reason. The physical planner compares the
original and rewritten forms, including for independent IN inputs.

The correlated-filter extension passes 42 relational tests, 486 integration
tests, 1,448 SQL cases and eight new distinct-plan forced/disabled cases. Its
prepare regressions and execution comparisons are recorded in
[the performance report](logical-plan-performance.md#correlated-scalar-and-row-membership-filters).
General membership decorrelation and performance acceptance remain outstanding.

The extension for outer values in projections over a scan passes 46 relational
tests, 487 integration tests, 1,468 SQL cases and eight new distinct-plan
forced/disabled cases. Twelve focused cases also pass against SQLite 3.50.4 and
the preceding compiler. The SQL cases include empty inputs, NULLs, duplicates,
explicit collation and integer arithmetic that overflows into a real value.
Logical inspection checks unavailable lowering forms and effectful outputs
remain dependent. Evidence is in
`perf/logical-plan/results/membership-outer-projection/`.

The generator can now place a numeric inner and outer column in the same
membership output, controlled by `in_outer_projection_probability`. Both
correlated profiles use a 30% probability and generate IN and NOT IN equally.
`correlated-selects` creates populated tables before generating only SELECTs;
the mixed profile retains DML and schema changes. Seed 57291020 at depth one
passes all 1,000 generated queries, with 233 distinct-plan checks, 212 same-plan
checks and 94 independently validated joined equivalents. The history contains
77 queries with outer values in membership outputs. These query counts are
separate from the rule trace's repeated preparation events.

The preceding mixed run, seed 57291019, stopped on an UPDATE FROM cursor panic
after 305 executions. Its source and SQL history remain recorded as deferred
work; it was not replayed, shrunk or investigated. An initial SELECT-only trial
had no tables and exercised no rewrites; it does not count as feature validation.
The setup regression now requires populated tables and comparisons of different
plans. Evidence is in `perf/logical-plan/results/membership-projection-generator/`.

The membership slice passes 1,780 SQL cases, 47 JSON tests and the forced/disabled
form checks. Seed 57291015 at depth five executes 1,954 of 2,000 generated
statements with no errors, 429 distinct-plan checks, 229 same-plan checks, and
205 independent joined equivalents. The full core rerun passes 2,498 tests. Two
concurrent MVCC failures in the earlier run did not recur in six isolated repeats
or the full rerun; their causes remain unresolved. Raw results and the combined
working-tree caveat are in
`perf/logical-plan/results/membership-lowering/validation.json`. These results do
not establish final performance acceptance.

The independent-pair generator also joins two inner aliases inside EXISTS and
NOT EXISTS. Seed 54321 at depth four checks 105 joined equivalents and 204
distinct forced/disabled plans with no errors. Opt-in tracing records 204
`PullDependentFilterOverJoin`, 208 `MergeSelectInnerJoin`, and 350
`PullDependentFilter` applications across preparation and EXPLAIN. The other four
normalization rules have zero applications in this run; their unit coverage does
not substitute for SQL generator coverage. The SQL, schema, log and per-rule
counts are retained in `perf/logical-plan/results/fuzz-joined-input-54321-depth-4/`.

Repeating that seed with dependency diagnostics preserves those results and rule
application counts. The trace records 17 predicate-effect and eight anti-predicate
placement declines, plus 239 input-shape declines across the two unnesting rules.
Binding fallbacks include 770 value-producing subqueries, 639 aggregates, 171
DISTINCT outputs, 85 scalar expressions containing execution resources or
subquery results, and 29 subqueries outside direct EXISTS filters. These events
include repeated preparations and EXPLAIN, not unique queries. The complete
trace and counters are in `perf/logical-plan/results/fuzz-dependencies-54321-depth-4/`.

With nested-filter rewriting, the same seed retains all 204 distinct-plan and
105 independent-equivalent checks without errors. It records 86 `PullLeftFilter`
and 214 `PullDependentFilterOverJoin` applications. The remaining-dependency
histogram changes from 546/31/2 compilations with zero/one/two dependencies to
554/25/0. The other four normalization rules still have no applications in this
run. Counts, SQL and the complete trace are retained in
`perf/logical-plan/results/fuzz-nested-filters-54321-depth-4/`.

Shared-producer coverage rewrites a producer once for two consumers, then
removes dependencies in a consumer of that rewritten producer. SQL cases cover
nested producers, NULLs, duplicates and empty inputs. The oracle checks forced
and disabled executions with different physical plans, including one CTE read
both outside and inside EXISTS. A budget test exhausts producer visits and
preserves the consumer's valid tree. Before-change JSON failures and completed
validation are retained in `perf/logical-plan/results/shared-producers-pilot/`:
2488 core tests and 36 fuzzer tests pass serially, along with 30 JSON tests,
1172 Turso SQL cases, 176 applicable SQLite cases, formatting and strict core
and fuzzer lint. Seventeen core tests remain ignored by the existing suite.
The parallel core run encountered an attached-reader checkpoint assertion;
the isolated test, 20 repeated isolated runs and the serial suite pass. The
record preserves that failure instead of claiming a successful parallel run.

The six normalization candidates come from the finite inventory in
[the design](logical-plan.md#rule-inventory). Their source links, deferred rules,
and procedural physical transformations remain in that inventory. Unit-level
construction coverage does not count as full SQL migration or successful general
unnesting. The remaining scope, execution measurements and per-workload prepare
criteria must still be completed.

`perf/logical-plan/results/duplicate-filter-rule/` retains 35 passing relational
tests, 477 passing query-processing integration tests, the forced/disabled form
comparison and 1,417 passing focused SQL cases. Seven integration tests remain
ignored by the suite; one test requiring unavailable host io_uring support was
explicitly excluded. The six new SQL cases also pass SQLite 3.50.4 and the
before-rule Turso binary. Before-rule structural checks fail, and the retained
JSON shows that all four predicates remain before this rule removes the repeated
one. Formatting and strict lint checks for the changed packages pass. Prepare
costs and outstanding performance failures are recorded in
[the performance report](logical-plan-performance.md#duplicate-filter-normalization).

`perf/logical-plan/results/normalization-inspection/` retains the failing
before-change inspection test, 52 passing JSON tests and 36 passing relational
tests. Coverage includes pure and failing predicates, repeated precondition names,
different failed preconditions, shared producer counts and the deterministic
projection snapshot. Strict lint and formatting pass. The initial generated
else-if form failed lint because multiple predicates used the same diagnostic;
the final generated function stops a rule's checks at the first failure.
