# Generated logical rules

`core/translate/relational/rules/logical.rules` declares the operators, Rust
helpers, and seven rules used by the current relational adapter. `core/build.rs`
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
field types are `Relation`, `Scalars`, `Outputs`, `JoinKind` and `TableId`.
`Bool` is the result of a predicate. Rust compilation checks these declarations
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

The driver processes a dependent join's left input first, tries to remove the
dependency, and then processes its right input. It does not revisit the left
subtree after replacing the dependent join. Normalization runs after processing
children, in priority order until no rule applies. Filter pushdown also normalizes
the filter's new location; it does not walk the unchanged input subtree again.

A pass permits at most 4096 visited nodes, 4096 rule applications and 4096 added
operators. Growth is charged using each rule's upper bound, without refunding later
removals. Filter and identity elimination reduce the tree's size;
filter merging removes an operator; projection pushdown reduces the number of
projections below that filter. Budget exhaustion leaves the last valid executable
tree in place and sets the inspection flag. Construction and the completed pass
are validated in debug builds; inspection validates all emitted logical trees.

`after.rewrites.applied_rules` contains a counter for every generated rule.
`pull_dependent_filter` counts both dependent-filter rules for the first inspection
schema's consumers. `added_nodes` reports the charged growth. The `logical_optimizer`
trace target reports each applied rule.
Serialization runs only for `FORMAT=JSON_LOGICAL`; ordinary prepares do not build
JSON. Each bound phase also reports `dependent_joins`, counting the root and each
shared producer once. Each dependent operator has `unnesting_rules` entries for
the two dependent-filter rules, with `rule`, `applicable` and, when false,
`decline_reason`. These use the same checks as the generated rules. The phase's
`dependency_declines` object groups those remaining failures by rule and reason.

These are checks on the displayed tree, not a history of rewrite attempts.
An applicable dependency can remain because the work budget was exhausted or
because a shared producer is not yet visited by the rewrite pass. The opt-in
`logical_optimizer` trace records actual failed rule checks separately; debug
events report remaining dependencies after rewriting and binding fallback
reasons. Remaining-dependency traversal runs only for inspection or enabled
tracing. Normalization-rule decline diagnostics remain outstanding.

| Decline code | Failed requirement |
|---|---|
| `right_input_shape` | The input must match the single-binding or filtered-inner-join shape implemented by this rule. |
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
| EliminateProject | Same identities, order, names, collation and other metadata; no effects or aliases | Generated and unit tested; SQL binding usually assigns fresh output identities |
| PushSelectIntoProject | Pure passthrough expressions; explicit column substitution; volatile/error negative cases | Generated and unit tested; derived-input migration remains outstanding |
| MergeSelectInnerJoin | Inner join only; pure predicates and reorderable inputs; semi/anti negative cases | Executed through SQL with a dependent filter; JSON and duplicate-preserving result tests |
| PullDependentFilter | Available outer bindings, one independent B-tree/shared/derived right input, effect guards, anti predicate placement | Existing SQL corpus, shared CTE inputs on both sides, JSON, forced/disabled oracle and instruction measurements |
| PullDependentFilterOverJoin | Independent inner join, pure inputs and predicates, available outer columns, projected correlation columns, anti predicate placement | Joined-input SQL/JSON and forced/disabled tests; column mapping, effect and growth-exhaustion tests |

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

The five normalization candidates come from the finite inventory in
[the design](logical-plan.md#rule-inventory). Their source links, deferred rules,
and procedural physical transformations remain in that inventory. Unit-level
construction coverage does not count as full SQL migration or successful general
unnesting. The remaining scope, execution measurements and per-workload prepare
criteria must still be completed.
