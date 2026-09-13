# Bound relational planning

Baseline: `a9a8779c1906247ae3ae78cd098ba713c27d8c9b`. This is a staged compiler
migration. The matrices below distinguish implementation from design; a legacy
path or a declined rewrite is not evidence of completed decorrelation.

## Boundary and lowering

The target boundary is SQL AST → binding → bound relations → logical rewriting →
physical planning → VDBE. Relation and column identities survive rewrites. SQL
names are used during binding and retained separately for result metadata.
Parameter indices are assigned by parsing and retained even when a rewrite removes
their expression. A reference to an enclosing scope records both its column
identity and scope depth.

The initial adapter consumes the already-bound portion of `SelectPlan`. Execution
metadata is retained in a lowering context, outside the relational operators and
their rewrite interface. This is an intermediate binding boundary: the current
binder still allocates subquery result resources. Moving those allocations to
lowering is required before the binding migration is complete.

`core/translate/relational/` owns the new representation, expression conversion,
properties, invariants, transformations and lowering. The first executable slice
is scan/filter/project and a dependent EXISTS/NOT EXISTS filter. Its transformation
moves a pure correlated predicate into an ordinary semi/anti join. The resulting
relations are lowered back into table groups and predicates for the existing join
search. An unchanged dependent alternative stays available for costing.

The existing join optimizer should be retained: it already considers indexes,
hash joins, sorts, correlated call counts and statistics. Logical equivalence
does not choose an access path. `SelectPlan` is the temporary physical interface,
not a second source of truth for the migrated expressions. Lowering must consume
the rewritten operators to construct those expressions and table groups.

Joins whose inputs contain aggregates, DISTINCT, limits, windows or set operations
need subplan boundaries. Lower them as FROM subqueries, using the existing
materialization/coroutine machinery; flatten only when SQL evaluation and join
boundaries allow it. A right-side multi-table semi join must remain a subplan:
the current semi-join loop stops on one table's match. Pure joined EXISTS inputs
now use this boundary: local predicates remain inside, and only columns used by
correlation predicates are projected to a fresh binding. Lowering turns the
former EXISTS body into a FROM subquery before join costing. Shared binding domains need
one producer and explicit references with fresh output identities. Reuse CTE
materialization for physical storage. Recursive references point to an iteration
input; they must not recursively expand the plan.

The incremental-view representation in `translate/logical.rs` remains separate.
`incremental/view.rs` builds it and `incremental/compiler.rs`, expression and
operator modules compile it into DBSP circuits. Its column positions and weighted
update semantics are different from a bound query's column identities and ordered
scalar subqueries. Migrating these consumers is unnecessary for query execution;
parity tests must keep existing view behavior intact. Sharing a name does not make
the two representations interchangeable.

## Operator contracts

All relations are bags unless explicitly stated otherwise. A column identity is
unique to a binding, not to a schema table. Every expression reference must be in
its operator's inputs or explicitly in an enclosing scope. Output order is an
ordered list, not a set. Uniqueness is a set of proven keys; absence of proof means
unknown. Nullability must account for outer join extension, and uniqueness must
distinguish SQL nullable UNIQUE constraints from a duplicate-free binding domain.

| Operator | Semantics and required properties |
|---|---|
| Scan | One binding of a catalog relation; output column types, affinities, collations and declared keys come from that binding. |
| Values / OneRow | Preserve row order and duplicates; no-FROM SELECT has one input row. |
| Project / Map | Evaluate named expressions; preserve multiplicity. A map adds columns; a projection chooses ordered outputs. |
| Filter | Keep only SQL true; false and NULL are rejected. Preserve the permitted evaluation order of effectful expressions. |
| Inner join | Produce matching pairs, multiplying multiplicities. A cross join is a true predicate. |
| Left / Full join | Preserve unmatched rows with NULLs on the absent side. ON and WHERE predicates remain distinct. |
| Semi / Anti join | Emit each left row with its original multiplicity when a match exists / does not exist. Right columns are not outputs. |
| Dependent join | Bind left columns in the right input per left row; specify scalar/row/EXISTS/IN result semantics explicitly. |
| Mark | Preserve left rows and produce true/false/NULL membership, including empty-right and NULL cases. Required for IN in expressions and NOT IN. |
| First | First row under the required ordering, or NULLs when empty. SQLite scalar subqueries do not enforce a one-row error. |
| Aggregate | Group NULLs together. Grouped empty input has no rows; ungrouped empty input has one aggregate row. Bare SQLite columns and min/max row selection require dedicated handling. |
| Distinct | Eliminate duplicates using SQL value and collation semantics, without changing the query's declared result metadata. |
| Sort / Limit | Explicit ordering, direction and NULL placement; limit/offset have SQLite conversion, negative and error behavior. |
| Set operation | Positional column mapping; UNION ALL adds multiplicities, SQLite UNION/INTERSECT/EXCEPT are distinct. |
| Shared reference | Read a single producer through a column mapping. References do not expand the producer. |
| Subquery | Execute one input behind a FROM boundary and map its ordered columns to a fresh binding. Preserve its sort, limit, and evaluation scope. Child bindings are not visible outside the boundary. |
| Iterate / Recursive reference | Seed plus repeated step, with explicit working input, UNION deduplication and SQLite queue ordering. |

Properties include outputs, outer references, nullability, keys, affinity,
collation, order requirements, and whether evaluating expressions can change
observable behavior. Deterministic is insufficient: an expression can still fail.
Unclassified extension functions and aggregates are conservative. CASE, COALESCE,
AND/OR and subquery evaluation positions must not become eager by rewriting.
Validate identities, arities, shared references and scope ownership after binding
and each pass in tests/debug configurations.

## Unnesting contracts and paper adaptations

Read with [Neumann and Kemper 2015](https://db.cs.tum.edu/teaching/ws2122/foundationsde/unnesting.pdf)
and the local `Neumann-Unnesting-1.pdf`. Section 3.1 supports moving dependent
filters; section 3.2 introduces the duplicate-free domain and NULL-equal binding
join. Section 4's domain substitution may evaluate extra groups; in Turso it also
needs a proof that those extra evaluations cannot fail or be nondeterministic.

The local `Neumann-Unnesting-2.pdf`, [formalization](https://arxiv.org/abs/2412.04294),
provides the bag proofs: theorem 4.1 for domains, lemmas 4.3–4.17 for ordinary
operators and 4.18 for nested dependent joins. Its mathematical aggregate rule
alone does not supply SQLite's ungrouped empty-input row. Domain propagation must
restore that row without feeding a fabricated NULL row into COUNT(*) or other
aggregates. Preserve aggregate FILTER and ordered arguments.

[Improving Unnesting of Complex Queries](https://15799.courses.cs.cmu.edu/spring2025/papers/11-unnesting/neumann-btw2025.pdf)
is the algorithm reference. Process dependencies from enclosing scopes toward
nested scopes; when encountering another dependent join, transform its left side
first and combine the needed bindings for the right side. Shared domains and fresh
column representatives avoid repeated subtree expansion. ORDER BY/LIMIT needs
per-domain first-row selection. Recursion carries domain columns through both the
seed and working input. These are required extensions, not claims that a generic
fallback has implemented them.

Binding equality uses NULL-equal comparison and must preserve the original storage
classes, affinity and collation. It is distinct from a user-written equality
predicate. For example, substituting `d = x` must retain its NULL rejection.
Domain deduplication must not merge values distinguishable by the subquery under
another collation or storage-class test. Dialect decisions belong in binding and
operator contracts; PostgreSQL scalar cardinality rules cannot be imported into
SQLite.

SQLite `DISTINCT` alone is not a valid domain key: it can merge integer `1` with
real `1.0`, and a NOCASE key can merge `A` with `a`. A subquery can distinguish
those bindings with `typeof` or concatenation. Domain construction therefore
needs storage-class and binary-value identity, and its join back must avoid
column-affinity coercion. Implementing and testing that identity remains part of
the domain operator work.

The planned domain projection keeps three expressions for each outer column:
the original column, `typeof(column)`, and `+column COLLATE binary`. Applying
DISTINCT to the complete tuple keeps separate integer/real values and text values
that differ under binary comparison. The original column remains available for
substitution with its original affinity and collation. Replacing it only with the
unary-plus expression would lose comparison affinity in the subquery. The binding
join will compare the storage-class expression and use `IS` on both unary-plus,
binary-collated values, including NULL bindings. This construction still requires
an executable domain producer, alias mappings and differential tests; ordinary
DISTINCT lowering alone does not implement domain unnesting.

## Scope and migration matrix

Status at design: legacy means existing behavior is retained and migration is
outstanding. Each executable slice updates this table with its actual tests.

| Supported input | Binding / representation | Rewrites / lowering | Required coverage | Status |
|---|---|---|---|---|
| Simple SELECT, expressions, inner joins | Existing resolver → bound relations | Physical lowering used with the EXISTS alternative; inspection also binds plain SELECTs | JSON, aliases, declared types, parameter slots | partial; ordinary prepare migration outstanding |
| Correlated EXISTS / NOT EXISTS filters | Explicit dependent semi/anti | Dependent filter rules → single-input semi/anti, including a wrapped independent inner join | `unnest-exists.sqltest`, `test_eqp_json.rs`, oracle forced/disabled test | executable bounded slice; performance comparison outstanding |
| IN / NOT IN, scalar and row subqueries | Dependent mark/first | Domain rules → subplan/mark/first | Empty, NULL, types, order, errors | legacy |
| DISTINCT | Duplicate removal after projection, with explicit output identities | Lower through existing physical DISTINCT; rewrite filters underneath | Ordered and computed outputs, aggregate outputs, NULLs, storage classes, collation, empty input, shared producers | executable outside an EXISTS body; hidden ordering expressions and domain propagation outstanding |
| GROUP BY, HAVING | Group keys, aggregate calls, modifiers, HAVING and named group outputs | Rewrite supported filters below aggregation; lower through existing group and aggregate execution | Empty input, duplicate/NULL groups, bare min/max columns, DISTINCT arguments and results, FILTER, overflow, ordering, shared producers | executable, including dependent EXISTS bodies and rewrites below them; removing the aggregate dependency and complete ordering coverage outstanding |
| ORDER BY, LIMIT/OFFSET | Ordered operators around the supported SELECT body | Existing physical sort and limit; DISTINCT precedes both | Projected order keys, aliases, empty input, zero limit, offset | represented in the bounded slice; per-domain lowering outstanding |
| Windows | Partition and ordering requirements | Partition by binding domain | Ties, empty input, frames | legacy |
| Compound SELECT | Explicit positional set mappings | Domain on both arms | Multiplicity, type/collation | legacy |
| FROM subqueries and views | Simple SELECT body behind an explicit ordered output mapping | Rewrite inside the boundary, then rebuild the physical FROM subplan | Ordered limited derived input with a rewritten EXISTS, metadata and parameter slots | bounded derived-input slice including aggregates and DISTINCT; compounds and full view coverage outstanding |
| Materialized nonrecursive CTEs | One producer, references with separate column identities | Rewrite and lower producers in dependency order, then lower consumers with the same materialization metadata | Two consumers, nested producers, duplicate/NULL/empty rows, physical single materialization | bounded shared-input slice; unsupported producer operators remain legacy |
| Recursive and outer-dependent CTEs | Iterate/ref or per-binding sharing | No recursive expansion | Queue semantics and dependent domains | legacy; outstanding |
| Virtual tables and table functions | Catalog binding with behavior properties | Keep xBestIndex in physical planning | Arguments, errors, ordering | legacy |
| INSERT SELECT / CTAS | Query plus destination | Same relation lowering | Constraints, metadata, writes | legacy |
| UPDATE / DELETE / UPSERT subqueries | Bound read scopes with write phases | Keep write safety in physical planner | Multi-connection and API tests | legacy |
| RETURNING and trigger expressions | Explicit post-write scope | Preserve evaluation phases | Parameters, post-write reads | legacy |
| PostgreSQL frontend | Dialect AST adapter | Preserve dialect operator semantics | pg-sqltests and wire tests | legacy |
| Incremental views | Existing DBSP representation | Separate, retained | Existing incremental-view tests | separate |

## Rule inventory

| Existing transformation | Location | Intended home / reason |
|---|---|---|
| Split conjunctions; bind aliases and names | planner, expr/binding | Binding, before operator rules |
| FROM flattening and join conversion | planner, select | Logical rules after scope and outer-join guards |
| Window splitting | window | Procedural construction until window operators migrate |
| EXISTS ignored projection/order/distinct | subquery | Binding semantics plus retained parameter slots |
| Scalar common subqueries | subquery | Shared references, only at compatible evaluation phases |
| Lift common OR terms | optimizer/lift_common_subexpressions | Logical rule with effect guards |
| Constant condition elimination | optimizer/mod | Logical normalization |
| Implied equalities and partial-index constraints | optimizer/constraints | Logical inference; index applicability stays physical |
| Outer-join simplification | optimizer/mod | Logical rule with null-rejection proof |
| EXISTS/IN semi-joins and aggregate unnesting | optimizer/unnest | Logical alternatives; retain cost comparison |
| MIN/MAX and COUNT fast paths | optimizer/mod | Physical implementations of aggregates |
| Sort elision, DISTINCT access paths | optimizer/order, mod | Physical required/provided ordering |
| Index methods, OR scans, join enumeration | optimizer/access_method, multi_index, join | Physical costing and access selection |
| DML ephemeral write sets | optimizer/mod | Physical write safety, outside equivalence DSL |
| Shared CTE materialization | subquery | Logical sharing, physical scheduling/storage |

The additional normalization rule `PullLeftFilter` is derived from the bag
definitions of semi and anti joins in equation (14) of the later unnesting paper.
For either join, filtering its left input and then joining preserves the same
left rows as joining first and applying that filter afterwards. Turso must also
prove that both inputs, the filter and the join predicates can be reordered;
otherwise the change could expose extra errors or nondeterministic evaluations.
The rule is limited to semi/anti joins to keep normalization direction explicit.
It is generated from the rule file and covers positive and negative guards.
The dependent-filter extension wraps independent semi/anti inputs and retries an
enclosing dependency once when rewriting its child changed that child. It does
not repeatedly traverse the nested fragment. SQL and JSON cases cover nesting
depths two and four, all EXISTS/NOT EXISTS combinations, NULLs, duplicates and
effect barriers. The complete nine-case execution comparison improves depth four
but fails the fixed criteria for automatic depth two and the anti case; ordinary
preparation parity also remains required. This supports a
further bounded class; distant-scope domain propagation remains separate work.

Finite Cockroach-derived candidate inventory (no source code is copied):

| Candidate / source | Decision and Turso preconditions | Tests / benchmark |
|---|---|---|
| EliminateSelect, select.opt | Include: empty filter only | Empty/nonempty filters; point lookup prepare |
| MergeSelects, select.opt | Include: pure total predicates, retain ordered terms | Effects and nested filters; predicate prepare |
| EliminateProject, project.opt | Include: identical output identities and metadata | Renames/column order; wide projection |
| PushSelectIntoProject, select.opt | Include only passthrough columns, pure total projection | CASE/error guards; derived-table prepare |
| MergeSelectInnerJoin, select.opt | Include: inner joins, pure total conditions | Outer-join negative cases; join prepare |
| DeduplicateSelectFilters, select.opt | Defer until effect/equivalence properties cover collations | Repeated volatile functions; expression scaling |
| EliminateJoinUnderProjectLeft, project.opt | Defer: proof of no duplication and row preservation needed | Nullable UNIQUE/FK counterexamples; join prepare |
| ConsolidateSelectFilters, select.opt | Retain existing procedural range analysis | Mixed affinity/collation; range prepare |

Sources: [select.opt](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/norm/rules/select.opt),
[project.opt](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/norm/rules/project.opt).
The five included candidates are generated from `rules/logical.rules`. Guard and
interaction tests cover their logical construction. `MergeSelectInnerJoin` and
`PullDependentFilter` also have combined SQL and JSON coverage. The other four
normalizations still need broader SQL-producing paths and per-rule workloads;
none of this closes the outstanding full-corpus performance criteria. See
[generated-rule coverage](logical-plan-rules.md#current-coverage).

## Passes, inspection and completion

The DSL followed the first executable slice's binding, shared-input, correlated
filter, inspection and result checks.
[Optgen](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/optgen/lang/doc.go)
is a reference for typed patterns, bindings, construction and Rust predicates.
The build now generates Rust and rejects unknown operators, wrong arities,
unbound replacement names, duplicate rules and invalid preconditions with
file/line diagnostics. Normalization and costed exploration have separate entry
points. [The language and driver](logical-plan-rules.md) define priorities,
traversal, pass boundaries, budgets and termination. Exploration rules declare a
maximum operator increase, reserved before replacing their input. Domain-producing
rules still need executable constructors and tests under that budget.

`EXPLAIN QUERY PLAN FORMAT=JSON_LOGICAL <statement>` returns the same single TEXT
column as `FORMAT=JSON`. The existing `version`, `sql`, `result_columns`, physical
`nodes`, and CTE materialization fields are retained. Plain `FORMAT=JSON` has no
logical capture or serialization. The additional object is:

```json
{"logical":{"version":1,"scopes":[{"before":{},"after":{},"selected":{}}]}}
```

Scopes are recorded in optimization completion order. `before` is the bound
input, `after` is the equivalent logical alternative before costing, and `selected`
is the chosen physical form rebound for inspection. Each bound form contains
`bindings`, `outer_references`, `retained_parameters`, `shared_inputs`, and `root`.
The root and its nested `inputs` contain deterministic preorder IDs, operator
types, output column IDs, outer references, and structured scalar expressions.
Output columns follow relational output order. A `subquery` node names its fresh
`relation` and the corresponding ordered `input_columns`; its child bindings stay
inside that node. Lowering consumes the rewritten body while retaining the FROM
subquery's physical scheduling and result metadata.
Column IDs contain a stable relation ID and either a position or `"rowid"`.
Projection expressions retain ordered result names, affinity, collation, and
nullability. Scalar references identify their scope and nesting depth.

A DISTINCT SELECT has `distinct(project(input))`, followed by `sort` and `limit`
when present. Sorting refers to projected column identities, including expressions
built from those columns. Lowering substitutes their defining expressions before
passing the SELECT to the existing physical planner. Matching uses exact AST
equality: commuting a comparison can change its collation. An ordering expression
that still reads an unprojected column, or that can fail or is nondeterministic,
currently reports a legacy reason. DISTINCT retains the projection's column
metadata and does not move across filters or joins.

An `aggregate` node contains its group keys, registered aggregate calls, HAVING
predicates and ordered result expressions. Aggregate calls expose arguments,
DISTINCT and FILTER separately. Its result expressions use SQLite's group
evaluation: aggregate calls read the group, and bare columns use the same input
row chosen by the existing executor, including the min/max behavior. Lowering
rebuilds those aggregate definitions and grouping state without preserving
registers or cursors. The node emits one row for empty ungrouped input before
HAVING, and no rows for empty grouped input. Ungrouped result nullability is
conservative because bare columns become NULL on empty input.

Ordering and LIMIT follow the aggregate output. Sorting can read projected
aggregate values and expressions built from them. A scalar result expression
that can fail or is nondeterministic still cannot move across sorting or LIMIT;
reading an aggregate already computed for the group does not itself move its
argument evaluation. Group sort directions and NULL placement already absorbed
by physical planning are restored as an explicit ordering when the adapter
encounters them. Rules currently descend into an aggregate input without moving
the aggregate across another operator. Aggregate EXISTS bodies, SELECT DISTINCT
over aggregation, hidden ordering expressions and general per-domain aggregation
remain implementation gaps.

`after.rewrites` reports `pull_dependent_filter`, named `applied_rules`, visited
nodes, charged `added_nodes`, and budget exhaustion. A pass visits at most 4096
nodes, applies at most 4096 rules and reserves at most 4096 added operators.
Unvisited dependencies remain
executable. Normal preparation also supports opt-in `logical_optimizer` tracing
for applied rules, failed unnesting preconditions, binding fallbacks and remaining
dependencies. Every bound inspection phase reports `dependent_joins` across its
root and shared producers, counting a producer once. Dependent operators report
each unnesting rule's applicability and first failed precondition; the phase's
`dependency_declines` groups these checks by rule and reason. They describe the
displayed tree, while tracing records checks made during rewriting. A form
outside the current adapter reports `{"status":"legacy","reason":"..."}`;
this is an implementation gap. Normalization-rule decline reasons and complete
shared-input, operator, and dialect coverage remain outstanding.

## Executable unnesting coverage

| Query class | Rule / preconditions | Evidence | Status |
|---|---|---|---|
| Direct EXISTS / NOT EXISTS in WHERE or an AND term | `pull_dependent_filter`; one independent B-tree, shared, or derived right input, all referenced columns available on the left; input is pure | SQL corpus and logical before/after assertions | implemented |
| Equality, inequality, IS, disjunction, several referenced columns | Retain the original comparison AST, affinity and collation; predicate is deterministic and cannot fail | NULL, duplicate, inequality and OR cases; forced/disabled oracle | implemented for the direct filter rule |
| Outer input with pure filters and inner joins | Preserve left multiplicity; no outer joins or hidden semi-join columns | Existing EXISTS joins plus invariant tests | implemented for this slice |
| Independent inner, semi or anti join inside EXISTS / NOT EXISTS | `PullDependentFilterOverJoin`; pure local predicates stay inside a derived input; correlation columns retain metadata and get fresh identities | Joined-input SQL/JSON, forced/disabled plans, mapping and growth tests | implemented for direct filters |
| Nondeterministic functions, possible errors, custom/locale collation callbacks | Do not move these expressions into a different join schedule | Short-circuit SQL and negative JSON guard tests | dependent evaluation is required without stronger proof |
| Anti predicate using only outer columns or constants | Current physical WHERE placement cannot represent all anti ON predicates | Existing constant-false/NULL and outer-only tests | lowering gap, retained dependent |
| Nested filters using their immediate outer scope | `PullLeftFilter` exposes a filter over a semi/anti input; retry the parent after a child rewrite | Depth-two/four JSON and parameter checks, all semi/anti combinations, SQLite duplicate/NULL results, effects and growth exhaustion | implemented for pure filter inputs; execution comparison has unresolved failures |
| Distant scopes and remaining dependent inputs | Bind explicit scope depth; only pull predicates whose outer columns are available | Existing nested result tests; invariant checks | general top-down domain propagation outstanding |
| EXISTS inside OR, CASE, projection, HAVING, ON | Needs a result-producing dependent operator rather than a row filter | Existing compatibility corpus | legacy; outstanding |
| IN / NOT IN / scalar / row subqueries | Mark/first semantics and NULL-aware domains | Existing compatibility corpus | legacy; outstanding |
| DISTINCT outside a direct correlated filter | Rewrite under duplicate removal; order keys refer to projected expressions | Eight SQL cases, JSON ordering and shared-producer structure, forced/disabled comparisons | executable; DISTINCT within a dependent body and domain propagation remain outstanding |
| Aggregates and HAVING outside a direct correlated filter | Rewrite below group evaluation; retain empty-input and aggregate modifier semantics | Fifteen SQL result/error cases, structured JSON, forced/disabled aggregate comparisons | executable bounded input rewrites; propagation through dependent aggregates remains outstanding |
| Joins of subplans, outer joins, set operations | Operator-specific domain rules and executable subplan lowering | Existing compatibility corpus | legacy; outstanding |
| ORDER BY, LIMIT/OFFSET | Represented and round-tripped outside a rewritten filter; right-side order/limit blocks the first rule | Existing limit and order cases | per-binding order/limit unnesting outstanding |
| Materialized CTE on the left of a direct EXISTS | One shared producer, two reference mappings; producer is pure and independent | `exists-over-two-materialized-cte-references` and JSON producer/storage assertions | implemented, including supported EXISTS filters inside the producer |
| Materialized CTE on the right of a direct EXISTS / NOT EXISTS | Rewrite independent producers before consumers; retain one materialization through semi/anti lowering; no volatile or failing producer expressions | Nested producers, NULL/duplicate/empty SQL cases, JSON dependency and materialization assertions, distinct forced/disabled plans | implemented for supported producer bodies; outer-dependent producers remain a migration gap |
| Windows, recursive/outer-dependent CTEs, views, virtual tables, DML scopes | Contracts above; no claim of decorrelation through fallback | Existing compatibility corpus | legacy; outstanding |

Completion requires the executable matrices, independently checked SQL results,
structural assertions, differential seeds and shrinkable regressions, and the
per-workload protocol in [logical-plan-performance.md](logical-plan-performance.md).
Legacy paths are removed only after replacement correctness and prepare cost are
demonstrated. A planned operator or a theoretical algebra rule is not executable
integration.

The first compiler slice is checked with `cargo test -p turso_core --lib
translate::relational`, the `query_processing::test_eqp_json` integration group,
parser unit tests, the differential oracle's
`every_supported_unnesting_form_returns_the_same_rows` test, and the existing
`unnest-exists`, `unnest-correlated`, and `explain-query-plan-json` SQL files.
The oracle compares structured physical operators and parent relationships,
excluding cost estimates and instruction offsets. Text scan descriptions alone
cannot distinguish an anti join from a correlated scan of the same table.
