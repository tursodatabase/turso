# Materialized-view maintenance as compiled VDBE

## Status

The old interpreted IVM engine has been deleted. Materialized-view maintenance
is now planned as a composable operator DAG and compiled into an ordinary VDBE
subprogram.

The branch currently has one unreleased hidden-storage format,
`DBSP_CIRCUIT_VERSION = 2`. Development changes that format in place; it must
not be bumped for each intermediate branch revision.

The operator-DAG rewrite and per-class member multisets for built-in
collations are implemented. The annotated IVM conformance corpus and the
differential fuzzer check the materialized-view equivalence invariant, exact
except for which of several equal values a view shows (see "Verification").
See [IVM_COMPAT.md](../IVM_COMPAT.md) for the current user-facing feature
matrix.

## The architectural boundary

The old engine duplicated SQL semantics. It had its own expression
interpreter, comparison and NULL rules, aggregate implementation, operator
runtime, and row-identity scheme. Every SQL rule could therefore diverge from
the regular query engine.

The rewrite removes that second evaluator:

1. `prepare_select_plan` and `optimize_plan` produce the same bound and
   optimized relational plan used by regular query execution.
2. The plan is lowered to a topologically ordered maintenance DAG containing
   `Scan`, `Alias`, `Filter`, `Project`, `Pad`, `Join`, `Aggregate`, and
   `SetOp` nodes.
3. Every edge has one output contract: relational values, logical bindings,
   available source rowids, and its transport identity.
4. Persistent state and output arrangements are derived once from the DAG.
   CREATE-time DDL, validation, and maintenance code generation consume that
   same catalog instead of independently classifying query shapes.
5. The DAG is compiled into one VDBE subprogram. Expressions, predicates,
   affinity, comparisons, collations, NULL behavior, and aggregate
   step/finalization use the regular VDBE machinery.
6. A materialized-view read with pending transaction changes invokes the same
   dependency-ordered maintenance programs used at statement completion and
   commit. There is no separate read-time evaluator.

What remains IVM-specific is the incremental algorithm: delta capture,
z-set weights, join product-rule passes, inverse aggregate updates,
set-membership transitions, hidden state, and stable output identity. That is
necessary specialization, but it must not grow another SQL expression layer.

## Change capture is not CDC

IVM records row images in a connection-local, transaction-wide change log.
Updates are logged as a removal of the old image followed by an insertion of
the new image. Failed statements rewind their log entries, savepoints rewind
to a mark, and rollback discards the log. Rewinding uses positions in the
log, but maintenance does not depend on the order of its rows: every
operator consolidates a delta before merging it into state.

This reuses some DML emission hooks that are also useful to Change Data
Capture, but it does not create CDC rows, expose a CDC protocol, or route
maintenance through the CDC subsystem. The IVM change log is transaction-local
input to maintenance programs and may spill through the normal asynchronous
I/O path when it grows.

## Composition model

Linear nodes (`Filter`, `Project`, `Alias`, and `Pad`) transform their input
delta. Stateful nodes own their integral:

- aggregates own per-group accumulator state and, where needed, value
  multisets;
- deduplicating set operations own per-branch membership counts;
- anti joins own, per input row, its multiplicity and its number of ON
  matches;
- any node whose current output must be probed by a downstream join receives
  an output arrangement.

Each binary join consumes two arbitrary upstream nodes. Its delta follows the
product rule against the inputs' maintained current outputs, so joins of
joins, aggregates feeding joins, outer joins feeding aggregates, and
set-operations feeding joins use the same node implementation. Five-way joins
are already covered; the old four-table shape cap no longer exists.

Outer joins are not join nodes. As in DBSP, they are built from operators
that are already incremental, and their deltas follow from the chain rule:

```text
A LEFT JOIN B  = (A JOIN B) UNION ALL pad(A ANTI JOIN B)
A RIGHT JOIN B = (A JOIN B) UNION ALL pad(B ANTI JOIN A)
A FULL JOIN B  = (A JOIN B) UNION ALL pad(A ANTI JOIN B) UNION ALL pad(B ANTI JOIN A)
```

A correlated `EXISTS` or `IN` subquery is a semi join and `NOT EXISTS` an
anti join; IVM always applies the regular planner's subquery-to-join
rewrite instead of letting cost decide, because the plan is rebuilt after
every schema change and a correlated subquery cannot be maintained. Semi and
anti joins share one operator: per row of the first input it keeps the
row's multiplicity and number of ON matches, and outputs the row while the
match count is non-zero (semi) or zero (anti).

`pad` is the `Pad` node: it appends NULL columns that keep the bindings of
the other side, so a padded row has the same schema as a joined row. A join
chain is lowered left to right, one binary composition per step, so a RIGHT
JOIN pads the whole prefix joined so far.

ON and USING terms belong to their join. A WHERE term runs right after the
last table it reads joins, unless a later join can fill one of its tables
with NULLs; then it runs after the last such join. For example, in
`a JOIN b ON ... RIGHT JOIN c ON ... WHERE a.x > 1`, the term must run after
the RIGHT JOIN, or `c` rows would lose their match and be padded instead of
removed.

Every compound branch is another DAG input. `UNION`, `UNION ALL`,
`INTERSECT`, and `EXCEPT` therefore accept joined, grouped, distinct, or
derived branches. Mixed compound chains are represented by one set-operation
node with an explicitly deduplicated prefix and an optional trailing
`UNION ALL` portion.

Non-recursive CTEs and FROM-clause derived tables arrive from the regular
planner as derived inputs. Multiple references to one CTE may currently
duplicate its subgraph rather than share a DAG node; this is a performance
issue, not a semantic special case.

Materialized views can depend on other materialized views. Maintenance is
scheduled in dependency order over the same transaction log, including for
read-your-own-writes.

## Divergences from DBSP

The engine follows DBSP: every edge carries a z-set delta, each operator's
delta follows from its inputs' deltas (linear operators map them, joins use
the bilinear rule, outer joins are compositions), and persistent state is
the integral of a delta. Where the implementation differs, this is why:

- **Joins read post-statement state.** The join delta is computed as
  `dA ⋈ B_new + A_new ⋈ dB − dA ⋈ dB`, which equals the textbook
  `dA ⋈ B_old + A_old ⋈ dB + dA ⋈ dB`. Maintenance runs after the
  statement has already written the base tables, so the new state is what
  is on disk; keeping an old copy would cost a second copy of every input.
- **Base tables are their own integrals.** A scan's arrangement is the base
  table's btree rather than a separate copy built from its delta. The btree
  is exactly the sum of all deltas applied to it.
- **One step is one statement.** Deltas are collected per statement (and
  before an in-transaction read of a view), not per row. Semi and anti joins
  first sum each row's changes to its multiplicity and match count, then
  visit each changed row once and emit `f(new counts) − f(old counts)`. That
  is DBSP's rule for a non-linear operator applied to the consolidated
  delta; it also makes the result independent of delta order.
- **Semi and anti joins look up the row image in the input arrangement**
  instead of carrying it through their own state, because the arrangement
  already holds the input's current rows. They do store the image they last
  published, because the input's old state is not kept (see the first
  point) and a retraction must repeat the exact image it removes.
- **An outer join runs the join work twice.** The inner join and each anti
  join probe the arrangements separately. Counting matches from the inner
  join's delta would share that work but couple the anti join to the inner
  join's row layout; the operators are kept independent for now.
- **Every delta is consolidated before a stateful merge, in bytecode.** As
  in DBSP, a delta is an unordered z-set. Before the view, an aggregate, a
  set operation, or an arrangement merges a delta, one shared step
  (`consolidate_delta`) sums the weights of equal rows and drops the rows
  that cancel. Rows are equal when their identity and the bytes of the
  rest of the row match, so `2` and `2.0`, or `'a'` and `'A'`, stay
  different rows. A row inserted and deleted by one statement therefore
  never reaches state, and after consolidation a retraction always refers
  to a row the operator holds; operators treat any other retraction as
  corrupt state. Semi and anti joins sum each row's changes with their own
  step (see the point above). The step sorts the delta (a VDBE sorter) and
  writes each distinct row once into a new ephemeral table, so every
  stateful input pays one sort and one ephemeral insert per distinct row.
  The order of the consolidated rows does not matter: state that stores
  values is keyed by the exact value (see "Persistent state and collation
  classes"), so an update from `2` to `2.0` changes two independent keys.
- **The view keeps one row per identity.** A view row is stored under its
  identity (a rowid) with its values and a weight. That is a z-set only
  while the identity determines the values, so the merge checks it: a
  delta row whose identity is already in the view must carry exactly the
  stored values, and a second image for one identity is an error. An
  update under one identity frees the slot and fills it again, so the sink
  reads the delta twice, applying its retractions on the first pass and its
  insertions on the second. That makes it independent of the delta's row
  order.

## Persistent state and collation classes

Hidden tables are assigned by DAG node and purpose:

- primary operator state;
- aggregate value multisets;
- aggregate group members;
- anti-join match counts;
- set-operation source identity and members;
- output arrangements;
- one circuit-version marker.

The visible view rowid comes from a source identity, a state rowid, or an
arrangement rowid. Joined rows are not identified by content hashes.

SQL equality is not always identity. Under `NOCASE`, `'a'` and `'A'` share
one GROUP BY, DISTINCT, MIN/MAX, or set-operation class; under any collation
the integer `2` and the real `2.0` do too. The result shows one member of the
class, and SQL does not say which: a fresh query shows the one its plan reads
first, which changes when an index is added.

A z-set has no order, so maintenance cannot follow a plan. Instead, an
operator whose key class can hold several exact values keeps, per class, the
multiset of its exact members with their counts. That is the class's
integral. The class shows its smallest member compared as `BINARY`, with an
integer before an equal real. The rule depends only on the members present
now, never on the plan or on the order in which rows arrived; after the shown
member is deleted, the class shows the smallest one left. One helper module
(`vdbe_maintenance::members`) implements this for GROUP BY and DISTINCT keys,
MIN/MAX values, DISTINCT aggregate arguments, and set-operation keys.

Collation equality decides only which class a value belongs to. State that
stores values is keyed by the exact value (its type and bytes): member
tables, aggregate value multisets, and output arrangements. An update from
`2` to `2.0` or from `'a'` to `'A'` therefore retracts one key and inserts
another, exactly as in the z-set. A key whose values can never be equal
without being identical (a `BINARY` column whose affinity stores numbers in
one type) needs no member table.

## Divergences from DBSP

A DBSP-style aggregate combines its contributions by addition and undoes a
retracted row by subtraction, with no state that only ever moves in one
direction. SUM/TOTAL/AVG follow this rule in full: a materialized view
tracks an exact integer total, a floating-point total, and two row counts
(live non-integer contributions, live non-NULL contributions), every one of
them a plain running sum that adds on insert and subtracts on retraction.
This is IVM's own state, separate from the "approx"/"ovrfl" flags the
window-function accumulator uses for the same functions (`core/types.rs`'s
`SumAggState`): that flag is sticky by design (SQLite's window frames never
clear it once set) and must stay that way for window functions, but a
materialized view has to equal a fresh evaluation of its defining SELECT
after every change, including a change that removes the row that set the
flag.

One place this state intentionally disagrees with SQLite's own `sum()`:
`sum()` reports "integer overflow" only if no row processed *after* the
overflow was a REAL value, which makes the error depend on the order rows
happen to be scanned in, not just on the current set of rows (two batch
evaluations of the same table can disagree on whether `sum()` errors,
depending on the table's physical row order on each occasion; an inserted
REAL row "forgives" an earlier integer overflow for the rest of that scan).
A materialized view has no such per-query scan order, so IVM instead reports
"integer overflow" exactly when every live contribution is an exact integer
and their exact total does not fit in `i64`; a non-integer contribution in
the same group always yields a REAL result rather than an error, matching
`total()`'s behavior. See `finalize_ivm_sum` in `core/vdbe/execute.rs`.

Floating-point addition is not associative, so a materialized view's
incrementally maintained REAL result can differ from a fresh evaluation of
the same SELECT in the last bit when rows are combined in a different order.
`@verify-materialized-views` compares REAL values bit-for-bit with no
tolerance (see `query_result_oracle::Value`'s `PartialEq`), so this is a
real, open risk for SUM/AVG over REAL columns, not merely a theoretical one;
it is called out as unresolved in "Sharing VDBE instructions does not make
update order irrelevant" below.

## Supported SQL surface

The following are implemented and have focused SQL coverage:

- deterministic filters and projections using regular scalar expressions;
- INNER, CROSS, LEFT, RIGHT, and FULL joins, including self-joins,
  non-equality predicates, USING/NATURAL joins, multi-way joins, and mixed
  join trees;
- grouped and scalar `COUNT`, `SUM`, `TOTAL`, `AVG`, `MIN`, and `MAX`;
- DISTINCT aggregate arguments and aggregate `FILTER (WHERE ...)`;
- HAVING over grouping keys and supported aggregate results;
- expressions over aggregate results;
- `SELECT DISTINCT`, including over grouped and joined inputs;
- `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`, and mixed compound chains,
  including joined and grouped branches;
- FROM-clause derived tables and non-recursive CTEs;
- arbitrary compositions covered by the DAG contracts, including
  set-op-to-aggregate, aggregate-to-aggregate, aggregate-to-join, and
  join/set-op nesting;
- built-in `BINARY`, `NOCASE`, and `RTRIM` state-key collations, including
  replacement of a class's shown member after retraction;
- dependencies between materialized views;
- INSERT, UPDATE, DELETE, UPSERT/REPLACE, statement abort, savepoint/rollback,
  transaction rollback, and read-your-own-writes;
- bounded transaction-log spilling, DROP cleanup, reopen/schema-cache
  behavior, and initial population in the creating transaction.

## IVM-specific gaps relative to regular SELECT execution

These are rejected at CREATE time:

- window functions;
- ORDER BY, LIMIT, and OFFSET in the defining query;
- subqueries outside the FROM clause that the regular planner cannot turn
  into a join (`NOT IN`, uncorrelated subqueries, subqueries under `OR`);
- SELECT without a FROM clause and VALUES queries;
- virtual-table FROM sources and base tables with virtual generated columns;
- non-deterministic expressions;
- bound parameters;
- aggregate functions other than `COUNT`, `SUM`, `TOTAL`, `AVG`, `MIN`, and
  `MAX`, including `group_concat`, `string_agg`, JSON aggregates,
  percentile/mode aggregates, and other non-invertible aggregates;
- aggregates with more than one argument;
- result or HAVING expressions that use a bare input column outside the GROUP
  BY keys;
- connection-registered custom collations on grouping, DISTINCT, MIN/MAX, or
  set-operation state keys;
- materialized views on attached databases;
- materialized views under experimental MVCC mode.

Non-recursive CTEs and FROM subqueries are not gaps.
Recursive CTEs and writable `WITHOUT ROWID` tables are not counted as IVM gaps
because Turso's regular engine does not currently support the relevant
behavior either.

## Verification

All IVM conformance files opt into `@verify-materialized-views`. After each
successful statement, the native sqltest backend discovers every live
materialized view and compares:

```sql
SELECT * FROM materialized_view
```

with a fresh execution of its stored defining SELECT. The comparison is an
unordered typed multiset: it checks column count, row multiplicity, NULL versus
empty values, and integer/real/text/blob type identity.

There is one exception. In a column whose value the view chose among several
equal values (a class member, see "Persistent state and collation classes"),
the view and the fresh query may legitimately show different members. The
plan reports those columns and their collation
(`MaintenancePlan::chosen_value_collations`): GROUP BY and DISTINCT keys,
MIN/MAX results, and set-operation keys whose class can hold several values,
followed through operators that pass the value on unchanged. In those columns
only, the oracle (`query_result_oracle::diff_result_sets_with_chosen_values`)
treats two values as equal when they compare equal under the collation:
`NOCASE` folds ASCII case, `RTRIM` ignores trailing spaces, and an integer
matches an equal real. Every other column, every row count, and every value
computed from a chosen value still compare exactly.

The differential fuzzer uses the same shared result-set oracle. It maintains
multiple views, checks every one after every statement, recursively generates
supported operator compositions, treats rejection of a supported composition
as a failure, and performs leaf mutations to ensure every dependency is
driven. Successfully minimized failures must become permanent `*.sqltest`
regressions.

The verifier found genuine bugs in collated GROUP BY, DISTINCT, MIN/MAX,
UNION, INTERSECT, and EXCEPT state. Those minimized cases are in the SQL
corpus. Unrelated regular-engine fuzz failures remain documented separately
under `bugs/`.

## Remaining architectural risks

### A value computed from a chosen member is not relaxed

The oracle accepts any collation-equal member only in a column that shows the
member itself. A view that computes from it, such as `upper(x) || '!'` over a
NOCASE group key, or a later `BINARY` comparison against it in a join or
`HAVING`, can legitimately differ from a fresh query, because the fresh query
computes from a different member. Such a difference is reported as a failure.

### Sharing VDBE instructions does not make update order irrelevant

Incremental SUM/AVG maintenance applies inverse and forward steps in the
order of the consolidated delta, while a fresh query scans all surviving
rows in physical-plan order.
Integer arithmetic is mostly robust to that distinction, but floating-point
aggregation is not associative. Exact result equality can therefore expose a
schedule difference even though both paths use the same aggregate
implementation.

This needs an explicit policy and tests. If exact equality is the invariant,
order-sensitive aggregates need state sufficient to reproduce batch order or
must be recomputed when their order changes. Treating “same opcode” as proof of
the same result would be unsafe.

### Stateful bytecode emission is still hand-built

The DAG removes combination-specific planners, but aggregate, join, set-op,
arrangement, and sink emitters still manually manage cursors, register layouts,
hidden records, signed counts, and transition ordering. That is a much smaller
duplication surface than a second evaluator, yet it is the main
maintainability burden.

The existing `OperatorStateCatalog`, `NodeOutputContract`, and shared stream
materialization boundary are the right direction. New operators should extend
those declarative contracts rather than add query-shape branches or rediscover
hidden-table names in codegen.

### Performance work is architectural, not just tuning

Intermediate deltas are commonly materialized into ephemeral tables, join
probes do not yet have general predicate-keyed arrangements, and repeated CTE
references may duplicate subgraphs. Complex transactions can therefore do
large scans and write substantial temporary state.

This is not a correctness roadblock, but supporting large views efficiently
will require arrangement selection and sharing as planner decisions. Adding
one-off fast paths per query shape would recreate the architecture this rewrite
removed.

## Indexes on materialized views (not supported yet)

`CREATE INDEX` on a materialized view is rejected. The old engine accepted it,
but the index went stale on the first write and the database could not be
reopened afterwards (#10056), so rejecting it removes no working feature.

How to support it:

- A materialized view is stored as an ordinary table btree: one row per view
  row, keyed by rowid, with the view columns followed by a hidden weight
  column. An index on it is an ordinary index btree keyed by the indexed
  columns and the rowid, so the regular planner can already use it for reads.
- The work is in maintenance. The view sink (`vdbe_maintenance/sink.rs`)
  writes the view btree with plain `Insert`/`Delete`. It must also maintain
  every index of the view: delete the index entries of a removed or replaced
  row image, insert the entries of the new image. Reuse the regular DML
  index-maintenance code generation instead of writing a second copy.
- `CREATE INDEX` backfills from the existing view rows through the normal path.
  Schema loading already registers materialized views before indexes, so such
  indexes reopen correctly.
- Reject `UNIQUE` indexes on views: a view may hold equal values in different
  rows, so a unique violation would make an unrelated write to a source table
  fail. The hidden weight column must not be indexable.
- Partial and expression indexes come for free only if the shared DML code
  handles them; otherwise reject them explicitly.
- Tests: writes to source tables of every kind (insert, update, delete, a row
  moving between groups, NULL-padded rows appearing and disappearing), reads
  that use the index (check `EXPLAIN QUERY PLAN`), `PRAGMA integrity_check`,
  reopen, and an index on a view that feeds another view.

## Bottom line

The branch no longer has the old fatal design of two SQL evaluators. Its
compositional north star is clear: regular planning semantics, one typed delta
contract, node-owned state, and one VDBE program.

The likely future walls are narrower and visible: values computed from a
chosen class member, non-associative aggregate schedules, and scalable arrangement
planning. They can be addressed within the DAG architecture, but only if they
become explicit contracts rather than operator-local special cases.
