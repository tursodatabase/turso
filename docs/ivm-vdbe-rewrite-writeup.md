# Materialized-view maintenance as compiled VDBE

## Status

The old interpreted IVM engine has been deleted. Materialized-view maintenance
is now planned as a composable operator DAG and compiled into an ordinary VDBE
subprogram.

The branch currently has one unreleased hidden-storage format,
`DBSP_CIRCUIT_VERSION = 2`. Development changes that format in place; it must
not be bumped for each intermediate branch revision.

The operator-DAG rewrite is implemented. The current uncommitted slice adds
exact representative provenance for built-in collations. All six annotated
IVM files pass 267 of 267 cases with exact invariant checking. Broad workspace
checks still need to be completed before the slice is committed.

## The architectural boundary

The old engine duplicated SQL semantics. It had its own expression
interpreter, comparison and NULL rules, aggregate implementation, operator
runtime, and row-identity scheme. Every SQL rule could therefore diverge from
the regular query engine.

The rewrite removes that second evaluator:

1. `prepare_select_plan` and `optimize_plan` produce the same bound and
   optimized relational plan used by regular query execution.
2. The plan is lowered to a topologically ordered maintenance DAG containing
   `Scan`, `Alias`, `Filter`, `Project`, `Join`, `Aggregate`, and `SetOp`
   nodes.
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
Updates are an ordered removal of the old image followed by an insertion of
the new image. Failed statements rewind their log entries, savepoints rewind
to a mark, and rollback discards the log.

This reuses some DML emission hooks that are also useful to Change Data
Capture, but it does not create CDC rows, expose a CDC protocol, or route
maintenance through the CDC subsystem. The IVM change log is transaction-local
input to maintenance programs and may spill through the normal asynchronous
I/O path when it grows.

## Composition model

Linear nodes (`Filter`, `Project`, and `Alias`) transform their input delta.
Stateful nodes own their integral:

- aggregates own per-group accumulator state and, where needed, value
  multisets;
- deduplicating set operations own per-branch membership counts;
- LEFT joins own match state for NULL-padded rows;
- any node whose current output must be probed by a downstream join receives
  an output arrangement.

Each binary join consumes two arbitrary upstream nodes. Its delta follows the
product rule against the inputs' maintained current outputs, so joins of
joins, aggregates feeding joins, outer joins feeding aggregates, and
set-operations feeding joins use the same node implementation. Five-way joins
are already covered; the old four-table shape cap no longer exists.

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

## Persistent state and exact representatives

Hidden tables are assigned by DAG node and purpose:

- primary operator state;
- aggregate value multisets;
- aggregate group-representative provenance;
- LEFT-join match state;
- set-operation source identity and representative provenance;
- output arrangements;
- one circuit-version marker.

The visible view rowid comes from a source identity, a state rowid, or an
arrangement rowid. Joined rows are not identified by content hashes.

SQL equality is not always byte equality. Under `NOCASE`, for example, `"a"`
and `"A"` share one GROUP BY, DISTINCT, MIN/MAX, or set-operation equivalence
class, while a fresh query still exposes one exact source spelling. Counts
alone cannot recover that spelling after the chosen source row is retracted.
The current state therefore stores both:

- the collation-aware equivalence key used for membership; and
- source provenance plus raw values used to select the same representative as
  a fresh query.

Representative selection also depends on the access direction chosen by the
regular optimizer. Maintenance planning runs the normal optimizer and
propagates that direction into aggregate representative selection.

## Supported SQL surface

The following are implemented and have focused SQL coverage:

- deterministic filters and projections using regular scalar expressions;
- INNER, CROSS, LEFT, and RIGHT joins, including self-joins, non-equality
  predicates, USING/NATURAL joins, multi-way joins, and mixed join trees;
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
  exact representative replacement after retraction;
- dependencies between materialized views;
- INSERT, UPDATE, DELETE, UPSERT/REPLACE, statement abort, savepoint/rollback,
  transaction rollback, and read-your-own-writes;
- bounded transaction-log spilling, DROP cleanup, reopen/schema-cache
  behavior, and initial population in the creating transaction.

## IVM-specific gaps relative to regular SELECT execution

These are rejected at CREATE time:

- FULL OUTER JOIN;
- window functions;
- ORDER BY, LIMIT, and OFFSET in the defining query;
- scalar, correlated, EXISTS, or IN subqueries outside the FROM clause;
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

RIGHT JOIN is not a gap: the regular planner rewrites it to LEFT JOIN before
maintenance lowering. Non-recursive CTEs and FROM subqueries are not gaps.
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

The differential fuzzer uses the same shared result-set oracle. It maintains
multiple views, checks every one after every statement, recursively generates
supported operator compositions, treats rejection of a supported composition
as a failure, and performs leaf mutations to ensure every dependency is
driven. Successfully minimized failures must become permanent `*.sqltest`
regressions.

The verifier recently found genuine exact-representative bugs in collated
GROUP BY, DISTINCT, MIN/MAX, UNION, INTERSECT, and EXCEPT state. Those minimized
cases are now in the SQL corpus. Unrelated regular-engine fuzz failures remain
documented separately under `bugs/`.

## Remaining architectural risks

### Physical representative semantics are the closest correctness wall

The query result sometimes exposes an arbitrary member of a collation
equivalence class. Which member is returned can depend on index choice,
iteration direction, compound branch order, and operator nesting. The current
provenance tables fix the known cases, but `Aggregate::input_direction` is a
narrow encoding of a broader fact: each operator edge may need a stable
presentation-order contract.

If more optimizer behavior is copied into individual emitters as ad hoc flags,
the architecture will start drifting again. The north star is to make
representative choice explicit metadata of the regular plan/stream contract,
then have every stateful operator consume that contract mechanically.

### Sharing VDBE instructions does not make update order irrelevant

Incremental SUM/AVG maintenance applies inverse and forward steps in delta
order, while a fresh query scans all surviving rows in physical-plan order.
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

## Bottom line

The branch no longer has the old fatal design of two SQL evaluators. Its
compositional north star is clear: regular planning semantics, one typed delta
contract, node-owned state, and one VDBE program.

The likely future walls are narrower and visible: exact representative/order
semantics, non-associative aggregate schedules, and scalable arrangement
planning. They can be addressed within the DAG architecture, but only if they
become explicit contracts rather than operator-local special cases.
