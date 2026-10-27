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
7. Feed resolved HIR into the existing plan, optimizer, and emitter pipeline.
8. Switch the production entry point after coverage is complete.
9. Delete `Bound*`, source-ID conversion, and planner-era binding code.

## Planner handoff

Keep the existing plan algorithms, optimizer, and emitter. Replace only work
already completed by semantic analysis:

- Build existing planning source records from resolved HIR sources.
- Move SELECT plan expressions from parser AST to resolved HIR expressions.
- Assemble existing SELECT plans from HIR without name lookup, star expansion,
  alias or ordinal replacement, function lookup, or expression rewriting.
- Switch SELECT preparation to HIR in one step; do not run both binders.
- Remove the name-resolution half of `TableReferences`, keeping column usage,
  join planning, index selection, and other physical planning data.

Do not convert HIR expressions back into parser AST. That would discard
resolved function, type, collation, and comparison choices, forcing later
stages to repeat semantic work.

HIR planned sources keep their document-local `SourceId` directly. They do not
allocate a `TableInternalId` or maintain a compatibility map. IDs for
planner-created runtime objects may remain separate.

## Current status

The cutover is complete: every statement is prepared from validated HIR or
from schema translators over the semantic context, and the legacy planner,
binder, emitter, `TableReferences` and `Resolver` are deleted. The
sections below record what was audited and how each statement kind runs.

### After the merge of main

Main kept building on the legacy planner after this branch forked. The
merge keeps the HIR paths and carries main's behavior over them, proven
by main's own sqltests:

- FROM clauses: SQLite-style RIGHT and FULL JOIN chaining (any number of
  right-kept joins in a block, unmatched passes in FROM order, planned
  reads for the unmatched rows, `RIGHT-JOIN t` plan rows), USING and
  NATURAL with SQLite's first-copy and merged-column rules, parenthesized
  join groups, and LATERAL subqueries that see the sources on their left
  and run once per outer row.
- Materialized views: maintenance is compiled to VDBE over HIR
  (`core/incremental/vdbe_maintenance`). The operator DAG holds HIR
  expressions over source identities, delta rows are bound to registers
  with `SourceBinding::Registers`, and node expressions lower through the
  shared expression lowering. The DBSP engine is gone; "DBSP version" in
  error text names the maintenance format version.
- Hash joins: main's pricing and materialization rules, the deferred probe
  target for rows waiting on a partition on disk, hash anti-joins for
  NOT EXISTS, EXPLAIN QUERY PLAN estimates, and correlated EXISTS filters
  deferred to the cheapest later loop.
- Planner: partial index predicates proved by one OR branch, `col IN (...)`
  implied by every OR branch, `count(col)` as `count(*)` on a NOT NULL
  column, custom-type range terms kept out of automatic index keys, order
  proofs never from hash builds, and a rewrite that stays a subquery when
  an inner filter can fail.
- Semantics and DML: FILTER rejected on scalar functions, row-value IN
  NULL rules, comparison collation from the left operand with BINARY
  columns winning, HAVING terms over GROUP BY keys moved to WHERE, STRICT
  checks of generated columns, UPSERT targets on generated-column indexes,
  foreign keys checked only when a key changes, UPDATE OF triggers matched
  by the names written, per-trigger-step conflict modes, ALTER COLUMN
  conversion, the whole-table DELETE fast path, and sequence names
  qualified at CREATE.
- Index methods (FTS): a query names any subset of an index's columns, in
  any order, and the index receives the chosen columns after the
  placeholder values; `x MATCH y` matches patterns as `fts_match`; a
  search whose arguments read the table's own row, or a table that loops
  later, cannot drive the scan, so the term is checked through the index
  for each row the loop finds (`emit_hir_predicate`); a search whose
  arguments read earlier tables loops inside them; `fts_score` over the
  selected columns and query reads the index's score, and any other
  `fts_match`, `fts_score` or MATCH fails at prepare time with an error
  that names the FTS index.

The sqltest expectations that differ from main only in instruction
addresses or ephemeral index names (`_s<id>` for a HIR source, where the
legacy planner wrote `_t<id>`) were updated to the HIR output.

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
| None | Statement and whole-trigger-program inputs produce validated HIR from an owned multi-database semantic catalog, and every statement is prepared from it. The legacy planner was deleted on 2026-10-21. |

### What goes through HIR

Audited on 2026-10-21 from `translate_statement`, after the legacy planner
was deleted:

- SELECT always goes through HIR; there is no guard and no fallback.
- DELETE, UPDATE and INSERT always go through HIR as well, with their
  triggers and foreign-key actions; the HIR entry points return nothing and
  there is no fallback left. The last guards to go: `sqlite_dbpage` is
  written through HIR under the same rule as legacy (a cli_only build opened
  with unsafe testing on); a STRICT column whose type is neither built in
  nor custom, which CREATE TABLE rejects, is an error; a target with
  materialized views built by another DBSP version gets legacy's error; a
  virtual column of a custom type holds the value its expression computes
  from the decoded columns it reads, which is what it shows, what its index
  stores and what a seek looks for (legacy decodes the computed value once
  more, so `cents AS (a * 2)` reads 0 while its index holds 4 and a seek
  looks for 400); and UPDATE ... FROM of a virtual table writes each joined
  row's new values into a scratch table keyed by rowid during the join and
  hands them to the table afterwards, as SQLite does (legacy failed it with
  "no such column: rowid"). What cannot happen, such as a DML target that
  is not a table or an automatic index on the target's loop, is an internal
  error.
- A CTE a DML statement reads more than once, in UPDATE ... FROM and in its
  expression subqueries, is filled once before any row changes, as legacy
  fills it, so every reader sees the same rows; a correlated subquery still
  reads it again on each run, as SQLite does.
- The elements of an array of a custom type are encoded one by one when
  written, as legacy encodes them, so STRICT tables with such columns go
  through HIR.
- Schema statements (CREATE TABLE AS, CREATE VIEW, CREATE MATERIALIZED
  VIEW, ATTACH, ALTER TABLE, DROP TABLE, pragmas) prepare the SELECTs and
  DML inside them through HIR: DROP TABLE checks foreign keys by lowering a
  DELETE of the whole table without its triggers, and ALTER TABLE rewrites
  rows through an HIR UPDATE. There is no `translate_inner` and no legacy
  plan left to reach.

CI runs the conformance corpora through HIR (`.github/workflows/sqltest.yml`),
with the snapshot tests regenerated from HIR's bytecode.

### Cutover deletion map

SELECT has no remaining HIR planning guard. Its planner handoff does not use a
resolver, `TableReferences`, parser-expression conversion, binding, or
rewriting. Remaining parser types are frozen enums and literals already owned
by HIR.

The legacy SELECT and DML planners, their binders and emitters, and the
bind and rewrite passes were deleted on 2026-10-21. The legacy expression
binder and translator (`expr/binding.rs`, `expr/translator.rs` and the
condition, binary, custom-type and function emitters next to them) were
deleted the same day, once their last callers moved to HIR:

- ALTER TABLE ADD COLUMN checks a new virtual column's NOT NULL and CHECK
  constraints against existing rows through a planned HIR SELECT of the
  table, and DROP COLUMN finds a partial index's use of the column by name
  in its WHERE clause.
- ATTACH and DETACH evaluate their arguments through HIR only.
- Materialized view maintenance analyzes a view's SELECT through
  `analyze_root` with subquery unnesting forced, and binds each delta row
  to registers before lowering node expressions.
- The planner's unused WHERE, LIMIT and rowid parsers are gone.

What is left of `expr/` is the helpers both HIR and the planner share: AST
walking, the storage-class mask, literal emission and conditional jumps.

The parser-expression optimizer went the same day: `compute_best_join_order`
and its access-method, multi-index, constraint and ordering code over
`JoinedTable`, `WhereTerm` and `TableReferences`; the `Plan`/`SelectPlan`
types and the plan display; `Table::FromClauseSubquery`; the statement's
legacy result-column metadata (PRAGMA and change-count columns are now
resolved result columns like every other); and the program builder's
cursor overrides, CTE name stack, hash-build signatures and subquery result
registers. HIR planning had its own generic versions of all of them
(`HirPlannedSource`, `HirWhereTerm`, `HirTableConstraints`,
`compute_hir_join_order`), which `plan.rs` and `optimizer/` now hold alone,
with the shared `PlannedSource`, `PredicateTerm`, `SeekDef`, `BitSet` and
`QueryDestination` types.

`Resolver`, the last legacy data type, was deleted on 2026-10-21. Schema
statements, pragmas and transaction statements now look databases, tables,
indexes, triggers, functions and collations up through the same
`SemanticContext` that semantic analysis builds from the captured catalog,
so no statement resolves a name outside the binding step's catalog.

Nothing of the legacy data types remains.

Each prepared statement captures the catalog once (`capture_catalog` in
`translate/mod.rs`) and builds one `SemanticContext` from it and the
connection's settings. Schema statements read that context directly, and
the `HirSession` that analyzes, plans and lowers SELECT, DML and trigger
programs borrows it instead of capturing a catalog of its own. The one
exception is `PRAGMA integrity_check` on an MVCC main database, which reads
the latest shared schema: it builds a second catalog over that schema with
`SemanticContext::catalog_with_main_schema`. Change data capture's register
expressions, compiled outside any statement, capture their own catalog.

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

### Production entry point

Since the cutover on 2026-10-20, every connection prepares top-level SELECT
statements through HIR: the owned semantic catalog, `analyze_root`,
`HirPlan::build`, and HIR lowering. Statements the engine prepares inside
another one, such as the copy statements of VACUUM, take the same path; like
legacy, they may write internal tables. There is no fallback: a SELECT that
HIR cannot lower fails to prepare. Top-level DELETE, UPDATE and INSERT also
go through HIR. The legacy planner, `TURSO_LEGACY_PLANNER` and
`Connection::set_experimental_hir` were deleted on 2026-10-21: the SELECT
and DML planners, binders, emitters and optimizer passes that only legacy
used are gone, and the remaining `plan.rs` types, optimizer and emitter
helpers are the ones HIR lowering builds on.

Result-column names, table names, and declared types come from the HIR outputs
and the same alias, `full_column_names`, and `short_column_names` rules as
legacy. With `full_column_names` on and `short_column_names` off, `SELECT *`
and `t.*` name each column after its source during analysis, as legacy and
SQLite do: the alias, or the table name or `(subquery-N)` without one, so a
FROM subquery passes those names up (`(subquery-0).a.id`). RETURNING `*`
keeps plain names, as in legacy. Two known differences are legacy artifacts: legacy reports its internal
window subquery as the table of every output of a window query, and BLOB as
the declared type of expressions such as `count(*)` there. HIR reports the real
table and no declared type, like SQLite. `get_column_type_info` infers
the type of a computed output by legacy's rules (literals, arithmetic
widening, comparisons and `||`); `get_column_inferred_type` reports the
resolved affinity for expression outputs instead of legacy's AST-shape
rules.

Expression subqueries (scalar, row, EXISTS, IN) run where their expression is
evaluated, the way SQLite codes subselects: the statement plan is installed in
`ProgramBuilder`, and expression lowering emits the prepared subquery shell
right before reading its result. Uncorrelated subqueries run once through
`Once`; correlated ones run again on every evaluation, with outer values read
through the source bindings in effect at that point. An IN seek driven by a
subquery emits the same shell when its loop opens.

`scripts/hir-conformance.sh` runs every `.sqltest` file through HIR,
one process per file, and groups failures by cause. The first run on
2026-10-14 passed 13602 conformance tests and failed 568. On 2026-10-16 a run
failed 29, with every file running to the end; legacy passes all of them. On
2026-10-19 every test passes. The script passes `--normalize-plans` to the
runner, which compares EXPLAIN QUERY PLAN rows (text and `FORMAT=JSON`, in
tests whose SQL says `EXPLAIN QUERY PLAN`) with the ids of generated names
(`ephemeral_t_t3`, `SUBQUERY 2`) replaced, each parent's `SUBQUERY` rows
moved after its other rows, and the row ids renumbered in that order. Those
are the parts that differ between the two planners when they choose the
same plan: legacy numbers rows by instruction address and lists an
uncorrelated subquery before the loops, HIR after them. A different loop,
index, or join method still fails. Expectations written as `expect pattern`
in `unnest-correlated` leave the cursor and register numbers open for the
same reason.

Fixed since the 2026-10-15 run, each following SQLite:

- An unknown table in `x.y` reports "no such column: x.y" everywhere, where
  legacy says "no such table: x" except in one ORDER BY case.
- The ORDER BY of an aggregate query without GROUP BY, and ORDER BY terms
  after a first term that is the rowid of the only table, are dropped. Their
  names are still resolved, on a copy of the analyzer state that is then
  restored, but row value widths SQLite checks only while generating code
  are not checked, except in window calls, which SQLite moves out first.
- Named windows keep their syntax and are analyzed where a window function
  uses them, so they may read the block's aggregates; one no function uses
  is never analyzed.
- The ORDER BY sorter is skipped when the outermost window already returns
  rows in that order, so LIMIT stops before later window rows are computed.
- Grouped rows keep the columns a virtual generated column is computed
  from.
- A join step a virtual table rejects (a table function whose argument reads
  a later table) is skipped instead of failing the query.
- `INDEXED BY` a partial index the query cannot use fails with "no query
  solution", except in a plain `SELECT count(*) FROM t`.
- A WHERE term equal to a partial index's predicate does not keep the index
  from covering the query.
- `SELECT min(x)` or `max(x)` over one table reads an index on x in the
  aggregate's order and stops at the first non-NULL value. Legacy returns
  NULL for MAX over an index that sorts NULLs last.

Like SQLite, an aggregate written in a subquery whose arguments read only
columns of the block the subquery is in (`SELECT (SELECT count(t.a)) FROM
t`) belongs to that block, which then runs as an aggregate query; analysis
counts each block's aggregates on the analyzer so a subquery can add one to
its enclosing block, and lowering collects them from the block's
subqueries. Only the directly enclosing block is considered, and only where
its expression may hold aggregates (not WHERE).

`SELECT a FROM t HAVING max(a) > 0` returns the row's value where legacy
returns NULL (SQLite rejects it).

RIGHT and FULL joins use SQLite's nested-loop method instead of legacy's hash
join. The right loop records each matched rowid in an ephemeral table, and
after the main loops a second pass scans the right table, or the ephemeral
table of a materialized subquery, for rows not in it,
with the left tables reading NULL. As in SQLite, WHERE terms run only after the
match is recorded, and the second pass checks WHERE terms and the ON terms of
the inner joins written after the outer join, which apply to its NULL-extended
rows the same way (`LEFT JOIN t3 ON ... JOIN t4 ON t3.d IS t4.d` keeps the
rows whose t3 side is NULL only where t4.d is NULL too), but not the ON terms
of the outer join itself or of the joins before it.
HIR keeps legacy's FULL JOIN errors for shapes legacy has no hash plan for
(chaining, a recursive CTE reference, USING or NATURAL). It accepts correlated
subqueries next to FULL JOIN. HIR matches SQLite where legacy does not:
`a, b FULL JOIN c ... WHERE ...` (legacy returns extra rows), RIGHT JOIN
followed by another join (legacy returns columns in the wrong order), and
Both still reject a RIGHT JOIN after another join, during analysis. An outer
hash join takes its keys from its own ON terms only, in both engines: a WHERE
term used as a key was never checked on the rows the join NULL-fills, so
`t FULL JOIN u ON t.x = u.x WHERE t.x = u.x` returned those rows too.

EXPLAIN QUERY PLAN names each loop of a block before any cursor opens, as
legacy does, so simple plans print the same rows with the same ids. HIR
prints the rows legacy prints for a distinct aggregate (`USE HASH TABLE FOR
count(DISTINCT)`), a correlated IN probe (`CORRELATED LIST SUBQUERY`, with
the subquery's loops under it), and a compound query's ORDER BY (next to
`COMPOUND QUERY`, not under it). A CTE read by several loops, or hinted
MATERIALIZED, is filled before the block's loops open, as legacy fills it:
its body rows stand on their own, `FORMAT=JSON` lists them under
`cte_materializations`, every reader reports `materialized_reuse`, and the
reader that filled the table reads it through the same cursor while the
others open a second cursor on it. Row ids still differ where the bytecode
differs (see `--normalize-plans` above).

Like legacy, a LEFT JOIN outside parentheses whose NULL row a WHERE term
rejects (a comparison or arithmetic on one of its columns, but not IS,
IS NULL, CASE or a function) is planned as an inner join, so its tables can
be joined in any order and its ON terms seek like WHERE terms; lowering then
emits no NULL row for it.

Like legacy's OR-by-union, a table whose chosen loop is a plain scan is
instead read through one seek per branch of a WHERE OR term (`v = 5 OR w =
3` with an index on each column), when every branch can seek an index or the
rowid and together they cost less than the scan. The branches add the rowids
they find to a row set, and the loop reads each of those rows once. Unlike
legacy, this is decided after the join order, not while choosing it, so it
may pick a different table of a join than legacy does. When every condition
of every branch is applied by that branch's seek, the OR term is dropped from
WHERE, as legacy drops it, so a subquery in it fills its list once; otherwise
it stays in WHERE and is checked again on each row (HIR has no per-branch
filters like legacy's). It is never used on the right side of an outer
join. A DELETE or UPDATE whose loop reads rows this way first collects their
rowids, as legacy does, since the row set gives them in rowid order through
a cursor the statement is about to write through.

Like legacy's `optimizer/unnest.rs`, `semantic_unnest.rs` turns correlated
subqueries into joins before planning. It rewrites HIR into HIR: the
subquery's source keeps its identity and moves into the outer block's FROM
clause, so no reference changes, and the empty subquery is left detached
(`Query::is_detached`). For each block, innermost queries first, it plans the
document with and without the rewrite and keeps the rewrite when the block's
query costs no more, counting each remaining subquery's cost for the times it
runs plus `cpu_cost_per_seek` per call, as legacy's `estimated_cost` does.
With legacy's checks:

- `EXISTS` and a whole-term `IN` become a semi-join and `NOT EXISTS` an
  anti-join (`JoinKind::Semi`/`Anti`) when the subquery reads one source,
  only filters it, and links to the outer row by `=` terms; their loops come
  last and stop at the first match, as legacy's do. The subquery may read
  a table-valued function, whose arguments become its terms. A link to a
  source a LEFT JOIN (its right side) or RIGHT JOIN (its left side) fills
  with NULL keeps the subquery; FULL JOIN sides do not, and a block with a
  FULL join whose correlated subqueries all became joins always keeps that
  form, as legacy does. The SELECT list of an EXISTS is never run, so an
  uncorrelated subquery in it is dropped, and its parameters are still
  counted for binding (`HirPlan::dropped_parameters`). Because HIR keeps a
  RIGHT JOIN in its written order where legacy turns it into a LEFT JOIN,
  the costs of the two forms, and so the choice, can differ there.
- A block with a FULL join gets no aggregate rewrite, as in legacy.
- A one-value aggregate subquery becomes a FROM subquery grouped by its link
  columns and LEFT JOINed on them (group-first). When computing a key no
  outer row asks for could fail (`sum`, string aggregates, any function), a
  direct WHERE comparison instead LEFT JOINs the inner table and groups by
  the outer rowid (join-first). A copy of a rewritten subquery in the same
  clause reads the same grouped table, as legacy's `same_query` does.

What stays a subquery still runs fast:

- A correlated subquery that only asks whether rows match (IN, EXISTS, or an
  order-insensitive aggregate) in a SELECT statement is planned for one run
  per outer row, so it builds an automatic index once and seeks it each run.
- A correlated `x IN (SELECT y ...)` that is a whole WHERE term runs like
  `EXISTS (SELECT ... AND x = y)`: the planner adds `x = y` to the
  subquery's WHERE terms, so it stops at the first match and can seek `y`.
  It is not used when an expression of the subquery or `x` can fail, since
  the probe stops early.
- A correlated ungrouped aggregate in a SELECT statement caches its result
  in an ephemeral index keyed by the outer values it reads (each with its
  `typeof()`), so each key is computed once. Legacy has no such cache. It
  is not used when the subquery reads an outer aggregate, calls a function
  that is not deterministic, or reads a virtual table.
- When every ORDER BY term is a GROUP BY key with the same collation, the
  group sorter orders its keys that way, and no second sorter runs for
  ORDER BY.
- Grouped rows keep only the columns read after grouping: the keys, the
  aggregate arguments, and columns that outputs, HAVING, ORDER BY or windows
  read outside a key.
- A correlated subquery opens its cursors once, under `Once`, instead of on
  every run.
- Like legacy (`join_lhs_and_rhs`, `try_hash_join_access_method`), the join
  search may hash-join a table with the one joined before it: that table is
  built into a hash table, keyed by the `=` terms between the two, and the
  new table is scanned and probes it, when that costs less than its best
  nested-loop access, with legacy's eligibility rules (no index on a join
  column, no index hint, no USING, no table a correlated subquery reads, no
  probe-to-build chaining). In loop order the probe comes first and the
  build table's loop looks rows up; EXPLAIN shows `HASH JOIN <probe>` then
  the build table, as legacy does. A LEFT or FULL join builds from its left
  table, records the build rows its ON terms match, and after the loops
  runs the other build rows with the right side NULL, like HIR's RIGHT
  JOIN; a FULL join also NULL-fills the build row for a probe row without a
  match, and must hash when it can. A hash table that outgrows its budget
  moves rows to disk; lookups that need them wait, and after the probe loop
  ends they run again by its rowid, so both tables must be rowid tables
  without custom types. The hash table is built once per statement from
  every row of its table, so, as in legacy, subqueries hash too, and the
  probe loop may run under earlier loops: each pass joins only its own
  waiting rows and keeps the in-memory part of the table for the next pass
  (the VDBE used to free it and keep the waiting rows of earlier passes,
  which gave legacy wrong rows too). A LEFT hash join under earlier loops
  starts each pass with no build row matched and ends it with its own
  unmatched build rows. A FULL hash join stays the block's first loop: the
  probe rows it adds must come out once, which legacy gets wrong. A block
  with another RIGHT or FULL join does not hash. The build keys need no
  Bloom filter.
- Legacy's materialized build input (`HirHashBuildInput`): when the build
  table was the probe of an earlier hash join, or has a term with an
  earlier table that the hash keys do not cover, the loops before the
  probe run once into a temporary table that the hash table is built from,
  with legacy's cost and its check after the join search. Each hash row
  carries the columns the query reads of every table those loops supply,
  so the block's loops are the probe scan and the lookup, and the query
  reads those tables from the hash row (`SourceBinding::Registers`),
  including GROUP BY keys, bare aggregate columns and a later loop's seek.
  EXPLAIN lists the loops that fill it under `MATERIALIZE hash build input
  for <table>`; legacy then lists those tables once more after the loops,
  which HIR does not (they are not read again), and nests a chain's
  inputs inside each other where legacy lists them one after another.
  The loops that fill it run their own LEFT JOINs, and a LEFT JOIN probe
  over such a hash table gets its unmatched build rows from the temporary
  table, keyed by its rowids. Unlike legacy, HIR does not materialize in a
  block that reads an outer row: legacy fills the input once and reuses
  it for every outer row, which returns wrong rows.

These planning rules are ports of legacy's, sharing its cost formulas and
order checks where the code allows:

- A grouped block aims for its GROUP BY order even with an ORDER BY
  (`compute_order_target`), so an index on the keys replaces the GROUP BY
  sorter; when every ORDER BY term is a key, the keys in ORDER BY order and
  directions replace both sorters.
- `x IS NULL` (and `x ISNULL`) seeks an index on x, with `IS`'s outer-join
  rule.
- Like legacy's multi-index OR, the join search weighs reading a table as
  one seek per branch of an OR term against its best other access, with
  the WHERE work each leaves on both sides, and a branch whose leftover
  condition runs a subquery keeps the table scanned.
- A FROM subquery or CTE that earlier loops constrain is stored once and read
  through a temporary index on those columns when that costs less
  (`find_best_access_method_for_subquery`), except when it reads an outer row
  or is the right side of a RIGHT or FULL JOIN. HIR builds the index from the
  stored table, so setup costs two passes. A correlated subquery counts the
  rows of an outer FROM subquery or CTE as its number of runs.
- A scanned FROM subquery or CTE returns its rows in the order of its ORDER
  BY, its GROUP BY keys, the one subquery it passes through, or the plain
  scan of its one table (an index's columns and then the rowid, or the
  rowid alone, when the subquery exposes them), so an outer ORDER BY in
  that order needs no sort; a CTE stored once is read backwards for the
  opposite order. A subquery read through the temporary index over its
  stored rows returns each seek's rows in the index's order after the seek
  prefix, in either direction, so an outer ORDER BY on the earlier loops'
  columns and then the seek column needs no sort either.
- A block with windows aims for the innermost window's PARTITION BY and
  ORDER BY order, like legacy's window source subquery, so an index or an
  ordered CTE replaces that window's sorter. EXPLAIN shows `USE SORTER FOR
  ORDER BY` for each window that sorts its input, innermost first, as
  legacy shows each source subquery's sort; legacy's `SCAN
  window_subquery_N` rows, which name those subqueries, have no HIR
  counterpart.
- A LEFT JOIN keeps the order of the loops before it for ORDER BY terms that
  do not read its own columns (`plan_satisfies_order_target`).
- When join orders cost the same, the FROM-order plan wins, and other plans
  replace it only when strictly cheaper, in legacy's map order.

Three queries where legacy returns wrong rows return SQLite's rows in HIR
(see `hir_correlated_in_probe_and_cached_aggregates_match_sqlite`): an IN
whose subquery joins two tables, and grouped tables for a subquery that reads
an outer aggregate or joins two tables.

Like legacy, a FROM subquery, a single-reference CTE and a recursive CTE run
as coroutines, so the outer loop pulls rows on demand and an outer LIMIT can
stop an endless recursive CTE. HIR still materializes them in blocks with an
outer join, where legacy can stream some of them.

A block that reads only one table, without GROUP BY or an index hint, is
matched against the query patterns of the table's index methods, such as
FTS (`hir_index_method.rs`), as legacy's fast path does. A pattern is SQL
like `SELECT fts_score(title, body, ?1) AS score FROM docs WHERE
fts_match(title, body, ?1) ORDER BY score DESC LIMIT ?`; it matches when the
block's WHERE term, ORDER BY and LIMIT have its shape, each `?` standing for
an expression of the query. The loop then runs `IndexMethodQuery` with those
expressions and reads the table through `DeferredSeek`, and every expression
of the block that a pattern column stands for, such as `fts_score(...)`, is
read from the index cursor. Without the index `fts_score` is always 0. A
pattern with ORDER BY or LIMIT is used only when the index applies every
WHERE term and rows leave the scan as output (no aggregate, DISTINCT or
window). Where legacy differs, HIR is right: FTS columns may come in any
order in a result column too (legacy reads 0 for the score then), the same
`?1` must stand for the same expression in every place, and a query OFFSET
the pattern lacks keeps the pattern from applying LIMIT (legacy drops the
OFFSET). In an inner join without parentheses or GROUP BY, a table whose
WHERE term matches a pattern without ORDER BY or LIMIT is read first,
through the index; the other tables are planned after it, treating its
columns as known values, so join terms can seek them. Legacy weighs this
against other join orders; HIR always takes it. A DELETE or UPDATE whose
WHERE term matches a pattern without ORDER BY or LIMIT reads its target
through the index too, and like legacy collects the rowids first, since the
index may stream its rows lazily. Legacy writes rows while scanning an index
method whose results are stored before the scan starts (the toy vector
index); HIR collects their rowids first as well. Like legacy, `x MATCH
'pattern'` in a WHERE or ON term stands for `fts_match`, so `x` must be a
column, or the first of a row of columns, of a table with an FTS index;
any other MATCH term fails with legacy's "unable to use function MATCH in
the requested context". A MATCH outside those terms, such as in a result
column, runs as the function in both engines.

### DML through HIR

DELETE from a B-tree table is lowered from HIR
(`semantic_lowering/delete.rs`). It follows legacy's bytecode: every index is
opened for writing, index keys are read from the table row, the entry of the
index the loop reads is removed after the row, and RETURNING rows wait in an
ephemeral table until every row is deleted. A WHERE clause with a subquery
first collects the rowids into a RowSet, then deletes them. HIR reads
RETURNING values before the row is deleted; legacy reads a copy of the row
after, which gives the same values when RETURNING has no subquery.

A virtual table is written as legacy writes it: it is opened with `VOpen`
and `VBegin`, and each row goes to the table's update method with
`VUpdate`. A DELETE passes the rowid of each row the loop finds; an UPDATE
passes the old rowid, the new rowid and every column, reading the ones it
does not set from the loop's cursor; an INSERT passes NULL, the given rowid
or NULL, and every column. Where legacy differs, HIR follows SQLite: every
VALUES row is inserted (legacy keeps the last), INSERT ... RETURNING
returns the values given, and DELETE or UPDATE ... RETURNING fail with
SQLite's error (legacy panics or returns nothing). `sqlite_dbpage` may be
written only by a cli_only build opened with unsafe testing on; any other
write to it is rejected as a write to a protected table.

Change data capture (`semantic_lowering/cdc.rs`) writes the rows legacy
writes. Semantic analysis resolves the capture table, and the sequence that
hands out its change ids under MVCC, onto each INSERT, UPDATE and DELETE
target (`hir::CaptureChanges`), except the capture table and its version
table. Each changed row gets an INSERT, UPDATE or DELETE row with the images
the mode keeps, written where legacy writes it: an INSERT after its AFTER
triggers, an UPDATE after its foreign-key actions and before its AFTER
triggers, a DELETE or a REPLACE before the row is deleted. A row an UPDATE
or UPSERT moves to a new rowid is captured as deleted and inserted. A
statement in autocommit mode then writes one COMMIT row. Where legacy is
wrong, HIR is not: legacy also writes a COMMIT row from each trigger and
foreign-key action it runs, files a moved row's new image under `before`,
and gives UPSERT DO UPDATE the new row as its before image. The `ALTER
TABLE` rewrite of `sqlite_schema` records the ALTER statement as the new
`sql` value, as legacy does.

Writes to a virtual table record no change data. Legacy records none
either, but writes a COMMIT row after each UPDATE and DELETE of one.

A table with materialized views is written like any other: the VM updates
the views as rows are inserted and deleted. A table with a dependent view
built by another DBSP version cannot be written: the statement fails with
an error naming the views, as before.

There is no other DML path. A write to `sqlite_schema` or to one of the
engine's internal tables fails with "table X may not be modified", unless
the engine itself runs the statement, as VACUUM does. The planner never
reads a DELETE or UPDATE target through an automatic index; a plan that did
would be an internal error, not a fallback.

UPDATE of a B-tree table is lowered from HIR (`semantic_lowering/update.rs`).
The new row
is built in registers, NOT NULL and UNIQUE are checked with the statement's
or the constraint's conflict mode (ABORT, FAIL, ROLLBACK, IGNORE), and only
indexes whose key or WHERE term reads a changed column are rewritten: every
new key is checked before any index changes, then old entries are deleted and
new ones inserted, as in legacy. HIR's NEW-row source is bound to those
registers, so new index keys, partial-index WHERE terms and RETURNING read the
new values. Like legacy, an UPDATE whose WHERE clause has a subquery first
collects the rowids it changes, so the subquery never sees a changed row; a
subquery in SET runs as the loop reaches each row, and the row of
`SET (a, b) = (SELECT ...)` is computed once per row. An UPDATE that sets the rowid
collects its rows first, checks that no other row has the new rowid (REPLACE
deletes that row after the indexes are checked), rewrites every index entry,
since each holds the rowid, and deletes and inserts the row under its new
rowid. An UPDATE that changes the key of the index its loop reads, or
that may REPLACE rows while reading an index, also collects its rows first,
as SQLite does (legacy loses a row when a constraint's REPLACE runs during
such a loop). UPDATE ... FROM stores the new rowid in its scratch table with the
other new values. Like SQLite, when the rowid is set twice, as `id` and
`rowid`, the last assignment wins. A RETURNING subquery of UPDATE ... FROM that is not correlated
runs once, as in SQLite (legacy ran one that read the target through a
derived table again for each row).

INSERT of VALUES rows or DEFAULT VALUES into a rowid B-tree table is lowered
from HIR (`semantic_lowering/insert.rs`). Several VALUES rows run as a
coroutine, as in legacy, so the row code is emitted once. The rowid comes
from the statement (checked against existing rows) or NewRowid; NOT NULL,
PRIMARY KEY and UNIQUE are checked with ABORT, FAIL, ROLLBACK or IGNORE before
anything is written, then every index entry and the row are written. The
target source is bound to the new row's registers for index expressions,
partial-index WHERE terms and RETURNING. As in legacy and SQLite, when
several VALUES rows read the target table (through a subquery) or the INSERT
fires triggers, every row is computed into an ephemeral table before any is
inserted.

CHECK constraints are checked as in legacy: an INSERT checks all of them once
the rowid is known, before NOT NULL; an UPDATE checks, after NOT NULL, only
those that read a changed column; an UPSERT DO UPDATE checks all of them with
ABORT. A NULL result passes, IGNORE skips the row, and REPLACE aborts. A
failure reports the constraint's name, or its expression as the user wrote
it. `PRAGMA ignore_check_constraints` turns them off.

A STRICT table gets no column affinity; like legacy, its new row is type
checked (`TypeCheck`) instead: once the rowid is known for INSERT, after NOT
NULL for UPDATE, and in place of affinity for UPSERT DO UPDATE. A value is
converted where SQLite allows and the statement fails otherwise.

Virtual generated columns are computed into the new row's registers, in
dependency order, once the row is built (for UPDATE also again after BEFORE
triggers), so index keys, NOT NULL checks and triggers that copy registers
see them; reads through the row's source compute them anyway. The record
holds only the stored columns, in their stored order. An UPDATE treats a
virtual column as changed when a column it reads changes.

AUTOINCREMENT is lowered as legacy does it (`semantic_lowering/autoincrement.rs`).
Each row opens `sqlite_sequence` and reads the target's row; a generated
rowid is one more than the larger of that value and the table's largest
rowid, and is stored at once; a rowid from the statement raises the stored
value before any constraint is checked, even if the row is then skipped;
after the row and its triggers the stored value is raised to the row's
rowid. An INSERT of several rows first adds the target's row with 0.
Every AUTOINCREMENT table also has a hidden sequence for MVCC; like legacy,
it is used only when the target's database runs MVCC. Then it hands out new
rowids and keeps `sqlite_sequence` itself, and a rowid from the statement, or
one an UPDATE or UPSERT DO UPDATE moves a row to, advances it.

Like SQLite, a DEFAULT on an INTEGER PRIMARY KEY is never used: a row that
does not give the rowid gets a new one (legacy uses the DEFAULT for DEFAULT
VALUES).

A STRICT table checks the type of each stored column once the rowid is
known; like SQLite, a virtual generated column is not checked.

INSERT into a table with custom types follows legacy: once the rowid is
known, each value is checked against the type its column accepts as input,
encoded with the column's type programs, and checked again against the
stored base type. NOT NULL checks the decoded value, so an encoding that
decodes to NULL is rejected; CHECK constraints and foreign-key checks
compare stored values (`with_stored_values`), while index expressions,
RETURNING and AFTER triggers see decoded values. Trigger programs decode
OLD and NEW themselves, so DELETE and UPDATE pass OLD as stored, and
foreign-key actions get the parent key decoded, as SQL values. UPDATE does
the same after NOT NULL, but only for the columns it sets; the others are
read and written back as stored. Like legacy, its NOT NULL check sees the
value before it is encoded. An UPSERT DO UPDATE reads the conflicting row
and `excluded` decoded, so, like legacy, it checks, encodes and checks every
column of the updated row, not only those it sets. A BEFORE trigger is
compiled to read NEW without decoding it, and is given NEW as SQL values:
the INSERT row before it is encoded, the UPDATE row with the columns it
does not set decoded, and a decoded copy of the UPSERT row. Virtual columns
of an UPDATE are computed from that decoded copy too. Legacy instead shows a
BEFORE UPDATE trigger the unchanged columns as stored, and computes virtual
columns from values decoded twice or not at all. The
STRICT/custom-type tests are skipped by the Rust conformance backend, so
they were checked with the CLI backend, statement block by block against
legacy. In queries, GROUP BY keeps the grouped rows' columns as stored, so
aggregates decode them once, and BETWEEN on a custom type is `x >= a AND
x <= b` with the type's operators, as in legacy.

An array column of a built-in element type is written as legacy writes it:
checked as ANY, parsed with `ArrayEncode` against its element type (only
that the elements are arrays, for the outer array of a multi-dimensional
one) and checked as a BLOB; UPDATE re-encodes only the arrays it sets, and
`SET a[i] = v` is `ArraySetElement`. In an UPSERT, `excluded.a || a` joins
the arrays, where legacy joins their text. An output reading an array column
of a custom type decodes each element for display, as legacy does.

INSERT into a WITHOUT ROWID table is lowered too. As in legacy, only the
primary key is checked (`NoConflict` on the table) and the record holds the
key columns first; foreign keys, REPLACE, UPSERT, change data capture,
materialized views and secondary indexes give legacy's errors. HIR also
checks every column of a composite key, where legacy let a duplicate in, and
accepts INSERT OR IGNORE, which legacy rejects.

A hidden column, as legacy treats it, takes no position in VALUES without a
column list and gets NULL rather than its DEFAULT.

A subquery in RETURNING whose own FROM clause names the target table runs
again for each row, as in SQLite and legacy, so it sees the rows written so
far; any other runs once. DELETE then reads RETURNING from a copy of the row
taken before the row and its index entries are deleted, so the subquery no
longer sees the row. A virtual column read through that copy is computed
from the copied columns (legacy returns another column's value there).

Triggers fired by these statements are compiled from HIR too
(`semantic_lowering/trigger.rs`). Each trigger is analyzed as its own HIR
document whose OLD and NEW sources are bound to registers, and is emitted as a
subprogram that the statement runs with `Program`, as in legacy. The
subprogram loads OLD and NEW from its parameters, tests WHEN, and lowers each
command with the same DELETE, UPDATE and INSERT code; a command's own
triggers are compiled the same way, and a trigger already being compiled is
not fired again. TEMP triggers fire first. Every command of every trigger is
checked before anything is emitted, so a command that cannot be lowered
fails the whole statement before any bytecode is written. As in SQLite, OLD.x and NEW.x compare without the
column's affinity (a rowid alias keeps INTEGER), and triggers stored in the
temp schema find unqualified tables by the normal temp-then-main lookup.

With triggers, DELETE and UPDATE collect the rowids first, then for each row
read the OLD row, run BEFORE triggers, check that the row still exists, write,
and run AFTER triggers. UPDATE reads the columns it does not set again after
BEFORE triggers and checks NOT NULL only then. INSERT runs BEFORE triggers
with NEW.rowid -1 when the rowid is not given yet.

With `PRAGMA foreign_keys = ON`, DELETE and INSERT check foreign keys the
way legacy does (`semantic_lowering/fkey.rs`), reading key values through the
HIR target source. Before a row is deleted, child rows that still point at
it are counted as violations (RESTRICT stops at once), found through a child
index that lists exactly the child key, or by scanning the child table; a
deleted child of a deferred constraint whose parent was missing takes back
its violation. Before a new row is written, its parent must exist: an
immediate constraint stops the statement, a deferred one adds a violation. A
row may be its own parent. After a new row is written, children of a
deferred or self-referencing constraint that point at its key take back
their violations. With foreign keys off, DML carries no foreign-key metadata,
so a child of a missing parent table can still be written.

Foreign-key actions (ON DELETE and ON UPDATE CASCADE, SET NULL and SET
DEFAULT) are compiled as legacy builds them (`semantic_lowering/fkey_action.rs`):
each is the SQL statement it stands for on the child table, matching the
child key against parameters (`DELETE FROM child WHERE c = ?1`, or an UPDATE
that sets the child key to the new key, NULL or its DEFAULT). That statement
is analyzed and lowered through HIR, with its own triggers and actions, into
a subprogram; an action reached again while it is being compiled, as in a
table that references itself, calls the program being built. A DELETE reads
the parent key before the row is deleted and runs the actions after; an
UPDATE runs them once the row is written, when the key changed, before
RETURNING. A statement with actions collects its rows first, and the
parent-side checks skip keys with an action. An UPSERT's DO UPDATE runs the
ON UPDATE actions as UPDATE does. Where legacy differs from SQLite, HIR
follows SQLite: a DELETE whose SET DEFAULT leaves a child without a parent
fails.

REPLACE is lowered for INSERT and UPDATE as legacy does it. The row the new
row conflicts with is deleted inline, with its index entries and the
foreign-key checks of deleting it (no DELETE trigger fires); an INSERT writes
an index whose conflict resolves to REPLACE as soon as it is checked, and an
INTEGER PRIMARY KEY with its own ON CONFLICT REPLACE after the indexes. NOT
NULL ON CONFLICT REPLACE stores the column's DEFAULT, resolved in semantic
analysis. UPDATE OR REPLACE first collects its rowids; after deleting the
other row an UPDATE seeks back to its own. The deleted row's ON DELETE
actions read its parent key before the delete and run right after it.

UPSERT is lowered as legacy does it. The constraints an ON CONFLICT clause
names are checked first, in clause order; a conflict on one goes to the first
clause naming it, or to a final catch-all clause. Like SQLite, any other
conflict resolves as without the UPSERT, by the statement's OR clause or the
constraint's own ON CONFLICT (legacy drops the UPSERT under OR IGNORE and stops
the statement on those conflicts). DO NOTHING skips the row. DO
UPDATE seeks to the conflicting row, reads it through the target and the new
values through `excluded`, builds the new row in registers bound to the
UPSERT's NEW-row source, checks foreign keys and every changed UNIQUE index
(with ABORT) before rewriting the indexes and the row, and returns the
updated row to RETURNING. INSERT triggers do not fire for it; the UPDATE triggers of the
columns it sets do, with the conflicting row as OLD and with ABORT: BEFORE
once the new row passed its CHECK constraints, AFTER once RETURNING has read
the row, as in SQLite (legacy fires them before RETURNING). A DO UPDATE that
sets the INTEGER PRIMARY KEY moves the row as UPDATE does: the new rowid must
be free (checked with ABORT before the indexes), every index entry is
rewritten, and the row is deleted and inserted under its new rowid. As in
SQLite, `rowid`, `oid` and `_rowid_` name the rowid there unless the table
has a column of that name, also on a table without an INTEGER PRIMARY KEY
(legacy rejects them). The ON
UPDATE actions of a parent key it changes run once the row is written,
before RETURNING.

Semantic analysis matches each ON CONFLICT target against the HIR target
source, as SQLite does: `rowid` or the bare INTEGER PRIMARY KEY column names
the rowid; otherwise the first UNIQUE index whose key has the same terms, in
any order, with the same collation and an equivalent WHERE clause for a
partial index. Unlike legacy, `ON CONFLICT(rowid)` is accepted, an unknown
column reports "no such column", and `a COLLATE x` on the INTEGER PRIMARY
KEY does not name the rowid.

INSERT ... SELECT runs the query as a coroutine that yields one row at a
time into the same row code as VALUES. When the query reads the target table,
or the INSERT fires triggers, the rows are first copied into an ephemeral
table, as legacy and SQLite do.

UPDATE ... FROM first runs the join of the target and the FROM clause (the
same loop emission as SELECT, `emit_dml_join_loops`), writing the assigned
values of each target row into a scratch table keyed by its rowid, then
updates each row of the scratch table. A target row joined more than once
keeps the values of the last joined row, so the join order decides it: the
target is planned first, as legacy ends up ordering it, except when the FROM
clause has a RIGHT or FULL JOIN, which must not NULL-extend the target. Like
legacy, a FROM clause that names a table twice, or whose NATURAL/USING column
is also exposed by two other tables, is rejected up front.

HIR DML does not use the deleted `Resolver`, `TableReferences` or AST
rewriting. Its settings (functions, dialect, custom types, double-quoted
strings, search path) come from the connection through `HirSettings`, the
checks that a target may be written run in `semantic_lowering`, and the
small bytecode helpers it shares with legacy (foreign-key probes, UNIQUE
error text, statement journal flags) are copied into
`semantic_lowering/emit.rs`. It still shares plain types with HIR SELECT
lowering, such as `ConditionMetadata` and `QueryDestination`.

### Schema statements through HIR

Schema statements have their own translators over the semantic context,
the same captured catalog semantic analysis uses, and the parts that read
or write table rows are planned through HIR:

- The UPDATE of `sqlite_schema` that ALTER TABLE generates is lowered from
  HIR, writing the new CREATE statement into the `sql` column.
- The SELECT of `CREATE TABLE ... AS SELECT` is planned through HIR
  (`PlannedSelect`) and runs as a coroutine. Column names follow the alias
  rules (a plain column read keeps its own name), and types come from each
  output's affinity.
- CREATE INDEX and REINDEX fill the index from `SELECT <index columns>,
  rowid FROM <table> NOT INDEXED [WHERE <partial condition>]`, planned
  through HIR. NOT INDEXED keeps REINDEX reading the table, not an index it
  rebuilds. Index columns are read as DML writes them: a plain column as
  stored, so a custom-typed value is not decoded, and an expression or
  virtual generated column computed from decoded values. The rowid output
  is written into the analyzed HIR after name resolution, so a table whose
  real columns take all of `rowid`, `_rowid_` and `oid` is filled the same
  way.

### Non-lowering phase complete

Validated HIR and HIR access planning now cover query roots, INSERT-owned
queries, top-level UPDATE and DELETE, and ordered trigger commands.

No legacy binding code is safely deletable before production cutover.
`translate_inner` still enters the AST plan builders, and the current emitter
still consumes parser expressions, `Plan`, and `TableReferences`.

The next phase must replace that production handoff. Only then can we delete:

- SELECT binding and rewriting in `select.rs`
- DML binding and rewriting in `insert.rs`, `update.rs`, and `delete.rs`
- `expr/binding.rs`
- name-resolution state and methods from `TableReferences`
- parser-expression fields retained solely by legacy plans

(All of these were deleted on 2026-10-21; see the cutover deletion map.)

This boundary must not be crossed by running HIR and legacy planning together,
or by converting HIR expressions back into parser AST.

Completed:

- HIR output references now read an already-evaluated register bound by
  `OutputId` in `ProgramBuilder`. Expression lowering never follows the output
  expression or evaluates volatile output expressions again; whole-query
  lowering will establish and clear these bindings around each query block.
- Aggregate and window result reads now use their stable HIR identities to
  find the result registers owned by `ProgramBuilder`. This replaces the
  legacy structural expression cache while preserving its unconditional copy
  and avoiding a second evaluation of arguments or filters.
- Scalar, row-value, and EXISTS subqueries now read runtime results through a
  `QueryId`-keyed binding in `ProgramBuilder`. Row widths are checked against
  the validated HIR query output, and row copies reuse the legacy target-range
  invariant instead of converting the subquery back into parser AST.
- Standalone HIR expression lowering now consumes frozen custom-column,
  custom-CAST, and custom-binary-operator programs directly. Custom binary
  calls preserve legacy operand swapping, literal encoding, result negation,
  and target-copy behavior while calling the exact function resolved by
  semantic analysis. No resolver, parser expression, or `TableReferences`
  lookup enters these paths.
- Merged USING/NATURAL columns now own resolved expressions for both sides.
  LEFT and RIGHT joins lower only the selected value; FULL joins short-circuit
  the same target register across both values. Validation still checks both
  owned expressions and requires the right side to retain a source-column
  identity.
- Trigger predicates and commands now have ordered HIR planner roots. SELECT
  commands retain their `QueryId`; UPDATE and DELETE commands reuse resolved
  target access planning without turning trigger OLD/NEW pseudo-sources into
  physical scans.
- Top-level DELETE now uses resolved HIR for target access selection. Its
  predicate, index hint, column-use counts, and target `SourceId` flow directly
  into the shared access planner without `TableReferences`, binding, rewriting,
  or parser-expression conversion.
- Top-level UPDATE now uses resolved HIR for target and `FROM` access selection.
  The NEW-row pseudo-source remains semantic write state and never becomes a
  physical scan source.
- HIR planning now starts from the owned document rather than a caller-supplied
  query ID. Query, DML, and trigger roots plan every reachable query once, while
  documents without query work produce an empty query-plan list.
- Parenthesized FROM groups now pass through complete HIR access planning.
  Outer-join predicates owned by a group become ready only when the current
  physical leaf completes that group, including nested and single-leaf groups;
  they remain residual predicates rather than unsafe leaf access constraints.
- HIR planner input now walks parenthesized FROM groups recursively. Physical
  leaves stay flat for the existing join planner, while nested group ranges,
  parent boundaries, join kinds, and outer-join predicate ownership remain
  explicit without a fake group cursor or a copied join tree.
- Greedy HIR join ordering now accepts those group boundaries. Constrained
  groups wait for their left side, nested prerequisites cannot deadlock an
  active parent group, and a started group finishes before planning continues
  outside it. Access selection still operates on the same flat physical leaves.
- HIR constraint and predicate masks now use one flat FROM layout instead of
  searching only the top-level semantic FROM list. Group leaves map to their
  physical positions, while nested RIGHT and FULL null extension stays local
  to the parenthesized group that owns the join.
- Temporary scope columns are now named and stored as expression bindings.
  Parenthesized FROM-group names resolve directly to their inner HIR column or
  merged-column expression, so the group source is only join structure and
  never becomes a fake runtime column source.
- Table-valued function calls now freeze hidden-column argument predicates in
  HIR. NULL uses `IS NULL`; other arguments use ordinary resolved equality
  semantics. HIR planning consumes those predicates with existing outer-join
  ownership and virtual-table access selection, without parser-expression
  rebinding.
- Expression subqueries now participate in HIR query dependency planning.
  Scalar, row, EXISTS, and IN subqueries are planned before their owning query
  through the iterative HIR expression walker; their resolved expression shape
  and comparison rules remain in the semantic document.
- Recursive CTE seed and arm queries now plan before their owner. The outer
  recursive source and each occurrence-local queue input retain `CteId`
  directly; queue ordering, comparison collations, compound operators, and
  LIMIT remain owned by HIR without a fake table identity or legacy recursive
  plan.
- HIR planning now resolves non-recursive CTE materialization into explicit
  `PerReference`, `Shared`, or `Explicit` policy. Reference counting stops at
  CTE-body boundaries, outer captures prevent sharing, and `NOT MATERIALIZED`
  keeps the legacy behavior of allowing safe multi-reference sharing.
- Non-recursive CTE sources now retain their `CteId`, body `QueryId`, and
  materialization choice in HIR planning. The body is planned before each
  referencing query block, and CTE scans reuse the resolved query-backed
  constraint, cardinality, and cost path without synthetic legacy subquery
  tables.
- Derived-query scans now collect resolved binary and `IN` constraints through
  the same HIR extractor as B-tree sources. They use child-query row estimates
  for generic filter selectivity but cannot produce index or automatic-index
  candidates.
- One HIR join input now represents physical B-tree and derived-query sources
  explicitly. Mixed joins use the same greedy ordering, connectivity,
  cross-product, and predicate-work loop; derived scans compete using their
  child row and cost estimates without a synthetic `FromClauseSubquery`.
- Derived `FROM` sources now retain their child `QueryId` in the final source
  loop. Query planning visits the child first, then uses its output cardinality
  and cost with the existing coroutine-scan formula; no legacy
  `FromClauseSubquery`, AST conversion, resolver, or `TableReferences` enters
  this path.
- Compound SELECT arms now receive HIR access plans while their final ORDER BY
  remains query-level. Operators, arm IDs, ordering, and limits stay in the
  owned semantic document instead of being copied into another query shape.
- Ordinary SELECT and VALUES queries now have an owned HIR query-plan entry
  point. The plan retains the semantic document and plans its blocks directly
  from document-local query and source identities.
- Query-block planning derives column-use counts from the owning resolved HIR
  query. Callers no longer build or pass a separate usage summary.
- Query-block planning now has one resolved-HIR entry point that owns planner
  scratch construction. Source-less SELECT and VALUES blocks produce empty-loop
  plans while retaining predicates, cardinality, and zero access cost.
- Final HIR B-tree plans own their predicates after access selection, including
  consumption state. Residual filters therefore survive planning instead of
  remaining in mutable caller scratch state.
- HIR planned sources no longer clone source names, aliases, or USING column
  names into parser-era planner fields. The HIR document remains their owner;
  physical join planning keeps only the resolved join kind.
- Final HIR B-tree plans retain only selected loops, output cardinality, and
  cost. Constraint arenas, candidate access methods, and dynamic-programming
  join state stay local to planning and are dropped at the handoff boundary.
- HIR planned sources no longer carry a placeholder legacy `Operation`.
  Selected HIR access lives only on the final planned loop; parser-expression
  sources retain their existing operation field through the shared source
  type's default parameter.
- Selected B-tree access methods now pass through one representation-generic
  application path. Legacy AST and HIR planning share automatic-index
  materialization, scan/seek/rowid/IN construction, and predicate-consumption
  rules; only expression extraction differs.
- Automatic-index construction now consumes a generic planned source. HIR
  supplies its resolved source identity, physical table, and frozen column-use
  mask directly; building the index no longer requires `JoinedTable`.
- Seek construction now shares the existing direction, range, equality, and
  NULL-boundary algorithm across parser and HIR expressions. HIR extracts seek
  values and frozen comparison affinity directly from resolved predicates;
  resolver and `TableReferences` are absent from that entry point.
- Seek definitions, keys, and range constraints now accept either parser or
  HIR expressions. The legacy plan keeps parser expressions through default
  type parameters, while HIR planning can keep resolved expressions without
  changing physical lowering.
- HIR query-block B-tree planning now has one boundary that collects resolved
  constraints, derives ANALYZE row estimates, owns the access-method arena, and
  returns the complete `JoinN`. Callers no longer assemble those planning steps
  themselves, and the boundary uses no AST, resolver, or `TableReferences`.
- Greedy HIR B-tree planning now builds a complete left-deep `JoinN`. It uses
  the existing ordering restrictions, connected-source preference,
  cross-product penalty, cardinality formula, and access-method arena without
  converting resolved expressions back to parser expressions.
- HIR B-tree join-step selection now accepts the mask of sources already
  joined. Later sources can therefore use resolved join constraints for index
  seeks and automatic indexes; complete HIR join planning supplies an empty
  left-hand side for its chosen starting source.
- Greedy HIR planning selects the starting source and every ordinary B-tree
  access method through one complete-plan entry point. The temporary public
  first-step scaffold has been removed.
- Ready-predicate masks, loop ownership, and residual expression work now flow
  through one representation-neutral join helper. HIR derives the inputs from
  resolved source dependencies and its iterative expression walker; only the
  legacy hash-join metadata still depends on parser expressions.
- Ordinary indexes, automatic temporary indexes, and `IN` seeks now compete in
  one source-generic B-tree selector. The legacy wrapper retains only AST-based
  multi-index scans; HIR uses the same single-source cost and replacement logic.
- `IN`-seek selection now returns the final planner `AccessMethod` through one
  expression-representation-neutral entry point. Legacy and HIR paths share
  consumed predicates, affinity, index identity, row estimates, and cost.
- Ordinary B-tree access selection now has one source-generic entry point that
  both chooses the candidate and builds its planner `AccessMethod`. Legacy and
  HIR callers no longer need representation-specific glue between those steps.
- Chosen B-tree candidates now become planner access methods through one
  expression-representation-neutral builder. Legacy and HIR constraints share
  covering, rows-per-seek, consumed-predicate, direction, and index facts.
- Partial-index candidates now retain the query predicate terms that proved
  them usable. Ordinary B-tree and `IN` access selection consume those frozen
  term positions instead of rebinding and rewriting the schema predicate after
  choosing an index.
- B-tree candidate scoring now runs one loop for legacy and HIR constraints.
  Source-specific order matching sits behind a small source trait; HIR keeps
  resolved source IDs and borrowed expressions through index choice.
- HIR expressions now share one iterative frame traversal for preorder walks
  and postorder folds. Partial-index selectivity uses the fold directly instead
  of maintaining its own traversal tasks and result stack.
- Partial-index candidates now carry their estimated stored-row fraction.
  Legacy collection runs its existing bound-AST estimator once; HIR collection
  applies the same formulas iteratively to the frozen resolved predicate.
  Repeated candidate scoring no longer binds partial-index expressions or
  needs `AvailableIndexes` for this estimate.
- B-tree order matching now shares one source-generic algorithm across legacy
  and HIR planning. HIR compares computed order terms against frozen resolved
  index expressions, while preserving the existing equality-prefix,
  collation, direction, NULL-order, custom-type, and rowid rules without AST
  conversion or adapter allocation.
- HIR ORDER BY terms now build planner order targets directly from resolved
  source, output, collation, direction, and NULL-order facts. Output references
  are followed inside the frozen document and computed expressions stay
  borrowed; planning performs no name lookup, catalog lookup, AST conversion,
  or expression clone.
- Order-target storage is generic over source identity and expression
  representation. The legacy planner keeps its existing table IDs and AST
  pointers, while HIR can borrow resolved expressions keyed by `SourceId`
  without cloning or converting them back to parser nodes.
- Base row-count estimation now consumes a physical `Table` instead of a
  parser-era `JoinedTable`. HIR query-block sources therefore receive the same
  ANALYZE statistics and subquery fallbacks before join scoring, without a
  compatibility source or separate estimate implementation.
- Whole-block HIR sources and constraints now enter the shared greedy
  starting-source selector directly. The selector reads frozen source
  definitions lazily, allocates no compatibility source vector, and applies
  HIR RIGHT/FULL ordering restrictions before scoring.
- The HIR plan context now collects constraints for every query-block source
  from the already assembled HIR sources and predicates. Whole-block planning
  no longer needs callers to reconstruct the per-source constraint loop.
- `IN`-seek access selection now consumes the shared borrowed access-source
  view. HIR supplies its planned source plus frozen definition, so rowid/index
  choice, collation, affinity, covering, and cost rules remain one algorithm.
- Greedy starting-source scoring and directed indexed-seek benefits now consume
  generic constraints and an exact-size iterator of borrowed source views.
  HIR can run the same loop without building an adapter vector.
- Greedy join ordering now consumes the same `JoinOrderingRestrictions` used
  by exhaustive legacy and HIR planning. Starting-source eligibility,
  indexed-seek benefits, and later-source eligibility no longer rebuild a
  separate LEFT-only dependency map.
- Indexed-seek scoring consumes one borrowed source view for its physical
  table and covering-index decision. Legacy sources and HIR sources therefore
  use the same scoring call without per-call callbacks; HIR covering reads its
  frozen source definition.
- Covering-index required-column rules are shared by legacy and HIR planned
  sources. HIR expression-key matching reads frozen resolved index expressions;
  output, grouping, HAVING, and ORDER BY usage is registered directly from
  resolved source and column identities. Parser-expression matching remains
  only as a temporary legacy wrapper.
- Filter multipliers, self-filter selectivity, and rows-per-seek costing now
  consume generic constraint records. AST and HIR constraints therefore use
  the same formulas rather than parallel cost implementations.
- Index seek-benefit costing now consumes a physical table, generic
  constraints, and a covering-index decision. Index page-width and ANALYZE
  lookups no longer require `JoinedTable`; the legacy path supplies only a
  temporary covering-index adapter.
- Legacy and HIR join planning now share one builder for CROSS and outer-join
  ordering restrictions. Preserved HIR RIGHT JOIN is constrained directly;
  the HIR path does not need the parser-era source swap.
- Query-block planning input now comes directly from HIR. It preserves lexical
  source order and exact join kinds, splits resolved ON and WHERE conjunctions,
  and builds USING/NATURAL equality predicates from frozen column references
  and comparison rules without resolver, `TableReferences`, or parser-expression
  conversion.
- HIR planned sources retain `SourceId`, resolved index hints, HIR expressions,
  and string join names. Their construction no longer depends on
  `ProgramBuilder`, planner table-ID allocation, or parser index-hint nodes.
- HIR constraint collection consumes `HirPlannedSource` and the complete index
  metadata frozen on its HIR source. It no longer accepts `JoinedTable`,
  `AvailableIndexes`, or a separate source ID; existing candidate,
  expression-index, partial-index, selectivity, and automatic-index rules are
  shared with the legacy planner where they remain physical planning work.
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

Standalone HIR lowering now also covers query `IN` probes. `QueryId` maps to
the ephemeral index cursor owned by query lowering; the probe uses the frozen
comparison affinity and collation. Its true, false, and NULL branches retain
the legacy opcode flow, including checking each row-value operand for NULL
before evaluating the next operand.

HIR subquery destination preparation now allocates the existing row-value,
EXISTS, and ephemeral-index destinations directly from the resolved query and
comparison metadata. Destination allocation installs the matching `QueryId`
binding in `ProgramBuilder`, and correlation comes from the query's frozen
capture set. Query-body emission remains the next lowering boundary.

Prepared HIR subqueries now use the existing execution shell: uncorrelated
queries receive `Once`, scalar and EXISTS results are initialized inside a
subroutine, and query `IN` opens its prepared ephemeral index. Scalar/list EQP
nodes and correlated re-execution retain the legacy opcode behavior. The shell
accepts a HIR query-body emitter and does not depend on AST plans or a resolver.

Constant SELECT and VALUES query bodies now lower directly from HIR into the
existing query destinations. They bind resolved outputs to consecutive
registers, preserve VALUES row order, stop scalar and EXISTS destinations after
the first row, populate IN indexes for every row, skip unnecessary EXISTS
output evaluation, and apply the existing array result decode step. FROM,
grouping, compounds, and ordering remain later query-body checkpoints.
No-FROM WHERE predicates use the shared iterative expression visitor to retain
AND/OR short-circuiting. LIMIT and OFFSET use the existing counter opcode flow,
including integer checks, early LIMIT-zero exit, offset skipping, and result-row
decrement.

Planned single-table B-tree scans now lower directly from HIR. Lowering opens
the resolved catalog table or index, binds its `SourceId` to the scan cursor,
evaluates the plan's unconsumed predicates, emits resolved outputs, and
preserves forward or backward full-scan, LIMIT, OFFSET, scalar, EXISTS, and
result destination control flow. The plan freezes whether an index is covering:
covering scans open only the index, while non-covering scans also open the table
and use `DeferredSeek`. HIR constraint planning now enforces its already-resolved
`INDEXED BY` and `NOT INDEXED` choices just as the legacy planner does. Seeks,
joins, and other source kinds remain later checkpoints.

Single-table rowid equality plans now lower through the same HIR B-tree loop.
The resolved comparison expression is evaluated by HIR expression lowering,
`SeekRowid` performs the one-row lookup, and no scan advance is emitted. This
also covers an `INTEGER PRIMARY KEY` rowid alias. Range seeks, joins, and other
source kinds remain later checkpoints.

Single-table range and equality-prefix seeks now share one generic seek emitter
between legacy AST and HIR lowering. The HIR adapter evaluates resolved key
expressions and uses frozen column type programs, while the common emitter keeps
the existing affinity, NULL matching, forward/backward bound, covering-index,
table lookup, and `DeferredSeek` behavior. Automatic-index seeks, `IN` seeks,
and joins remain later checkpoints.

Single-table `IN` seeks now lower from HIR through the same two-level loop as
legacy plans. Literal values use resolved HIR expression lowering while the
shared materializer keeps the unique ephemeral index, affinity, and duplicate
handling. Query sources reuse their prepared `QueryId` index binding. Rowid
lookups still use `SeekRowid`; index lookups still use `SeekGE`/`IdxGT`, scan
duplicate keys, and defer table lookup only when the selected index is not
covering. Joins and automatic indexes remain later checkpoints.

Ordinary comma, inner, and cross joins now lower as nested HIR B-tree loops.
All cursors open before entering the outer loop, each residual predicate runs
at the first loop where all of its resolved `SourceId` inputs are positioned,
and loop advances close from inner to outer. Later index seeks read outer HIR
columns directly and retain covering and deferred-table behavior. Outer joins,
automatic indexes, and non-B-tree sources remain later checkpoints.

Automatic indexes for later ordinary-join sources now use one shared build
emitter for legacy AST and HIR plans. HIR lowering opens the base table but
creates the ephemeral cursor only through `OpenAutoindex`, builds covering
payload and virtual generated-column keys from the resolved source, restores
the index source binding before probing, and keeps the existing affinity,
NULL-matching, bloom-filter, and seek behavior. Outer joins and non-B-tree
sources remain later checkpoints.

LEFT JOINs with a direct table or an ordinary parenthesized join group on the
right now lower with the same match-flag control used by the
legacy main loop. Shared metadata helpers reset and set the flag, check it at
right-side exhaustion, null-extend every active index/table cursor, and re-enter
after ON predicates so post-join WHERE predicates see the synthetic NULL row.
The final HIR plan preserves FROM-group boundaries, so grouped ON predicates run
only after the whole group is positioned and grouped WHERE terms run after the
match flag. Downstream loops restart for the unmatched row. Indexed and
automatic-index right sides use the same path. RIGHT/FULL joins and nested outer
joins inside a parenthesized right side remain later checkpoints.

Non-correlated derived `FROM` sources now lower directly from their prepared
`QueryId`. Lowering materializes the child HIR query once into an ephemeral
table, binds that cursor to the derived `SourceId`, and feeds it through the same
nested-loop driver as ordinary B-tree scans. No parser expression or synthetic
table reference is rebuilt. Non-correlated ordinary CTEs use that same path with
their resolved `CteId` and prepared body `QueryId`. Shared and explicitly
materialized CTEs register one ephemeral table in `ProgramBuilder`; later source
references open duplicate cursors over that storage. Correlated query-backed
sources remain a later checkpoint.

Recursive CTE sources now use one queue runtime for legacy and HIR plans. The
legacy adapter still exposes each dequeued row through its pseudo cursor; the
HIR adapter binds each resolved recursive-input `SourceId` directly to the
dequeued registers. UNION ALL, UNION seen-row filtering, queue ordering and
collation, NULL ordering, LIMIT, and OFFSET keep the existing runtime behavior.
No parser expression, resolver scope, or synthetic table reference is rebuilt.
Compound seed queries now use compound HIR lowering. Correlated recursive
bodies remain with the correlated-query checkpoint.

Compound SELECT lowering now consumes prepared HIR blocks directly. UNION ALL
shares one LIMIT/OFFSET state across its arms. UNION, EXCEPT, and INTERSECT use
the existing ephemeral-index opcode flow with resolved output collations.
Compound ORDER BY uses resolved output identities, HIR type facts, collations,
sort direction, and NULL ordering; LIMIT/OFFSET is applied after sorting. This
also permits compound recursive CTE seed queries without rebuilding AST nodes.
Ordinary single-block DISTINCT and ORDER BY now use one HIR row-output path.
DISTINCT hashes the resolved output registers with their frozen collations
before applying OFFSET. ORDER BY inserts rows directly from the source loop,
uses resolved type facts for custom comparators, and drains through the same
sorter-output helper as compound ordering before applying LIMIT/OFFSET. Combined
DISTINCT and ORDER BY deduplicates before sorter insertion. No parser expression
or `TableReferences` sidecar is rebuilt. Aggregate argument ORDER BY and window
output remain later SELECT checkpoints.

Ungrouped aggregate SELECT now lowers from planned HIR. Stable `AggregateId`
values own accumulator registers; aggregate arguments, filters, DISTINCT
collations, custom comparators, and final calls use semantic HIR facts. HIR and
legacy paths share one aggregate-step opcode switch, so this adds no second
copy of function behavior. Bare columns and rowid reads capture only values
needed after the scan, preserving SQLite first-row and empty-input behavior
without forcing unused columns into covering-index plans. As in SQLite, and
unlike legacy, a query with exactly one min() or max() aggregate, grouped or
not, takes its bare columns from a row holding the aggregate's value: the
first row, then each row whose value changes the aggregate. Also unlike
legacy, which rejects it, an aggregate's arguments may have an ORDER BY, as
in SQLite: `group_concat(a ORDER BY b)` keeps each row's ORDER BY values, a
sequence number and its arguments in an ephemeral index, and steps through
it in order when it is finalized, per group in a grouped query. Tied rows
keep their order, and DISTINCT keeps the first of equal arguments in that
order. A window call still rejects it, with legacy's error. Ordered-set
percentile aggregates evaluate their direct fraction once before opening the
input scan, reject local-row and subquery dependencies with the legacy error,
and reuse the same range-check and aggregate-step opcode helpers as legacy
lowering. Sorter-backed GROUP BY now stores resolved key and source values,
detects boundaries with HIR key collations, and reuses the same aggregate
step/finalize state for each group. HAVING and output expressions read the
captured group row directly from register-bound HIR sources. Aggregate-free and
DISTINCT-aggregate groups use the same path. Single-source B-tree plans now
reuse the shared HIR index-order proof to skip the GROUP BY sorter only when it
consumes every resolved key with the required collation and custom comparator.
Both sorted and input-ordered rows use one group-boundary and aggregate path.
Multi-source and combined GROUP BY/ORDER BY proofs, aggregate argument ORDER BY,
and windows remain later SELECT checkpoints.

Virtual-table SELECT sources now use their planned HIR access directly.
Lowering opens and binds a virtual cursor, evaluates the optimizer-selected HIR
arguments into the `VFilter` argument registers, and advances with `VNext`.
Column reads already use the shared HIR expression lowering through `VColumn`;
no parser expression, resolver, or table-reference sidecar is rebuilt.

Window function stepping now consumes a representation-neutral runtime record.
It owns prepared argument and FILTER value sources, result registers,
collations, custom comparators, and moving-frame min/max state. The legacy
adapter preserves direct buffered-column reads and late JSON expression
recomputation, while aggregate opcode selection is shared with HIR lowering
through prepared argument registers. Frame movement, peer handling, positional
lookups, partition buffering, and row output still use the existing legacy
window plan; adding the HIR adapter and then removing that remaining plan
dependency are later checkpoints.

Resolved HIR now builds an explicit input layout for one effective window.
PARTITION BY and ORDER BY values are deduplicated in legacy order; ordinary
arguments and FILTER predicates receive stable buffer slots. JSON aggregates
keep their argument expression for late recomputation and buffer only its
resolved column, rowid, aggregate, and subquery leaves, matching the existing
subtype-preserving rule without rewriting expressions into subquery-column
AST nodes. Stable `WindowFunctionId` values select calls for each window and
missing or mismatched identities fail as HIR invariants. This plan is not yet
routed into production frame lowering.

HIR window-buffer plans now create the same representation-neutral
`WindowFunctionRuntime` used by legacy lowering. Resolved argument facts supply
collations and custom comparators, stable window identities bind result
registers, and resolved frame facts drive the shared moving min/max setup. The
HIR adapter needs no resolver, parser expression, or `TableReferences` sidecar.
Frame movement and row buffering are still not routed to HIR.

HIR window step and inverse emission now read direct arguments and filters from
their planned buffer columns. Subtype-sensitive arguments load only their
planned HIR leaves from the buffer and recompute through the existing HIR
expression walker using scoped node-to-register inputs. The same generic
step/inverse loops serve legacy and HIR expressions; HIR does not create AST
nodes or mutate global source bindings. Production partition buffering and
frame-loop routing remain later checkpoints.

HIR window-buffer columns now retain their original resolved expression even
for direct source-column slots. A source row can be lowered into one contiguous
register range in the planned partition, order, argument, and filter order by
the existing HIR expression walker. Direct columns are not reconstructed as
new expression nodes, and windows with no buffered values consume no
registers. The row is not yet inserted into the production window buffer or
driven through the frame loop.

Window buffer cursor creation is now shared below the legacy/HIR boundary.
Both paths can open the same ephemeral table roles in the same order: current,
write, end, and the optional moving-start and positional-lookup cursors. Legacy
window initialization uses this helper without changing its frame plan or
opcode flow. HIR window plans now open this same cursor set from their resolved
frame and function requirements. Their ephemeral table schema carries only the
planned record width because expression types, collations, and comparison rules
remain owned by resolved HIR facts. Window input register ranges and buffer
insertion are now representation-neutral as well. Legacy and HIR rows use the
same `MakeRecord`, `NewRowid`, and `Insert` sequence without rebuilding HIR
expressions or consulting table references. The production HIR window frame
loop does not call this insertion path yet. HIR window plans also retain the
exact buffer slot for every PARTITION BY and ORDER BY term. Reused expressions
share one stored value while repeated terms and their original order remain
explicit for partition and peer comparisons.
PARTITION BY terms now retain their analyzed type and collation facts in HIR,
matching ORDER BY terms instead of discarding those facts during window
construction.
Window key metadata stays iterator-based until `Insn::Compare` requires its
owned vector. Legacy and HIR partition/peer comparisons use the same compare
and branch emitter; HIR keys take collations directly from resolved terms.
The partition-change sequence is shared too: compare keys, flush the previous
partition, reset rowid, then save the new keys. The emitted key width drives
the copy without collecting or walking the key iterator a second time.
Legacy and HIR window input paths now allocate the rowid, saved partition keys,
and flush return register through the same input state. HIR partition handling
passes its resolved key iterator directly into the shared change sequence.
Legacy and HIR also use one iterator-based gather operation to restore ORDER BY
term order from deduplicated input slots. Repeated or reordered terms emit
direct register copies without building an intermediate slot vector.
Legacy and HIR now allocate current ORDER BY keys plus input and cursor peer
reference registers through the same frame-gated state. First-row peer seeding
uses the same copy sequence and preserves the legacy register order.
Peer-group changes now use one shared emitter too. The no-ORDER case is an
explicit all-rows state; keyed comparisons accept lazy legacy or resolved HIR
key metadata, then update the remembered keys with the same copy sequence.
All buffered-cursor ORDER BY reads now use one iterator-based emitter. Legacy
extracts rewritten buffer-column indexes in one place; HIR passes its resolved
slot iterator directly, including repeated and reordered terms.
First-row partition reset is now shared too. Legacy and HIR clear the
accumulator block, reset moving min/max state, and zero positional frame
counters through one runtime emitter without collecting function state.
Window frame tracking now has explicit `None`, positional-counter, and
EXCLUDE-rowid states. Legacy and HIR allocate the same state, so partial
rowid pairs and simultaneous positional/EXCLUDE tracking are unrepresentable.
Bounded frame offsets now share evaluation timing and runtime checks. HIR
classifies resolved expressions with the iterative expression fold, lowers
accepted constants directly, and preserves SQLite's NULL substitution for
columns, functions, subqueries, and current-time values.
Frame-offset registers now use one explicit `None`, start, end, or both state.
Legacy and HIR allocate that state from their own frame bounds, while the
runtime loop consumes the same accessors and keeps the existing opcode order.
Window step, per-operation, output, and flush routing now consume an
expression-free frame shape. Legacy and HIR map their own bounds into the same
edge enum; runtime routing no longer matches legacy `FrameBoundary` values.
The crossed-bound check for same-kind bounded frames is shared as well. One
explicit order-check enum selects the existing `Le`/`Ge` branch, and both HIR
and legacy frame state can classify that check without carrying expressions.
First-row frame cursor positioning is shared without allocating a cursor list.
HIR and legacy lowering rewind the optional start cursor, current cursor, and
end cursor in the same order.
The `FOLLOWING` start delay is shared too: bounded `ROWS`/`GROUPS` frames keep
the existing `end - start` register adjustment, while `RANGE` remains exempt.
The crossed-bound empty-frame guard now owns the shared branch, cursor rewind,
rowid reset, and sorter reset sequence. Representation-specific aggregate and
row output remain callbacks in their original bytecode positions.
Window aggregate result emission now consumes the shared function runtime and
an explicit value/finalize mode. HIR and legacy use the same min/max, positional,
`AggValue`, and `AggFinal` paths without collecting a temporary min/max vector.
HIR bounded frame offsets now have one partition-entry emitter. It rebuilds the
start and end countdown registers in legacy order through the existing shared
offset checks, ready for the HIR first-row frame path.
First-row buffer insertion, empty-frame handling, following-bound delay, cursor
rewind, and final jump now share one emitter. It accepts prepared frame state
and the existing aggregate/row phase callback; HIR tests exercise the sequence
without a resolver or table references. Production HIR frame-loop wiring remains
a later checkpoint.
The HIR first-row adapter now evaluates partition offsets before insertion and
supplies resolved aggregate results to that sequence. EXCLUDE frames retain
full-frame result evaluation; row output remains a callback after cursor rewind.
HIR first-row initialization now composes peer-reference seeding, accumulator
and frame-counter reset, and that frame adapter in legacy order. Prepared peer,
tracking, function, and buffer state are reused across partition entries.
Subsequent rows now share buffer insertion followed by the frame-gated peer
check. HIR passes resolved ORDER BY metadata directly; ROWS skips comparison,
and RANGE/GROUPS without ORDER BY jump to the step end after insertion.
Bounded RANGE comparisons now consume a resolved buffer slot, direction, NULL
order, and collation through a shared key descriptor. Legacy and HIR adapters
use the same numeric guards, DESC reversal, NULL branches, and comparison flow.
Operation offset gates now share countdown emission and RANGE comparison
selection. Missing offsets emit nothing; ROWS/GROUPS keep their decrementing
`IfPos`, while HIR and legacy RANGE gates return the same retry-label position
and preserve operation-specific cursor order and boundary comparisons.
Window operations now share the complete control flow: unbounded-inverse no-op,
offset gates, aggregate-result reads, RANGE cursor guards, tracked step/inverse,
row deletion, cursor advance, EOF exits, peer repeats, and RANGE retry branches.
Legacy and HIR adapters supply aggregate/output and key emitters. HIR deletion
uses resolved frame facts and literal offsets with legacy integer truncation;
its peer checks read planned buffer slots and resolved collations directly.
Per-row result preparation now shares buffered output reads, first_value /
nth_value and lag / lead lookups, EXCLUDE frame scans, and the output Gosub.
HIR supplies planned buffer slots, prepared function result registers, resolved
peer keys, and HIR aggregate emission for scan rows. The HIR row-result adapter
can serve as the operation loop's output callback, including EXCLUDE scans.
Streaming and partition-flush control flow now share the legacy frame-pattern
branches. HIR adapters compose partition checks, first-row initialization,
subsequent insertion, operation loops, and row-result callbacks. Flush preserves
its partition-boundary and end-of-source entries, EOF exits, buffer reset,
EXCLUDE rowid reset, and jump over the output subroutine. Tests lower these
pieces together using resolved HIR for ROWS, GROUPS, RANGE, and EXCLUDE frames.
The HIR SELECT output subroutine now evaluates expressions from supplied buffer
registers and bound window results, then reuses SELECT DISTINCT, destination,
sorter, and LIMIT/OFFSET emission. DISTINCT and OFFSET skips return to the window
loop; direct LIMIT exits through the query's done label. Sorted output applies
LIMIT/OFFSET when draining. Separate sort-input mappings preserve encoded custom
keys independently of decoded SELECT values. Row-dependent expressions stay
inside the subroutine. Outer-input planning now walks SELECT and ORDER BY HIR,
retaining columns, rowids, aggregates, subqueries, and other-window results in
buffer slots.
Current-window calls keep their computed result bindings; scalar expressions
remain at output time. Equivalent terminals reuse slots while every exact HIR
node gets a register mapping. Encoded sort terminals use separate slots and
source emission suppresses custom decoding only for those slots. Prepared
registers feed shared row-result reads and the HIR output subroutine.
Planned SELECT blocks with one window now lower end to end. The block's rows
play the role of the legacy window source subquery: plain, grouped, ungrouped
aggregate, and FROM-less rows go to a window input target instead of the SELECT
output. Rows are sorted by PARTITION BY ascending and then ORDER BY, skipping
terms equal to an earlier key, with resolved collations, NULL order, and
custom comparators. Keys of custom types that decode values sort by their
stored value, and grouped rows add a sequence column so equal keys keep group
order, both as in legacy. A window with no keys streams rows straight into the
buffer. The sorted rows then run the shared window step, and the flush emits
the SELECT output subroutine, so DISTINCT, ORDER BY, LIMIT, and OFFSET apply to
window output rows. Aggregates, grouped rowids, and bare columns used only by a
window definition are now collected too.

Several windows in one block lower as one layer per window, nested like the
legacy window source subqueries: the first window is the outermost layer. The
block's rows feed the innermost layer, and each finished row of an inner layer
feeds the next outer layer instead of the SELECT output. Each layer buffers
only what its consumer reads: the final SELECT outputs and ORDER BY terms for
the outermost layer, and the outer layer's buffer columns and stored-value
sort keys for the others. A layer's own window results stay bound to their
result registers, and other windows' results are buffered like any value.

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
