# Turso Incremental View Maintenance Compatibility

Turso materialized views are an experimental Turso extension. They store the
result of a defining `SELECT` and use Incremental View Maintenance (IVM) to
update that result in the same transaction as changes to its source tables.

This document describes the SQL and operational surface currently supported by
IVM. It is narrower than Turso's regular query engine. Unless stated otherwise,
an unsupported definition is rejected by `CREATE MATERIALIZED VIEW`.

See [COMPAT.md](COMPAT.md) for regular SQLite compatibility and the
[CREATE MATERIALIZED VIEW reference](docs/sql-reference/statements/create-materialized-view.mdx)
for syntax and examples.

## Status

| Area | Status | Comment |
|------|--------|---------|
| Materialized views | 🚧 Experimental | Must be enabled with the experimental views option. |
| Initial population | ✅ Yes | The defining query is evaluated when the view is created. |
| Incremental maintenance | ✅ Yes | Source changes and view changes share one transaction. |
| Automatic refresh | ✅ Yes | There is no manual `REFRESH` operation. |
| Persistent format stability | 🚧 Experimental | Internal hidden storage may change; an incompatible view must be dropped and recreated. See [Views from older versions](#views-from-older-versions). |

### Views from older versions

Materialized views are experimental, so the no-breaking-changes rule for
databases does not cover their internal storage. Each view records the storage
version it was created with. When a view's version differs from the current
one:

- Reading the view fails with an "incompatible version" error.
- `INSERT`, `UPDATE`, and `DELETE` on any table the view reads from fail, so
  the stale view can never fall out of sync silently.
- `DROP VIEW` still works. Recreate the view with the same `CREATE
  MATERIALIZED VIEW` statement to fill it again from the current source data.

Every view created before materialized views were compiled to VDBE bytecode
(storage version 1), and every view created before a run of inner joins was
maintained as one join (storage version 2), is in this state.

In the tables below, ✅ means the feature is implemented and part of the
intended supported surface. 🚧 identifies an implemented feature with an
important qualification. ❌ means IVM does not currently accept that feature,
even if the regular query engine does.

## Defining queries

### Relational operators

| Feature | Status | Comment |
|---------|--------|---------|
| `SELECT ... FROM` | ✅ Yes | At least one FROM source is required. |
| `WHERE` | ✅ Yes | Deterministic predicates use regular query-engine expression semantics. |
| Projection expressions | ✅ Yes | Deterministic scalar expressions and aliases are supported. |
| `SELECT DISTINCT` | ✅ Yes | Includes NULLs and built-in collation semantics. |
| FROM-clause derived tables | ✅ Yes | Derived inputs may themselves contain joins, aggregates, or set operations. |
| Non-recursive CTEs | ✅ Yes | Multiple references may currently duplicate maintenance work. |
| Materialized-view sources | ✅ Yes | Materialized views may depend on other materialized views. |
| Plain view sources | ✅ Yes | Writes to the tables a plain view reads maintain the materialized view. A plain view that a materialized view depends on cannot be dropped. |
| Correlated `EXISTS`, `NOT EXISTS`, and `IN (SELECT ...)` | ✅ Yes | Maintained as semi and anti joins whenever the regular planner can rewrite the subquery into one. |
| Correlated scalar aggregate subqueries | ✅ Yes | When the regular planner can rewrite them into a join with a grouped table. |
| Other subqueries outside the FROM clause | ❌ No | Includes `NOT IN (SELECT ...)`, uncorrelated subqueries, and subqueries under `OR`. Literal `IN (...)` lists are supported. |
| `SELECT` without a FROM clause | ❌ No | Includes constant-only SELECTs. |
| `VALUES` queries | ❌ No | |
| `ORDER BY` | ❌ No | Rejected in the materialized-view definition. Reads from the completed view may use regular `ORDER BY`. |
| `LIMIT` / `OFFSET` | ❌ No | Rejected in the materialized-view definition. |
| Window functions | ❌ No | |
| Recursive CTEs | ❌ No | Turso's regular engine does not currently support these either. |

### Joins

| Feature | Status | Comment |
|---------|--------|---------|
| `INNER JOIN` | ✅ Yes | Equality and non-equality predicates are supported. |
| `CROSS JOIN` | ✅ Yes | |
| `LEFT [OUTER] JOIN` | ✅ Yes | NULL-padded rows are maintained as matches appear and disappear. |
| `RIGHT [OUTER] JOIN` | ✅ Yes | Any position in a join chain. |
| `FULL [OUTER] JOIN` | ✅ Yes | NULL-padded rows of both sides are maintained. |
| `USING` / `NATURAL JOIN` | ✅ Yes | Including RIGHT and FULL joins, where the merged column follows the regular planner. |
| Self joins | ✅ Yes | |
| Multi-way and nested joins | ✅ Yes | Covered through five-way joins; there is no fixed shape limit. |
| Joins over derived, aggregate, distinct, or set-operation results | ✅ Yes | Both join inputs use the same composable stream contract. |

Cost of maintaining a join: a run of inner joins is maintained as one join
that stores no intermediate join results. A changed row of one table is
joined with the other tables one at a time, following the `=` terms between
them. A table whose compared column is its INTEGER PRIMARY KEY or has an index
is looked up by key, so a small write reads only the matching rows. When the
compared column has no index, the view creates one, named
`__turso_internal_dbsp_index_v3_<table>__<hash of the definition>`. When the
WHERE clause has terms that read only that table, the index only holds the
rows that pass them (a term that reads the rowid is left out of the index
and only checked by the join). Views that need the same index share one.
These indexes cost space and make writes to those tables slower, like any
index. They are kept up to date by every write to the table, `DROP VIEW`
removes one once no remaining view needs it, `DROP INDEX` refuses them, and
`VACUUM` creates them again. A user index on the same columns is used
instead of creating one, including a partial index whose WHERE is one or
more of the view's terms on that table. A user index created after the view
replaces the view's own index at the next `VACUUM`. If the user index is
dropped later, the view does not create a replacement, and the table is read
once per write statement. The same happens when an index cannot find every
matching row, for example when the two compared columns convert values to
different types before comparing. A table with no `=` term to the tables
joined before it is paired with every row.

### Aggregation

| Feature | Status | Comment |
|---------|--------|---------|
| `GROUP BY` | ✅ Yes | Expressions, aliases, ordinals, NULL keys, and built-in collations are supported. |
| Scalar aggregation | ✅ Yes | Aggregation without `GROUP BY`, including the empty-input row, is supported. |
| `COUNT(*)` / `COUNT(expr)` | ✅ Yes | |
| `SUM` / `TOTAL` / `AVG` | 🚧 Partial | Implemented for grouped and scalar queries. Exact floating-point parity across every retraction order remains an active hardening area because floating-point addition is not associative. |
| `MIN` / `MAX` | ✅ Yes | Includes deletion of the current extreme, and of the value shown for a class of equal values (see the note under collations). |
| Aggregate `DISTINCT` | ✅ Yes | Supported for the aggregates above. |
| Aggregate `FILTER (WHERE ...)` | ✅ Yes | Supported for grouped and scalar aggregates. |
| `HAVING` | ✅ Yes | May use grouping keys and supported aggregate results. |
| Expressions over aggregate results | ✅ Yes | Includes compositions such as `SUM(x) / COUNT(*)`. |
| Bare input columns outside `GROUP BY` | ❌ No | Rejected in result and `HAVING` expressions. |
| Multi-argument aggregates | ❌ No | |
| Other aggregates | ❌ No | Includes `group_concat`, `string_agg`, JSON aggregates, percentile/mode aggregates, and extension aggregates. |

### Compound queries

| Feature | Status | Comment |
|---------|--------|---------|
| `UNION ALL` | ✅ Yes | Duplicate multiplicity is preserved. |
| `UNION` | ✅ Yes | |
| `INTERSECT` | ✅ Yes | |
| `EXCEPT` | ✅ Yes | |
| Mixed compound chains | ✅ Yes | Deduplicating and `UNION ALL` portions may be combined. |
| Complex compound branches | ✅ Yes | Branches may contain filters, joins, grouping, distinct, or derived inputs. |
| Set operations feeding joins or aggregates | ✅ Yes | |

### Expressions, sources, and collations

| Feature | Status | Comment |
|---------|--------|---------|
| Deterministic scalar expressions | ✅ Yes | Evaluated by regular VDBE expression bytecode. |
| `BINARY`, `NOCASE`, and `RTRIM` | ✅ Yes | Supported on grouping, DISTINCT, MIN/MAX, and set-operation state keys. See the note below on which of several equal values a view shows. |
| Non-deterministic expressions | ❌ No | For example, expressions using non-deterministic functions are rejected. |
| Bound parameters | ❌ No | View definitions are persistent schema objects. |
| Virtual-table FROM sources | ❌ No | |
| Tables with virtual generated columns | ❌ No | |
| Connection-registered custom collations | 🚧 Partial | Not supported on grouping, DISTINCT, MIN/MAX, or set-operation state keys. Other uses are not currently claimed as a compatibility guarantee. |
| Attached-database sources or targets | ❌ No | Materialized views are limited to the main database. |
| `WITHOUT ROWID` source tables | ❌ No | Writable `WITHOUT ROWID` behavior is not currently supported by Turso proper, so this is not an additional IVM compatibility gap. |

**Which of several equal values a view shows.** Values can compare equal
without being identical: `'a'` and `'A'` under `NOCASE`, `'x'` and `'x  '`
under `RTRIM`, or the integer `2` and the real `2.0`. A GROUP BY key, a
`SELECT DISTINCT` row, a `MIN`/`MAX` result, or a `UNION`/`INTERSECT`/`EXCEPT`
row then stands for several such values, and SQL does not say which one the
result shows. A plain `SELECT` shows the one its query plan reads first, so
the answer can change when an index is added. A materialized view always shows
the smallest of the values present, compared as `BINARY`, with an integer
before an equal real. For example, with rows `'a'` and `'A'` in a `NOCASE`
column, `SELECT x, COUNT(*) FROM t GROUP BY x` may show `a|2`, while a view
over it shows `A|2`. After `'A'` is deleted, the view shows `a|1`. A
`DISTINCT` aggregate such as `SUM(DISTINCT x)` likewise uses the smallest
value of each class.

**Integer overflow in `SUM`.** A view keeps the exact total of each group. A
write fails with `integer overflow` when it would leave a group's `SUM` outside
the 64-bit integer range; the order of earlier writes does not matter. For
example, with a group holding `9223372036854775807`, inserting `1` into it
fails, while inserting `-5` and then `1` succeeds. SQLite accepts such a write
into the base table and reports the overflow when the sum is read, so with a
materialized view the same error surfaces earlier, on the write.

## Maintenance and schema behavior

| Feature | Status | Comment |
|---------|--------|---------|
| `INSERT`, `UPDATE`, and `DELETE` on source tables | ✅ Yes | Includes changes crossing filter, join, group, and set-membership boundaries. |
| UPSERT, `REPLACE`, and `INSERT OR REPLACE` on source tables | ✅ Yes | Old and new row images are maintained in statement order. |
| Read-your-own-writes | ✅ Yes | Reading a view applies pending dependency-ordered maintenance first. |
| Explicit transactions | ✅ Yes | The view commits or rolls back atomically with its sources. |
| Statement abort | ✅ Yes | Failed-statement changes are rewound. |
| Savepoints and rollback | ✅ Yes | Includes `ROLLBACK TO` and full transaction rollback. |
| Multiple affected views | ✅ Yes | All dependent views are maintained in dependency order. |
| Dependency chains | ✅ Yes | A materialized view may feed another materialized view. |
| Reopen and schema-cache refresh | ✅ Yes | Persistent views are reconstructed from `sqlite_schema`. |
| Large transaction change logs | ✅ Yes | The transaction-local log can spill through the normal asynchronous I/O path. |
| `DROP VIEW` | ✅ Yes | Removes the visible view and its internal state. |
| Dropping or altering a source with dependents | 🚧 Guarded | Rejected while dependent materialized views exist. Drop the dependent views first. |
| Creating indexes on source tables | ✅ Yes | Does not invalidate maintenance; the defining query is replanned normally. |
| Direct writes to a materialized view | ❌ No | Materialized views are read-only query results. |
| `CREATE INDEX` on a materialized view | ❌ No | Rejected. View maintenance does not yet keep secondary indexes up to date; see "Indexes on materialized views" in docs/ivm-vdbe-rewrite-writeup.md. |
| `TEMP` / `TEMPORARY` materialized views | ❌ No | |
| Experimental MVCC mode | ❌ No | Materialized views currently require the regular WAL-backed transaction path. |

Queries *reading* a materialized view use the regular query engine. The
restrictions above apply to the persistent defining query and its incremental
maintenance, not to a later `SELECT` over the stored result.

## Correctness verification

IVM SQL conformance tests opt into `@verify-materialized-views`. After each
successful statement, the native sqltest backend compares every live
materialized view with a fresh execution of its stored defining `SELECT`.
Comparison is an unordered typed multiset and checks:

- column count;
- row multiplicity;
- `NULL` versus empty values; and
- integer, real, text, and blob type identity.

The IVM differential fuzzer uses the same result oracle, checks every live view
after every statement, and generates compositions of the supported operators.
Every successfully minimized differential-fuzzer failure must become a
permanent `*.sqltest` regression case.

For implementation architecture and known engineering risks, see
[Materialized-view maintenance as compiled VDBE](docs/ivm-vdbe-rewrite-writeup.md).
