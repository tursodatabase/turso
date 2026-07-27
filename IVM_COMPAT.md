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
| Persistent format stability | 🚧 Experimental | Internal hidden storage may change; an incompatible view must be dropped and recreated. |

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
| Scalar, correlated, `EXISTS`, or `IN (SELECT ...)` subqueries | ❌ No | Subqueries outside the FROM clause are rejected. Literal `IN (...)` lists are supported. |
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
| `RIGHT [OUTER] JOIN` | ✅ Yes | The regular planner rewrites it to a left join before IVM lowering. |
| `USING` / `NATURAL JOIN` | ✅ Yes | |
| Self joins | ✅ Yes | |
| Multi-way and nested joins | ✅ Yes | Covered through five-way joins; there is no fixed shape limit. |
| Joins over derived, aggregate, distinct, or set-operation results | ✅ Yes | Both join inputs use the same composable stream contract. |
| `FULL OUTER JOIN` | ❌ No | |

### Aggregation

| Feature | Status | Comment |
|---------|--------|---------|
| `GROUP BY` | ✅ Yes | Expressions, aliases, ordinals, NULL keys, and built-in collations are supported. |
| Scalar aggregation | ✅ Yes | Aggregation without `GROUP BY`, including the empty-input row, is supported. |
| `COUNT(*)` / `COUNT(expr)` | ✅ Yes | |
| `SUM` / `TOTAL` / `AVG` | 🚧 Partial | Implemented for grouped and scalar queries. Exact floating-point parity across every retraction order remains an active hardening area because floating-point addition is not associative. |
| `MIN` / `MAX` | ✅ Yes | Includes deletion of the current extreme and built-in collation representative replacement. |
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
| `BINARY`, `NOCASE`, and `RTRIM` | ✅ Yes | Supported on grouping, DISTINCT, MIN/MAX, and set-operation state keys. |
| Non-deterministic expressions | ❌ No | For example, expressions using non-deterministic functions are rejected. |
| Bound parameters | ❌ No | View definitions are persistent schema objects. |
| Virtual-table FROM sources | ❌ No | |
| Tables with virtual generated columns | ❌ No | |
| Connection-registered custom collations | 🚧 Partial | Not supported on grouping, DISTINCT, MIN/MAX, or set-operation state keys. Other uses are not currently claimed as a compatibility guarantee. |
| Attached-database sources or targets | ❌ No | Materialized views are limited to the main database. |
| `WITHOUT ROWID` source tables | ❌ No | Writable `WITHOUT ROWID` behavior is not currently supported by Turso proper, so this is not an additional IVM compatibility gap. |

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
