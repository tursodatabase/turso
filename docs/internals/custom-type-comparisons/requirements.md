# Custom type comparison: requirements

What any design must satisfy. Checked against PostgreSQL 17 by hostile
reviews. "#11143" is the earlier attempt, tursodatabase/turso#11143; its
failures are kept as examples.

Terms:
- **Value**: what a typed expression means. Two values are the same value
  when the type's equality says so, even if they print differently
  (PostgreSQL numeric `1.0` and `1.00`).
- **Equality**: the type's `=`. NULL = anything is NULL. For floats,
  NaN = NaN is true and NaN sorts above every other value (PostgreSQL).
- **Grouping equality**: equality, except NULL equals NULL. Used by
  GROUP BY, DISTINCT, DISTINCT ON, UNION/INTERSECT/EXCEPT, PARTITION BY,
  window peers, `IS [NOT] DISTINCT FROM`.
- Joins (hash or not), IN, NOT IN, ANY/ALL, UNIQUE, ON CONFLICT and FK use
  equality: NULL matches nothing, and UNIQUE allows many NULLs.
- **Order**: the type's `<`.
- Every expression has a type (possibly a plain SQLite type) or is an
  untyped literal or parameter. Functions and operators declare result
  types. "Whose type is known" in this document means every expression.

## R1. The plan doesn't change the answer
Index or table scan, nested loop or hash join, sorter or index order, the
min/max index shortcut or a full aggregate: the result rows are the same
under the type's equality. Their text may differ when equal values print
differently, and which of several equal values min/max, DISTINCT or a
LIMIT after ORDER BY returns is unspecified (as in PostgreSQL).
- Fails on #11143: type `ENCODE value*2 DECODE value/2`, index on `c`,
  `SELECT max(c) FROM b` returns 5 instead of 10.
- This includes comparisons of two different types (R6): `int = numeric`,
  `date = timestamptz` give the same rows in a hash join, IN list,
  compound select and nested loop.

## R2. Each type says what it supports, and every site obeys it
A type has one of: no equality, equality only, or equality and order.
- Equality and order agree: `a = b` exactly when neither `a < b` nor `b < a`.
- Equal values hash alike.
- Every equality site uses the type's equality or grouping equality (see
  Terms). Every order site (`<`, BETWEEN, ORDER BY, min/max, range seeks,
  window ORDER BY and RANGE frames, ORDER BY inside aggregates) uses the
  type's order. The site lists are illustrative; tests must cover every
  equality and order site in the grammar.
- A site that needs something the type doesn't support is an error naming
  the type, at prepare time. PostgreSQL: `json = json`, `DISTINCT json`,
  `json PRIMARY KEY`, `ORDER BY point` all error.
- Where the type allows collations (text-like types), equality and order
  are per (type, collation). Nondeterministic collations need not be
  implemented, but the design must not rule them out.
- Example failure: `neg` type (`ENCODE -value DECODE -value`), `ORDER BY c`
  and `c > 4` disagree about which values are large.
- Reported by a reviewer, not yet reproduced: tursopg on #11143 sorts and
  dedupes inet, cidr, macaddr and jsonb differently from PostgreSQL.

## R3. A typed value keeps its meaning wherever it goes
A typed value keeps its type's equality and order through subqueries,
CTEs, views, compound selects, CASE (both forms), COALESCE, IFNULL,
NULLIF, min/max, aggregates with DISTINCT or ORDER BY, window functions
(lag, lead, first_value, rank peers), JOIN USING, INSERT ... SELECT,
UPDATE ... FROM, triggers (`new.x`), RETURNING, CAST, and function results.
- Fails on #11143: `coalesce(c, 0) > 4` vs `c > 4` on `neg`.
- Fails on #11143: lossy DECODE, `WHERE x = 1.0` finds nothing on the table
  and two rows through a subquery.
- Untyped literals are different: PostgreSQL resolves `'1'` from context,
  so `'1' = 1` is true but `WITH c AS (SELECT '1' x) SELECT x = 1 FROM c`
  errors (the CTE made it text). The PostgreSQL frontend follows
  PostgreSQL's resolution rules for these.

## R4. NULL stays NULL
ENCODE and DECODE must map NULL to NULL and non-NULL to non-NULL. A
violation is a runtime error naming the type, never a silent NULL. The
error aborts the statement like any other runtime error (statement
rollback).
- Fails on main: `ENCODE 1/value`, `INSERT 0` silently stores NULL.
- Fails on #11143: `DECODE nullif(value,'')`, `WHERE w IS NULL` finds a row
  that `SELECT w IS NULL` says is not NULL.
ENCODE errors propagate as errors.

## R5. Determinism and session settings
- DECODE gives the same output for the same input. It reads no session
  settings and no clock. `random()` stays rejected everywhere.
- ENCODE and COERCE are input conversion (next bullet): they may read the
  clock and session settings for inputs that name them (`'now'`, a
  timestamptz without an offset). A value already of type T converts the
  same way every time.
- Input conversion may read session settings (time zone). The stored form
  must not depend on them: `'2024-01-01 00:00+02'::timestamptz` stores the
  same bytes under every TimeZone. An index never needs rebuilding when a
  setting changes.
- Output conversion may read session settings (timestamptz prints in the
  session TimeZone).

## R6. Literals and constants
- An untyped literal compared with, assigned to, or combined with a value
  of type T is a value of T: it goes through T's input conversion. This
  holds in both frontends, in WHERE and in writes, so `a = 'AB'` and
  `INSERT ... ON CONFLICT (a)` agree. (On main they don't.) Type-parameter
  coercion (COERCE: `numeric(10,2)` rounding, `varchar(3)` length) applies
  to writes only, never to comparisons: on `numeric(10,2)`, `n = 3.504` is
  false for a stored 3.50, and `varchar(3)_col = 'abcdef'` is false, not
  an error (PostgreSQL).
- A literal whose value is fixed (`'2024-13-45'` as a date) is checked at
  prepare time, even in a branch that never runs and on an empty table
  (PostgreSQL does this).
- A literal is converted with the session settings in force when the
  statement is prepared, including `'now'` and `'today'`: an explicitly
  prepared statement keeps that result after `SET TimeZone` (PostgreSQL
  does this). A new statement uses the current settings. Literals in
  views, DEFAULT and partial indexes are converted at CREATE.
- Bound parameters are converted per execution, not per row. `now()`
  equals the transaction start time.
- These literals must be accepted with PostgreSQL's meaning: `'today'`,
  `'infinity'`, `'-infinity'`, `'11:00 PM'`, `'01/01/2024'` (DateStyle
  MDY), `'20240101'`, `'1.2.3.4/32'` for inet, `'epoch'`. Any other
  literal PostgreSQL accepts for an implemented type and tursopg rejects
  is a bug.

## R7. Comparing two different types
- Which type pairs compare, and how, is decided by the frontend, never by
  type-name checks in core.
- PostgreSQL frontend: follow PostgreSQL. Allowed: int2/4/8, float4/8,
  numeric among themselves; date/timestamp/timestamptz; inet/cidr. Not
  allowed: `inet = text`, `int = text`, `bool = int`, `date = text`,
  `json = jsonb`, arrays of different element types. Where PostgreSQL
  converts the column side (int vs numeric compares as numeric, numeric vs
  float8 as float8), so does tursopg.
- Comparisons that depend on the session time zone (date vs timestamptz)
  use the zone at execution time.
- Common types for UNION, INTERSECT, EXCEPT, CASE, COALESCE, VALUES follow
  PostgreSQL's type category and preferred-type rules (`date UNION
  timestamp` is timestamp; `json UNION jsonb` errors).
- SQLite frontend: a constant (untyped literal, parameter, constant
  expression) compared with a custom type is converted to the custom type
  (R6). A non-constant of a plain type is never converted: it compares
  with the custom type's value directly under SQLite rules when both are
  the same kind of value (both numbers, both text, both blobs); otherwise
  it is an error naming both types. Whether a statement errors must not
  depend on the plan.
- Equal values of two different types hash alike once converted to the
  type the comparison uses (R1 last bullet).

## R8. User types
- Every capability a user type has gives the same answer on every plan and
  through every wrapper (R1, R3). A capability it lacks is an error naming
  the type and what is missing.
- The design must say what a type with no comparison declaration supports,
  and why, on the merits for a new user. What main or any earlier version
  did is not a reason. If an operation is an error, the error says how to
  declare the missing capability.
- If a user declaration about meaning (for example "ENCODE keeps order") is
  trusted and false, results may be in the wrong order, but every plan
  must still give the same rows (R1). Declarations must not be able to
  break plan agreement.

## R9. Writes
UNIQUE, PRIMARY KEY, ON CONFLICT and automatic indexes use equality (many
NULLs allowed in UNIQUE). FK parent/child use the frontend's cross-type
rules (PostgreSQL: int2 → int8, int → numeric, timestamptz → date work).
CHECK and generated columns see the value. `UPDATE t SET a = a` leaves
indexes consistent. `PRAGMA integrity_check` must report an index entry
that disagrees with its table under the type's equality and order (test:
write a wrong entry by hand and see the report).

## R10. Collations, untyped storage and schema changes (SQLite frontend)
- `COLLATE` on a custom-type column either applies to the type's equality
  and order (only if the type allows collations) or is rejected at CREATE
  TABLE. Never silently ignored.
- Storing a custom-type value into an untyped or `ANY` column stores the
  DECODE result as a plain value; it stops being the custom type. Docs say so.
- `DROP TYPE` while a column or index uses the type errors. A type cannot
  be redefined in place.

## R11. PostgreSQL types the design must be able to express
Implement now: date, timestamp, timestamptz, time, uuid, bool, bytea,
numeric, int/float, inet, cidr, macaddr, json, jsonb, arrays. Must be
expressible without new core code per type (not implemented yet): interval
(equality coarser than its fields), timetz, enum (declaration order,
changeable by ALTER TYPE), ranges (canonical forms, empty), domains
(domain over domain; UNION of two of the same domain keeps it), composite
types (NULL fields compare equal for grouping), char(n) padding, NaN and
-0. "Expressible" is shown by a written sketch for each (which comparator,
which conversions, which Dialect rules), reviewed, not by code.

## R12. Frontend boundaries
Behavior that differs between frontends goes behind the `Dialect` trait.
PostgreSQL semantics live in postgres/. Types that core provides to both
frontends (numeric, json, uuid, arrays, ...) register through the same
mechanism as frontend types, with no separate path. Core contains no
per-type code for frontend types: not by name, by id, or by enum variant.

## R13. Errors and EXPLAIN
- PostgreSQL frontend: the SQLSTATE sent on the wire matches PostgreSQL's
  for: operator does not exist (42883), could not identify an
  equality/ordering operator (42883), invalid input syntax (22P02),
  cannot cast (42846), UNION types cannot be matched (42804; PostgreSQL 17
  gives 42846 for `json UNION jsonb`, follow PostgreSQL per case). The message
  text names the same types PostgreSQL names; exact wording need not match.
- EXPLAIN shows the comparison a site uses (the comparator's name in the
  collation column, as `numeric` is shown today).

## R14. Plain SQLite is unchanged
SQLite-frontend schemas without custom types: no change in results or
bytecode. (PostgreSQL-frontend types such as int vs numeric may change,
per R7.)

## R15. Performance
- Converting a constant: once per execution (R6).
- For a column whose type has order, on a 100k-row table, EXPLAIN of
  `c = k`, `c < k` and `ORDER BY c` with an index on `c` shows an index
  search or index-order scan, and the result matches `NOT INDEXED`. This
  includes types whose order is not SQLite's order of the stored bytes.
- At most one conversion (ENCODE, DECODE or key computation) per row per
  site. No conversion inside a comparison.
- Plain int, real and text comparisons: no instruction-count regression
  beyond 2% on the existing callgrind benchmarks.

## R16. No backward compatibility
Custom types are experimental. Databases, indexes and schemas created
with custom types by any earlier version are unsupported: what happens to
them is out of scope, including wrong results. Do not design, test or
review for them.

## Testing oracle
- PostgreSQL 17 in a database with `C` collation, so text order is
  bytewise (tursopg doesn't implement locale collations; separate work).
  `redesign/pg.sh` creates such databases.
- PostgreSQL's own plan-dependent text (R1) is compared by value, not text.

## Not covered (stated so nobody assumes otherwise)
NULLS FIRST/LAST defaults (PostgreSQL sorts NULL last ascending, SQLite
first; already handled by the frontends today), MERGE, client result-type
reporting, ALTER TYPE beyond enums, locale collations.
