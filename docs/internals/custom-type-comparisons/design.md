# Custom type comparison: design (part A)

> # ⚠️ FAIL CLOSED ⚠️
>
> **When the rules don't say exactly what a comparison, sort, hash or
> conversion means, it is an error. Never a default.**
>
> - No "unknown, so use SQLite rules". No "the values look alike, so
>   compare them". No trait read off another trait (text kind ≠
>   collatable).
> - No implicit conversion except the ones written down here (constants,
>   section 4).
> - An error tells the user what to write instead, usually a CAST.
> - Loosening a rule later is easy. Taking back a wrong answer users
>   depend on is not.
>
> Every design step and every review checks for new fallbacks.

Requirements: `requirements.md`. Map of the code before the redesign:
`code-map.md`. Progress: `README.md`.

- **Part A (this document):** every custom type compares by the value
  SELECT shows, the same way on every plan and through every wrapper.
  Where part A can't give PostgreSQL's answer, it gives an error, never a
  different answer.
- **Part B (later):** Rust-defined PostgreSQL types with compact value
  forms, a comparator registry, PostgreSQL's typing rules. Section 9.

## 1. Forms

- **stored** = `COERCE(affinity_base(ENCODE(x)))`. Lives in table records.
- **value** = `DECODE(stored)`, or `stored` when the type has no DECODE.
  Lives everywhere else: registers, index entries, sorters, hash tables,
  ephemeral tables, subquery/CTE outputs, aggregate accumulators, trigger
  NEW/OLD, `excluded.*`, RETURNING.
- **CAST(x AS T)** = `DECODE(COERCE(affinity_base(ENCODE(x))))`, the same
  value a column read gives. `translate_cast_expr` already ENCODEs and
  DECODEs; it gains `affinity_base` (without it, an integer-based type
  whose ENCODE returns `'5'` gives `'5'` from CAST and 5 from a column).
- **WITHOUT ROWID**: a custom-type primary-key column is an error at
  CREATE TABLE.

### Determinism
- **DECODE** recomputes index entries on DELETE and UPDATE, so it must be
  pure. CREATE TYPE rejects non-deterministic built-ins and extension
  functions not registered as deterministic. Date/time functions raise a
  runtime error on `'now'`, `'localtime'` and `'utc'`, as SQLite does in
  CHECK and index expressions (`sqlite3NotPureFunc`).
- **ENCODE and COERCE** are input conversion (R5) and may read the clock:
  `INSERT ... VALUES ('now')` into a date column works. `random()` and
  other non-deterministic built-ins are rejected.
- **Persisted expressions** (index expressions, partial-index WHERE,
  CHECK, generated columns, DEFAULT): constants converted inside them
  run ENCODE and COERCE under the DECODE rule, so `WHERE d > 'now'` in a
  partial index is an error and index membership never drifts. CREATE
  runs each such conversion once and reports its error then (R6).
- Pure extension functions (`uuid_str`, `uuid_blob`, ...) are registered
  as deterministic (today every extension function registers as
  non-deterministic, core/ext/mod.rs `register_scalar_function`).

### Readers and index entries
- A table-cursor read: `Column` then DECODE. An index-cursor read
  (covering scans, the outer-scope index-cursor fallback in
  `translate_column_expr`, automatic indexes): `Column` only.
- A virtual generated column of type T computes
  `DECODE(COERCE(affinity_base(ENCODE(expr))))` once per read.
- One funnel owns every index write, probe and read of a custom-type
  column: `emit_index_entry_value` (writes and probes) and the reader rule
  above. The step that adds it derives the list from the code (every
  index insert, delete, probe and seek-key builder; known so far:
  insert.rs:3014 and 3593, emitter/mod.rs:2060 and 2169, close.rs:1038
  `emit_autoindex`, index.rs:1320, seek.rs:23 and its callers, schema.rs:7758,
  update.rs:2043 and its unique-check probe, the RIGHT JOIN match index,
  the ALTER TABLE rewrite and validation scans, `integrity_check`'s
  expected entries at integrity_check.rs:560-620 and 774-807). The
  custom-type sqltests run `PRAGMA integrity_check` after every statement.

### Writes
For each custom-type column of a new row: `s = COERCE(affinity_base(
ENCODE(x)))` and `v = DECODE(s)`, both always computed, so a DECODE that
errors fails the write whatever indexes exist. BEFORE triggers,
`excluded.*`, CHECK, generated columns, FK checks, index entries and
RETURNING see `v`. A BEFORE trigger that assigns NEW.c recomputes c's `s`
and `v`. A domain's CHECK applies to writes only, not to comparisons.

### VACUUM
VACUUM copies table records and index entries byte for byte, as SQLite's
VACUUM does through the transfer optimization (sqlite vacuum.c:312 runs
`INSERT ... SELECT*FROM`, which insert.c `xferOptimization` turns into a
raw record copy). Turso's VACUUM today re-reads and re-inserts through SQL
(core/translate/vacuum.rs), which runs DECODE then ENCODE and COERCE and
can change stored bytes.

### Schema changes
- ADD COLUMN with DEFAULT: the default goes through the write path once
  at ALTER time; a non-constant default is rejected.
- ALTER COLUMN TYPE: each row's `DECODE_old(s)` goes through the new
  type's write path; every index containing the column is rebuilt; FK
  children referencing the column are re-checked.
- DROP TYPE errors while a column, domain, type, index, CHECK, generated
  column, view or trigger uses the type (views, triggers and CHECK found
  by parsing their SQL).

## 2. What a type compares like

Each type has a **value kind** (integer, real, number, text or
blob), a **comparison** and a **capability** (`None`, `Equality`,
`Order`).

1. **Registered traits.** Whoever defines a type registers its traits
   through one function (`register_type_traits`), next to the definition
   in effect: core for the types core defines, `postgres/` for the ones
   tursopg defines itself (postgres/frontend/catalog.rs) (R12). Part A
   registrations:

   | type | value kind | comparison | capability |
   |---|---|---|---|
   | numeric | number | `Type(Numeric)` | Order |
   | uuid | text | BINARY | Order |
   | date, time, timestamp, timestamptz, varchar | text | text | Order |
   | boolean, smallint, bigint | integer | integer | Order |
   | bytea | blob | blob | Order |
   | json, jsonb, inet, cidr, macaddr, macaddr8 | text | — | None |

   A type gets a capability only where comparing its part-A value gives
   PostgreSQL's answer. jsonb equality ignores key order; inet, cidr and
   macaddr accept several spellings of one value; json has no equality.
2. **STRUCT and UNION**: capability None (bytewise comparison would make
   a numeric field 1.0 differ from 1.00). Field-wise comparison is part B.
3. **No DECODE**: the base type's traits (the base may be a custom type;
   domains inherit this way).
4. **DECODE**: from `type_of(DECODE expr)`, with `value` bound to the
   base type. A custom result type U gives U's traits. A plain SQLite
   type gives that kind, SQLite rules with the expression's collation,
   Order.
5. **No unknown kind.** When `type_of(DECODE expr)` is `Any`, CREATE
   TYPE fails: `cannot tell what kind of value DECODE of type t returns;
   wrap it in CAST(... AS <type>)`. The CAST converts every value, so the
   kind it names is always true. `Any` must mean the result really
   varies (`unixepoch(x, 'subsec')`, a CASE or coalesce mixing kinds, a
   C extension function, which has no way to declare a type), never "not
   tracked":
   - Built-in functions already declare their result type through an
     exhaustive match (`ScalarFunc::result_type`).
   - Functions core registers through the extension API (time_*,
     regexp, uuid_*) declare their result type at registration. A test
     fails for any core-registered function without one.
   - A result type that differs by dialect, such as `round(integer)`
     (real in SQLite, exact in PostgreSQL), comes from the dialect, not
     `Any`.
   
   A type whose DECODE is `Any` when the schema loads (a native
   extension function that declared a type at CREATE TYPE but is not
   loaded now) gets capability None, so every comparison errors with the
   same hint. It is never compared by a guessed rule.
6. **`EQUALITY ONLY`** on CREATE TYPE lowers a type to Equality. Nothing
   raises a type above what rules 1-4 give. tursopg's enums use it:
   PostgreSQL orders them by declaration, not by label text, so they get
   no Order until part B.

- **Arrays** compare element by element with the element type's
  comparison, then the shorter first; NULL elements equal each other and
  sort after non-NULL (PostgreSQL), so `ARRAY[1,NULL] = ARRAY[1,NULL]`.
  `Type(Array)`/`Type(TextArray)` (core/type_ordering.rs) are checked
  against this. A custom type may be an array element only when this
  follows from its definition: its value form is its stored form (no
  DECODE in its chain), its comparison is the plain SQLite comparison of
  its kind, and its capability is at least Equality. `boolean[]`,
  `smallint[]`, `date[]`, `uuid[]` and `varchar[]` pass; `numeric[]`
  (comparison `Type(Numeric)`) and any type with DECODE are errors.
- **Affinity.** Custom-type values have no affinity: none on index
  entries of custom-type columns, none in comparisons involving one
  (`satisfies_index_affinity`, FK affinity strings,
  `emit_pk_uniqueness_check`).
- **COLLATE** and **window RANGE offsets** are traits of their own, not
  read off the value kind. A text kind doesn't make a type collatable,
  and an integer kind doesn't give it RANGE offsets.
  - Registered types declare both. Part A: only varchar is collatable,
    as in PostgreSQL (`uuid_col COLLATE nocase` and `ORDER BY ts COLLATE
    x` are errors). RANGE offsets are declared per (type, offset type)
    pair, as PostgreSQL's `in_range` functions are: smallint and bigint
    with integer offsets, numeric with integer or numeric offsets (using
    numeric's exact `+` and `-`). Part A has no interval type, so
    `RANGE 1 PRECEDING` over a date, time or timestamp is an error, as in
    PostgreSQL; so is any offset over boolean.
  - A user type is collatable if its DECODE result is text. It takes
    RANGE offsets only for an offset type it declares `+` and `-` for.
  - Anything else is an error naming the type.

## 3. Comparisons

```rust
fn comparison_for(sides: &[&ExprType], need: Need, ctx) -> Result<Comparison>
enum Need { Equality, Order }
struct Comparison {
    conversions: SmallVec<[Option<Conversion>; 2]>, // constants only (section 4)
    comparator: ComparatorHandle,  // opaque; wraps CollationSeq
    affinity: Option<Affinity>,    // None when a custom type is involved
}
```

- Replaces `comparison_collation`, `expr_collation`, `in_list_collation`,
  `in_subquery_collation`, `compound_column_collation`,
  `first_collation`, `key_collation`.
- Equality sites: `=`, `!=`, IS, IS DISTINCT FROM, IN, ANY/ALL, `CASE x
  WHEN`, NULLIF, joins, GROUP BY, DISTINCT, compound, PARTITION BY,
  window peers, UNIQUE, ON CONFLICT, FK. Order sites: `<` family,
  BETWEEN, ORDER BY, min/max, window ORDER BY and RANGE, range seeks,
  ORDER BY inside aggregates. Capability too low: prepare error naming
  the type and the site.
- The optimizer keeps the `Comparison` on each constraint. An index
  column is usable when its side has no conversion and the comparator and
  affinity match the index column's.
- Hash joins and bloom filters hash with `ComparatorHandle::hash`, equal
  values alike (`Type(Numeric)` hashes the parsed number). A comparator
  with no hash gets no hash join.
- FK lookups and child scans use the FK's `Comparison`. Row values: one
  comparison per component. IN lists: one per element.
- Window RANGE with an offset over a custom type: allowed when the type
  takes RANGE offsets (section 2), otherwise an error.

### Which pairs compare (`Dialect::common_type`)
**SQLite default.** A custom type T against a non-constant side compares
values directly only when:
- the other side is T (any parameters), or the two are linked only
  through domains (a domain and its base, directly or through other
  domains),
- T's comparison is `Type(Numeric)` and the other side is integer or
  real (real: numeric converts to real first, as PostgreSQL does).

Anything else errors at prepare: `cannot compare type X with type Y; use
CAST`. In particular:
- Two different types never compare because their values look alike. A
  `celsius` and a `fahrenheit` type over REAL, or uuid and a user type
  over text, need a CAST.
- A type with its own ENCODE or DECODE never compares directly with a
  plain value, its base type included. A constant goes through ENCODE
  (section 4) and a column would not, so `email = 'A@x.com'` and
  `email = text_col` holding `'A@x.com'` would give different answers.
  `CAST(text_col AS email)` converts the column the same way.
- A side whose type is `Any`, such as a column declared without a type.

Constants, parameters included, convert to T (section 4), so `WHERE id =
?` works.

**PostgreSQL** (`PostgresDialect::common_type`, part A): direct
comparison only where it gives PostgreSQL's answer: same type, domain and
base, smallint/integer/bigint among themselves (plain INTEGER included),
numeric with those, varchar with text. Every other pair errors until part
B, including `date = timestamp`.

### Result types
CASE, COALESCE, IFNULL, NULLIF, `iif`, VALUES, compound columns, JOIN
USING, recursive CTE columns, and every function whose result is one of
its arguments (`min`, `max`, `lag`, `lead`, `first_value`, `last_value`,
`nth_value`, declared once in the function registry as pass-through):
1. Non-constant arguments decide the type through `Dialect::common_type`
   (same rules as pairs; a pair that can't compare is an error).
2. Constant arguments convert to that type (section 4).
3. No custom-typed argument: SQLite rules.

## 4. Constants

- A **constant** is an untyped literal, a parameter, or a deterministic
  expression built only from those. `CAST(x AS T)` has type T.
- A constant meeting type T gets `cmp_input(x) = DECODE(affinity_base(
  ENCODE(x)))`, **no COERCE** (R6): on `numeric(10,2)`, `n = 3.504` is
  false; `varchar(3)_col = 'abcdef'` and `smallint_col = 40000` are false.
- Conversions run once per execution, before any row is read, including
  in branches that never run. In persisted expressions they also run at
  CREATE (section 1).

## 5. NULL

ENCODE, DECODE and COERCE are skipped for NULL input. A non-NULL input
that gives NULL is a runtime error naming the type and the expression,
unless the expression is `value`.

## 6. Deleted

`OPERATOR '<'`, `TypeDef::is_sortable`, `index_order_differs_from_value_order`,
`suppress_custom_type_decode`, `needs_decode`, seek-key ENCODE,
decode-for-sort and decode-for-CHECK/trigger/FK copies, `type_of`'s
"any custom argument makes COALESCE/min/max custom" rule.

## 7. Performance

- Plain SQLite: `comparison_for` runs at translate time; the runtime
  `CollationSeq` is unchanged. A diff of the compiled programs against
  main, over the whole test corpus, shows that comparisons without custom
  types compile the same (step2-comparison-for.md section 1).
- Custom types: covering scans skip DECODE; constants convert once per
  execution.
- Not met in part A: numeric parses its text in each comparison; tursopg
  uuid index entries are 36-byte text.

## 8. Steps

Each merges alone with cargo test and both conformance suites green,
with sqltest expectations rewritten in the same commit.
1. NULL checks and determinism (sections 1, 5).
2. `comparison_for` and `ComparatorHandle` with today's rules, no
   behavior change, proven by a program dump diff against main
   (step2-comparison-for.md).
3. Traits per section 2 through `register_type_traits`; `type_of` with a
   `value` binding; result types for core-registered functions and the
   CREATE TYPE error for an `Any` DECODE; `Need` and capability errors;
   comparators from column declarations (`key_collation`,
   `Column::collation()`) through the same rules; array rules; delete `OPERATOR '<'` and
   `is_sortable`; `EQUALITY ONLY`; ORDER BY sort keys become values
   (deleting `OPERATOR '<'` while sorting stored values would give wrong
   orders); rewrite `custom_type_ordering.sqltest`.
4. Values in hash payloads, CHECK, trigger and FK copies.
5. The index funnel for writes, probes and reads, still storing the
   stored form; `integrity_check` after every statement in the
   custom-type sqltests.
6. VACUUM copies records byte for byte.
7. Index entries become values; constants convert in every comparison
   (seek keys and filters), carried as `Conversion`s on `Comparison`;
   `Comparison.affinity`, with no affinity on custom-type entries; CAST
   gains `affinity_base`.
8. Pair rules and result types (section 3) through
   `Dialect::common_type` and its PostgresDialect override,
   persisted-expression rules, schema changes.

## 9. Part B

### What part A leaves it
- Registers, index entries, sorter and hash keys hold the value form;
  table records hold the stored form. Part B changes what the value form
  is for Rust-defined types (date as days, uuid as 16 bytes, numeric as
  order-preserving bytes). Every use of such a value that expects text
  (output, string functions, CAST to text, untyped columns) then needs an
  output conversion.
- Comparisons go through `ComparatorHandle`, pairs and result types
  through `Dialect::common_type`, traits through `register_type_traits`.

### Must cover
- Rust-defined types, their input (`'today'`, `'epoch'`, `'infinity'`,
  `'11:00 PM'`, `'01/01/2024'`, `'20240101'`), and output.
- Comparisons for jsonb, inet, cidr, macaddr, STRUCT/UNION field-wise,
  enums, ranges, composites, arrays of custom types.
- PostgreSQL typing: parameter types, the cast table, cross-type
  comparisons (`date = timestamp`, DST), function result types (`now()`,
  `current_date`, comparison results as boolean), volatility, literal
  checks at prepare, SQLSTATEs.

## 10. Not in this design

These are bugs of their own, found while reviewing:
- Sync replay binds stored bytes as ordinary parameters
  (`database_replay_generator.rs`), so a replayed custom-type column is
  ENCODEd twice.
- tursopg's `bytea` has no input conversion (`b = '\xdead'` is false),
  and `timestamp`'s ENCODE keeps milliseconds and converts offsets to UTC.
