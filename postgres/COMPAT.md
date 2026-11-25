# Turso Postgres Compatibility Reference

This document tracks PostgreSQL feature compatibility for the Turso Postgres
frontend. The feature list is based on the official
[PostgreSQL feature matrix](https://www.postgresql.org/about/featurematrix/),
with additional rows for baseline features the matrix does not enumerate.

Status legend:

| Status | Meaning |
|--------|---------|
| ✅ Supported | Feature works and is covered by tests |
| 🟡 Partial | Feature works with limitations (see notes) |
| ❌ Not supported | Feature is not implemented |

## PostgreSQL version

The reference version is PostgreSQL 18. The PostgreSQL conformance tests, the
SQLSTATE fuzzer and the `pg_dump` differential fuzzer compare Turso against a
PostgreSQL 18 server, and CI uses PostgreSQL 18 clients. The version is set in
`postgres/version`. Behavior that differs between PostgreSQL versions follows
PostgreSQL 18, and a newer release is supported only after the version there
is changed and the tests pass against it.

## Architecture

The frontend parses SQL with `pg_query` (libpg_query, the real PostgreSQL
grammar), so virtually all PostgreSQL *syntax* is accepted at parse time.
Support is decided by the translator (`postgres/parser/translator.rs`), which
converts the PostgreSQL AST into Turso's native AST, and by what the Turso
engine can execute. Components:

- `postgres/parser` — parse + translate PostgreSQL SQL to Turso AST
- `postgres/frontend` — pg_catalog emulation, COPY, schemas, session handling
- `postgres/server` — PostgreSQL wire protocol (v3) server, built on `pgwire`
- `postgres/cli` — `tursopg`, a psql-like REPL that can also host the server

Because the parser accepts the full PostgreSQL grammar, a clause the
translator does not implement must be rejected explicitly, or its meaning is
silently dropped. The translator fails closed: unimplemented clauses produce
a "... is not supported" error rather than running with different semantics.
The few remaining cases that are accepted without their full meaning are
called out as "silently ignored" in the notes of the tables below.

## Core SQL baseline

Basics not enumerated by the official feature matrix.

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT (projections, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET) | ✅ Supported | |
| JOINs (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, USING) | ✅ Supported | |
| UNION / UNION ALL / INTERSECT / EXCEPT | 🟡 Partial | Including ORDER BY/LIMIT on compounds; INTERSECT ALL / EXCEPT ALL, a set operation nested on the right (`A UNION B INTERSECT C`, `A EXCEPT (B UNION C)`) and ORDER BY/LIMIT/WITH on a parenthesized branch are rejected |
| Subqueries (FROM, IN, EXISTS, scalar) | ✅ Supported | `= ANY(array)` and `<> ALL(array)` work, including bound text-array parameters; other ANY/ALL array operators are rejected; `ALL`/row comparison subqueries error; an EXISTS subquery may select no columns (`EXISTS (SELECT FROM t)`), but a SELECT with no columns anywhere else is rejected |
| INSERT (column lists, multi-row VALUES, DEFAULT, DEFAULT VALUES, INSERT ... SELECT) | ✅ Supported | Without a column list, `INSERT ... SELECT *` must give a value for every column that is not generated; duplicate target columns, array-element/field targets, OVERRIDING USER VALUE and ON CONFLICT with DEFAULT VALUES are rejected |
| UPDATE (incl. FROM clause) | ✅ Supported | Multi-column `SET (a,b) = (...)` not supported; duplicate assignments and array-element/field targets rejected |
| DELETE | 🟡 Partial | `USING` clause rejected |
| CREATE TABLE | ✅ Supported | PK, NOT NULL, UNIQUE, DEFAULT, CHECK, FK (with ON DELETE/UPDATE actions); IF NOT EXISTS; tables are created STRICT |
| CREATE TABLE AS / SELECT INTO | 🟡 Partial | Schema derived from the SELECT; a result column whose type converts values (timestamptz, timestamp, date, time, uuid, json, numeric, boolean, a comparison or other condition, inet, domains, now()), is rejected with 0A000 because the copy would be a plain column; a copy of a varchar(n) or smallint column keeps the values but not the length or range check; WITH NO DATA supported (lowered to LIMIT 0, so errors in an overridden LIMIT go unreported); explicit column list rejected; INTO on the first leaf of a compound SELECT (legal in PG) rejected; TEMP, WITH (...), TABLESPACE, USING and ON COMMIT rejected; completes with `SELECT n` like PostgreSQL, though an IF NOT EXISTS skip tags `SELECT 0` instead of `CREATE TABLE AS` |
| ALTER TABLE | 🟡 Partial | ADD/DROP COLUMN, RENAME TABLE/COLUMN, OWNER TO, ENABLE/DISABLE ROW LEVEL SECURITY work (RENAME TABLE, like CREATE INDEX, needs CREATE on the schema, which only a superuser has until GRANT exists); one subcommand per statement; ALTER COLUMN TYPE, IF [NOT] EXISTS (except for OWNER TO) and CASCADE rejected; SET/DROP DEFAULT, SET/DROP NOT NULL, ADD CONSTRAINT rejected |
| ALTER TABLE/VIEW/MATERIALIZED VIEW/SEQUENCE/INDEX ... OWNER TO | 🟡 Partial | Changes the stored owner, which may then alter and drop the relation; a table's indexes and serial sequences move with it, ALTER INDEX changes nothing, and a serial column's sequence refuses its own OWNER TO (0A000); superuser-only for now, so an owner cannot transfer ownership |
| CREATE INDEX | ✅ Supported | UNIQUE, multi-column, partial (WHERE), expression indexes, IF NOT EXISTS. An index expression or a WHERE clause that uses `->` or `->>` fails with `0A000` |
| CREATE VIEW | ✅ Supported | A table named without a schema is bound to the schema it is found in when the view is created, so the view keeps reading it when `search_path` changes; column aliases supported when they name every select-list column; TEMP, UNLOGGED, OR REPLACE, WITH (options) and WITH CHECK OPTION rejected |
| COMMENT ON | 🟡 Partial | Stored in `pg_description` for tables, views, materialized views, indexes, sequences, table columns and schemas. Any other kind of object, a column of a view, a catalog relation such as `pg_class`, and the index of a PRIMARY KEY or UNIQUE constraint, is rejected with 0A000 |
| CREATE SCHEMA / DROP SCHEMA | ✅ Supported | A schema is stored in the main database under an id; its objects are stored under names that start with `__turso_ns<id>_`, which no user object may be named; an object qualified with `main` or `temp` is an error, since PostgreSQL has no such schemas; `public` is a schema like any other, created when a new database is made, and once dropped it stays dropped until CREATE SCHEMA public; DROP ... CASCADE; AUTHORIZATION rejected; CREATE TABLE without a schema creates the table in the first schema of `search_path` that exists; CREATE SCHEMA can contain CREATE TABLE, VIEW, INDEX and SEQUENCE statements, which create their objects in the new schema, but not inside a transaction block. Databases with schemas in `turso-postgres-schema-*.db` files from earlier versions are not supported; their schemas must be recreated. A database made by an earlier version, whose tables are in no schema, is refused when a session connects. |
| ALTER SEQUENCE | 🟡 Partial | OWNED BY { table.column \| NONE } works, which is how a serial column's sequence belongs to its table; the other options are rejected |
| CREATE SEQUENCE / nextval / currval / setval | ✅ Supported | START, INCREMENT, MIN/MAXVALUE, CYCLE; AS smallint/integer/bigint bounds the range; CACHE 1 accepted, a larger CACHE rejected (Turso does not cache sequence values); OWNED BY a column, TEMP and other options rejected; pg_sequences view; pg_sequence exposes the parameters of the sequences of every schema (bigint, cache 1); pg_class lists the sequences of every schema with relkind S (sequence dumps remain incomplete); a sequence named without a schema is looked up through `search_path`, and a column default, CHECK constraint, view or policy binds it to the schema it is in when the table, column, view or policy is created; a serial column's sequence is created in the table's schema |
| Identity columns (GENERATED ALWAYS / BY DEFAULT AS IDENTITY) | ✅ Supported | smallint, integer and bigint columns; the column is NOT NULL and gets its values from a sequence named `<table>_<column>_seq`, which takes the options of CREATE SEQUENCE (START WITH, INCREMENT BY, MINVALUE, MAXVALUE, CYCLE) and is bounded by the range of the column type; GENERATED ALWAYS rejects a value other than DEFAULT in INSERT, UPDATE and ON CONFLICT DO UPDATE with SQLSTATE 428C9, while COPY FROM and INSERT ... OVERRIDING SYSTEM VALUE write the values they are given; another column type (22023) and a DEFAULT on an identity column (42601) are rejected; OVERRIDING USER VALUE is rejected; ALTER SEQUENCE ... OWNED BY on its sequence is rejected (0A000) |
| CREATE DOMAIN | ✅ Supported | Base type + DEFAULT, NOT NULL, CHECK constraints enforced; array base types, type modifiers and non-binary COLLATE rejected |
| CREATE TYPE ... AS ENUM | ✅ Supported | Values validated on write; other CREATE TYPE forms unsupported |
| DROP TABLE / DROP COLUMN / DROP SEQUENCE and sequences | 🟡 Partial | DROP TABLE drops the sequences the table owns and DROP COLUMN drops the sequences the column owns (a serial column, or `ALTER SEQUENCE ... OWNED BY table.column`); each of them, and DROP SEQUENCE, is refused with 2BP01 when a column default, CHECK constraint, view, trigger or policy that is not dropped with it still calls the sequence. DROP SCHEMA ... CASCADE is rejected when an object of another schema uses a sequence of the schema, where PostgreSQL drops that object |
| DROP TABLE / DROP VIEW [, ...] [CASCADE] | 🟡 Partial | Without CASCADE, refused with 2BP01 when a view reads the relation, with the views that read it, directly or through other views, in DETAIL, and PostgreSQL's HINT. CASCADE drops those views too, of every schema, as does DROP SCHEMA ... CASCADE. Several relations can be named; all are dropped or none. No NOTICE names what CASCADE dropped. CASCADE is rejected with 0A000 when an object that is not a view depends on what it drops, such as a column default that calls a sequence of the table, where PostgreSQL drops the default. A role that is not a superuser needs to own the views that CASCADE drops, where PostgreSQL needs it to own only the relations it names. A table that a foreign key of another table references is still dropped, where PostgreSQL refuses without CASCADE, because foreign keys are not enforced |
| TRUNCATE | 🟡 Partial | Lowered to DELETE of one table; several tables, CASCADE and RESTART IDENTITY rejected |
| BEGIN / COMMIT / ROLLBACK | ✅ Supported | Every transaction is snapshot isolation, i.e. REPEATABLE READ; `ISOLATION LEVEL REPEATABLE READ` is accepted, every other level, DEFERRABLE and AND CHAIN are rejected. READ ONLY and READ WRITE are enforced, on BEGIN and on SET TRANSACTION; switching a READ ONLY transaction to READ WRITE after its first query is rejected with SQLSTATE 25001 like PostgreSQL. A statement error aborts the transaction but keeps its savepoints, so ROLLBACK TO recovers it; a failed COMMIT rolls the transaction back and returns the connection to idle |
| LOCK TABLE | 🟡 Partial | ACCESS SHARE without NOWAIT is accepted as a no-op inside a transaction for pg_dump compatibility; table names are not checked, no locks are acquired, and no read snapshot is started; other modes are rejected |
| Casts (`expr::type`, CAST) | ✅ Supported | Also `int4(x)`-style cast functions |
| Parameters (`$1`, `$2`, ...) | 🟡 Partial | Work through the extended wire protocol; text-format values only |
| Operators: `\|\|`, `%`, bitwise, LIKE/ILIKE [ESCAPE], SIMILAR TO [ESCAPE], `~`/`~*`/`!~`/`!~*`, IS [NOT] DISTINCT FROM, BETWEEN | ✅ Supported | BETWEEN SYMMETRIC is rejected. Regex operators lower to REGEXP; `~*`/`!~*` add the `(?i)` flag. SIMILAR TO converts its pattern to a regex the way `similar_to_escape()` does. LIKE is case-sensitive with `\` as the default escape; ILIKE applies LOWER() to both sides, then matches like LIKE. In CREATE INDEX, LIKE and ILIKE need a string-literal pattern without ESCAPE, and SIMILAR TO needs a string-literal pattern and ESCAPE |
| Dollar-quoted strings, escape strings (`E'...'`), bit/hex string literals | ✅ Supported | |
| generate_series | ✅ Supported | In FROM and with joins; table aliases name the output column (`AS g` gives `g`), explicit column aliases take precedence (`AS g(x)` gives `x`), and without an alias the column is named `generate_series`; qualified and unqualified calls resolve even with an empty search_path |
| pg_catalog emulation | 🟡 Partial | See Backend section |
| SET / SHOW / RESET | 🟡 Partial | Only search_path and custom parameters such as `app.user_id` (`SET [LOCAL]`, `SET ... TO DEFAULT`, `SET ... FROM CURRENT`, `RESET`, `RESET ALL`, `SHOW`, `set_config`, `current_setting`). SET of a few parameters to the value Turso already behaves as (e.g. `statement_timeout = 0`, `DateStyle = ISO`, `extra_float_digits = 3`, as sent by pg_dump) is accepted as a no-op; anything else is rejected |

## Backend

The pg_catalog tables emulated (live, reflecting real schema): `pg_class`,
`pg_namespace`, `pg_attribute`, `pg_type` (builtin + array + enum types),
`pg_index`, `pg_constraint`, `pg_attrdef`, `pg_tables`, `pg_sequence`, `pg_sequences`,
`pg_database`, `pg_roles` (built-in `postgres` superuser and `pg_database_owner`, and roles from `CREATE ROLE`), `pg_auth_members` (memberships from `GRANT role TO role`), `pg_proc`, `pg_am`,
`pg_policy` (row-level security policies, with `polqual` and `polwithcheck`
as the text `pg_get_expr` prints), `pg_policies`, plus `pg_input_error_info`. Present but
always empty: `pg_trigger`, `pg_statistic_ext`, `pg_inherits`, `pg_rewrite`,
`pg_foreign_table`, `pg_partitioned_table`, `pg_collation`,
`pg_publication*`, `pg_seclabels` (security labels are not supported).
`pg_description` is a stored table. It has the comments that `COMMENT ON`
stored, and the default `standard public schema` comment for `public`, so
pg_dump does not emit a command to clear that comment on restore. Dropping a
relation, a column or a schema deletes its comments. The public schema reports
`pg_database_owner` as its owner, with CREATE and USAGE for that role and USAGE
for PUBLIC. These are catalog defaults; role management is not implemented.
`pg_depend` has the rows of each view's `_RETURN` rule: the rule is part of
the view, and depends on each table and view that the view reads, as whole
relations and not column by column. The rule is not in `pg_rewrite` and the
view is not in `pg_class`. DROP reads these rows to find the views that depend
on what it drops.
`pg_operator` has the rows of PostgreSQL 18, with PostgreSQL's oids, for the
operators `=`, `<>`, `<`, `<=`, `>`, `>=`, `+`, `-`, `*`, `/`, `%`, `&`, `|`,
`<<`, `>>`, `~`, `!~`, `~*`, `!~*` and `||` whose operand and result types are
all among boolean, smallint, integer, bigint, real, double precision, numeric,
text, date, timestamp, timestamptz and uuid: 216 of PostgreSQL's 799 rows. It
leaves out the comparisons of two different types among date, timestamp and
timestamptz, which fail with 0A000, every operator of another type, such as
bytea, time, interval, json, jsonb, inet and the array types, and every other
operator name, such as `^`, `@` and `~~`. `oprcode`, `oprrest` and `oprjoin`
are 0, because `pg_proc` does not have PostgreSQL's functions under their oids.
`pg_statistic_ext` exposes the hidden system column
`tableoid` for pg_dump discovery. `pg_trigger` exposes `tgparentid` for
parent-trigger lookup and hidden `tableoid` during dump discovery.
`pg_rewrite` also exposes hidden `tableoid` for rule discovery, and `pg_policy`
exposes it for policy discovery. `pg_publication`, `pg_publication_rel`, and
`pg_publication_namespace` expose hidden `tableoid` for publication discovery.
`pg_get_triggerdef(oid)` and `pg_get_triggerdef(oid, pretty)` return NULL:
PostgreSQL triggers are not supported and `pg_trigger` is empty.
Catalog introspection functions: `format_type`,
`pg_get_constraintdef`, `pg_get_indexdef`, `pg_get_expr` (`pg_get_expr` and
`pg_get_constraintdef` print `CHECK` constraints, and `pg_get_expr` prints
column defaults in `pg_attrdef.adbin`, as PostgreSQL does for the forms the
manual lists under `CREATE POLICY`, and return TursoPG's stored SQL for
other forms; relation oid and pretty printing do not change the output),
`pg_get_userbyid`, `pg_*_is_visible`, `pg_encoding_to_char`,
Casting a value of date, time, timestamp, timestamptz, json, inet, cidr,
macaddr, macaddr8 or an enum to a type that does not read text fails with
42846. A number cast to, or stored in a column of, one of these types or jsonb
is read as its text: `20200101::date` and storing 20200101 in a date column
give 2020-01-01, where PostgreSQL refuses both. TursoPG binds a parameter sent
without a type as a number when its text is one, so refusing numbers would
refuse such parameters too. `date::json` and `date::inet` are still accepted, as are `1::json`, `1::jsonb`,
`1::inet`, `1::cidr` and `1::macaddr`, which PostgreSQL refuses. A jsonb value
may be cast to a number: `'1'::jsonb::int`, `::bigint`, `::numeric`, `::float8`.
`pg_input_is_valid`, `to_char` (numeric; other argument types fail with
0A000), `now`/`clock_timestamp`/
`transaction_timestamp`/`statement_timestamp`. `to_timestamp` fails with 0A000.
`date_trunc(text, timestamptz)` for the units microseconds, milliseconds,
second, minute, hour, day, week, month, quarter, year, decade, century and
millennium, with every spelling in PostgreSQL's units table. An unknown unit fails
with 22023. The units `timezone`, `timezone_h` and `timezone_m`, the
three-argument form with a time zone, and other argument types, such as
timestamp, fail with 0A000.
Session information functions: `version()`, `current_database()` /
`current_catalog`, `current_schema` (call, bare-keyword, and FROM-position
forms; the first schema of `search_path` that exists), `current_schemas(boolean)`
(existing schemas in search order, optionally including implicit `pg_catalog`), `pg_backend_pid()`, `quote_ident()`, `quote_literal()`;
`obj_description()`/`col_description()` read `pg_description`; `obj_description()`
knows the catalogs `pg_class` and `pg_namespace` and returns NULL for any other.
A call of either in a column DEFAULT, a CHECK constraint or an index, or with an aggregate or window function in an argument, is rejected with 0A000.
DML against pg_catalog is rejected. `pg_typeof` is not
implemented.
Configuration settings functions: `set_config()` and `current_setting()`,
for `search_path` and for custom parameters (see Configuration Management).
Setting changes follow the transaction as in PostgreSQL: a value that
`set_config()` sets with `is_local = true` ends with the transaction,
ROLLBACK undoes the setting changes of the transaction, ROLLBACK TO
SAVEPOINT undoes the changes after the savepoint, and a statement that
fails undoes its own changes.

| Feature | Status | Notes |
|---------|--------|-------|
| 64-bit large objects | ❌ Not supported | |
| Advisory locks | ❌ Not supported | |
| Custom background workers | ❌ Not supported | |
| Disk based FSM | ❌ Not supported | |
| Dynamic Background Workers | ❌ Not supported | |
| EXPLAIN (BUFFERS) support | ❌ Not supported | EXPLAIN is not supported at all |
| EXPLAIN (MEMORY) | ❌ Not supported | |
| EXPLAIN (SERIALIZE) support | ❌ Not supported | |
| EXPLAIN (WAL) support | ❌ Not supported | |
| "jsonlog" logging format | ❌ Not supported | |
| Loadable plugin infrastructure for monitoring the planner | ❌ Not supported | |
| Payload support for LISTEN/NOTIFY | ❌ Not supported | LISTEN/NOTIFY not supported |
| pg_stat_checkpointer system view | ❌ Not supported | |
| pg_stat_io - I/O metrics view | ❌ Not supported | |
| pg_wait_events system view | ❌ Not supported | |
| Server statistics in shared memory | ❌ Not supported | |
| SQL-standard information schema | ❌ Not supported | Only an `information_schema` row in pg_namespace; no views |
| Support for anonymous shared memory | ❌ Not supported | |
| XML, JSON and YAML output for EXPLAIN | ❌ Not supported | |

## Data Types, Functions, & Operators

Type mapping: serial/smallserial/bigserial (and serial2/4/8) become
`INTEGER NOT NULL DEFAULT nextval(...)` with an implicit sequence named
`<table>_<column>_seq`, or, when a relation of the schema has that name,
the first of `<table>_<column>_seq1`, `..._seq2`, ... that none has. boolean,
smallint, bigint, uuid, date, timestamp[tz], bytea, json, jsonb, inet,
cidr, macaddr, macaddr8 map to Turso custom types. varchar(n) and
numeric(p,s) keep their type modifiers; type modifiers on any other type,
including char(n), timestamp(p) and bit(n), are rejected. xml, tsvector/tsquery,
bit/varbit, geometric types degrade to TEXT; money to REAL; OID/reg* types to
INTEGER. Unknown type names pass through as custom types.

A comparison, `AND`, `OR`, `NOT`, `IS` tests, `IN`, `BETWEEN`, `LIKE`, `EXISTS`,
the regular expression operators, `= ANY(array)`, `@>`, `<@`, `&&` and the
functions that answer a condition are boolean: they print as `t` or `f` and are
described with OID 16. The type is kept through a subquery in FROM, a CTE, a
view, a VALUES list, a UNION, a CASE and a coalesce. Still different from
PostgreSQL: a boolean cast to text is `1` or `0`, a boolean in an array prints
as `1` or `0`, arithmetic on a boolean fails with 42601 and not 42883, CREATE
TABLE AS of a condition fails with 0A000, and `pg_is_in_recovery()` is true
where a primary gives false. A subquery in FROM and a VALUES list in FROM keep only this type
and the types of table columns: any other column, such as `1` or `now()`, is
described with a wrong type, such as bytea, and a date or time value from it
prints as it is stored.

time, timetz and interval, and arrays of every date and time type
(`date[]`, `timestamp[]` and `timestamptz[]` included), are rejected as not
supported (0A000) in column types, casts, typed literals and domains. So are
`CURRENT_TIME`, `LOCALTIME` and the built-in functions that
return date, time, timetz or interval, such as `age`, `make_date`, `to_date`
and `date()`.

The one use of interval that works is a timestamp or timestamptz plus or
minus an interval literal, such as `now() - interval '7 days'` or
`created_at + '1 hour'::interval`. The left side must be a timestamp or
timestamptz: a column, `now()` or another function that returns one, a cast or
a typed literal. The result has the type of the left side. The literal is a
list of integers, each optionally signed and followed by one of the units
microsecond, millisecond, second, minute, hour, day, week, month or year
(singular or plural, any case), such as `'1 day 2 hours'` or `'-1 month'`. The
months are added first, and a day past the end of the month becomes its last
day (`timestamp '2026-03-31' - interval '1 month'` is 2026-02-28), then the
days, then the time, as in PostgreSQL. A result before year 1 or after 9999
fails with 0A000. Other interval literals (`'1:30'`, `'1 day ago'`, `'1.5
days'`, `interval '1' day`), `interval + timestamp`, a date or a value of
unknown type on the left, a computed or parameter interval, and an interval
in a generated column or an index expression fail with 0A000.

`now()`, `transaction_timestamp()` and `CURRENT_TIMESTAMP` are the time the
transaction started, `LOCALTIMESTAMP` is that time as a timestamp in UTC, and
`CURRENT_DATE` is its date;
`statement_timestamp()` is the time the client's query arrived, shared by all
statements of one query string;
`clock_timestamp()` reads the clock at each call. All of them have microsecond
precision.

Comparing values of two different custom types fails when either type changes
the values it stores (a STRUCT or UNION, a type with ENCODE or a DECODE other
than `value`), unless the two already have one order in common, as `numeric`
and `bigint` do. A domain compares as the type it is declared on, so two
domains over one type compare as that type. So a `date`, a `timestamp` and a
`timestamptz` fail with one another, and so does a `timestamptz` with a
`varchar(n)`. PostgreSQL refuses the `timestamptz` with `varchar(n)` pair too,
with 42883.
The error is `cannot compare type x with type y` with SQLSTATE 0A000. Turso
refuses comparisons that PostgreSQL may run and does not know which ones, so
the code differs from PostgreSQL's 42883 where PostgreSQL also refuses. It covers `=`, `<`,
`IS DISTINCT FROM`, BETWEEN, IN, `IN (SELECT ...)` and `IN (VALUES ...)`,
`CASE x WHEN`, `NULLIF`, `min` and `max`, and joins, with or without an index.
Not refused yet, and compared as stored: a join with a VALUES list in FROM or
in a CTE, such as `WITH v(x) AS (VALUES (ts)) SELECT * FROM t, v WHERE d = x`.
The same two types in
one result column fail the same way: in UNION, UNION ALL, INTERSECT and EXCEPT,
also with a VALUES part such as `SELECT d FROM t UNION ALL VALUES (CAST('2024-01-05' AS timestamp))`, a
multi-row VALUES that is not the row source of an INSERT, a recursive CTE with
UNION or UNION ALL, the results of CASE, `coalesce`, `ifnull` and `iif`, and `lag` and `lead`
with a default of another type. A string literal compared with a custom type is read as that type.
Two arrays compare when their element types compare, so `date[]` with
`timestamp[]` fails. So do the array functions and operators that compare
an array's elements with other values or put values into one array, such as
`array_position(ats, d)`, `array_append(ats, d)`, `array_set_element(ats, 1, d)`,
`ats || d`, `ARRAY[d, ts]`, `@>`, `<@` and `&&` for a `timestamp[]` and a
`date`. A function that turns its arguments into text, such as `concat(ats, d)`,
compares nothing and is accepted. PostgreSQL converts or refuses these. Two types that only check a value, such as `varchar(n)` and
`smallint`, compare as they did before this check, as stored. PostgreSQL refuses
that pair, so Turso accepts a comparison that PostgreSQL rejects.

| Feature | Status | Notes |
|---------|--------|-------|
| Arrays of compound types | ❌ Not supported | |
| Array support | ✅ Supported | `type[]` columns, ARRAY[...] literals, subscripts `a[i]`, slices `a[i:j]`, `@>`, `<@`, `&&`, array_length, array_append, array_agg; backed by native Turso arrays |
| ENUM data type | ✅ Supported | CREATE TYPE ... AS ENUM; values validated on write; DROP TYPE [IF EXISTS] |
| GUID/UUID data type | ✅ Supported | uuid columns, gen_random_uuid(), input validation, text casts |
| macaddr8 data type | ✅ Supported | Along with inet, cidr, macaddr (round-trip tested) |
| Multiranges | ❌ Not supported | |
| NULLs in Array | ✅ Supported | `ARRAY[1, NULL, 3]` round-trips |
| Phrase search | ❌ Not supported | tsvector/tsquery degrade to TEXT; no full-text search |
| Range types | ❌ Not supported | |
| smallserial type | ✅ Supported | serial2/serial4/serial8 aliases too; serial implies NOT NULL + implicit sequence, not PRIMARY KEY |
| Type modifier support | 🟡 Partial | varchar(n) length enforced ("value too long for varchar"); numeric(p,s) scale applied; modifiers on other types and in casts (`x::varchar(3)`) rejected |
| UUIDv7 | ❌ Not supported | |
| XML data type | ❌ Not supported | xml columns degrade to TEXT; no XML functions |

## Indexing & Constraints

| Feature | Status | Notes |
|---------|--------|-------|
| Block-range (BRIN) indexes | ❌ Not supported | |
| B-tree bottom-up index deletion | ❌ Not supported | |
| B-tree deduplication | ❌ Not supported | |
| Concurrent GiST indexes | ❌ Not supported | |
| Covering Indexes for B-trees (INCLUDE) | ❌ Not supported | INCLUDE rejected |
| Covering indexes for GiST (INCLUDE) | ❌ Not supported | |
| Deferrable unique constraints | ❌ Not supported | DEFERRABLE / INITIALLY DEFERRED rejected on every constraint |
| Exclusion constraints | ❌ Not supported | |
| GIN (Generalized Inverted Index) indexes | ❌ Not supported | `USING <method>` other than btree rejected |
| GIN indexes partial match | ❌ Not supported | |
| GIN index performance and size improvements | ❌ Not supported | |
| GiST (Generalized Search Tree) indexes | ❌ Not supported | |
| Indexes on expressions | ✅ Supported | Partial (WHERE) indexes also supported |
| Index-only scans | ❌ Not supported | |
| Index-only scans on GiST | ❌ Not supported | |
| Index support for IS NULL | ❌ Not supported | |
| In-memory Bitmap Indexes | ❌ Not supported | |
| K-nearest neighbor GiST support | ❌ Not supported | |
| K-nearest neighbor SP-GiST support | ❌ Not supported | |
| Non-blocking CREATE INDEX | ❌ Not supported | CONCURRENTLY accepted; the index is built the ordinary way |
| Parallel B-tree index scans | ❌ Not supported | |
| Parallelized CREATE INDEX for BRIN indexes | ❌ Not supported | |
| Parallelized CREATE INDEX for B-tree indexes | ❌ Not supported | |
| Parallelized CREATE INDEX for GIN indexes | ❌ Not supported | |
| Skip scan on multicolumn B-tree indexes | ❌ Not supported | |
| Space-Partitioned GiST (SP-GiST) indexes | ❌ Not supported | |
| SP-GiST indexes for range types | ❌ Not supported | |
| UNIQUE NULLS NOT DISTINCT | ❌ Not supported | Rejected |
| WAL support for hash indexes | ❌ Not supported | |

## SQL

| Feature | Status | Notes |
|---------|--------|-------|
| ANY_VALUE aggregate | ❌ Not supported | |
| DISTINCT ON | 🟡 Partial | Rejected: COLLATE in a DISTINCT ON expression or in its ORDER BY; a volatile function such as `nextval()` or `random()` in the select list; in a query with aggregates, an expression that reads a column outside GROUP BY (PostgreSQL accepts a column that depends on a grouped primary key); an aggregate that appears only in a DISTINCT ON expression; an expression computed on an aggregate when there is no GROUP BY |
| FETCH FIRST .. WITH TIES | ❌ Not supported | `FETCH FIRST n ROWS ONLY` works (lowered to LIMIT); WITH TIES rejected |
| GROUPING SETS, CUBE and ROLLUP support | ❌ Not supported | Translation error |
| INSERT/UPDATE/DELETE RETURNING | ✅ Supported | Including `RETURNING *`; on UPDATE ... FROM, `RETURNING *` is rejected (it would omit the FROM tables' columns) |
| LATERAL clause | 🟡 Partial | LATERAL subqueries work with a comma, [CROSS] JOIN, and LEFT JOIN. Column alias lists such as `AS s(x)` work on subqueries, unnest(), generate_series(), current_schema(), current_database() and version(); outer columns in LIMIT/OFFSET are not supported; LATERAL tvf() is not supported     |
| MERGE | ❌ Not supported | |
| MERGE ... RETURNING | ❌ Not supported | |
| Multirow VALUES | ✅ Supported | In INSERT and as standalone VALUES lists |
| ORDER BY / LIMIT on a standalone VALUES list | ❌ Not supported | Rejected: "ORDER BY clause is not allowed with VALUES clause" |
| Non-decimal integer literals | ✅ Supported | `0x`, `0o`, `0b` |
| ORDER BY NULLS FIRST/LAST | ✅ Supported | Honored in SELECT, window, and compound SELECT ORDER BY; without it, NULLs sort last for ASC and first for DESC, as in PostgreSQL; rejected (matching SQLite) in CREATE INDEX |
| ORDER BY of a SELECT DISTINCT | 🟡 Partial | Every ORDER BY term must be in the select list, as in PostgreSQL. Rejected: COLLATE in the select list or in the ORDER BY of such a query |
| ORDER BY a signed integer constant | ❌ Not supported | `ORDER BY +1` and `ORDER BY -(+1)`, also as a DISTINCT ON expression, are rejected. PostgreSQL sorts by the constant, where the engine would read a position in the select list |
| ORDER BY USING operator | 🟡 Partial | Built-in `<` and `>` (including `pg_catalog` qualification) are translated to ascending and descending order; other operators are rejected |
| range_agg range type aggregation function | ❌ Not supported | |
| Recursive queries | 🟡 Partial | WITH RECURSIVE works with SQLite semantics (row-at-a-time recursive term, so e.g. DISTINCT in the recursive term over a multi-row anchor can differ from PG); SEARCH/CYCLE clauses are rejected; a recursive query whose top-level set operation is not UNION or UNION ALL raises 42P19, and ORDER BY, LIMIT, OFFSET and FOR UPDATE/SHARE on it raise 0A000, as in PostgreSQL |
| regexp_count, regexp_instr, regexp_like | ❌ Not supported | Regex *operators* (`~`, `~*`, SIMILAR TO) work |
| Return OLD and NEW values from modified rows | ❌ Not supported | |
| Row-wise comparison | ❌ Not supported | Row constructors `(a,b) < (c,d)` fail to translate |
| SELECT ... FOR UPDATE/SHARE | ❌ Not supported | Rejected |
| SELECT FOR NO KEY UPDATE/SELECT FOR KEY SHARE lock modes | ❌ Not supported | Rejected |
| SQL standard interval handling | ❌ Not supported | interval is rejected (0A000), except timestamp ± interval literal |
| SYSTEM_USER | ❌ Not supported | current_user, current_role and session_user are supported |
| TABLE statement | ✅ Supported | |
| Underscores (_) for thousands separators | ✅ Supported | |
| unnest/array_agg | 🟡 Partial | array_agg works; unnest works in FROM with up to 100 arrays, and not in the select list. `unnest(a, b)` needs a column alias list that gives its columns different names, because two columns named `unnest` are rejected |
| Upsert (INSERT ... ON CONFLICT DO ...) | ✅ Supported | DO NOTHING and DO UPDATE SET ... (with EXCLUDED and conflict targets) |
| Window functions | 🟡 Partial | Aggregate window functions (COUNT/SUM/AVG/MIN/MAX OVER), row_number, PARTITION BY/ORDER BY, frame clauses, and named WINDOW clauses work; rank, dense_rank, lag, lead, etc. are not implemented |
| WITHIN GROUP clause | 🟡 Partial | Supports mode(), percentile_cont(), and percentile_disc() with one ORDER BY expression; DESC is not supported; NULLS FIRST/LAST is accepted and has no effect because these functions skip NULLs |
| WITH ORDINALITY clause | 🟡 Partial | Works on generate_series(), unnest(), json_each(), current_schema(), current_database(), version(), and on json_to_record / json_to_recordset and their jsonb_ variants in `ROWS FROM (f(...) AS (...))`; rejected on other functions and on ROWS FROM with several functions. Two columns with the same name, as in `generate_series(1, 2) WITH ORDINALITY AS ordinality`, are rejected. The ordinality column has type `integer`; in PostgreSQL it is `bigint` |
| WITH queries (Common Table Expressions) | ✅ Supported | Including WITH RECURSIVE; MATERIALIZED hints accepted |
| Writable WITH queries (Common Table Expressions) | ❌ Not supported | "CTE query is not a SELECT statement" |

## Data Definition Language (DDL)

| Feature | Status | Notes |
|---------|--------|-------|
| ALTER object IF EXISTS | 🟡 Partial | ALTER TABLE IF EXISTS ... OWNER TO skips a missing table; for the other subcommands it works when the table exists, but a missing table still errors instead of being skipped |
| ALTER TABLE ... ADD UNIQUE/PRIMARY KEY USING INDEX | ❌ Not supported | ADD CONSTRAINT is rejected |
| ALTER TABLE ... SET ACCESS METHOD | ❌ Not supported | |
| ALTER TABLE ... SET LOGGED / UNLOGGED | ❌ Not supported | |
| Changing column types (ALTER TABLE .. ALTER COLUMN TYPE) | ❌ Not supported | Translates but breaks at execution ("no such column" on subsequent use) |
| CREATE ACCESS METHOD | ❌ Not supported | |
| CREATE TABLE ... (LIKE) with foreign tables, views and composite types | ❌ Not supported | LIKE rejected |
| DROP object IF EXISTS | ✅ Supported | TABLE, INDEX, VIEW, MATERIALIZED VIEW, TYPE, DOMAIN, SEQUENCE, SCHEMA |
| ON COMMIT clause for CREATE TEMPORARY TABLE | ❌ Not supported | Rejected, like TEMP itself |
| REINDEX CONCURRENTLY | ❌ Not supported | REINDEX not supported at all |
| Stored generated columns | 🟡 Partial | GENERATED ALWAYS AS (...) STORED, including ADD COLUMN; the type of the expression is not checked against the column type at CREATE, so a mismatch fails at INSERT instead; immutability is checked against a list of known non-immutable functions, and date/time casts and functions are checked using the declared types of the columns they read; `+`, `-`, `||` and casts to text or date/time types need the type of their operands, which is known for a literal, a cast, a column of the table being created, and the result of an operator with a known type rule, so any other operand (a function result, a `CASE` or `COALESCE`, or any column in `ADD COLUMN`) is rejected as not supported (0A000); a generated column of an array type, or one whose expression reads an array column, is rejected with 42601; numeric, varchar(n), smallint, bigint, uuid, enum and other scalar types are allowed and check or convert their values; `extract` and `date_part` are not implemented; an expression that uses `->` or `->>` fails with `0A000`; `@>` and `<@` compare arrays only, so they return NULL for jsonb values |
| Temporal constraints | ❌ Not supported | |
| Temporary tables (CREATE TEMP TABLE) | ❌ Not supported | Rejected (UNLOGGED is accepted and creates an ordinary table) |
| Typed tables | ❌ Not supported | |
| Virtual generated columns | ❌ Not supported | GENERATED ... AS rejected |

## Performance

| Feature | Status | Notes |
|---------|--------|-------|
| Abbreviated keys | ❌ Not supported | |
| Asynchronous commit | ❌ Not supported | |
| Asynchronous I/O (AIO) | ❌ Not supported | |
| Automatic plan invalidation | ❌ Not supported | |
| Background checkpointer | ❌ Not supported | |
| Background writer | ❌ Not supported | |
| Base backup throttling | ❌ Not supported | |
| CREATE STATISTICS - most-common values (MCV) statistics | ❌ Not supported | |
| CREATE STATISTICS - multicolumn | ❌ Not supported | |
| CREATE STATISTICS - "OR" and "IN/ANY" statistics | ❌ Not supported | |
| Cross datatype hashing support | ❌ Not supported | |
| Distributed checkpointing | ❌ Not supported | |
| Foreign keys marked as NOT VALID | ❌ Not supported | NOT VALID rejected on every constraint; so are MATCH FULL/PARTIAL and ON DELETE SET NULL/DEFAULT (columns) |
| Frozen page map | ❌ Not supported | |
| Full text search | ❌ Not supported | tsvector/tsquery degrade to TEXT |
| Hash aggregation can use disk | ❌ Not supported | |
| Hashing support for DISTINCT/UNION/INTERSECT/EXCEPT | ❌ Not supported | |
| Hashing support for FULL OUTER JOIN, LEFT OUTER JOIN and RIGHT OUTER JOIN | ❌ Not supported | |
| Heap Only Tuples (HOT) | ❌ Not supported | |
| Improved performance for sorts exceeding working memory | ❌ Not supported | |
| Improved window function performance | ❌ Not supported | |
| Incremental sort | ❌ Not supported | |
| Incremental sort for SELECT DISTINCT | ❌ Not supported | |
| Incremental sort for window functions | ❌ Not supported | |
| Inlined WITH queries (Common Table Expressions) | ❌ Not supported | |
| Inlining of SQL functions | ❌ Not supported | |
| Just-in-Time (JIT) compilation for expression evaluation and tuple deforming | ❌ Not supported | |
| Load balancing for libpq / psql | ❌ Not supported | |
| LZ4 compression for TOAST tables | ❌ Not supported | |
| Multi-core scalability for read-only workloads | ❌ Not supported | |
| Multiple temporary tablespaces | ❌ Not supported | |
| Outer join reordering | ❌ Not supported | |
| Parallel bitmap heap scans | ❌ Not supported | |
| Parallel FULL and RIGHT joins | ❌ Not supported | |
| Parallel full table scans (sequential scans) | ❌ Not supported | |
| Parallel hash joins | ❌ Not supported | |
| Parallel JOIN, aggregate | ❌ Not supported | |
| Parallel merge joins | ❌ Not supported | |
| Parallel query | ❌ Not supported | |
| Parallel "SELECT DISTINCT" | ❌ Not supported | |
| Partial sort capability (top-n sorting) | ❌ Not supported | |
| Query pipelining | ❌ Not supported | |
| Reduced lock levels for ALTER TABLE commands | ❌ Not supported | |
| SELECT ... FOR UPDATE/SHARE NOWAIT | ❌ Not supported | Rejected |
| Set costs specific to TABLESPACEs | ❌ Not supported | |
| Shared row level locking | ❌ Not supported | |
| SIMD support for ARM | ❌ Not supported | |
| SIMD support for x86 | ❌ Not supported | |
| SKIP LOCKED clause | ❌ Not supported | Rejected |
| Synchronized sequential scanning | ❌ Not supported | |
| TABLESAMPLE clause | ❌ Not supported | Translation error |
| Tablespaces | ❌ Not supported | |
| Unlogged tables | ❌ Not supported | |
| WAL buffer auto-tuning | ❌ Not supported | |

## JSON

| Feature | Status | Notes |
|---------|--------|-------|
| Improved set of JSON functions and operators | ❌ Not supported | |
| JSONB data type | 🟡 Partial | jsonb columns supported (Turso custom type) |
| JSONB operators `->` and `->>` | 🟡 Partial | `->` and `->>` work: a text key is the name of a field, not a JSON path, and of two fields with the same name the last one counts. `->>` returns text, and it writes a number as jsonb does (`1e2` as `100`), where PostgreSQL's json keeps the text of the input. A number with more digits than numeric allows makes `->>` fail with `22003`, where PostgreSQL fails when it makes the jsonb value. An object or an array comes back as it is stored, where PostgreSQL uses the jsonb output format (`{"h": 10, "w": 20}`). `->` returns text, not jsonb, with a number as it is stored. The types of the operands are checked for each value when the query runs: a text left operand and a bigint key work, and a table without rows gives no error, where PostgreSQL reports `42883`. An integer key on a scalar returns NULL, as PostgreSQL does for json. For jsonb, PostgreSQL reads a scalar as an array of one element: `'"x"'::jsonb ->> 0` is `x`. A `CREATE INDEX` or a stored generated column that uses `->` or `->>` fails with `0A000` |
| JSONB operators `@>` and `<@` | 🟡 Partial | `@>` and `<@` compare jsonb values as PostgreSQL does when neither operand is an array. A json or text value is compared as jsonb too, where PostgreSQL has no `@>` for it and fails with `42883`, and two string literals are compared as jsonb, where PostgreSQL fails with `42725`. Two values that are arrays and not JSON documents are compared as arrays: a column of a VALUES list or of a table that CREATE TABLE AS made has no array type, and two string literals such as `'{a}'` are not JSON. Thus `'{}'::jsonb @> '{a}'` is false, because `{}` is also an empty array, where PostgreSQL fails with `22P02`. An index expression and a stored generated column use the array function for `@>` and `<@`, which returns NULL for jsonb values |
| JSONB operators `#>` and `#>>` | 🟡 Partial | `#>` and `#>>` find the value at a path of field names and array indexes, and return it as `->` and `->>` do. Thus `#>` returns text, not jsonb. A text value on the right is read as a text array literal, where PostgreSQL fails with `42883`. An unquoted element with a space inside, such as `{- 1}`, fails with `22P02`, and an array literal with two dimensions is read as an array with one dimension. Two string literals around `#>` or `#>>` work, where PostgreSQL fails with `42725`. A `CREATE INDEX` or a stored generated column that uses `#>` or `#>>` fails with `0A000` |
| JSONB operators `?`, `?\|` and `?&` | 🟡 Partial | `?`, `?\|` and `?&` test the keys at the top level. A json or text value is read as jsonb for these operators too, where PostgreSQL has them only for jsonb and fails with `42883`. The right operand of `?` is read as text, so `j ? 1` tests the key `1`, where PostgreSQL fails with `42883`. A text value on the right of `?\|` and `?&` is read as a text array literal, where PostgreSQL fails with `42883`. An unquoted element with a space inside, such as `{- 1}`, fails with `22P02`, and an array literal with two dimensions is read as an array with one dimension. A `CREATE INDEX` or a stored generated column that uses `?`, `?\|` or `?&` fails with `0A000` |
| JSONB operator `@@` | ❌ Not supported | It fails to translate |
| JSONB-modifying operators and functions | ❌ Not supported | |
| JSONB subscripting | ❌ Not supported | |
| JSON data type | 🟡 Partial | Same as jsonb (see above) |
| array_to_json | ✅ Supported | |
| json_strip_nulls | 🟡 Partial | Input nested deeper than 1000 levels errors with `stack depth limit exceeded`, where PostgreSQL's limit depends on `max_stack_depth`; a text or jsonb argument is accepted, where PostgreSQL only takes json; `jsonb_strip_nulls` is not supported |
| json_each | 🟡 Partial | Returns `key` (text) and `value` (json), accepts WITH ORDINALITY and column aliases. The wire protocol describes `value` as `text` (OID 25), where PostgreSQL describes it as `json` (OID 114). Input nested deeper than 1000 levels errors with `stack depth limit exceeded`, where PostgreSQL's limit depends on `max_stack_depth`; a text or jsonb argument is accepted, where PostgreSQL only takes json. `jsonb_each`, `json_each_text`, `jsonb_each_text` and the other functions that return members are not supported |
| Table functions of SQLite (json_tree, completion, ...) | ❌ Not supported | Refused with `0A000` as a function in FROM; PostgreSQL has no such function and reports `42883`. The `pragma_*` functions work and check the privilege of the PRAGMA |
| json_to_record / json_to_recordset (and jsonb_ variants) | 🟡 Partial | Work with a column definition list, after the alias or inside ROWS FROM (); WITH ORDINALITY works on the ROWS FROM form; ROWS FROM with several functions is not supported; error messages for malformed input differ from PostgreSQL; a json/jsonb column drops whitespace (`{"y": 2}` comes back as `{"y":2}`); an array column (`int[]`) accepts only an array literal string such as `"{1,2}"`, not a JSON array |
| SQL/JSON constructors | ❌ Not supported | |
| SQL/JSON: datetime() | ❌ Not supported | |
| SQL/JSON IS JSON | ❌ Not supported | |
| SQL/JSON JSON_TABLE | ❌ Not supported | |
| SQL/JSON path expressions | ❌ Not supported | |
| SQL/JSON query functions | ❌ Not supported | |

## Partitioning & Inheritance

| Feature | Status | Notes |
|---------|--------|-------|
| Accelerated partition pruning | ❌ Not supported | |
| Declarative table partitioning | ❌ Not supported | PARTITION BY / PARTITION OF rejected |
| Default partition | ❌ Not supported | |
| Foreign key references for partitioned tables | ❌ Not supported | |
| Foreign table inheritance | ❌ Not supported | |
| Partitioning by a hash key | ❌ Not supported | |
| Partition pruning during query execution | ❌ Not supported | |
| Support for PRIMARY KEY, FOREIGN KEY, indexes, and triggers on partitioned tables | ❌ Not supported | |
| Table inheritance (INHERITS) | ❌ Not supported | INHERITS rejected |
| Table partitioning | ❌ Not supported | PARTITION BY / PARTITION OF rejected |
| UPDATE on a partition key | ❌ Not supported | |

## Views & Materialized Views

| Feature | Status | Notes |
|---------|--------|-------|
| Materialized views | ✅ Supported | Incrementally maintained (live, DBSP-based) — always fresh, unlike PostgreSQL snapshots; REFRESH MATERIALIZED VIEW is accepted as a no-op; WITH NO DATA (on CREATE or REFRESH) is rejected |
| Materialized views with concurrent refresh | 🟡 Partial | Moot: views are always fresh; REFRESH (CONCURRENTLY) is a no-op |
| SECURITY INVOKER views | ❌ Not supported | |
| Temporary VIEWs | ❌ Not supported | Rejected |
| Updatable views | ❌ Not supported | |
| WITH CHECK clause | ❌ Not supported | |

## Replication

Replication is not supported.

## Backup, Restore, & Data Integrity

Backup and restore is not supported.

## Upgrade

Upgrade is not supported.

## Data Import & Export

| Feature | Status | Notes |
|---------|--------|-------|
| COPY table FROM 'file' (text format) | ✅ Supported | DELIMITER (one character), NULL string, HEADER (not HEADER MATCH), column lists, backslash escapes, `\.` end-of-data marker; atomic (rolls back on malformed rows); other options rejected |
| COPY table TO STDOUT | ✅ Supported | Simple query protocol, text format; column lists, search_path lookup, COPY text escaping; excludes generated columns by default and rejects them in explicit lists; database reads use blocking helpers, rows are sent one at a time |
| COPY FROM STDIN | ❌ Not supported | Rejected with an error |
| COPY FROM ... WHERE | ❌ Not supported | Rejected |
| COPY ... ON_ERROR | ❌ Not supported | Rejected |
| COPY with arbitrary SELECT | ❌ Not supported | COPY TO requires a plain table |
| CSV support for COPY | ❌ Not supported | `FORMAT csv` and `FORMAT binary` rejected with an error |

## Configuration Management

| Feature | Status | Notes |
|---------|--------|-------|
| SET / SHOW configuration parameters | 🟡 Partial | Only search_path and custom parameters; other parameters are rejected, except SET to a value matching existing behaviour (see SET / SHOW / RESET above) |
| Custom configuration parameters (`app.user_id`) | 🟡 Partial | In server mode, all clients share the values |
| DISCARD | 🟡 Partial | Only `DISCARD ALL`, which resets every parameter and forgets `currval` values. In server mode it does not drop the client's protocol-level prepared statements, and it resets the parameters of all clients |
| ALTER SYSTEM | ❌ Not supported | |
| Fractional input for "integer" values | ❌ Not supported | |
| Include directives for pg_hba.conf and pg_ident.conf | ❌ Not supported | |
| Per user/database server configuration settings | ❌ Not supported | |
| pg_config system view | ❌ Not supported | |
| Regular expression matching in pg_hba.conf and pg_ident.conf | ❌ Not supported | |

## Security

| Feature | Status | Notes |
|---------|--------|-------|
| Authentication (any method) | ❌ Not supported | The wire server trusts every connection; no password, MD5, SCRAM, or certificate checks |
| Channel binding for SCRAM authentication | ❌ Not supported | |
| Client can require SCRAM channel binding | ❌ Not supported | |
| Client-specified requirements for authentication | ❌ Not supported | |
| Column level permissions | ❌ Not supported | |
| Default permissions | ❌ Not supported | |
| Direct TLS negotiation ("sslnegotiation") | ❌ Not supported | |
| FIPS mode validation | ❌ Not supported | |
| GRANT/REVOKE ON ALL TABLES/SEQUENCES/FUNCTIONS | 🟡 Partial | `GRANT` and `REVOKE ... ON ALL TABLES IN SCHEMA` and `... ON ALL SEQUENCES IN SCHEMA` change the privileges in each relation's `pg_class.relacl`; functions are not supported |
| GSSAPI client and server-side encryption | ❌ Not supported | |
| GSSAPI support | ❌ Not supported | |
| Kerberos credential delegation | ❌ Not supported | |
| krb5 authentication (without gssapi) | ❌ Not supported | |
| Large object access controls | ❌ Not supported | |
| LDAP server discovery | ❌ Not supported | |
| Multifactor authentication via valid client SSL/TLS certificate | ❌ Not supported | |
| Native LDAP authentication | ❌ Not supported | |
| Native RADIUS authentication | ❌ Not supported | |
| OAuth authentication / authorization | ❌ Not supported | |
| Per user/database connection limits | ❌ Not supported | |
| Predefined roles | ❌ Not supported | |
| Privileges for setting configuration parameters | ❌ Not supported | |
| ROLES | 🟡 Partial | `CREATE ROLE name` with the options `BYPASSRLS`, `NOBYPASSRLS`, `INHERIT`, `NOINHERIT` and `NOLOGIN`, `SET [LOCAL] ROLE`, `RESET ROLE`, `SHOW role`; `GRANT role TO role` and `REVOKE role FROM role` by a superuser, shown in `pg_auth_members`, without `WITH ADMIN`, `WITH INHERIT`, `WITH SET`, `GRANTED BY` or `CASCADE`; a role may `SET ROLE` to the roles it is a member of, and with `INHERIT` has their privileges, acts as the owner of what they own, and is subject to their row-level security policies; roles that are not superusers may read and write the tables they own or were granted `SELECT`, `INSERT`, `UPDATE` and `DELETE` on, with SELECT also needed when a write reads the table's columns, and call `nextval` with `USAGE` or `UPDATE` and `setval` with `UPDATE` on the sequence, but not change the schema they do not own; `GRANT` and `REVOKE` of privileges on whole tables and sequences, `ALL` included, change `pg_class.relacl`; `TRUNCATE` needs the `TRUNCATE` privilege, and the `REFERENCES`, `TRIGGER` and `MAINTAIN` privileges let a role do nothing yet; `pg_roles` lists the built-in `postgres` superuser, `pg_database_owner`, and created roles; `DROP ROLE` and `DROP ROLE IF EXISTS` by a superuser, which fail while the role owns a relation, has privileges on one or is a policy's role, and drop its memberships; roles belong to one database file, not to the whole server as in PostgreSQL |
| Row-level security | 🟡 Partial | `ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY` works and is shown in `pg_class.relrowsecurity`; `CREATE POLICY` and `DROP POLICY` work, with permissive and restrictive `USING` policies limiting what roles read; `UPDATE` and `DELETE` change only the rows the policies of the command allow, with the `SELECT` policies too when the statement reads columns, and new rows of `INSERT` and `UPDATE` must pass `WITH CHECK`, and the `SELECT` policies with `RETURNING`, or the statement fails with 42501; `TRUNCATE` is not subject to policies, as in PostgreSQL; `INSERT ... ON CONFLICT DO UPDATE` checks the conflicting row against the `UPDATE` and `SELECT` policies and fails if it does not pass; a new row is checked after its `NOT NULL` constraints instead of before; `pg_policy` lists the policies, with conditions printed as PostgreSQL's `pg_get_expr` prints them for common forms and as `CREATE POLICY` wrote them otherwise, and so does `pg_policies`; policies cannot use subqueries with `FROM`, and the columns they use cannot be renamed or dropped; `FORCE ROW LEVEL SECURITY` is not supported; views apply it as the view's owner, except that a superuser reading a view sees every row |
| SCRAM-SHA-256 authentication | ❌ Not supported | |
| Search+bind mode operation for LDAP authentication | ❌ Not supported | |
| security_barrier option on views | ❌ Not supported | |
| Security Service Provider Interface (SSPI) | ❌ Not supported | |
| SHA-2 encryption for password hashing | ❌ Not supported | |
| SSL certificate validation in libpq | ❌ Not supported | |
| SSL client certificate authentication | ❌ Not supported | |
| SSPI authentication via GSSAPI | ❌ Not supported | |
| Support using the client's OS trusted CA | ❌ Not supported | |
| TLS v1.3 cipher suite allowlisting | ❌ Not supported | |

## Transactions and Visibility

| Feature | Status | Notes |
|---------|--------|-------|
| Cursors | ❌ Not supported | DECLARE/FETCH/MOVE not translated |
| Savepoints | ✅ Supported | SAVEPOINT, RELEASE, ROLLBACK TO |
| Serializable Snapshot Isolation | ❌ Not supported | BEGIN ISOLATION LEVEL SERIALIZABLE is rejected |
| Two-phase commit | ❌ Not supported | PREPARE TRANSACTION rejected |
| Updatable cursors | ❌ Not supported | |

## VACUUM and Maintenance

| Feature | Status | Notes |
|---------|--------|-------|
| VACUUM / ANALYZE statements | ❌ Not supported | Rejected with an error |
| Inserted data can trigger autovacuum | ❌ Not supported | |
| Integrated autovacuum daemon | ❌ Not supported | |
| Page freezing optimizations | ❌ Not supported | |
| Parallelized VACUUM for indexes | ❌ Not supported | |
| Parallel vacuumdb jobs | ❌ Not supported | |
| Radix tree memory structure for vacuum | ❌ Not supported | |
| Vacuum "emergency mode" | ❌ Not supported | |
| Visibility map for vacuuming | ❌ Not supported | |

## Foreign Data Wrappers

| Feature | Status | Notes |
|---------|--------|-------|
| Certificate authentication with postgres_fdw | ❌ Not supported | |
| CREATE FOREIGN TABLE ... LIKE | ❌ Not supported | |
| Foreign data wrapper query parallelism | ❌ Not supported | |
| Foreign data wrappers | ❌ Not supported | |
| Foreign tables | ❌ Not supported | |
| IMPORT FOREIGN SCHEMA | ❌ Not supported | |
| Import foreign table partitions | ❌ Not supported | |
| Parallel query execution on remote databases | ❌ Not supported | |
| postgres_fdw parallel commit | ❌ Not supported | |
| postgres_fdw pushdown | ❌ Not supported | |
| postgres_fdw SCRAM authentication passthrough | ❌ Not supported | |
| PostgreSQL Foreign Data Wrapper | ❌ Not supported | |
| Writable Foreign Data Wrappers | ❌ Not supported | |

## Custom Functions, Stored Procedures, & Triggers

| Feature | Status | Notes |
|---------|--------|-------|
| ALTER TABLE ENABLE/DISABLE TRIGGER | ❌ Not supported | |
| ALTER TABLE / ENABLE REPLICA TRIGGER/RULE | ❌ Not supported | |
| BEGIN ATOMIC function bodies | ❌ Not supported | |
| CALL syntax for executing procedures | ❌ Not supported | |
| CREATE FUNCTION | ❌ Not supported | Rejected with an error. A function that PostgreSQL rejects gets PostgreSQL's error; otherwise the error names the first unsupported feature, such as the language, RETURNS SETOF or an option |
| Column level triggers | ❌ Not supported | |
| CREATE PROCEDURE syntax for SQL stored procedures | ❌ Not supported | |
| Event triggers | ❌ Not supported | |
| FILTER clause for aggregate functions | ✅ Supported | |
| ORDER BY support within aggregates | ❌ Not supported | Silently dropped |
| Per function GUC settings | ❌ Not supported | |
| Per function statistics | ❌ Not supported | |
| RETURN QUERY EXECUTE | ❌ Not supported | |
| RETURNS TABLE | ❌ Not supported | |
| Statement level triggers | ❌ Not supported | CREATE TRIGGER not translated |
| Statement level TRUNCATE triggers | ❌ Not supported | |
| Triggers on views | ❌ Not supported | |
| Variadic functions | ❌ Not supported | VARIADIC arguments rejected |
| WHEN clause for CREATE TRIGGER | ❌ Not supported | |

## Procedural Languages

| Feature | Status | Notes |
|---------|--------|-------|
| Procedural languages (PL/pgSQL, PL/Python, ...) | ❌ Not supported | CREATE FUNCTION and DO are rejected with an error |
| CASE in pl/pgsql | ❌ Not supported | |
| CONTINUE statement for PL/pgSQL | ❌ Not supported | |
| CREATE TRANSFORM | ❌ Not supported | |
| DO statement for pl/perl | ❌ Not supported | |
| DO statement for pl/pgsql | ❌ Not supported | |
| EXCEPTION support in PL/pgSQL | ❌ Not supported | |
| EXECUTE USING in PL/pgSQL | ❌ Not supported | |
| FOREACH IN ARRAY in pl/pgsql | ❌ Not supported | |
| IN/OUT/INOUT parameters for pl/pgsql and PL/SQL | ❌ Not supported | |
| Named parameters | ❌ Not supported | Named arguments in calls, such as `jsonb_path_exists(j, p, silent => true)`, are rejected |
| Non-superuser language creation | ❌ Not supported | |
| pl/pgsql installed by default | ❌ Not supported | |
| Polymorphic functions | ❌ Not supported | |
| Python 3 support for pl/python | ❌ Not supported | |
| Qualified function parameters | ❌ Not supported | |
| Query parallelism for RETURN QUERY | ❌ Not supported | |
| RETURN QUERY in pl/pgsql | ❌ Not supported | |
| ROWS and COST specification for functions | ❌ Not supported | |
| Scrollable and updatable cursor support for pl/pgsql | ❌ Not supported | |
| SQLERRM/SQLSTATE for pl/pgsql | ❌ Not supported | |
| Unicode object support in PL/python | ❌ Not supported | |
| User defined exceptions | ❌ Not supported | |
| Validator function for pl/perl | ❌ Not supported | |

## Extensions

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE EXTENSION .. CASCADE | ❌ Not supported | |
| Extension installation | ❌ Not supported | CREATE EXTENSION not translated |
| Trusted extensions | ❌ Not supported | |

## Internationalisation

| Feature | Status | Notes |
|---------|--------|-------|
| Built-in, platform independent immutable collation | ❌ Not supported | |
| casefold | ❌ Not supported | |
| Column-level collation support | ❌ Not supported | Binary collations (C, POSIX, ucs_basic, default) are accepted, as Turso compares text byte-wise; any other COLLATE is rejected |
| Database level collation | ❌ Not supported | |
| Default ICU collations for clusters/databases | ❌ Not supported | |
| EUC_JIS_2004 / SHIFT_JIS_2004 support | ❌ Not supported | |
| ICU collations | ❌ Not supported | |
| LIKE comparisons for nondeterministic collations | ❌ Not supported | |
| Multibyte encoding support, incl. UTF8 | 🟡 Partial | UTF-8 only (Turso native encoding); no other server encodings |
| Multiple language support | ❌ Not supported | |
| Nondeterministic collations | ❌ Not supported | |
| pg_unicode_fast collation | ❌ Not supported | |
| Unicode string literals and identifiers | ❌ Not supported | |
| UTF8 support on Windows | ❌ Not supported | |

## Client Applications

| Feature | Status | Notes |
|---------|--------|-------|
| psql-style REPL | 🟡 Partial | `tursopg` (not psql itself); supports `\d[+]`, `\dt[+]`, `\di`, `\dv`, `\dn`, `\dT`, `\du`/`\dg`, `\df`, `\l`, `\x`, `\timing`, `\echo`, `\conninfo`, `\?`, `\q`, and interactive `quit`/`exit`; no `\copy`, `\i`, `\e`, `\set`, `\pset`, `\g`, `\watch` |
| pgbench | ❌ Not supported | |
| pg_combinebackup | ❌ Not supported | |
| pg_createsubscriber | ❌ Not supported | |
| pg_prewarm | ❌ Not supported | |
| pg_rewind | ❌ Not supported | |
| pg_standby | ❌ Not supported | |
| pg_upgrade | ❌ Not supported | |
| pg_waldump | ❌ Not supported | |
| pg_walsummary | ❌ Not supported | |
| pg_xlogdump | ❌ Not supported | |
| psql \bind | ❌ Not supported | |
| psql \dconfig | ❌ Not supported | |
| psql pipeline queries | ❌ Not supported | |
| psql named prepared statements | ❌ Not supported | SQL-level PREPARE/EXECUTE/DEALLOCATE are not translated |
| Version aware psql | ❌ Not supported | |

## Additional Modules (contrib)

| Feature | Status | Notes |
|---------|--------|-------|
| adminpack | ❌ Not supported | |
| auth_delay | ❌ Not supported | |
| auto_explain | ❌ Not supported | |
| btree_gin | ❌ Not supported | |
| btree_gist | ❌ Not supported | |
| citext | ❌ Not supported | |
| dblink | ❌ Not supported | |
| dblink asynchronous notification support | ❌ Not supported | |
| file_fdw | ❌ Not supported | |
| fuzzystrmatch | ❌ Not supported | |
| hstore | ❌ Not supported | |
| intarray | ❌ Not supported | |
| isn (ISBN) | ❌ Not supported | |
| KNN support for CUBE | ❌ Not supported | |
| ltree | ❌ Not supported | |
| pageinspect | ❌ Not supported | |
| passwordcheck | ❌ Not supported | |
| pg_buffercache | ❌ Not supported | |
| pg_freespacemap | ❌ Not supported | |
| pg_logicalinspect | ❌ Not supported | |
| pg_overexplain | ❌ Not supported | |
| pg_stat_statements | ❌ Not supported | |
| pgstattuple | ❌ Not supported | |
| pg_trgm | ❌ Not supported | |
| pg_trgm regular expressions indexing | ❌ Not supported | |
| pg_walinspect | ❌ Not supported | |
| seg | ❌ Not supported | |
| sepgsql | ❌ Not supported | |
| sslinfo | ❌ Not supported | |
| tablefunc | ❌ Not supported | |
| tcn | ❌ Not supported | |
| tsearch2 compatibility wrapper | ❌ Not supported | |
| unaccent | ❌ Not supported | |
| uuid-ossp | ❌ Not supported | gen_random_uuid() (core) is available |
| xml2 | ❌ Not supported | |

## Network

| Feature | Status | Notes |
|---------|--------|-------|
| Full SSL support | ❌ Not supported | SSLRequest answered with "SSL not available"; plaintext only |
| IPv6 support | 🟡 Partial | Server binds whatever address it is given, including IPv6 literals; no dual-stack handling |
| V2 client protocol | ❌ Not supported | |
| V3 client protocol | 🟡 Partial | Via pgwire: simple query and extended query (Parse/Bind/Execute/Describe/Sync) protocols; trust auth only; parameter values must be text-format; Execute row limits ignored (no portal suspension); text COPY TO STDOUT in simple queries; no COPY FROM STDIN, CancelRequest, or NotificationResponse; a simple query with several statements runs each statement in its own transaction, not in one implicit transaction block |

## Platforms

Not applicable: the Turso Postgres frontend is Rust and builds on every
platform Turso supports; PostgreSQL's platform/compiler feature entries do
not carry over.
