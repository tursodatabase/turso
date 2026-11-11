# Step 3 of #11314: date and time functions, and `'now'` as input

Status: section B is step 3a, implemented with this doc. Sections A1, A2 and C are step 3b, still a design.

## TLDR

After this step, in tursopg:

```sql
BEGIN;
SELECT now() = transaction_timestamp(), now() = 'now'::timestamptz;   -- t | t, in every statement of the block
SELECT extract(year FROM date '2024-01-05'), pg_typeof(current_date);  -- 2024 | date
SELECT date_trunc('month', timestamp '2024-01-05 10:30:15');          -- 2024-01-01 00:00:00
SELECT to_char(timestamp '2024-01-05 09:03:07', 'YYYY-MM-DD HH24:MI'); -- 2024-01-05 09:03 (NULL on main)
SELECT make_date(2024, 1, 5), 'tomorrow'::date - 'today'::date;        -- 2024-01-05 | 1
COMMIT;
CREATE INDEX ON t((s::date));
INSERT INTO t(s) VALUES ('2024-01-05');  -- ok
INSERT INTO t(s) VALUES ('today');       -- error: the value of 'today' changes, so it can't be an index key
```

and in tursodb, `now()` and `CREATE TABLE t(ts timestamptz DEFAULT (now())) STRICT`, and a row
holding `'now'` under `CREATE INDEX ON t(julianday(s))` is refused as in SQLite (#11661).

Four PRs, ~865 engine lines added and ~50 removed (tests and docs not counted), in two parts:

| Part | PR | What | Engine lines | Needs |
|---|---|---|---|---|
| 3a, now | B | statement and transaction clock; `now()`, `transaction_timestamp()`, `statement_timestamp()`, `clock_timestamp()`; `current_date`, `localtimestamp`, `localtime` as casts of `now()`; in tursopg, `CURRENT_TIME`, `to_char` of a date and the time extension's functions become errors | ~180 | step 1 |
| 3b, after step 5 | A1 | dialect functions see argument types; `pg_typeof`, `make_date`, `extract`, `date_part` | ~240 | step 1 (#11492) |
| | A2 | `date_trunc`, `to_char` for the four types | ~190 | A1 |
| | C | `'now'`, `'today'`, `'tomorrow'`, `'yesterday'` as input; refused when a stored expression reads them while a row is written, and in DEFAULT and views when translated | ~255 added, ~50 removed | step 1, B |

#11314 says each step is one PR. At ~865 lines that is twice the ~400-line limit, so this
design splits it. 3a goes first because only the clock gives wrong answers today: after
`INSERT INTO a VALUES (now())`, `ts <= current_timestamp` is false, and one `SELECT now()` over
20,000 rows gives 80 different values (PostgreSQL: true, and 1 value). The rest of step 3 is
missing functions and input words, which give errors, not wrong answers. A1/A2 and B don't
depend on each other.

## What exists today

- Step 1 (#11492, open): `date`, `time`, `timestamp`, `timestamptz` are built-in types for both
  dialects, `CREATE TYPE date(value text) BASE text ENCODE temporal_encode(value, 'date')
  DECODE value OPERATOR '<'` (core/schema.rs on that branch). Values are stored as fixed text:
  `2024-01-05`, `10:30:15.123456`, `2024-01-05 10:30:15.123456`, and `timestamptz` in UTC with
  no suffix. `exec_temporal_encode` (core/functions/datetime_input.rs) refuses `now`, `today`,
  `tomorrow`, `yesterday`, `epoch`, `infinity`, `-infinity`, `allballs` (`Fields::read`).
- Step 2 (#11611, merged): a string literal compared with a custom-type value goes through
  ENCODE. The one place that decides this is `read_untyped_operand_as`
  (core/translate/expr/custom_types.rs:406).
- The clock: Turso already copies SQLite. SQLite resets `Vdbe.iCurrentTime`
  (vdbeInt.h:470) at the start of each `sqlite3VdbeExec` call, which is one `sqlite3_step`
  (vdbe.c:956), and `sqlite3StmtCurrentTime` reads the OS clock on the first request and keeps
  it (vdbeapi.c:1183). Turso has `ProgramState.current_time` (core/vdbe/mod.rs:1152), cleared
  when a step ends (mod.rs:2620) and on reset (mod.rs:1479), and read through `StatementClock`
  (core/vdbe/execute.rs:12495). Triggers and FK actions get the parent's value
  (`Statement::set_current_time`, core/statement.rs:798, called at execute.rs:6037-6039).
- tursopg on main (measured):

  | SQL | tursopg | PostgreSQL 17 |
  |---|---|---|
  | `now()` | `2026-11-10 14:15:56.201`, read on every call | transaction start, µs |
  | `statement_timestamp()`, `transaction_timestamp()` | same as `now()` | statement / transaction start |
  | `current_timestamp`, `localtimestamp` | `2026-11-10 14:15:56` (SQLite's, per step) | = `now()` |
  | `current_date`, `localtime` | SQLite's text | `date`, `time` |
  | `to_char(timestamp '…', 'YYYY')` | NULL | `2024` |
  | `make_date(2024, 1, 5)` | bytes from the time extension | `2024-01-05` |
  | `extract`, `date_part`, `date_trunc`, `pg_typeof` | no such function | see matrix |

## 1. The clock

### What PostgreSQL does (measured, `pg_clock.sql`)

Inside `BEGIN; SELECT pg_sleep(0.2); …`:

| Expression | Result |
|---|---|
| `now() = transaction_timestamp()`, `current_timestamp = now()` | t |
| `now() < statement_timestamp()` | t |
| `'now'::timestamptz = now()`, `'now'::timestamp = localtimestamp` | t |
| `localtimestamp = now()::timestamp`, `current_date = now()::date`, `localtime = now()::time` | t |
| `statement_timestamp() = statement_timestamp()` | t |
| `clock_timestamp() > statement_timestamp()` | t |
| `length(to_char(now(), 'US'))` | 6 (microseconds) |

So: `now()`, `transaction_timestamp()`, `current_timestamp`, `localtimestamp`, `localtime`,
`current_date` and the word `'now'` all read the time the transaction started; in autocommit
that is the statement's start. `statement_timestamp()` is fixed for one statement.
`clock_timestamp()` reads the clock on every call.

### Where it goes

SQLite has no transaction time, so there is no SQLite placement for it. It follows what
SQLite does for the step time: the value lives next to the thing it belongs to.

- `ProgramState.statement_times: Option<StatementTimes>` next to `current_time`, holding the
  statement's time and its transaction's time. Both are fixed on the statement's first request
  for the time and kept until `reset` (mod.rs:1486). Unlike `current_time` they are not
  cleared when a step ends. Copying the transaction time into the statement keeps `now()` the
  same for the whole statement, even when another statement on the connection runs `BEGIN` or
  `COMMIT` between two of its steps.
- `Connection.transaction_time: RwLock<Option<WallClockInstant>>` next to `auto_commit`
  (core/connection.rs:409). It is set, from the I/O clock, where a user transaction starts:
  `BEGIN` (execute.rs:5377), a `SAVEPOINT` that starts a transaction (execute.rs:5605) and
  `wal_insert_begin` (connection.rs:2490). That is PostgreSQL's rule: the time of `BEGIN`.
  `Connection::end_transaction` clears it, and every return to autocommit goes through it
  (connection.rs, vacuum.rs, execute.rs, mod.rs: 9 places). `Connection::transaction_time`
  asserts that autocommit has no time.
- The transactions the engine runs for itself, VACUUM (vacuum.rs:1973) and reading the schema
  back after a schema change in autocommit (execute.rs:14733), set `auto_commit = false`
  without a time, so a statement inside one uses its own statement time. Reading the schema
  back inside a user transaction keeps the user's time.
- The MVCC restore after an inner transaction aborts (statement.rs:1467) goes back to the
  outer transaction with the time it had, saved with the rest of the outer state
  (execute.rs:14422). The inner transaction's error rollback had already ended the outer one.
- One method decides what "now" means for the PostgreSQL clock, and `clock_timestamp()`
  fixes the statement time before it reads the clock. They are methods of
  `ScalarFuncContext`, which gets `ProgramState.statement_times` as a `Cell`:

  ```rust
  fn transaction_time(&self) -> WallClockInstant {
      self.statement_times().transaction
  }

  fn clock_time(&self) -> WallClockInstant {
      // statement_timestamp() <= clock_timestamp() whichever the query calls first.
      self.statement_times();
      self.io.current_time_wall_clock()
  }

  fn statement_times(&self) -> StatementTimes {
      // Read on the first request, then kept. The transaction time is the
      // connection's, or the statement time in autocommit, where a statement
      // is its own transaction.
  }
  ```

  The times are read lazily: a statement that never asks for the time never reads the clock.
  PostgreSQL guarantees `now() <= statement_timestamp() <= clock_timestamp()` in any order of
  evaluation. With the code above: inside a transaction, the transaction time was read at
  `BEGIN`, before any later statement reads its time; in autocommit `now()` is the statement
  time; and every `clock_timestamp()` reads the clock after the statement time is fixed. (All
  three come from the I/O clock; a wall clock that jumps back breaks this in PostgreSQL too.)
- The other choice was to read the statement time eagerly when a statement starts. That is one
  clock read for every statement run, including the ones that never ask for the time, and it
  changes the sequence of clock reads the simulator replays. The lazy read costs one extra
  clock read only in a statement that calls `clock_timestamp()`. It needs `clock_timestamp()`
  to be a core function, because it uses `ProgramState`. The cost of the lazy read: a
  statement that first asks for the time after reading many rows gets a later time than
  PostgreSQL's statement start; the order above still holds.
- `BEGIN` reads the clock once per transaction even if nothing asks for the time, because
  PostgreSQL's `now()` is the time of `BEGIN` and can't be read later.
- Triggers and FK actions get the parent's `statement_times` the same way they get
  `current_time` today (execute.rs:6008 resolves it first, then passes it). Without the
  resolve-first, a trigger that reads the time first would fix its own copy and the parent
  would later read a different one.
- MVCC: `BEGIN CONCURRENT` is a transaction like any other. The one MVCC-only path is the
  sequence restore above.
- The SQLite functions (`date('now')`, `CURRENT_TIMESTAMP` in tursodb) keep the per-step
  SQLite clock. Only the PostgreSQL functions and the PostgreSQL types' `'now'` use
  `transaction_time`.

Difference from PostgreSQL that this leaves: several statements in one simple-query message
share one transaction in PostgreSQL; tursopg runs each in its own. Not this step's problem; it
is how tursopg runs the message.

### Functions

Three functions need `ProgramState` (the statement's time), which dialect functions don't get
(`Dialect::exec_scalar_function` receives only the connection, core/dialect/mod.rs:233). They
are core `ScalarFunc`s:

| Function | Type | Value | Name resolved by |
|---|---|---|---|
| `now()` | timestamptz | `transaction_time` | core (both dialects) |
| `transaction_timestamp()` | timestamptz | `transaction_time` | `PostgresDialect::resolve_function` |
| `statement_timestamp()` | timestamptz | `statement_time` | `PostgresDialect::resolve_function` |
| `clock_timestamp()` | timestamptz | `clock_time` | `PostgresDialect::resolve_function` |

Only `now` goes in core's name table, so tursodb gets `now()` and nothing else (Q3).

The rest are not functions of their own. PostgreSQL defines them by `now()`, and the clock
table above shows it (`localtimestamp = now()::timestamp`, and so on), so tursopg writes them
as casts of `now()`:

| SQL (tursopg) | becomes | Type |
|---|---|---|
| `CURRENT_TIMESTAMP` | `now()` | timestamptz |
| `CURRENT_DATE` | `CAST(now() AS date)` | date |
| `LOCALTIMESTAMP` | `CAST(now() AS timestamp)` | timestamp (the session zone is UTC) |
| `LOCALTIME` | `CAST(now() AS time)` | time |
| `CURRENT_TIME` | error | (PostgreSQL: timetz) |

A CAST to a custom type runs the input through ENCODE then DECODE
(`emit_custom_type_cast`, core/translate/expr/translator.rs:878 on #11492), and step 1's
`date` and `time` input already take a full timestamp (`2024-01-05 10:00:00` → `2024-01-05`;
`2024-01-05 10:00` → `10:00:00`, tests in datetime_input.rs `reads_postgresql_input_forms`).
`pg_typeof(current_date)` is `date` because the CAST's type is.

Values are written by step 1's formatter (`Fields::format`), so `now()` has the same text as a
stored `timestamptz` and `'now'::timestamptz = now()` holds. The three functions are
non-deterministic, so the existing checks already refuse them, and the casts of them, in index
expressions, partial-index WHERE and generated columns.

tursopg: the translator's `SqlvalueFunction` arm (postgres/parser/translator.rs:3349) maps
these keywords to SQLite's `Literal::Current*` today, and maps `CURRENT_TIME` and `LOCALTIME`
both to `Literal::CurrentTime`. It emits the forms above instead, with one arm per keyword.
`CURRENT_TIME` is an error (Q5): its type is `timetz`, which prints an offset
(`10:30:15.123456+00`), and returning a `time` would print a different value. A precision
argument (`localtimestamp(2)`) stays an error, as today. The chrono-based `now`,
`transaction_timestamp`, `statement_timestamp` and `clock_timestamp` in
postgres/frontend/functions.rs:134-135 are deleted.

tursodb: `now()` is a new function name; SQLite has none. `localtimestamp` and `localtime`
stay "no such column" errors (Q2): making an identifier that names no column into a value
would turn a misspelled column into the time. `CURRENT_DATE`, `CURRENT_TIME` and
`CURRENT_TIMESTAMP` in tursodb stay SQLite's (Q3).

`DEFAULT now()` in tursodb: SQLite's grammar allows only a literal, a signed number, an
identifier or `(expr)` after DEFAULT (parse.y:390-400), so `DEFAULT now()` is a syntax error in
sqlite3 and Turso alike. `DEFAULT (now())` already parses, and SQLite allows a non-constant
function there (`DEFAULT (random())`). This design supports `DEFAULT (now())` and makes no
grammar change (Q1).

## 2. `'now'`, `'today'`, `'tomorrow'`, `'yesterday'` as input

### What PostgreSQL accepts (measured, session zone UTC)

| Input | date | time | timestamp | timestamptz |
|---|---|---|---|---|
| `now` | today | time of `now()` | `localtimestamp` | `now()` |
| `today` | today | error | today 00:00 | today 00:00 UTC |
| `tomorrow`, `yesterday` | ±1 day | error | ±1 day, 00:00 | ±1 day, 00:00 UTC |
| `' NOW '`, `'Now'` | as `now` | | | |
| `today 10:00`, `10:00 today` | | | today 10:00 | |
| `tomorrow 10:00` | | | tomorrow 10:00 | |
| `today +02` | | | | `2026-11-09 22:00:00+00` (midnight at +02) |
| `now 10:00`, `now +02`, `today today`, `nowx` | error | error | error | error |
| `'today'::date - 'yesterday'::date` | 1 | | | |

### Where it goes

Step 1's `Reader` (datetime_input.rs) already reads fields one at a time like PostgreSQL's
`DecodeDateTime`. `today`, `tomorrow`, `yesterday` become a date field (they set year, month and
day, so `today 2024-01-05` fails as a repeated field, as in PostgreSQL). `now` sets every field
including the zone, so any other field after it fails. `time` refuses the three date words.
`epoch`, `infinity`, `-infinity` and `allballs` stay refused (not this step).

`exec_temporal_encode(value, type_name)` gets a third argument, `now: &dyn Fn() -> WallClockInstant`,
called only when one of the words is read. The VDBE (execute.rs:11335 on #11492) passes
`transaction_time` read through the call's `StatementClock`, which records that the clock was
read (rule 1 below). tursopg's `pg_input_is_valid` (postgres/frontend/functions.rs:1460) passes
the wall clock, since only validity matters there.

`temporal_encode` stays deterministic for the optimizer: a constant conversion (`d > '2024-01-05'`)
is still computed once per statement, as step 2 does, and `d > 'today'` is computed once per
statement too, with the transaction's time. SQLite does the same with its date functions: they
are `PURE_DATE`, which is `SQLITE_FUNC_CONSTANT | SQLITE_FUNC_SLOCHNG` ("value constant during a
single query", sqliteInt.h:2082, 2201-2204; date.c:1886-1892), although `julianday('now')`
reads the clock. So "deterministic" here means "the same within one statement", and nothing
that trusts it may store the result. The refusals below are the only thing that keeps the
result out of stored data, and their tests are what show it.

### What must be refused, and where

A stored value must never depend on the time it was computed. Turso recomputes index keys and
generated columns from the row (on UPDATE, DELETE, integrity_check, CREATE INDEX), so a key
computed from `'today'` yesterday is not found today. #11661 shows the same thing on main with
`julianday(s)` and a row holding `'now'`: `UPDATE` fails with `IdxDelete: no matching index
entry` and `PRAGMA integrity_check` reports the row missing.

PostgreSQL refuses STABLE functions in index expressions, partial-index predicates and generated
columns. Its date/time input functions are STABLE (`date_in`, `time_in`, `timestamp_in`,
`timestamptz_in`: `provolatile = 's'`), and so are `now()`, `to_char`, the `timestamptz` forms
of `extract`/`date_part`/`date_trunc`, and the casts `timestamptz::date` and `::text`. It fixes
a literal at `CREATE` time instead (`WHERE d > 'now'` is stored as `WHERE d > '2026-11-10'::date`).
Turso stores SQL text and can't fix it.

So there are two rules, for two different mistakes:

1. **A stored expression must not read the clock.** Index expressions, partial-index WHERE,
   generated columns and CHECK. Checked when the expression runs, as SQLite does (Q6). One check
   covers `CAST(s AS date)` with `s = 'today'`, `d > 'now'`, and SQLite's `julianday(s)` with
   `s = 'now'` (#11661).
2. **A word that PostgreSQL fixes at CREATE must not be read later.** `DEFAULT 'now'` and a
   `'now'` literal in a view: PostgreSQL keeps the time of CREATE, and Turso would read the time
   of each INSERT or query, which is a different answer. Checked when the SQL is translated,
   because no row is involved (Q4).

CHECK is under rule 1 although it stores nothing: `PRAGMA integrity_check` evaluates CHECK
constraints again on every row (core/translate/integrity_check.rs:346-431, as SQLite does,
pragma.c:2035), so `CHECK (d > 'now')` would pass when the row is written and fail later.
SQLite's check covers CHECK for the same reason.

Measured in PostgreSQL 17:

| Statement | PostgreSQL 17 | This design |
|---|---|---|
| `CREATE INDEX ON x((s::date))` (`s` text), same for time/timestamp/timestamptz | error: functions in index expression must be marked IMMUTABLE | ok; a row whose `s` is one of the words is refused |
| `CREATE INDEX ON x((ts::date))`, `((d::timestamp))` | ok | ok |
| `CREATE INDEX ON x((tz::date))` | error | ok (zone is always UTC) |
| `CREATE INDEX ON x((extract(year from d)))`, `((date_trunc('day', ts)))`, `((make_date(2024,1,1)))` | ok | ok |
| `CREATE INDEX ON x((extract(year from tz)))`, `((date_trunc('day', tz)))` | error | ok (zone is always UTC) |
| `CREATE INDEX ON x((to_char(ts, 'YYYY')))` | error | ok (no locale or zone setting) |
| `CREATE INDEX ON x(((d > 'now')))` | ok, fixed at CREATE | every write refused; CREATE INDEX refused if the table has rows |
| `CREATE INDEX ON x(d) WHERE s::date > '2020-01-01'` | error: functions in index predicate must be marked IMMUTABLE | ok; a row whose `s` is one of the words is refused |
| `CREATE INDEX ON x(d) WHERE d > 'now'` | ok, fixed at CREATE | every write refused; CREATE INDEX refused if the table has rows |
| `CREATE INDEX ON x(d) WHERE d > now()`, `WHERE d > current_date` | error | error at CREATE (already: non-deterministic) |
| `g date GENERATED ALWAYS AS (s::date) STORED` | error: generation expression is not immutable | ok; a row whose `s` is one of the words is refused |
| `g date GENERATED ALWAYS AS ('now') STORED` | ok, fixed at CREATE | every write refused |
| `g boolean GENERATED ALWAYS AS (d > 'today') STORED` | ok, fixed at CREATE | every write refused |
| `g timestamp GENERATED ALWAYS AS (d) STORED` | ok | ok |
| `g numeric GENERATED ALWAYS AS (extract(year from d)) STORED` | ok | ok |
| `g timestamptz GENERATED ALWAYS AS (now()) STORED` | error | error at CREATE |
| `CHECK (s::date > '2000-01-01')` | ok | ok; a row whose `s` is one of the words is refused |
| `CHECK (d < 'now')` | ok, fixed at CREATE | every write refused |
| `DEFAULT 'now'`, `DEFAULT 'today'` | ok, fixed at CREATE TABLE | error at CREATE TABLE |
| `DEFAULT now()`, `DEFAULT current_date`, `DEFAULT localtimestamp` | ok, per row | ok, per row |
| `DEFAULT 'now'::text::timestamp` | ok, per row | ok, per row |
| view `SELECT 'now'::date, d > 'now'` | fixed at CREATE VIEW | error when the view is queried |
| `PREPARE p AS SELECT 'now'::timestamptz = now()` | fixed at PREPARE (f on EXECUTE) | read per run (t) |

Turso accepts more than PostgreSQL in the "ok; a row … is refused" rows. That gives no wrong
answer: every row it accepts has a value that doesn't change.

The PREPARE row stays different: PostgreSQL parses the statement again when it plans it again,
so what it returns depends on its plan cache.

### Rule 1: refused when the expression runs, as SQLite does

SQLite allows `CREATE INDEX ON t(julianday(s))` and refuses the row. Functions in index
expressions, partial-index WHERE, generated columns and CHECK are emitted as `OP_PureFunc`
instead of `OP_Function` (`sqlite3VdbeAddFunctionCall`, vdbeaux.c:449). The context comes from
the resolver, which stores it on the expression (resolve.c:1259, expr.c:5613). The date
functions call `sqlite3NotPureFunc` (vdbeaux.c:5635) when they meet `'now'` (date.c:455, 462) or
`'localtime'`/`'utc'` (date.c:878, 905), and it raises `non-deterministic use of julianday() in
an index` (or `in a generated column`, `in a CHECK constraint`, vdbeaux.c:5644-5652).

Turso does the same, with the check in one place instead of in each function:

- `Insn::Function` (core/vdbe/insn.rs:1541) gets `context: FunctionContext`, one of `Query`,
  `Index`, `GeneratedColumn`, `Check` (SQLite's `p5`). Because it is a field of the
  instruction, it stays right when a constant call is moved to run once per statement.
- One function decides the context: `ProgramBuilder::emit_stored_expr(kind, |program| …)` sets
  the builder's current context for the closure and sets `Query` again after it. It asserts the
  context is `Query` on entry, since stored expressions don't nest. Every `Insn::Function` takes
  the builder's current context. These helpers call it around their `translate_expr` or
  `translate_condition_expr`:

  | Expression | Emitted by |
  |---|---|
  | index key | `emit_index_column_value_old_image` (emitter/mod.rs:2113), `_new_image` (:2222), `_for_insert` (insert.rs:3546), `_from_cursor` (index.rs:1354) |
  | partial-index WHERE | `emit_partial_index_check` (insert.rs:1393), emitter/mod.rs:2174, index.rs:520 and 620, integrity_check.rs:444, upsert.rs:682 |
  | generated column | `emit_generated_columns` (emitter/gencol.rs:52 on #11492), `emit_gencol_expr_from_registers` (:85) |
  | a generated column's value through its type's ENCODE | `emit_custom_type_encode_columns`, for generated columns only. A STORED column's ENCODE runs with the rest of the row, outside the helpers above, and without this `g date AS ('now') STORED` would read the clock in a query context. |
  | CHECK | `emit_check_constraints` (emitter/mod.rs:2427), integrity_check.rs:346-431 |

- Every read of the clock or of the local time zone during one call goes through the call's
  `StatementClock` (execute.rs:12495), which gets `read: Cell<bool>`. That is SQLite's step
  clock for `date('now')`, the transaction time for `temporal_encode`'s words, and the zone for
  the `localtime` and `utc` modifiers (datetime.rs:527, 570).
- After the call, `op_function` checks once: the context is not `Query` and the clock was
  read → error, with SQLite's text (`non-deterministic use of julianday() in an index`). For
  `temporal_encode` the text names the input and the type, not the function:
  `non-deterministic use of 'now' as a date in a CHECK constraint`. tursopg gives it SQLSTATE
  42P17, PostgreSQL's code for the same rule at CREATE.

The check is in `op_function`, not in each function, so a function that reads the clock can't
forget it. `temporal_encode` (a `ScalarFunc`, execute.rs:11335 on #11492) and the SQLite date
functions get it from the same line, which fixes #11661. The `localtime` and `utc` modifiers
need their zone read to go through `StatementClock` (~10 lines).

With that, the static check `is_deterministic_datetime_call` (core/schema.rs:6936-6979), which
refuses `julianday('now')` and the modifiers at CREATE by looking at literal arguments, is a
second copy of the same rule, and it goes (~−50 lines). `CREATE INDEX ON t(julianday('now'))`
on an empty table then succeeds and its first INSERT fails, as in SQLite. The refusal of
`CURRENT_TIMESTAMP`, `CURRENT_DATE` and `CURRENT_TIME` anywhere in a stored expression stays
(index.rs:1298, schema.rs:7116, 9025): SQLite refuses those at CREATE, like `random()`.

**Where a forgotten helper is caught.** A type that makes a forgotten site fail to compile was
the first choice. It doesn't fit here:

- Wrapping the schema fields (`IndexColumn.expr`, `Index.where_clause`, `CheckConstraint.expr`
  at core/schema.rs:5489, the generated column's expression) in a type whose only emit method
  sets the context still needs a way to read the `ast::Expr` inside. Their 100+ other uses
  (expression-index matching in the planner, ALTER and DROP dependency checks, printing the
  SQL) read it, and with that reader a forgotten site can call `translate_expr` on it again.
- SQLite's own placement, the context stored on the expression, would be a new `ast::Expr`
  variant or field; `Expr::SubqueryResult` alone shows a new variant costs ~60 match arms.

So a generated test catches it instead: every kind of stored expression × every path that
evaluates it, each with a row holding `'now'`, expecting the error.

| Dimension | Values |
|---|---|
| expression | expression index, partial-index WHERE, virtual generated column, stored generated column, CHECK |
| path | INSERT VALUES, INSERT SELECT, UPDATE, UPSERT DO UPDATE, REPLACE, CREATE INDEX over existing rows, ALTER TABLE ADD COLUMN (generated, CHECK), SELECT of a virtual generated column, `PRAGMA integrity_check` |
| journal | WAL, MVCC |

It uses `julianday(s)` in sqlite-sqltests, so the expected error comes from real SQLite, and a
copy with `CAST(s AS date)` and the other three types on Turso only (SQLite has no `date`
type), expecting the same text. The old-image helper (UPDATE and DELETE computing the old key)
can only see `'now'` in a database written before this change, so that case uses a fixture
database written by main.

### Rule 2: refused when the SQL is translated

`DEFAULT 'now'`: `validate_default_expr` (core/translate/schema.rs:147, called for CREATE TABLE
at :712 and ALTER TABLE ADD COLUMN at alter.rs:1299) refuses a string literal that is one of
the four words, or `CAST` of one, when the type reads the clock. `DEFAULT now` (an identifier,
which SQLite reads as the string `'now'`, parse.y:400) is refused the same way.
`DEFAULT 'now'::text::timestamp` is not: the inner CAST's operand is the literal, and its
type is `text`.

A literal in a view: the places that decide a literal becomes one of the types,
`read_untyped_operand_as` (core/translate/expr/custom_types.rs:406) for `d > 'now'` and the CAST
of a literal for `'now'::date`, refuse one of the words when `resolver.enclosing_view()`
(core/translate/catalog.rs:167) is set. CREATE VIEW doesn't plan its query
(core/translate/view.rs:444-457 only walks it), so the error comes when the view is first
queried. That is how SQLite and Turso report every other error in a view's query (`CREATE VIEW
v AS SELECT nosuch FROM t` succeeds in sqlite3 3.54.1; `SELECT * FROM v` fails).

"Is it one of the four words" is step 1's `Reader`, called from those three places through one
function, so there is no second list of words. Each call site gets a test that fails when its
call is removed. Error text: `cannot use 'now' in a DEFAULT: PostgreSQL fixes it when the table
is created; use DEFAULT now()` and `cannot use 'now' in a view: …`, both SQLSTATE 0A000 in
tursopg.

## 3. Functions

All of these read and write the fixed text forms. What they return depends on the argument's
type (PostgreSQL resolves the overload at parse time), and at run time a `date` is just text.
So the type has to come from translation.

### How a dialect function learns its argument types (A1)

Core resolves dialect functions by name and argument count (`Dialect::resolve_function`,
core/dialect/mod.rs:222; called from core/translate/catalog.rs:180), before argument types are
known. New hook: when core emits a `Func::Dialect` call, it asks the dialect once more with
the arguments' `type_of` results:

```rust
/// Picks the variant of `func` for these argument types, as PostgreSQL resolves an overload.
fn resolve_overload(&self, func: &Func, arg_types: &[ExprType]) -> Result<Func> { Ok(func.clone()) }
```

PostgresDialect returns a `Func::Dialect` whose name names the variant (`extract(date)`,
`to_char(timestamptz)`, `pg_typeof(date)`), or an error for argument types PostgreSQL has no
variant for (`function extract(unknown, text) does not exist`, SQLSTATE 42883). The functions
themselves live in postgres/frontend/functions.rs. Core stays free of PostgreSQL names.

`pg_typeof(x)` doesn't evaluate `x`: its variant returns a constant. The name it returns is the
one tursopg's wire protocol reports for `SELECT x` (`resolve_pg_type_for_column`,
postgres/server/lib.rs:437). That mapping moves to postgres/frontend so both use the same
function. `pg_typeof` returns `text`: there is no `regtype`, so `pg_typeof(1) = 'integer'::regtype`
stays an error.

### Placement

| Function | tursopg | tursodb |
|---|---|---|
| `extract`, `date_part`, `date_trunc`, `to_char` (dates), `make_date`, `pg_typeof` | postgres/frontend/functions.rs, via `resolve_overload` | not added (Q3) |
| `now()` | core `ScalarFunc` (it needs `ProgramState`) | the same core function |
| `transaction_timestamp()`, `statement_timestamp()`, `clock_timestamp()` | core `ScalarFunc`s, names resolved by `PostgresDialect::resolve_function` | not added (Q3) |
| `current_date` (type `date`), `localtimestamp`, `localtime` | translator writes `CAST(now() AS …)` | `localtimestamp`, `localtime`: "no such column", as today (Q2); `CURRENT_DATE` stays SQLite's (Q3) |
| `current_time` | error (Q5) | stays SQLite's (Q3) |
| `DEFAULT now()` | already parses | `DEFAULT (now())`, no grammar change (Q1) |

### The time extension's functions in tursopg (B)

The time extension (core/time/mod.rs) is always registered, for both dialects
(core/ext/mod.rs:337-338). Most of its names are `time_*` and `dur_*`, but three are PostgreSQL
names with a different meaning: `make_date` (:233), `make_timestamp` (:242) and `to_timestamp`
(:444) return the extension's 13-byte BLOB. tursopg resolves them when
`PostgresDialect::resolve_function` doesn't know the name (core/translate/catalog.rs:173-186),
so `make_date(2024, 1, 5)` returns bytes (#11093 is the same for `to_timestamp`).

Refusing those three names in PostgresDialect would be a list that has to follow the extension.
Instead the dialect says which built-in extensions it uses:

- `Dialect::has_builtin_extension(&self, extension: BuiltinExtension) -> bool`, with
  `BuiltinExtension::{Uuid, Series, Time, Percentile, Regexp}`, default `true` (SQLite: all
  of them). PostgresDialect matches every variant, with no `_` arm, and returns `false` for
  `Time`.
- `register_global_builtin_extensions` (core/ext/mod.rs:294) asks it before it registers each
  extension. A database's dialect is fixed when it opens, and these registrations are per
  database, so tursopg never has the functions at all. Extensions an application loads are not
  affected.

tursopg then gives the error it gives for any unknown function for all three, until A1 adds
`make_date` to
postgres/frontend/functions.rs. `make_timestamp` and `to_timestamp` stay errors (#11093 open).
tursodb keeps all of the time extension.

### Existing bugs this step fixes

- **`to_char(timestamp, text)` returns NULL in tursopg.** `exec_to_char`
  (postgres/frontend/functions.rs:1045 on #11492) handles numbers and returns `None`, read as NULL, for
  everything else. Slipup or design smell: design smell, a function body that answers NULL for
  input it does not handle. B makes an unhandled type an error (~10 lines); A2 adds the date
  forms.
- **`make_date`, `make_timestamp`, `to_timestamp` return bytes in tursopg** (#11093). Slipup or
  design smell: design smell, every function of a SQLite-dialect extension resolves in tursopg.
  B hides the time extension in tursopg (above); A1 adds `make_date`.
- **tursopg `now()` reads the clock on every call, with milliseconds.** `now() = now()` can be
  false, two rows of one INSERT can differ, and it keeps milliseconds where PostgreSQL keeps
  microseconds. Slipup or design smell: design smell, the function lives where it can't see the
  statement's clock.
- **tursopg `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `LOCALTIME`, `CURRENT_DATE` are SQLite's:**
  whole seconds, text type, per step. Slipup or design smell: design smell, PostgreSQL keywords
  mapped to SQLite look-alikes with different meaning.

- **#11661: a row holding `'now'` under `CREATE INDEX ON t(julianday(s))` corrupts the
  index.** Slipup or design smell: design smell, the check looks at literal arguments at
  CREATE, but the value comes from the row. C refuses it when the expression runs (rule 1).

Not fixed here: #11093 keeps `make_timestamp` and `to_timestamp`, which become errors in
tursopg but are not implemented.

## 4. Expected results (PostgreSQL 17, `SET TIME ZONE 'UTC'`)

Scripts and full output: `pg_funcs.sql`, `pg_more.sql`, `pg_clock.sql`, `pg_refuse.sql` in the
side-session notes folder (`step3-pg_*.{sql,out}`). The `.sqltest` files take their expected
values from these outputs, not from Turso. tursopg tests go in
postgres/conformance/pg-sqltests/ so they also run against real PostgreSQL (`run-postgres`).

### extract (numeric) | date_part (double precision)

Inputs: `date '2024-01-05'`, `time '10:30:15.123456'`, `timestamp '2024-01-05 10:30:15.123456'`,
`timestamptz '2024-01-05 10:30:15+02'`.

| field | date | time | timestamp | timestamptz |
|---|---|---|---|---|
| year | 2024 \| 2024 | 0A000 \| 0A000 | 2024 \| 2024 | 2024 \| 2024 |
| month, quarter, week | 1 | 0A000 | 1 | 1 |
| day, doy | 5 | 0A000 | 5 | 5 |
| dow, isodow | 5 | 0A000 | 5 | 5 |
| isoyear | 2024 | 0A000 | 2024 | 2024 |
| decade, century, millennium | 202, 21, 3 | 0A000 | 202, 21, 3 | 202, 21, 3 |
| hour | 0A000 \| 0 | 10 | 10 | 8 |
| minute | 0A000 \| 0 | 30 | 30 | 30 |
| second | 0A000 \| 0 | 15.123456 | 15.123456 | 15.000000 \| 15 |
| milliseconds | 0A000 \| 0 | 15123.456 | 15123.456 | 15000.000 \| 15000 |
| microseconds | 0A000 \| 0 | 15123456 | 15123456 | 15000000 |
| epoch | 1704412800 | 37815.123456 | 1704450615.123456 | 1704443415.000000 \| 1704443415 |
| julian | 2460315 | 0A000 | 2460315.43767504000000000000 \| 2460315.43767504 | 2460315.35434027777777777778 \| 2460315.3543402776 |
| timezone, timezone_hour, timezone_minute | 0A000 | 0A000 | 0A000 | 0 |
| nosuchfield | 22023 | 22023 | 22023 | 22023 |

`a | b` means extract gives `a`, date_part gives `b`; one value means both. `0A000` is
`unit "…" not supported for type …`, `22023` is `unit "…" not recognized for type …`.
`date_part(…, date)` reads the date as a timestamp, so `hour` is 0 where extract errors.
Field names are case-insensitive and may be quoted (`extract('year' FROM …)`).
`extract(epoch FROM timestamp 'infinity')` = `Infinity`, `extract(day FROM date 'infinity')`
is NULL (infinity input is not this step; listed for later). `extract(year FROM '2024-01-05')`
is `42725 function … is not unique`; `extract(year FROM '2024-01-05'::text)` is `42883`.
Result types: `numeric` and `double precision`.

### date_trunc

| field | date → timestamptz | timestamp | timestamptz `…10:30:15+02` |
|---|---|---|---|
| microseconds | 2024-01-05 00:00:00+00 | 2024-01-05 10:30:15.123456 | 2024-01-05 08:30:15+00 |
| milliseconds | | 2024-01-05 10:30:15.123 | |
| hour | 2024-01-05 00:00:00+00 | 2024-01-05 10:00:00 | 2024-01-05 08:00:00+00 |
| day | 2024-01-05 00:00:00+00 | 2024-01-05 00:00:00 | 2024-01-05 00:00:00+00 |
| week (`2024-12-31`) | 2024-12-30 00:00:00+00 | 2024-12-30 00:00:00 | |
| month (`2024-12-30`) | 2024-12-01 00:00:00+00 | 2024-12-01 00:00:00 | |
| quarter / decade / century (`2024-05-05`) | | 2024-04-01, 2020-01-01, 2001-01-01 | |
| millennium | 2001-01-01 00:00:00+00 | 2001-01-01 00:00:00 | |
| nosuchfield | 22023 | 22023 | 22023 |
| `' day'` | | 22023 | |
| `'DAY'` | | 2024-01-05 00:00:00 | |

`date_trunc(…, time)` is the `interval` variant (returns `interval`): refused until step 4.
`date_trunc('day', tz, 'Europe/Helsinki')` = `2024-01-04 22:00:00+00`: named zones are step 7,
refused. `date_trunc(NULL, …)` is NULL.

### to_char (returns text)

| value | pattern | result |
|---|---|---|
| `timestamp '2024-01-05 09:03:07.123456'` | `YYYY-MM-DD HH24:MI:SS.MS.US` | `2024-01-05 09:03:07.123.123456` |
| same | `HH HH12 AM am PM` | `09 09 AM am AM` |
| `timestamp '2024-01-05 21:03:07'` | `HH12 AM` | `09 PM` |
| `timestamp '2024-01-05 09:03:07'` | `Mon MON mon Month FMMonth Day FMDay Dy DY` | `Jan JAN jan January   January Friday    Friday Fri FRI` |
| same | `DDD D ID IW WW Q J CC YY Y` | `005 6 5 01 01 1 2460315 21 24 4` |
| same | `FMDD FMMM DDth DDTH FMDDth` | `5 1 05th 05TH 5th` |
| same | `"Year" YYYY "of" "the" "x"` | `Year 2024 of the x` |
| same | `IYYY-IW-ID` | `2024-01-5` |
| same | `SSSS SSSSS` | `32587 32587` |
| same | `Q RM rm` | `1 I    i   ` |
| same | `xyz \\ YYYY` | `x4z \\ 2024` (`y` is a pattern) |
| same | `''` | empty string |
| same | `NULL` | NULL |
| `timestamptz '2024-01-05 09:03:07+02'` | `YYYY-MM-DD HH24:MI:SS TZ OF TZH:TZM` | `2024-01-05 07:03:07 UTC +00 +00:00` |
| `timestamp '…'` | `TZ OF` | ` +00` (empty TZ) |
| `date '2024-01-05'` | `YYYY-MM-DD HH24` | `2024-01-05 00` |
| `time '10:30:15'` | `HH24:MI` | `10:30` in PostgreSQL (interval variant): refused until step 4 |

`to_char(1234.5, '9999.9')` keeps working (` 1234.5`).

### make_date

| call | result |
|---|---|
| `make_date(2024, 1, 5)` | `2024-01-05`, type `date` |
| `make_date('2024', '1', '5')` | `2024-01-05` (string literals read as integers) |
| `make_date(-44, 3, 15)` | `0044-03-15 BC`: BC dates are outside step 1's range, so 22008 here (list it) |
| `make_date(10000, 1, 1)` | `10000-01-01`: 5-digit years are outside step 1's range, 22008 here |
| `make_date(2024, 2, 30)`, `(2024, 13, 1)`, `(0, 1, 1)` | 22008 `date field value out of range: …` |
| `make_date(5874898, 1, 1)` | 22008 `date out of range` |
| `make_date(NULL, 1, 1)` | NULL |
| `make_date(2024.6, 1, 5)` | 42883 `function make_date(numeric, integer, integer) does not exist` |

### pg_typeof

`date`, `time without time zone`, `timestamp without time zone`, `timestamp with time zone`
for the four types; `integer` for `1`, `numeric` for `1.5`, `text` for `'a'::text`, `unknown`
for `'a'` and `NULL`, `boolean`, `bigint`, `double precision`, `integer` for `date - date`.
`pg_typeof(now())` and `pg_typeof(current_timestamp)` = `timestamp with time zone`;
`localtimestamp` → `timestamp without time zone`; `localtime` → `time without time zone`;
`current_date` → `date`; `extract(…)` → `numeric`; `date_part(…)` → `double precision`;
`to_char(…)` → `text`; `date_trunc(…, date)` → `timestamp with time zone`.
Where tursopg's own literal typing differs (`1.5`), the test records PostgreSQL's answer and
the row is `@skip`ped with the reason, not changed to Turso's.

### Clock tests

Rust integration tests (tests/integration/), because they need two statements, a wait and
`BEGIN`: the rows of the clock table in section 1, plus: two rows of one `INSERT … SELECT now()`
are equal; `now()` in an `AFTER INSERT` trigger equals the statement's; a new transaction after
`COMMIT`, `ROLLBACK`, outermost `RELEASE` and an error gets a later time; the same under MVCC.
The `'now'` table in section 2 goes in `.sqltest` as relations (`'today'::date = current_date`,
`'tomorrow'::date - 'today'::date = 1`), never as clock values.

The order of the three clocks, in postgres/conformance/pg-sqltests/ (true on real PostgreSQL),
with `clock_timestamp()` first in the select list so that a lazy `statement_time` read after it
would fail:

```sql
SELECT clock_timestamp() >= statement_timestamp(), statement_timestamp() >= now();  -- t | t
BEGIN;
SELECT clock_timestamp() >= statement_timestamp(), statement_timestamp() >= now();  -- t | t
COMMIT;
```

`now() < statement_timestamp()` inside a transaction needs a wait, so it is in the Rust test.

## 5. Size

| PR | Engine lines | Content |
|---|---|---|
| A1 | ~240 | `resolve_overload` hook + call at the one `Func::Dialect` emission site, core/translate/expr/translator.rs:1102 (~40); type-name mapping moved for `pg_typeof` (~50); `make_date` (~30); `extract`/`date_part` (~120: each argument read once into a chrono `NaiveDateTime`, fields from `Datelike`/`Timelike`, per-type refusals, numeric scale) |
| A2 | ~190 | `date_trunc` (~40, chrono); `to_char` as one table from pattern to field, plus `FM`/`TH` and quoted text (~150) |
| B | ~180 | `statement_time`, `transaction_time` set at three transaction starts (~35); three core `ScalarFunc`s (`now` and `transaction_timestamp` are one), `clock_timestamp()` fixing `statement_time` first, three names in `PostgresDialect::resolve_function` (~65); tursopg translator writes `now()` and the casts, one arm per keyword, `CURRENT_TIME` an error, chrono functions deleted (~35); `to_char` errors on a value it doesn't handle (~10); time extension hidden in tursopg: a dialect method asked before each built-in extension is registered (~35) |
| C | ~255 added, ~50 removed | the four words in `Reader` (~70); clock argument to `exec_temporal_encode` (~25); rule 1: `FunctionContext` on `Insn::Function` (~10), `emit_stored_expr` (~30), the helpers calling it (~25), `StatementClock.read` and the one check in `op_function` (~20), messages (~10), modifiers' zone read through `StatementClock` (~10); rule 2: view literals (~30), `DEFAULT 'now'` (~25); `is_deterministic_datetime_call` deleted (~−50) |

Order: B is 3a and goes now. A1 → A2 and C are 3b and go after step 5 of #11314; C needs B's
clock and step 1's `Reader`. A1 and B don't depend on each other: `make_date` resolves through
`PostgresDialect::resolve_function`, before the extension fallback.

## 6. Decisions

The questions the sections above refer to as Q1-Q6, and their answers.

- **Q1.** `DEFAULT (now())` is enough for tursodb. No grammar change.
- **Q2.** No `localtimestamp`/`localtime` in tursodb. Reading an identifier that names no
  column as a value fails open: a misspelled column becomes the time. tursodb's error today is
  a correct answer.
- **Q3.** tursodb gets only `now()`, which the core function gives for free. Its `CURRENT_*`
  stay SQLite's.
- **Q4.** `'now'` literals in views are refused too: PostgreSQL fixes them at CREATE VIEW, so
  reading them at query time is a wrong answer. Prepared statements read them per run:
  PostgreSQL parses again when it plans again, so its own answer depends on its plan cache.
- **Q5.** `CURRENT_TIME` is an error: `timetz` prints an offset, and a `time` would print a
  different value.
- **Q6.** Stored expressions are checked when they run, as SQLite does. #11661 needs that
  check anyway (SQLite accepts `CREATE INDEX ON t(julianday(s))`), and a static check would be
  a second copy of "a stored value must not read the clock". `DEFAULT 'now'` and view literals
  stay a check at translation, because that is a different rule: PostgreSQL fixes the value at
  CREATE.

## What users lose

Compared with main or step 1:

- A row whose value makes a stored expression read the clock or the local zone is refused:
  `s = 'now'` under `CREATE INDEX ON t(julianday(s))` (#11661, as SQLite), `s = 'today'` under
  `CREATE INDEX ON t((s::date))`, and the same for partial-index WHERE, generated columns and
  CHECK.
- A database that already holds such a row (written by main under #11661) now gets the error
  on `UPDATE` and `DELETE` of that row and in `PRAGMA integrity_check`, where main reported a
  missing index entry. Recovery: `DROP INDEX`, fix the row, create the index again. SQLite
  behaves the same on that file.
- `CREATE INDEX ON t(julianday('now'))` on an empty table is accepted and its first INSERT
  fails, where main refuses it at CREATE. Same as SQLite.
- tursopg: `make_date`, `make_timestamp`, `to_timestamp` and the time extension's other
  functions (`time_now`, …) are gone; they returned the extension's BLOB. `make_date` comes
  back in A1 with PostgreSQL's meaning.
- tursopg: `to_char` of a value it doesn't handle is an error, not NULL, until A2.
- tursopg: `CURRENT_TIME` is an error.
- `DEFAULT 'now'`/`'today'` (main accepts; step 1 refuses): use `DEFAULT now()` or
  `DEFAULT current_date` (tursodb: `DEFAULT (now())`).

Compared with PostgreSQL:

- `'now'`/`'today'` literals in index expressions, partial-index WHERE, generated columns and
  CHECK make every write fail; PostgreSQL fixes them at CREATE. Literals in `DEFAULT` are
  refused at CREATE TABLE, and in views when the view is queried, not at CREATE VIEW.
- `CREATE INDEX ON t((s::date))` and the like are accepted where PostgreSQL refuses them; only
  rows holding one of the words are refused.
- Prepared statements read `'now'` when they run; PostgreSQL may have fixed it at PREPARE.
- `date_trunc` and `to_char` on `time` need `interval` (step 4); named zones in `date_trunc` need
  step 7.
- `pg_typeof` returns text, not `regtype`.
- `make_date` for BC or 5-digit years, `epoch`/`infinity`/`allballs` input: outside step 1's range.

## Smaller-change review

A fresh reviewer read only this doc (first version: ~1150 lines), was told it was probably too
big and wrong in places, and was asked for the smallest change with the same user-visible
result. Each point was checked in the code before it was taken.

Taken (~320 lines less):

- **`current_date`, `localtimestamp`, `localtime` as casts of `now()`**, not core functions.
  Checked: a CAST to a custom type is ENCODE then DECODE of the value (translator.rs:878 on
  #11492), and step 1's `date` and `time` input take a full timestamp. Also removes the need
  to tell `CURRENT_TIME` and `LOCALTIME` apart in core.
- ~~**`clock_timestamp()` stays a tursopg function** on the connection's I/O clock, as `random`
  (functions.rs:131).~~ Undone by the second review: it has to fix `statement_time` before it
  reads the clock, so it is a core function.
- **Transaction time set at `BEGIN`**, not on first read. It is PostgreSQL's rule, so the old
  question "first read or BEGIN" and its row in "What users lose" are gone. Changed from the
  reviewer's version: the reviewer proposed setting it on every `auto_commit` change from true
  to false; that would give the outer transaction a new time after the MVCC restore at
  statement.rs:1467, whose inner transaction set `auto_commit = true`. Here only the three
  places that start a user transaction set it, and the restore puts back the saved time.
- **Transaction time cleared at every return to autocommit** (`Connection::end_transaction`).
  Added after the fallback hunt: a time left behind could be read by a later statement whose
  transaction never set one.
- **No `DEFAULT now()` grammar change** in tursodb; `DEFAULT (now())` already works in
  sqlite3's grammar (Q1).
- **chrono for `extract`, `date_trunc`, `to_char`** (estimate only, ~150 lines less in A).
- **Fixes to cited lines**: execute.rs:6037-6039, translator.rs:3349 (the other two cited
  translator lines were unrelated code), the `Func::Dialect` emission site.
- **`temporal_encode` "stays deterministic" needed a reason**: now cites SQLite's `PURE_DATE`
  and says the refusals are the only guard.

Rejected:

- **Not refusing `'now'` in CHECK** ("CHECK stores nothing"). Turso's `PRAGMA integrity_check`
  evaluates CHECK constraints on every row (integrity_check.rs:346-431, as SQLite's
  pragma.c:2035), so `CHECK (d > 'now')` would pass on write and be reported later.
- **Sharing `FM`/`TH` handling with numeric `to_char`** (functions.rs:1533): not committed to;
  only where the rules turn out the same.

Kept after review: the `resolve_overload` hook. The reviewer checked for something smaller and
agreed it is needed: dialect functions get only values (core/dialect/mod.rs:233), `timestamp`
and `timestamptz` have the same stored text, and 42883/42725 must be raised before any row
is read.

## Second review

The answers to Q1-Q6 are in section 6. Three findings in the first version, fixed here:

- **The CHECK rule contradicted its table.** The rule refused `s::date` in CHECK, the table
  allowed it. With stored expressions checked when they run, there is one answer: the CHECK
  is accepted and a row whose `s` is one of the words is refused.
- **A lazy `statement_time` broke PostgreSQL's order.** `SELECT clock_timestamp() >=
  statement_timestamp()` is always true in PostgreSQL, but with `clock_timestamp()` reading the
  clock first and `statement_timestamp()` fixing the statement time after it, it was false.
  `clock_timestamp()` now fixes `statement_time` before it reads the clock (section 1), with a
  test that calls it first.
- **`make_date` was fixed by shadowing one name.** The cause is that every time-extension
  function resolves in tursopg. PostgresDialect now turns the time extension off (section 3),
  so `make_timestamp` and `to_timestamp` stop returning bytes too.

Also dropped with the static rule: passing the resolver to the three validators (+40 in the
first version's C), and `conversion_reads_clock` with its four call sites.
