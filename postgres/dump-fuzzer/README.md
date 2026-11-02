# PostgreSQL / Turso dump differential fuzzer

`pg-dump-fuzzer` generates SQL with Hegel, applies the same SQL to fresh
PostgreSQL and `tursopg` databases, dumps each with the same `pg_dump`, and
compares parsed structure. A failed command, invalid dump, or mismatch fails
the property. Hegel shrinks the generated SQL choices and prints the reduced
workload. Replay reads the saved profile unless `--profile` overrides it.
Failures are never discarded because a frontend rejects them.

## Use

From the repository root:

```sh
cargo build -p tursopg -p turso_pg_dump_fuzzer
POSTGRES_TEST_USER=turso POSTGRES_VERSION=18 postgres/conformance/with-postgres.sh target/debug/pg-dump-fuzzer fuzz --cases 1000 --seed 42
```

`with-postgres.sh` uses `PG_DSN` if set, otherwise starts disposable PostgreSQL
in Docker. The reference role needs CREATEDB permission; the compatible profile requires its
name to be `turso` (the table profile accepts other names). Each case creates a
uniquely named database and drops only that database. The provided database
is used only as an administrative connection. Use a disposable server.
PostgreSQL clients must support `pg_dump --restrict-key` (including current
patched PostgreSQL 17 and PostgreSQL 18 clients).

```sh
# Replay saved SQL against both servers (PG_DSN required).
target/debug/pg-dump-fuzzer replay path/to/input.sql

# Compare existing dumps without connecting to a server.
target/debug/pg-dump-fuzzer compare postgres.sql turso.sql

# Select executables and output directory before the subcommand.
target/debug/pg-dump-fuzzer --binary target/debug/tursopg --output /tmp/dump-fuzz fuzz --cases 100 --seed 42

# Parser, oracle, and process failure tests; no server required.
cargo test -p turso_pg_dump_fuzzer
```

`--postgres-dsn` overrides `PG_DSN`. `--binary` defaults to
`target/debug/tursopg` and also accepts `TURSOPG_BINARY`. `--psql` and
`--pg-dump` select client executables. Defaults are relative to the working
directory; the Makefile supplies absolute executable paths.

For an optimized fuzzer build, use `cargo build -p turso_pg_dump_fuzzer --profile
pg-dump-fuzzer` and run `target/pg-dump-fuzzer/pg-dump-fuzzer`. This profile
enables panic unwinding, which Hegel needs to catch failures, shrink cases, and
write the final report. The workspace's ordinary release profile aborts on
panic and cannot run the `fuzz` subcommand. Debug builds also support unwinding.

## CI

The SQL Tests workflow runs the `PostgreSQL dump differential fuzzer` job on
pull requests, pushes to main, and manual workflow dispatches. It checks the
fuzzer and comparator, then runs 100 cases with an automatic seed, the compatible profile,
and `--fail-fast` against PostgreSQL 18. Known failing opt-in features remain
disabled. CI builds and checks the fuzzer with the `pg-dump-fuzzer` Cargo profile;
the database server still uses the release profile.

The job uploads `pg-dump-fuzzer-results` for one day, including the JSON
generation summary and any failed cases with their SQL, dumps, and logs.
Download that artifact from the workflow run to inspect or replay a failure.
Run the same configuration locally with a new automatic seed:

```sh
make -C postgres/conformance fuzz-pgdump FUZZ_ARGS='--cases 100 --fail-fast'
```

## Feature configuration

The hard-coded compatible configuration is in `src/generation/config.rs`. It disables
`rls`, `policies`, `grants`, and `default-privileges` by default,
including the dependent operations described below. Tables, data, and indexes
remain enabled, including column defaults, mixed-case names, and DESC index keys.

Use `fuzz --enable-features` to opt features back in, and `--disable-features`
for additional exclusions. Both accept comma-separated names and can be
repeated. They only apply to the compatible profile; unknown names and use with the table
profile are errors. Conflicting enable/disable requests are rejected.
Enabling a child also enables its required parent: `force-rls` enables `rls`,
`alter-policy`/`drop-policy` enable `policies`, and `revokes` enables `grants`. Index subfeatures (`composite-indexes`,
`unique-indexes`, `descending-indexes`) enable `indexes`.
Explicitly disabling a required parent is an error.
When opting into RLS with FORCE enabled, the reference dump role needs
BYPASSRLS; otherwise PostgreSQL itself rejects the dump. Use
`--disable-features force-rls` to exercise RLS without FORCE.

```sh
# Current compatible defaults: no exclusion list needed.
make -C postgres/conformance fuzz-pgdump FUZZ_ARGS='--cases 1000 --seed 42 --fail-fast'

# Retest policies, but leave ALTER POLICY disabled.
make -C postgres/conformance fuzz-pgdump FUZZ_ARGS='--cases 100 --fail-fast --enable-features policies --disable-features alter-policy'
```

| Name | SQL omitted |
| --- | --- |
| `defaults` | Column DEFAULT clauses |
| `mixed-case-identifiers` | Mixed-case table and column names; lowercase quoted names remain |
| `indexes` | CREATE INDEX |
| `composite-indexes` | Index keys with more than one column |
| `unique-indexes` | UNIQUE indexes |
| `descending-indexes` | DESC index keys (ASC remains) |
| `rls` | ENABLE, FORCE, and DISABLE ROW LEVEL SECURITY |
| `force-rls` | FORCE only; ENABLE/DISABLE remain |
| `policies` | CREATE, ALTER, and DROP POLICY |
| `alter-policy` | ALTER POLICY only |
| `drop-policy` | DROP POLICY only |
| `grants` | Table GRANT and REVOKE |
| `revokes` | REVOKE only; GRANT remains |
| `default-privileges` | ALTER DEFAULT PRIVILEGES |

Policies can be generated without RLS. Default privileges can be generated
without table grants. Disabling a parent removes dependent statements so, for
example, disabling policies cannot leave an ALTER POLICY for a missing policy.
The comparator and privilege-dump options are unchanged: exclusions never
filter SQL, data, ACLs, or differences out of the dumps.

Failure artifacts include `disabled-features.json`. Replay copies that metadata
but executes the saved SQL verbatim, even if it contains disabled syntax. The
metadata records how the original SQL was generated; it does not filter replay.

### Frontend observations

Schema generation enables column defaults, mixed-case table and column names,
and ascending and descending index keys. The PostgreSQL catalogs preserve table
name spelling and index direction, and render integer literal defaults using
PostgreSQL's type casts. The comparator still compares these AST fields exactly.

Access-control generators remain disabled: RLS, policies, grants, and default
privileges require separate frontend support. Their child operations have not
been tested independently of those unsupported parent operations.

## Fail fast

Add `--fail-fast` to stop after the first failing workload, keeping its SQL,
dumps, logs, and profile. Shrinking is disabled, and Hegel's final diagnostic
uses the saved failure instead of executing the workload again. Cleanup still
finishes before exit. Without the flag, normal shrinking and replay remain enabled.

```sh
make -C postgres/conformance fuzz-pgdump FUZZ_ARGS='--cases 1000 --seed 42 --fail-fast'
```

## Progress

Interactive terminals show an `indicatif` spinner with elapsed time, attempt
number, pass/fail counts, and the current operation: saving input, creating the
reference database, starting Turso, loading SQL, dumping each engine, cleanup,
and AST comparison. Failures print their artifact paths immediately.

After the first failure the display marks subsequent attempts as rechecks that
include shrinking/replay. Hegel does not expose its current phase here, so this
is not an exact phase indicator. Attempts count generated SQL workloads sent to the runner, excluding discarded
generator draws. They include shrinking and replay and can
exceed `--cases`; there is no percentage or estimated completion time.

Redirected stderr receives plain stage lines instead of terminal animation.
`--no-progress` disables both forms of progress output, including for `replay`
and `compare`. Hegel's own failure diagnostics still appear. The final summary
reports elapsed time and attempt/pass/fail counts, including when Hegel fails.

## Generation summary

Every fuzz campaign prints a summary and saves a unique `summary-*.json` under
`--output`, including on failure. `--no-progress` silences progress messages,
not this final report. The terminal report uses comfy-table for grouped, aligned
counts: campaign results, generated input, per-engine loads/data, then SQL and
schema feature tables. Interactive terminals use Unicode borders and color;
redirected output uses ASCII borders without ANSI escapes. Set `NO_COLOR=1` to
disable color in a terminal. Yellow feature counts mean fewer complete loads
than generated occurrences; they do not by themselves indicate a dump mismatch.
Disabled generators are listed separately, and unexercised features remain visible.
Successful case directories are still removed; their
aggregate counts survive in the report. Reports include the configured seed (null when automatic) and resolved
feature exclusions, but no connection strings.

The schema detail rows break down column types (INTEGER, BIGINT, TEXT, BOOLEAN),
nullable/required columns (PRIMARY KEY implies required), columns with/without
defaults, and negative/zero/positive integer defaults. Index rows show uniqueness,
method, and single/multiple columns. Policy rows show permissive/restrictive mode,
ALL/SELECT/INSERT/UPDATE/DELETE commands, USING, and WITH CHECK. GRANT, REVOKE,
and default privileges each have separate counts for those five privileges;
ALL counts as its own choice, not as five separate privileges.

These counts describe declarations encountered in workloads, not the final schema
after policy drops or other changes. The generator uses inline column
constraints and single- or multi-column B-tree indexes with ASC/DESC keys.
The report includes a table-width histogram and unique/non-unique index counts. The detailed rows use the
same generated/complete-PostgreSQL/complete-Turso columns as SQL feature totals and
are stored under each `features` map in JSON, including zero counts.

The report separates three measurements:

- **Generated:** parsed SQL workloads, table counts, initially empty/populated
  tables, explicit VALUES rows and NULL cells, and counts for each SQL feature.
  Fuzz workloads populate tables with VALUES inserts only. For externally
  supplied SQL, initial population counts explicit VALUES rows; rows produced
  by INSERT SELECT are not inferred.
  Feature rows include zero counts so unexercised syntax remains visible.
- **Executed:** fully loaded workloads and their feature counts per engine.
  A failed load contributes no completed feature counts. Separately, psql's
  successful INSERT/UPDATE/DELETE command tags supply acknowledged affected-row
  totals, including the successful prefix of a failed load. These are row
  operations, not distinct rows or a final table size.
- **Dumped:** successfully parsed COPY dumps per engine, their rows,
  empty/populated COPY blocks, NULL cells, and duplicate rows beyond the first
  occurrence within each table. Literal `\N` and empty text are distinct from
  NULL. Invalid or incomplete dumps contribute no dump counts.

These are workload-generation observations, not engine code coverage. All
actual runner executions count, including shrinking/replay; they are not unique
examples or Hegel generation-phase statistics. Cached fail-fast diagnostics do
not count again. The SQL counters describe this generator's table/VALUES workload,
not arbitrary SQL semantics (for example, they do not simulate transactions).

## Comparison rules

SQL is parsed using `pg_query`, the same PostgreSQL grammar library used by
the frontend. Comments, whitespace, parser source locations, and the fixed
psql restriction wrapper are excluded. All other AST fields remain, including
object names, schema qualification, defaults, constraints, index definitions,
and statement order.

Default text COPY data is decoded into rows of strings and null values,
following [PostgreSQL's text COPY format](https://www.postgresql.org/docs/current/sql-copy.html#SQL-COPY-FORMATS).
Rows are sorted within each COPY block, preserving duplicate counts, column
order, empty strings, literal `\N`, and nulls. Escape spellings such as octal
and hex compare by decoded value. Numeric and other typed COPY values remain
text; this is structural dump equivalence, not a proof of SQL semantic
equivalence. Unsupported COPY options, malformed rows, and missing terminators
fail rather than being omitted. Dumps use UTF-8 text.

INSERT-format dumps can also be compared: contiguous INSERT statements are
sorted while retaining duplicates. The fuzzer uses normal COPY dumps because
`pg_dump --inserts` requires DECLARE CURSOR, which Turso does not support.
SQL statement ordering outside those data blocks is significant.

The dump command uses `--no-owner` to exclude deployment-specific ownership.
The default `--profile compatible` retains privileges. `--profile tables` additionally
uses `--no-privileges` to focus on table/data dumps. Default privileges can name
the creating role even with `--no-owner`; use a reference role named `turso`
when comparing those against Turso's fixed role name. The CLI enforces this for
the compatible profile. The dump Makefile targets request this role from the disposable Docker
helper; an existing `PG_DSN` must already use that role. Other differences, including SET commands
from different server versions, are retained. Use compatible PostgreSQL/client
versions when interpreting failures. The comparator does not restore dumps.
Offline comparison accepts wrapper-free SQL or the fixed
`TursoPgdumpConformance` restriction key generated by this runner.

## Extending generation

Generation lives in the library under `src/generation/`:

- `mod.rs`: `cases()` produces typed `Workload` values; `workload()` maps those
  values to SQL for the CLI.
- `model.rs`: tables, columns, constraints, values, rows, statement variants,
  and the single SQL rendering implementation.
- `config.rs`: profiles, feature switches, prerequisites, and compatible defaults.
- `schema.rs`: Hegel generators for table definitions and indexes.
- `data.rs`: Hegel generators for values, rows, and INSERT VALUES.
- `access_control.rs`: Hegel generators for policies, privileges, and RLS.

Generators implement Hegel's `Generator<T>` interface, using `map`,
`flat_map`, `optional`, and `#[hegel::composite]` to compose typed values.
For example, `gs::integers::<i64>().map(Value::Integer)` generates an SQL integer
value without formatting it. The table generator is chained with `flat_map` so
subsequent statement generators use the schema produced in the same case.

An insert is represented as data:

```rust
Statement::Insert {
    table: table.name.clone(),
    row: Row(vec![Value::Integer(1), Value::Text("example".into())]),
}
```

Add a statement variant and its renderer to `model.rs`, then return that
variant from a generator in the appropriate module. Compose it in
`table_statements()` in the required execution order. Do not build SQL strings
inside generators. There is no separate custom generator trait or plugin registry.

Columns declare their types and constraints once. Table rendering and row
generation both read those definitions. Primary keys imply non-null unique
values; a Boolean primary key limits generated rows to two. Index keys and policy
predicates select columns from the generated table. Numeric policy predicates
use integer comparisons; text and Boolean columns use IS NULL or IS NOT NULL.
There are no fixed column-name assumptions.

The renderer quotes identifiers and escapes text centrally. Identifiers must
contain 1–63 UTF-8 bytes and no NUL. Text values cannot contain NUL. Text rendering
assumes `standard_conforming_strings = on`; `cases()` emits this setting first.
Callers constructing `Workload` manually must include
`Statement::StandardConformingStrings` or establish that session setting.

Add feature switches in `config.rs` and coverage counters in `summary.rs` and
`report.rs` as needed. Extend `generation/tests.rs` for the generated model,
rendered PostgreSQL AST, constraints, and feature exclusions. Combinator changes
can change the SQL produced by an older seed; saved SQL cases remain replayable.

The CLI imports the library generator; it handles arguments, replay metadata,
progress, and database campaigns. Generation modules do not launch databases.

## Generated workloads

The generator covers simple tables, indexes, data, and optional access-control
statements. Use PostgreSQL 18 as the reference server:
set `POSTGRES_VERSION=18` when using `with-postgres.sh`.

| SQL surface | Generator coverage |
| --- | --- |
| Tables/types | 1–8 columns with independently chosen INTEGER, BIGINT, TEXT, BOOLEAN types; optional numeric defaults, nullability, and one optional primary key; varied quoted names |
| Indexes | 0–3 B-tree indexes per table; 1–3 distinct columns in varying order; ASC/DESC keys; optional UNIQUE when a primary-key column is included |
| Grants | GRANT/REVOKE SELECT, INSERT, UPDATE, DELETE, ALL ON TABLE to/from PUBLIC |
| Default privileges | ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT on tables to PUBLIC |
| Policies | CREATE POLICY for ALL/SELECT/INSERT/UPDATE/DELETE; PERMISSIVE/RESTRICTIVE; USING and WITH CHECK; ALTER/DROP POLICY |
| RLS | ENABLE, FORCE, DISABLE ROW LEVEL SECURITY |
| Roles/membership | Not generated: roles are cluster-wide and pg_dump does not serialize role definitions; needs an isolated cluster plus pg_dumpall coverage |
| Restore round-trip | Dumps compared; restore/runtime authorization testing remains separate work |

The `compatible` profile can generate access-control syntax through
`--enable-features`; its current hard-coded defaults exclude the known failures
listed above. Any rejection of an enabled feature remains a failure. `tables`
uses the same table/data generator without access-control statements:

```sh
POSTGRES_TEST_USER=turso POSTGRES_VERSION=18 postgres/conformance/with-postgres.sh target/debug/pg-dump-fuzzer --profile compatible fuzz --cases 1000 --seed 42
postgres/conformance/with-postgres.sh target/debug/pg-dump-fuzzer --profile tables fuzz --cases 1000 --seed 42
```

Each workload has zero to four tables and zero to twelve initial rows per table.
Data generation uses INSERT VALUES only. A table without a primary key can
receive one additional duplicate row by repeating a generated VALUES row.
No SELECT, UPDATE, DELETE, or INSERT SELECT statements are generated.
Unique indexes include a primary-key column, so data generation cannot violate
their uniqueness. Integers span their Rust type
domains; strings include Unicode and control characters. NUL is removed because
PostgreSQL text cannot contain it. Materialized text and row counts are bounded
to keep database creation and shrinking practical.

Partial/expression indexes, checks, serial/generated columns, enums,
and views are not currently generated. Other SQL types,
foreign keys, custom schemas, transactional migrations, and concurrent writers
are not yet generated. PUBLIC policies exercise catalog serialization but do
not validate role-specific authorization or policy enforcement.

AST differences may still be semantically equivalent. The comparator retains
casts and other expression structure so these differences remain visible.

## Failures and replay

Failed cases retain `input.sql`, its comparison profile in `profile.txt`, original dumps, normalized `.ast.json` files
when parsing succeeds, command logs, and `failure.txt` under
`postgres/conformance/pgdump/results/fuzz/case-*`. Successful cases are deleted.
Each failed shrinking attempt has its own directory. Hegel's final failure
reports the directory for its reduced case; replay that `input.sql` directly.
A seed repeats choices for the same generator and Hegel version; SQL artifacts
remain useful when the generator changes. Native Hegel replay blobs are not
exposed by this CLI.

Commands have a 60-second timeout, server startup has a 10-second timeout,
and child processes are killed and reaped on exit. If database cleanup fails,
the error remains a failure and `drop.log` identifies the attempt. Forced
termination of the fuzzer can interrupt cleanup.
