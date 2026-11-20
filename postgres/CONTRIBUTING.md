# Contributing to the Turso Postgres Front-End

This is a PostgreSQL front-end for Turso: `tursopg` speaks the PostgreSQL wire
protocol, parses SQL with the real PostgreSQL grammar (libpg_query), and runs
queries through the `turso_core` engine. The goal is to behave like a real
PostgreSQL server, so most of the work is closing the gap between what real
PostgreSQL does and what the front-end does today.

Three things keep that work honest:

- **[`COMPAT.md`](COMPAT.md)** — feature-by-feature compatibility status, and
  where Turso intentionally diverges from PostgreSQL.
- **[`conformance/upstream/`](conformance/upstream/)** — regression tests
  imported verbatim from the PostgreSQL source tree, run against the front-end
  so divergence can't sneak in unnoticed.
- **[`conformance/pg-sqltests/`](conformance/pg-sqltests/)** — `.sqltest` tests
  that CI runs against the front-end and against a real PostgreSQL server
  (`make -C postgres/conformance run-postgres`). Tests of Turso-specific
  behavior have `@skip-if postgres "reason"`. Tests for SQL that the SQLite
  frontend also runs are in
  [`testing/shared-sqltests/`](../testing/shared-sqltests/), which CI runs on
  every backend.

## Verification workflow

When changing the parser, translator, or query behavior:

1. Check the relevant page of the PostgreSQL documentation.
2. Confirm the behavior against a real PostgreSQL server.
3. Encode the observed behavior in a test — an upstream regression script that
   now passes, a `.sqltest` in `conformance/pg-sqltests/` (or in
   `testing/shared-sqltests/` when the SQLite frontend must give the same
   result), or a Rust test (see below) — so it can't regress.

If Turso intentionally diverges from PostgreSQL, document that in
[`COMPAT.md`](COMPAT.md). Pay particular attention to clauses that parse but
are silently ignored: the parser accepts the full PostgreSQL grammar, so a
feature the translator drops produces wrong results, not an error.

## Known gaps

A known gap is a case where tursopg gives a different result than PostgreSQL. Each gap is a test with an ID, for example `E-19`.

- Gap tests are in [`conformance/pg-sqltests/gaps/`](conformance/pg-sqltests/gaps/), one file per category: `params-casts` (A), `operators-functions` (B), `output-formats` (C), `errors` (D), `ddl-catalog` (E).
- A test name is a short title in lower case, for example `int4-overflow-literal-cast`. The comment line above it gives the gap ID and the title. An open gap also gives its severity, for example `# A-07 (S1): int4 overflow literal cast`. When you fix a gap, remove the severity.
- Tests that share setup statements use a named `setup` block and `@setup`.
- The default `expect` block is the PostgreSQL result. `make -C postgres/conformance run-postgres` checks it.
- A gap test has a `@compat-gap "<kind>:<object>"` label and an `expect @tursopg` block with the current tursopg result. `make -C postgres/conformance run` checks it. The label names the PostgreSQL function, type, operator, cast or syntax form that the fix changes, for example `function:age`, `type:int4`, `operator:jsonb @> jsonb`, `cast:timestamptz -> date` or `syntax:on-conflict`. [`testing/sqltest/docs/dsl-spec.md`](../testing/sqltest/docs/dsl-spec.md) gives the rules.
- For errors, give the SQLSTATE with `@sqlstate <code>`. The default `expect error` block may give the PostgreSQL message. The `expect @tursopg` error block of a gap test may give the tursopg message, and the tursopg run compares it. tursopg fixes an error gap when it returns the PostgreSQL SQLSTATE, whatever its message.
- When tursopg gives the PostgreSQL result, the tursopg run fails with "gap fixed". Delete `@compat-gap` and the `expect @tursopg` block in the same change. When tursopg returns the PostgreSQL SQLSTATE with another message, keep the `expect @tursopg` error block with that SQLSTATE and the tursopg message.
- Keep the test when you fix a gap. CI fails when a gap test is deleted, renamed, or loses its gap with other SQL, setups, PostgreSQL answer, or skip, backend or requires modifiers. A reviewer decides whether a change that must delete a gap test merges with that check failing.
- An `expect @tursopg` block without `@compat-gap` may only change the error message of an error with the same SQLSTATE.
- `make -C postgres/conformance gaps` prints the gap counts by kind, label and class.
- Use `@skip "<gap id>: reason"` when the tursopg result is not stable for another reason.
- A test without an `@tursopg` block matches PostgreSQL. It is a regression test.
- `run-postgres` needs a server with the `en_US.UTF-8` locale, which is the default of the Docker image. B-12, B-74, B-94 and B-95 give different results with the `C` locale or an ICU locale.
- Cases that need bound parameters, a database reopen, two commands in one prepared statement, MVCC, or a query after a failed statement are in [`tests/integration/postgres/gaps.rs`](tests/integration/postgres/gaps.rs), named `gap_<id>_<short_title>`. A test of a gap that matches PostgreSQL asserts the PostgreSQL result. A test of an open gap is named `gap_<id>_tursopg_returns_wrong_result_<short_title>` and asserts the current wrong tursopg result, with the PostgreSQL result in a comment above it. When you fix the gap, its test fails: change the assert to the PostgreSQL result and remove `tursopg_returns_wrong_result` from the name. Run them with `cargo test -p turso_pg_tests --test integration_tests gaps::`.

Severity:

| Severity | Meaning |
|----------|---------|
| S1 | Wrong result or wrong data, with no error |
| S2 | Error where PostgreSQL succeeds |
| S3 | Wrong error code or message |
| S4 | Catalog or metadata gap |

A catalog or metadata gap is S4, even when tursopg returns an error or no rows.

## Running the upstream regression suite

The upstream regression suite is the day-to-day test loop. Run it with:

```bash
postgres/conformance/run.py                    # whole corpus
postgres/conformance/run.py boolean            # single test by name
postgres/conformance/run.py --max-diff-lines 0 boolean   # full diffs
```

This builds `tursopg` and the `pgregress` runner, starts a server on a fresh
temporary database, runs every `.sql` script under
[`conformance/upstream/`](conformance/upstream/), and byte-compares the psql
transcript against the paired `.out` file. Failed tests write the actual
transcript and a unified diff to `postgres/regress/results/`.

To drive an already-running server instead:

```bash
cargo run -p tursopg -- --server 127.0.0.1:5432 /tmp/regress.db &
cargo run -p turso_pg_regress -- --dsn 'postgres://127.0.0.1:5432/regression'
```

## Differential dump fuzzing

The Rust crate [`dump-fuzzer/`](dump-fuzzer/) uses Hegel to generate SQL, load it
into fresh PostgreSQL and Turso databases, and compare their `pg_dump` output.
It compares PostgreSQL ASTs and decoded COPY rows rather than dump bytes.

Install PostgreSQL clients that support `pg_dump --restrict-key`. Use Docker,
or set `PG_DSN` to a disposable PostgreSQL 18 server (the version in
`postgres/version`), using a `turso` role with LOGIN and CREATEDB. Matching
role names avoids default-privilege differences:

```bash
make -C postgres/conformance test-pgdump
make -C postgres/conformance test-pgdump FILE=pgdump/tables.sql
make -C postgres/conformance fuzz-pgdump FUZZ_ARGS='--cases 1000 --seed 42'
```

The Rust binary also supports direct fixture replay and offline comparison.
See the [fuzzer README](dump-fuzzer/README.md) for commands, comparison rules,
generator coverage, and failure artifacts. The previous Python byte comparator
has been replaced by this binary.

## The corpus is pristine

Everything under [`conformance/upstream/`](conformance/upstream/) is imported
**verbatim** from the PostgreSQL source tree (`src/test/regress/sql/` and
`expected/`). Never edit these files to match Turso behavior — a failing diff
is the to-do list, not a bug in the test. Only touch the corpus to import new
files or sync with upstream.

To grow the corpus, copy a `<name>.sql` script and its `<name>.out` expected
transcript from the same PostgreSQL release into `conformance/upstream/`. New
files are picked up automatically — no registration step. Prefer scripts with
few dependencies on other tests; the upstream suite shares one database, and
some scripts assume tables created by `test_setup.sql`.

The runner (`postgres/regress/`) plays the role of upstream's `pg_regress`:
it speaks the wire protocol directly and reproduces psql's transcript output
byte-for-byte (echoed input, aligned tables, `LINE n:` error markers). If a
corpus file needs a psql feature the runner doesn't emulate yet (see the
module docs in `postgres/regress/main.rs` for known gaps), extend the runner —
never adapt the corpus. If every test suddenly fails with uniform, systematic
diffs, suspect the runner's psql emulation before suspecting the server.

## Embedded errors

The `turso_pg` connection and statement APIs return `turso_pg::Error`.
Its `Display` implementation uses the PostgreSQL message, and its
`sqlstate`, `detail`, `table_name`, `column_name`, and `constraint_name`
fields are available without a separate conversion. `core_error()` and
`std::error::Error::source()` expose the original engine error.

The CLI prints these fields and the wire server serializes them. Error
classification and PostgreSQL wording belong in the frontend. Explicit
access through `Connection::inner()` uses the core API and its error type.

## Rust tests

Focused tests that don't fit the transcript format live in Rust:

- `postgres/parser/tests/` — parse/translate coverage (valid and invalid SQL).
- `postgres/tests/integration/` — end-to-end coverage by area (catalog, COPY,
  dialect, sequences, ...), driven through the front-end API.

Run them with:

```bash
cargo test -p turso_pg_parser -p turso_pg_tests
```

Every behavior change needs a test that fails without the change and passes
with it. Upstream regression scripts double as tests here: if your change
shrinks a `.diff` under `postgres/regress/results/`, say so in the commit
message.
