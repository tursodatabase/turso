# Contributing to the Turso Postgres Front-End

This is a PostgreSQL front-end for Turso: `tursopg` speaks the PostgreSQL wire
protocol, parses SQL with the real PostgreSQL grammar (libpg_query), and runs
queries through the `turso_core` engine. The goal is to behave like a real
PostgreSQL server, so most of the work is closing the gap between what real
PostgreSQL does and what the front-end does today.

Two things keep that work honest:

- **[`COMPAT.md`](COMPAT.md)** — feature-by-feature compatibility status, and
  where Turso intentionally diverges from PostgreSQL.
- **[`conformance/upstream/`](conformance/upstream/)** — regression tests
  imported verbatim from the PostgreSQL source tree, run against the front-end
  so divergence can't sneak in unnoticed.

## Verification workflow

When changing the parser, translator, or query behavior:

1. Check the relevant page of the PostgreSQL documentation.
2. Confirm the behavior against a real PostgreSQL server.
3. Encode the observed behavior in a test — an upstream regression script that
   now passes, or a Rust test (see below) — so it can't regress.

If Turso intentionally diverges from PostgreSQL, document that in
[`COMPAT.md`](COMPAT.md). Pay particular attention to clauses that parse but
are silently ignored: the parser accepts the full PostgreSQL grammar, so a
feature the translator drops produces wrong results, not an error.

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
