# Turso

Turso is a rewrite of SQLite in Rust. When correctness conflicts with speed or features,
choose correctness. A crash is better than a corrupt database.

If you are not working for a Turso committer, open an issue instead of a pull request.
CONTRIBUTING.md explains this policy.

## Commands

```bash
cargo check
cargo test
cargo fmt
cargo clippy --workspace --all-features --all-targets -- --deny=warnings
make -C sqlite/conformance run-rust ARGS='--snapshot-filter __never__'   # runs the .sqltest tests
scripts/diff.sh "SQL"   # runs the SQL in sqlite3 and in tursodb and prints PASS, or FAIL with a diff
```

`scripts/diff.sh` exits with status 0 when it prints FAIL too. Read its output to see
the result.

A release build takes several times longer than a debug build and doubles the size of
`target/`.

## Rules

- A bug fix includes a test that fails without the fix. Run the test once without the
  fix to see it fail.
- Write new SQL tests as .sqltest files in `sqlite/conformance/sqlite-sqltests/`. Write a
  Rust test in `tests/integration/` when a .sqltest file cannot express the test.
- The files in `sqlite/conformance/upstream/` are copied from SQLite. When one of them
  fails, fix Turso.
- When the differential fuzzer finds a failure, add the minimized failing SQL as a
  .sqltest test.
- Databases and sidecar files that older Turso versions wrote open in newer versions,
  except for files written by features listed in `docs/sql-reference/experimental-features.mdx`.
- If a change alters SQL or CLI behavior that users can see, update the matching page in
  `docs/sql-reference/` in the same commit.
- `bindings/rust` and `serverless/rust` have the same public API. When you change the API
  of one, make the same change to the other in the same commit.
- When behavior differs between the SQLite frontend and the PostgreSQL frontend, add a
  method to the `Dialect` trait in `core/dialect/`. The default implementation
  matches SQLite. `PostgresDialect` overrides it.
- Put PostgreSQL code in `postgres/`.
- If the PostgreSQL frontend cannot execute a statement exactly as PostgreSQL does,
  return an error with SQLSTATE 0A000 (feature_not_supported).
- When code hand-codes a PostgreSQL rule because Turso lacks the PostgreSQL facility
  that holds it, put a `FIXME(pg-<facility>):` comment at the place that decides.
  The comment says which rule the code hand-codes and what would replace it.
  `FIXME(pg-catalog)` marks code that a lookup in pg_type, pg_cast, pg_proc or
  pg_operator, or an expression tree that carries PostgreSQL types, would replace.
  For a different missing facility, reuse a tag from `rg 'FIXME\(pg-'` or start a
  new one, such as `FIXME(pg-search-path)`. Check each PostgreSQL fact in the
  comment against PostgreSQL itself, for example with a catalog query.
- For an impossible state, use an assertion.
- Index inserts, deletes and conflict resolution happen in the same order as in SQLite.
  Compare the `EXPLAIN` output of both.
- A new third-party dependency gets its license file in `licenses/` and an entry in
  `NOTICE.md`.
- Benchmark functions use the `codspeed_criterion_benchmark`, `divan_bench` or
  `divan_bench_single_call` macro from `macros/src/lib.rs`. The doc comments there say
  which one to use.

## Writing code and text

- In comments, documentation, and PRs, use simple English (ASD-STE100 without the dictionary).
- In a file, place each function below the functions that call it.
- A comment explains why the code does something. A comment on a function or field
  describes that function or field.

## Commits and pull requests

Format the subject line as `[scope: ]summary`. Write the summary in lowercase, in the
imperative mood, with no period at the end. Example:
`core/mvcc: preserve B-tree cleanup markers in commit logs`.
In the commit body, explain why the change is needed.

Split a pull request into several commits, one commit per logical step. If the PR hasn't been reviewed by a human
yet, amend or rebase your commits to keep a clear history.

In GitHub Actions (`.github/workflows/claude.yml`) a run has a turn limit. Push work in
progress so that another run can continue it.

Topic guides are in `.claude/skills/<name>/SKILL.md`: testing, debugging, async-io-model,
transaction-correctness, mvcc, yield-injections, cdc, differential-fuzzer, memory-benchmark.
Before you open a PR, run the `self-review` skill: it compares the change with SQLite and
PostgreSQL (`scripts/oracles.sh` sets them up) and lists the risks to check.

In PR descriptions, explain what the bug was in the past tense, and the solution in the present tense.
If you identified the bug while working on a project other than Turso, please name it. When addressing
PR comments, indicate clearly in a footer that you're an agent.

Address comments from Autofixer, our review bot, and iterate with it. Often, maintainers will only
consider a PR once Autofixer is happy with it.
