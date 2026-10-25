# Turso Agent Guidelines

SQLite rewrite in Rust. 40+ crate workspace.

## Quick Reference

```bash
cargo build                    # build (debug; release builds are slow and double the target dir)
cargo check                    # type check. prefer to `cargo build`
cargo test                     # rust unit/integration tests
cargo fmt                      # format (required)
cargo clippy --workspace --all-features --all-targets -- --deny=warnings  # lint
cargo run -q --bin tursodb -- -q # run the interactive cli (debug build)

make test                      # TCL compat + sqlite3 + extensions + MVCC
make test-single TEST=foo.test # single TCL test
make -C sqlite/conformance run-rust ARGS='--snapshot-filter __never__'  # sqltest runner (preferred for new tests)
CI=1 make -C sqlite/conformance run-rust  # use only if snapshot tests are required

scripts/diff.sh "SQL" [label]  # compare sqlite3 vs tursodb output
```

## Testing

### Running Tests

- `cargo test` - Rust unit and integration tests
- `make test` - broad compatibility suite (TCL, sqlite3, extensions, MVCC)
- `make test-single TEST=foo.test` - single legacy TCL test
- `make -C sqlite/conformance run-rust ARGS='--snapshot-filter __never__'` - preferred `.sqltest` runner for new coverage
- `CI=1 make -C sqlite/conformance run-rust` - only when snapshot tests are required
- `make -C postgres/conformance run` - run the PostgreSQL `.sqltest` corpus against tursopg
- `make -C postgres/conformance run-postgres` - run the PostgreSQL `.sqltest` corpus against a real PostgreSQL server (needs Docker, or a server at `PG_DSN`)

### Test Organization

Default: add coverage to the narrowest existing test harness that can express the bug. Prefer extending an existing test file or directory over creating a new one.

- `sqlite/conformance/sqlite-sqltests/` - preferred for SQL conformance coverage. These tests run the same scenario against both Turso and SQLite, so use them first for parser, planner, executor, and SQL semantics work that fits the `.sqltest` DSL.
- `tests/integration/` - primary fallback when the behavior cannot be expressed cleanly in `.sqltest`. Put API-level regressions, multi-connection orchestration, storage assertions, injected failures, timeout behavior, and other Rust-driven scenarios here.
- `sqlite/conformance/upstream/` - imported upstream SQLite golden tests. They are fixed compatibility coverage, so a Turso behavior change should be fixed in Turso rather than by editing them; edits here are for upstream syncs or harness maintenance.
- `postgres/conformance/pg-sqltests/` - `.sqltest` coverage for the PostgreSQL frontend. Tests are meant to pass on both tursopg (`run`) and real PostgreSQL (`run-postgres`).
- `testing/cli_tests/` - CLI-focused Python coverage for shell behavior and end-to-end command workflows.
- `tests/fuzz/` - minimized fuzz regressions and targeted edge cases that are easier to keep as Rust tests.
- `testing/simulator/` and `testing/concurrent-simulator/` - deterministic concurrency, scheduling, and failure-injection coverage for state-machine and I/O correctness.
- `testing/differential-oracle/` and `testing/stress/` - differential and long-running stress tooling. Use these for deeper investigation or specialized validation, not as the first stop for a focused regression test.

### Differential-Fuzzer Regressions

A minimized differential-fuzzer failure should end up as a permanent
`*.sqltest` regression case as part of its fix.
The seed and minimized fuzzer artifact are debugging aids, not substitutes for
the focused SQL regression.

## Structure

```
limbo/
├── core/           # Database engine (translate/, storage/, vdbe/, io/, mvcc/)
├── sqlite/
│   └── parser/     # SQL parser (lexer, AST, grammar)
├── cli/            # tursodb CLI (REPL, MCP server, sync server)
├── bindings/       # Python, JS, Java, .NET, Go, Rust
├── extensions/     # crypto, regexp, csv, fuzzy, ipaddr, percentile
├── testing/        # simulator/, concurrent-simulator/, differential-oracle/
├── sync/           # engine/, sdk-kit/ (Turso Cloud sync)
├── sdk-kit/        # High-level SDK abstraction
└── tools/          # dbhash utility, thesis (local Antithesis runner)
```

## Where to Look

| Task | Location | Notes |
|------|----------|-------|
| Query execution | `core/vdbe/execute.rs` | 12k LOC bytecode interpreter |
| SQL compilation | `core/translate/` | AST → bytecode, optimizer in `optimizer/` |
| B-tree/pages | `core/storage/btree.rs` | 10k LOC, SQLite-compatible format |
| WAL/durability | `core/storage/wal.rs` | Write-ahead log, checkpointing |
| SQL parsing | `sqlite/parser/src/parser.rs` | 11k LOC recursive descent |
| Add extension | `extensions/core/` | ExtensionApi, scalar/aggregate/vtab traits |
| Add binding | `bindings/` | PyO3, NAPI, JNI, FRB, CGO patterns |
| Deterministic tests | `testing/simulator/` | Fault injection, differential testing |
| New SQL tests | `sqlite/conformance/sqlite-sqltests/` | `.sqltest` format preferred |
| Quick sqlite3 diff | `scripts/diff.sh` | Compare sqlite3 vs tursodb output for a query |
| MVCC testing REPL | `cli/mvcc_repl.rs` | Multi-conn concurrent txn testing REPL        |
| User docs | `docs/sql-reference/` | statements/, functions/, pragmas.mdx, cli/ — update with any user-visible change |

## Guides

- **[Testing](docs/agent-guides/testing.md)** - test types, when to use, how to write
- **[Code Quality](docs/agent-guides/code-quality.md)** - correctness rules, Rust patterns, comments
- **[Debugging](docs/agent-guides/debugging.md)** - bytecode comparison, logging, sanitizers
- **[PR Workflow](docs/agent-guides/pr-workflow.md)** - commits, CI, dependencies
- **[Transaction Correctness](docs/agent-guides/transaction-correctness.md)** - WAL, checkpointing, concurrency
- **[Storage Format](docs/agent-guides/storage-format.md)** - file format, B-trees, pages
- **[Async I/O Model](docs/agent-guides/async-io-model.md)** - IOResult, state machines, re-entrancy
- **[MVCC](docs/agent-guides/mvcc.md)** - multi-version concurrency, snapshot isolation, checkpointing

## Commit Messages

Use an optional component scope followed by a lowercase imperative summary with
no trailing period:

```text
[scope: ]<imperative summary>

<why the change is needed and what invariant or bug it addresses>

<non-obvious implementation details or tradeoffs, if needed>

Tests: <relevant validation, if useful>

Fixes #1234
```

For example: `core/mvcc: preserve B-tree cleanup markers in commit logs`.
Explain intent rather than narrating the diff. Omit the body only when the
subject fully explains a trivial change. Conventional Commit prefixes such as
`feat(scope):` are not required. See [CONTRIBUTING.md](CONTRIBUTING.md) for a
complete example.

Before opening a PR, read the "PR Workflow" document linked above.

## Benchmark Naming

- Criterion benchmark functions use `#[turso_macros::codspeed_criterion_benchmark]` so stable and nightly CodSpeed runs get distinct benchmark names.
- Divan benchmark functions use `#[turso_macros::divan_bench]` for the same stable/nightly naming behavior.

## Core Principles

1. **Correctness paramount.** Production DB, not a toy. Crash > corrupt
2. **SQLite compatibility.** Compare bytecode with `EXPLAIN`
3. **Prove changes with tests.** A fix is only believable when a test fails without it and passes with it, so add or update such a test and run it both ways. A change that looks right but was never seen failing is how bugs slip through. If a test isn't practical (docs, pure refactors) or you can't run it, say so plainly rather than calling the change done.
4. **Assert invariants.** Surface failures loudly. For states that should be impossible, an assertion is better than a defensive if-statement that hides the problem.
5. **Own your regressions.** Tests that fail after your change are most likely caused by it, so start by debugging them directly. Stashing or reverting to check whether they also fail on main usually costs more time than it saves.
6. **Validate your hypotheses.** If you suspect a given cause for a bug, confirm it with concrete evidence (a repro, a log, a flip test) before building on it.
7. **Fix root causes, not symptoms** When designing a fix, ask yourself: is this a symptom of a deeper root cause? would my fix hide a problem that's created upstream?
8. **Driver API parity** Embedded (`bindings/rust`) and serverless (`serverless/rust`) drivers expose the same public API; add features to both in the same change. Spec: `serverless/conformance/differential/README.md`.
9. **Backward compatibility** Users' existing databases and sidecar files created by earlier Turso versions need to keep working with new versions. If a change can't preserve that, raise it before going further.
10. **Keep user docs current.** If a change adds, removes, or changes user-visible SQL behavior (syntax, functions, pragmas, data types, CLI commands, errors), update the matching page in `docs/sql-reference/` in the same change. For example, a new `JOIN` type goes in `docs/sql-reference/statements/select.mdx`.
11. **Dialect-specific behavior belongs on the `Dialect` trait.** If a behavior differs between the SQLite and PostgreSQL frontends (error handling, type semantics, name resolution, etc.), add a method to `core/dialect/mod.rs` with a default that matches SQLite and override it in `PostgresDialect`. `DatabaseOpts` is a poor fit because it is client-public API and clients shouldn't control dialect behavior; CLI flags and wire-protocol fields are a poor fit because they don't reach embedded databases.
12. **Keep PostgreSQL code out of `core/`.** PostgreSQL behavior and state belong in `postgres/`, so `core/` stays a SQLite engine with generic hooks. Unsure where something fits? Ask.

## Use plain language instead of complex jargon

OOGA BOOGA! Programming already complex! Use simple word! Say what you mean! Examples:

```diff
-    /// Number of generated statements outside the engines' shared executable domain.
+    /// Number of statements skipped because EXPLAIN failed in at least one engine.

...

-    fn empty_schema_only_selects_bootstrap_safe_statements() {
+    fn empty_schema_never_chooses_a_statement_that_needs_a_table() {
```

No-one knows what the hell a bootstrap-safe statement is. Everyone knows what "a statement that needs a table" is.
Prefer literal words over metaphors. Words like load-bearing, pin, bite, sharp, arm, guard, bless, wedge, retire,
retarget, answer, settle, carry, land, honor, rebase, and hold usually hide what is actually happening; say the plain
thing instead. Prefer the domain's established terms over invented ones.

## Code flows from top to bottom

A reader should be able to read a file from the top down without jumping
ahead to find what a name means. The general rule: place a function after
all of its call sites. Callers come first, callees follow.

```text
pub fn commit()         // entry point
fn write_frames()       // called by commit
fn sync_wal()           // called by write_frames
```

When adding a helper, put it below the functions that call it, not at the
end of the file or wherever the cursor happened to be.

## Prefer expressive code over comments

Good names and structure should explain most code, and comments that restate the code go stale. Where the code
alone can't explain why something is done, a short plain comment is welcome.

## CI Note

Running in GitHub Action? Max-turns limit in `.github/workflows/claude.yml`. OK to push WIP and continue in another action. Stay focused, avoid rabbit holes.
