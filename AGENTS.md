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
make -C postgres/conformance run          # PostgreSQL .sqltest corpus against tursopg
make -C postgres/conformance run-postgres # same corpus against real PostgreSQL (Docker, or PG_DSN)

scripts/diff.sh "SQL" [label]  # compare sqlite3 vs tursodb output
```

## Where Tests Go

Add coverage to the narrowest existing harness that can express the bug. Extend an existing file over creating a new one.

- `sqlite/conformance/sqlite-sqltests/` - preferred. Runs the same `.sqltest` against Turso and SQLite; use first for parser, planner, executor, and SQL semantics.
- `tests/integration/` - Rust-driven scenarios that don't fit `.sqltest`: API regressions, multi-connection orchestration, storage assertions, injected failures, timeouts.
- `sqlite/conformance/upstream/` - imported SQLite golden tests. Fix Turso, don't edit these; edits are for upstream syncs or harness maintenance.
- `postgres/conformance/pg-sqltests/` - PostgreSQL frontend `.sqltest`; must pass on both tursopg and real PostgreSQL.
- `testing/cli_tests/` - Python CLI and end-to-end command tests.
- `tests/fuzz/` - minimized fuzz regressions and edge cases kept as Rust tests.
- `testing/simulator/`, `testing/concurrent-simulator/` - deterministic concurrency, scheduling, and failure-injection coverage.
- `testing/differential-oracle/`, `testing/stress/` - deeper investigation tooling, not the first stop for a focused regression.
- A minimized differential-fuzzer failure becomes a permanent `.sqltest` case as part of its fix. The seed and artifact are debugging aids, not the regression test.

## Where to Look

| Task | Location | Notes |
|------|----------|-------|
| Query execution | `core/vdbe/execute.rs` | 12k LOC bytecode interpreter |
| SQL compilation | `core/translate/` | AST → bytecode, optimizer in `optimizer/` |
| B-tree/pages | `core/storage/btree.rs` | 10k LOC, SQLite-compatible format |
| WAL/durability | `core/storage/wal.rs` | Write-ahead log, checkpointing |
| SQL parsing | `sqlite/parser/src/parser.rs` | 11k LOC recursive descent |
| SQLite dialect | `sqlite/frontend/` | `turso_sqlite` crate |
| PostgreSQL frontend | `postgres/` | tursopg |
| CLI | `cli/` | REPL, MCP server, sync server; `mvcc_repl.rs` for multi-conn txn testing |
| Add extension | `extensions/core/` | ExtensionApi, scalar/aggregate/vtab traits |
| Add binding | `bindings/` | PyO3, NAPI, JNI, FRB, CGO patterns |
| Sync | `sync/`, `sdk-kit/` | Turso Cloud sync engine, SDK abstraction |
| Benchmarks | `benches/` | criterion, divan |
| User docs | `docs/sql-reference/` | statements/, functions/, pragmas.mdx, cli/ — update with any user-visible change |

## Guides

- **[Testing](docs/agent-guides/testing.md)** - test types, when to use, how to write
- **[Code Quality](docs/agent-guides/code-quality.md)** - correctness rules, Rust patterns, comments
- **[Debugging](docs/agent-guides/debugging.md)** - bytecode comparison, logging, sanitizers
- **[PR Workflow](docs/agent-guides/pr-workflow.md)** - commits, CI, dependencies; read before opening a PR
- **[Transaction Correctness](docs/agent-guides/transaction-correctness.md)** - WAL, checkpointing, concurrency
- **[Storage Format](docs/agent-guides/storage-format.md)** - file format, B-trees, pages
- **[Async I/O Model](docs/agent-guides/async-io-model.md)** - IOResult, state machines, re-entrancy
- **[MVCC](docs/agent-guides/mvcc.md)** - multi-version concurrency, snapshot isolation, checkpointing

## Core Principles

1. **Correctness paramount.** Production DB, not a toy. Crash > corrupt.
2. **SQLite compatibility.** Compare bytecode with `EXPLAIN`.
3. **Prove changes with tests.** A fix is believable only when a test fails without it and passes with it; run it both ways. If a test isn't practical or you can't run it, say so instead of calling the change done.
4. **Assert invariants.** For impossible states, an assertion beats a defensive if-statement that hides the problem.
5. **Own your regressions.** Tests that fail after your change are most likely caused by it; debug them directly rather than stashing to check main.
6. **Validate hypotheses.** Confirm a suspected cause with a repro, log, or flip test before building on it.
7. **Fix root causes, not symptoms.** Ask whether the fix hides a problem created upstream.
8. **Driver API parity.** Embedded (`bindings/rust`) and serverless (`serverless/rust`) expose the same API; change both together. Spec: `serverless/conformance/differential/README.md`.
9. **Backward compatibility.** Existing databases and sidecar files must keep working. If a change can't preserve that, raise it first.
10. **Keep user docs current.** Any user-visible SQL, CLI, or error change updates the matching `docs/sql-reference/` page in the same change.
11. **Dialect-specific behavior goes on the `Dialect` trait** in `core/dialect/mod.rs`, SQLite default, overridden in `PostgresDialect`. Not `DatabaseOpts` (client-public), not CLI flags or wire fields (don't reach embedded databases).
12. **Keep PostgreSQL code out of `core/`.** It belongs in `postgres/`; `core/` stays a SQLite engine with generic hooks. Unsure? Ask.

## Coding Style

- **Plain words, no jargon.** Say "statements skipped because EXPLAIN failed", not "statements outside the shared executable domain". Say "a statement that needs a table", not "bootstrap-safe statement".
- **Literal over metaphor.** Avoid load-bearing, pin, bite, sharp, arm, guard, bless, wedge, retire, retarget, answer, settle, carry, land, honor, rebase, hold. Don't say "run" when a more precise word exists. Prefer the domain's established terms over invented ones.
- **Callers first, callees after.** A file reads top to bottom: `commit()` then `write_frames()` then `sync_wal()`. Put a new helper below the functions that call it, not at the end of the file.
- **Names over comments.** Comments that restate code go stale; comment only where the code can't explain why.
- **A comment describes its item.** Who updates a field, in what order, when it's computed, or how the code used to work are facts about other code or the past; leave them out.
- **Commit messages:** `[scope: ]<imperative summary>` in lowercase, no trailing period, e.g. `core/mvcc: preserve B-tree cleanup markers in commit logs`. Body explains why and what invariant or bug it addresses, not the diff. Optional `Tests:` line and `Fixes #1234`. Conventional Commit prefixes not required. See [CONTRIBUTING.md](CONTRIBUTING.md).
- **Benchmark naming:** criterion uses `#[turso_macros::codspeed_criterion_benchmark]`, divan uses `#[turso_macros::divan_bench]`, so stable and nightly CodSpeed runs get distinct names.

## CI Note

Running in GitHub Action? Max-turns limit in `.github/workflows/claude.yml`. OK to push WIP and continue in another action. Stay focused, avoid rabbit holes.
