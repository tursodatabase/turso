# Turso Agent Guidelines

SQLite rewrite in Rust. 40+ crate workspace.

## Quick Reference

```bash
cargo build                    # build. never build with --release
cargo test                     # rust unit/integration tests
cargo fmt                      # format (required)
cargo clippy --workspace --all-features --all-targets -- --deny=warnings  # lint
cargo run -q --bin tursodb -- -q # run the interactive cli. never run with --release

make test                      # TCL compat + sqlite3 + extensions + MVCC
make test-single TEST=foo.test # single TCL test
make -C testing/sqltests run-rust ARGS='--snapshot-filter __never__'  # sqltest runner (preferred for new tests)
CI=1 make -C testing/sqltests run-rust  # use only if snapshot tests are required

scripts/diff.sh "SQL" [label]  # compare sqlite3 vs tursodb output
```

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
└── tools/          # dbhash utility
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
| New SQL tests | `testing/sqltests/tests/` | `.sqltest` format preferred |
| Quick sqlite3 diff | `scripts/diff.sh` | Compare sqlite3 vs tursodb output for a query |
| MVCC testing REPL | `cli/mvcc_repl.rs` | Multi-conn concurrent txn testing REPL        |

## Guides

- **[Testing](docs/agent-guides/testing.md)** - test types, when to use, how to write
- **[Code Quality](docs/agent-guides/code-quality.md)** - correctness rules, Rust patterns, comments
- **[Debugging](docs/agent-guides/debugging.md)** - bytecode comparison, logging, sanitizers
- **[PR Workflow](docs/agent-guides/pr-workflow.md)** - commits, CI, dependencies
- **[Transaction Correctness](docs/agent-guides/transaction-correctness.md)** - WAL, checkpointing, concurrency
- **[Storage Format](docs/agent-guides/storage-format.md)** - file format, B-trees, pages
- **[Async I/O Model](docs/agent-guides/async-io-model.md)** - IOResult, state machines, re-entrancy
- **[MVCC](docs/agent-guides/mvcc.md)** - experimental multi-version concurrency (WIP)

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

## Benchmark Naming

- Criterion benchmark functions must use `#[turso_macros::codspeed_criterion_benchmark]` so stable and nightly CodSpeed runs get distinct benchmark names.
- Divan benchmark functions must use `#[turso_macros::divan_bench]` for the same stable/nightly naming behavior.

## Core Principles

1. **Correctness paramount.** Production DB, not a toy. Crash > corrupt
2. **SQLite compatibility.** Compare bytecode with `EXPLAIN`
3. **Every change needs a test.** Must fail without change, pass with it
4. **Assert invariants.** Don't silently fail. Don't hedge with if-statements
5. **Own your regressions.** If tests fail after your change, they are your regressions. Debug them directly. Never stash/revert to "check if they fail on main" — that wastes time and is categorically banned.
6. **Validate your hypotheses.**: If you suspect a given cause for a bug, validate it and provide incontrovertible evidence. NEVER make unearned assumptions.

## CI Note

Running in GitHub Action? Max-turns limit in `.github/workflows/claude.yml`. OK to push WIP and continue in another action. Stay focused, avoid rabbit holes.
