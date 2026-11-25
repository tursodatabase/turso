---
name: debugging
description: How to debug tursodb using bytecode comparison, logging, ThreadSanitizer, deterministic simulation, stack usage scripts, and corruption analysis tools
---
# Debugging Guide

## Bytecode Comparison

When Turso and SQLite give different results, run `EXPLAIN` on both:

```bash
sqlite3 :memory: "EXPLAIN SELECT 1 + 1;"
cargo run --bin tursodb :memory: "EXPLAIN SELECT 1 + 1;"
```

Different bytecode means a bug in code generation (`core/translate/`). Same bytecode
with different results means a bug in the VM or storage layer.

## Logging

`tursodb` logs with `tracing`. It reads `RUST_LOG` (default level: warn) and writes to
stderr, or to a file with `-t <file>` (`--tracing-output`).

```bash
RUST_LOG=turso_core=trace cargo run --bin tursodb -- -t /tmp/turso.log :memory: 'SELECT 1;'
```

Logs can reach megabytes. `make test` sets `RUST_LOG=off`, so use `cargo run` or `cargo test` to get logs.

## Threading Issues

Run the stress test under ThreadSanitizer. It needs a nightly toolchain with the `rust-src` component, and `--target` set to the host target (`x86_64-unknown-linux-gnu` on Linux, `aarch64-apple-darwin` on Apple Silicon):

```bash
RUSTFLAGS="-Zsanitizer=thread" cargo +nightly run -Zbuild-std --target x86_64-unknown-linux-gnu \
  -p turso_stress -- --vfs syscall --nr-threads 4 --nr-iterations 1000
```

Flags: `--vfs`, `--nr-threads` (`-t`), `--nr-iterations` (`-i`); see `--help` for the rest.

## Deterministic Simulation

Replay a failure with its seed. The simulator binary is named `limbo_sim`.

```bash
RUST_LOG=limbo_sim=debug cargo run --bin limbo_sim -- -s <seed>

# Whopper (concurrent simulator). Repeat a run with the same seed and flags.
SEED=1234 ./testing/concurrent-simulator/bin/run
```

See `testing/concurrent-simulator/README.md` for flags such as `--mode` and `--swarm`.

## Stack Usage

Parsing and translating an expression recurse once per level of nesting, so frame
size limits how deep an expression can be. Scripts in `scripts/stack/` measure it
(`-h` lists options). Use a release build (`cargo build --release --bin tursodb`);
debug builds do not reuse stack slots, so their frames say little about what users run.

```bash
scripts/stack/frame-sizes.sh '<function name pattern>'        # frame size per function; -c compares two binaries
scripts/stack/min-stack.sh -S -b target/release/tursodb deep.sql   # smallest stack that runs a script
scripts/stack/stack-profile.sh deep.sql                 # functions on the stack at its deepest point (lldb)
```

Functions that recurse per level should be small. Move rare and large paths into
`#[inline(never)]` helpers, like SQLite's `SQLITE_NOINLINE`. LLVM reserves the full
frame when a function starts, so a large value in any branch counts at each level.
Move the work before and after the recursive call into `#[inline(never)]` helpers,
and keep large values (plans, contexts, AST nodes) in a `Box`.

`sqlite/conformance/turso-sqltests/nesting-limits.sqltest` runs queries at the
nesting limits, and one level past them, on the Rust backend. CI runs it with a
release build, on worker threads with a 2 MiB stack. A debug build skips it. When
you add a kind of nesting, add a query there.

## Corruption

For WAL corruption and integrity failures, use `scripts/corruption-debug-tools/`.
See [references/CORRUPTION-TOOLS.md](./references/CORRUPTION-TOOLS.md).
